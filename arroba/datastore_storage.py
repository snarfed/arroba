"""Google Cloud Datastore implementation of repo storage."""
import json
import logging

import dag_cbor
import dag_json
from google.cloud import ndb
from multiformats import CID, multicodec, multihash

from .repo import Repo
from .storage import BlockMap, Storage, SUBSCRIBE_REPOS_NSID
from .util import dag_cbor_cid

logger = logging.getLogger(__name__)


class WriteOnce:
    """:class:`ndb.Property` mix-in, prevents changing it once it's set."""
    def _set_value(self, entity, value):
        existing = self._get_value(entity)
        if existing is not None and value != existing:
            raise ndb.ReadonlyPropertyError(f"{self._name} can't be changed")

        return super()._set_value(entity, value)


class JsonProperty(ndb.TextProperty):
    """Fork of ndb's that subclasses TextProperty instead of BlobProperty.

    This makes values show up as normal, human-readable, serialized JSON in the
    web console.
    https://github.com/googleapis/python-ndb/issues/874#issuecomment-1442753255

    Duplicated in oauth-dropins/webutil:
    https://github.com/snarfed/webutil/blob/main/models.py
    """
    def _validate(self, value):
        if not isinstance(value, dict):
            raise TypeError('JSON property must be a dict')

    def _to_base_type(self, value):
        as_str = json.dumps(value, separators=(',', ':'), ensure_ascii=True)
        return as_str.encode('ascii')

    def _from_base_type(self, value):
        if not isinstance(value, str):
            value = value.decode('ascii')
        return json.loads(value)


class ComputedJsonProperty(JsonProperty, ndb.ComputedProperty):
    """Custom ComputedProperty for JSON values that stores them as strings.

    ...instead of like StructuredProperty, with "entity" type, which bloats them
    unnecessarily in the datastore.
    """
    def __init__(self, *args, **kwargs):
        kwargs['indexed'] = False
        super().__init__(*args, **kwargs)


class WriteOnceBlobProperty(WriteOnce, ndb.BlobProperty):
    pass


class AtpBlock(ndb.Model):
    """A data record, MST node, or commit.

    Key name is the DAG-CBOR base32 CID of the data.

    Properties:
    * dag_cbor: bytes, DAG-CBOR encoded value
    * data: dict, DAG-JSON value, only used for human debugging
    * seq: int, sequence number for the subscribeRepos event stream

    """
    dag_cbor = WriteOnceBlobProperty(required=True)
    seq = ndb.IntegerProperty(required=True)

    @ComputedJsonProperty
    def data(self):
        return json.loads(dag_json.encode(dag_cbor.decode(self.dag_cbor)))

    @staticmethod
    def create(data, seq):
        """Writes a new AtpBlock to the datastore.

        Args:
          data: dict value
          seq: integer

        Returns:
          :class:`AtpBlock`
        """
        assert seq > 0
        encoded = dag_cbor.encode(data)
        digest = multihash.digest(encoded, 'sha2-256')
        cid = CID('base58btc', 1, multicodec.get('dag-cbor'), digest)

        block = AtpBlock(id=cid.encode('base32'), dag_cbor=encoded, seq=seq)
        block.put()
        return block


class AtpRepo(ndb.Model):
    """An ATProto repo.

    Key name is DID. Only stores the repo's metadata. Blocks are stored in
    :class:`AtpBlock`s.

    Properties:
    * handles: str, repeated, optional
    * head: str CID
    """
    handles = ndb.StringProperty(repeated=True)
    head = ndb.StringProperty(required=True)


class AtpSequence(ndb.Model):
    """A sequence number for a given event stream NSID.

    Sequence numbers are monotonically increasing, without gaps (which ATProto
    doesn't require), starting at 1. Background:
    https://atproto.com/specs/event-stream#sequence-numbers

    Key name is XRPC method NSID.

    At first, I considered using datastore allocated ids for sequence numbers,
    but they're not guaranteed to be monotonically increasing, so I switched to
    this.
    """
    next = ndb.IntegerProperty(required=True)

    @classmethod
    @ndb.transactional()
    def get_next(cls, nsid):
        """Returns the next sequence number for a given NSID.

        Creates a new :class:`AtpSequence` entity if one doesn't already exist
        for the given NSID.

        Args:
          nsid: str, the subscription XRPC method for this sequence number

        Returns:
          integer, next sequence number for this NSID
        """
        seq = AtpSequence.get_or_insert(nsid, next=1)
        ret = seq.next
        seq.next += 1
        seq.put()
        return ret


class DatastoreStorage(Storage):
    """Google Cloud Datastore implementation of :class:`Storage`.

    Sequence numbers in :class:`AtpBlock` are allocated per commit; all blocks
    in a given commit will have the same sequence number. They're currently
    sequential counters, starting at 1, stored in an :class:`AtpSequence` entity.

    See :class:`Storage` for method details.
    """
    def create_repo(self, repo):
        assert repo.did
        assert repo.cid

        handles = [repo.handle] if repo.handle else []
        atp_repo = AtpRepo(id=repo.did, handles=handles,
                           head=repo.cid.encode('base32'))
        atp_repo.put()
        logger.info(f'Stored repo {atp_repo}')

    def load_repo(self, did=None, handle=None):
        assert bool(did) ^ bool(handle), f'{did} {handle}'

        repo = None
        if did:
            repo = AtpRepo.get_by_id(did)
        else:
            repo = AtpRepo.query(AtpRepo.handles == handle).get()

        if not repo:
            logger.info(f"Couldn't find repo for {did} {handle}")
            return None

        logger.info(f'Loading repo {repo}')
        self.head = CID.decode(repo.head)
        handle = repo.handles[0] if repo.handles else None
        return Repo.load(self, cid=self.head, handle=handle)

    def read(self, cid):
        block = AtpBlock.get_by_id(cid.encode('base32'))
        if block:
            return dag_cbor.decode(block.dag_cbor)

    def read_many(self, cids):
        found, missing = self._read_blocks(cids)
        found_objs = {cid: dag_cbor.decode(block.dag_cbor)
                      for cid, block in found.items()}
        return found_objs, missing

    def read_blocks(self, cids):
        found, missing = self._read_blocks(cids)
        blocks = BlockMap((cid, block.dag_cbor) for cid, block in found.items())
        return blocks, missing

    def _read_blocks(self, cids):
        """Internal helper, loads AtpBlocks for a set of cids.

        Args:
          cids: sequence of :class:`CID`

        Returns:
          tuple, (dict mapping found CIDs to AtpBlocks, list of CIDs not found)
        """
        keys = [ndb.Key(AtpBlock, cid.encode('base32')) for cid in cids]
        got = list(zip(cids, ndb.get_multi(keys)))
        found = {CID.decode(block.key.id()): block
                 for _, block in got if block is not None}
        missing = [cid for cid, block in got if block is None]
        return found, missing

    def has(self, cid):
        return self.read(cid) is not None

    def write(self, block):
        seq = AtpSequence.get_next(SUBSCRIBE_REPOS_NSID)
        return CID.decode(AtpBlock.create(block, seq=seq).key.id())

    @ndb.transactional()
    def apply_commit(self, commit_data):
        assert commit_data.seq
        ndb.put_multi(AtpBlock(id=cid.encode('base32'), dag_cbor=block,
                               seq=commit_data.seq)
                      for cid, block in commit_data.blocks.items())
        self.head = commit_data.cid

        commit = dag_cbor.decode(commit_data.blocks[commit_data.cid])
        head_encoded = self.head.encode('base32')
        repo = AtpRepo.get_or_insert(commit['did'], head=head_encoded)
        if repo.head == head_encoded:
            logger.info(f'Created new repo {repo}')
        else:
            # already existed in datastore
            repo.head = head_encoded
            logger.info(f'Updated repo {repo}')
            repo.put()

    def next_seq(self, nsid):
        assert nsid
        return AtpSequence.get_next(nsid)
