"""Google Cloud Datastore implementation of repo storage."""
import json
import logging

import dag_cbor
import dag_json
from google.cloud import ndb
from multiformats import CID, multicodec, multihash

from .repo import Repo
from . import storage
from .storage import Action, Block, Storage, SUBSCRIBE_REPOS_NSID
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


class CommitOp(ndb.Model):
    """Repo operations - creates, updates, deletes - included in a commit.

    Used in a StructuredProperty inside AtpBlock; not stored directly in the
    datastore.

    https://googleapis.dev/python/python-ndb/latest/model.html#google.cloud.ndb.model.StructuredProperty
    """
    action = ndb.StringProperty(required=True, choices=['create', 'update', 'delete'])
    path = ndb.StringProperty(required=True)
    cid = ndb.StringProperty()  # unset for deletes


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

    created = ndb.DateTimeProperty(auto_now_add=True)
    updated = ndb.DateTimeProperty(auto_now=True)


class AtpBlock(ndb.Model):
    """A data record, MST node, or commit.

    Key name is the DAG-CBOR base32 CID of the data.

    Properties:
    * encoded: bytes, DAG-CBOR encoded value
    * data: dict, DAG-JSON value, only used for human debugging
    * seq: int, sequence number for the subscribeRepos event stream
    """
    repo = ndb.KeyProperty(AtpRepo, required=True)
    encoded = WriteOnceBlobProperty(required=True)
    seq = ndb.IntegerProperty(required=True)
    ops = ndb.StructuredProperty(CommitOp, repeated=True)

    created = ndb.DateTimeProperty(auto_now_add=True)

    @ComputedJsonProperty
    def decoded(self):
        return json.loads(dag_json.encode(dag_cbor.decode(self.encoded)))

    @property
    def cid(self):
        return CID.decode(self.key.id())

    @staticmethod
    def create(*, repo_did, data, seq):
        """Writes a new AtpBlock to the datastore.

        If the block already exists in the datastore, leave it untouched.
        Notably, leave its sequence number as is, since it will be lower than
        this current sequence number.

        Args:
          repo_did: str
          data: dict value
          seq: integer

        Returns:
          :class:`AtpBlock`
        """
        assert seq > 0
        encoded = dag_cbor.encode(data)
        digest = multihash.digest(encoded, 'sha2-256')
        cid = CID('base58btc', 1, multicodec.get('dag-cbor'), digest)

        repo_key = ndb.Key(AtpRepo, repo_did)
        atp_block = AtpBlock.get_or_insert(cid.encode('base32'), repo=repo_key,
                                           encoded=encoded, seq=seq)
        assert atp_block.seq <= seq
        return atp_block

    def to_block(self):
        """Converts to :class:`Block`.

        Returns:
          :class:`Block`
        """
        ops = [storage.CommitOp(action=Action[op.action.upper()], path=op.path,
                                cid=CID.decode(op.cid) if op.cid else None)
               for op in self.ops]
        return Block(cid=self.cid, encoded=self.encoded, seq=self.seq, ops=ops)

    @classmethod
    def from_block(cls, *, repo_did, block):
        """Converts a :class:`Block` to an :class:`AtpBlock`.

        Args:
          repo_did: str
          block: :class:`Block`

        Returns:
          :class:`AtpBlock`
        """
        ops = [CommitOp(action=op.action.name.lower(), path=op.path,
                        cid=op.cid.encode('base32') if op.cid else None)
               for op in (block.ops or [])]
        return AtpBlock(id=block.cid.encode('base32'), encoded=block.encoded,
                        repo=ndb.Key(AtpRepo, repo_did), seq=block.seq, ops=ops)


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

    created = ndb.DateTimeProperty(auto_now_add=True)
    updated = ndb.DateTimeProperty(auto_now=True)

    @classmethod
    @ndb.transactional()
    def allocate(cls, nsid):
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
        assert repo.head

        handles = [repo.handle] if repo.handle else []
        atp_repo = AtpRepo(id=repo.did, handles=handles,
                           head=repo.head.cid.encode('base32'))
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
            return block.to_block()

    def read_many(self, cids):
        keys = [ndb.Key(AtpBlock, cid.encode('base32')) for cid in cids]
        got = list(zip(cids, ndb.get_multi(keys)))
        return {cid: block.to_block() if block else None
                for cid, block in got}

    def read_from_seq(self, seq):
        assert seq >= 0
        for atp_block in AtpBlock.query(AtpBlock.seq >= seq)\
                                 .order(AtpBlock.seq):
            yield atp_block.to_block()

    def has(self, cid):
        return self.read(cid) is not None

    def write(self, repo_did, obj):
        seq = AtpSequence.allocate(SUBSCRIBE_REPOS_NSID)
        return AtpBlock.create(repo_did=repo_did, data=obj, seq=seq).cid

    @ndb.transactional()
    def apply_commit(self, commit_data):
        seq = AtpSequence.allocate(SUBSCRIBE_REPOS_NSID)

        for block in commit_data.blocks.values():
            template = AtpBlock.from_block(
                repo_did=commit_data.commit.decoded['did'], block=block)
            atp_block = AtpBlock.get_or_insert(
                template.key.id(), repo=template.repo, encoded=block.encoded,
                seq=seq, ops=template.ops)
            block.seq = seq

        self.head = commit_data.commit.cid

        commit = commit_data.commit.decoded
        head_encoded = self.head.encode('base32')
        repo = AtpRepo.get_or_insert(commit['did'], head=head_encoded)
        if repo.head == head_encoded:
            logger.info(f'Created new repo {repo}')
        else:
            # already existed in datastore
            repo.head = head_encoded
            logger.info(f'Updated repo {repo}')
            repo.put()

    def allocate_seq(self, nsid):
        assert nsid
        return AtpSequence.allocate(nsid)

    def last_seq(self, nsid):
        assert nsid
        return AtpSequence.get_by_id(nsid).next - 1
