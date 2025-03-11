"""Google Cloud Datastore implementation of repo storage."""
from datetime import timezone
from functools import wraps
from io import BytesIO
import json
import logging
import mimetypes
import requests

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
import dag_cbor
import dag_json
from google.cloud import ndb
from google.cloud.ndb import context
from google.cloud.ndb.exceptions import ContextError
from lexrpc import ValidationError
from multiformats import CID, multicodec, multihash
from PIL import Image, ImageFile
from pymediainfo import MediaInfo

from .mst import MST
from .repo import Repo
from .server import server
from . import storage
from .storage import Action, Block, Storage, SUBSCRIBE_REPOS_NSID
from .util import (
    dag_cbor_cid,
    tid_to_int,
    DEACTIVATED,
    DELETED,
    TOMBSTONED,
    InactiveRepo,
)

logger = logging.getLogger(__name__)

# Allow bad .ico files with truncated transparency masks
# https://github.com/python-pillow/Pillow/issues/6507#issuecomment-2199724849
ImageFile.LOAD_TRUNCATED_IMAGES = True


class WriteOnce:
    """:class:`ndb.Property` mix-in, prevents changing it once it's set."""
    def _set_value(self, entity, value):
        existing = self._get_value(entity)
        if existing is not None and value != existing:
            raise ndb.ReadonlyPropertyError(f"{self._name} can't be changed")

        return super()._set_value(entity, value)


class JsonProperty(ndb.TextProperty):
    """Fork of ndb's that subclasses :class:`ndb.TextProperty` instead of :class:`ndb.BlobProperty`.

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
    """Custom :class:`ndb.ComputedProperty` for JSON values that stores them as
    strings.

    ...instead of like :class:`ndb.StructuredProperty`, with "entity" type, which
    bloats them unnecessarily in the datastore.
    """
    def __init__(self, *args, **kwargs):
        kwargs['indexed'] = False
        super().__init__(*args, **kwargs)


class WriteOnceBlobProperty(WriteOnce, ndb.BlobProperty):
    pass


class CommitOp(ndb.Model):
    """Repo operations - creates, updates, deletes - included in a commit.

    Used in a :class:`StructuredProperty` inside :class:`AtpBlock`; not stored
    directly in the datastore.

    https://googleapis.dev/python/python-ndb/latest/model.html#google.cloud.ndb.model.StructuredProperty
    """
    action = ndb.StringProperty(required=True, choices=('create', 'update', 'delete'))
    path = ndb.StringProperty(required=True)
    cid = ndb.StringProperty()  # unset for deletes


class AtpRepo(ndb.Model):
    r"""An ATProto repo.

    Key name is DID. Only stores the repo's metadata. Blocks are stored in
    :class:`AtpBlock`\s.

    Attributes:
    * handles (str): repeated, optional
    * head (str): CID
    * signing_key (str)
    * rotation_key (str)
    * status (str)
    """
    handles = ndb.StringProperty(repeated=True)
    head = ndb.StringProperty(required=True)
    # TODO: add password hash?

    # these are both secp256k1 private keys, PEM-encoded bytes
    # https://atproto.com/specs/cryptography
    signing_key_pem = ndb.BlobProperty(required=True)
    # TODO: rename this recovery_key_pem?
    # https://discord.com/channels/1097580399187738645/1098725036917002302/1153447354003894372
    rotation_key_pem = ndb.BlobProperty()
    status = ndb.StringProperty(choices=(DEACTIVATED, DELETED, TOMBSTONED))

    created = ndb.DateTimeProperty(auto_now_add=True)
    updated = ndb.DateTimeProperty(auto_now=True)

    @property
    def signing_key(self):
        """(ec.EllipticCurvePrivateKey)"""
        return serialization.load_pem_private_key(self.signing_key_pem,
                                                  password=None)

    @property
    def rotation_key(self):
        """(ec.EllipticCurvePrivateKey` or None)"""
        if self.rotation_key_pem:
            return serialization.load_pem_private_key(self.rotation_key_pem,
                                                      password=None)


class AtpBlock(ndb.Model):
    """A data record, MST node, repo commit, or other event.

    Key name is the DAG-CBOR base32 CID of the data.

    Events should have a fully-qualified ``$type`` field that's one of the
    ``message`` types in ``com.atproto.sync.subscribeRepos``, eg
    ``com.atproto.sync.subscribeRepos#tombstone``.

    Properties:
    * repo (google.cloud.ndb.Key): DID of the first repo that included this block
    * encoded (bytes): DAG-CBOR encoded value
    * data (dict): DAG-JSON value, only used for human debugging
    * seq (int): sequence number for the subscribeRepos event stream
    """
    repo = ndb.KeyProperty(AtpRepo, required=True)
    encoded = WriteOnceBlobProperty(required=True)
    seq = ndb.IntegerProperty(required=True)
    ops = ndb.StructuredProperty(CommitOp, repeated=True)

    created = ndb.DateTimeProperty(auto_now_add=True)

    @property
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
          repo_did (str):
          data (dict): value
          seq (int):

        Returns:
          :class:`AtpBlock`
        """
        assert seq > 0
        encoded = dag_cbor.encode(data)
        digest = multihash.digest(encoded, 'sha2-256')
        cid = CID('base58btc', 1, 'dag-cbor', digest)

        repo_key = ndb.Key(AtpRepo, repo_did)
        atp_block = AtpBlock.get_or_insert(cid.encode('base32'), repo=repo_key,
                                           encoded=encoded, seq=seq)
        assert atp_block.seq <= seq
        return atp_block

    def to_block(self):
        """Converts to :class:`Block`.

        Returns:
          Block
        """
        ops = [storage.CommitOp(action=Action[op.action.upper()], path=op.path,
                                cid=CID.decode(op.cid) if op.cid else None)
               for op in self.ops]
        return Block(cid=self.cid, encoded=self.encoded, seq=self.seq, ops=ops,
                     time=self.created, repo=self.repo)

    @classmethod
    def from_block(cls, *, repo_did, block):
        """Converts a :class:`Block` to an :class:`AtpBlock`.

        Args:
          repo_did (str)
          block (Block)

        Returns:
          AtpBlock
        """
        ops = [CommitOp(action=op.action.name.lower(), path=op.path,
                        cid=op.cid.encode('base32') if op.cid else None)
               for op in (block.ops or [])]
        created = block.time.astimezone(timezone.utc).replace(tzinfo=None)
        return AtpBlock(id=block.cid.encode('base32'), encoded=block.encoded,
                        repo=ndb.Key(AtpRepo, repo_did), seq=block.seq, ops=ops,
                        created=created)


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
    @ndb.transactional(retries=10)
    def allocate(cls, nsid):
        """Returns the next sequence number for a given NSID.

        Creates a new :class:`AtpSequence` entity if one doesn't already exist
        for the given NSID.

        Args:
          nsid (str): the subscription XRPC method for this sequence number

        Returns:
          integer, next sequence number for this NSID
        """
        seq = AtpSequence.get_or_insert(nsid, next=1)
        ret = seq.next
        seq.next += 1
        seq.put()
        return ret

    @classmethod
    def last(cls, nsid):
        """Returns the last sequence number for a given NSID.

        Creates a new :class:`AtpSequence` entity if one doesn't already exist
        for the given NSID.

        Args:
          nsid (str): the subscription XRPC method for this sequence number

        Returns:
          integer, last sequence number for this NSID
        """
        seq = AtpSequence.get_or_insert(nsid, next=1)
        return seq.next - 1


class AtpRemoteBlob(ndb.Model):
    """A blob available at a public HTTP URL that we don't store ourselves.

    Key ID is the URL.

    * https://atproto.com/specs/data-model#blob-type
    * https://atproto.com/specs/xrpc#blob-upload-and-download

    TODO:
    * follow redirects, use final URL as key id
    * abstract this in :class:`Storage`
    """
    cid = ndb.StringProperty(required=True)
    size = ndb.IntegerProperty(required=True)
    mime_type = ndb.StringProperty(required=True, default='application/octet-stream')

    # only populated if mime_type is image/* or video/*
    # used in images.aspectRatio in app.bsky.embed.images
    # and aspectRatio in app.bsky.embed.video
    width = ndb.IntegerProperty()
    height = ndb.IntegerProperty()

    # only populated if mime_type is video/*
    # used to enforce maximum duration
    duration = ndb.IntegerProperty()

    created = ndb.DateTimeProperty(auto_now_add=True)
    updated = ndb.DateTimeProperty(auto_now=True)

    @classmethod
    def get_or_create(cls, *, url=None, get_fn=requests.get, max_size=None,
                      accept_types=None, name=''):
        """Returns a new or existing :class:`AtpRemoteBlob` for a given URL.

        If there isn't an existing :class:`AtpRemoteBlob`, fetches the URL over
        the network and creates a new one for it.

        Args:
          url (str)
          get_fn (callable): for making HTTP GET requests
          max_size (int, optional): the ``maxSize`` parameter for this blob
            field in its lexicon, if any
          accept_types (sequence of str, optional): the ``accept`` parameter for
            this blob field in its lexicon, if any. The set of allowed MIME types.
          name (str, optional): blob field name in lexicon

        Returns:
          AtpRemoteBlob: existing or newly created blob

        Raises:
          requests.RequestException: if the HTTP request to fetch the blob failed
          lexrpc.ValidationError: if the blob is over ``max_size``, its type is
            not in ``accept_types`` or it is a video with a duration above the 3m
            limit
        """
        def validate_size(size):
            if max_size and size > max_size:
                raise ValidationError(f'{url} Content-Length {size} is over {name} blob maxSize {max_size}')

        def validate_duration(duration):
            # enforce 3m maximum video duration
            # https://bsky.app/profile/bsky.app/post/3lk26lxn6sk2u
            max_duration = 3 * 60_000 # milliseconds
            if duration and duration > max_duration:
                raise ValidationError(f'{url} duration {duration / 1000} is over {max_duration / 1000}s')

        assert url
        blob = cls.get_by_id(url)
        if blob:
            validate_size(blob.size)
            validate_duration(blob.duration)
            server.validate_mime_type(blob.mime_type, accept_types, name=url)
            return blob

        resp = get_fn(url, stream=True)
        resp.raise_for_status()

        mime_type = resp.headers.get('Content-Type')
        if not mime_type:
            mime_type, _ = mimetypes.guess_type(url)
        length = resp.headers.get('Content-Length')
        logger.info(f'Got {resp.status_code} {mime_type} {length} bytes {resp.url}')

        # check type
        server.validate_mime_type(mime_type, accept_types, name=url)

        # check size
        try:
            length = int(length)
        except (TypeError, ValueError):
            length = None  # read body and check length manually below
        if length:
            validate_size(length)

        # now ready to fetch body
        digest = multihash.digest(resp.content, 'sha2-256')
        cid = CID('base58btc', 1, 'raw', digest).encode('base32')

        # note that if the initial URL redirects, we still store it in the
        # AtpRemoteBlob, not the final resolved URL after redirects.
        logger.info(f'Creating new AtpRemoteBlob for {url} CID {cid}')
        blob = cls(id=url, cid=cid, size=len(resp.content))
        if mime_type:
            blob.mime_type = mime_type

        if mime_type and mime_type.startswith('image/'):
            try:
                with Image.open(BytesIO(resp.content)) as image:
                    blob.width, blob.height = image.size
            except (OSError, RuntimeError, Image.DecompressionBombError) as e:
                logger.info(e)
        elif mime_type and mime_type.startswith('video/'):
            try:
                media_info = MediaInfo.parse(BytesIO(resp.content))
                if len(media_info.video_tracks) == 1:
                    track = media_info.video_tracks[0]
                    blob.width = track.width
                    blob.height = track.height
                    blob.duration = track.duration
            except (OSError, RuntimeError) as e:
                logger.info(e)

        blob.put()

        # re-validate size in case the server didn't give us Content-Length.
        # do this after storing blob so that we don't re-download it next time.
        validate_size(len(resp.content))

        validate_duration(blob.duration)

        return blob

    def as_object(self):
        """Returns an ATProto ``blob`` object for this blob.

        https://atproto.com/specs/data-model#blob-type

        Returns:
          dict: with ``$type: blob`` and ``ref``, ``mimeType``, and ``size`` fields
        """
        return {
            '$type': 'blob',
            'ref': CID.decode(self.cid),
            'mimeType': self.mime_type,
            'size': self.size,
        }


class DatastoreStorage(Storage):
    """Google Cloud Datastore implementation of :class:`Storage`.

    Sequence numbers in :class:`AtpBlock` are allocated per commit; all blocks
    in a given commit will have the same sequence number. They're currently
    sequential counters, starting at 1, stored in an :class:`AtpSequence` entity.

    See :class:`Storage` for method details.
    """
    ndb_client = None
    ndb_context_kwargs = None

    def __init__(self, *, ndb_client=None, ndb_context_kwargs=None):
        """Constructor.

        Args:
          ndb_client (google.cloud.ndb.Client): used when there isn't already
            an ndb context active
          ndb_context_kwargs (dict): optional, used when creating a new ndb context
        """
        super().__init__()
        self.ndb_client = ndb_client
        self.ndb_context_kwargs = ndb_context_kwargs or {}

    def ndb_context(fn):
        @wraps(fn)
        def decorated(self, *args, **kwargs):
            ctx = context.get_context(raise_context_error=False)

            with ctx.use() if ctx else self.ndb_client.context(**self.ndb_context_kwargs):
                ret = fn(self, *args, **kwargs)

            return ret

        return decorated

    @ndb_context
    def create_repo(self, repo):
        assert repo.did
        assert repo.head

        handles = [repo.handle] if repo.handle else []

        signing_key_pem = repo.signing_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
        rotation_key_pem = None
        if repo.rotation_key:
            rotation_key_pem = repo.rotation_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )

        atp_repo = AtpRepo(id=repo.did, handles=handles,
                           head=repo.head.cid.encode('base32'),
                           signing_key_pem=signing_key_pem,
                           rotation_key_pem=rotation_key_pem,
                           status=repo.status)
        atp_repo.put()
        logger.info(f'Stored repo {atp_repo}')

    @ndb_context
    def load_repo(self, did_or_handle):
        assert did_or_handle
        atp_repo = (AtpRepo.get_by_id(did_or_handle)
                    or AtpRepo.query(AtpRepo.handles == did_or_handle).get())

        if not atp_repo:
            logger.info(f"Couldn't find repo for {did_or_handle}")
            return None

        logger.info(f'Loading repo {atp_repo.key}')
        self.head = CID.decode(atp_repo.head)
        handle = atp_repo.handles[0] if atp_repo.handles else None

        return Repo.load(self, cid=self.head, handle=handle, status=atp_repo.status,
                         signing_key=atp_repo.signing_key,
                         rotation_key=atp_repo.rotation_key)

    @ndb_context
    def load_repos(self, after=None, limit=500):
        query = AtpRepo.query()
        if after:
            query = query.filter(AtpRepo.key > AtpRepo(id=after).key)

        # duplicates parts of Repo.load but batches reading blocks from storage
        atp_repos = query.fetch(limit=limit)

        cids = [CID.decode(r.head) for r in atp_repos]
        blocks = self.read_many(cids)  # dict mapping CID to block
        heads = [blocks[cid] for cid in cids]

        # MST.load doesn't read from storage
        msts = [MST.load(storage=self, cid=block.decoded['data']) for block in heads]
        return [Repo(storage=self, mst=mst, head=head, status=atp_repo.status,
                     handle=atp_repo.handles[0] if atp_repo.handles else None,
                     signing_key=atp_repo.signing_key,
                     rotation_key=atp_repo.rotation_key)
                for atp_repo, head, mst in zip(atp_repos, heads, msts)]

    @ndb_context
    def _set_repo_status(self, repo, status):
        assert status in (DEACTIVATED, DELETED, TOMBSTONED, None)
        repo.status = status  # in memory only

        @ndb.transactional()
        def update():
            atp_repo = AtpRepo.get_by_id(repo.did)
            atp_repo.status = status
            atp_repo.put()

        update()

    def store_repo(self, repo):
        @ndb.transactional()
        def store():
            atp_repo = AtpRepo.get_by_id(repo.did)
            atp_repo.populate(
                handles=[repo.handle] if repo.handle else [],
                status=repo.status,
            )
            atp_repo.put()
            logger.info(f'Stored repo {atp_repo}')

        store()

    @ndb_context
    def read(self, cid):
        block = AtpBlock.get_by_id(cid.encode('base32'))
        if block:
            return block.to_block()

    @ndb_context
    def read_many(self, cids):
        keys = [ndb.Key(AtpBlock, cid.encode('base32')) for cid in cids]
        got = list(zip(cids, ndb.get_multi(keys)))
        return {cid: block.to_block() if block else None
                for cid, block in got}

    # can't use @ndb_context because this is a generator, not a normal function
    def read_blocks_by_seq(self, start=0, repo=None):
        assert start >= 0

        cur_seq = start
        cur_seq_cids = []

        while True:
            ctx = context.get_context(raise_context_error=False)
            with ctx.use() if ctx else self.ndb_client.context(**self.ndb_context_kwargs):
                # lexrpc event subscription handlers like subscribeRepos call this
                # on a different thread, so if we're there, we need to create a new
                # ndb context
                try:
                    query = AtpBlock.query(AtpBlock.seq >= cur_seq).order(AtpBlock.seq)
                    if repo:
                        query = query.filter(AtpBlock.repo == AtpRepo(id=repo).key)
                    # unproven hypothesis: need strong consistency to make sure we
                    # get all blocks for a given seq, including commit
                    # https://console.cloud.google.com/errors/detail/CO2g4eLG_tOkZg;service=atproto-hub;time=P1D;refresh=true;locations=global?project=bridgy-federated
                    for atp_block in query.iter(read_consistency=ndb.STRONG):
                        if atp_block.seq != cur_seq:
                            cur_seq = atp_block.seq
                            cur_seq_cids = []
                        if atp_block.key.id() not in cur_seq_cids:
                            cur_seq_cids.append(atp_block.key.id())
                            yield atp_block.to_block()

                    # finished cleanly
                    break

                except ContextError as e:
                    logging.warning(f'lost ndb context! re-querying at {cur_seq}. {e}')
                    # continue loop, restart query

            # Context.use() resets this to the previous context when it exits,
            # but that context is bad now, so make sure we get a new one at the
            # top of the loop
            context._state.context = None

    @ndb_context
    def has(self, cid):
        return self.read(cid) is not None

    @ndb_context
    def write(self, repo_did, obj, seq=None):
        if seq is None:
            seq = self.allocate_seq(SUBSCRIBE_REPOS_NSID)
        return AtpBlock.create(repo_did=repo_did, data=obj, seq=seq).to_block()

    @ndb_context
    def write_blocks(self, blocks):
        ndb.put_multi(AtpBlock.from_block(repo_did=b.repo, block=b) for b in blocks)

    @ndb_context
    # retry aggressively because repo writes can be bursty and cause high
    # contention. (ndb does exponential backoff.)
    # https://console.cloud.google.com/errors/detail/CKbL5KSX98uZHw;time=P1D;locations=global?project=bridgy-federated
    @ndb.transactional(retries=10)
    def apply_commit(self, commit_data):
        commit = commit_data.commit.decoded
        if repo := AtpRepo.get_by_id(commit['did']):
            if repo.status:
                raise InactiveRepo(repo.key.id(), repo.status)

        seq = tid_to_int(commit_data.commit.decoded['rev'])
        assert seq

        for block in commit_data.blocks.values():
            template = AtpBlock.from_block(
                repo_did=commit_data.commit.decoded['did'], block=block)
            # get_or_insert so we don't wipe out any existing blocks' sequence
            # numbers. (occasionally we see existing blocks recur, eg MST nodes.)
            atp_block = AtpBlock.get_or_insert(
                template.key.id(), repo=template.repo, encoded=block.encoded,
                seq=seq, ops=template.ops)
            block.seq = seq

        self.head = commit_data.commit.cid
        head_encoded = self.head.encode('base32')

        if repo:
            logger.info(f'Updating {repo.key}')
            repo.head = head_encoded
            repo.put()

    @ndb_context
    def allocate_seq(self, nsid):
        assert nsid
        return AtpSequence.allocate(nsid)

    @ndb_context
    def last_seq(self, nsid):
        assert nsid
        return AtpSequence.last(nsid)
