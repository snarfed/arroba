"""Google Cloud Datastore implementation of repo storage."""
from datetime import timedelta, timezone
from functools import wraps
from io import BytesIO
import json
import logging
import mimetypes
import os
import requests
import threading

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
import dag_cbor
import dag_json
from google.cloud import ndb
from google.cloud.ndb import context
from google.cloud.ndb.exceptions import ContextError
from google.cloud.ndb.key import _MAX_KEYPART_BYTES
from lexrpc import ValidationError
from multiformats import CID, multicodec, multihash
from pymediainfo import MediaInfo

from .mst import MST
from .repo import Repo
from .server import server
from . import storage
from .storage import Action, Block, Storage, SUBSCRIBE_REPOS_NSID
from . import util
from .util import (
    dag_cbor_cid,
    tid_to_int,
    DEACTIVATED,
    DELETED,
    TOMBSTONED,
    InactiveRepo,
)

logger = logging.getLogger(__name__)

BLOB_REFETCH_AGE = timedelta(days=float(os.environ.get('BLOB_REFETCH_DAYS', 7)))
BLOB_REFETCH_TYPES = tuple(os.environ.get('BLOB_REFETCH_TYPES', 'image').split(','))
BLOB_MAX_BYTES = int(os.environ.get('BLOB_MAX_BYTES', 100_000_000))
# https://bsky.app/profile/bsky.app/post/3lk26lxn6sk2u
VIDEO_MAX_DURATION = timedelta(minutes=3)

MEMCACHE_SEQUENCE_ALLOCATION = \
    os.environ.get('BLOB_MAX_BYTES', '').strip().lower() not in ('', '0', 'false')
MEMCACHE_SEQUENCE_BATCH = int(os.environ.get('MEMCACHE_SEQUENCE_BATCH', 1000))
MEMCACHE_SEQUENCE_BUFFER = int(os.environ.get('MEMCACHE_SEQUENCE_BUFFER', 100))
# clients must set this to a pymemcache.Client to use MEMCACHE_SEQUENCE_ALLOCATION
memcache = None
# maps string nsid to integer max sequence number, the lower bound on the
# AtpSequence's current value. this is the highest seq we can allocate from memcache
# without allocating a new batch from the datastore and updating the stored
# AtpSequence's value.
max_seqs = {}
max_seqs_lock = threading.Lock()


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
    cid = ndb.StringProperty()       # unset for deletes
    prev_cid = ndb.StringProperty()  # unset for creates


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
                                cid=CID.decode(op.cid) if op.cid else None,
                                prev_cid=CID.decode(op.prev_cid) if op.prev_cid else None)
               for op in self.ops]
        return Block(cid=self.cid, encoded=self.encoded, seq=self.seq, ops=ops,
                     time=self.created, repo=self.repo)

    @classmethod
    def from_block(cls, block):
        """Converts a :class:`Block` to an :class:`AtpBlock`.

        Args:
          block (Block)

        Returns:
          AtpBlock:
        """
        ops = [CommitOp(action=op.action.name.lower(), path=op.path,
                        cid=op.cid.encode('base32') if op.cid else None,
                        prev_cid=op.prev_cid.encode('base32') if op.prev_cid else None)
               for op in (block.ops or [])]
        created = block.time.astimezone(timezone.utc).replace(tzinfo=None)
        repo_key = ndb.Key(AtpRepo, block.repo) if block.repo else None
        return AtpBlock(id=block.cid.encode('base32'), encoded=block.encoded,
                        repo=repo_key, seq=block.seq, ops=ops, created=created)


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
    # propagation=context.TransactionOptions.INDEPENDENT is important here so that we
    # don't include this in heavy, long-running commit transactions, since it's a
    # single-row bottleneck! (the default is join=True.)
    @ndb.transactional(propagation=context.TransactionOptions.INDEPENDENT, join=None)
    def allocate(cls, nsid):
        """Returns the next sequence number for a given NSID.

        Creates a new :class:`AtpSequence` entity if one doesn't already exist
        for the given NSID.

        Args:
          nsid (str): the subscription XRPC method for this sequence number

        Returns:
          integer, next sequence number for this NSID
        """
        logger.info(f'allocating seq via datastore for {nsid}')
        seq = cls.get_or_insert(nsid, next=1)
        ret = seq.next
        seq.next += 1
        seq.put()
        logger.info(f'  allocated seq {ret}')
        return ret

    @classmethod
    def last(cls, nsid):
        """Returns the last allocated sequence number for a given NSID.

        Args:
          nsid (str): the subscription XRPC method for this sequence number

        Returns:
          integer, last sequence number for this NSID, or None if we don't know it
        """
        if seq := cls.get_by_id(nsid):
            return seq.next - 1


class AtpRemoteBlob(ndb.Model):
    """A blob available at a public HTTP URL that we don't store ourselves.

    Key ID is the URL, truncated if necessary.

    * https://atproto.com/specs/data-model#blob-type
    * https://atproto.com/specs/xrpc#blob-upload-and-download

    TODO:
    * follow redirects, use final URL as key id
    * abstract this in :class:`Storage`
    """
    url = ndb.TextProperty()
    'full length URL'
    cid = ndb.StringProperty()
    size = ndb.IntegerProperty()
    mime_type = ndb.StringProperty(required=True, default='application/octet-stream')
    repos = ndb.KeyProperty(repeated=True)

    # only populated if mime_type is image/* or video/*
    # used in images.aspectRatio in app.bsky.embed.images
    # and aspectRatio in app.bsky.embed.video
    width = ndb.IntegerProperty()
    height = ndb.IntegerProperty()

    # only populated if mime_type is video/*
    # used to enforce maximum duration
    duration = ndb.IntegerProperty()
    'in ms'

    last_fetched = ndb.DateTimeProperty(tzinfo=timezone.utc)
    status = ndb.StringProperty(choices=('inactive',))
    'None means active'

    created = ndb.DateTimeProperty(auto_now_add=True)
    updated = ndb.DateTimeProperty(auto_now=True)


    @classmethod
    def get_or_create(cls, *, url=None, repo=None, get_fn=requests.get,
                      max_size=None, accept_types=None, name=''):
        """Returns a new or existing :class:`AtpRemoteBlob` for a given URL.

        If there isn't an existing :class:`AtpRemoteBlob`, or if the existing one
        needs to be reloaded, fetches the URL over the network.

        Args:
          url (str)
          repo (AtpRepo): optional
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
        assert url
        url_key = url
        if len(url_key) > _MAX_KEYPART_BYTES:
            # TODO: handle Unicode chars. naive approach is to UTF-8 encode,
            # truncate, then decode, but that might cut mid character. easier to just
            # hope/assume the URL is already URL-encoded.
            url_key = url[:_MAX_KEYPART_BYTES]
            logger.warning(f'Truncating URL {url} to {_MAX_KEYPART_BYTES} chars: {url_key}')

        # if the blob already exists, just add this repo if necessary and return it
        @ndb.transactional()
        def get_or_insert():
            repos = [repo.key] if repo else []
            blob = cls.get_or_insert(url_key, repos=repos)
            blob.url = url
            if repo and repo.key not in blob.repos:
                blob.repos.append(repo.key)
                blob.put()
            return blob

        blob = get_or_insert()
        if blob.status:
            raise requests.HTTPError(f'Blob {url_key} is {blob.status}')

        blob.maybe_fetch(get_fn=get_fn)
        blob.validate(max_size=max_size, accept_types=accept_types, name=name)
        return blob

    def maybe_fetch(self, get_fn=requests.get):
        """Fetches the blob from its URL and updates its metadata, if necessary.

        Args:
          get_fn (callable, optional): for making HTTP GET requests
        """
        if ((self.cid or self.last_fetched)
            and self.mime_type.split('/')[0] not in BLOB_REFETCH_TYPES):
            # already fetched, and we don't refetch this type
            return
        elif self.last_fetched and self.last_fetched >= util.now() - BLOB_REFETCH_AGE:
            # we've (re)fetched this recently
            return

        url = self.url or self.key.id()
        logger.info(f'(Re)fetching blob URL {url}')
        self.last_fetched = util.now()

        try:
            resp = get_fn(url, stream=True)
            # if this is our first try, give up if it's not serving.
            # otherwise, 4xx is conclusive; others like 5xx aren't
            if resp.status_code // 100 == 4 or (not resp.ok and not self.cid):
                logger.info('Marking blob inactive')
                self.status = 'inactive'
            resp.raise_for_status()
        except OSError as e:
            if not self.cid:
                self.status = 'inactive'
            raise requests.HTTPError(f"Couldn't fetch blob: {e}")
        finally:
            self.put()

        # check type, size
        self.mime_type = (resp.headers.get('Content-Type')
                          or mimetypes.guess_type(url)[0]
                          or 'application/octet-stream')
        length = resp.headers.get('Content-Length')
        logger.info(f'Got {resp.status_code} {self.mime_type} {length} bytes {resp.url}')

        try:
            length = self.size = int(length)
        except (TypeError, ValueError):
            pass  # read body and check length manually below

        if self.size and self.size > BLOB_MAX_BYTES:
            self.put()
            raise ValidationError(f'{url} Content-Length {length} is over BLOB_MAX_BYTES')

        # calculate CID and update blob
        digest = multihash.digest(resp.content, 'sha2-256')
        self.cid = CID('base58btc', 1, 'raw', digest).encode('base32')
        self.size = len(resp.content)
        self.generate_metadata(resp.content)
        self.status = None
        self.put()

    def as_object(self):
        """Returns an ATProto ``blob`` object for this blob.

        https://atproto.com/specs/data-model#blob-type

        Returns:
          dict or None: with ``$type: blob`` and ``ref``, ``mimeType``, and
            ``size`` fields. If :attr:`cid` is unset, returns None
        """
        if self.cid:
            return {
                '$type': 'blob',
                'ref': CID.decode(self.cid),
                'mimeType': self.mime_type,
                'size': self.size,
            }

    def generate_metadata(self, content):
        """Extracts and stores metadata from an image or video.

        Uses ``self.mime_type`` to determine whether/how to parse the content.

        Args:
          content (bytes)
        """
        try:
            media_info = MediaInfo.parse(BytesIO(content))
            tracks = media_info.video_tracks or media_info.image_tracks
            if not tracks:
                return

            track = tracks[0]
            self.width = track.width
            self.height = track.height
            if track.duration:
                self.duration = int(float(track.duration))
        except (OSError, RuntimeError, TypeError, ValueError) as e:
            logger.info(e)

    def validate(self, max_size=None, accept_types=None, name=''):
        """Checks that this blob satisfies size and type constraints.

        Args:
          max_size (int, optional): the ``maxSize`` parameter for this blob
            field in its lexicon, if any
          accept_types (sequence of str, optional): the ``accept`` parameter for
            this blob field in its lexicon, if any. The set of allowed MIME types.
          name (str, optional): blob field name in lexicon
        """
        url = self.url or self.key.id()

        server.validate_mime_type(self.mime_type, accept_types, name=name)

        if self.size:
            if self.size > BLOB_MAX_BYTES:
                raise ValidationError(f'{url} size {self.size} is over BLOB_MAX_BYTES')
            elif max_size and self.size > max_size:
                raise ValidationError(f'{url} size {self.size} is over {name} blob maxSize {max_size}')

        if self.duration and timedelta(milliseconds=self.duration) > VIDEO_MAX_DURATION:
            raise ValidationError(f'{url} duration {self.duration / 1000}s is over limit {VIDEO_MAX_DURATION}')


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
        handle = atp_repo.handles[0] if atp_repo.handles else None

        return Repo.load(self, cid=CID.decode(atp_repo.head), handle=handle,
                         status=atp_repo.status, signing_key=atp_repo.signing_key,
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

        @ndb.transactional(join=True)
        def update():
            atp_repo = AtpRepo.get_by_id(repo.did)
            atp_repo.status = status
            atp_repo.put()

        update()

    def store_repo(self, repo):
        @ndb.transactional(join=True)
        def store():
            atp_repo = AtpRepo.get_by_id(repo.did)
            atp_repo.handles = [repo.handle] if repo.handle else []
            atp_repo.status = repo.status
            atp_repo.head = repo.head.cid.encode('base32')
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
            # lexrpc event subscription handlers like subscribeRepos call this
            # on a different thread, so if we're there, we need to create a new
            # ndb context
            ctx = context.get_context(raise_context_error=False)
            with (ctx.use() if ctx
                  else self.ndb_client.context(**self.ndb_context_kwargs)):
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
                    logger.warning(f'lost ndb context! re-querying at {cur_seq}. {e}')
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
        keys = [AtpBlock(id=b.cid.encode('base32')).key for b in blocks]
        existing = AtpBlock.query(AtpBlock.key.IN(keys)).fetch(keys_only=True)
        existing_cids = [key.id() for key in existing]
        ndb.put_multi(AtpBlock.from_block(b) for b in blocks
                      if b.cid.encode('base32') not in existing_cids)

    @ndb_context
    @ndb.transactional()
    def _commit(self, *args, **kwargs):
        """Just runs :meth:`Storage._commit` in a transaction."""
        return super()._commit(*args, **kwargs)

    @ndb_context
    def allocate_seq(self, nsid):
        assert nsid

        if MEMCACHE_SEQUENCE_ALLOCATION == 'shadow':
            self._allocate_seq_memcache(nsid + '-shadow')
            return AtpSequence.allocate(nsid)
        elif MEMCACHE_SEQUENCE_ALLOCATION is True:
            return self._allocate_seq_memcache(nsid)
        else:
            return AtpSequence.allocate(nsid)

    @ndb_context
    def last_seq(self, nsid):
        assert nsid
        if MEMCACHE_SEQUENCE_ALLOCATION is True:
            return memcache.get(self._memcache_seq_key(nsid))
        else:
            return AtpSequence.last(nsid)

    def _memcache_seq_key(self, nsid):
        """Returns the sequence number memcache key for a given NSID.

        Raises ``AssertionError`` if memcache sequence allocation isn't enabled.

        Args:
          nsid (str)

        Returns:
          str: memcache key
        """
        assert MEMCACHE_SEQUENCE_ALLOCATION
        assert nsid
        return f'{nsid}-last-seq'

    def _allocate_seq_memcache(self, nsid):
        """Allocates a single sequence number from memcache.

        The memcache key is ``[nsid]-last-seq``. Its value is the last sequence
        number we've allocated.

        Backed by :class:`AtpSequence` in the datastore, but only allocates from it
        in batches.

        See :meth:`allocate` for args etc.
        """
        global max_seqs
        assert memcache
        assert MEMCACHE_SEQUENCE_BATCH > MEMCACHE_SEQUENCE_BUFFER > 1, \
            (MEMCACHE_SEQUENCE_BATCH, MEMCACHE_SEQUENCE_BUFFER)

        logger.info(f'allocating seq via memcache for {nsid}')

        key = self._memcache_seq_key(nsid)
        seq = memcache.incr(key, 1)
        if seq is None:  # not in memcache
            with max_seqs_lock:
                # can't use last() because it looks in memcache
                max_seqs[nsid] = AtpSequence.get_or_insert(nsid, next=1).next - 1
                # we'll allocate a new batch below
                if memcache.add(key, max_seqs[nsid]):
                    logger.info(f'  initialized memcache sequence counter {key} to {max_seqs[nsid]}')
            seq = memcache.incr(key, 1)

        @ndb.transactional(propagation=context.TransactionOptions.INDEPENDENT,
                           join=None)
        def alloc_batch():
            stored_seq = AtpSequence.get_or_insert(nsid, next=1)
            if stored_seq.next - seq < MEMCACHE_SEQUENCE_BUFFER:
                stored_seq.next = seq + MEMCACHE_SEQUENCE_BATCH
                logger.info(f'  allocating {MEMCACHE_SEQUENCE_BATCH} seqs batch for {nsid}, up to {stored_seq.next}')
                stored_seq.put()
            max_seqs[nsid] = stored_seq.next

        with max_seqs_lock:
            if max_seqs.get(nsid, 0) - seq < MEMCACHE_SEQUENCE_BUFFER:
                alloc_batch()

        assert seq and seq <= max_seqs[nsid], (seq, max_seqs[nsid])
        logger.info(f'  allocated seq {seq}')
        return seq
