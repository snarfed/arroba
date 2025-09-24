"""Bluesky / AT Protocol repo implementation.

https://atproto.com/guides/data-repos

Heavily based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/src/repo.ts

Huge thanks to the Bluesky team for working in the public, in open source, and to
Daniel Holmgren and Devin Ivy for this code specifically!
"""
from collections import defaultdict, namedtuple
import copy
import logging

from carbox import car
from cryptography.hazmat.primitives.asymmetric import ec
import dag_cbor
from multiformats import CID

from . import util
from . import mst
from . import storage as storage_mod

logger = logging.getLogger(__name__)


Write = namedtuple('Write', [
    'action',      # :class:`Action`
    'collection',  # str
    'rkey',        # str
    'record',      # dict
], defaults=[None] * 4)


class Repo:
    """AT Protocol data repo implementation, storage agnostic.

    Attributes:
      did (str): repo DID (dynamic property)
      version (int): AT Protocol version (dynamic property)
      storage (Storage)
      mst (MST)
      head (Block): head commit
      handle (str)
      status (str): None (if active) or ``'deactivated'``, ``'deleted'``,
        or ``'tombstoned'`` (deprecated)
      callback (callable: (CommitData | dict) => None): called on new commits
        and other repo events. May be set directly by clients. None means no
        callback. The parameter will be a :class:`CommitData` for commits, dict
        record with ``$type`` for other ``com.atproto.sync.subscribeRepos``
        messages.
    """
    storage = None
    mst = None
    head = None
    handle = None
    callback = None
    signing_key = None
    rotation_key = None
    status = None

    def __init__(self, *, storage=None, mst=None, head=None, handle=None,
                 status=None, callback=None, signing_key=None, rotation_key=None):
        """Constructor.

        Args:
          storage (Storage): required
          mst (MST)
          head (Block): head commit
          handle (str)
          status (str): None (if active) or ``'deactivated'``, ``'deleted'``,
            or ``'tombstoned'`` (deprecated)
          callback (callable, (CommitData | dict) => None)
          signing_key (ec.EllipticCurvePrivateKey): required
          rotation_key (ec.EllipticCurvePrivateKey)
        """
        assert storage
        assert signing_key

        self.storage = storage
        self.mst = mst
        self.head = head
        self.handle = handle
        self.status = status
        self.callback = callback
        self.signing_key = signing_key
        self.rotation_key = rotation_key

    def __eq__(self, other):
        return (self.head and other.head
                and self.version == other.version
                and self.did == other.did
                and self.head == other.head)

    @property
    def did(self):
        if self.head:
            return self.head.decoded['did']

    @property
    def version(self):
        if self.head:
            return self.head.decoded['version']

    def get_record(self, collection, rkey):
        """

        Args:
          collection (str)
          rkey (str)

        Returns:
          dict: node, record or commit or serialized :class:`MST`
        """
        cid = self.mst.get(f'{collection}/{rkey}')
        if cid:
            return self.storage.read(cid).decoded

    def get_contents(self):
        """

        Returns:
          dict mapping str collection to dict mapping str rkey to dict record:
        """
        entries = self.mst.list()
        blocks = self.storage.read_many([e.value for e in entries])
        contents = defaultdict(dict)
        for entry in entries:
            collection, rkey = entry.key.split('/', 2)
            contents[collection][rkey] = blocks[entry.value].decoded

        return contents

    @classmethod
    def create(cls, storage, did, *, signing_key, rotation_key=None, **kwargs):
        """

        Args:
          did (str)
          storage (Storage)
          signing_key (ec.EllipticCurvePrivateKey):
          rotation_key (ec.EllipticCurvePrivateKey):
          kwargs: passed through to :class:`Repo` constructor

        Returns:
          Repo:
        """
        repo = Repo(storage=storage, mst=mst.MST.create(storage=storage),
                    signing_key=signing_key, rotation_key=rotation_key, **kwargs)
        initial_commit = storage.commit(repo, [], repo_did=did)
        assert repo.head
        assert repo.did

        storage.write_event(repo=repo, type='identity', handle=kwargs.get('handle'))
        storage.write_event(repo=repo, type='account', active=True)

        # TODO: #sync event should be after #account/#identity but before first #commit
        # https://github.com/bluesky-social/proposals/tree/main/0006-sync-iteration#staying-synchronized-sync-event-auto-repair-and-account-status
        # https://github.com/snarfed/arroba/issues/52#issuecomment-2816324912
        commit = initial_commit.commit
        sync_blocks = [car.Block(cid=commit.cid, data=commit.encoded,
                                 decoded=commit.decoded)]
        blocks_bytes = car.write_car([commit.cid], sync_blocks)
        storage.write_event(repo=repo, type='sync', blocks=blocks_bytes,
                            rev=commit.decoded['rev'])

        storage.create_repo(repo)
        if repo.callback:
            repo.callback(initial_commit)

        return repo

    @classmethod
    def load(cls, storage, cid=None, **kwargs):
        """
        Args:
          storage (Storage)
          cid (CID): optional
          kwargs: passed through to :class:`Repo` constructor

        Returns:
          Repo:
        """
        commit_cid = cid or storage.head
        assert commit_cid, 'No cid provided and none in storage'

        commit_block = storage.read(commit_cid)
        tree = mst.MST.load(storage=storage, cid=commit_block.decoded['data'])
        logger.info(f'loaded repo for {commit_block.decoded["did"]} at commit {commit_cid}')
        return Repo(storage=storage, mst=tree, head=commit_block, **kwargs)
