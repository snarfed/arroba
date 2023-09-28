"""Bluesky / AT Protocol repo implementation.

https://atproto.com/guides/data-repos

Heavily based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/src/repo.ts

Huge thanks to the Bluesky team for working in the public, in open source, and to
Daniel Holmgren and Devin Ivy for this code specifically!
"""
from collections import defaultdict, namedtuple
import logging

from cryptography.hazmat.primitives.asymmetric import ec
import dag_cbor
from multiformats import CID

from . import util
from .diff import Diff
from .mst import MST
from .storage import (
    Action,
    Block,
    CommitData,
    CommitOp,
    Storage,
    SUBSCRIBE_REPOS_NSID,
)

logger = logging.getLogger(__name__)


Write = namedtuple('Write', [
    'action',      # :class:`Action`
    'collection',  # str
    'rkey',        # str
    'record',      # dict
], defaults=[None] * 4)


def writes_to_commit_ops(writes):
    """Converts :class:`Write`s to :class:`CommitOp`s.

    Args:
      write: iterable of :class:`Write`

    Returns:
      list of :class:`CommitOp`
    """
    if not writes:
        return writes

    return [CommitOp(action=write.action,
                     path=f'{write.collection}/{write.rkey}',
                     cid=util.dag_cbor_cid(write.record) if write.record else None)
            for write in writes]


class Repo:
    """AT Protocol data repo implementation, storage agnostic.

    Attributes:
      did: str, dynamic, repo DID
      storage: :class:`Storage`
      mst: :class:`MST`
      head: :class:`Block`, head commit
      handle: str
      callback: callable, (:class:`CommitData`) => None, called on new commits
        May be set directly by clients. None means no callback.
    """
    storage = None
    mst = None
    head = None
    handle = None
    callback = None
    signing_key = None
    rotation_key = None

    def __init__(self, *, storage=None, mst=None, head=None, handle=None,
                 callback=None, signing_key=None, rotation_key=None):
        """Constructor.

        Args:
          storage: :class:`Storage`, required
          mst: :class:`MST`
          commit: dict, head commit
          cid: :class:`CID`, head CID
          callback: callable, (:class:`CommitData`) => None
          signing_key: :class:`ec.EllipticCurvePrivateKey`, required
          rotation_key: :class:`ec.EllipticCurvePrivateKey`
        """
        assert storage
        assert signing_key

        self.storage = storage
        self.mst = mst
        self.head = head
        self.handle = handle
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
        """

        Returns:
          str, DID
        """
        if self.head:
            return self.head.decoded['did']

    @property
    def version(self):
        """

        Returns:
          int, AT Protocol version
        """
        if self.head:
            return self.head.decoded['version']

    def get_record(self, collection, rkey):
        """

        Args:
          collection: str
          rkey: str

        Returns:
          dict node, record or commit or serialized MST
        """
        cid = self.mst.get(f'{collection}/{rkey}')
        if cid:
            return self.storage.read(cid).decoded

    def get_contents(self):
        """

        Returns:
          dict, {str collection: {str rkey: dict record}}
        """
        entries = self.mst.list()
        blocks = self.storage.read_many([e.value for e in entries])
        contents = defaultdict(dict)
        for entry in entries:
            collection, rkey = entry.key.split('/', 2)
            contents[collection][rkey] = blocks[entry.value].decoded

        return contents

    @classmethod
    def create_from_commit(cls, storage, commit_data, *,
                           signing_key, rotation_key=None, **kwargs):
        """

        Args:
          storage: :class:`Storage`
          commit_data: :class:`CommitData`
          signing_key: :class:`ec.EllipticCurvePrivateKey`, passed through to
            :class:`Storage.create_repo`
          rotation_key: :class:`ec.EllipticCurvePrivateKey`, optional, passed
            through to :class:`Storage.create_repo`
          kwargs: passed through to :class:`Repo` constructor

        Returns:
          :class:`Repo`
        """
        storage.apply_commit(commit_data)

        # avoid reading from storage, since if we're in a transaction, those
        # reads won't see writes that happened in apply_commit. instead,
        # construct Repo and MST in memory from existing data.
        mst = MST(storage=storage, pointer=commit_data.commit.decoded['data'])
        repo = Repo(storage=storage, mst=mst, head=commit_data.commit,
                    signing_key=signing_key, rotation_key=rotation_key,
                    **kwargs)

        storage.create_repo(repo, signing_key=signing_key, rotation_key=rotation_key)
        if repo.callback:
            repo.callback(commit_data)
        return repo

    @classmethod
    def create(cls, storage, did, *, signing_key, rotation_key=None,
               initial_writes=None, **kwargs):
        """

        Args:
          storage: :class:`Storage`
          did: string
          signing_key: :class:`ec.EllipticCurvePrivateKey`, passed through to
            :class:`Storage.create_repo`
          rotation_key: :class:`ec.EllipticCurvePrivateKey`, optional, passed
            through to :class:`Storage.create_repo`
          initial_writes: sequence of :class:`Write`
          kwargs: passed through to :class:`Repo` constructor

        Returns:
          :class:`Repo`, self
        """
        # initial commit
        commit_data = cls.format_commit(storage=storage, repo_did=did,
                                        signing_key=signing_key,
                                        writes=initial_writes)
        return cls.create_from_commit(storage, commit_data, signing_key=signing_key,
                                      rotation_key=rotation_key, **kwargs)

    @classmethod
    def load(cls, storage, cid=None, **kwargs):
        """
        Args:
          storage: :class:`Storage`
          cid: :class:`CID`, optional
          kwargs: passed through to :class:`Repo` constructor

        Returns:
          :class:`Repo`
        """
        commit_cid = cid or storage.head
        assert commit_cid, 'No cid provided and none in storage'

        commit_block = storage.read(commit_cid)
        mst = MST.load(storage=storage, cid=commit_block.decoded['data'])
        logger.info(f'loaded repo for {commit_block.decoded["did"]} at commit {commit_cid}')
        return Repo(storage=storage, mst=mst, head=commit_block, **kwargs)

    @classmethod
    def format_commit(cls, *, repo=None, storage=None, repo_did=None,
                      signing_key=None, mst=None, cur_head=None, writes=None):
        """Creates, but does not store, a new commit.

        If `repo` is provided, all other kwargs should be omitted except
        (optionally) `writes`. Otherwise, `storage`, `repo_did`, and
        `signing_key` are required.

        If `repo` is provided, its `mst` attribute is set to the new
        :class:`MST` resulting from applying this commit.

        Args:
          repo: :class:`Repo`, optional
          storage: :class:`Storage`, optional
          repo_did: str, optional
          signing_key: :class:`ec.EllipticCurvePrivateKey`, optional
          mst: :class:`MST`, optional
          cur_head: :class:`CID`, optional
          writes: sequence of :class:`Write`, optional

        Returns:
          :class:`CommitData`
        """
        if repo:
            assert (not storage and not repo_did and not signing_key and not mst
                    and not cur_head)
            storage = repo.storage
            repo_did = repo.did
            signing_key = repo.signing_key
            mst = repo.mst
            cur_head = repo.head.cid

        if not mst:
            mst = MST.create(storage=storage)

        commit_blocks = {}  # maps CID to Block
        if writes is None:
            writes = []
        orig_mst = mst

        for write in writes:
            assert isinstance(write, Write), type(write)
            data_key = f'{write.collection}/{write.rkey}'

            if write.action == Action.DELETE:
                mst = mst.delete(data_key)
                continue

            block = Block(decoded=write.record)
            commit_blocks[block.cid] = block
            if write.action == Action.CREATE:
                mst = mst.add(data_key, block.cid)
            else:
                assert write.action == Action.UPDATE
                mst = mst.update(data_key, block.cid)

        root, unstored_blocks = mst.get_unstored_blocks()
        commit_blocks.update(unstored_blocks)

        # ensure we're not missing any blocks that were removed and then
        # re-added in this commit
        diff = Diff.of(mst, orig_mst)
        missing = diff.new_cids - commit_blocks.keys()
        if missing:
            commit_blocks.update(storage.read_many(missing))

        commit = util.sign({
            'did': repo_did,
            'version': 3,
            # reuse subscribeRepos sequence number as rev
            # https://github.com/bluesky-social/atproto/discussions/1607
            'rev': util.int_to_tid(storage.allocate_seq(SUBSCRIBE_REPOS_NSID),
                                   clock_id=0),
            'prev': cur_head,
            'data': root,
        }, signing_key)
        commit_block = Block(decoded=commit, ops=writes_to_commit_ops(writes))
        commit_blocks[commit_block.cid] = commit_block

        if repo:
            repo.mst = mst

        return CommitData(commit=commit_block, prev=cur_head, blocks=commit_blocks)

    def apply_commit(self, commit_data):
        """

        Args:
          commit_data: :class:`CommitData`

        Returns:
          :class:`Repo`, self
        """
        self.storage.apply_commit(commit_data)
        self.head = commit_data.commit
        if self.callback:
            self.callback(commit_data)
        return self

    def apply_writes(self, writes):
        """

        Args:
          writes: :class:`Write` or sequence of :class:`Write`

        Returns:
          :class:`Repo`, self
        """
        if isinstance(writes, Write):
            writes = [writes]

        commit_data = Repo.format_commit(repo=self, writes=writes)
        self.apply_commit(commit_data)
        return self
