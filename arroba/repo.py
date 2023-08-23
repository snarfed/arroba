"""Bluesky / AT Protocol repo implementation.

https://atproto.com/guides/data-repos

Heavily based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/src/repo.ts

Huge thanks to the Bluesky team for working in the public, in open source, and to
Daniel Holmgren and Devin Ivy for this code specifically!
"""
from collections import defaultdict, namedtuple
import logging

import dag_cbor
from multiformats import CID

from . import util
from .diff import Diff
from .mst import MST
from .storage import Action, Block, CommitData, CommitOp, Storage

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
    callback = None

    def __init__(self, *, storage=None, mst=None, head=None, handle=None):
        """Constructor.

        Args:
          storage: :class:`Storage`
          mst: :class:`MST`
          commit: dict, head commit
          cid: :class:`CID`, head CID
        """
        assert storage
        self.storage = storage
        self.mst = mst
        self.head = head
        self.handle = handle

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
    def format_init_commit(cls, storage, did, key, initial_writes=None):
        """

        TODO refactor to reuse format_commit?

        Args:
          storage: :class:`Storage`
          did: string,
          key: :class:`ec.EllipticCurvePrivateKey`
          initial_writes: sequence of :class:`Write`

        Returns:
          :class:`CommitData`
        """
        new_blocks = {}  # maps CID to Block

        mst = MST.create(storage=storage)
        if initial_writes:
            for record in initial_writes:
                block = Block(decoded=record.record)
                new_block[block.cid] = block
                data_key = util.format_data_key(record.collection, record.rkey)
                mst = mst.add(data_key, block.cid)

        root, blocks = mst.get_unstored_blocks()
        new_blocks.update(blocks)

        commit = util.sign_commit({
            'did': did,
            'version': 2,
            'prev': None,
            'data': root,
        }, key)
        commit_block = Block(decoded=commit, ops=writes_to_commit_ops(initial_writes))
        new_blocks[commit_block.cid] = commit_block
        return CommitData(commit=commit_block, prev=None, blocks=new_blocks)

    @classmethod
    def create_from_commit(cls, storage, commit_data, **kwargs):
        """

        Args:
          storage: :class:`Storage`
          commit_data: :class:`CommitData`
          kwargs: passed through to :class:`Repo` constructor

        Returns:
          :class:`Repo`
        """
        storage.apply_commit(commit_data)
        repo = cls.load(storage, commit_data.commit.cid, **kwargs)
        storage.create_repo(repo)
        return repo

    @classmethod
    def create(cls, storage, did, key, initial_writes=None, **kwargs):
        """

        Args:
          storage: :class:`Storage`
          did: string
          key: :class:`ec.EllipticCurvePrivateKey`
          initial_writes: sequence of :class:`Write`
          kwargs: passed through to :class:`Repo` constructor

        Returns:
          :class:`Repo`, self
        """
        commit = cls.format_init_commit(
            storage,
            did,
            key,
            initial_writes,
        )
        return cls.create_from_commit(storage, commit, **kwargs)

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

    def format_commit(self, writes, key):
        """

        Args:
          writes: :class:`Write` or sequence of :class:`Write`
          key: :class:`ec.EllipticCurvePrivateKey`

        Returns:
          :class:`CommitData`
        """
        commit_blocks = {}  # maps CID to Block
        if isinstance(writes, Write):
            writes = [writes]

        mst = self.mst
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
        diff = Diff.of(mst, self.mst)
        missing = diff.new_cids - commit_blocks.keys()
        if missing:
            commit_blocks.update(self.storage.read_many(missing))

        commit = util.sign_commit({
            'did': self.did,
            'version': 2,
            'prev': self.head.cid,
            'data': root,
        }, key)
        commit_block = Block(decoded=commit, ops=writes_to_commit_ops(writes))
        commit_blocks[commit_block.cid] = commit_block

        self.mst = mst
        return CommitData(commit=commit_block, prev=self.head.cid,
                          blocks=commit_blocks)

    def apply_commit(self, commit_data):
        """

        Args:
          commit_data: :class:`CommitData`

        Returns:
          :class:`Repo`, self
        """
        self.storage.apply_commit(commit_data)
        self.head = commit_data.commit
        return self

    def apply_writes(self, writes, key):
        """

        Args:
          writes: :class:`Write` or sequence of :class:`Write`
          key: :class:`ec.EllipticCurvePrivateKey`

        Returns:
          :class:`Repo`, self
        """
        commit_data = self.format_commit(writes, key)
        self.apply_commit(commit_data)
        if self.callback:
            self.callback(commit_data)
        return self

    # def format_rebase(self, key):
    #     """TODO

    #     Args:
    #       key?

    #     Returns:
    #       rebase: :class:`RebaseData`
    #     """
    #     preserved_cids = self.mst.all_cids()
    #     blocks = {}  # CID -> Block
    #     commit = util.sign_commit({
    #         'did': self.did,
    #         'version': 2,
    #         'prev': None,
    #         'data': self.commit.mst,
    #     }, key)

    #     block = Block(decoded=commit)
    #     blocks[block.cid] = block
    #     return {
    #         'commit': block.cid,
    #         'rebased': self.cid,
    #         'blocks': blocks,
    #         'preserved_cids': preserved_cids.to_list(),
    #     }

    # def apply_rebase(self, rebase):
    #     """TODO

    #     Args:
    #       rebase: :class:`RebaseData`

    #     Returns:
    #       :class:`Repo`, self
    #     """
    #     self.storage.apply_rebase(rebase)
    #     return Repo.load(self.storage, rebase.commit)

    # def rebase(self, key):
    #   """
    #
    #   Args:
    #     key: :class:`ec.EllipticCurvePrivateKey`
    #   """
    #     rebase_data = self.format_rebase(key)
    #     return self.apply_rebase(rebase_data)
