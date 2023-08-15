"""Bluesky / AT Protocol repo implementation.

https://atproto.com/guides/data-repos

Heavily based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/src/repo.ts

Huge thanks to the Bluesky team for working in the public, in open source, and to
Daniel Holmgren and Devin Ivy for this code specifically!
"""
from collections import defaultdict, namedtuple
from enum import auto, Enum
import logging

import dag_cbor
from multiformats import CID

from . import util
from .diff import Diff
from .mst import MST
from .storage import BlockMap, CommitData, Storage

logger = logging.getLogger(__name__)


class Action(Enum):
    """Used in :meth:`Repo.format_commit`."""
    CREATE = auto()
    UPDATE = auto()
    DELETE = auto()


Write = namedtuple('Write', [
    'action',      # :class:`Action`
    'collection',  # str
    'rkey',        # str
    'record',      # dict
], defaults=[None] * 4)


class Repo:
    """AT Protocol data repo implementation, storage agnostic.

    Attributes:
      storage: :class:`Storage`
      mst: :class:`MST`
      commit: dict, head commit    # TODO: replace these with CommitData?
      cid: :class:`CID`, head CID
      handle: str
      callback: callable, (:class:`CommitData`) => None, called on new commits
        May be set directly by clients. None means no callback.
    """
    storage = None
    mst = None
    commit = None
    cid = None
    callback = None

    def __init__(self, *, storage=None, mst=None, commit=None, cid=None,
                 handle=None):
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
        self.commit = commit
        self.cid = cid
        self.handle = handle

    @property
    def did(self):
        """

        Returns:
          str, DID
        """
        if self.commit:
            return self.commit['did']

    @property
    def version(self):
        """

        Returns:
          int, AT Protocol version
        """
        if self.commit:
            return self.commit['version']

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
            return self.storage.read(cid)

    def get_contents(self):
        """

        Returns:
          dict, {str collection: {str rkey: dict record}}
        """
        entries = self.mst.list()
        nodes, missing = self.storage.read_many(e.value for e in entries)
        assert not missing, f'get_contents missing: {missing}'

        contents = defaultdict(dict)
        for entry in entries:
            collection, rkey = entry.key.split('/', 2)
            contents[collection][rkey] = nodes[entry.value]

        return contents

    @classmethod
    def format_init_commit(cls, storage, did, key, initial_writes=None):
        """
        Args:
          storage: :class:`Storage`
          did: string,
          key: :class:`Crypto.PublicKey.ECC.EccKey`
          initial_writes: sequence of :class:`Write`

        Returns:
          :class:`CommitData`
        """
        new_blocks = BlockMap()

        mst = MST.create(storage=storage)
        if initial_writes:
            for record in initial_writes:
                cid = new_blocks.add(record.record)
                data_key = util.format_data_key(record.collection, record.rkey)
                mst = mst.add(data_key, cid)

        root, blocks = mst.get_unstored_blocks()
        new_blocks.update(blocks)

        commit = util.sign_commit({
            'did': did,
            'version': 2,
            'prev': None,
            'data': root,
        }, key)
        commit_cid = new_blocks.add(commit)
        return CommitData(cid=commit_cid, prev=None, blocks=new_blocks)

    @classmethod
    def create_from_commit(cls, storage, commit, **kwargs):
        """

        Args:
          storage: :class:`Storage`
          commit: :class:`CommitData`
          kwargs: passed through to :class:`Repo` constructor

        Returns:
          :class:`Repo`
        """
        storage.apply_commit(commit)
        return cls.load(storage, commit.cid, **kwargs)

    @classmethod
    def create(cls, storage, did, key, initial_writes=None, **kwargs):
        """

        Args:
          storage: :class:`Storage`
          did: string
          key: :class:`Crypto.PublicKey.ECC.EccKey`
          initial_writes: sequence of :class:`Write`
          kwargs: passed through to :class:`Repo` constructor

        Returns:
          :class:`Repo`
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

        commit = storage.read(commit_cid)
        mst = MST.load(storage=storage, cid=commit['data'])
        logger.info(f'loaded repo for {commit["did"]} at commit {commit_cid}')
        return Repo(storage=storage, mst=mst, commit=commit, cid=commit_cid, **kwargs)

    def format_commit(self, writes, key):
        """

        Args:
          writes: :class:`Write` or sequence of :class:`Write`
          key: :class:`Crypto.PublicKey.ECC.EccKey`

        Returns:
          :class:`CommitData`
        """
        commit_blocks = BlockMap()
        if isinstance(writes, Write):
            writes = [writes]

        mst = self.mst
        for write in writes:
            assert isinstance(write, Write), type(write)
            data_key = f'{write.collection}/{write.rkey}'
            if write.action == Action.CREATE:
                cid = commit_blocks.add(write.record)
                mst = mst.add(data_key, cid)
            elif write.action == Action.UPDATE:
                cid = commit_blocks.add(write.record)
                mst = mst.update(data_key, cid)
            elif write.action == Action.DELETE:
                mst = mst.delete(data_key)

        root, unstored_blocks = mst.get_unstored_blocks()
        commit_blocks.update(unstored_blocks)

        # ensure we're not missing any blocks that were removed and then
        # re-added in this commit
        diff = Diff.of(mst, self.mst)
        missing = diff.new_cids - commit_blocks.keys()
        if missing:
            storage_blocks, not_found = self.storage.read_blocks(missing)
            # this shouldn't ever happen
            assert not not_found, \
                'Could not find block for commit in Datastore or storage'
            commit_blocks.update(storage_blocks)

        commit = util.sign_commit({
            'did': self.did,
            'version': 2,
            'prev': self.cid,
            'data': root,
        }, key)
        commit_cid = commit_blocks.add(commit)

        self.mst = mst
        return CommitData(cid=commit_cid, prev=self.cid, blocks=commit_blocks)

    def apply_commit(self, commit_data):
        """

        Args:
          commit_data: :class:`CommitData`

        Returns:
          :class:`Repo`
        """
        self.storage.apply_commit(commit_data)
        self.commit = dag_cbor.decode(commit_data.blocks[commit_data.cid])
        self.cid = commit_data.cid
        return self

    def apply_writes(self, writes, key):
        """

        Args:
          writes: :class:`Write` or sequence of :class:`Write`
          key: :class:`Crypto.PublicKey.ECC.EccKey`

        Returns:
          :class:`Repo`
        """
        commit = self.format_commit(writes, key)
        if self.callback:
            self.callback(commit)
        return self.apply_commit(commit)

    # def format_rebase(self, key):
    #     """TODO

    #     Args:
    #       key?

    #     Returns:
    #       rebase: :class:`RebaseData`
    #     """
    #     preserved_cids = self.mst.all_cids()
    #     blocks = BlockMap()
    #     commit = util.sign_commit({
    #         'did': self.did,
    #         'version': 2,
    #         'prev': None,
    #         'data': self.commit.mst,
    #     }, key)

    #     commit_cid = blocks.add(commit)
    #     return {
    #         'commit': commit_cid,
    #         'rebased': self.cid,
    #         'blocks': blocks,
    #         'preserved_cids': preserved_cids.to_list(),
    #     }

    # def apply_rebase(self, rebase):
    #     """TODO

    #     Args:
    #       rebase: :class:`RebaseData`

    #     Returns:
    #       :class:`Repo`
    #     """
    #     self.storage.apply_rebase(rebase)
    #     return Repo.load(self.storage, rebase.commit)

    # def rebase(self, key):
    #   """
    #
    #   Args:
    #     key: :class:`Crypto.PublicKey.ECC.EccKey`
    #   """
    #     rebase_data = self.format_rebase(key)
    #     return self.apply_rebase(rebase_data)
