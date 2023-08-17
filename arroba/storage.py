"""Bluesky repo storage base class and in-memory implementation.

Lightly based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/src/storage/repo-storage.ts
"""
from collections import namedtuple

import dag_cbor
from multiformats import CID, multicodec, multihash

from .util import dag_cbor_cid


CommitData = namedtuple('CommitData', [
  'cid',     # CID
  'blocks',  # BlockMap
  'prev',    # CID or None
])
# commit record format is:
# {
#     'version': 2,
#     'did': [repo],
#     'prev': [CID],
#     'data': [CID],
# }


class BlockMap(dict):
    """dict subclass that stores blocks as CID => blocks (bytes) mappings.

    A block is a DAG-CBOR encoded node, ie a record, MST entry, or commit.
    """
    def add(self, val):
        """Encodes a value as a block and adds it.

        TODO: remove or refactor? keep the invariant that BlockMap stores
        blocks, Storage stores records?

        Args:
          val: dict, record

        Returns:
          :class:`CID`
        """
        block = dag_cbor.encode(val)
        digest = multihash.digest(block, 'sha2-256')
        cid = CID('base58btc', 1, multicodec.get('dag-cbor'), digest)
        self[cid] = block
        return cid

    def byte_size(self):
        """Returns the cumulative size of all blocks, in bytes.

        Returns:
          int
        """
        return sum(len(b) for b in self.values())


class Storage:
    """Abstract base class for storing nodes: records, MST entries, and commits.

    Concrete subclasses should implement this on top of physical storage,
    eg database, filesystem, in memory.

    # TODO: batch operations?

    Attributes:
      head: :class:`CID`
    """
    head = None

    def store_repo(self, repo):
        """Stores a new repo's metadata in storage.

        Only stores the repo's DID, handle, and head commit CID, not blocks!

        Args:
          repo: :class:`Repo`
        """
        raise NotImplementedError()

    def load_repo(self, did=None, handle=None):
        """Loads a repo from storage.

        Either did or handle should be provided, but not both.

        Args:
          did: str, optional
          handle: str, optional

        Returns:
          :class:`Repo`, or None if the did or handle weren't found
        """
        raise NotImplementedError()

    def read(self, cid):
        """Reads a node from storage.

        Args:
          cid: :class:`CID`

        Returns:
          dict, a record, commit, or serialized MST node, or None if the given
          CID is not stored
        """
        raise NotImplementedError()

    def read_many(self, cids):
        """Batch read multiple nodes from storage.

        Args:
          sequence of :class:`CID`

        Returns:
          tuple: (dict {:class:`CID`: dict node},
                  sequence of :class:`CID` that weren't found)
        """
        raise NotImplementedError()

    def read_blocks(self, cids):
        """Batch read multiple blocks from storage.

        Args:
          sequence of :class:`CID`

        Returns:
          tuple: (:class:`BlockMap` with found blocks,
                  sequence of :class:`CID` that weren't found)
        """
        raise NotImplementedError()

    def has(self, cid):
        """Checks if a given :class:`CID` is currently stored.

        Args:
          cid: :class:`CID`

        Returns:
          boolean
        """
        raise NotImplementedError()

    def write(self, node):
        """Writes a node to storage.

        Args:
          node: a record, commit, or serialized MST node

        Returns:
          :class:`CID`
        """
        raise NotImplementedError()

    def apply_commit(self, commit):
        """Writes a commit to storage.

        Args:
          commit: :class:`CommitData`
        """
        raise NotImplementedError()


class MemoryStorage(Storage):
    """In memory storage implementation.

    Attributes:
      repos: list of :class:`Repo`
      blocks: :class:`BlockMap`
      head: :class:`CID`
    """
    repos = []
    blocks = None
    head = None

    def __init__(self):
        self.blocks = BlockMap()

    def store_repo(self, repo):
        if repo not in self.repos:
            repos.append(repo)

    def load_repo(self, did=None, handle=None):
        assert bool(did) ^ bool(handle), f'{did} {handle}'

        for repo in self.repos:
            if (did and repo.did == did) or (handle and repo.handle == handle):
                return repo

    def read(self, cid):
        return dag_cbor.decode(self.blocks[cid])

    def read_many(self, cids):
        blocks, missing = self.read_blocks(cids)
        nodes = {cid: dag_cbor.decode(block) for cid, block in blocks.items()}
        return nodes, missing

    def read_blocks(self, cids):
        found = {}
        missing = []

        for cid in cids:
            block = self.blocks.get(cid)
            if block:
                found[cid] = block
            else:
                missing.append(cid)

        return found, missing

    def has(self, cid):
        return cid in self.blocks

    def write(self, node):
        self.blocks.add(node)

    def apply_commit(self, commit_data):
        self.blocks.update(commit_data.blocks)
        self.head = commit_data.cid
