"""Bluesky repo storage base class and in-memory implementation.

Lightly based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/src/storage/repo-storage.ts
"""
import dag_cbor.encoding
from multiformats import CID, multicodec, multihash

from .util import dag_cbor_cid


class BlockMap(dict):
    """dict subclass that stores blocks as CID => bytes mappings."""
    def add(self, val):
        """Encodes a value as a block and stores it.

        Args:
          val: dict, record

        Returns:
          :class:`CID`
        """
        block = dag_cbor.encoding.encode(val)
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
    """Abstract base class for storing all nodes: records, MST, commit chain.

    Concrete subclasses should implement this on top of physical storage,
    eg database, filesystem, in memory.

    # TODO: batch operations?
    """

    def read(self, cid):
        """Reads a node from storage.

        Args:
          cid: :class:`CID`

        Returns:
          dict, a record, commit, or serialized MST node
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


class MemoryStorage(Storage):
    """In memory storage implementation."""

    def __init__(self):
        self.store = {}

    def read(self, cid):
        return self.store[cid]

    def has(self, cid):
        return cid in self.store

    def write(self, node):
        self.store[dag_cbor_cid(node)] = node
