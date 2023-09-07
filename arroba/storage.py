"""Bluesky repo storage base class and in-memory implementation.

Lightly based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/src/storage/repo-storage.ts
"""
from collections import namedtuple
from enum import auto, Enum

import dag_cbor
from multiformats import CID, multicodec, multihash

from .util import dag_cbor_cid

SUBSCRIBE_REPOS_NSID = 'com.atproto.sync.subscribeRepos'


class Action(Enum):
    """Used in :meth:`Repo.format_commit`.

    TODO: switch to StrEnum once we can require Python 3.11.
    """
    CREATE = auto()
    UPDATE = auto()
    DELETE = auto()

# TODO: Should this be a subclass of Block?
CommitData = namedtuple('CommitData', [
    'commit',  # Block
    'blocks',  # dict of CID to Block
    'prev',    # CID or None
], defaults=[None])  # for ops

CommitOp = namedtuple('CommitOp', [  # for subscribeRepos
    'action',  # Action
    'path',    # str
    'cid',     # CID, or None for DELETE
])

# commit record format is:
# {
#     'version': 2,
#     'did': [repo],
#     'prev': [CID],
#     'data': [CID],
# }


class Block:
    """An ATProto block: a record, MST entry, or commit.

    Can start from either encoded bytes or decoded object, with or without CID.
    Decodes, encodes, and generates CID lazily, on demand, on attribute access.

    Based on :class:`carbox.car.Block`.

    Attributes:
      _cid: :class:`CID`, lazy-loaded
      _decoded: dict, lazy-loaded
      _encoded: bytes, lazy-loaded
      seq: integer, com.atproto.sync.subscribeRepos sequence number
      ops: list of :class:`CommitOp` if this is a commit, otherwise None
    """
    def __init__(self, *, cid=None, decoded=None, encoded=None, seq=None,
                 ops=None):
        """Constructor.

        Args:
          cid: :class:`CID`, optional
          decoded: dict, optional
          encoded: bytes, optional
        """
        assert encoded or decoded
        self._cid = cid
        self._encoded = encoded
        self._decoded = decoded
        self.seq = seq
        self.ops = ops

    @property
    def cid(self):
        """
        Returns:
          :class:`CID`
        """
        if self._cid is None:
            digest = multihash.digest(self.encoded, 'sha2-256')
            self._cid = CID('base58btc', 1, multicodec.get('dag-cbor'), digest)
        return self._cid

    @property
    def encoded(self):
        """
        Returns:
          bytes, DAG-CBOR encoded
        """
        if self._encoded is None:
            self._encoded = dag_cbor.encode(self.decoded)
        return self._encoded

    @property
    def decoded(self):
        """
        Returns:
          dict, decoded object
        """
        if self._decoded is None:
            self._decoded = dag_cbor.decode(self.encoded)
        return self._decoded

    def __eq__(self, other):
        """Compares by CID only."""
        return self.cid == other.cid

    def __hash__(self):
        return hash(self.cid)


class Storage:
    """Abstract base class for storing nodes: records, MST entries, and commits.

    Concrete subclasses should implement this on top of physical storage,
    eg database, filesystem, in memory.

    # TODO: batch operations?

    Attributes:
      head: :class:`CID`
    """
    head = None

    def create_repo(self, repo):
        """Stores a new repo's metadata in storage.

        Only stores the repo's DID, handle, and head commit CID, not blocks!

        If the repo already exists in storage, this should update it instead of
        failing.

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
          :class:`Block` or None if not found
        """
        raise NotImplementedError()

    def read_many(self, cids, require_all=True):
        """Batch read multiple nodes from storage.

        Args:
          cids: sequence of :class:`CID`
          require_all: boolean, whether to assert that all cids are found

        Returns:
          dict: {:class:`CID`: :class:`Block` or None if not found}
        """
        raise NotImplementedError()

    def read_blocks_by_seq(self, start=0):
        """Batch read blocks from storage by `subscribeRepos` sequence number.

        Args:
          seq: integer, optional `subscribeRepos` sequence number to start from.
            Defaults to 0.

        Returns:
          iterable or generator of :class:`Block`, starting from `seq`,
          inclusive, in ascending `seq` order
        """
        raise NotImplementedError()

    def read_commits_by_seq(self, start=0):
        """Batch read commits from storage by `subscribeRepos` sequence number.

        Args:
          seq: integer, optional `subscribeRepos` sequence number to start from.
            Defaults to 0.

        Returns:
          generator of :class:`CommitData`, starting from `seq`, inclusive, in
          ascending `seq` order
        """
        assert start >= 0

        seq = commit_block = blocks = None

        for block in self.read_blocks_by_seq(start=start):
            assert block.seq
            if block.seq != seq:  # switching to a new commit's blocks
                if commit_block:
                    assert blocks
                    yield CommitData(blocks=blocks, commit=commit_block,
                                     prev=commit_block.decoded.get('prev'))
                else:
                    assert blocks is None  # only the first commit
                seq = block.seq
                blocks = {}  # maps CID to Block
                commit_block = None

            blocks[block.cid] = block
            if block.decoded.keys() == set(['version', 'did', 'prev', 'data', 'sig']):
                commit_block = block

        # final commit
        if blocks:
            assert blocks and commit_block
            yield CommitData(blocks=blocks, commit=commit_block,
                             prev=commit_block.decoded.get('prev'))

    def has(self, cid):
        """Checks if a given :class:`CID` is currently stored.

        Args:
          cid: :class:`CID`

        Returns:
          boolean
        """
        raise NotImplementedError()

    def write(self, repo_did, obj):
        """Writes a node to storage.

        Generates new sequence number(s) as necessary for newly stored blocks.

        Args:
          repo_did: str
          obj: dict, a record, commit, or serialized MST node

        Returns:
          :class:`CID`
        """
        raise NotImplementedError()

    def apply_commit(self, commit_data):
        """Writes a commit to storage.

        Generates a new sequence number and uses it for all blocks in the commit.

        Args:
          commit: :class:`CommitData`
        """
        raise NotImplementedError()

    def allocate_seq(self, nsid):
        """Generates and returns a sequence number for the given NSID.

        Sequence numbers must be monotonically increasing positive integers, per
        NSID. They may have gaps. Background:
        https://atproto.com/specs/event-stream#sequence-numbers

        Args:
          nsid: str, subscription XRPC method this sequence number is for

        Returns:
          integer
        """
        raise NotImplementedError()

    def last_seq(self, nsid):
        """Returns the last (highest) stored sequence number for the given NSID.

        Args:
          nsid: str, subscription XRPC method this sequence number is for

        Returns:
          integer
        """
        raise NotImplementedError()


class MemoryStorage(Storage):
    """In memory storage implementation.

    Attributes:
      repos: list of :class:`Repo`
      blocks: dict: {:class:`CID`: :class:`Block`}
      head: :class:`CID`
      sequences: dict, maps str NSID to integer next sequence number
    """
    repos = None
    blocks = None
    head = None
    sequences = None

    def __init__(self):
        self.blocks = {}
        self.repos = []
        self.sequences = {}

    def create_repo(self, repo):
        if repo not in self.repos:
            self.repos.append(repo)

    def load_repo(self, did=None, handle=None):
        assert bool(did) ^ bool(handle), f'{did} {handle}'

        for repo in self.repos:
            if (did and repo.did == did) or (handle and repo.handle == handle):
                return repo

    def read(self, cid):
        return self.blocks.get(cid)

    def read_many(self, cids, require_all=True):
        cids = list(cids)
        found = {cid: self.blocks.get(cid) for cid in cids}
        if require_all:
            assert len(found) == len(cids), (len(found), len(cids))
        return found

    def read_blocks_by_seq(self, start=0):
        assert start >= 0
        return sorted((b for b in self.blocks.values() if b.seq >= start),
                      key=lambda b: b.seq)

    def has(self, cid):
        return cid in self.blocks

    def write(self, repo_did, obj):
        block = Block(decoded=obj, seq=self.allocate_seq(SUBSCRIBE_REPOS_NSID))
        if block not in self.blocks:
            self.blocks.add(block)
        return block.cid

    def apply_commit(self, commit_data):
        seq = self.allocate_seq(SUBSCRIBE_REPOS_NSID)
        for block in commit_data.blocks.values():
            block.seq = seq

        # only add new blocks so we don't wipe out any existing blocks' sequence
        # numbers. (occasionally we see existing blocks recur, eg MST nodes.)
        for cid, block in commit_data.blocks.items():
            self.blocks.setdefault(cid, block)

        self.head = commit_data.commit.cid
        # the Repo will generally already be in self.repos, and it updates its
        # own head cid, so no need to do that here manually.

    def allocate_seq(self, nsid):
        assert nsid
        next = self.sequences.setdefault(nsid, 1)
        self.sequences[nsid] += 1
        return next

    def last_seq(self, nsid):
        assert nsid
        return self.sequences[nsid] - 1
