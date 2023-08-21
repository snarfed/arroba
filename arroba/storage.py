"""Bluesky repo storage base class and in-memory implementation.

Lightly based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/src/storage/repo-storage.ts
"""
from collections import namedtuple

import dag_cbor
from multiformats import CID, multicodec, multihash

from .util import dag_cbor_cid

SUBSCRIBE_REPOS_NSID = 'com.atproto.sync.subscribeRepos'


CommitData = namedtuple('CommitData', [
    # STATE: change cid to commit Block
    'cid',     # CID
    'blocks',  # dict of CID to Block
    'prev',    # CID or None
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
    """

    def __init__(self, *, cid=None, decoded=None, encoded=None, seq=None):
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


# STATE: need to expose seq for each block from storage?
# and also need to be able to collect blocks with same seq into commit?
# decode each block, identify commit, use the single commit for each seq?
# and assert if a given seq has no commit, since there should always be one


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

    def has(self, cid):
        """Checks if a given :class:`CID` is currently stored.

        Args:
          cid: :class:`CID`

        Returns:
          boolean
        """
        raise NotImplementedError()

    def write(self, obj):
        """Writes a node to storage.

        Generates new sequence number(s) as necessary for newly stored blocks.

        Args:
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

    def next_seq(self, nsid):
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

    # STATE: need separate get_next_seq and allocate_seq ? so that we can check cursor against current next seq and error if cursor is past it


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

    def has(self, cid):
        return cid in self.blocks

    def write(self, obj):
        block = Block(decoded=obj, seq=self.next_seq(SUBSCRIBE_REPOS_NSID))
        if block not in self.blocks:
            self.blocks.add(block)
        return block.cid

    def apply_commit(self, commit_data):
        seq = self.next_seq(SUBSCRIBE_REPOS_NSID)
        for block in commit_data.blocks.values():
            block.seq = seq

        # only add new blocks so we don't wipe out any existing blocks' sequence
        # numbers. (occasionally we see existing blocks recur, eg MST nodes.)
        for cid, block in commit_data.blocks.items():
            self.blocks.setdefault(cid, block)

        self.head = commit_data.cid
        # the Repo will generally already be in self.repos, and it updates its
        # own head cid, so no need to do that here manually.

    def next_seq(self, nsid):
        assert nsid
        next = self.sequences.setdefault(nsid, 1)
        self.sequences[nsid] += 1
        return next
