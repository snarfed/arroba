"""Bluesky repo storage base class and in-memory implementation.

Lightly based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/src/storage/repo-storage.ts
"""
from collections import namedtuple
from enum import auto, Enum
import itertools

import dag_cbor
from multiformats import CID, multicodec, multihash

from . import util
from .util import dag_cbor_cid, DEACTIVATED, tid_to_int, TOMBSTONED, InactiveRepo

SUBSCRIBE_REPOS_NSID = 'com.atproto.sync.subscribeRepos'


class Action(Enum):
    """Used in :meth:`Repo.format_commit`.

    TODO: switch to StrEnum once we can require Python 3.11.
    """
    CREATE = auto()
    UPDATE = auto()
    DELETE = auto()

# TODO: Should this be a subclass of Block?
# TODO: generalize to handle other events
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
# https://atproto.com/specs/repository#commit-objects
#
# {
#     'version': 3,
#     'did': [repo],
#     'rev': [str, TID],
#     'data': [CID],
#     'prev': [CID or None],
#     'sig': [bytes],
# }


class Block:
    r"""An ATProto block: a record, :class:`MST` entry, commit, or other event.

    Can start from either encoded bytes or decoded object, with or without
    :class:`CID`. Decodes, encodes, and generates :class:`CID` lazily, on
    demand, on attribute access.

    Events should have a fully-qualified ``$type`` field that's one of the
    ``message`` types in ``com.atproto.sync.subscribeRepos``, eg
    ``com.atproto.sync.subscribeRepos#tombstone``.

    Based on :class:`carbox.car.Block`.

    Attributes:
      cid (CID): lazy-loaded (dynamic property)
      decoded (dict): decoded object (dynamic property)
      encoded (bytes): DAG-CBOR encoded data (dynamic property)
      seq (int): ``com.atproto.sync.subscribeRepos`` sequence number
      ops (list): :class:`CommitOp`\s if this is a commit, otherwise None
      time (datetime): when this block was first created
      repo (str): DID of a repo that includes this block. Occasionally, blocks
        may be included in more than one repo, so this may be *any* repo that
        includes it. In practice, it's often the first or last repo that
        included it.
    """
    def __init__(self, *, cid=None, decoded=None, encoded=None, seq=None,
                 ops=None, time=None, repo=None):
        """Constructor.

        Args:
          cid (CID): optional
          decoded (dict): optional
          encoded (bytes): optional
        """
        assert encoded or decoded
        self._cid = cid
        self._encoded = encoded
        self._decoded = decoded
        self.seq = seq
        self.ops = ops
        self.time = time or util.now()
        self.repo = repo

    def __str__(self):
        return f'<Block: {self.cid}>'

    @property
    def cid(self):
        if self._cid is None:
            digest = multihash.digest(self.encoded, 'sha2-256')
            self._cid = CID('base58btc', 1, 'dag-cbor', digest)
        return self._cid

    @property
    def encoded(self):
        if self._encoded is None:
            self._encoded = dag_cbor.encode(self.decoded)
        return self._encoded

    @property
    def decoded(self):
        if self._decoded is None:
            self._decoded = dag_cbor.decode(self.encoded)
        return self._decoded

    def __eq__(self, other):
        """Compares by CID only."""
        return self.cid == other.cid

    def __hash__(self):
        return hash(self.cid)


class Storage:
    """Abstract base class for storing nodes: records, MST entries, commits, etc.

    Concrete subclasses should implement this on top of physical storage,
    eg database, filesystem, in memory.

    TODO: batch operations?

    Attributes:
      head (CID)
    """
    head = None

    def create_repo(self, repo):
        """Stores a new repo's metadata in storage.

        Only stores the repo's DID, handle, and head commit :class:`CID`, not
        blocks!

        If the repo already exists in storage, this should update it instead of
        failing.

        Args:
          repo (Repo)
        """
        raise NotImplementedError()

    def load_repo(self, did_or_handle):
        """Loads a repo from storage.

        Args:
          did_or_handle (str): optional

        Returns:
          Repo, or None if the did or handle wasn't found:
        """
        raise NotImplementedError()

    def store_repo(self, repo):
        """Writes a repo to storage.

        Right now only writes some metadata:
        * handle
        * status

        Args:
          repo (Repo)
        """
        raise NotImplementedError()

    def load_repos(self, after=None, limit=500):
        """Loads multiple repos from storage.

        Repos are returned in lexicographic order of their DIDs, ascending.
        Tombstoned repos are included.

        Args:
          after (str): optional DID to start at, *exclusive*
          limit (int): maximum number of repos to return

        Returns:
          sequence of Repo:
        """
        raise NotImplementedError()

    def deactivate_repo(self, repo):
        """Marks a repo as deactivated.

        * Stores a ``com.atproto.sync.subscribeRepos#account`` block with its
          own sequence number.
        * If :attr:`Repo.callback` is populated, calls it with the
          ``com.atproto.sync.subscribeRepos#account`` message.
        * Calls :meth:`Repo._set_status` to mark the repo as deactivated in storage.

        After this, any attempt to write to this repo will raise
        :class:`InactiveRepo`.

        Args:
          repo (Repo)
        """
        self._set_repo_status(repo, DEACTIVATED)
        block = self.write_event(repo=repo, type='account',
                                 active=False, status='deactivated')

    def activate_repo(self, repo):
        """Marks a repo as active.

        Only needed after deactivating. Does nothing if the repo is tombstoned.

        * Stores a ``com.atproto.sync.subscribeRepos#account`` block with its
          own sequence number.
        * If :attr:`Repo.callback` is populated, calls it with the
          ``com.atproto.sync.subscribeRepos#account`` message.
        * Calls :meth:`Repo._set_status` to mark the repo as active in storage.

        Args:
          repo (Repo)
        """
        self._set_repo_status(repo, None)
        block = self.write_event(repo=repo, type='account', active=True)

    def tombstone_repo(self, repo):
        """Marks a repo as tombstoned.

        * Stores a ``com.atproto.sync.subscribeRepos#tombstone`` block with its
          own sequence number.
        * If :attr:`Repo.callback` is populated, calls it with the
          ``com.atproto.sync.subscribeRepos#tombstone`` message.
        * Calls :meth:`Repo._set_status` to mark the repo as deactivated in storage.

        After this, any attempt to write to this repo will raise
        :class:`InactiveRepo`.

        Args:
          repo (Repo)
        """
        self._set_repo_status(repo, TOMBSTONED)
        block = self.write_event(repo=repo, type='tombstone')

    def _tombstone_repo(self, repo):
        """Marks a repo as tombstoned in storage.

        Args:
          repo (Repo)
        """
        raise NotImplementedError()

    def read(self, cid):
        """Reads a node from storage.

        Args:
          cid (CID)

        Returns:
          Block, or None if not found:
        """
        raise NotImplementedError()

    def read_many(self, cids, require_all=True):
        """Batch read multiple nodes from storage.

        Args:
          cids (sequence of CID)
          require_all (bool): whether to assert that all cids are found

        Returns:
          dict: {:class:`CID`: :class:`Block` or None if not found}
        """
        raise NotImplementedError()

    def read_blocks_by_seq(self, start=0, repo=None):
        """Batch read blocks from storage by ``subscribeRepos`` sequence number.

        Args:
          seq (int): optional ``subscribeRepos`` sequence number to start from.
            Defaults to 0.
          repo (str): optional repo DID. If not provided, all repos are included.

        Returns:
          iterable or generator: all :class:`Block` s starting from ``seq``,
          inclusive, in ascending ``seq`` order
        """
        raise NotImplementedError()

    def read_events_by_seq(self, start=0, repo=None):
        """Batch read commits and other events by ``subscribeRepos`` sequence number.

        Args:
          start (int): optional ``subscribeRepos`` sequence number to start from,
            inclusive. Defaults to 0.
          repo (str): optional repo DID. If not provided, all repos are included.

        Returns:
          generator: generator of :class:`CommitData` for commits and dict
          messages for other events, starting from ``seq``, inclusive, in
          ascending ``seq`` order
        """
        assert start >= 0

        seq = commit_block = blocks = None

        def make_commit():
            for op in commit_block.ops:
                if (op.action in (Action.CREATE, Action.UPDATE)
                        and op.cid not in blocks):
                    record = self.read(op.cid)
                    assert record
                    blocks[op.cid] = record
            return CommitData(blocks=blocks, commit=commit_block,
                              prev=commit_block.decoded.get('prev'))

        for block in self.read_blocks_by_seq(start=start, repo=repo):
            assert block.seq
            if block.seq != seq:  # switching to a new commit's blocks
                if commit_block:
                    yield make_commit()
                else:
                    # we shouldn't have any dangling blocks that we don't serve
                    assert not blocks
                seq = block.seq
                blocks = {}  # maps CID to Block
                commit_block = None

            if block.decoded.get('$type', '').startswith(
                    'com.atproto.sync.subscribeRepos#'):  # non-commit message
                yield block.decoded
                continue

            blocks[block.cid] = block
            commit_fields = ['version', 'did', 'rev', 'prev', 'data', 'sig']
            if block.decoded.keys() == set(commit_fields):
                commit_block = block

        # final commit
        if blocks:
            assert commit_block, f'seq {seq}'
            yield make_commit()

    def has(self, cid):
        """Checks if a given :class:`CID` is currently stored.

        Args:
          cid (CID)

        Returns:
          bool:
        """
        raise NotImplementedError()

    def write(self, repo_did, obj, seq=None):
        """Writes a node to storage.

        Args:
          repo_did (str):
          obj (dict): a record, commit, serialized :class:`MST` node, or
            `subscribeRepos` event/message
          seq (int or None): sequence number. If not provided, a new one will be
            allocated.

        Returns:
          Block:

        Raises:
          InactiveError: if the repo is not active
        """
        raise NotImplementedError()

    def write_event(self, repo, type, **kwargs):
        """Writes a ``subscribeRepos`` event to storage.

        Args:
          repo (Repo)
          type (str): ``account`` or ``identity``
          kwargs: included in the event, eg ``active``, `status``

        Returns:
          Block:

        Raises:
          InactiveError: if the repo is not active
        """
        assert type in ('account', 'identity', 'tombstone'), type

        seq = self.allocate_seq(SUBSCRIBE_REPOS_NSID)
        block = self.write(repo.did, {
            '$type': f'com.atproto.sync.subscribeRepos#{type}',
            'seq': seq,
            'did': repo.did,
            'time': util.now().isoformat(),
            **kwargs,
        }, seq=seq)

        if repo.callback:
            repo.callback(block.decoded)
        return block

    def write_blocks(self, blocks):
        """Batch write blocks to storage.

        Overwrites any existing stored blocks with the same CIDs! Does not
        allocate sequence numbers!

        Args:
          blocks (sequence of :class:`Block`)
        """
        raise NotImplementedError()

    def apply_commit(self, commit_data):
        """Writes a commit to storage.

        Generates a new sequence number and uses it for all blocks in the commit.

        Args:
          commit (CommitData)

        Raises:
          InactiveError: if the repo is not active
        """
        raise NotImplementedError()

    def allocate_seq(self, nsid):
        """Generates and returns a sequence number for the given NSID.

        Sequence numbers must be monotonically increasing positive integers, per
        NSID. They may have gaps. Background:
        https://atproto.com/specs/event-stream#sequence-numbers

        Args:
          nsid (str): subscription XRPC method this sequence number is for

        Returns:
          int:
        """
        raise NotImplementedError()

    def last_seq(self, nsid):
        """Returns the last (highest) stored sequence number for the given NSID.

        Args:
          nsid (str): subscription XRPC method this sequence number is for

        Returns:
          int:
        """
        raise NotImplementedError()


class MemoryStorage(Storage):
    """In memory storage implementation.

    Attributes:
      repos (dict mapping str DID to :class:`Repo`)
      blocks (dict): {:class:`CID`: :class:`Block`}
      head (CID)
      sequences (dict): {str NSID: int next sequence number}
    """
    repos = None
    blocks = None
    head = None
    sequences = None

    def __init__(self):
        self.blocks = {}
        self.repos = {}
        self.sequences = {}

    def create_repo(self, repo):
        self.repos[repo.did] = repo

    def load_repo(self, did_or_handle):
        assert did_or_handle

        for repo in self.repos.values():
            if did_or_handle in (repo.did, repo.handle):
                return repo

    def store_repo(self, repo):
        stored = self.repos[repo.did]
        stored.handle = repo.handle
        stored.statue = repo.status

    def load_repos(self, after=None, limit=500):
        it = iter(sorted(self.repos.values(), key=lambda repo: repo.did))

        if after:
            it = itertools.dropwhile(lambda repo: repo.did <= after, it)

        return list(itertools.islice(it, limit))

    def _set_repo_status(self, repo, status):
        repo.status = status

    def read(self, cid):
        return self.blocks.get(cid)

    def read_many(self, cids, require_all=True):
        cids = list(cids)
        found = {cid: self.blocks.get(cid) for cid in cids}
        if require_all:
            assert len(found) == len(cids), (len(found), len(cids))
        return found

    def read_blocks_by_seq(self, start=0, repo=None):
        assert start >= 0
        return sorted((b for b in self.blocks.values()
                       if b.seq >= start and (not repo or b.repo == repo)),
                      key=lambda b: b.seq)

    def has(self, cid):
        return cid in self.blocks

    def write(self, repo_did, obj, seq=None):
        if seq is None:
            seq = self.allocate_seq(SUBSCRIBE_REPOS_NSID)

        block = Block(decoded=obj, seq=seq, repo=repo_did)
        if block not in self.blocks:
            self.blocks[block.cid] = block
        return block

    def write_blocks(self, blocks):
        self.blocks.update({b.cid: b for b in blocks})

    def apply_commit(self, commit_data):
        if repo := self.repos.get(commit_data.commit.repo):
            if repo.status:
                raise InactiveRepo(repo.did, repo.status)

        seq = tid_to_int(commit_data.commit.decoded['rev'])
        assert seq

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
