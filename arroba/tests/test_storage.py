"""Unit tests for storage.py."""
import os

import dag_cbor
from multiformats import CID

from ..repo import Repo, Write
from ..storage import Action, Block, MemoryStorage, SUBSCRIBE_REPOS_NSID
from ..util import dag_cbor_cid, next_tid, TOMBSTONED, TombstonedRepo

from .testutil import NOW, TestCase

DECODED = {'foo': 'bar'}
ENCODED = b'\xa1cfoocbar'
CID_ = CID.decode('bafyreiblaotetvwobe7cu2uqvnddr6ew2q3cu75qsoweulzku2egca4dxq')


class StorageTest(TestCase):
    def test_block_encoded(self):
        block = Block(encoded=ENCODED)
        self.assertEqual(DECODED, block.decoded)
        self.assertEqual(CID_, block.cid)

    def test_block_decoded(self):
        block = Block(decoded=DECODED)
        self.assertEqual(ENCODED, block.encoded)
        self.assertEqual(CID_, block.cid)

    def test_block_eq(self):
        self.assertEqual(Block(decoded=DECODED), Block(encoded=ENCODED))

    def test_block_hash(self):
        self.assertEqual(id(Block(decoded=DECODED)), id(Block(encoded=ENCODED)))

    def test_read_commits_by_seq(self):
        commit_cids = []

        storage = MemoryStorage()
        repo = Repo.create(storage, 'did:web:user.com', signing_key=self.key)
        commit_cids.append(repo.head.cid)

        tid = next_tid()
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        commit_cid = repo.apply_writes([create])
        commit_cids.append(repo.head.cid)

        delete = Write(Action.DELETE, 'co.ll', tid)
        commit_cid = repo.apply_writes([delete])
        commit_cids.append(repo.head.cid)

        self.assertEqual(commit_cids, [cd.commit.cid for cd in
                                       storage.read_commits_by_seq()])
        self.assertEqual(commit_cids[1:], [cd.commit.cid for cd in
                                           storage.read_commits_by_seq(start=2)])

    def test_read_commits_by_seq_include_record_block_even_if_preexisting(self):
        # https://github.com/snarfed/bridgy-fed/issues/1016#issuecomment-2109276344
        commit_cids = []

        storage = MemoryStorage()
        repo = Repo.create(storage, 'did:web:user.com', signing_key=self.key)
        commit_cids.append(repo.head.cid)

        first = Write(Action.CREATE, 'co.ll', next_tid(), {'foo': 'bar'})
        commit_cid = repo.apply_writes([first])
        commit_cids.append(repo.head.cid)

        prev = repo.head.cid
        second = Write(Action.CREATE, 'co.ll', next_tid(), {'foo': 'bar'})
        commit_cid = repo.apply_writes([second])

        commits = list(storage.read_commits_by_seq(start=3))
        self.assertEqual(1, len(commits))
        self.assertEqual(repo.head.cid, commits[0].commit.cid)
        self.assertEqual(prev, commits[0].prev)

        record = Block(decoded={'foo': 'bar'})
        self.assertEqual(record, commits[0].blocks[record.cid])

    def test_tombstone_repo(self):
        seen = []
        storage = MemoryStorage()
        repo = Repo.create(storage, 'did:user', signing_key=self.key)
        self.assertEqual(1, storage.last_seq(SUBSCRIBE_REPOS_NSID))

        repo.callback = lambda event: seen.append(event)
        storage.tombstone_repo(repo)

        self.assertEqual(TOMBSTONED, repo.status)

        self.assertEqual(2, storage.last_seq(SUBSCRIBE_REPOS_NSID))
        expected = {
            '$type': 'com.atproto.sync.subscribeRepos#tombstone',
            'seq': 2,
            'did': 'did:user',
            'time': NOW.isoformat(),
        }
        self.assertEqual([expected], seen)
        self.assertEqual(expected, storage.read(dag_cbor_cid(expected)).decoded)

        with self.assertRaises(TombstonedRepo):
            storage.load_repo('did:user')
