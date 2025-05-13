"""Unit tests for storage.py."""
import os

import dag_cbor
from multiformats import CID

from ..repo import Repo, Write
from ..datastore_storage import DatastoreStorage
from ..storage import Action, Block, MemoryStorage, SUBSCRIBE_REPOS_NSID
from ..util import dag_cbor_cid, next_tid, DEACTIVATED, TOMBSTONED

from .testutil import DatastoreTest, NOW, TestCase

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

    def test_read_events_by_seq(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)
        init = repo.head.cid

        tid = next_tid()
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        repo.apply_writes([create])
        create = repo.head.cid

        delete = Write(Action.DELETE, 'co.ll', tid)
        repo.apply_writes([delete])
        delete = repo.head.cid

        events = list(self.storage.read_events_by_seq())
        self.assertEqual(6, len(events))
        self.assertEqual(init, events[0].commit.cid)
        self.assertEqual('com.atproto.sync.subscribeRepos#identity', events[1]['$type'])
        self.assertEqual('com.atproto.sync.subscribeRepos#account', events[2]['$type'])
        self.assertEqual('com.atproto.sync.subscribeRepos#sync', events[3]['$type'])
        self.assertEqual(create, events[4].commit.cid)
        self.assertEqual(delete, events[5].commit.cid)

        events = self.storage.read_events_by_seq(start=5)
        self.assertEqual([create, delete], [cd.commit.cid for cd in events])

    def test_read_events_by_seq_repo(self):
        alice = Repo.create(self.storage, 'did:alice', signing_key=self.key)
        alice_init = alice.head.cid

        bob = Repo.create(self.storage, 'did:bob', signing_key=self.key)

        create = Write(Action.CREATE, 'co.ll', next_tid(), {'foo': 'bar'})
        alice.apply_writes([create])

        create = Write(Action.CREATE, 'co.ll', next_tid(), {'baz': 'biff'})
        bob.apply_writes([create])

        events = list(self.storage.read_events_by_seq(repo='did:alice'))
        self.assertEqual(5, len(events))
        self.assertEqual(alice_init, events[0].commit.cid)
        self.assertEqual('com.atproto.sync.subscribeRepos#identity', events[1]['$type'])
        self.assertEqual('com.atproto.sync.subscribeRepos#account', events[2]['$type'])
        self.assertEqual('com.atproto.sync.subscribeRepos#sync', events[3]['$type'])
        self.assertEqual(alice.head.cid, events[4].commit.cid)

        events = self.storage.read_events_by_seq(repo='did:alice', start=5)
        self.assertEqual([alice.head.cid], [cd.commit.cid for cd in events])

    def test_read_events_by_seq_include_record_block_even_if_preexisting(self):
        # https://github.com/snarfed/bridgy-fed/issues/1016#issuecomment-2109276344
        commit_cids = []

        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)
        commit_cids.append(repo.head.cid)

        prev_prev = repo.head.cid
        first = Write(Action.CREATE, 'co.ll', next_tid(), {'foo': 'bar'})
        repo.apply_writes([first])
        commit_cids.append(repo.head.cid)

        prev = repo.head.cid
        second = Write(Action.CREATE, 'co.ll', next_tid(), {'foo': 'bar'})
        repo.apply_writes([second])

        commits = list(self.storage.read_events_by_seq(start=5))
        self.assertEqual(2, len(commits))

        record = Block(decoded={'foo': 'bar'})
        self.assertEqual(prev, commits[0].commit.cid)
        self.assertEqual(prev_prev, commits[0].prev)
        self.assertEqual(record, commits[0].blocks[record.cid])

        self.assertEqual(repo.head.cid, commits[1].commit.cid)
        self.assertEqual(prev, commits[1].prev)
        self.assertEqual(record, commits[1].blocks[record.cid])

    def test_read_events_by_seq_empty_commit(self):
        # https://github.com/snarfed/arroba/issues/52
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)

        repo.apply_writes([])

        commits = list(self.storage.read_events_by_seq(start=5))
        self.assertEqual(1, len(commits))
        mst_root = repo.mst.get_pointer()
        self.assertEqual({
            repo.head.cid: repo.head,
            mst_root: self.storage.read(mst_root),
        }, commits[0].blocks)

    def test_read_events_tombstone_then_commit(self):
        alice = Repo.create(self.storage, 'did:alice', signing_key=self.key)

        self.storage.tombstone_repo(alice)

        bob = Repo.create(self.storage, 'did:bob', signing_key=self.key)

        events = list(self.storage.read_events_by_seq())
        self.assertEqual(alice.head.cid, events[0].commit.cid)
        self.assertEqual(1, events[0].commit.seq)

        self.assertEqual({
            '$type': 'com.atproto.sync.subscribeRepos#tombstone',
            'seq': 5,
            'did': 'did:alice',
            'time': NOW.isoformat(),
        }, events[4])

        self.assertEqual(bob.head.cid, events[5].commit.cid)
        self.assertEqual(6, events[5].commit.seq)

    def test_read_events_commit_then_tombstone(self):
        alice = Repo.create(self.storage, 'did:alice', signing_key=self.key)
        self.storage.tombstone_repo(alice)

        events = list(self.storage.read_events_by_seq())
        self.assertEqual(5, len(events))
        self.assertEqual(alice.head.cid, events[0].commit.cid)
        self.assertEqual(1, events[0].commit.seq)

        self.assertEqual({
            '$type': 'com.atproto.sync.subscribeRepos#tombstone',
            'seq': 5,
            'did': 'did:alice',
            'time': NOW.isoformat(),
        }, events[4])

    def test_load_repo(self):
        created = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)

        got = self.storage.load_repo('did:web:user.com')
        self.assertEqual('did:web:user.com', got.did)
        self.assertEqual(created.head, got.head)
        self.assertIsNone(got.status)

    def test_store_repo(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)
        repo.handle = 'foo.bar'
        self.storage.store_repo(repo)

        got = self.storage.load_repo('did:web:user.com')
        self.assertEqual('foo.bar', repo.handle)

    def test_load_repos(self):
        alice = Repo.create(self.storage, 'did:web:alice', signing_key=self.key)
        bob = Repo.create(self.storage, 'did:plc:bob', signing_key=self.key)
        self.storage.tombstone_repo(bob)

        got_bob, got_alice = self.storage.load_repos()
        self.assertEqual('did:web:alice', got_alice.did)
        self.assertEqual(alice.head, got_alice.head)
        self.assertIsNone(got_alice.status)

        self.assertEqual('did:plc:bob', got_bob.did)
        self.assertEqual(bob.head, got_bob.head)
        self.assertEqual('tombstoned', got_bob.status)

    def test_load_repos_after(self):
        Repo.create(self.storage, 'did:web:alice', signing_key=self.key)
        Repo.create(self.storage, 'did:plc:bob', signing_key=self.key)

        got = self.storage.load_repos(after='did:plc:bob')
        self.assertEqual(1, len(got))
        self.assertEqual('did:web:alice', got[0].did)

        got = self.storage.load_repos(after='did:web:a')
        self.assertEqual(1, len(got))
        self.assertEqual('did:web:alice', got[0].did)

        got = self.storage.load_repos(after='did:web:alice')
        self.assertEqual([], got)

    def test_load_repos_limit(self):
        Repo.create(self.storage, 'did:web:alice', signing_key=self.key)
        Repo.create(self.storage, 'did:plc:bob', signing_key=self.key)

        got = self.storage.load_repos(limit=2)
        self.assertEqual(2, len(got))

        got = self.storage.load_repos(limit=1)
        self.assertEqual(1, len(got))
        self.assertEqual('did:plc:bob', got[0].did)

    def test_tombstone_repo(self):
        seen = []
        repo = Repo.create(self.storage, 'did:user', signing_key=self.key)
        self.assertEqual(4, self.storage.last_seq(SUBSCRIBE_REPOS_NSID))

        repo.callback = lambda event: seen.append(event)
        self.storage.tombstone_repo(repo)

        self.assertEqual(TOMBSTONED, repo.status)

        self.assertEqual(5, self.storage.last_seq(SUBSCRIBE_REPOS_NSID))
        expected = {
            '$type': 'com.atproto.sync.subscribeRepos#tombstone',
            'seq': 5,
            'did': 'did:user',
            'time': NOW.isoformat(),
        }
        self.assertEqual([expected], seen)
        self.assertEqual(expected, self.storage.read(dag_cbor_cid(expected)).decoded)
        self.assertEqual(TOMBSTONED, repo.status)
        self.assertEqual(TOMBSTONED, self.storage.load_repo('did:user').status)

    def test_deactivate_repo(self):
        seen = []
        repo = Repo.create(self.storage, 'did:user', signing_key=self.key)
        self.assertEqual(4, self.storage.last_seq(SUBSCRIBE_REPOS_NSID))

        repo.callback = lambda event: seen.append(event)
        self.storage.deactivate_repo(repo)
        self.assertEqual(DEACTIVATED, repo.status)
        self.assertEqual(DEACTIVATED, self.storage.load_repo('did:user').status)

        self.assertEqual(5, self.storage.last_seq(SUBSCRIBE_REPOS_NSID))
        expected = {
            '$type': 'com.atproto.sync.subscribeRepos#account',
            'seq': 5,
            'did': 'did:user',
            'time': NOW.isoformat(),
            'active': False,
            'status': 'deactivated',
        }
        self.assertEqual([expected], seen)
        self.assertEqual(expected, self.storage.read(dag_cbor_cid(expected)).decoded)

    def test_activate_repo(self):
        seen = []
        repo = Repo.create(self.storage, 'did:user', signing_key=self.key,
                           status=DEACTIVATED)
        self.assertEqual(4, self.storage.last_seq(SUBSCRIBE_REPOS_NSID))

        repo.callback = lambda event: seen.append(event)
        self.storage.activate_repo(repo)
        self.assertIsNone(repo.status)

        self.assertEqual(5, self.storage.last_seq(SUBSCRIBE_REPOS_NSID))
        expected = {
            '$type': 'com.atproto.sync.subscribeRepos#account',
            'seq': 5,
            'did': 'did:user',
            'time': NOW.isoformat(),
            'active': True,
        }
        self.assertEqual([expected], seen)
        self.assertEqual(expected, self.storage.read(dag_cbor_cid(expected)).decoded)
        self.assertIsNone(repo.status)
        self.assertIsNone(self.storage.load_repo('did:user').status)

    def test_write_event(self):
        repo = Repo.create(self.storage, 'did:user', signing_key=self.key)
        self.assertEqual(4, self.storage.last_seq(SUBSCRIBE_REPOS_NSID))

        block = self.storage.write_event(repo=repo, type='identity',
                                    active=False, status='foo')
        self.assertEqual({
            '$type': 'com.atproto.sync.subscribeRepos#identity',
            'seq': 5,
            'did': 'did:user',
            'time': NOW.isoformat(),
            'active': False,
            'status': 'foo',
        }, block.decoded)
        self.assertEqual(block, self.storage.read(block.cid))

    def test_write_blocks(self):
        repo = Repo.create(self.storage, 'did:user', signing_key=self.key)

        existing = self.storage.write('did:first', {'foo': 'bar'}, seq=123)

        blocks = [
            Block(repo='did:a', decoded={'foo': 'bar'}, seq=456),
            Block(repo='did:b', decoded={'foo': 'baz'}, seq=789),
        ]
        self.storage.write_blocks(blocks)

        got = self.storage.read_many([b.cid for b in blocks])
        self.assertEqual(blocks, list(got.values()))


class DatastoreStorageTest(StorageTest, DatastoreTest):
    """Run all of StorageTest's tests with DatastoreStorage."""
    pass
