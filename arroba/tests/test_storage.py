"""Unit tests for storage.py."""
from itertools import chain
import os
import random

import dag_cbor
from multiformats import CID

from ..datastore_storage import DatastoreStorage
from ..mst import MST
from ..repo import Repo, Write
from ..storage import (
    Action,
    Block,
    CommitOp,
    MAX_OPERATIONS_PER_COMMIT,
    MAX_RECORD_SIZE_BYTES,
    MemoryStorage,
    SUBSCRIBE_REPOS_NSID,
)
from ..util import dag_cbor_cid, next_tid, DEACTIVATED, TOMBSTONED

from .testutil import DatastoreTest, NOW, TestCase
from .test_repo import RepoTest

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
        self.storage.commit(repo, [create])
        create = repo.head.cid

        delete = Write(Action.DELETE, 'co.ll', tid)
        self.storage.commit(repo, [delete])
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
        self.storage.commit(alice, [create])

        create = Write(Action.CREATE, 'co.ll', next_tid(), {'baz': 'biff'})
        self.storage.commit(bob, [create])

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
        self.storage.commit(repo, [first])
        commit_cids.append(repo.head.cid)

        prev = repo.head.cid
        second = Write(Action.CREATE, 'co.ll', next_tid(), {'foo': 'bar'})
        self.storage.commit(repo, [second])

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

        self.storage.commit(repo, [])

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
        self.assertEqual(4, self.storage.sequences.last(SUBSCRIBE_REPOS_NSID))

        repo.callback = lambda event: seen.append(event)
        self.storage.tombstone_repo(repo)

        self.assertEqual(TOMBSTONED, repo.status)

        self.assertEqual(5, self.storage.sequences.last(SUBSCRIBE_REPOS_NSID))
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
        self.assertEqual(4, self.storage.sequences.last(SUBSCRIBE_REPOS_NSID))

        repo.callback = lambda event: seen.append(event)
        self.storage.deactivate_repo(repo)
        self.assertEqual(DEACTIVATED, repo.status)
        self.assertEqual(DEACTIVATED, self.storage.load_repo('did:user').status)

        self.assertEqual(5, self.storage.sequences.last(SUBSCRIBE_REPOS_NSID))
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
        repo = Repo.create(self.storage, 'did:user', signing_key=self.key)
        self.storage.deactivate_repo(repo)
        self.assertEqual(5, self.storage.sequences.last(SUBSCRIBE_REPOS_NSID))

        repo.callback = lambda event: seen.append(event)
        self.storage.activate_repo(repo)
        self.assertIsNone(repo.status)

        self.assertEqual(6, self.storage.sequences.last(SUBSCRIBE_REPOS_NSID))
        expected = {
            '$type': 'com.atproto.sync.subscribeRepos#account',
            'seq': 6,
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
        self.assertEqual(4, self.storage.sequences.last(SUBSCRIBE_REPOS_NSID))

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

    def test_commit_basic_operations(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)

        profile = {
            '$type': 'app.bsky.actor.profile',
            'displayName': 'Alice',
            'avatar': 'https://alice.com/alice.jpg',
            'description': None,
        }

        tid = next_tid()
        self.storage.commit(repo, Write(
            action=Action.CREATE,
            collection='my.stuff',
            rkey=tid,
            record=profile,
        ))
        self.assertEqual(profile, repo.get_record('my.stuff', tid))

        self.assertEqual(profile, repo.get_record('my.stuff', tid))
        reloaded = Repo.load(self.storage, cid=repo.head.cid,
                             signing_key=self.key)
        self.assertEqual(profile, reloaded.get_record('my.stuff', tid))

        profile['description'] = "I'm the best"
        self.storage.commit(repo, Write(
            action=Action.UPDATE,
            collection='my.stuff',
            rkey=tid,
            record=profile,
        ))
        self.assertEqual(profile, repo.get_record('my.stuff', tid))

        self.assertEqual(profile, repo.get_record('my.stuff', tid))
        reloaded = Repo.load(self.storage, cid=repo.head.cid,
                             signing_key=self.key)
        self.assertEqual(profile, reloaded.get_record('my.stuff', tid))

        self.storage.commit(repo, Write(
            action=Action.DELETE,
            collection='my.stuff',
            rkey=tid,
        ))
        self.assertIsNone(repo.get_record('my.stuff', tid))

        reloaded = Repo.load(self.storage, cid=repo.head.cid,
                             signing_key=self.key)
        self.assertIsNone(reloaded.get_record('my.stuff', tid))

    def test_commit_creates(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)

        data = {
            'example.foo': self.random_objects(10),
            'example.bar': self.random_objects(20),
            'example.baz': self.random_objects(30),
        }

        writes = list(chain(*(
            [Write(Action.CREATE, coll, tid, obj) for tid, obj in objs.items()]
            for coll, objs in data.items())))
        self.storage.commit(repo, writes)
        self.assertEqual(data, repo.get_contents())

    def test_commit_updates_and_deletes(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)

        objs = list(self.random_objects(20).items())
        creates = [Write(Action.CREATE, 'co.ll', tid, obj) for tid, obj in objs]
        self.storage.commit(repo, creates)

        random.shuffle(objs)
        updates = [Write(Action.UPDATE, 'co.ll', tid, {'bar': 'baz'})
                   for tid, _ in objs]
        self.storage.commit(repo, updates)

        random.shuffle(objs)
        deletes = [Write(Action.DELETE, 'co.ll', tid) for tid, _ in objs]
        self.storage.commit(repo, deletes)

        self.assertEqual({}, repo.get_contents())

    def test_commit_delete_with_record_no_cid_in_op(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)

        tid = next_tid()
        self.storage.commit(repo, Write(
            action=Action.CREATE,
            collection='my.stuff',
            rkey=tid,
            record={'x': 'y'},
        ))
        self.assertEqual({'my.stuff': {tid: {'x': 'y'}}}, repo.get_contents())

        self.storage.commit(repo, [Write(
            Action.DELETE,
            collection='my.stuff',
            rkey=tid,
            record={'x': 'y'},
        )])
        self.assertEqual({}, repo.get_contents())
        self.assertEqual([CommitOp(
            action=Action.DELETE,
            path=f'my.stuff/{tid}',
            cid=None,  # should be None even though the input op (wrongly) had a record
            prev_cid=dag_cbor_cid({'x': 'y'}),
        )], repo.head.ops)

    def test_commit_noop_update_doesnt_commit(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)
        tid = next_tid()
        self.storage.commit(repo, [Write(Action.CREATE, 'co.ll', tid, {'x': 'y'})])
        self.storage.commit(repo, [Write(Action.UPDATE, 'co.ll', tid, {'x': 'y'})])
        self.assertEqual([], repo.head.ops)

    def test_commit_update_nonexistent_record_raises_ValueError(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)
        update = Write(Action.UPDATE, 'co.ll', next_tid(), {'x': 'y'})
        with self.assertRaises(ValueError):
            self.storage.commit(repo, update)

    def test_commit_delete_nonexistent_record_raises_ValueError(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)
        update = Write(Action.DELETE, 'co.ll', next_tid(), {'x': 'y'})
        with self.assertRaises(ValueError):
            self.storage.commit(repo, update)

    def test_commit_too_many_operations(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)
        writes = [Write(Action.CREATE, 'co.ll', next_tid(), {'x': i})
                  for i in range(MAX_OPERATIONS_PER_COMMIT + 1)]
        with self.assertRaises(ValueError) as cm:
            self.storage.commit(repo, writes)

    def test_commit_record_too_large(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)
        large_record = {
            '$type': 'app.bsky.feed.post',
            'text': 'x' * MAX_RECORD_SIZE_BYTES,
        }

        create = Write(Action.CREATE, 'co.ll', next_tid(), large_record)
        with self.assertRaises(ValueError) as cm:
            self.storage.commit(repo, create)

    def test_commit_callback(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)
        seen = []
        repo_test = RepoTest()

        # create new object with callback
        repo.callback = lambda commit: seen.append(commit)
        tid = next_tid()
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        self.storage.commit(repo, [create])

        self.assertEqual(1, len(seen))
        repo_test.assertCommitIs(seen[0], create, 5)

        # update object
        update = Write(Action.UPDATE, 'co.ll', tid, {'foo': 'baz'})
        self.storage.commit(repo, [update])
        self.assertEqual(2, len(seen))
        repo_test.assertCommitIs(seen[1], update, 6,
                                 prev_record=dag_cbor_cid({'foo': 'bar'}))

        # unset callback, update again
        repo.callback = None
        update = Write(Action.UPDATE, 'co.ll', tid, {'biff': 0})
        self.storage.commit(repo, [update])
        self.assertEqual(2, len(seen))


class DatastoreStorageTest(StorageTest, DatastoreTest):
    """Run all of StorageTest's tests with DatastoreStorage."""
    def test_create_commit_datastore_transaction_retry(self):
        # fake what a Repo.create => Storage.commit retry due to datastore transaction
        # contention  would look like.
        # TODO: find a way to mock this inside ndb or the datastore API istelf
        initial_commit = Block(decoded={
            'did': 'did:alice',
            'prev': None,
        })
        repo = Repo(storage=self.storage, mst=MST.create(storage=self.storage),
                    signing_key=self.key, head=initial_commit)
        self.storage.commit(repo, [], repo_did='did:alice')
        assert repo.head is not initial_commit

    def test_create_commit_non_empty_repo_with_repo_did(self):
        head = Block(decoded={
            'did': 'did:alice',
            'prev': "set because this isn't an initial empty commit",
        })
        repo = Repo(storage=self.storage, mst=MST.create(storage=self.storage),
                    signing_key=self.key, head=head)
        with self.assertRaises(AssertionError):
            self.storage.commit(repo, [], repo_did='did:alice')
