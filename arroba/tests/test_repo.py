"""Unit tests for repo.py.

Heavily based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/tests/repo.test.ts

Huge thanks to the Bluesky team for working in the public, in open source, and to
Daniel Holmgren and Devin Ivy for this code specifically!
"""
import copy
from itertools import chain
import random

from carbox import car
import dag_cbor

from .. import firehose
from ..datastore_storage import DatastoreStorage
from ..repo import Repo, Write
from ..server import server
from ..storage import Action, CommitOp, MemoryStorage
from .. import util
from ..util import dag_cbor_cid, next_tid, verify_sig

from .testutil import DatastoreTest, NOW, TestCase


class RepoTest(TestCase):
    def setUp(self):
        super().setUp()
        server._validate = False
        self.repo = Repo.create(self.storage, 'did:web:user.com', handle='user.com',
                                signing_key=self.key)

    def assertCommitIs(self, commit_data, write, seq, prev_record=None):
        self.assertEqual(3, commit_data.commit.decoded['version'])
        self.assertEqual('did:web:user.com', commit_data.commit.decoded['did'])
        self.assertEqual(util.int_to_tid(seq, clock_id=0),
                         commit_data.commit.decoded['rev'])

        mst_entry_cid = commit_data.commit.decoded['data']
        mst_entry = commit_data.blocks[mst_entry_cid].decoded

        record_cid = None
        if write.record:
            record_cid = dag_cbor_cid(write.record)
            self.assertEqual([{
                'k': f'co.ll/{util.int_to_tid(util._tid_ts_last)}'.encode(),
                'p': 0,
                't': None,
                'v': record_cid,
            }], mst_entry['e'])
            self.assertEqual(write.record,
                             commit_data.blocks[record_cid].decoded)

        self.assertEqual([CommitOp(
            action=write.action,
            path=f'{write.collection}/{write.rkey}',
            cid=util.dag_cbor_cid(write.record) if write.record else None,
            prev_cid=prev_record,
        )], commit_data.commit.ops)

        for block in commit_data.blocks.values():
            self.assertEqual(seq, block.seq)
            self.assertEqual('did:web:user.com', block.repo)

    def test_metadata(self):
        self.assertEqual(3, self.repo.version)
        self.assertEqual('did:web:user.com', self.repo.did)

    def test_create(self):
        # setUp called Repo.create
        did = self.repo.did
        repo = self.storage.load_repo(did)
        self.assertEqual(did, repo.did)
        self.assertIsNotNone(repo.head)

        blocks = [b.decoded for b in self.storage.read_blocks_by_seq()]

        # commit
        #
        # (the order we get these two blocks is non-deterministic)
        commit, root = blocks[:2] if 'version' in blocks[0] else reversed(blocks[:2])
        commit_cid = util.dag_cbor_cid(commit)
        commit = copy.copy(commit)
        commit.pop('sig')

        self.assertEqual({
            'e': [],
            'l': None,
        }, root)
        self.assertEqual({
            'version': 3,
            'data': self.repo.mst.get_pointer(),
            'did': 'did:web:user.com',
            'prev': None,
            'rev': '2222222222322',
            # 'sig': ...
        }, commit)

        # non-commit events
        sync_root, sync_blocks = car.read_car(blocks[4]['blocks'])
        self.assertEqual([commit_cid], sync_root)
        sync_blocks[0].decoded.pop('sig', None)
        self.assertEqual([commit], [b.decoded for b in sync_blocks])

        blocks[4].pop('blocks', None)
        self.assertEqual([{
            '$type': 'com.atproto.sync.subscribeRepos#identity',
            'seq': 2,
            'did': 'did:web:user.com',
            'time': NOW.isoformat(),
            'handle': 'user.com',
        }, {
            '$type': 'com.atproto.sync.subscribeRepos#account',
            'seq': 3,
            'did': 'did:web:user.com',
            'time': NOW.isoformat(),
            'active': True,
        }, {
            '$type': 'com.atproto.sync.subscribeRepos#sync',
            'seq': 4,
            'did': 'did:web:user.com',
            # 'blocks': ...
            'rev': '2222222222322',
            'time': NOW.isoformat(),
        }], blocks[2:])

        self.assertEqual(
            # TODO: should be #identity, #account, #sync, #commit
            ['#commit', '#identity', '#account', '#sync'],
            [firehose.process_event(event)[0]['t']
             for event in self.storage.read_events_by_seq()])

    def test_basic_operations(self):
        profile = {
            '$type': 'app.bsky.actor.profile',
            'displayName': 'Alice',
            'avatar': 'https://alice.com/alice.jpg',
            'description': None,
        }

        tid = next_tid()
        self.storage.commit(self.repo, Write(
            action=Action.CREATE,
            collection='my.stuff',
            rkey=tid,
            record=profile,
        ))
        self.assertEqual(profile, self.repo.get_record('my.stuff', tid))

        self.assertEqual(profile, self.repo.get_record('my.stuff', tid))
        reloaded = Repo.load(self.storage, cid=self.repo.head.cid,
                             signing_key=self.key)
        self.assertEqual(profile, reloaded.get_record('my.stuff', tid))

        profile['description'] = "I'm the best"
        self.storage.commit(self.repo, Write(
            action=Action.UPDATE,
            collection='my.stuff',
            rkey=tid,
            record=profile,
        ))
        self.assertEqual(profile, self.repo.get_record('my.stuff', tid))

        self.assertEqual(profile, self.repo.get_record('my.stuff', tid))
        reloaded = Repo.load(self.storage, cid=self.repo.head.cid,
                             signing_key=self.key)
        self.assertEqual(profile, reloaded.get_record('my.stuff', tid))

        self.storage.commit(self.repo, Write(
            action=Action.DELETE,
            collection='my.stuff',
            rkey=tid,
        ))
        self.assertIsNone(self.repo.get_record('my.stuff', tid))

        reloaded = Repo.load(self.storage, cid=self.repo.head.cid,
                             signing_key=self.key)
        self.assertIsNone(reloaded.get_record('my.stuff', tid))

    def test_creates(self):
        data = {
            'example.foo': self.random_objects(10),
            'example.bar': self.random_objects(20),
            'example.baz': self.random_objects(30),
        }

        writes = list(chain(*(
            [Write(Action.CREATE, coll, tid, obj) for tid, obj in objs.items()]
            for coll, objs in data.items())))
        self.storage.commit(self.repo, writes)
        self.assertEqual(data, self.repo.get_contents())

    def test_updates_and_deletes(self):
        objs = list(self.random_objects(20).items())

        creates = [Write(Action.CREATE, 'co.ll', tid, obj) for tid, obj in objs]
        self.storage.commit(self.repo, creates)

        random.shuffle(objs)
        updates = [Write(Action.UPDATE, 'co.ll', tid, {'bar': 'baz'})
                   for tid, _ in objs]
        self.storage.commit(self.repo, updates)

        random.shuffle(objs)
        deletes = [Write(Action.DELETE, 'co.ll', tid) for tid, _ in objs]
        self.storage.commit(self.repo, deletes)

        self.assertEqual({}, self.repo.get_contents())

    def test_delete_with_record_no_cid_in_op(self):
        tid = next_tid()
        self.storage.commit(self.repo, Write(
            action=Action.CREATE,
            collection='my.stuff',
            rkey=tid,
            record={'x': 'y'},
        ))
        self.assertEqual({'my.stuff': {tid: {'x': 'y'}}}, self.repo.get_contents())

        self.storage.commit(self.repo, [Write(
            Action.DELETE,
            collection='my.stuff',
            rkey=tid,
            record={'x': 'y'},
        )])
        self.assertEqual({}, self.repo.get_contents())
        self.assertEqual([CommitOp(
            action=Action.DELETE,
            path=f'my.stuff/{tid}',
            cid=None,  # should be None even though the input op (wrongly) had a record
            prev_cid=util.dag_cbor_cid({'x': 'y'}),
        )], self.repo.head.ops)

    def test_noop_update_doesnt_commit(self):
        tid = next_tid()
        self.storage.commit(self.repo, [Write(Action.CREATE, 'co.ll', tid, {'x': 'y'})])
        self.storage.commit(self.repo, [Write(Action.UPDATE, 'co.ll', tid, {'x': 'y'})])
        self.assertEqual([], self.repo.head.ops)

    def test_update_nonexistent_record_raises_ValueError(self):
        update = Write(Action.UPDATE, 'co.ll', next_tid(), {'x': 'y'})
        with self.assertRaises(ValueError):
            self.storage.commit(self.repo, update)

    def test_delete_nonexistent_record_raises_ValueError(self):
        update = Write(Action.DELETE, 'co.ll', next_tid(), {'x': 'y'})
        with self.assertRaises(ValueError):
            self.storage.commit(self.repo, update)

    def test_has_a_valid_signature_to_commit(self):
        assert verify_sig(self.repo.head.decoded, self.key.public_key())

    def test_load(self):
        loaded = Repo.load(self.storage, self.repo.head.cid,
                           signing_key=self.key)
        self.assertEqual(self.repo.head, loaded.head)

    def test_load_from_storage(self):
        objs = self.random_objects(5)
        self.storage.commit(self.repo, [Write(Action.CREATE, 'co.ll', tid, obj)
                                        for tid, obj in objs.items()])

        reloaded = Repo.load(self.storage, self.repo.head.cid, signing_key=self.key)

        self.assertEqual(3, reloaded.version)
        self.assertEqual('did:web:user.com', reloaded.did)
        self.assertEqual({'co.ll': objs}, reloaded.get_contents())

    def test_commit_callback(self):
        seen = []

        # create new object with callback
        self.repo.callback = lambda commit: seen.append(commit)
        tid = next_tid()
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        self.storage.commit(self.repo, [create])

        self.assertEqual(1, len(seen))
        self.assertCommitIs(seen[0], create, 5)

        # update object
        update = Write(Action.UPDATE, 'co.ll', tid, {'foo': 'baz'})
        self.storage.commit(self.repo, [update])
        self.assertEqual(2, len(seen))
        self.assertCommitIs(seen[1], update, 6,
                            prev_record=util.dag_cbor_cid({'foo': 'bar'}))

        # unset callback, update again
        self.repo.callback = None
        update = Write(Action.UPDATE, 'co.ll', tid, {'biff': 0})
        self.storage.commit(self.repo, [update])
        self.assertEqual(2, len(seen))


class DatastoreRepoTest(RepoTest, DatastoreTest):
    """Run all of RepoTest's tests with DatastoreStorage."""
    pass
