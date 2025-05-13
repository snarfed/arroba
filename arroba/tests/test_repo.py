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
from ..repo import Repo, Write, writes_to_commit_ops
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
        sync_blocks[1].decoded.pop('sig', None)
        self.assertEqual([root, commit], [b.decoded for b in sync_blocks])

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

    def test_does_basic_operations(self):
        profile = {
            '$type': 'app.bsky.actor.profile',
            'displayName': 'Alice',
            'avatar': 'https://alice.com/alice.jpg',
            'description': None,
        }

        tid = next_tid()
        self.repo.apply_writes(Write(
            action=Action.CREATE,
            collection='my.stuff',
            rkey=tid,
            record=profile,
        ))
        self.assertEqual(profile, self.repo.get_record('my.stuff', tid))

        reloaded = Repo.load(self.storage, cid=self.repo.head.cid,
                             signing_key=self.key)
        self.assertEqual(profile, reloaded.get_record('my.stuff', tid))

        profile['description'] = "I'm the best"
        self.repo.apply_writes(Write(
            action=Action.UPDATE,
            collection='my.stuff',
            rkey=tid,
            record=profile,
        ))
        self.assertEqual(profile, self.repo.get_record('my.stuff', tid))

        reloaded = Repo.load(self.storage, cid=self.repo.head.cid,
                             signing_key=self.key)
        self.assertEqual(profile, reloaded.get_record('my.stuff', tid))

        self.repo.apply_writes(Write(
            action=Action.DELETE,
            collection='my.stuff',
            rkey=tid,
        ))
        self.assertIsNone(self.repo.get_record('my.stuff', tid))

        reloaded = Repo.load(self.storage, cid=self.repo.head.cid,
                             signing_key=self.key)
        self.assertIsNone(reloaded.get_record('my.stuff', tid))

    def test_adds_content_collections(self):
        data = {
            'example.foo': self.random_objects(10),
            'example.bar': self.random_objects(20),
            'example.baz': self.random_objects(30),
        }

        writes = list(chain(*(
            [Write(Action.CREATE, coll, tid, obj) for tid, obj in objs.items()]
            for coll, objs in data.items())))

        self.repo.apply_writes(writes)
        self.assertEqual(data, self.repo.get_contents())

    def test_edits_and_deletes_content(self):
        objs = list(self.random_objects(20).items())

        self.repo.apply_writes([Write(Action.CREATE, 'co.ll', tid, obj)
                                for tid, obj in objs])

        random.shuffle(objs)
        self.repo.apply_writes([Write(Action.UPDATE, 'co.ll', tid, {'bar': 'baz'})
                                for tid, _ in objs])

        random.shuffle(objs)
        self.repo.apply_writes([Write(Action.DELETE, 'co.ll', tid)
                                for tid, _ in objs])

        self.assertEqual({}, self.repo.get_contents())

    def test_noop_update_doesnt_commit(self):
        tid = next_tid()
        self.repo.apply_writes([Write(Action.CREATE, 'co.ll', tid, {'x': 'y'})])
        self.repo.apply_writes([Write(Action.UPDATE, 'co.ll', tid, {'x': 'y'})])
        self.assertEqual([], self.repo.head.ops)

    def test_has_a_valid_signature_to_commit(self):
        assert verify_sig(self.repo.head.decoded, self.key.public_key())

    def test_load(self):
        loaded = Repo.load(self.storage, self.repo.head.cid,
                           signing_key=self.key)
        self.assertEqual(self.repo.head, loaded.head)

    def test_load_from_storage(self):
        objs = self.random_objects(5)
        self.repo.apply_writes([Write(Action.CREATE, 'co.ll', tid, obj)
                                for tid, obj in objs.items()])

        reloaded = Repo.load(self.storage, self.repo.head.cid,
                             signing_key=self.key)

        self.assertEqual(3, reloaded.version)
        self.assertEqual('did:web:user.com', reloaded.did)
        self.assertEqual({'co.ll': objs}, reloaded.get_contents())

    def test_apply_writes_callback(self):
        seen = []

        # create new object with callback
        self.repo.callback = lambda commit: seen.append(commit)
        tid = next_tid()
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        self.repo.apply_writes([create])

        self.assertEqual(1, len(seen))
        self.assertCommitIs(seen[0], create, 5)

        # update object
        update = Write(Action.UPDATE, 'co.ll', tid, {'foo': 'baz'})
        self.repo.apply_writes([update])
        self.assertEqual(2, len(seen))
        self.assertCommitIs(seen[1], update, 6,
                            prev_record=util.dag_cbor_cid({'foo': 'bar'}))

        # unset callback, update again
        self.repo.callback = None
        update = Write(Action.UPDATE, 'co.ll', tid, {'biff': 0})
        self.repo.apply_writes([update])
        self.assertEqual(2, len(seen))

    def test_apply_commit_callback(self):
        seen = []

        # create new object with callback
        self.repo.callback = lambda commit: seen.append(commit)
        create = Write(Action.CREATE, 'co.ll', next_tid(), {'foo': 'bar'})
        self.repo.apply_commit(Repo.format_commit(repo=self.repo, writes=[create]))

        self.assertEqual(1, len(seen))
        self.assertCommitIs(seen[0], create, 5)

    def test_writes_to_commit_ops(self):
        tid = next_tid()
        path = f'co.ll/{tid}'
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        foo_bar_cid = util.dag_cbor_cid({'foo': 'bar'})
        expected_create = CommitOp(action=Action.CREATE, path=path, cid=foo_bar_cid)
        self.assertEqual([expected_create], writes_to_commit_ops([create]))

        self.repo.apply_writes([create])

        foo_baz_cid = util.dag_cbor_cid({'foo': 'baz'})
        update = Write(Action.UPDATE, 'co.ll', tid, {'foo': 'baz'})
        expected_update = CommitOp(action=Action.UPDATE, path=path, cid=foo_baz_cid,
                                   prev_cid=foo_bar_cid)
        self.assertEqual([expected_update], writes_to_commit_ops([update], repo=self.repo))

        delete = Write(Action.DELETE, 'co.ll', tid)
        expected_delete = CommitOp(action=Action.DELETE, path=path, prev_cid=foo_bar_cid)
        self.assertEqual([expected_delete], writes_to_commit_ops([delete], repo=self.repo))

        # even if we set record for a delete, writes_to_commit_ops shouldn't include cid
        delete = Write(Action.DELETE, 'co.ll', tid, record={'foo': 'bar'})
        self.assertEqual([expected_delete], writes_to_commit_ops([delete], repo=self.repo))

        self.assertEqual([expected_create, expected_update, expected_delete],
                         writes_to_commit_ops([create, update, delete], repo=self.repo))



class DatastoreRepoTest(RepoTest, DatastoreTest):
    """Run all of RepoTest's tests with DatastoreStorage."""
    pass
