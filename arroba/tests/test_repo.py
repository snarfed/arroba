"""Unit tests for repo.py.

Heavily based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/tests/repo.test.ts

Huge thanks to the Bluesky team for working in the public, in open source, and to
Daniel Holmgren and Devin Ivy for this code specifically!
"""
import copy
from itertools import chain
import random

import dag_cbor

from ..server import server
from ..datastore_storage import DatastoreStorage
from ..repo import Repo, Write, writes_to_commit_ops
from ..storage import Action, CommitOp, MemoryStorage
from .. import util
from ..util import dag_cbor_cid, next_tid, verify_sig

from .testutil import DatastoreTest, NOW, TestCase


class RepoTest(TestCase):
    STORAGE_CLS = MemoryStorage

    def setUp(self):
        super().setUp()
        server._validate = False
        self.storage = self.STORAGE_CLS()
        self.repo = Repo.create(self.storage, 'did:web:user.com', handle='user.com',
                                signing_key=self.key)

    def assertCommitIs(self, commit_data, write, seq):
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

        self.assertEqual(writes_to_commit_ops([write]), commit_data.commit.ops)

        for block in commit_data.blocks.values():
            self.assertEqual(seq, block.seq)
            self.assertEqual('did:web:user.com', block.repo)

    def test_metadata(self):
        self.assertEqual(3, self.repo.version)
        self.assertEqual('did:web:user.com', self.repo.did)

    def test_create(self):
        # setUp called Repo.create
        events = list(self.storage.read_blocks_by_seq())
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
        }], [e.decoded for e in events[2:]])

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
        self.assertCommitIs(seen[0], create, 4)

        # update object
        update = Write(Action.UPDATE, 'co.ll', tid, {'foo': 'baz'})
        self.repo.apply_writes([update])
        self.assertEqual(2, len(seen))
        self.assertCommitIs(seen[1], update, 5)

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
        self.assertCommitIs(seen[0], create, 4)


class DatastoreRepoTest(RepoTest, DatastoreTest):
    """Run all of RepoTest's tests with DatastoreStorage."""
    STORAGE_CLS = DatastoreStorage
