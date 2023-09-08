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
        self.storage = self.STORAGE_CLS()
        self.repo = Repo.create(self.storage, 'did:web:user.com',
                                signing_key=self.key)

    def test_metadata(self):
        self.assertEqual(2, self.repo.version)
        self.assertEqual('did:web:user.com', self.repo.did)

    def test_does_basic_operations(self):
        profile = {
            '$type': 'app.bsky.actor.profile',
            'displayName': 'Alice',
            'avatar': 'https://alice.com/alice.jpg',
            'description': None,
        }

        tid = next_tid()
        repo = self.repo.apply_writes(Write(
            action=Action.CREATE,
            collection='my.stuff',
            rkey=tid,
            record=profile,
        ), self.key)
        self.assertEqual(profile, repo.get_record('my.stuff', tid))

        reloaded = Repo.load(self.storage, cid=repo.head.cid)
        self.assertEqual(profile, reloaded.get_record('my.stuff', tid))

        profile['description'] = "I'm the best"
        repo = repo.apply_writes(Write(
            action=Action.UPDATE,
            collection='my.stuff',
            rkey=tid,
            record=profile,
        ), self.key)
        self.assertEqual(profile, repo.get_record('my.stuff', tid))

        reloaded = Repo.load(self.storage, cid=repo.head.cid)
        self.assertEqual(profile, reloaded.get_record('my.stuff', tid))

        repo = repo.apply_writes(Write(
            action=Action.DELETE,
            collection='my.stuff',
            rkey=tid,
        ), self.key)
        self.assertIsNone(repo.get_record('my.stuff', tid))

        reloaded = Repo.load(self.storage, cid=repo.head.cid)
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

        self.repo.apply_writes(writes, self.key)
        self.assertEqual(data, self.repo.get_contents())

    def test_edits_and_deletes_content(self):
        objs = list(self.random_objects(20).items())

        self.repo.apply_writes(
            [Write(Action.CREATE, 'co.ll', tid, obj) for tid, obj in objs],
            self.key)

        random.shuffle(objs)
        self.repo.apply_writes(
            [Write(Action.UPDATE, 'co.ll', tid, {'bar': 'baz'}) for tid, _ in objs],
            self.key)

        random.shuffle(objs)
        self.repo.apply_writes(
            [Write(Action.DELETE, 'co.ll', tid) for tid, _ in objs],
            self.key)

        self.assertEqual({}, self.repo.get_contents())

    def test_has_a_valid_signature_to_commit(self):
        assert verify_sig(self.repo.head.decoded, self.key.public_key())

    def test_loads_from_blockstore(self):
        objs = self.random_objects(5)
        self.repo.apply_writes(
            [Write(Action.CREATE, 'co.ll', tid, obj)
             for tid, obj in objs.items()],
            self.key)

        reloaded = Repo.load(self.storage, self.repo.head.cid)

        self.assertEqual(2, reloaded.version)
        self.assertEqual('did:web:user.com', reloaded.did)
        self.assertEqual({'co.ll': objs}, reloaded.get_contents())

    def test_callback(self):
        def assertCommitIs(commit_data, write, seq):
            mst_entry_cid = commit_data.commit.decoded['data']
            mst_entry = commit_data.blocks[mst_entry_cid].decoded

            record_cid = None
            if write.record:
                record_cid = dag_cbor_cid(write.record)
                self.assertEqual([{
                    'k': f'co.ll/{util._tid_last}'.encode(),
                    'p': 0,
                    't': None,
                    'v': record_cid,
                }], mst_entry['e'])
                self.assertEqual(write.record,
                                 commit_data.blocks[record_cid].decoded)

            self.assertEqual(writes_to_commit_ops([write]), commit_data.commit.ops)

            for block in commit_data.blocks.values():
                self.assertEqual(seq, block.seq)

        seen = []

        # create new object with callback
        self.repo.callback = lambda commit: seen.append(commit)
        tid = next_tid()
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        self.repo.apply_writes([create], self.key)

        self.assertEqual(1, len(seen))
        assertCommitIs(seen[0], create, 2)

        # update object
        update = Write(Action.UPDATE, 'co.ll', tid, {'foo': 'baz'})
        self.repo.apply_writes([update], self.key)
        self.assertEqual(2, len(seen))
        assertCommitIs(seen[1], update, 3)

        # unset callback, update again
        self.repo.callback = None
        update = Write(Action.UPDATE, 'co.ll', tid, {'biff': 0})
        self.repo.apply_writes([update], self.key)
        self.assertEqual(2, len(seen))


class DatastoreRepoTest(RepoTest, DatastoreTest):
    """Run all of RepoTest's tests with DatastoreStorage."""
    STORAGE_CLS = DatastoreStorage
