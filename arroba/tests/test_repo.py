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

from ..repo import Action, Repo, Write
from ..storage import MemoryStorage
from .. import util
from ..util import dag_cbor_cid, next_tid
from .testutil import NOW, TestCase


class RepoTest(TestCase):

    def setUp(self):
        super().setUp()
        self.storage = MemoryStorage()
        self.repo = Repo.create(self.storage, 'did:web:user.com', self.key)

    def test_metadata(self):
        self.assertEqual(2, self.repo.version)
        self.assertEqual('did:web:user.com', self.repo.did)

    async def test_does_basic_operations(self):
        profile = {
            '$type': 'app.bsky.actor.profile',
            'displayName': 'Alice',
            'avatar': 'https://alice.com/alice.jpg',
            'description': None,
        }

        tid = next_tid()
        await self.repo.apply_writes(Write(
            action=Action.CREATE,
            collection='my.stuff',
            rkey=tid,
            record=profile,
        ), self.key)
        self.assertEqual(profile, self.repo.get_record('my.stuff', tid))

        profile['description'] = "I'm the best"
        await self.repo.apply_writes(Write(
            action=Action.UPDATE,
            collection='my.stuff',
            rkey=tid,
            record=profile,
        ), self.key)
        self.assertEqual(profile, self.repo.get_record('my.stuff', tid))

        await self.repo.apply_writes(Write(
            action=Action.DELETE,
            collection='my.stuff',
            rkey=tid,
        ), self.key)
        self.assertIsNone(self.repo.get_record('my.stuff', tid))

    async def test_adds_content_collections(self):
        data = {
            'example.foo': self.random_objects(10),
            'example.bar': self.random_objects(20),
            'example.baz': self.random_objects(30),
        }

        writes = list(chain(*(
            [Write(Action.CREATE, coll, tid, obj) for tid, obj in objs.items()]
            for coll, objs in data.items())))

        await self.repo.apply_writes(writes, self.key)
        self.assertEqual(data, self.repo.get_contents())

    async def test_edits_and_deletes_content(self):
        objs = list(self.random_objects(20).items())

        await self.repo.apply_writes(
            [Write(Action.CREATE, 'co.ll', tid, obj) for tid, obj in objs],
            self.key)

        random.shuffle(objs)
        await self.repo.apply_writes(
            [Write(Action.UPDATE, 'co.ll', tid, {'bar': 'baz'}) for tid, _ in objs],
            self.key)

        random.shuffle(objs)
        await self.repo.apply_writes(
            [Write(Action.DELETE, 'co.ll', tid) for tid, _ in objs],
            self.key)

        self.assertEqual({}, self.repo.get_contents())

    def test_has_a_valid_signature_to_commit(self):
        assert util.verify_commit_sig(self.repo.commit, self.key)

    async def test_loads_from_blockstore(self):
        objs = self.random_objects(5)
        await self.repo.apply_writes(
            [Write(Action.CREATE, 'co.ll', tid, obj)
             for tid, obj in objs.items()],
            self.key)

        reloaded = Repo.load(self.storage, self.repo.cid)

        self.assertEqual(2, reloaded.version)
        self.assertEqual('did:web:user.com', reloaded.did)
        self.assertEqual({'co.ll': objs}, reloaded.get_contents())

    async def test_subscriptions(self):
        def assertCommitIs(commit_data, obj):
            commit = dag_cbor.decode(commit_data.blocks[commit_data.commit])
            mst_entry = dag_cbor.decode(commit_data.blocks[commit['data']])
            cid = dag_cbor_cid(obj)
            self.assertEqual([{
                'k': f'co.ll/{util._tid_last}'.encode(),
                'p': 0,
                't': None,
                'v': cid,
            }], mst_entry['e'])
            self.assertEqual(obj, dag_cbor.decode(commit_data.blocks[cid]))

        commits_a = []
        def callback_a(commit):
            commits_a.append(commit)

        # create new object; a is subscribed
        await self.repo.subscribe(callback_a)
        tid = next_tid()
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        await self.repo.apply_writes([create], self.key)

        self.assertEqual(1, len(commits_a))
        assertCommitIs(commits_a[0], {'foo': 'bar'})

        # update object; a and b are subscribed
        commits_b = []
        def callback_b(commit):
            commits_b.append(commit)

        await self.repo.subscribe(callback_b)
        update = Write(Action.UPDATE, 'co.ll', tid, {'foo': 'baz'})
        await self.repo.apply_writes([update], self.key)

        self.assertEqual(2, len(commits_a))
        assertCommitIs(commits_a[1], {'foo': 'baz'})
        self.assertEqual(1, len(commits_b))
        assertCommitIs(commits_b[0], {'foo': 'baz'})

        # delete object; b is subscribed
        await self.repo.unsubscribe(callback_a)
        delete = Write(Action.UPDATE, 'co.ll', tid, {'biff': 0})
        await self.repo.apply_writes([delete], self.key)

        self.assertEqual(2, len(commits_a))
        self.assertEqual(2, len(commits_b))
        assertCommitIs(commits_b[1], {'biff': 0})

