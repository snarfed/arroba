"""Unit tests for repo.py.

Heavily based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/tests/repo.test.ts

Huge thanks to the Bluesky team for working in the public, in open source, and to
Daniel Holmgren and Devin Ivy for this code specifically!
"""
import copy
from itertools import chain
import random

from ..repo import Action, Repo, Write
from ..storage import MemoryStorage
from ..util import next_tid
from .testutil import NOW, TestCase


class RepoTest(TestCase):

    def setUp(self):
        super().setUp()
        self.repo = Repo.create(MemoryStorage(), 'did:web:user.com', self.key)

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
        self.assertEqual(profile, self.repo.get_record('my.stuff', tid))

        profile['description'] = "I'm the best"
        repo = self.repo.apply_writes(Write(
            action=Action.UPDATE,
            collection='my.stuff',
            rkey=tid,
            record=profile,
        ), self.key)
        self.assertEqual(profile, self.repo.get_record('my.stuff', tid))

        repo = self.repo.apply_writes(Write(
            action=Action.DELETE,
            collection='my.stuff',
            rkey=tid,
        ), self.key)
        self.assertIsNone(self.repo.get_record('my.stuff', tid))

    def test_adds_content_collections(self):
        def random_obj():
            return {'foo': random.randint(0, 1000)}

        data = {
            'example.foo': [random_obj() for i in range(3)],
            # 'example.bar': [random_obj() for i in range(20)],
            # 'example.baz': [random_obj() for i in range(30)],
        }
        print(data)

        writes = chain(*([Write(Action.CREATE, coll, next_tid(), obj) for obj in objs]
                         for coll, objs in data.items()))
        self.repo.apply_writes(writes, self.key)
        self.assertEqual(data, self.repo.get_contents())

    def test_edits_and_deletes_content(self):
        edited = util.edit_repo(repo, repo_data, keypair, {
            adds: 20,
            updates: 20,
            deletes: 20,
            repo: edited.repo
        })
        contents = self.repo.get_contents()
        self.assertEqual(contents, repo_data)

    def test_has_a_valid_signature_to_commit(self):
        assert verify_commit_sig(self.repo.commit, keypair.did())

    def test_loads_from_blockstore(self):
        reloaded_repo = self.repo.load(MemoryStorage(), self.repo.cid)

        contents = reloaded_self.Repo.get_contents()
        self.assertEqual(contents, repo_data)
        self.assertEqual(self.repo.did, keypair.did())
        self.assertEqual(2, self.repo.version)
