"""Unit tests for storage.py."""
import os

import dag_cbor
from multiformats import CID

from ..util import next_tid
from ..repo import Repo, Write
from ..storage import Action, Block, MemoryStorage

from .testutil import TestCase

DECODED = {'foo': 'bar'}
ENCODED = b'\xa1cfoocbar'
CID_ = CID.decode('bafyreiblaotetvwobe7cu2uqvnddr6ew2q3cu75qsoweulzku2egca4dxq')


class StorageTest(TestCase):
    def setUp(self):
        super().setUp()
        self.storage = MemoryStorage()

    def store_writes(self, did):
        repo = Repo.create(self.storage, did, signing_key=self.key)
        commit_cids = [repo.head.cid]

        tid = next_tid()
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        commit_cid = repo.apply_writes([create])
        commit_cids.append(repo.head.cid)

        delete = Write(Action.DELETE, 'co.ll', tid)
        commit_cid = repo.apply_writes([delete])
        commit_cids.append(repo.head.cid)

        return commit_cids

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
        did = 'did:web:user.com'
        commit_cids = self.store_writes(did)
        self.assertEqual(commit_cids, [cd.commit.cid for cd in
                                       self.storage.read_commits_by_seq(repo=did)])
        self.assertEqual(commit_cids[1:], [cd.commit.cid for cd in
                                           self.storage.read_commits_by_seq(start=2)])

    def test_read_commits_by_repo(self):
        def check(did, expected):
            got = [cd.commit.cid for cd in self.storage.read_commits_by_seq(repo=did)]
            self.assertEqual(expected, got)

        # just this repo
        user_cids = self.store_writes('did:web:user.com')
        check('did:web:user.com', user_cids)

        # another repo
        other_cids = self.store_writes('did:web:other')
        check('did:web:user.com', user_cids)
        check('did:web:other', other_cids)

    def test_read_commits_by_repo_seq(self):
        def check(did, expected, start):
            got = [cd.commit.cid for cd in
                   self.storage.read_commits_by_seq(repo=did, start=start)]
            self.assertEqual(expected[1:], got)

        # just this repo
        user_cids = self.store_writes('did:web:user.com')
        check('did:web:user.com', user_cids, 2)

        # another repo
        other_cids = self.store_writes('did:web:other')
        check('did:web:user.com', user_cids, 2)
        check('did:web:other', other_cids, 5)
