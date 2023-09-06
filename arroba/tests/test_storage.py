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
        repo = Repo.create(storage, 'did:web:user.com', self.key)
        commit_cids.append(repo.head.cid)

        tid = next_tid()
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        commit_cid = repo.apply_writes([create], self.key)
        commit_cids.append(repo.head.cid)

        delete = Write(Action.DELETE, 'co.ll', tid)
        commit_cid = repo.apply_writes([delete], self.key)
        commit_cids.append(repo.head.cid)

        self.assertEqual(commit_cids, [cd.commit.cid for cd in
                                       storage.read_commits_by_seq()])
        self.assertEqual(commit_cids[1:], [cd.commit.cid for cd in
                                           storage.read_commits_by_seq(start=2)])
