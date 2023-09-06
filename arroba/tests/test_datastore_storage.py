"""Unit tests for datastore_storage.py."""
import os

from google.cloud import ndb

import dag_cbor
from multiformats import CID

from ..datastore_storage import (
    AtpBlock,
    AtpRepo,
    AtpSequence,
    DatastoreStorage,
    WriteOnceBlobProperty,
)
from ..repo import Action, Repo, Write
from ..storage import Block, CommitData, MemoryStorage, SUBSCRIBE_REPOS_NSID
from ..util import dag_cbor_cid, next_tid

from . import test_repo
from .testutil import DatastoreTest

CIDS = [
    CID.decode('bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454'),
    CID.decode('bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm'),
    CID.decode('bafyreibj4lsc3aqnrvphp5xmrnfoorvru4wynt6lwidqbm2623a6tatzdu'),
]


class DatastoreStorageTest(DatastoreTest):

    def test_create_load_repo(self):
        self.assertIsNone(self.storage.load_repo(handle='han.dull'))
        self.assertIsNone(self.storage.load_repo(did='did:web:user.com'))

        repo = Repo.create(self.storage, 'did:web:user.com', key=self.key,
                           handle='han.dull')
        self.storage.create_repo(repo)

        self.assertEqual(repo, self.storage.load_repo(did='did:web:user.com'))
        self.assertEqual(repo, self.storage.load_repo(handle='han.dull'))
        self.assertEqual('han.dull', self.storage.load_repo(handle='han.dull').handle)

    def test_create_load_repo_no_handle(self):
        repo = Repo.create(self.storage, 'did:web:user.com', key=self.key)
        self.storage.create_repo(repo)
        self.assertEqual([], AtpRepo.get_by_id('did:web:user.com').handles)
        self.assertIsNone(self.storage.load_repo(handle='han.dull'))

    def test_atp_block_create(self):
        data = {'foo': 'bar'}
        AtpBlock.create(repo_did='did:web:user.com', data=data, seq=1)
        stored = AtpBlock.get_by_id(dag_cbor_cid(data).encode('base32'))
        self.assertEqual('did:web:user.com', stored.repo.id())
        self.assertEqual(data, stored.decoded)
        self.assertGreater(stored.seq, 0)

    def test_write_once(self):
        class Foo(ndb.Model):
            prop = WriteOnceBlobProperty()

        foo = Foo(prop=b'x')
        with self.assertRaises(ndb.ReadonlyPropertyError):
            foo.prop = b'y'
        with self.assertRaises(ndb.ReadonlyPropertyError):
            foo.prop = None

        foo = Foo()
        foo.prop = b'x'
        with self.assertRaises(ndb.ReadonlyPropertyError):
            foo.prop = b'y'

        foo.put()
        foo = foo.key.get()
        with self.assertRaises(ndb.ReadonlyPropertyError):
            foo.prop = b'y'

    def test_read_write_has(self):
        self.assertIsNone(self.storage.read(CIDS[0]))
        self.assertFalse(self.storage.has(CIDS[0]))

        data = {'foo': 'bar'}
        cid = self.storage.write(repo_did='did:web:user.com', obj=data)
        self.assertEqual(data, self.storage.read(cid).decoded)
        self.assertTrue(self.storage.has(cid))

    def test_read_many(self):
        self.assertEqual({cid: None for cid in CIDS},
                         self.storage.read_many(CIDS))

        data = [{'foo': 'bar'}, {'baz': 'biff'}]
        stored = [self.storage.write(repo_did='did:web:user.com', obj=d)
                  for d in data]

        cids = [stored[0], CIDS[0], stored[1]]
        self.assertEqual(
            {dag_cbor_cid(d): Block(decoded=d) for d in data} | {CIDS[0]: None},
            self.storage.read_many(cids))

    def test_read_blocks_by_seq(self):
        AtpSequence.allocate(SUBSCRIBE_REPOS_NSID)
        foo = self.storage.write(repo_did='did:plc:123', obj={'foo': 2})  # seq 2
        AtpSequence.allocate(SUBSCRIBE_REPOS_NSID)
        bar = self.storage.write(repo_did='did:plc:123', obj={'bar': 4})  # seq 4
        baz = self.storage.write(repo_did='did:plc:123', obj={'baz': 5})  # seq 5

        self.assertEqual([foo, bar, baz],
                         [b.cid for b in self.storage.read_blocks_by_seq()])
        self.assertEqual([bar, baz],
                         [b.cid for b in self.storage.read_blocks_by_seq(start=3)])
        self.assertEqual([bar, baz],
                         [b.cid for b in self.storage.read_blocks_by_seq(start=4)])
        self.assertEqual([], [b.cid for b in self.storage.read_blocks_by_seq(start=6)])

    def assert_same_seq(self, cids):
        """
        Args:
          cids: iterable of str base32 CIDs
        """
        cids = list(cids)
        assert cids
        blocks = ndb.get_multi(ndb.Key(AtpBlock, cid) for cid in cids)
        assert len(blocks) == len(cids)

        seq = blocks[0].seq
        for block in blocks[1:]:
            self.assertEqual(ndb.Key(AtpRepo, 'did:web:user.com'), block.repo)
            self.assertEqual(seq, block.seq)

    def test_apply_commit(self):
        self.assertEqual(0, AtpBlock.query().count())

        objs = [
            {'foo': 'bar'},
            {'baz': 'biff'},
        ]
        blocks = {dag_cbor_cid(obj): Block(decoded=obj) for obj in objs}

        # new repo with initial commit
        repo = Repo.create(self.storage, 'did:web:user.com', self.key)
        self.assert_same_seq(b.key.id() for b in AtpBlock.query())

        # new commit
        writes = [Write(Action.CREATE, 'coll', next_tid(), obj) for obj in objs]
        commit_data = repo.format_commit(writes, self.key)

        self.storage.apply_commit(commit_data)
        self.assertEqual(commit_data.commit.cid, self.storage.head)
        self.assert_same_seq(k.encode('base32') for k in commit_data.blocks.keys())

        repo = self.storage.load_repo(did='did:web:user.com')
        self.assertEqual('did:web:user.com', repo.did)
        self.assertEqual(commit_data.commit.cid, repo.head.cid)

        atp_repo = AtpRepo.get_by_id('did:web:user.com')
        self.assertEqual(commit_data.commit.cid, CID.decode(atp_repo.head))

        found = self.storage.read_many(commit_data.blocks.keys())
        # found has one extra MST Data node
        # TODO: it has 5 with DatastoreStorage, 4 with MemoryStorage. why?
        #
        # MemoryStorage (RepoTest):
        # (…gca4dxq, {'foo': 'bar'})
        # (…6bdyhou, {'baz': 'biff'})
        # (…4xykrgy, {'e': [{'k': b'coll/1641092645088', 'p': 0, 't': None, 'v': …6bdyhou}], 'l': …2pbjdti})
        # (…2pbjdti, {'e': [{'k': b'coll/1641092645087', 'p': 0, 't': None, 'v': …gca4dxq}], 'l': None})
        # (…yurzh4q, {'did': 'did:web:user.com', 'sig': ..., 'data': …4xykrgy, 'prev': …wv6sv24, 'version': 2})
        #
        # DatastoreStorage (DatastoreRepoTest):
        # (…gca4dxq, {'foo': 'bar'})
        # (…6bdyhou, {'baz': 'biff'})
        # (…kdx57f4, {'e': [{'k': b'coll/1641092645000', 'p': 0, 't': None, 'v': …gca4dxq}, {'k': b'1', 'p': 17, 't': None, 'v': …6bdyhou}], 'l': None})
        # (…wratxoe, {'did': 'did:web:user.com', 'sig': ..., 'data': …kdx57f4, 'prev': …gxqvj3u, 'version': 2})
        #
        # print([(f.cid, f.decoded) for f in found.values()])
        # self.assertEqual(4, len(found))
        decoded = [block.decoded for block in found.values()]
        self.assertIn(objs[0], decoded)
        self.assertIn(objs[1], decoded)
        cid = commit_data.commit.cid
        self.assertEqual(commit_data.commit.decoded, found[cid].decoded)

        repo = self.storage.load_repo(did='did:web:user.com')
        self.assertEqual(cid, repo.head.cid)

        atp_repo = AtpRepo.get_by_id('did:web:user.com')
        self.assertEqual(cid, CID.decode(atp_repo.head))
