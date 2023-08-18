"""Unit tests for ndb_storage.py."""
import os

from google.cloud import ndb
import requests

import dag_cbor
from google.auth.credentials import AnonymousCredentials
from multiformats import CID

from ..datastore_storage import (
    AtpBlock,
    AtpRepo,
    DatastoreStorage,
    WriteOnceBlobProperty,
)
from ..repo import Action, Repo, Write
from ..storage import BlockMap, CommitData, MemoryStorage
from ..util import dag_cbor_cid, next_tid

from .testutil import TestCase

os.environ.setdefault('DATASTORE_EMULATOR_HOST', 'localhost:8089')

CIDS = [
    CID.decode('bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454'),
    CID.decode('bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm'),
    CID.decode('bafyreibj4lsc3aqnrvphp5xmrnfoorvru4wynt6lwidqbm2623a6tatzdu'),
]


class DatastoreStorageTest(TestCase):
    ndb_client = ndb.Client(project='app', credentials=AnonymousCredentials())

    def setUp(self):
        super().setUp()
        self.storage = DatastoreStorage()

        # clear datastore
        requests.post(f'http://{self.ndb_client.host}/reset')

        # disable in-memory cache
        # https://github.com/googleapis/python-ndb/issues/888
        self.ndb_context = self.ndb_client.context(cache_policy=lambda key: False)
        self.ndb_context.__enter__()

    def tearDown(self):
        self.ndb_context.__exit__(None, None, None)
        super().tearDown()

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
        AtpBlock.create(data, seq=1)
        stored = AtpBlock.get_by_id(dag_cbor_cid(data).encode('base32'))
        self.assertEqual(data, stored.data)
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
        cid = self.storage.write(data)
        self.assertEqual(data, self.storage.read(cid))
        self.assertTrue(self.storage.has(cid))

    def test_read_many_read_blocks(self):
        self.assertEqual((BlockMap(), CIDS),
                         self.storage.read_many(CIDS))

        data = [{'foo': 'bar'}, {'baz': 'biff'}]
        stored = [self.storage.write(d) for d in data]

        cids = [stored[0], CIDS[0], stored[1]]
        self.assertEqual(({dag_cbor_cid(d): d for d in data}, [CIDS[0]]),
                         self.storage.read_many(cids))

        map = BlockMap()
        map.add(data[0])
        map.add(data[1])
        self.assertEqual((map, [CIDS[0]]), self.storage.read_blocks(cids))

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
            self.assertEqual(seq, block.seq)

    def test_apply_commits(self):
        self.assertEqual(0, AtpBlock.query().count())

        objs = [
            {'foo': 'bar'},
            {'baz': 'biff'},
        ]
        blocks = BlockMap()
        blocks.add(objs[0])
        blocks.add(objs[1])

        # new repo with initial commit
        repo = Repo.create(self.storage, 'did:web:user.com', self.key)
        self.assert_same_seq(b.key.id() for b in AtpBlock.query())

        # new commit
        writes = [Write(Action.CREATE, 'coll', next_tid(), obj) for obj in objs]
        commit = repo.format_commit(writes, self.key)

        self.storage.apply_commit(commit)
        self.assertEqual(commit.cid, self.storage.head)
        self.assert_same_seq(k.encode('base32') for k in commit.blocks.keys())

        repo = self.storage.load_repo(did='did:web:user.com')
        self.assertEqual('did:web:user.com', repo.did)
        self.assertEqual(commit.cid, repo.cid)

        atp_repo = AtpRepo.get_by_id('did:web:user.com')
        self.assertEqual(commit.cid, CID.decode(atp_repo.head))

        found, missing = self.storage.read_many(commit.blocks.keys())
        # found has one extra MST Data node
        self.assertEqual(4, len(found))
        self.assertIn(objs[0], found.values())
        self.assertIn(objs[1], found.values())
        commit_obj = dag_cbor.decode(commit.blocks[commit.cid])
        self.assertEqual(commit_obj, found[commit.cid])
        self.assertEqual([], missing)

        repo = self.storage.load_repo(did='did:web:user.com')
        self.assertEqual(commit.cid, repo.cid)

        atp_repo = AtpRepo.get_by_id('did:web:user.com')
        self.assertEqual(commit.cid, CID.decode(atp_repo.head))
