"""Unit tests for datastore_storage.py."""
import os
from unittest.mock import MagicMock, patch

from google.cloud import ndb

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
import dag_cbor
from multiformats import CID

from ..datastore_storage import (
    AtpBlock,
    AtpRemoteBlob,
    AtpRepo,
    AtpSequence,
    DatastoreStorage,
    WriteOnceBlobProperty,
)
from ..repo import Action, Repo, Write
from ..storage import Block, CommitData, MemoryStorage, SUBSCRIBE_REPOS_NSID
from ..util import dag_cbor_cid, new_key, next_tid

from . import test_repo
from .testutil import DatastoreTest, requests_response

CIDS = [
    CID.decode('bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454'),
    CID.decode('bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm'),
    CID.decode('bafyreibj4lsc3aqnrvphp5xmrnfoorvru4wynt6lwidqbm2623a6tatzdu'),
]


class DatastoreStorageTest(DatastoreTest):
    def store_writes(self, did):
        cids = []
        seq = AtpSequence.allocate(SUBSCRIBE_REPOS_NSID)
        cids.append(self.storage.write(repo_did=did, obj={'foo': seq}))
        seq = AtpSequence.allocate(SUBSCRIBE_REPOS_NSID)
        cids.append(self.storage.write(repo_did=did, obj={'bar': seq}))
        cids.append(self.storage.write(repo_did=did, obj={'baz': seq}))
        return cids

    def check_read_blocks(self, expected, **kwargs):
        """expected is a sequence of CID."""
        got = [b.cid for b in self.storage.read_blocks_by_seq(**kwargs)]
        self.assertEqual(expected, got)

    def test_atpsequence_allocate_new(self):
        self.assertIsNone(AtpSequence.query().get())
        self.assertEqual(1, AtpSequence.allocate('foo'))
        self.assertEqual(2, AtpSequence.get_by_id('foo').next)

    def test_atpsequence_allocate_existing(self):
        AtpSequence(id='foo', next=42).put()
        self.assertEqual(42, AtpSequence.allocate('foo'))
        self.assertEqual(43, AtpSequence.get_by_id('foo').next)

    def test_atpsequence_last_new(self):
        self.assertIsNone(AtpSequence.query().get())
        self.assertEqual(0, AtpSequence.last('foo'))
        self.assertEqual(1, AtpSequence.get_by_id('foo').next)

    def test_atpsequence_last_existing(self):
        AtpSequence(id='foo', next=42).put()
        self.assertEqual(41, AtpSequence.last('foo'))
        self.assertEqual(42, AtpSequence.get_by_id('foo').next)

    def test_create_load_repo(self):
        self.assertIsNone(self.storage.load_repo('han.dull'))
        self.assertIsNone(self.storage.load_repo('did:web:user.com'))

        rotation_key = new_key(seed=4597489735324)
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key,
                           rotation_key=rotation_key, handle='han.dull')

        self.assertEqual(repo, self.storage.load_repo('did:web:user.com'))
        self.assertEqual(repo, self.storage.load_repo('han.dull'))
        self.assertEqual('han.dull', self.storage.load_repo('han.dull').handle)

        atp_repo = AtpRepo.get_by_id('did:web:user.com')
        self.assertEqual(rotation_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ), atp_repo.rotation_key_pem)
        self.assertEqual(self.key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ), atp_repo.signing_key_pem)

    def test_create_load_repo_no_handle(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key,
                           rotation_key=self.key)
        # self.storage.create_repo(repo)
        self.assertEqual([], AtpRepo.get_by_id('did:web:user.com').handles)
        self.assertIsNone(self.storage.load_repo('han.dull'))

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
        cids = self.store_writes('did:plc:123')

        self.check_read_blocks(cids)
        self.check_read_blocks(cids[1:], start=3)
        self.check_read_blocks(cids[1:], start=4)
        self.check_read_blocks(cids[2:], start=5)
        self.check_read_blocks([], start=6)

    def test_read_blocks_by_repo(self):
        alice_cids = self.store_writes('did:plc:alice')
        self.check_read_blocks(alice_cids, repo='did:plc:alice')

        bob_cids = self.store_writes('did:plc:bob')
        self.check_read_blocks(alice_cids, repo='did:plc:alice')
        self.check_read_blocks(bob_cids, repo='did:plc:bob')

    def test_read_blocks_by_repo_seq(self):
        alice_cids = self.store_writes('did:plc:alice')
        self.check_read_blocks(alice_cids[1:], repo='did:plc:alice', start=3)
        self.check_read_blocks([], repo='did:plc:alice', start=6)

        bob_cids = self.store_writes('did:plc:bob')
        self.check_read_blocks(alice_cids[1:], repo='did:plc:alice', start=3)
        self.check_read_blocks(alice_cids[2:], repo='did:plc:alice', start=5)
        self.check_read_blocks([], repo='did:plc:alice', start=6)

        self.check_read_blocks(bob_cids[1:], repo='did:plc:bob', start=8)
        self.check_read_blocks(bob_cids[2:], repo='did:plc:bob', start=10)
        self.check_read_blocks([], repo='did:plc:bob', start=11)

    def test_read_blocks_by_repo(self):
        alice_cids = self.store_writes('did:plc:alice')
        self.check_read_blocks(alice_cids, repo='did:plc:alice')

        bob_cids = self.store_writes('did:plc:bob')
        self.check_read_blocks(alice_cids, repo='did:plc:alice')
        self.check_read_blocks(bob_cids, repo='did:plc:bob')

    def test_read_blocks_by_seq_no_ndb_context(self):
        AtpSequence.allocate(SUBSCRIBE_REPOS_NSID)
        block = self.storage.write(repo_did='did:plc:123', obj={'foo': 2})

        self.ndb_context.__exit__(None, None, None)
        self.assertEqual([block], [b.cid for b in self.storage.read_blocks_by_seq()])

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
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)
        self.assert_same_seq(b.key.id() for b in AtpBlock.query())

        # new commit
        writes = [Write(Action.CREATE, 'coll', next_tid(), obj) for obj in objs]
        commit_data = Repo.format_commit(repo=repo, writes=writes)

        self.storage.apply_commit(commit_data)
        self.assertEqual(commit_data.commit.cid, self.storage.head)
        self.assert_same_seq(k.encode('base32') for k in commit_data.blocks.keys())

        repo = self.storage.load_repo('did:web:user.com')
        self.assertEqual('did:web:user.com', repo.did)
        self.assertEqual(commit_data.commit.cid, repo.head.cid)

        atp_repo = AtpRepo.get_by_id('did:web:user.com')
        self.assertEqual(commit_data.commit.cid, CID.decode(atp_repo.head))

        found = self.storage.read_many(commit_data.blocks.keys())
        # found has one extra MST Data node
        self.assertEqual(4, len(found))
        decoded = [block.decoded for block in found.values()]
        self.assertIn(objs[0], decoded)
        self.assertIn(objs[1], decoded)
        cid = commit_data.commit.cid
        self.assertEqual(commit_data.commit.decoded, found[cid].decoded)

        repo = self.storage.load_repo('did:web:user.com')
        self.assertEqual(cid, repo.head.cid)

        atp_repo = AtpRepo.get_by_id('did:web:user.com')
        self.assertEqual(cid, CID.decode(atp_repo.head))

    def test_create_remote_blob(self):
        mock_get = MagicMock(return_value=requests_response(
            'blob contents', headers={'Content-Type': 'foo/bar'}))
        cid = CID.decode('bafkreicqpqncshdd27sgztqgzocd3zhhqnnsv6slvzhs5uz6f57cq6lmtq')

        blob = AtpRemoteBlob.get_or_create(url='http://blob', get_fn=mock_get)
        mock_get.assert_called_with('http://blob')
        self.assertEqual({
            '$type': 'blob',
            'ref': cid,
            'mimeType': 'foo/bar',
            'size': 13,
        }, blob.as_object())

        mock_get.reset_mock()
        got = AtpRemoteBlob.get_or_create(url='http://blob')
        self.assertEqual(blob, got)
        mock_get.assert_not_called()
