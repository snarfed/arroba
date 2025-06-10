"""Unit tests for datastore_storage.py."""
from collections import namedtuple
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec
import dag_cbor
from google.cloud import ndb
from lexrpc import ValidationError
from multiformats import CID
from pymediainfo import MediaInfo

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
from ..util import (
    dag_cbor_cid,
    DEACTIVATED,
    InactiveRepo,
    new_key,
    next_tid,
    TOMBSTONED,
)

from . import test_repo
from .testutil import DatastoreTest, requests_response

CIDS = [
    CID.decode('bafyreie5cvv4h45feadgeuwhbcutmh6t2ceseocckahdoe6uat64zmz454'),
    CID.decode('bafyreie5737gdxlw5i64vzichcalba3z2v5n6icifvx5xytvske7mr3hpm'),
    CID.decode('bafyreibj4lsc3aqnrvphp5xmrnfoorvru4wynt6lwidqbm2623a6tatzdu'),
]
BLOB_CID = CID.decode('bafkreicqpqncshdd27sgztqgzocd3zhhqnnsv6slvzhs5uz6f57cq6lmtq')


class DatastoreStorageTest(DatastoreTest):

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

    def test_create_deactivated_repo(self):
        # rotation_key = new_key(seed=4597489735324)
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key,
                           rotation_key=self.key, status=DEACTIVATED)

        self.assertEqual(DEACTIVATED, repo.status)
        self.assertEqual(DEACTIVATED, self.storage.load_repo('did:web:user.com').status)
        self.assertEqual(DEACTIVATED, AtpRepo.get_by_id('did:web:user.com').status)

    def test_create_load_repo_no_handle(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key,
                           rotation_key=self.key)
        self.assertEqual([], AtpRepo.get_by_id('did:web:user.com').handles)
        self.assertIsNone(self.storage.load_repo('han.dull'))

    def test_tombstone_repo(self):
        repo = Repo.create(self.storage, 'did:user', signing_key=self.key)
        self.assertIsNone(AtpRepo.get_by_id('did:user').status)

        self.storage.tombstone_repo(repo)
        self.assertEqual(TOMBSTONED, repo.status)
        self.assertEqual(TOMBSTONED, AtpRepo.get_by_id('did:user').status)

    def test_load_repos(self):
        alice = Repo.create(self.storage, 'did:web:alice', signing_key=self.key)
        bob = Repo.create(self.storage, 'did:plc:bob', signing_key=self.key)
        self.storage.tombstone_repo(bob)

        got_bob, got_alice = self.storage.load_repos()
        self.assertEqual('did:web:alice', got_alice.did)
        self.assertEqual(alice.head, got_alice.head)
        self.assertIsNone(got_alice.status)

        self.assertEqual('did:plc:bob', got_bob.did)
        self.assertEqual(bob.head, got_bob.head)
        self.assertEqual('tombstoned', got_bob.status)

    def test_load_repos_after(self):
        Repo.create(self.storage, 'did:web:alice', signing_key=self.key)
        Repo.create(self.storage, 'did:plc:bob', signing_key=self.key)

        got = self.storage.load_repos(after='did:plc:bob')
        self.assertEqual(1, len(got))
        self.assertEqual('did:web:alice', got[0].did)

        got = self.storage.load_repos(after='did:web:a')
        self.assertEqual(1, len(got))
        self.assertEqual('did:web:alice', got[0].did)

        got = self.storage.load_repos(after='did:web:alice')
        self.assertEqual([], got)

    def test_load_repos_limit(self):
        Repo.create(self.storage, 'did:web:alice', signing_key=self.key)
        Repo.create(self.storage, 'did:plc:bob', signing_key=self.key)

        got = self.storage.load_repos(limit=2)
        self.assertEqual(2, len(got))

        got = self.storage.load_repos(limit=1)
        self.assertEqual(1, len(got))
        self.assertEqual('did:plc:bob', got[0].did)

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
        block = self.storage.write(repo_did='did:web:user.com', obj=data)
        self.assertEqual(data, self.storage.read(block.cid).decoded)
        self.assertTrue(self.storage.has(block.cid))

    def test_read_many(self):
        self.assertEqual({cid: None for cid in CIDS},
                         self.storage.read_many(CIDS))

        data = [{'foo': 'bar'}, {'baz': 'biff'}]
        stored = [self.storage.write(repo_did='did:web:user.com', obj=d)
                  for d in data]

        cids = [stored[0].cid, CIDS[0], stored[1].cid]
        self.assertEqual(
            {dag_cbor_cid(d): Block(decoded=d) for d in data} | {CIDS[0]: None},
            self.storage.read_many(cids))

    def test_read_blocks_by_seq(self):
        AtpSequence.allocate(SUBSCRIBE_REPOS_NSID)
        foo = self.storage.write(repo_did='did:plc:123', obj={'foo': 2})  # seq 2
        AtpSequence.allocate(SUBSCRIBE_REPOS_NSID)
        bar = self.storage.write(repo_did='did:plc:123', obj={'bar': 4})  # seq 4
        baz = self.storage.write(repo_did='did:plc:123', obj={'baz': 5})  # seq 5

        self.assertEqual([foo.cid, bar.cid, baz.cid],
                         [b.cid for b in self.storage.read_blocks_by_seq()])
        self.assertEqual([bar.cid, baz.cid],
                         [b.cid for b in self.storage.read_blocks_by_seq(start=3)])
        self.assertEqual([bar.cid, baz.cid],
                         [b.cid for b in self.storage.read_blocks_by_seq(start=4)])
        self.assertEqual([], [b.cid for b in self.storage.read_blocks_by_seq(start=6)])

    def test_read_blocks_by_seq_repo(self):
        foo = self.storage.write(repo_did='did:plc:123', obj={'foo': 2})
        bar = self.storage.write(repo_did='did:plc:456', obj={'bar': 3})
        baz = self.storage.write(repo_did='did:plc:123', obj={'baz': 4})

        self.assertEqual(
            [foo.cid, baz.cid],
            [b.cid for b in self.storage.read_blocks_by_seq(repo='did:plc:123')])
        self.assertEqual(
            [baz.cid],
            [b.cid for b in self.storage.read_blocks_by_seq(repo='did:plc:123',
                                                            start=3)])
        self.assertEqual(
            [bar.cid],
            [b.cid for b in self.storage.read_blocks_by_seq(repo='did:plc:456')])
        self.assertEqual(
            [],
            [b.cid for b in self.storage.read_blocks_by_seq(repo='did:plc:789')])

    def test_read_blocks_by_seq_no_ndb_context(self):
        AtpSequence.allocate(SUBSCRIBE_REPOS_NSID)
        block = self.storage.write(repo_did='did:plc:123', obj={'foo': 2})

        self.ndb_context.__exit__(None, None, None)
        self.assertEqual([block], list(self.storage.read_blocks_by_seq()))

    def test_read_blocks_by_seq_ndb_context_closes_while_running(self):
        AtpSequence.allocate(SUBSCRIBE_REPOS_NSID)
        blocks = [
            self.storage.write(repo_did='did:plc:123', obj={'foo': 2}),
            self.storage.write(repo_did='did:plc:123', obj={'bar': 3}),
        ]

        call = self.storage.read_blocks_by_seq()
        self.assertEqual(blocks[0], next(call))
        self.ndb_context.__exit__(None, None, None)
        self.assertEqual([blocks[1]], list(call))

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

        objs = [{
            '$type': 'app.bsky.actor.profile',
            'displayName': 'Alice',
            'description': 'hi there',
        }, {
            '$type': 'app.bsky.feed.post',
            'text': 'My original post',
            'createdAt': '2007-07-07T03:04:05.000Z',
        }]
        blocks = {dag_cbor_cid(obj): Block(decoded=obj) for obj in objs}

        # new repo with initial commit
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key)
        self.assert_same_seq(b.key.id() for b in AtpBlock.query()
                             if b.decoded.get('$type') not in (
                                 'com.atproto.sync.subscribeRepos#account',
                                 'com.atproto.sync.subscribeRepos#identity',
                                 'com.atproto.sync.subscribeRepos#sync',
                             ))

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

    def test_apply_commit_inactive_repo(self):
        repo = Repo.create(self.storage, 'did:web:user.com', signing_key=self.key,
                           status=DEACTIVATED)
        head = repo.head

        with self.assertRaises(InactiveRepo):
            self.storage.apply_commit(Repo.format_commit(repo=repo, writes=[
                Write(Action.CREATE, 'coll', next_tid(), {
                    '$type': 'app.bsky.actor.profile',
                    'displayName': 'Alice',
                    'description': 'hi there',
                })]))

        repo = self.storage.load_repo('did:web:user.com')
        self.assertEqual(head, repo.head)

    def test_create_remote_blob(self):
        mock_get = MagicMock(return_value=requests_response('blob contents', headers={
            'Content-Type': 'foo/bar',
            'Content-Length': '123',
        }))
        blob = AtpRemoteBlob.get_or_create(url='http://blob', get_fn=mock_get,
                                           max_size=456)
        mock_get.assert_called_with('http://blob', stream=True)
        self.assertEqual({
            '$type': 'blob',
            'ref': BLOB_CID,
            'mimeType': 'foo/bar',
            'size': 13,
        }, blob.as_object())

        mock_get.reset_mock()
        got = AtpRemoteBlob.get_or_create(url='http://blob')
        self.assertEqual(blob, got)
        mock_get.assert_not_called()

    def test_create_remote_blob_infer_mime_type_from_url(self):
        mock_get = MagicMock(return_value=requests_response('blob contents'))

        blob = AtpRemoteBlob.get_or_create(url='http://my/blob.png', get_fn=mock_get,
                                           max_size=456)
        mock_get.assert_called_with('http://my/blob.png', stream=True)
        self.assertEqual({
            '$type': 'blob',
            'ref': BLOB_CID,
            'mimeType': 'image/png',
            'size': 13,
        }, blob.as_object())

        mock_get.reset_mock()
        got = AtpRemoteBlob.get_or_create(url='http://my/blob.png')
        self.assertEqual(blob, got)
        mock_get.assert_not_called()

    def test_create_remote_blob_image_aspect_ratio(self):
        image_bytes = Path(__file__).with_name('keyboard.png').read_bytes()
        cid = CID.decode('bafkreicjoxic5d37v2ae3wfxkjgxtvx5hsp4ejjemaog322z5dvm7kgtvq')
        mock_get = MagicMock(return_value=requests_response(image_bytes))

        blob = AtpRemoteBlob.get_or_create(url='http://my/blob.png', get_fn=mock_get)
        mock_get.assert_called_with('http://my/blob.png', stream=True)
        self.assertEqual({
            '$type': 'blob',
            'ref': cid,
            'mimeType': 'image/png',
            'size': 9003,
        }, blob.as_object())
        self.assertEqual(21, blob.width)
        self.assertEqual(12, blob.height)

    def test_create_remote_blob_video_aspect_ratio(self):
        video_bytes = Path(__file__).with_name('video.mp4').read_bytes()
        cid = CID.decode('bafkreibgl7xrsfokhkqozokegnrmp2evy55h3tbjwy24ffbmuet3ytohz4')
        mock_get = MagicMock(return_value=requests_response(video_bytes))

        blob = AtpRemoteBlob.get_or_create(url='http://my/blob.mp4', get_fn=mock_get)
        mock_get.assert_called_with('http://my/blob.mp4', stream=True)
        self.assertEqual({
            '$type': 'blob',
            'ref': cid,
            'mimeType': 'video/mp4',
            'size': 30109,
        }, blob.as_object())
        self.assertEqual(1280, blob.width)
        self.assertEqual(720, blob.height)

    def test_create_remote_blob_default_mime_type(self):
        mock_get = MagicMock(return_value=requests_response('blob contents'))

        blob = AtpRemoteBlob.get_or_create(url='http://blob', get_fn=mock_get)
        mock_get.assert_called_with('http://blob', stream=True)
        self.assertEqual({
            '$type': 'blob',
            'ref': BLOB_CID,
            'mimeType': 'application/octet-stream',
            'size': 13,
        }, blob.as_object())

        mock_get.reset_mock()
        got = AtpRemoteBlob.get_or_create(url='http://blob')
        self.assertEqual(blob, got)
        mock_get.assert_not_called()

    def test_create_remote_blob_content_length_over_max_size(self):
        mock_get = MagicMock(return_value=requests_response('blob contents', headers={
            'Content-Type': 'foo/bar',
            'Content-Length': '123',
        }))
        with self.assertRaises(ValidationError):
            AtpRemoteBlob.get_or_create(url='http://blob', get_fn=mock_get,
                                        max_size=99)

    def test_get_or_create_local_blob_content_length_over_max_size(self):
        AtpRemoteBlob(id='http://blob', size=123, cid='').put()
        mock_get = MagicMock()

        with self.assertRaises(ValidationError):
            AtpRemoteBlob.get_or_create(url='http://blob', get_fn=mock_get,
                                        max_size=99)

        mock_get.assert_not_called()

    def test_create_blob_no_content_length_over_max_size(self):
        mock_get = MagicMock(return_value=requests_response('blob contents'))
        with self.assertRaises(ValidationError):
            AtpRemoteBlob.get_or_create(url='http://blob', get_fn=mock_get,
                                        max_size=10)

    def test_create_blob_content_type_in_accept(self):
        mock_get = MagicMock(return_value=requests_response('blob contents', headers={
            'Content-Type': 'foo/bar',
        }))

        for i in range(2):  # first time fetches, second uses datastore entity
            blob = AtpRemoteBlob.get_or_create(url='http://blob', get_fn=mock_get,
                                               accept_types=['baz/biff', 'foo/*'])
            self.assertEqual({
                '$type': 'blob',
                'ref': BLOB_CID,
                'mimeType': 'foo/bar',
                'size': 13,
            }, blob.as_object())

        mock_get.assert_called_once()

    def test_create_remote_blob_content_type_not_in_accept(self):
        mock_get = MagicMock(return_value=requests_response('blob contents', headers={
            'Content-Type': 'foo/bar',
        }))

        with self.assertRaises(ValidationError):
            AtpRemoteBlob.get_or_create(url='http://blob', get_fn=mock_get,
                                        accept_types=['baz/biff'])

        mock_get.assert_called_with('http://blob', stream=True)

    def test_create_remote_blob_content_type_missing(self):
        mock_get = MagicMock(return_value=requests_response('blob contents'))

        with self.assertRaises(ValidationError):
            AtpRemoteBlob.get_or_create(url='http://blob', get_fn=mock_get,
                                        accept_types=['baz/biff'])

        mock_get.assert_called_with('http://blob', stream=True)

    def test_create_remote_blob_content_type_missing_accept_all(self):
        mock_get = MagicMock(return_value=requests_response('blob contents'))

        blob = AtpRemoteBlob.get_or_create(url='http://blob', get_fn=mock_get,
                                           accept_types=['*/*'])
        mock_get.assert_called_with('http://blob', stream=True)
        self.assertEqual({
            '$type': 'blob',
            'ref': BLOB_CID,
            'mimeType': 'application/octet-stream',
            'size': 13,
        }, blob.as_object())

    def test_get_or_create_local_blob_content_type_not_in_accept(self):
        AtpRemoteBlob(id='http://blob', size=123, cid='', mime_type='foo/bar').put()
        mock_get = MagicMock()

        with self.assertRaises(ValidationError):
            AtpRemoteBlob.get_or_create(url='http://blob', get_fn=mock_get,
                                        accept_types=['baz/biff'])

        mock_get.assert_not_called()

    @patch.object(MediaInfo, 'parse')
    def test_create_remote_blob_video_over_max_duration(self, mock_parse):
        # Track = namedtuple('Track', ('width', 'height', 'duration'))
        track = MagicMock(width=123, height=456, duration=5* 60_000)
        mock_parse.return_value = MagicMock(video_tracks=[track])

        mock_get = MagicMock(return_value=requests_response(b'some video', headers={
            'Content-Type': 'video/mp4',
        }))
        with self.assertRaises(ValidationError):
            AtpRemoteBlob.get_or_create(url='http://blob', get_fn=mock_get)

        self.assertEqual(b'some video', mock_parse.call_args.args[0].read())

    @patch('arroba.datastore_storage._MAX_KEYPART_BYTES', 20)
    def test_create_remote_blob_truncate_url(self):
        mock_get = MagicMock(return_value=requests_response('blob contents'))
        blob = AtpRemoteBlob.get_or_create(url='http://my/long/blob.png',
                                           get_fn=mock_get)
        self.assertEqual('http://my/long/blob.', blob.key.id())
        mock_get.assert_called_with('http://my/long/blob.png', stream=True)

        got = AtpRemoteBlob.get_or_create(url='http://my/long/blob.png')
        self.assertEqual(blob, got)
