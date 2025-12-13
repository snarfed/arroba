"""Unit tests for repo.py.

Heavily based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/tests/repo.test.ts

Huge thanks to the Bluesky team for working in the public, in open source, and to
Daniel Holmgren and Devin Ivy for this code specifically!
"""
import copy
from unittest.mock import ANY, call, patch

from carbox import car
import dag_cbor

from .. import firehose
from ..datastore_storage import DatastoreStorage
from ..repo import Repo, Write
from ..server import server
from ..storage import Action, CommitOp, MemoryStorage
from .. import util
from ..util import dag_cbor_cid, next_tid, verify_sig

from . import testutil
from .testutil import NOW


class RepoTest(testutil.TestCase):
    def setUp(self):
        super().setUp()
        server._validate = False
        self.repo = Repo.create(self.storage, 'did:web:user.com', handle='user.com',
                                signing_key=self.key)

    def assertCommitIs(self, commit_data, write, seq, prev_record=None):
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

        self.assertEqual([CommitOp(
            action=write.action,
            path=f'{write.collection}/{write.rkey}',
            cid=util.dag_cbor_cid(write.record) if write.record else None,
            prev_cid=prev_record,
        )], commit_data.commit.ops)

        for block in commit_data.blocks.values():
            self.assertEqual(seq, block.seq)
            self.assertEqual('did:web:user.com', block.repo)

    def test_metadata(self):
        self.assertEqual(3, self.repo.version)
        self.assertEqual('did:web:user.com', self.repo.did)

    def test_create(self):
        # setUp called Repo.create
        did = self.repo.did
        repo = self.storage.load_repo(did)
        self.assertEqual(did, repo.did)
        self.assertIsNotNone(repo.head)

        blocks = [b.decoded for b in self.storage.read_blocks_by_seq()]

        # commit
        #
        # (the order we get these two blocks is non-deterministic)
        commit, root = blocks[:2] if 'version' in blocks[0] else reversed(blocks[:2])
        commit_cid = util.dag_cbor_cid(commit)
        commit = copy.copy(commit)
        commit.pop('sig')

        self.assertEqual({
            'e': [],
            'l': None,
        }, root)
        self.assertEqual({
            'version': 3,
            'data': self.repo.mst.get_pointer(),
            'did': 'did:web:user.com',
            'prev': None,
            'rev': '2222222222322',
            # 'sig': ...
        }, commit)

        # non-commit events
        sync_root, sync_blocks = car.read_car(blocks[4]['blocks'])
        self.assertEqual([commit_cid], sync_root)
        sync_blocks[0].decoded.pop('sig', None)
        self.assertEqual([commit], [b.decoded for b in sync_blocks])

        blocks[4].pop('blocks', None)
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
        }, {
            '$type': 'com.atproto.sync.subscribeRepos#sync',
            'seq': 4,
            'did': 'did:web:user.com',
            # 'blocks': ...
            'rev': '2222222222322',
            'time': NOW.isoformat(),
        }], blocks[2:])

        self.assertEqual(
            # TODO: should be #identity, #account, #sync, #commit
            ['#commit', '#identity', '#account', '#sync'],
            [firehose.process_event(event)[0]['t']
             for event in self.storage.read_events_by_seq()])

    def test_has_a_valid_signature_to_commit(self):
        assert verify_sig(self.repo.head.decoded, self.key.public_key())

    def test_load(self):
        loaded = Repo.load(self.storage, self.repo.head.cid,
                           signing_key=self.key)
        self.assertEqual(self.repo.head, loaded.head)

    def test_load_from_storage(self):
        objs = self.random_objects(5)
        self.storage.commit(self.repo, [Write(Action.CREATE, 'co.ll', tid, obj)
                                        for tid, obj in objs.items()])

        reloaded = Repo.load(self.storage, self.repo.head.cid, signing_key=self.key)

        self.assertEqual(3, reloaded.version)
        self.assertEqual('did:web:user.com', reloaded.did)
        self.assertEqual({'co.ll': objs}, reloaded.get_contents())


class DatastoreRepoTest(RepoTest, testutil.DatastoreTest):
    """Run all of RepoTest with DatastoreStorage."""
    pass


@patch('arroba.datastore_storage.MEMCACHE_SEQUENCE_ALLOCATION', True)
@patch('arroba.datastore_storage.MEMCACHE_SEQUENCE_BATCH', 5)
@patch('arroba.datastore_storage.MEMCACHE_SEQUENCE_BUFFER', 3)
class DatastoreMemcacheSequenceAllocationRepoTest(RepoTest, testutil.DatastoreTest):
    """Run all of RepoTest with DatastoreStorage and memcache sequence allocation."""
    pass
