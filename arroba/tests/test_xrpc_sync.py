"""Unit tests for xrpc_sync.py."""
import copy
from datetime import datetime, timedelta
from io import BytesIO
import threading
from threading import Barrier, Event, Semaphore, Thread
import time
from unittest import skip
from unittest.mock import patch

from carbox.car import Block, read_car
import dag_cbor
from google.cloud import ndb
from google.cloud.ndb.exceptions import ContextError
from lexrpc.base import XrpcError
from lexrpc.server import Redirect
from multiformats import CID
import os

from .. import datastore_storage
from ..datastore_storage import AtpRemoteBlob, DatastoreStorage
from .. import firehose
from ..repo import Repo, Write, writes_to_commit_ops
from .. import server
from ..storage import Action, Storage, SUBSCRIBE_REPOS_NSID
from .. import util
from ..util import dag_cbor_cid, int_to_tid, next_tid, tid_to_int
from .. import xrpc_sync

from . import testutil


def load(blocks):
    """
    Args:
      root: CID
      blocks: sequence of Block

    Returns:
      dict mapping str path (collection/rkey) to JSON object
    """
    decoded = {b.cid: b.decoded for b in blocks}
    objs = {}

    for block in blocks:
        path = b''
        for entry in block.decoded.get('e', []):
            path = path[:entry['p']] + entry['k']
            objs[path.decode()] = decoded[entry['v']]

    return objs


class XrpcSyncTest(testutil.XrpcTestCase):

    def setUp(self):
        super().setUp()

        self.data = {}  # maps path to obj
        writes = []
        for coll in 'com.example.posts', 'com.example.likes':
            for rkey, obj in self.random_objects(5).items():
                writes.append(Write(Action.CREATE, coll, rkey, obj))
                self.data[f'{coll}/{rkey}'] = obj

        self.repo.apply_writes(writes)

    def test_get_checkout(self):
        resp = xrpc_sync.get_checkout({}, did='did:web:user.com')
        roots, blocks = read_car(resp)
        self.assertEqual(self.data, load(blocks))

    def test_get_repo(self):
        resp = xrpc_sync.get_repo({}, did='did:web:user.com')
        roots, blocks = read_car(resp)

        # first block is repo head commit
        commit = blocks[0].decoded
        assert isinstance(commit.pop('data'), CID)
        assert isinstance(commit.pop('prev'), CID)
        assert isinstance(commit.pop('sig'), bytes)
        self.assertEqual({
            'version': 3,
            'did': 'did:web:user.com',
            'rev': '2222222222722',
        }, commit)

        self.assertEqual(self.data, load(blocks))

    def test_get_repo_since(self):
        since = self.repo.head.seq

        # create a record
        create = Write(Action.CREATE, 'co.ll', '123', {'foo': 'bar'})
        cur = self.repo.apply_writes([create])

        resp = xrpc_sync.get_repo({}, did='did:web:user.com',
                                  since=util.int_to_tid(since))
        roots, blocks = read_car(resp)

        decoded = [b.decoded for b in blocks]
        self.assertIn(cur.head.decoded, decoded)
        self.assertIn({'foo': 'bar'}, decoded)

    def test_get_repo_not_found(self):
        with self.assertRaises(XrpcError) as cm:
            xrpc_sync.get_repo({}, did='did:unknown')

        self.assertEqual('RepoNotFound', cm.exception.name)

    def test_get_repo_tombstoned(self):
        server.storage.tombstone_repo(self.repo)

        with self.assertRaises(XrpcError) as cm:
            xrpc_sync.get_repo({}, did='did:web:user.com')

        self.assertEqual('RepoDeactivated', cm.exception.name)

    def test_get_repo_status(self):
        resp = xrpc_sync.get_repo_status({}, did='did:web:user.com')
        self.assertEqual({
            'did': 'did:web:user.com',
            'active': True,
        }, resp)

    def test_get_repo_status_not_found(self):
        with self.assertRaises(XrpcError) as cm:
            xrpc_sync.get_repo_status({}, did='did:unknown')

        self.assertEqual('RepoNotFound', cm.exception.name)

    def test_get_repo_status_tombstoned(self):
        server.storage.tombstone_repo(self.repo)

        resp = xrpc_sync.get_repo_status({}, did='did:web:user.com')
        self.assertEqual({
            'did': 'did:web:user.com',
            'active': False,
            'status': 'deactivated',
        }, resp)

    def test_list_repos(self):
        eve = Repo.create(server.storage, 'did:plc:eve', signing_key=self.key)
        server.storage.tombstone_repo(eve)

        expected_eve = {
            'did': 'did:plc:eve',
            'head': eve.head.cid.encode('base32'),
            'rev': '2222222222a22',
            'active': False,
            'status': 'deactivated',
        }
        expected_user = {
            'did': 'did:web:user.com',
            'head': self.repo.head.cid.encode('base32'),
            'rev': '2222222222722',
            'active': True,
        }

        self.assertEqual(
            {'repos': [expected_eve, expected_user]},
            xrpc_sync.list_repos({}))
        self.assertEqual(
            {'repos': [expected_eve], 'cursor': 'did:plc:eve'},
            xrpc_sync.list_repos({}, limit=1))
        self.assertEqual(
            {'repos': [expected_user]},
            xrpc_sync.list_repos({}, cursor='did:plc:eve'))
        self.assertEqual(
            {'repos': []},
            xrpc_sync.list_repos({}, cursor='did:web:user.com'))

    def test_get_head(self):
        resp = xrpc_sync.get_head({}, did='did:web:user.com')
        self.assertEqual({'root': self.repo.head.cid.encode('base32')}, resp)

    def test_get_latest_commit(self):
        resp = xrpc_sync.get_latest_commit({}, did='did:web:user.com')
        self.assertEqual({
            'cid': self.repo.head.cid.encode('base32'),
            'rev': '2222222222722',
        }, resp)

    def test_get_record(self):
        path, obj = next(iter(self.data.items()))
        coll, rkey = path.split('/')
        resp = xrpc_sync.get_record({}, did='did:web:user.com', collection=coll,
                                    rkey=rkey)

        expected = Block(decoded=obj)
        roots, blocks = read_car(resp)
        self.assertEqual([expected.cid], roots)
        self.assertEqual([expected], blocks)

    def test_get_record_not_found(self):
        with self.assertRaises(ValueError):
            resp = xrpc_sync.get_record({}, did='did:web:user.com',
                                        collection='com.example.posts', rkey='9999')

    def test_get_blocks_empty(self):
        resp = xrpc_sync.get_blocks({}, did='did:web:user.com', cids=[])
        roots, blocks = read_car(resp)
        self.assertEqual([], blocks)

    def test_get_blocks(self):
        cids = [dag_cbor_cid(record).encode('base32')
                for record in self.data.values()]
        resp = xrpc_sync.get_blocks({}, did='did:web:user.com', cids=cids)
        roots, blocks = read_car(resp)
        self.assertCountEqual(self.data.values(), [b.decoded for b in blocks])

    def test_get_blocks_not_found(self):
        cid = dag_cbor_cid(next(iter(self.data.values()))).encode('base32')

        with self.assertRaises(XrpcError) as cm:
            xrpc_sync.get_blocks({}, did='did:web:user.com', cids=[cid, 'nope'])

        self.assertEqual('BlockNotFound', cm.exception.name)

    def test_get_blocks_repo_tombstoned(self):
        server.storage.tombstone_repo(self.repo)

        with self.assertRaises(XrpcError) as cm:
            xrpc_sync.get_blocks({}, did='did:web:user.com', cids=[])

        self.assertEqual('RepoDeactivated', cm.exception.name)

    # based on atproto/packages/pds/tests/sync/sync.test.ts
    # def test_get_repo_creates_and_deletes(self):
    #     ADD_COUNT = 10
    #     DEL_COUNT = 4

    #     uris = []
    #     for i in range(ADD_COUNT):
    #         obj, uri = makePost(sc, did)
    #         repoData.setdefault(uri.collection, {})[uri.rkey] = obj
    #         uris.append(uri)

    #     # delete two that are already sync & two that have not been
    #     for i in range(DEL_COUNT):
    #         uri = uris[i * 5]
    #         agent.api.app.bsky.feed.post.delete({
    #             'repo': did,
    #             'collection': uri.collection,
    #             'rkey': uri.rkey,
    #         })
    #         del repoData[uri.collection][uri.rkey]

    #     car = xrpc_sync.get_repo({},
    #         did='did:web:user.com',
    #         earliest=currRoot,
    #     )

    #     currRepo = repo.Repo.load(storage, currRoot)
    #     synced = repo.loadDiff(
    #         currRepo,
    #         Uint8Array(car),
    #         did,
    #         ctx.repoSigningKey.did(),
    #     )
    #     self.assertEqual(ADD_COUNT + DEL_COUNT, synced.writeLog.length)
    #     ops = collapseWriteLog(synced.writeLog)
    #     # -2 because of dels of records, +2 because of dels of old records
    #     self.assertEqual(ADD_COUNT, ops.length)
    #     loaded = repo.Repo.load(storage, synced.root)
    #     contents = loaded.getContents()
    #     self.assertEqual(repoData, contents)

    #     currRoot = synced.root

    # def test_syncs_current_root(self):
    #     root = xrpc_sync.get_head({}, did='did:web:user.com')
    #     self.assertEqual(currRoot, root.root)

    # def test_syncs_commit_range(self):
    #     local = storage.getCommits(currRoot as CID, null)
    #     assert local, 'Could not get local commit path'

    #     memoryStore = MemoryBlockstore()
    #     # first we load some baseline data (needed for parsing range)
    #     first = xrpc_sync.get_repo({},
    #         did='did:web:user.com',
    #         latest=local[2].commit,
    #     )
    #     firstParsed = repo.readCar(Uint8Array(first))
    #     memoryStore.putMany(firstParsed.blocks)

    #     # then we load some commit range
    #     second = xrpc_sync.get_repo({},
    #         did='did:web:user.com',
    #         'earliest': local[2].commit,
    #         'latest': local[15].commit,
    #     )
    #     secondParsed = repo.readCar(Uint8Array(second))
    #     memoryStore.putMany(secondParsed.blocks)

    #     # then we verify we have all the commits in the range
    #     commits = memoryStore.getCommits(
    #         local[15].commit,
    #         local[2].commit,
    #     )
    #     assert commits, 'expected commits to be defined'
    #     localSlice = local.slice(2, 15)
    #     self.assertEqual(localSlice.length, commits.length)
    #     for fromRemote, fromLocal in zip(commits, localSlice):
    #         self.assertEqual(fromLocal.commit, fromRemote.commit)
    #         self.assertEqual(fromLocal.blocks, fromRemote.blocks)

    # def test_sync_a_repo_checkout(self):
    #     car = xrpc_sync.get_checkout({}, did=did)
    #     checkoutStorage = MemoryBlockstore()
    #     loaded = repo.loadCheckout(
    #         checkoutStorage,
    #         Uint8Array(car),
    #         did='did:web:user.com',
    #         ctx.repoSigningKey.did(),
    #     )
    #     self.assertEqual(repoData, loaded.contents)
    #     loadedRepo = repo.Repo.load(checkoutStorage, loaded.root)
    #     self.assertEqual(repoData, loadedRepo.getContents())

    # def test_sync_a_record_proof(self):
    #     collection = Object.keys(repoData)[0]
    #     rkey = Object.keys(repoData[collection])[0]
    #     car = xrpc_sync.get_record({},
    #         did={}, 'did:web:user.com',
    #         collection,
    #         rkey,
    #     )
    #     records = repo.verifyRecords(
    #         Uint8Array(car),
    #         did='did:web:user.com',
    #         ctx.repoSigningKey.did(),
    #     )
    #     claim = {
    #         collection,
    #         rkey,
    #         'record': repoData[collection][rkey],
    #     }

    #     self.assertEqual(1, records.length)
    #     self.assertEqual(claim.record, records[0].record)
    #     result = repo.verifyProofs(
    #         Uint8Array(car),
    #         [claim],
    #         did='did:web:user.com',
    #         ctx.repoSigningKey.did(),
    #     )
    #     self.assertEqual(1, result.verified.length)
    #     self.assertEqual(0, result.unverified.length)

    # def test_sync_a_proof_of_non(self):
    #     collection = Object.keys(repoData)[0]
    #     rkey = TID.nextStr() # rkey that doesn't exist
    #     car = xrpc_sync.get_record({},
    #         did='did:web:user.com',
    #         collection,
    #         rkey,
    #     )
    #     claim = {
    #         collection,
    #         rkey,
    #         'record': null,
    #     }

    #     result = repo.verifyProofs(
    #         Uint8Array(car),
    #         [claim],
    #         did='did:web:user.com',
    #         ctx.repoSigningKey.did(),
    #     )
    #     self.assertEqual(1, result.verified.length)
    #     self.assertEqual(0, result.unverified.length)

    # def test_sync_blocks(self):
    #     # let's just get some cids to reference
    #     collection = Object.keys(repoData)[0]
    #     rkey = Object.keys(repoData[collection])[0]
    #     proof_car = xrpc_sync.get_record({},
    #         did='did:web:user.com',
    #         collection=collection,
    #         rkey=rkey,
    #     )
    #     proof_blocks = readCar(Uint8Array(proof_car))
    #     cids = proof_blocks.blocks.entries().map((e) => e.cid)
    #     res = xrpc_sync.get_blocks({},
    #         did='did:web:user.com',
    #         cids,
    #     )
    #     car = readCar(Uint8Array(res))
    #     self.assertEqual(0, car.roots.length)
    #     expect(car.blocks.equals(proof_blocks.blocks))

    # def test_syncs_images(self):
    #     img1 = sc.uploadFile(
    #         did='did:web:user.com',
    #         'tests/image/fixtures/key-landscape-small.jpg',
    #         'image/jpeg',
    #     )
    #     img2 = sc.uploadFile(
    #         did='did:web:user.com',
    #         'tests/image/fixtures/key-portrait-small.jpg',
    #         'image/jpeg',
    #     )
    #     sc.post(did='did:web:user.com', 'blah', undefined, [img1])
    #     sc.post(did='did:web:user.com', 'blah', undefined, [img1, img2])
    #     sc.post(did='did:web:user.com', 'blah', undefined, [img2])
    #     res = xrpc_sync.get_commit_path({}, did=did)
    #     commits = res.commits
    #     blobs_for_first = xrpc_sync.list_blobs({},
    #         did='did:web:user.com',
    #         earliest=commits.at(-4),
    #         latest=commits.at(-3),
    #     )
    #     blobs_for_second = xrpc_sync.list_blobs({},
    #         did='did:web:user.com',
    #         earliest=commits.at(-3),
    #         latest=commits.at(-2),
    #     )
    #     blobs_for_third = xrpc_sync.list_blobs({},
    #         did='did:web:user.com',
    #         earliest=commits.at(-2),
    #         latest=commits.at(-1),
    #     )
    #     blobs_for_range = xrpc_sync.list_blobs({},
    #         did='did:web:user.com',
    #         earliest=commits.at(-4),
    #     )
    #     blobs_for_repo = xrpc_sync.list_blobs({},
    #         did='did:web:user.com',
    #     )
    #     cid1 = img1.image.ref
    #     cid2 = img2.image.ref

    #     self.assertEqual([cid1], blobs_for_first.cids)
    #     self.assertEqual([cid1, cid2].sort(), blobs_for_second.cids.sort())
    #     self.assertEqual([cid2], blobs_for_third.cids)
    #     self.assertEqual([cid1, cid2].sort(), blobs_for_range.cids.sort())
    #     self.assertEqual([cid1, cid2].sort(), blobs_for_repo.cids.sort())

    # def test_does_not_sync_repo_unauthed(self):
    #     # Could not find repo for DID
    #     with self.assertRaises(ValueError):
    #         xrpc_sync.get_repo({}, did=did)

    # def test_syncs_repo_to_owner_or_admin(self):
    #         assert xrpc_sync.get_repo(
    #             {}, did=did,
    #             { 'headers': { 'authorization': f'Bearer {sc.accounts[did].accessJwt}' } },
    #         )
    #
    #         assert xrpc_sync.get_repo(
    #             {}, did=did,
    #             { 'headers': { 'authorization': adminAuth() } },
    #         )

    # def test_does_not_sync_current_root_unauthed(self):
    #     # Could not find root for DID
    #     with self.assertRaises(ValueError):
    #         xrpc_sync.get_head({}, did=did)

    # def test_does_not_sync_commit_path_unauthed(self):
    #     # Could not find root for DID
    #     with self.assertRaises(ValueError):
    #         xrpc_sync.get_commit_path({}, did=did)

    # def test_does_not_sync_a_repo_checkout_unauthed(self):
    #     # Could not find root for DID
    #     with self.assertRaises(ValueError):
    #         xrpc_sync.get_checkout({}, did=did)

    # def test_does_not_sync_a_record_proof_unauthed(self):
    #     collection = Object.keys(repoData)[0]
    #     rkey = Object.keys(repoData[collection])[0]
    #     # Could not find repo for DID
    #     with self.assertRaises(ValueError):
    #         xrpc_sync.get_record({},
    #             did='did:web:user.com',
    #             collection=collection,
    #             rkey=rkey,
    #         })

    # def test_does_not_sync_blocks_unauthed(self):
    #     cid = cidForCbor({})

    #     # Could not find repo for DID
    #     with self.assertRaises(ValueError):
    #         xrpc_sync.get_blocks({},
    #             did='did:web:user.com',
    #             cids=[cid],
    #         )

    # def test_does_not_sync_images_unauthed(self):
    #     # Could not find root for DID
    #     with self.assertRaises(ValueError):
    #         xrpc_sync.list_blobs({}, did=did)

    #     # get blob
    #     image_cid = sc.posts[did].at(-1).images[0].image.ref
    #     assert image_cid

    #     # blob not found
    #     with self.assertRaises(ValueError):
    #         xrpc_sync.get_blob({},
    #             did='did:web:user.com',
    #             cid=image_cid,
    #         )

    # # atproto/packages/repo/tests/sync/checkout.test.ts
    # def test_sync_checkout_skips_existing_blocks(self):
    #     commit_path = storage.getCommitPath(repo.cid, null)
    #     assert commit_path, 'Could not get commit_path'
    #     hasGenesisCommit = syncStorage.has(commit_path[0])
    #     self.assertFalse(hasGenesisCommit)

    # def test_does_not_sync_duplicate_blocks(self):
    #     carBytes = streamToBuffer(sync.getCheckout(storage, repo.cid))
    #     car = CarReader.fromBytes(carBytes)
    #     cids = CidSet()
    #     for block in car.blocks():
    #         assert not cids.has(block.cid), f'duplicate block: {block.cid}'
    #         cids.add(block.cid)

    # def test_throws_on_a_bad_signature(self):
    #     badRepo = util.addBadCommit(repo, keypair)
    #     checkoutCar = streamToBuffer(sync.getCheckout(storage, badRepo.cid))
    #     with self.assertRaises(ValueError):
    #         sync.loadCheckout(syncStorage, checkoutCar, repoDid, keypair.did())

    # based on atproto/packages/pds/tests/sync/list.test.ts
    # def test_paginates_listed_hosted_repos(self):
    #     full = xrpc_sync.list_repos({})
    #     pt1 = xrpc_sync.list_repos({}, limit=2)
    #     pt2 = xrpc_sync.list_repos({}, cursor=pt1.cursor)
    #     self.assertEqual(full.repos, pt1.repos + pt2.repos)


@patch('arroba.firehose.NEW_EVENTS_TIMEOUT', timedelta(seconds=.01))
class SubscribeReposTest(testutil.XrpcTestCase):
    def setUp(self):
        super().setUp()
        self.repo.callback = lambda _: firehose.send_events()
        firehose.reset()

    def tearDown(self):
        if firehose.collector:
            firehose.collector.join(timeout=2)
            self.assertFalse(firehose.collector.is_alive())

        threads = list(threading.enumerate())
        self.assertEqual(1, len(threads), threads)

        super().tearDown()

    def subscribe(self, received, delivered=None, started=None, limit=None, cursor=None):
        """subscribeRepos websocket client. May be run in a thread.

        Args:
          received: list, each received (header, payload) tuple will be appended
          delivered: :class:`Semaphore`, optional, released once after receiving
            each message
          started: :class:`Event`, optional, notified once the thread has started
          limit: integer, optional. If set, returns after receiving this many
            messages
          cursor: integer, passed to subscribeRepos
        """
        subscription = xrpc_sync.subscribe_repos(cursor=cursor)
        if started:
            started.set()

        for i, (header, payload) in enumerate(subscription):
            self.assertIn(header, [
                {'op': 1, 't': '#account'},
                {'op': 1, 't': '#commit'},
                {'op': 1, 't': '#identity'},
                {'op': 1, 't': '#sync'},
                {'op': 1, 't': '#tombstone'},
                {'op': -1},
            ])
            received.append((header, payload))
            if delivered:
                delivered.release()
            if limit is not None and i == limit - 1:
                return

    def assertCommit(self, event, record=None, write=None, repo=None, cur=None,
                     prev=None, prev_record=None, seq=None, check_commit=True):
        """
        TODO: with check_commit=True, this doesn't currently support more than one
        create, total, in a repo, since it assumes a single-leaf MST layout below.
        fix that!

        Args:
          event ((header, payload) tuple)
          record (dict)
          write (write)
          repo (Repo)
          prev (Block): previous head commit, or None if this is the repo's
            first commit
          prev_record (CID): for updates and deletes, the previous version of the
            updated/deleted record
          seq (int)
        """
        if not repo:
            repo = self.repo
        if not cur:
            cur = repo.head.cid

        header, payload = copy.deepcopy(event)
        self.assertEqual({'op': 1, 't': '#commit'}, header)

        blocks = payload.pop('blocks')
        msg_roots, msg_blocks = read_car(blocks)
        self.assertEqual([cur], msg_roots)

        ops = []
        if write:
            ops = [{
                'action': write.action.name.lower(),
                'path': f'{write.collection}/{write.rkey}',
                'cid': (util.dag_cbor_cid(write.record)
                        if write.action in (Action.CREATE, Action.UPDATE) else None),
            }]
            if write.action in (Action.UPDATE, Action.DELETE):
                assert prev_record
                ops[0]['prev'] = prev_record

        self.assertEqual({
            'repo': repo.did,
            'commit': cur,
            'ops': ops,
            'time': testutil.NOW.isoformat(),
            'seq': seq,
            'rev': int_to_tid(seq, clock_id=0),
            # TODO
            'since': None,
            'rebase': False,
            'tooBig': False,
            'blobs': [],
            'prevData': prev.decoded['data'] if prev else None,
        }, payload)

        if record:
            record_cid = dag_cbor_cid(record)
            mst_entry = {
                'e': [{
                    'k': f'co.ll/{write.rkey}'.encode(),
                    'v': record_cid,
                    'p': 0,
                    't': None,
                }],
                'l': None,
            }
        else:
            # TODO: check mst_entry in msg_records
            # this one isn't in the delete commit in test_subscribe_repos,
            # probably since it was already sent earlier
            mst_entry = {
                'e': [],
                'l': None,
            }

        msg_records = [b.decoded for b in msg_blocks]
        # TODO: if I util.sign(commit_record), the sig doesn't match. why?
        for msg_record in msg_records:
            msg_record.pop('sig', None)

        if check_commit:
            commit_record = {
                'version': 3,
                'did': repo.did,
                'data': dag_cbor_cid(mst_entry),
                'rev': int_to_tid(seq, clock_id=0),
                'prev': prev.cid if prev else None,
            }
            self.assertIn(commit_record, msg_records)

        if record:
            self.assertIn(record, msg_records)

    def test_basic(self, *_):
        firehose.start(limit=3)

        received_a = []
        delivered_a = Semaphore(value=0)
        started_a = Event()
        subscriber_a = Thread(target=self.subscribe,
                              args=[received_a, delivered_a, started_a, 2])
        subscriber_a.start()
        started_a.wait()

        # create, subscriber_a
        prev = self.repo.head
        tid = next_tid()
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        self.repo.apply_writes([create])
        delivered_a.acquire()

        self.assertCommit(received_a[0], {'foo': 'bar'}, write=create, prev=prev, seq=5)

        # update, subscriber_a and subscriber_b
        received_b = []
        delivered_b = Semaphore(value=0)
        started_b = Event()
        subscriber_b = Thread(target=self.subscribe,
                              args=[received_b, delivered_b, started_b, 2])
        subscriber_b.start()
        started_b.wait()

        prev = self.repo.head
        prev_record = self.repo.mst.get(f'co.ll/{tid}')
        update = Write(Action.UPDATE, 'co.ll', tid, {'foo': 'baz'})
        self.repo.apply_writes([update])
        delivered_a.acquire()
        delivered_b.acquire()

        self.assertCommit(received_a[1], {'foo': 'baz'}, write=update, prev=prev,
                          prev_record=prev_record, seq=6)
        self.assertCommit(received_b[0], {'foo': 'baz'}, write=update, prev=prev,
                          prev_record=prev_record, seq=6)

        subscriber_a.join()

        # update, subscriber_b
        prev = self.repo.head
        prev_record = self.repo.mst.get(f'co.ll/{tid}')
        delete = Write(Action.DELETE, 'co.ll', tid)
        self.repo.apply_writes([delete])
        delivered_b.acquire()

        self.assertCommit(received_b[1], write=delete, seq=7, prev=prev,
                          prev_record=prev_record)

        subscriber_b.join()

    def test_delete(self, *_):
        firehose.start(limit=2)

        orig_commit = self.repo.head
        tid = next_tid()

        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        self.repo.apply_writes([create])
        after_create = self.repo.head
        record_cid = self.repo.mst.get(f'co.ll/{tid}')

        delete = Write(Action.DELETE, 'co.ll', tid)
        self.repo.apply_writes([delete])
        after_delete = self.repo.head

        received = []
        self.subscribe(received, limit=2, cursor=5)

        self.assertEqual(2, len(received))
        self.assertCommit(received[0], {'foo': 'bar'}, cur=after_create.cid,
                          write=create, prev=orig_commit, seq=5)
        self.assertCommit(received[1], cur=after_delete.cid, write=delete,
                          prev=after_create, prev_record=record_cid, seq=6)

    @patch('arroba.firehose.SUBSCRIBE_REPOS_BATCH_DELAY', timedelta(seconds=.01))
    def test_batch_delay(self, *_):
        self.test_basic()

    def test_cursor_zero(self, *_):
        orig_commit = self.repo.head
        record_cids = []
        tid = next_tid()

        writes = []   # Writes
        commits = []  # Blocks
        for val in 'bar', 'baz', 'biff':
            write = Write(Action.CREATE if val == 'bar' else Action.UPDATE,
                          'co.ll', tid, {'foo': val})
            writes.append(write)
            self.repo.apply_writes([write])
            commits.append(self.repo.head)
            record_cids.append(self.repo.mst.get(f'co.ll/{tid}'))

        firehose.start(limit=0)

        received = []
        self.subscribe(received, limit=7, cursor=0)

        self.assertEqual(8, server.storage.allocate_seq(SUBSCRIBE_REPOS_NSID))
        self.assertCommit(received[0], cur=orig_commit.cid, seq=1)

        self.assertCommit(
            received[4], {'foo': 'bar'}, cur=commits[0].cid, write=writes[0],
            prev=orig_commit, seq=5)
        self.assertCommit(
            received[5], {'foo': 'baz'}, cur=commits[1].cid, write=writes[1],
            prev=commits[0], prev_record=record_cids[0], seq=6)
        self.assertCommit(
            received[6], {'foo': 'biff'}, cur=commits[2].cid, write=writes[2],
            prev=commits[1], prev_record=record_cids[1], seq=7)

    def test_cursor_past_current_seq(self, *_):
        received = []
        self.subscribe(received, cursor=999)
        self.assertEqual([
            ({'op': -1},
             {
                 'error': 'FutureCursor',
                 'message': 'Cursor 999 is past our current sequence number 4',
             }),
        ], received)

    @patch('arroba.firehose.ROLLBACK_WINDOW', 2)
    def test_cursor_before_rollback_window(self, *_):
        while seq := server.storage.allocate_seq(SUBSCRIBE_REPOS_NSID):
            if seq >= 6:
                break
        assert seq == 6

        firehose.start(limit=1)

        write = Write(Action.CREATE, 'co.ll', next_tid(), {'foo': 'bar'})
        prev = self.repo.head
        self.repo.apply_writes([write])

        sub = iter(xrpc_sync.subscribe_repos(cursor=4))

        header, payload = next(sub)
        self.assertEqual({'op': 1, 't': '#info'}, header)
        self.assertEqual({'name': 'OutdatedCursor'}, payload)

        self.assertCommit(next(sub), {'foo': 'bar'}, write=write, seq=7, prev=prev)

    @patch('arroba.firehose.PRELOAD_WINDOW', 1)
    def test_cursor_before_preload_window(self, *_):
        commits = [self.repo.head]
        writes = []

        # two writes before starting firehose server, first will be before
        # preload window
        tid = next_tid()
        write = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        writes.append(write)
        self.repo.apply_writes(write)
        commits.append(self.repo.head)

        write = Write(Action.UPDATE, 'co.ll', tid, {'foo': 'baz'})
        writes.append(write)
        self.repo = self.repo.apply_writes(write)
        commits.append(self.repo.head)

        firehose.start(limit=1)

        # one more write
        write = Write(Action.UPDATE, 'co.ll', tid, {'foo': 'qux'})
        writes.append(write)
        self.repo = self.repo.apply_writes(write)
        commits.append(self.repo.head)

        received = []
        self.subscribe(received, limit=3, cursor=5)

        self.assertEqual(3, len(received))
        self.assertCommit(received[0], {'foo': 'bar'}, write=writes[0],
                          cur=commits[1].cid, prev=commits[0], seq=5)
        self.assertCommit(received[1], {'foo': 'baz'}, write=writes[1],
                          cur=commits[2].cid, prev=commits[1], seq=6,
                          prev_record=dag_cbor_cid({'foo': 'bar'}))
        self.assertCommit(received[2], {'foo': 'qux'}, write=writes[2],
                          cur=commits[3].cid, prev=commits[2], seq=7,
                          prev_record=dag_cbor_cid({'foo': 'baz'}))

    @patch('arroba.firehose.PRELOAD_WINDOW', 1)
    @patch('arroba.firehose.ROLLBACK_WINDOW', 4)
    def test_cursor_before_preload_window_multiple_subscribers(self, *_):
        commits = [self.repo.head]
        writes = []

        # three writes before starting firehose server, first two before
        # preload window
        for val in 'bar', 'baz', 'biff':
            write = Write(Action.CREATE, 'co.ll', next_tid(), {'foo': val})
            writes.append(write)
            self.repo.apply_writes([write])
            commits.append(self.repo.head)

        firehose.start(limit=4)

        # one more write
        write = Write(Action.CREATE, 'co.ll', next_tid(), {'foo': 'qux'})
        writes.append(write)
        self.repo.apply_writes([write])
        commits.append(self.repo.head)

        # first subscriber, one seq before preload window
        received = []
        self.subscribe(received, limit=3, cursor=6)

        self.assertEqual(3, len(received))
        self.assertCommit(received[0], {'foo': 'baz'}, write=writes[1], seq=6,
                          cur=commits[2].cid, prev=commits[1], check_commit=False)
        self.assertCommit(received[1], {'foo': 'biff'}, write=writes[2], seq=7,
                          cur=commits[3].cid, prev=commits[2], check_commit=False)
        self.assertCommit(received[2], {'foo': 'qux'}, write=writes[3], seq=8,
                          cur=commits[4].cid, prev=commits[3], check_commit=False)

        # second subscriber, two seqs before preload window
        received = []
        self.subscribe(received, limit=4, cursor=5)

        self.assertEqual(4, len(received))
        self.assertCommit(received[0], {'foo': 'bar'}, write=writes[0], seq=5,
                          cur=commits[1].cid, prev=commits[0], check_commit=False)
        self.assertCommit(received[1], {'foo': 'baz'}, write=writes[1], seq=6,
                          cur=commits[2].cid, prev=commits[1], check_commit=False)
        self.assertCommit(received[2], {'foo': 'biff'}, write=writes[2], seq=7,
                          cur=commits[3].cid, prev=commits[2], check_commit=False)
        self.assertCommit(received[3], {'foo': 'qux'}, write=writes[3], seq=8,
                          cur=commits[4].cid, prev=commits[3], check_commit=False)

        # three more writes after the first subscribers, exercise discarding
        # rollback buffers
        for val in 123, 456, 789:
            write = Write(Action.CREATE, 'co.ll', next_tid(), {'xyz': val})
            writes.append(write)
            self.repo.apply_writes([write])
            commits.append(self.repo.head)

        # final subscriber should get only the last four writes, due to
        # ROLLBACK_WINDOW=4
        received = []
        self.subscribe(received, limit=4, cursor=8)

        self.assertEqual(4, len(received))
        self.assertCommit(received[0], {'foo': 'qux'}, write=writes[3], seq=8,
                          cur=commits[4].cid, prev=commits[3], check_commit=False)
        self.assertCommit(received[1], {'xyz': 123}, write=writes[4], seq=9,
                          cur=commits[5].cid, prev=commits[4], check_commit=False)
        self.assertCommit(received[2], {'xyz': 456}, write=writes[5], seq=10,
                          cur=commits[6].cid, prev=commits[5], check_commit=False)
        self.assertCommit(received[3], {'xyz': 789}, write=writes[6], seq=11,
                          cur=commits[7].cid, prev=commits[6], check_commit=False)

    @patch('arroba.firehose.PRELOAD_WINDOW', 2)
    @patch('arroba.firehose.ROLLBACK_WINDOW', 4)
    def test_rollback_handoff(self, *_):
        # ...specifically, when our in-memory rollback window isn't fully loaded, and
        # a subscriber connects with a cursor before it, we read events starting at
        # the full rollback position inside subscribe(), then hand off to the
        # in-memory window.
        #
        # to check that we don't miss events during that handoff, we block
        # subscribe() after it's looked at the current rollback window's position,
        # then make some writes to advance the window, then let subscribe() continue.

        # 1: start firehose (this blocks until it loads preload window)
        firehose.start(limit=2)

        # 2: make subscriber block when it starts to read the full rollback
        subscriber_read_one = Barrier(2)
        subscriber_start_full_rollback = Barrier(2)

        orig_read_events_by_seq = server.storage.read_events_by_seq
        def read_events_blocking(**kwargs):
            for i, event in enumerate(orig_read_events_by_seq(**kwargs)):
                if i == 0:
                    subscriber_read_one.wait()
                elif i == 1:
                    subscriber_start_full_rollback.wait()
                yield event

        # 3: start subscriber
        with patch.object(server.storage, 'read_events_by_seq',
                          side_effect=read_events_blocking):
            received = []
            started = Event()
            subscriber = Thread(target=self.subscribe,
                                kwargs={'received': received, 'limit': 6, 'cursor': 0})
            subscriber.start()
            subscriber_read_one.wait()

        # 4: two more writes. read_events_by_seq is no longer mocked. block until the
        # firehose reads them and advances the rollback window.
        commits = [self.repo.head]
        writes = []
        for val in 'bar', 'baz':
            write = Write(Action.CREATE, 'co.ll', next_tid(), {'foo': val})
            writes.append(write)
            self.repo.apply_writes([write])
            commits.append(self.repo.head)

        firehose.collector.join()

        # 5: release subscriber, let it start reading full rollback, check that it
        # gets all five commits
        subscriber_start_full_rollback.wait()
        subscriber.join()

        self.assertEqual(6, len(received))
        self.assertCommit(received[4], {'foo': 'bar'}, write=writes[0], seq=5,
                          cur=commits[1].cid, prev=commits[0], check_commit=False)
        self.assertCommit(received[5], {'foo': 'baz'}, write=writes[1], seq=6,
                          cur=commits[2].cid, prev=commits[1], check_commit=False)

    @patch('arroba.firehose.PRELOAD_WINDOW', 1)
    @patch('arroba.firehose.ROLLBACK_WINDOW', 4)
    def test_merge_handoff_into_rollback(self, *_):
        self.repo.apply_writes([Write(Action.CREATE, 'co.ll', next_tid(), {'foo': 'bar'})])

        # collect preload window (seq 5), then stop firehose
        firehose.start(limit=0)
        firehose.collector.join()
        self.assertEqual([5], [event[1]['seq'] for event in firehose.rollback])

        # subscribe from cursor 4, check that seq 4 gets merged into rollback window
        received = []
        self.subscribe(received, limit=2, cursor=4)
        self.assertEqual([4, 5], [event[1]['seq'] for event in received])
        self.assertEqual([4, 5], [event[1]['seq'] for event in firehose.rollback])

        # subscribe from cursor 2, check that seqs 2-3 get merged into rollback window
        received = []
        self.subscribe(received, limit=4, cursor=2)
        self.assertEqual([2, 3, 4, 5], [event[1]['seq'] for event in received])
        self.assertEqual([2, 3, 4, 5], [event[1]['seq'] for event in firehose.rollback])

        # subscribing again from cursor 2 should read entirely from rollback window
        with patch.object(server.storage, 'read_events_by_seq',
                          side_effect=AssertionError('oops')):
            received = []
            self.subscribe(received, limit=4, cursor=2)
            self.assertEqual([2, 3, 4, 5], [event[1]['seq'] for event in received])

    def test_include_preexisting_record_block(self, *_):
        # https://github.com/snarfed/bridgy-fed/issues/1016#issuecomment-2109276344
        # preexisting {'foo': 'bar'} record
        other_repo = Repo.create(server.storage, 'did:web:other.com',
                                 handle='han.do', signing_key=self.key)
        other_repo.apply_writes(
            [Write(Action.CREATE, 'co.ll', next_tid(), {'foo': 'bar'})])

        # start subscriber
        firehose.start(limit=1)

        received = []
        delivered = Semaphore(value=0)
        started = Event()
        subscriber = Thread(target=self.subscribe,
                            args=[received, delivered, started, 1])
        subscriber.start()
        started.wait()

        # add the same record; subscribeRepos should include record block
        prev = self.repo.head
        second = Write(Action.CREATE, 'co.ll', next_tid(), {'foo': 'bar'})
        self.repo.apply_writes([second])
        delivered.acquire()

        self.assertEqual(1, len(received))
        self.assertCommit(received[0], {'foo': 'bar'}, write=second, prev=prev, seq=10)

        subscriber.join()

    def test_tombstoned(self, *_):
        # already mocked out, just changing its value
        firehose.NEW_EVENTS_TIMEOUT = timedelta(seconds=.5)

        firehose.start(limit=8)

        # second repo: bob
        bob_repo = Repo.create(server.storage, 'did:bob',
                               handle='bo.bb', signing_key=self.key)
        bob_repo.callback = lambda _: firehose.send_events()

        # tombstone user
        server.storage.tombstone_repo(self.repo)

        # write to bob
        prev = bob_repo.head
        tid = next_tid()
        write = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        bob_repo.apply_writes([write])

        # subscribe should serve both, from historical blocks
        received = []
        delivered = Semaphore(value=0)
        subscriber = Thread(target=self.subscribe, args=[received, delivered],
                            kwargs={'limit': 12, 'cursor': 0})
        subscriber.start()

        # first six events are initial commits and events for each repo
        for i in range(8):
            delivered.acquire()

        # tombstone
        delivered.acquire()
        header, payload = received[8]
        self.assertEqual({'op': 1, 't': '#tombstone'}, header)
        self.assertEqual({
            'seq': 9,
            'did': 'did:web:user.com',
            'time': testutil.NOW.isoformat(),
        }, payload)

        # bob's write, now from streaming
        delivered.acquire()
        self.assertCommit(received[9], {'foo': 'bar'}, write=write, repo=bob_repo,
                          prev=prev, seq=10)

        # another write to bob
        bob_repo.apply_writes([Write(Action.DELETE, 'co.ll', tid)])
        delivered.acquire()

        # now tombstone bob, served from streaming
        server.storage.tombstone_repo(bob_repo)
        delivered.acquire()

        self.assertEqual(12, len(received))
        header, payload = received[11]
        self.assertEqual({'op': 1, 't': '#tombstone'}, header)
        self.assertEqual({
            'seq': 12,
            'did': 'did:bob',
            'time': testutil.NOW.isoformat(),
        }, payload)

        subscriber.join()

    def test_skipped_seq(self, *_):
        # already mocked out, just changing its value
        firehose.NEW_EVENTS_TIMEOUT = timedelta(seconds=1)

        # https://github.com/snarfed/arroba/issues/34
        firehose.start(limit=2)

        received = []
        delivered = Semaphore(value=0)
        started = Event()
        subscriber = Thread(target=self.subscribe,
                              args=[received, delivered, started, 2])

        # prepare two writes with seqs 5 and 6
        write_5 = Write(Action.CREATE, 'co.ll', next_tid(), {'a': 'b'})
        commit_5 = Repo.format_commit(repo=self.repo, writes=[write_5])
        self.assertEqual(5, tid_to_int(commit_5.commit.decoded['rev']))

        write_6 = Write(Action.CREATE, 'co.ll', next_tid(), {'x': 'y'})
        commit_6 = Repo.format_commit(repo=self.repo, writes=[write_6])
        self.assertEqual(6, tid_to_int(commit_6.commit.decoded['rev']))

        prev = self.repo.head

        with self.assertLogs() as logs:
            subscriber.start()
            started.wait()

            # first write, skip seq 5, write with seq 6 instead
            self.repo.apply_commit(commit_6)
            head_6 = self.repo.head.cid

            # there's a small chance that this could be flaky, if >.2s elapses
            # between starting the subscriber above and receiving the second
            # write below
            time.sleep(.1)

            # shouldn't receive the event yet
            self.assertEqual(0, len(received))

            # second write, use seq 5 that we skipped above
            self.repo.apply_commit(commit_5)

            delivered.acquire()
            delivered.acquire()

        self.assertIn('INFO:arroba.firehose:Waiting for seq 5', logs.output)

        # should receive both commits
        self.assertEqual(2, len(received))
        self.assertCommit(received[0], {'a': 'b'}, write=write_5,
                          cur=self.repo.head.cid, prev=prev, seq=5)
        self.assertCommit(received[1], {'x': 'y'}, write=write_6, cur=head_6,
                          prev=prev, seq=6, check_commit=False)

        subscriber.join()

    @patch('arroba.firehose.WAIT_FOR_SKIPPED_SEQ_WINDOW', 10)
    @patch('arroba.firehose.SUBSCRIBE_REPOS_BATCH_DELAY', timedelta(seconds=.01))
    def test_dont_wait_for_old_skipped_seq(self, *_):
        # already mocked out, just changing its value
        firehose.NEW_EVENTS_TIMEOUT = timedelta(seconds=60)

        # skip seq 5, prepare commit with seq 6
        server.storage.allocate_seq(SUBSCRIBE_REPOS_NSID)
        self.assertEqual(5, server.storage.last_seq(SUBSCRIBE_REPOS_NSID))
        write_6 = Write(Action.CREATE, 'co.ll', next_tid(), {'x': 'y'})
        commit_6 = Repo.format_commit(repo=self.repo, writes=[write_6])
        self.assertEqual(6, tid_to_int(commit_6.commit.decoded['rev']))
        prev = self.repo.head

        for i in range(11):
            server.storage.allocate_seq(SUBSCRIBE_REPOS_NSID)

        firehose.start(limit=1)
        self.repo.apply_commit(commit_6)

        start = datetime.now()
        with self.assertLogs() as logs:
            received = []
            self.subscribe(received=received, limit=1, cursor=5)
        end = datetime.now()

        # shouldn't have waited
        for log in logs.output:
            self.assertNotIn('Waiting for seq', log)
        self.assertLess(end - start, timedelta(seconds=1))  # ie we didn't wait 60s

        # should receive seq 6 commits
        self.assertEqual(1, len(received))
        self.assertCommit(received[0], {'x': 'y'}, write=write_6, cur=self.repo.head.cid,
                          prev=prev, seq=6, check_commit=False)


class DatastoreXrpcSyncTest(XrpcSyncTest, testutil.DatastoreTest):
    # getBlob depends on DatastoreStorage
    def test_get_blob(self):
        cid = 'bafkreicqpqncshdd27sgztqgzocd3zhhqnnsv6slvzhs5uz6f57cq6lmtq'
        AtpRemoteBlob(id='http://blob', cid=cid, size=13).put()

        with self.assertRaises(Redirect) as r:
            resp = xrpc_sync.get_blob({}, did='did:web:user.com', cid=cid)

        self.assertEqual(301, r.exception.status)
        self.assertEqual('http://blob', r.exception.to)
        self.assertIn('Cache-Control', r.exception.headers)

    def test_get_blob_missing(self):
        with self.assertRaises(ValueError) as e:
            resp = xrpc_sync.get_blob({}, did='did:web:user.com', cid='nope')

        self.assertIn('Cache-Control', e.exception.headers)

    def test_get_blob_multiple(self):
        cid = 'bafkreicqpqncshdd27sgztqgzocd3zhhqnnsv6slvzhs5uz6f57cq6lmtq'
        now = testutil.NOW.replace(tzinfo=None)
        AtpRemoteBlob(id='http://blob/a', cid=cid, size=13, updated=now).put()
        AtpRemoteBlob(id='http://blob/b', cid=cid, size=13,
                      updated=now + timedelta(days=1)).put()

        with self.assertRaises(Redirect) as r:
            resp = xrpc_sync.get_blob({}, did='did:web:user.com', cid=cid)

        self.assertEqual(301, r.exception.status)
        self.assertEqual('http://blob/b', r.exception.to)


@patch('arroba.datastore_storage.AtpBlock.created._now',
       return_value=testutil.NOW.replace(tzinfo=None))
class DatastoreSubscribeReposTest(SubscribeReposTest, testutil.DatastoreTest):
    @patch('arroba.datastore_storage.AtpBlock.created._now',
           return_value=testutil.NOW.replace(tzinfo=None))
    def setUp(self, _):
        super().setUp()

    def subscribe(self, *args, **kwargs):
        try:
            ndb.context.get_context()
            super().subscribe(*args, **kwargs)
        except ContextError:
            # we may be in a separate thread; make a new ndb context
            with self.ndb_client.context():
                super().subscribe(*args, **kwargs)
