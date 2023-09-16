"""Unit tests for xrpc_sync.py.

TODO:
* getCheckout commit param
* getRepo earliest, latest params
"""
from io import BytesIO
from threading import Semaphore, Thread
from unittest import skip

from carbox.car import Block, read_car
import dag_cbor
from google.cloud import ndb
from google.cloud.ndb.exceptions import ContextError

from ..datastore_storage import DatastoreStorage
from ..repo import Repo, Write, writes_to_commit_ops
from .. import server
from ..storage import Action, Storage, SUBSCRIBE_REPOS_NSID
from .. import util
from ..util import dag_cbor_cid, int_to_tid, next_tid
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
        self.assertEqual(self.data, load(blocks))

    @skip
    def test_lists_hosted_repos_in_order_of_creation(self):
        resp = xrpc_sync.list_repos({})
        self.assertEqual([{
            'did': 'did:web:user.com',
            'head': self.repo.head.cid.encode('base32'),
        }], resp)

    def test_get_head(self):
        resp = xrpc_sync.get_head({}, did='did:web:user.com')
        self.assertEqual({'root': self.repo.head.cid.encode('base32')}, resp)

    def test_get_latest_commit(self):
        resp = xrpc_sync.get_latest_commit({}, did='did:web:user.com')
        self.assertEqual({
            'cid': self.repo.head.cid.encode('base32'),
            'rev': '2222222222422',
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

    # based atproto/packages/pds/tests/sync/list.test.ts
    # def test_paginates_listed_hosted_repos(self):
    #     full = xrpc_sync.list_repos({})
    #     pt1 = xrpc_sync.list_repos({}, limit=2)
    #     pt2 = xrpc_sync.list_repos({}, cursor=pt1.cursor)
    #     self.assertEqual(full.repos, pt1.repos + pt2.repos)


class SubscribeReposTest(testutil.XrpcTestCase):
    def setUp(self):
        super().setUp()
        self.repo.callback = lambda commit_data: xrpc_sync.send_new_commits()

    def subscribe(self, received, delivered=None, limit=None, cursor=None):
        """subscribeRepos websocket client. Run in a thread.

        Args:
          received: list, each received message will be appended
          delivered: :class:`Semaphore`, optional, released once after receiving
            each message
          limit: integer, optional. If set, returns after receiving this many
            messages
          cursor: integer, passed to subscribeRepos
        """
        for i, (header, payload) in enumerate(
                xrpc_sync.subscribe_repos(cursor=cursor)):
            self.assertIn(header, [
                {'op': 1, 't': '#commit'},
                {'op': -1},
            ])
            received.append(payload)
            if delivered:
                delivered.release()
            if limit and i == limit - 1:
                return

    def assertCommitMessage(self, commit_msg, record=None, write=None,
                            cur=None, prev=None, seq=None):
        cur = cur or self.repo.head.cid

        blocks = commit_msg.pop('blocks')
        msg_roots, msg_blocks = read_car(blocks)
        self.assertEqual([cur], msg_roots)

        self.assertEqual({
            'repo': 'did:web:user.com',
            'commit': cur,
            'ops': [{
                'action': op.action.name.lower(),
                'path': op.path,
                'cid': op.cid,
            } for op in writes_to_commit_ops([write] if write else [])],
            'time': testutil.NOW.isoformat(),
            'seq': seq,
            'rev': int_to_tid(seq),
            # TODO
            'since': None,
            'rebase': False,
            'tooBig': False,
            'blobs': [],
        }, commit_msg)

        if record:
            record_cid = dag_cbor_cid(record)
            mst_entry = {
                'e': [{
                    'k': f'co.ll/{util._tid_last}'.encode(),
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

        commit_record = {
            'version': 3,
            'did': 'did:web:user.com',
            'data': dag_cbor_cid(mst_entry),
            'rev': int_to_tid(seq, clock_id=0),
            'prev': prev,
        }

        msg_records = [b.decoded for b in msg_blocks]
        # TODO: if I util.sign(commit_record), the sig doesn't match. why?
        for msg_record in msg_records:
            msg_record.pop('sig', None)

        self.assertIn(commit_record, msg_records)
        if record:
            self.assertIn(record, msg_records)

    def test_subscribe_repos(self):
        received_a = []
        delivered_a = Semaphore(value=0)
        subscriber_a = Thread(target=self.subscribe,
                              args=[received_a, delivered_a, 2])
        subscriber_a.start()

        # create, subscriber_a
        prev = self.repo.head.cid
        tid = next_tid()
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        self.repo.apply_writes([create])
        delivered_a.acquire()

        self.assertEqual(1, len(received_a))
        self.assertCommitMessage(received_a[0], {'foo': 'bar'}, write=create,
                                 prev=prev, seq=2)

        # update, subscriber_a and subscriber_b
        received_b = []
        delivered_b = Semaphore(value=0)
        subscriber_b = Thread(target=self.subscribe,
                              args=[received_b, delivered_b, 2])
        subscriber_b.start()

        prev = self.repo.head.cid
        update = Write(Action.UPDATE, 'co.ll', tid, {'foo': 'baz'})
        self.repo.apply_writes([update])
        delivered_a.acquire()
        delivered_b.acquire()

        self.assertEqual(2, len(received_a))
        self.assertCommitMessage(received_a[1], {'foo': 'baz'}, write=update,
                                 prev=prev, seq=3)
        self.assertEqual(1, len(received_b))
        self.assertCommitMessage(received_b[0], {'foo': 'baz'}, write=update,
                                 prev=prev, seq=3)

        subscriber_a.join()

        # update, subscriber_b
        prev = self.repo.head.cid
        delete = Write(Action.DELETE, 'co.ll', tid,)
        self.repo.apply_writes([delete])
        delivered_b.acquire()

        self.assertEqual(2, len(received_a))
        self.assertEqual(2, len(received_b))
        self.assertCommitMessage(received_b[1], write=delete, prev=prev, seq=4)

        subscriber_b.join()

    def test_subscribe_repos_cursor_zero(self):
        commit_cids = [self.repo.head.cid]
        writes = [None]
        tid = next_tid()
        for val in 'bar', 'baz', 'biff':
            write = Write(Action.CREATE if val == 'bar' else Action.UPDATE,
                          'co.ll', tid, {'foo': val})
            writes.append(write)
            commit_cid = self.repo.apply_writes([write])
            commit_cids.append(self.repo.head.cid)

        received = []
        subscriber = Thread(target=self.subscribe, args=[received],
                            kwargs={'limit': 4, 'cursor': 0})
        subscriber.start()
        subscriber.join()

        self.assertEqual(5, server.storage.allocate_seq(SUBSCRIBE_REPOS_NSID))

        self.assertCommitMessage(
            received[0], record=None, cur=commit_cids[0], prev=None, seq=1)

        for i, val in enumerate(['bar', 'baz', 'biff'], start=1):
            self.assertCommitMessage(
                received[i], {'foo': val}, cur=commit_cids[i], write=writes[i],
                prev=commit_cids[i - 1], seq=i + 1)

    def test_subscribe_repos_cursor_past_current_seq(self):
        received = []
        self.subscribe(received, cursor=999)
        self.assertEqual([({
            'error': 'FutureCursor',
            'message': 'Cursor 999 is past current sequence number 1',
        })], received)


class DatastoreXrpcSyncTest(XrpcSyncTest, testutil.DatastoreTest):
    STORAGE_CLS = DatastoreStorage


class DatastoreSubscribeReposTest(SubscribeReposTest, testutil.DatastoreTest):
    STORAGE_CLS = DatastoreStorage

    def subscribe(self, *args, **kwargs):
        try:
            ndb.context.get_context()
            super().subscribe(*args, **kwargs)
        except ContextError:
            # we're in a separate thread; make a new ndb context
            with self.ndb_client.context():
                super().subscribe(*args, **kwargs)

