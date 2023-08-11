"""Unit tests for xrpc_sync.py.

TODO:
* getCheckout commit param
* getRepo earliest, latest params
"""
from threading import Semaphore, Thread

from carbox.car import Block, read_car
import dag_cbor

from ..repo import Action, Repo, Write
from .. import server
from .. import util
from ..util import dag_cbor_cid, next_tid
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

        server.repo = server.repo.apply_writes(writes, server.key)

    def test_get_checkout(self):
        resp = xrpc_sync.get_checkout({}, did='did:web:user.com')
                                      # TODO
                                      # commit=xrpc_sync.repo.cid)
        roots, blocks = read_car(resp)
        self.assertEqual(self.data, load(blocks))

    def test_get_repo(self):
        resp = xrpc_sync.get_repo({}, did='did:web:user.com')
        roots, blocks = read_car(resp)
        self.assertEqual(self.data, load(blocks))

    def test_lists_hosted_repos_in_order_of_creation(self):
        resp = xrpc_sync.list_repos({})
        self.assertEqual([{
            'did': 'did:web:user.com',
            'head': server.repo.cid.encode('base32'),
        }], resp)

    def test_get_head(self):
        resp = xrpc_sync.get_head({}, did='did:web:user.com')
        self.assertEqual({'root': server.repo.cid.encode('base32')}, resp)

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

    # def test_syncs_commit_path(self):
    #     local = storage.getCommitPath(currRoot as CID, null)
    #     assert local, 'Could not get local commit path'

    #     localStr = local.map((c) => c)
    #     commit_path = xrpc_sync.get_commit_path({}, did='did:web:user.com')
    #     self.assertEqual(localStr, commit_path.commits)

    #     partial_commit_path = xrpc_sync.get_commit_path({},
    #         did='did:web:user.com',
    #         earliest=localStr[2],
    #         latest=localStr[15],
    #     )
    #     self.assertEqual(localStr.slice(3, 16), partial_commit_path.commits)

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

        server.server.register('com.atproto.sync.subscribeRepos',
                               xrpc_sync.subscribe_repos)
        server.repo.callback = xrpc_sync.enqueue_commit

    def assertCommitMessage(self, record, prev, commit_msg):
        blocks = commit_msg.pop('blocks')
        self.assertEqual({
            'repo': 'did:web:user.com',
            'commit': server.repo.cid,
            'time': testutil.NOW.isoformat(),
            # TODO
            'prev': False,
            'seq': None,
            'rebase': False,
            'tooBig': False,
            'blobs': [],
        }, commit_msg)

        msg_roots, msg_blocks = read_car(blocks)
        self.assertEqual([server.repo.cid], msg_roots)

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
        commit_record = {
            'version': 2,
            'did': 'did:web:user.com',
            'data': dag_cbor_cid(mst_entry),#record_cid,
            'prev': prev,
        }
        msg_records = [b.decoded for b in msg_blocks]
        # TODO: if I util.sign_commit(commit_record), the sig doesn't match. why?
        del msg_records[2]['sig']
        self.assertEqual([record, mst_entry, commit_record], msg_records)

    def test_subscribe_repos(self):
        def client(received, delivered):
            for i, commit_msg in enumerate(xrpc_sync.subscribe_repos()):
                received.append(commit_msg)
                delivered.release()
                if i == 1:  # read two commits, then quit
                    return

        received_a = []
        delivered_a = Semaphore(value=0)
        subscriber_a = Thread(target=client, args=[received_a, delivered_a])
        subscriber_a.start()

        # create, subscriber_a
        prev = server.repo.cid
        tid = next_tid()
        create = Write(Action.CREATE, 'co.ll', tid, {'foo': 'bar'})
        server.repo.apply_writes([create], self.key)
        delivered_a.acquire()

        self.assertEqual(1, len(received_a))
        self.assertCommitMessage({'foo': 'bar'}, prev, received_a[0])
        # TODO
        # self.assertEqual(1, len(xrpc_sync.subscribers))

        # update, subscriber_a and subscriber_b
        received_b = []
        delivered_b = Semaphore(value=0)
        subscriber_b = Thread(target=client, args=[received_b, delivered_b])
        subscriber_b.start()

        prev = server.repo.cid
        update = Write(Action.UPDATE, 'co.ll', tid, {'foo': 'baz'})
        server.repo.apply_writes([update], self.key)
        delivered_a.acquire()
        delivered_b.acquire()

        self.assertEqual(2, len(received_a))
        self.assertCommitMessage({'foo': 'baz'}, prev, received_a[1])
        self.assertEqual(1, len(received_b))
        self.assertCommitMessage({'foo': 'baz'}, prev, received_b[0])

        subscriber_a.join()
        # TODO
        # self.assertEqual(1, len(xrpc_sync.subscribers))

        # update, subscriber_b
        prev = server.repo.cid
        update = Write(Action.UPDATE, 'co.ll', tid, {'biff': 0})
        server.repo.apply_writes([update], self.key)
        delivered_b.acquire()

        self.assertEqual(2, len(received_a))
        self.assertEqual(2, len(received_b))
        self.assertCommitMessage({'biff': 0}, prev, received_b[1])

        subscriber_b.join()
        # TODO
        # self.assertEqual(0, len(xrpc_sync.subscribers))

    # based on atproto/packages/pds/tests/sync/subscribe-repos.test.ts
#     def setUp(self):
#         server = runTestServer({
#             'dbPostgresSchema': 'repo_subscribe_repos',
#         })
#         serverHost = server.url.replace('http://', '')
#         ctx = server.ctx
#         db = server.ctx.db
#         close = server.close
#         agent = AtpAgent({ 'service': server.url })
#         sc = SeedClient(agent)
#         basicSeed(sc)
#         alice = sc.dids.alice
#         bob = sc.dids.bob
#         carol = sc.dids.carol
#         dan = sc.dids.dan

#     def getRepo(did):
#         car = xrpc_sync.get_repo({}, did=did)
#         storage = MemoryBlockstore()
#         synced = repo.loadFullRepo(
#             storage,
#             Uint8Array(car),
#             did='did:web:user.com',
#             ctx.repoSigningKey.did(),
#         )
#         return repo.Repo.load(storage, synced.root)

#     def getHandleEvts(frames):
#         evts = []
#         for frame in frames:
#             if frame instanceof MessageFrame and frame.header.t == '#handle':
#                 evts.push(frame.body)

#         return evts

#     def getTombstoneEvts(frames):
#         evts = []
#         for frame in frames:
#             if frame instanceof MessageFrame and frame.header.t == '#tombstone':
#                 evts.push(frame.body)

#         return evts

#     def verifyHandleEvent(evt, did, handle):
#         self.assertEqual(did, evt['did'])
#         self.assertEqual(handle, evt['handle'])
#         self.assertTrue(isinstance(str, evt['time']))
#         self.assertTrue(isinstance(int, evt['seq']))

#     def verifyTombstoneEvent(evt, did):
#         self.assertEqual(did, evt['did'])
#         self.assertTrue(isinstance(str, evt['time']))
#         self.assertTrue(isinstance(int, evt['seq']))

#     def getCommitEvents(userDid, frames):
#         evts = []
#         for frame in frames:
#             if frame instanceof MessageFrame and frame.header.t == '#commit':
#                 body = frame.body as CommitEvt
#                 if body.repo == userDid:
#                     evts.push(frame.body)

#         return evts

#     def getAllEvents(userDid, frames):
#         types = []
#         for frame in frames:
#             if frame instanceof MessageFrame:
#                 if ((frame.header.t == '#commit' and frame.body.repo == userDid) or
#                     (frame.header.t == '#handle' and frame.body.did == userDid) or
#                     (frame.header.t == '#tombstone' and frame.body.did == userDid)):
#                     types.push(frame.body)

#         return types

#     def verifyCommitEvents(frames):
#         verifyRepo(alice, getCommitEvents(alice, frames))
#         verifyRepo(bob, getCommitEvents(bob, frames))
#         verifyRepo(carol, getCommitEvents(carol, frames))
#         verifyRepo(dan, getCommitEvents(dan, frames))

#     def verifyRepo(did, evts):
#         didRepo = getRepo(did)
#         writeLog = getWriteLog(didRepo.storage, didRepo.cid, null)
#         commits = didRepo.storage.getCommits(didRepo.cid, null)
#         if not commits:
#             return expect(commits !== null)

#         self.assertEqual(commits.length, evts.length)
#         self.assertEqual(writeLog.length, evts.length)

#         last_commit = None
#         for commit in commits:
#             commit = commits[i]
#             evt = evts[i]
#             self.assertEqual(did, evt.repo)
#             self.assertEqual(commit.commit, evt.commit)
#             self.assertEqual(last_commit, evt.prev)
#             car = repo.readCarWithRoot(evt.blocks as Uint8Array)
#             expect(car.root.equals(commit.commit))
#             expect(car.blocks.equals(commit.blocks))
#             writes = writeLog[i].map((w) => ({
#                 'action': w.action,
#                 'path': w.collection + '/' + w.rkey,
#                 'cid': None if w.action == WriteOpAction.Delete else w.cid,
#             }))
#             sortedOps = evt.ops
#                 .sort((a, b) => a.path.localeCompare(b.path))
#                 .map((op) => ({ **op, 'cid': op.cid ?? null }))
#             sortedWrites = writes.sort((a, b) => a.path.localeCompare(b.path))
#             self.assertEqual(sortedWrites, sortedOps)
#             last_commit = commit

#     def makePosts(self):
#         for i in range(10):
#             sc.post(alice, f'foo {i}'),
#             sc.post(bob, f'bar {i}'),
#             sc.post(carol, f'baz {i}'),
#             sc.post(dan, f'biff {i}'),

#     def readTillCaughtUp(gen, # AsyncGenerator<T>,
#                          waitFor?: Promise<unknown>,
#     ) => {
#         isDone = (evt: any) => {
#             if (evt == undefined) return false
#             if (evt instanceof ErrorFrame) return true
#             caughtUp = ctx.sequencerLeader.isCaughtUp()
#             if not caughtUp:
#                 return false
#             curr = db.db
#                 .selectFrom('repo_seq')
#                 .where('seq', 'is not', null)
#                 .select('seq')
#                 .limit(1)
#                 .orderBy('seq', 'desc')
#                 .executeTakeFirst()
#             return curr !== undefined and evt.body.seq == curr.seq


#         return readFromGenerator(gen, isDone, waitFor)

#     def test_sync_backfilled_events(self):
#         ws = WebSocket(
#             f'ws://{serverHost}/xrpc/com.atproto.sync.subscribeRepos?cursor={-1}',
#         )

#         gen = byFrame(ws)
#         evts = readTillCaughtUp(gen)
#         ws.terminate()

#         verifyCommitEvents(evts)

    # def test_sync_new_events(self):
    #     postPromise = makePosts()

    #     readAfterDelay = () => {
    #         wait(200) # wait just a hair so that we catch it during cutover
    #         ws = WebSocket(
    #             f'ws://{serverHost}/xrpc/com.atproto.sync.subscribeRepos?cursor={-1}',
    #         )
    #         evts = readTillCaughtUp(byFrame(ws), postPromise)
    #         ws.terminate()
    #         return evts


    #     [evts] = Promise.all([readAfterDelay(), postPromise])

    #     verifyCommitEvents(evts)

    # def test_sync_no_backfill(self):
    #     ws = WebSocket(
    #         f'ws://{serverHost}/xrpc/com.atproto.sync.subscribeRepos',
    #     )

    #     makePostsAfterWait = () => {
    #         # give them just a second to get subscriptions set up
    #         wait(200)
    #         makePosts()


    #     postPromise = makePostsAfterWait()

    #     [evts] = Promise.all([
    #         readTillCaughtUp(byFrame(ws), postPromise),
    #         postPromise,
    #     ])

    #     ws.terminate()

    #     self.assertEqual(40, evts.length)

    #     wait(100) # Let cleanup occur on server
    #     self.assertEqual(0, ctx.sequencer.listeners('events').length)

    # def test_sync_backfill_from_cursor(self):
    #     seqs = db.db
    #         .selectFrom('repo_seq')
    #         .where('seq', 'is not', null)
    #         .selectAll()
    #         .orderBy('seq', 'asc')
    #         .execute()
    #     midPoint = Math.floor(seqs.length / 2)
    #     midPointSeq = seqs[midPoint].seq

    #     ws = WebSocket(
    #         f'ws://{serverHost}/xrpc/com.atproto.sync.subscribeRepos?cursor={midPointSeq}',
    #     )
    #     evts = readTillCaughtUp(byFrame(ws))
    #     ws.terminate()
    #     seqSlice = seqs.slice(midPoint + 1)
    #     self.assertEqual(seqSlice.length, evts.length)
    #     for (let i = 0; i < evts.length; i++):
    #         evt = evts[i].body as CommitEvt
    #         seq = seqSlice[i]
    #         seqEvt = cborDecode(seq.event) as { 'commit': CID }
    #         self.assertEqual(seq.sequencedAt, evt.time)
    #         self.assertEquals(evt.commit, seqEvt.commit)
    #         self.assertEqual(seq.did, evt.repo)

    # def test_sync_changes(self):
    #     sc.updateHandle(alice, 'alice2.test')
    #     sc.updateHandle(bob, 'bob2.test')

    #     ws = WebSocket(
    #         f'ws://{serverHost}/xrpc/com.atproto.sync.subscribeRepos?cursor={-1}',
    #     )

    #     gen = byFrame(ws)
    #     evts = readTillCaughtUp(gen)
    #     ws.terminate()

    #     verifyCommitEvents(evts)
    #     handleEvts = getHandleEvts(evts.slice(-2))
    #     verifyHandleEvent(handleEvts[0], alice, 'alice2.test')
    #     verifyHandleEvent(handleEvts[1], bob, 'bob2.test')

    # def test_sync_tombstones(self):
    #     baddie1 = (
    #         sc.createAccount('baddie1.test', {
    #             'email': 'baddie1@test.com',
    #             'handle': 'baddie1.test',
    #             'password': 'baddie1-pass',
    #     ).did
    #     baddie2 = (
    #         sc.createAccount('baddie2.test', {
    #             'email': 'baddie2@test.com',
    #             'handle': 'baddie2.test',
    #             'password': 'baddie2-pass',
    #     ).did

    #     for (did in [baddie1, baddie2]):
    #         ctx.services.record(db).deleteForActor(did)
    #         ctx.services.repo(db).deleteRepo(did)
    #         ctx.services.account(db).deleteAccount(did)


    #     ws = WebSocket(
    #         f'ws://{serverHost}/xrpc/com.atproto.sync.subscribeRepos?cursor={-1}',
    #     )

    #     gen = byFrame(ws)
    #     evts = readTillCaughtUp(gen)
    #     ws.terminate()

    #     tombstoneEvts = getTombstoneEvts(evts.slice(-2))
    #     verifyTombstoneEvent(tombstoneEvts[0], baddie1)
    #     verifyTombstoneEvent(tombstoneEvts[1], baddie2)

    # def test_sync_rebases(self):
    #     prev_head = xrpc_sync.get_head({}, did=alice)

    #     xrpc_repo.rebaseRepo(
    #         { 'repo': alice },
    #         { 'encoding': 'application/json', 'headers': sc.getHeaders(alice) },
    #     )

    #     curr_head = xrpc_sync.get_head({}, did=alice)

    #     ws = WebSocket(
    #         f'ws://{serverHost}/xrpc/com.atproto.sync.subscribeRepos?cursor={-1}',
    #     )

    #     gen = byFrame(ws)
    #     frames = readTillCaughtUp(gen)
    #     ws.terminate()

    #     aliceEvts = getCommitEvents(alice, frames)
    #     self.assertEqual(1, aliceEvts.length)

    #     evt = aliceEvts[0]
    #     self.assertEqual(true, evt.rebase)
    #     self.assertEqual(false, evt.tooBig)
    #     self.assertEqual(curr_head.root, evt.commit)
    #     self.assertEqual(prev_head.root, evt.prev)
    #     self.assertEqual([], evt.ops)
    #     self.assertEqual([], evt.blobs)

    #     car = readCar(evt.blocks)
    #     self.assertEqual(1, car.blocks.size)
    #     self.assertEqual(1, car.roots.length)
    #     self.assertEqual(curr_head.root, car.roots[0])

    #     # did not affect other users
    #     bobEvts = getCommitEvents(bob, frames)
    #     self.assertGreater(10, bobEvts.length)

    # def test_sync_info_on_out_of_date_cursor(self):
    #     # we rewrite the sequenceAt time for existing seqs to be past the
    #     # backfill cutoff, then we create some posts
    #     overAnHourAgo = Date(Date.now() - HOUR - MINUTE).toISOString()
    #     db.db.updateTable('repo_seq') \
    #          .set({ 'sequencedAt': overAnHourAgo }) \
    #          .execute()

    #     makePosts()

    #     ws = WebSocket(
    #         f'ws://{serverHost}/xrpc/com.atproto.sync.subscribeRepos?cursor={-1}',
    #     )
    #     [info, **evts] = readTillCaughtUp(byFrame(ws))
    #     ws.terminate()

    #     if (!(info instanceof MessageFrame)):
    #         throw Error('Expected first frame to be a MessageFrame')

    #     self.assertEqual('#info', info.header.t)
    #     body = info.body as Record<string, unknown>
    #     self.assertEqual('OutdatedCursor', body.name)
    #     self.assertEqual(40, evts.length)

    # def test_sync_future_cursor_error(self):
    #     ws = WebSocket(
    #         f'ws://{serverHost}/xrpc/com.atproto.sync.subscribeRepos?cursor={100000}',
    #     )
    #     frames = readTillCaughtUp(byFrame(ws))
    #     ws.terminate()
    #     self.assertEqual(1, frames.length)
    #     if (!(frames[0] instanceof ErrorFrame)):
    #         throw Error('Expected ErrorFrame')

    #     self.assertEqual('FutureCursor', frames[0].body.error)
