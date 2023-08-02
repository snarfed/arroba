"""Unit tests for xrpc_repo.py.

TODO:

* paging, cursors
"""
import itertools

from arroba import xrpc_repo

from .. import server
from .. import util
from . import testutil


class XrpcRepoTest(testutil.TestCase):

    def setUp(self):
        super().setUp()
        server.init()

    def last_at_uri(self):
        return f'at://did:web:user.com/app.bsky.feed.post/{util._tid_last}'

    def test_describe_repo(self):
        with self.assertRaises(ValueError):
            xrpc_repo.describe_repo({}, repo='unknown')

        resp = xrpc_repo.describe_repo({}, repo='user.com')
        self.assertEqual('did:web:user.com', resp['did'])
        self.assertEqual('user.com', resp['handle'])

    # atproto/packages/pds/tests/crud.test.ts
    def test_create_record(self):
        resp = xrpc_repo.create_record({
            'repo': 'did:web:user.com',
            'collection': 'app.bsky.feed.post',
            'record': {
                '$type': 'app.bsky.feed.post',
                'text': 'Hello, world!',
                'createdAt': testutil.NOW.isoformat(),
            },
        })
        self.assertEqual(self.last_at_uri(), resp['uri'])

    def test_list_records(self):
        resp = xrpc_repo.list_records({}, repo='did:web:user.com',
                                      collection='app.bsky.feed.post')
        self.assertEqual([], resp['records'])

        self.test_create_record()
        resp = xrpc_repo.list_records({}, repo='did:web:user.com',
                                      collection='app.bsky.feed.post')
        self.assertEqual(1, len(resp['records']))
        self.assertEqual('Hello, world!', resp['records'][0]['value']['text'])

    def test_get_record(self):
        self.test_create_record()

        resp = xrpc_repo.get_record({},
            repo='did:web:user.com',
            collection='app.bsky.feed.post',
            rkey=str(util._tid_last),
        )
        self.assertEqual(self.last_at_uri(), resp['uri'])
        self.assertEqual('Hello, world!', resp['value']['text'])

    def test_get_record_not_found(self):
        with self.assertRaises(ValueError):
            xrpc_repo.get_record({
                'repo': 'did:web:user.com',
                'collection': 'app.bsky.feed.post',
                'rkey': '99999',
            })

    def test_delete_record(self):
        self.test_create_record()

        xrpc_repo.delete_record({
            'repo': 'did:web:user.com',
            'collection': 'app.bsky.feed.post',
            'rkey': util._tid_last,
        })
        resp = xrpc_repo.list_records({},
            repo='did:web:user.com',
            collection='app.bsky.feed.post',
        )
        self.assertEqual([], resp['records'])

    def test_delete_nonexistent_record(self):
        # noop
        xrpc_repo.delete_record({
            'repo': 'did:web:user.com',
            'collection': 'app.bsky.feed.post',
            'rkey': '9999',
        })

    # def test_cruds_records_with_the_semantic_sugars(self):
    #     res1 = aliceAgent.api.app.bsky.feed.post.create(
    #         { 'repo': 'did:web:user.com' },
    #         {
    #             '$type': 'app.bsky.feed.post',
    #             'text': 'Hello, world!',
    #             'createdAt': testutil.NOW.isoformat(),
    #         },
    #     )

    #     res2 = agent.api.app.bsky.feed.post.list({
    #         'repo': 'did:web:user.com',
    #     })
    #     self.assertEqual(1, res2.records.length)

    #     aliceAgent.api.app.bsky.feed.post.delete({
    #         'repo': 'did:web:user.com',
    #         'rkey': res1.uri.rkey,
    #     })

    #     res3 = agent.api.app.bsky.feed.post.list({
    #         'repo': 'did:web:user.com',
    #     })
    #     self.assertEqual(0, res3.records.length)

    # def test_attaches_images_to_a_post(self):
    #     file = fs.readFile('tests/image/fixtures/key-landscape-small.jpg')
    #     uploadedRes = xrpc_repo.upload_blob(file, {
    #         'encoding': 'image/jpeg',
    #     })
    #     uploaded = uploadedRes.blob

    #     # Expect blobstore not to have image yet
    #     #
    #     # BlobNotFoundError
    #     with self.assertRaises(ValueError):
    #         ctx.blobstore.getBytes(uploaded.ref)

    #     # Associate image with post, image should be placed in blobstore
    #     res = aliceAgent.api.app.bsky.feed.post.create(
    #         { 'repo': 'did:web:user.com' },
    #         {
    #             '$type': 'app.bsky.feed.post',
    #             'text': "Here's a key!",
    #             'createdAt': testutil.NOW.isoformat(),
    #             'embed': {
    #                 '$type': 'app.bsky.embed.images',
    #                 'images': [{ 'image': uploaded, 'alt': '' }],
    #             },
    #         },
    #     )

    #     # Ensure image is on post record
    #     post = aliceAgent.api.app.bsky.feed.post.get({
    #         'rkey': res.uri.rkey,
    #         'repo': 'did:web:user.com',
    #     })
    #     images = post.value.embed.images
    #     self.assertEqual(1, images.length)
    #     self.assertTrue(uploaded.ref.equals(images[0].image.ref))

    #     # Ensure that the uploaded image is now in the blobstore, i.e. doesn't
    #     # throw BlobNotFoundError
    #     ctx.blobstore.getBytes(uploaded.ref)
    #     # Cleanup
    #     aliceAgent.api.app.bsky.feed.post.delete({
    #         'rkey': res.uri.rkey,
    #         'repo': 'did:web:user.com',
    #     })

    # def test_creates_records_with_the_correct_key_described_by_the_schema(self):
    #     res = aliceAgent.api.app.bsky.actor.profile.create(
    #         { 'repo': 'did:web:user.com' },
    #         {
    #             'displayName': 'alice',
    #             'createdAt': testutil.NOW.isoformat(),
    #         },
    #     )
    #     self.assertEqual('self', res.uri.rkey)

    # def _setUp(self):
    #     def createPost(text):
    #         res = aliceAgent.api.app.bsky.feed.post.create(
    #             { 'repo': 'did:web:user.com' },
    #             {
    #                 '$type': 'app.bsky.feed.post',
    #                 text,
    #                 'createdAt': testutil.NOW.isoformat(),
    #             },
    #         )
    #         return res.uri

    #     uri1 = createPost('Post 1')
    #     uri2 = createPost('Post 2')
    #     uri3 = createPost('Post 3')
    #     uri4 = createPost('Post 4')
    #     uri5 = createPost('Post 5')

    # def test_in_forwards_order(self):
    #     resps = []
    #     cursor = None
    #     while True:
    #         resps.append(agent.api.app.bsky.feed.post.list({
    #             'repo': 'did:web:user.com',
    #             'cursor': cursor,
    #             'limit': 2,
    #         }))
    #         cursor = resp.cursor
    #         if not cursor:
    #             break

    #     for resp in resps:
    #         self.assertLessEqual(2, resp.records.length)

    #     full = agent.api.app.bsky.feed.post.list({
    #         'repo': 'did:web:user.com',
    #     })

    #     self.assertEqual(5, full.records.length)
    #     self.assertEqual(itertools.chain(r.records for r in full),
    #                      itertools.chain(r.records for r in resps))

    # def test_in_reverse_order(self):
    #     resps = []
    #     cursor = None
    #     while True:
    #         resps.append(agent.api.app.bsky.feed.post.list({
    #             'repo': 'did:web:user.com',
    #             'reverse': true,
    #             'cursor': cursor,
    #             'limit': 2,
    #         }))
    #         cursor = resp.cursor
    #         if not cursor:
    #             break

    #     for resp in resps:
    #         self.assertLessEqual(2, resp.records.length)

    #     full = agent.api.app.bsky.feed.post.list({
    #         'repo': 'did:web:user.com',
    #         'reverse': true,
    #     })

    #     self.assertEqual(5, full.records.length)
    #     self.assertEqual(itertools.chain(r.records for r in full),
    #                      itertools.chain(r.records for r in resps))

    # def test_reverses(self):
    #     forwards = agent.api.app.bsky.feed.post.list({
    #         'repo': 'did:web:user.com',
    #     })
    #     reverse = agent.api.app.bsky.feed.post.list({
    #         'repo': 'did:web:user.com',
    #         'reverse': true,
    #     })
    #     self.assertEqual(uri1.rkey, forwards.cursor)
    #     self.assertEqual(uri5.rkey, reverse.cursor)
    #     self.assertEqual(5, forwards.records.length)
    #     self.assertEqual(5, reverse.records.length)
    #     self.assertEqual(reverse.records, reversed(forwards.records))

    # def test_deletes_a_record_if_it_exists(self):
    #     data = xrpc_repo.create_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': { 'text': 'post', 'createdAt': testutil.NOW.isoformat() },
    #     })
    #     xrpc_repo.delete_record({
    #         'repo': uri.host,
    #         'collection': post.uri.collection,
    #         'rkey': post.uri.rkey,
    #     })

    #     # Could not locate record
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.get_record({
    #             repo=post.uri.host,
    #             collection=uri.collection,
    #             rkey=post.uri.rkey,
    #         })

    # def noop_if_record_doesnt_exist(self):
    #     data = xrpc_repo.create_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': { 'text': 'post', 'createdAt': testutil.NOW.isoformat() },
    #     })
    #     xrpc_repo.delete_record({
    #         'repo': post.uri.host,
    #         'collection': post.uri.collection,
    #         'rkey': post.uri.rkey,
    #     })

    #     # Could not locate record
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.get_record({
    #             repo=post.uri.host,
    #             collection=uri.collection,
    #             rkey=post.uri.rkey,
    #         })

    #     attemptDelete = xrpc_repo.delete_record({
    #         'repo': post.uri.host,
    #         'collection': post.uri.collection,
    #         'rkey': post.uri.rkey,
    #     })
    #     assert attemptDelete

    # def _setUp(self):
    #     profilePath = {
    #         'collection': ids.AppBskyActorProfile,
    #         'rkey': 'self',
    #     }

    # def test_create_new_record(self):
    #     # Could not locate record
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.get_record(**profilePath, repo=bob.did)

    #     data = xrpc_repo.put_record({
    #         **profilePath,
    #         'repo': bob.did,
    #         'record': {
    #             'displayName': 'Robert',
    #         },
    #     })
    #     self.assertEqual('at://{bob.did}/{ids.AppBskyActorProfile}/self', put.uri)

    #     data = xrpc_repo.get_record(
    #         **profilePath,
    #         repo=bob.did,
    #     )
    #     self.assertEqual({
    #         '$type': ids.AppBskyActorProfile,
    #         'displayName': 'Robert',
    #     }, profile.value)

    # def test_updates_a_record_if_it_already_exists(self):
    #     data = xrpc_repo.put_record({
    #         **profilePath,
    #         'repo': bob.did,
    #         'record': {
    #             'displayName': 'Robert',
    #             'description': 'Dog lover',
    #         },
    #     })
    #     self.assertEqual('at://{bob.did}/{ids.AppBskyActorProfile}/self', put.uri)

    #     data = xrpc_repo.get_record(
    #         **profilePath,
    #         repo=bob.did,
    #     )
    #     self.assertEqual({
    #         '$type': ids.AppBskyActorProfile,
    #         'displayName': 'Robert',
    #         'description': 'Dog lover',
    #     }, profile.value)

    # def test_fails_on_user_mismatch(self):
    #     # Authentication Required
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.put_record({
    #             'repo': bob.did,
    #             'collection': ids.AppBskyGraphFollow,
    #             'rkey': TID.nextStr(),
    #             'record': {
    #                 'subject': 'did:web:user.com',
    #                 'createdAt': testutil.NOW.isoformat(),
    #             },
    #         })

    # def test_fails_on_invalid_record(self):
    #     # Invalid app.bsky.actor.profile record: Record/description must be a string
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.put_record({
    #             **profilePath,
    #             'repo': bob.did,
    #             'record': {
    #                 'displayName': 'Robert',
    #                 'description': 3.141,
    #             },
    #         })

    #     data = xrpc_repo.get_record(
    #         **profilePath,
    #         repo=bob.did,
    #     )
    #     self.assertEqual({
    #         '$type': ids.AppBskyActorProfile,
    #         'displayName': 'Robert',
    #         'description': 'Dog lover',
    #     }, profile.value)

    # def test_defaults_an_undefined_$type_on_records(self):
    #     res = xrpc_repo.create_record({
    #         'repo': 'did:web:user.com',
    #         'collection': 'app.bsky.feed.post',
    #         'record': {
    #             'text': 'blah',
    #             'createdAt': testutil.NOW.isoformat(),
    #         },
    #     })
    #     got = xrpc_repo.get_record(
    #         repo='did:web:user.com',
    #         collection=res.uri.collection,
    #         rkey=res.uri.rkey,
    #     )
    #     self.assertEqual(res.uri.collection, got.value['$type'])

    # def test_requires_the_schema_to_be_known_if_validating(self):
    #     # Lexicon not found: lex:com.example.foobar
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.create_record({
    #         'repo': 'did:web:user.com',
    #         'collection': 'com.example.foobar',
    #         'record': { '$type': 'com.example.foobar' },
    #     })

    # def test_requires_the_type_to_match_the_schema(self):
    #     # Invalid $type: expected app.bsky.feed.post, got app.bsky.feed.like
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.create_record({
    #             'repo': 'did:web:user.com',
    #             'collection': 'app.bsky.feed.post',
    #             'record': { '$type': 'app.bsky.feed.like' },
    #         })

    # def test_validates_the_record_on_write(self):
    #     # Invalid app.bsky.feed.post record: Record must have the property "text"
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.create_record({
    #             'repo': 'did:web:user.com',
    #             'collection': 'app.bsky.feed.post',
    #             'record': { '$type': 'app.bsky.feed.post' },
    #         })

    # # compare and swap
    # def _setUp(self):
    #     recordCount = 0 # Ensures unique cids
    #     postRecord = lambda: {
    #         'text': f'post ({++recordCount})',
    #         'createdAt': testutil.NOW.isoformat(),
    #     }
    #     profileRecord = lambda: {
    #         'displayName': f'ali ({++recordCount})',
    #     }

    # def test_createRecord_succeeds_on_proper_commit_cas(self):
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })
    #     data = xrpc_repo.create_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'swapCommit': head.root,
    #         'record': postRecord(),
    #     })
    #     checkPost = xrpc_repo.get_record(
    #         repo=post.uri.host,
    #         collection=post.uri.collection,
    #         rkey=post.uri.rkey,
    #     )
    #     assert checkPost

    # def test_createRecord_fails_on_bad_commit_cas(self):
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })

    #     # Update repo, change head
    #     xrpc_repo.create_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': postRecord(),
    #     })

    #     # createRecord.InvalidSwapError
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.create_record({
    #             'repo': 'did:web:user.com',
    #             'collection': ids.AppBskyFeedPost,
    #             'swapCommit': staleHead.root,
    #             'record': postRecord(),
    #         })

    # def test_deleteRecord_succeeds_on_proper_commit_cas(self):
    #     data = xrpc_repo.create_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': postRecord(),
    #     })
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })
    #     xrpc_repo.delete_record({
    #         'repo': post.uri.host,
    #         'collection': post.uri.collection,
    #         'rkey': post.uri.rkey,
    #         'swapCommit': head.root,
    #     })

    #     # Could not locate record
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.get_record(
    #             repo=post.uri.host,
    #             collection=post.uri.collection,
    #             rkey=post.uri.rkey,
    #         )

    # def test_deleteRecord_fails_on_bad_commit_cas(self):
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })
    #     data = xrpc_repo.create_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': postRecord(),
    #     })

    #     # deleteRecord.InvalidSwapError
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.delete_record({
    #             'repo': post.uri.host,
    #             'collection': post.uri.collection,
    #             'rkey': post.uri.rkey,
    #             'swapCommit': staleHead.root,
    #         })

    #     checkPost = xrpc_repo.get_record(
    #         repo=post.uri.host,
    #         collection=post.uri.collection,
    #         rkey=post.uri.rkey,
    #     )
    #     assert checkPost

    # def test_deleteRecord_succeeds_on_proper_record_cas(self):
    #     data = xrpc_repo.create_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': postRecord(),
    #     })

    #     repo.deleteRecord({
    #         'repo': post.uri.host,
    #         'collection': post.uri.collection,
    #         'rkey': post.uri.rkey,
    #         'swapRecord': post.cid,
    #     })

    #     # Could not locate record
    #     with self.assertRaises(ValueError):
    #         repo.getRecord(
    #             repo=post.uri.host,
    #             collection=post.uri.collection,
    #             rkey=post.uri.rkey,
    #         )

    # def test_deleteRecord_fails_on_bad_record_cas(self):
    #     data = xrpc_repo.create_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': postRecord(),
    #     })

    #     # deleteRecord.InvalidSwapError
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.delete_record({
    #             'repo': post.uri.host,
    #             'collection': post.uri.collection,
    #             'rkey': post.uri.rkey,
    #             'swapRecord': (cidForCbor({})),
    #         })

    #     assert xrpc_repo.get_record(
    #         repo=post.uri.host,
    #         collection=post.uri.collection,
    #         rkey=post.uri.rkey,
    #     )

    # def test_putRecord_succeeds_on_proper_commit_cas(self):
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })
    #     data = xrpc_repo.put_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyActorProfile,
    #         'rkey': 'self',
    #         'swapCommit': head.root,
    #         'record': profileRecord(),
    #     })
    #     data = xrpc_repo.get_record(
    #         repo='did:web:user.com',
    #         collection=ids.AppBskyActorProfile,
    #         rkey='self',
    #     )
    #     self.assertEqual(profile.cid, checkProfile.cid)

    # def test_putRecord_fails_on_bad_commit_cas(self):
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })

    #     # Update repo, change head
    #     xrpc_repo.create_record(
    #         repo='did:web:user.com',
    #         collection=ids.AppBskyFeedPost,
    #         record=postRecord(),
    #     )

    #     # putRecord.InvalidSwapError
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.put_record({
    #             'repo': 'did:web:user.com',
    #             'collection': ids.AppBskyActorProfile,
    #             'rkey': 'self',
    #             'swapCommit': staleHead.root,
    #             'record': profileRecord(),
    #         })

    # def test_putRecord_succeeds_on_proper_record_cas(self):
    #     # Start with missing profile record, to test swapRecord=null
    #     xrpc_repo.delete_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyActorProfile,
    #         'rkey': 'self',
    #     })

    #     # Test swapRecord w/ null (ensures create)
    #     data = xrpc_repo.put_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyActorProfile,
    #         'rkey': 'self',
    #         'swapRecord': null,
    #         'record': profileRecord(),
    #     })

    #     data = xrpc_repo.get_record(
    #         repo='did:web:user.com',
    #         collection=ids.AppBskyActorProfile,
    #         rkey='self',
    #     )
    #     self.assertEqual(profile1.cid, checkProfile1.cid)

    #     # Test swapRecord w/ cid (ensures update)
    #     data = xrpc_repo.put_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyActorProfile,
    #         'rkey': 'self',
    #         'swapRecord': profile1.cid,
    #         'record': profileRecord(),
    #     })

    #     data = xrpc_repo.get_record(
    #         repo='did:web:user.com',
    #         collection=ids.AppBskyActorProfile,
    #         rkey='self',
    #     )
    #     self.assertEqual(profile2.cid, checkProfile2.cid)

    # def test_putRecord_fails_on_bad_record_cas(self):
    #     # Test swapRecord w/ null (ensures create)
    #     # putRecord.InvalidSwapError
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.put_record({
    #             'repo': 'did:web:user.com',
    #             'collection': ids.AppBskyActorProfile,
    #             'rkey': 'self',
    #             'swapRecord': null,
    #             'record': profileRecord(),
    #         })

    #     # Test swapRecord w/ cid (ensures update)
    #     # putRecord.InvalidSwapError
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.put_record({
    #             'repo': 'did:web:user.com',
    #             'collection': ids.AppBskyActorProfile,
    #             'rkey': 'self',
    #             'swapRecord': (cidForCbor({})),
    #             'record': profileRecord(),
    #         })

    # def test_applyWrites_succeeds_on_proper_commit_cas(self):
    #     data = sync.getHead({ 'did': 'did:web:user.com' })
    #     xrpc_repo.apply_writes({
    #         'repo': 'did:web:user.com',
    #         'swapCommit': head.root,
    #         'writes': [{
    #             '$type': f'{ids.ComAtprotoRepoApplyWrites}#create',
    #             'action': 'create',
    #             'collection': ids.AppBskyFeedPost,
    #             'value': { '$type': ids.AppBskyFeedPost, **postRecord() },
    #         }],
    #     })

    # def test_applyWrites_fails_on_bad_commit_cas(self):
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })

    #     # Update repo, change head
    #     xrpc_repo.create_record({
    #         'repo': 'did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': postRecord(),
    #     })

    #     # applyWrites.InvalidSwapError,
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.apply_writes({
    #             'repo': 'did:web:user.com',
    #             'swapCommit': staleHead.root,
    #             'writes': [
    #                 {
    #                     '$type': f'{ids.ComAtprotoRepoApplyWrites}#create',
    #                     'action': 'create',
    #                     'collection': ids.AppBskyFeedPost,
    #                     'value': { '$type': ids.AppBskyFeedPost, **postRecord() },
    #                 },
    #             ],
    #         })

    # def test_write_fail_on_cbor_to_lex_fail(self):
    #     result = defaultFetchHandler(
    #         aliceAgent.service.origin + '/xrpc/com.atproto.repo.createRecord',
    #         'post',
    #         { **aliceAgent.api.xrpc.headers, 'Content-Type': 'application/json' },
    #         json.dumps({
    #             'repo': 'did:web:user.com',
    #             'collection': 'app.bsky.feed.post',
    #             'record': {
    #                 'text': 'x',
    #                 'createdAt': testutil.NOW.isoformat(),
    #                 'deepObject': createDeepObject(4000),
    #             },
    #         }),
    #     )
    #     self.assertEqual(400, result.status)
    #     self.assertEqual({
    #         'error': 'InvalidRequest',
    #         'message': 'Bad record',
    #     }, result.body)

    # def test_prevents_duplicate_likes(self):
    #     now = testutil.NOW.isoformat()
    #     uriA = AtUri.make(bob.did, 'app.bsky.feed.post', TID.nextStr())
    #     cidA = cidForCbor({ 'post': 'a' })
    #     uriB = AtUri.make(bob.did, 'app.bsky.feed.post', TID.nextStr())
    #     cidB = cidForCbor({ 'post': 'b' })

    #     likes = []
    #     for uri, cid in [(uriA, cidA), (uriB, cidB), (uriA, cidA)]:
    #         likes.append(xrpc_repo.create_record({
    #             'repo': 'did:web:user.com',
    #             'collection': 'app.bsky.feed.like',
    #             'record': {
    #                 '$type': 'app.bsky.feed.like',
    #                 'subject': { 'uri': uri, 'cid': cid },
    #                 'createdAt': now,
    #             },
    #         }))

    #     # Could not locate record
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.get_record(
    #             repo='did:web:user.com',
    #             collection='app.bsky.feed.like',
    #             rkey=AtUri(likes[0].uri).rkey,
    #         )

    #     assert xrpc_repo.get_record(
    #         repo='did:web:user.com',
    #         collection='app.bsky.feed.like',
    #         rkey=AtUri(likes[1].uri).rkey,
    #     )
    #     assert xrpc_repo.get_record(
    #         repo='did:web:user.com',
    #         collection='app.bsky.feed.like',
    #         rkey=AtUri(likes[2].uri).rkey,
    #     )

    # def test_prevents_duplicate_reposts(self):
    #     now = testutil.NOW.isoformat()
    #     uriA = AtUri.make(bob.did, 'app.bsky.feed.post', TID.nextStr())
    #     cidA = cidForCbor({ 'post': 'a' })
    #     uriB = AtUri.make(bob.did, 'app.bsky.feed.post', TID.nextStr())
    #     cidB = cidForCbor({ 'post': 'b' })

    #     reposts = []
    #     for uri, cid in [(uriA, cidA), (uriB, cidB), (uriA, cidA)]:
    #         reposts.append(xrpc_repo.create_record({
    #             'repo': 'did:web:user.com',
    #             'collection': 'app.bsky.feed.repost',
    #             'record': {
    #                 '$type': 'app.bsky.feed.repost',
    #                 'subject': { 'uri': uriA, 'cid': cidA },
    #                 'createdAt': now,
    #             }
    #         }))

    #     # Could not locate record
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.get_record(
    #             repo='did:web:user.com',
    #             collection='app.bsky.feed.repost',
    #             rkey=AtUri(reposts[0].uri).rkey,
    #         )
    #     assert xrpc_repo.get_record(
    #         repo='did:web:user.com',
    #         collection='app.bsky.feed.repost',
    #         rkey=AtUri(reposts[1].uri).rkey,
    #     )
    #     assert xrpc_repo.get_record(
    #         repo='did:web:user.com',
    #         collection='app.bsky.feed.repost',
    #         rkey=AtUri(reposts[2].uri).rkey,
    #     )
