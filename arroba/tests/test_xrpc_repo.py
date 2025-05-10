"""Unit tests for xrpc_repo.py.

TODO:

* paging, cursors
"""
import itertools
import json
import os
from pathlib import Path
from urllib.parse import urlencode
from unittest.mock import patch

from flask import request
from multiformats import CID
import requests
from werkzeug.exceptions import HTTPException

from .. import did
from ..datastore_storage import DatastoreStorage
from ..repo import Repo, Write
from .. import server
from ..storage import Action
from .. import util
from . import testutil
from .. import xrpc_repo

CID1 = CID.decode('bafyreiblaotetvwobe7cu2uqvnddr6ew2q3cu75qsoweulzku2egca4dxq')
CID2 = CID.decode('bafyreie7xn4ec3mhapvf7gefkxo7ktko5xkdijm7l7qn54tk3hda633wxy')
CID1_STR = CID1.encode('base32')
CID2_STR = CID2.encode('base32')

SNARFED2_DID = 'did:plc:5zspv27pk4iqtrl2ql2nykjh'
SNARFED2_DID_DOC = {
    'id': 'did:plc:5zspv27pk4iqtrl2ql2nykjh',
    'alsoKnownAs': ['at://snarfed2.bsky.social'],
    'verificationMethod': [{
        'id': 'did:plc:5zspv27pk4iqtrl2ql2nykjh#atproto',
        'publicKeyMultibase': 'zQ3shuteTZT6t9ek6UcKu7UfVES2AJgpadj2bT5zD8NHFqLce',
    }],
}
SNARFED2_HEAD = CID.decode('bafyreihrulqpzqf2vrjc6ef3phj27x2ohidkpf2ctk23mlk2fdyitegqeu')
with open(Path(__file__).parent / 'snarfed2.car', 'rb') as f:
    SNARFED2_CAR = f.read()
with open(Path(__file__).parent / 'snarfed2.json') as f:
    SNARFED2_RECORDS = json.load(f)
    blob = SNARFED2_RECORDS['app.bsky.actor.profile']['self']['avatar']
    blob['ref'] = CID.decode(blob['ref']['/'])


class XrpcRepoTest(testutil.XrpcTestCase):

    def last_at_uri(self):
        tid = util.int_to_tid(util._tid_ts_last)
        return f'at://did:web:user.com/app.bsky.feed.post/{tid}'

    @patch('requests.get', return_value=testutil.requests_response({'foo': 'bar'}))
    def test_describe_repo(self, _):
        with self.assertRaises(ValueError):
            xrpc_repo.describe_repo({}, repo='unknown')

        resp = xrpc_repo.describe_repo({}, repo='did:web:user.com')
        self.assertEqual({
            'did': 'did:web:user.com',
            'handle': 'han.dull',
            'didDoc': {
                'foo': 'bar',
            },
            'collections': [
                'app.bsky.actor.profile',
                'app.bsky.feed.like',
                'app.bsky.feed.post',
                'app.bsky.feed.repost',
                'app.bsky.graph.block',
                'app.bsky.graph.follow',
                'chat.bsky.actor.declaration',
            ],
            'handleIsCorrect': True,
        }, resp)

    @patch('requests.get', return_value=testutil.requests_response('', status=500))
    def test_describe_repo_did_doc_fetch_error(self, _):
        with self.assertRaises(ValueError) as e:
            resp = xrpc_repo.describe_repo({}, repo='did:web:user.com')

        self.assertEqual("Couldn't resolve did:web:user.com", str(e.exception))

    # based on atproto/packages/pds/tests/crud.test.ts
    def test_create_record(self):
        self.prepare_auth()
        resp = xrpc_repo.create_record({
            'repo': 'at://did:web:user.com',
            'collection': 'app.bsky.feed.post',
            'record': {
                '$type': 'app.bsky.feed.post',
                'text': 'Hello, world!',
                'createdAt': testutil.NOW.isoformat(),
            },
        })
        self.assertEqual({
            'cid': 'bafyreibwxoxuto2bj2lsspzs6dl4kw6cyu3goswuxi5qbhpc2xlqvnnjg4',
            'uri': self.last_at_uri(),
        }, resp)

    def test_list_records(self):
        resp = xrpc_repo.list_records({}, repo='did:web:user.com',
                                      collection='app.bsky.feed.post')
        self.assertEqual([], resp['records'])

        self.test_create_record()
        resp = xrpc_repo.list_records({}, repo='did:web:user.com',
                                      collection='app.bsky.feed.post')
        self.assertEqual(1, len(resp['records']))
        self.assertEqual('Hello, world!', resp['records'][0]['value']['text'])

    def test_list_records_encodes_cids_blobs(self):
        repo = server.load_repo('did:web:user.com')

        repo.apply_writes([
            Write(action=Action.CREATE,
                  collection=coll,
                  rkey=str(i),
                  record=record)
            for i, (coll, record) in enumerate([
                ('test.coll', {'cid': CID1}),
                ('test.coll', {'blob': {'$type': 'blob', 'ref': CID2}}),
                ('test.other_coll', {'foo': 'bar'}),
            ])])

        resp = xrpc_repo.list_records({}, repo='did:web:user.com',
                                      collection='test.coll')
        self.assertEqual({
            'records': [{
                'uri': 'at://did:web:user.com/test.coll/0',
                'cid': 'bafyreiebpz6rwjafxxc3ed4r6ukq54ioctdrgj4r5ejr3eestqecypzeja',
                'value': {'cid': {'$link': CID1_STR}},
            }, {
                'uri': 'at://did:web:user.com/test.coll/1',
                'cid': 'bafyreig6osigu5lx7oi7nlwx6oi6jjgnwwpjislog7dd34j2m6mt47wspm',
                'value': {'blob': {'$type': 'blob', 'ref': {'$link': CID2_STR}}},
            }],
        }, resp)

    def test_get_record(self):
        self.test_create_record()

        resp = xrpc_repo.get_record({},
            repo='did:web:user.com',
            collection='app.bsky.feed.post',
            rkey=util.int_to_tid(util._tid_ts_last),
        )
        self.assertEqual({
            'cid': 'bafyreibwxoxuto2bj2lsspzs6dl4kw6cyu3goswuxi5qbhpc2xlqvnnjg4',
            'uri': self.last_at_uri(),
            'value': {
                '$type': 'app.bsky.feed.post',
                'text': 'Hello, world!',
                'createdAt': testutil.NOW.isoformat(),
            },
        }, resp)

    def test_get_record_encodes_cids_blobs(self):
        repo = server.load_repo('did:web:user.com')
        repo.apply_writes([Write(
            action=Action.CREATE,
            collection='test.coll',
            rkey='self',
            record={
            'cid': CID1,
                'blob': {
                    '$type': 'blob',
                    'ref': CID2,
                    'mimeType': 'foo/bar',
                    'size': 13,
                },
            })])

        resp = xrpc_repo.get_record({},
            repo='did:web:user.com',
            collection='test.coll',
            rkey='self',
        )
        self.assertEqual({
            'uri': 'at://did:web:user.com/test.coll/self',
            'cid': 'bafyreibnpyb6kyzty7i67aunjykm56jgjzb3cfqzmcnkkvoa46ztzqt2ka',
            'value': {
                'cid': {'$link': CID1_STR},
                'blob': {
                    '$type': 'blob',
                    'ref': {'$link': CID2_STR},
                    'mimeType': 'foo/bar',
                    'size': 13,
                },
            },
        }, resp)

    def test_get_record_not_found_no_app_view_env_var(self):
        with self.assertRaises(ValueError):
            xrpc_repo.get_record({},
                repo='did:web:user.com',
                collection='app.bsky.feed.post',
                rkey='99999',
            )

    @patch('requests.get')
    def test_get_record_not_found_fall_back_to_app_view(self, mock_get):
        resp = {
            'uri': 'at://did:web:other/app.bsky.feed.post/99999',
            'cid': 'sydddddd',
            'value': {'foo': 'bar'},
        }
        mock_get.return_value = testutil.requests_response(resp)

        params = {
            'repo': 'did:web:other',
            'collection': 'app.bsky.feed.post',
            'rkey': '99999',
        }
        os.environ.update({
            'APPVIEW_HOST': 'app.vue',
            'APPVIEW_JWT': 'jay-dublyew-tee',
        })
        self.assertEqual(resp, xrpc_repo.get_record({}, **params))

        mock_get.assert_called_once_with(
            'https://app.vue/xrpc/com.atproto.repo.getRecord?' + urlencode(params),
            headers={
                'User-Agent': util.USER_AGENT,
                'Content-Type': 'application/json',
                'Authorization': 'Bearer jay-dublyew-tee',
            }, json=None, data=None)

    # TODO: what does getRecord return not found? uri and value in output are
    # required, and it doesn't declare any errors
    # @patch('requests.get')
    # def test_get_record_not_found_locally_or_app_view(self):
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.get_record({},
    #             repo='did:web:user.com',
    #             collection='app.bsky.feed.post',
    #             rkey='99999',
    #         )

    @patch('requests.get')
    def test_get_record_not_found_locally_or_app_view(self, mock_get):
        mock_get.return_value = testutil.requests_response({'my': 'err'}, status=400)

        os.environ.update({
            'APPVIEW_HOST': 'app.vue',
            'APPVIEW_JWT': 'jay-dublyew-tee',
        })
        with self.assertRaises(HTTPException) as e:
            xrpc_repo.get_record({},
                repo='did:web:user.com',
                collection='app.bsky.feed.post',
                rkey='99999')

        resp = e.exception.get_response()
        self.assertEqual(400, resp.status_code)
        self.assertEqual({'my': 'err'}, resp.json)

    def test_delete_record(self):
        self.prepare_auth()
        self.test_create_record()

        xrpc_repo.delete_record({
            'repo': 'at://did:web:user.com',
            'collection': 'app.bsky.feed.post',
            'rkey': util.int_to_tid(util._tid_ts_last),
        })

        with self.assertRaises(ValueError):
            xrpc_repo.get_record({},
                repo='did:web:user.com',
                collection='app.bsky.feed.post',
                rkey=util.int_to_tid(util._tid_ts_last),
            )

        resp = xrpc_repo.list_records({},
            repo='did:web:user.com',
            collection='app.bsky.feed.post',
        )
        self.assertEqual([], resp['records'])

    def test_delete_nonexistent_record(self):
        self.prepare_auth()
        # noop
        xrpc_repo.delete_record({
            'repo': 'at://did:web:user.com',
            'collection': 'app.bsky.feed.post',
            'rkey': '9999',
        })

    def test_writes_without_auth_fail(self):
        self.prepare_auth()
        del request.headers['Authorization']

        input = {  # union of all inputs
            'repo': 'at://did:web:user.com',
            'collection': 'app.bsky.feed.post',
            'rkey': '9999',
            'record': {
                '$type': 'app.bsky.feed.post',
                'text': 'Hello, world!',
                'createdAt': testutil.NOW.isoformat(),
            },
        }

        with self.assertRaises(ValueError):
            xrpc_repo.create_record(input)

        with self.assertRaises(ValueError):
            xrpc_repo.delete_record(input)

        with self.assertRaises(ValueError):
            xrpc_repo.put_record(input)

    def test_authed_writes_without_repo_token_return_not_implemented(self):
        self.prepare_auth()
        del os.environ['REPO_TOKEN']

        input = {
            'repo': 'at://did:web:user.com',
        }

        with self.assertRaises(NotImplementedError):
            xrpc_repo.create_record(input)

        with self.assertRaises(NotImplementedError):
            xrpc_repo.delete_record(input)

        with self.assertRaises(NotImplementedError):
            xrpc_repo.put_record(input)

    def test_put_new_record(self):
        self.prepare_auth()
        resp = xrpc_repo.put_record({
            'repo': 'at://did:web:user.com',
            'collection': 'app.bsky.actor.profile',
            'rkey': 'self',
            'record': {'displayName': 'Ms. Alice'},
        })
        self.assertEqual('at://did:web:user.com/app.bsky.actor.profile/self',
                         resp['uri'])

        resp = xrpc_repo.get_record({},
            repo='did:web:user.com',
            collection='app.bsky.actor.profile',
            rkey='self',
        )
        self.assertEqual({'displayName': 'Ms. Alice'}, resp['value'])

    def test_put_update_existing_record(self):
        self.prepare_auth()
        self.test_put_new_record()

        resp = xrpc_repo.put_record({
            'repo': 'at://did:web:user.com',
            'collection': 'app.bsky.actor.profile',
            'rkey': 'self',
            'record': {'displayName': 'Mr. Bob'},
        })
        self.assertEqual('at://did:web:user.com/app.bsky.actor.profile/self',
                         resp['uri'])

        resp = xrpc_repo.get_record({},
            repo='did:web:user.com',
            collection='app.bsky.actor.profile',
            rkey='self',
        )
        self.assertEqual({'displayName': 'Mr. Bob'}, resp['value'])

    def test_import_repo_not_authed(self):
        with self.assertRaises(ValueError):
            xrpc_repo.import_repo(SNARFED2_CAR)

    def test_import_repo_existing(self):
        self.prepare_auth()

        Repo.create(server.storage, SNARFED2_DID, handle='han.dull',
                    signing_key=self.key)

        with self.assertRaises(ValueError):
            xrpc_repo.import_repo(SNARFED2_CAR)

    @patch('requests.get', return_value=testutil.requests_response(SNARFED2_DID_DOC))
    def test_import_repo(self, _):
        self.prepare_auth()

        self.assertIsNone(server.storage.load_repo(SNARFED2_DID))

        xrpc_repo.import_repo(SNARFED2_CAR)
        repo = server.storage.load_repo(SNARFED2_DID)
        self.assertDictEqual(SNARFED2_RECORDS, dict(repo.get_contents()))
        self.assertEqual(SNARFED2_HEAD, repo.head.cid)
        self.assertEqual('did:plc:5zspv27pk4iqtrl2ql2nykjh', repo.did)
        self.assertEqual('snarfed2.bsky.social', repo.handle)

    @patch('requests.get')
    def test_import_repo_bad_signature(self, mock_get):
        self.prepare_auth()
        mock_get.return_value = testutil.requests_response({
            'id': 'did:plc:5zspv27pk4iqtrl2ql2nykjh',
            'alsoKnownAs': ['at://snarfed2.bsky.social'],
            'verificationMethod': [{
                'id': 'did:plc:5zspv27pk4iqtrl2ql2nykjh#atproto',
                'publicKeyMultibase': did.encode_did_key(self.key.public_key()),
            }],
        })

        with self.assertRaises(ValueError) as e:
            xrpc_repo.import_repo(SNARFED2_CAR)

        self.assertTrue(str(e.exception).startswith("Couldn't verify signature"),
                        e.exception)

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
    #         'repo': 'at://did:web:user.com',
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
    #         'repo': 'at://did:web:user.com',
    #         'collection': 'com.example.foobar',
    #         'record': { '$type': 'com.example.foobar' },
    #     })

    # def test_requires_the_type_to_match_the_schema(self):
    #     # Invalid $type: expected app.bsky.feed.post, got app.bsky.feed.like
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.create_record({
    #             'repo': 'at://did:web:user.com',
    #             'collection': 'app.bsky.feed.post',
    #             'record': { '$type': 'app.bsky.feed.like' },
    #         })

    # def test_validates_the_record_on_write(self):
    #     # Invalid app.bsky.feed.post record: Record must have the property "text"
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.create_record({
    #             'repo': 'at://did:web:user.com',
    #             'collection': 'app.bsky.feed.post',
    #             'record': { '$type': 'app.bsky.feed.post' },
    #         })

    # # compare and swap
    # def _setUp(self):
    #     recordCount = 0 # Ensures unique cids
    #     post_record = lambda: {
    #         'text': f'post ({++recordCount})',
    #         'createdAt': testutil.NOW.isoformat(),
    #     }
    #     profile_record = lambda: {
    #         'displayName': f'ali ({++recordCount})',
    #     }

    # def test_create_record_succeeds_on_proper_commit_cas(self):
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })
    #     data = xrpc_repo.create_record({
    #         'repo': 'at://did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'swapCommit': head.root,
    #         'record': post_record(),
    #     })
    #     checkPost = xrpc_repo.get_record(
    #         repo=post.uri.host,
    #         collection=post.uri.collection,
    #         rkey=post.uri.rkey,
    #     )
    #     assert checkPost

    # def test_create_record_fails_on_bad_commit_cas(self):
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })

    #     # Update repo, change head
    #     xrpc_repo.create_record({
    #         'repo': 'at://did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': post_record(),
    #     })

    #     # create_record.InvalidSwapError
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.create_record({
    #             'repo': 'at://did:web:user.com',
    #             'collection': ids.AppBskyFeedPost,
    #             'swapCommit': staleHead.root,
    #             'record': post_record(),
    #         })

    # def test_delete_record_succeeds_on_proper_commit_cas(self):
    #     data = xrpc_repo.create_record({
    #         'repo': 'at://did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': post_record(),
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

    # def test_delete_record_fails_on_bad_commit_cas(self):
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })
    #     data = xrpc_repo.create_record({
    #         'repo': 'at://did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': post_record(),
    #     })

    #     # delete_record.InvalidSwapError
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

    # def test_delete_record_succeeds_on_proper_record_cas(self):
    #     data = xrpc_repo.create_record({
    #         'repo': 'at://did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': post_record(),
    #     })

    #     repo.delete_record({
    #         'repo': post.uri.host,
    #         'collection': post.uri.collection,
    #         'rkey': post.uri.rkey,
    #         'swapRecord': post.cid,
    #     })

    #     # Could not locate record
    #     with self.assertRaises(ValueError):
    #         repo.get_record(
    #             repo=post.uri.host,
    #             collection=post.uri.collection,
    #             rkey=post.uri.rkey,
    #         )

    # def test_delete_record_fails_on_bad_record_cas(self):
    #     data = xrpc_repo.create_record({
    #         'repo': 'at://did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': post_record(),
    #     })

    #     # delete_record.InvalidSwapError
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

    # def test_put_record_succeeds_on_proper_commit_cas(self):
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })
    #     data = xrpc_repo.put_record({
    #         'repo': 'at://did:web:user.com',
    #         'collection': ids.AppBskyActorProfile,
    #         'rkey': 'self',
    #         'swapCommit': head.root,
    #         'record': profile_record(),
    #     })
    #     data = xrpc_repo.get_record(
    #         repo='did:web:user.com',
    #         collection=ids.AppBskyActorProfile,
    #         rkey='self',
    #     )
    #     self.assertEqual(profile.cid, checkProfile.cid)

    # def test_put_record_fails_on_bad_commit_cas(self):
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })

    #     # Update repo, change head
    #     xrpc_repo.create_record(
    #         repo='did:web:user.com',
    #         collection=ids.AppBskyFeedPost,
    #         record=post_record(),
    #     )

    #     # put_record.InvalidSwapError
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.put_record({
    #             'repo': 'at://did:web:user.com',
    #             'collection': ids.AppBskyActorProfile,
    #             'rkey': 'self',
    #             'swapCommit': staleHead.root,
    #             'record': profile_record(),
    #         })

    # def test_put_record_succeeds_on_proper_record_cas(self):
    #     # Start with missing profile record, to test swapRecord=null
    #     xrpc_repo.delete_record({
    #         'repo': 'at://did:web:user.com',
    #         'collection': ids.AppBskyActorProfile,
    #         'rkey': 'self',
    #     })

    #     # Test swapRecord w/ null (ensures create)
    #     data = xrpc_repo.put_record({
    #         'repo': 'at://did:web:user.com',
    #         'collection': ids.AppBskyActorProfile,
    #         'rkey': 'self',
    #         'swapRecord': null,
    #         'record': profile_record(),
    #     })

    #     data = xrpc_repo.get_record(
    #         repo='did:web:user.com',
    #         collection=ids.AppBskyActorProfile,
    #         rkey='self',
    #     )
    #     self.assertEqual(profile1.cid, checkProfile1.cid)

    #     # Test swapRecord w/ cid (ensures update)
    #     data = xrpc_repo.put_record({
    #         'repo': 'at://did:web:user.com',
    #         'collection': ids.AppBskyActorProfile,
    #         'rkey': 'self',
    #         'swapRecord': profile1.cid,
    #         'record': profile_record(),
    #     })

    #     data = xrpc_repo.get_record(
    #         repo='did:web:user.com',
    #         collection=ids.AppBskyActorProfile,
    #         rkey='self',
    #     )
    #     self.assertEqual(profile2.cid, checkProfile2.cid)

    # def test_put_record_fails_on_bad_record_cas(self):
    #     # Test swapRecord w/ null (ensures create)
    #     # put_record.InvalidSwapError
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.put_record({
    #             'repo': 'at://did:web:user.com',
    #             'collection': ids.AppBskyActorProfile,
    #             'rkey': 'self',
    #             'swapRecord': null,
    #             'record': profile_record(),
    #         })

    #     # Test swapRecord w/ cid (ensures update)
    #     # put_record.InvalidSwapError
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.put_record({
    #             'repo': 'at://did:web:user.com',
    #             'collection': ids.AppBskyActorProfile,
    #             'rkey': 'self',
    #             'swapRecord': (cidForCbor({})),
    #             'record': profile_record(),
    #         })

    # def test_applyWrites_succeeds_on_proper_commit_cas(self):
    #     data = sync.getHead({ 'did': 'did:web:user.com' })
    #     xrpc_repo.apply_writes({
    #         'repo': 'at://did:web:user.com',
    #         'swapCommit': head.root,
    #         'writes': [{
    #             '$type': f'{ids.ComAtprotoRepoApplyWrites}#create',
    #             'action': 'create',
    #             'collection': ids.AppBskyFeedPost,
    #             'value': { '$type': ids.AppBskyFeedPost, **post_record() },
    #         }],
    #     })

    # def test_applyWrites_fails_on_bad_commit_cas(self):
    #     data = xrpc_sync.getHead({ 'did': 'did:web:user.com' })

    #     # Update repo, change head
    #     xrpc_repo.create_record({
    #         'repo': 'at://did:web:user.com',
    #         'collection': ids.AppBskyFeedPost,
    #         'record': post_record(),
    #     })

    #     # applyWrites.InvalidSwapError,
    #     with self.assertRaises(ValueError):
    #         xrpc_repo.apply_writes({
    #             'repo': 'at://did:web:user.com',
    #             'swapCommit': staleHead.root,
    #             'writes': [
    #                 {
    #                     '$type': f'{ids.ComAtprotoRepoApplyWrites}#create',
    #                     'action': 'create',
    #                     'collection': ids.AppBskyFeedPost,
    #                     'value': { '$type': ids.AppBskyFeedPost, **post_record() },
    #                 },
    #             ],
    #         })

    # def test_write_fail_on_cbor_to_lex_fail(self):
    #     result = defaultFetchHandler(
    #         aliceAgent.service.origin + '/xrpc/com.atproto.repo.createRecord',
    #         'post',
    #         { **aliceAgent.api.xrpc.headers, 'Content-Type': 'application/json' },
    #         json.dumps({
    #             'repo': 'at://did:web:user.com',
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
    #             'repo': 'at://did:web:user.com',
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
    #             'repo': 'at://did:web:user.com',
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


class DatastoreXrpcRepoTest(XrpcRepoTest, testutil.DatastoreTest):
    pass
