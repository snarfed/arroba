"""Unit tests for xrpc_proxy.py."""
import gzip
import os
from unittest.mock import patch

from flask import Flask
import jwt
from lexrpc.flask_server import init_flask
from lexrpc.server import Server
from webutil.testutil import NOW, requests_response
from webutil.util import HTTP_TIMEOUT
from werkzeug.exceptions import HTTPException
from werkzeug.wrappers import Response

# testutil must come first; it breaks the repo/storage circular import
from . import testutil
from ..repo import Repo, Write
from .. import server
from ..storage import Action
from ..util import dag_cbor_cid, int_to_tid
from .. import xrpc_proxy

APPVIEW_DID_DOC = {
    'id': 'did:web:a.pp',
    'service': [{
        'id': '#foo',
        'type': 'Foo',
        'serviceEndpoint': 'https://a.pp',
    }],
}

# rev of the repo's initial commit in ReadAfterWriteTest.setUp. MemoryStorage
# starts sequence numbers, and so revs, at 1.
INIT_REV = int_to_tid(1, clock_id=0)
REV_HEADER = {'Atproto-Repo-Rev': INIT_REV}

OTHER_CID = dag_cbor_cid({'other': 'post'}).encode('base32')
OTHER_POST = {
    'uri': 'at://did:plc:other/app.bsky.feed.post/3kother',
    'cid': OTHER_CID,
    'author': {'did': 'did:plc:other', 'handle': 'oth.er'},
    'record': {
        '$type': 'app.bsky.feed.post',
        'text': 'other',
        'createdAt': '2022-01-01T00:00:00.000Z',
    },
    'indexedAt': '2022-01-01T00:00:00.000Z',
    'viewer': {},
}
OTHER_REPLY = {
    **OTHER_POST,
    'uri': 'at://did:plc:other/app.bsky.feed.post/3kreply',
}
OTHER_FEED = {'feed': [{'post': OTHER_POST}]}

MY_POST_URI = 'at://did:web:user.com/app.bsky.feed.post/3kmine'
MY_POST = {
    '$type': 'app.bsky.feed.post',
    'text': 'mine',
    'createdAt': '2022-01-02T03:04:05.000Z',
}
MY_POST_VIEW = {
    'uri': MY_POST_URI,
    'cid': dag_cbor_cid(MY_POST).encode('base32'),
    'author': {'did': 'did:web:user.com', 'handle': 'han.dull'},
    'record': MY_POST,
    'replyCount': 0,
    'repostCount': 0,
    'likeCount': 0,
    'quoteCount': 0,
    'indexedAt': NOW.isoformat(),
    'viewer': {},
}

MY_LIKE_URI = 'at://did:web:user.com/app.bsky.feed.like/3klike'
MY_LIKE = {
    '$type': 'app.bsky.feed.like',
    'subject': {'uri': OTHER_POST['uri'], 'cid': OTHER_CID},
    'createdAt': '2022-01-02T03:04:05.000Z',
}

BLOB_CID = dag_cbor_cid({'a': 'blob'})
BLOB = {
    '$type': 'blob',
    'ref': BLOB_CID,
    'mimeType': 'image/jpeg',
    'size': 1234,
}
GET_BLOB_URL = f'https://localhost:8080/xrpc/com.atproto.sync.getBlob?did=did:web:user.com&cid={BLOB_CID.encode("base32")}'

MY_PROFILE_VIEW = {
    'did': 'did:web:user.com',
    'handle': 'han.dull',
    'displayName': 'Old',
    'description': 'old description',
}



@patch('arroba.did.resolve', return_value=APPVIEW_DID_DOC)
@patch('webutil.util.session.request',
       side_effect=lambda *args, **kwargs: requests_response({'feed': []}))
class XrpcProxyTest(testutil.TestCase):
    authed_did = 'did:web:user.com'
    authed_scopes = ['atproto', 'transition:generic']

    def setUp(self):
        super().setUp()

        self.server = Server(validate=False, require_lexicons=False)
        self.app = Flask(__name__, static_folder=None)

        server.authenticate = lambda: (self.authed_did, self.authed_scopes)

        init_flask(self.server, self.app, fallback=xrpc_proxy.handler)

        self.repo = Repo.create(self.storage, 'did:web:user.com', handle='han.dull',
                                signing_key=self.key)
        self.client = self.app.test_client()
        xrpc_proxy.signing_key.cache.clear()

    def tearDown(self):
        server.authenticate = server.global_token_auth

    def assert_jwt(self, mock_request, **expected):
        """Decodes the outbound Authorization header and checks its claims."""
        headers = mock_request.call_args.kwargs['headers']
        token = headers['Authorization'].removeprefix('Bearer ')
        self.assertEqual({
            'alg': 'ES256K',
            'aud': 'did:web:a.pp',
            'exp': 1641092945,
            'iss': 'did:web:user.com',
            **expected,
        }, jwt.decode(token, self.key, algorithms=['ES256K'],
                      audience='did:web:a.pp', options={'verify_exp': False}))

    def test_query(self, mock_request, _):
        resp = self.client.get('/xrpc/x.y.query?limit=3',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})
        self.assertEqual(200, resp.status_code)
        self.assertEqual({'feed': []}, resp.json)
        self.assertEqual('*', resp.headers['Access-Control-Allow-Origin'])

        args, kwargs = mock_request.call_args
        self.assertEqual(
            ('GET', 'https://a.pp/xrpc/x.y.query?limit=3'),
            args)
        self.assertEqual(b'', kwargs['data'])
        self.assertEqual(HTTP_TIMEOUT, kwargs['timeout'])
        self.assert_jwt(mock_request, lxm='x.y.query')

    def test_procedure_forwards_body(self, mock_request, _):
        resp = self.client.post(
            '/xrpc/x.y.proc',
            json={'seenAt': '2022-01-02T03:04:05Z'},
            headers={'atproto-proxy': 'did:web:a.pp#foo'})
        self.assertEqual(200, resp.status_code)

        args, kwargs = mock_request.call_args
        self.assertEqual(('POST', 'https://a.pp/xrpc/x.y.proc'), args)
        self.assertEqual(b'{"seenAt": "2022-01-02T03:04:05Z"}', kwargs['data'])
        self.assertEqual('application/json', kwargs['headers']['Content-Type'])
        self.assert_jwt(mock_request, lxm='x.y.proc')

    def test_forwards_request_headers(self, mock_request, _):
        self.client.get('/xrpc/x.y.query', headers={
            'atproto-proxy': 'did:web:a.pp#foo',
            'Accept-Language': 'en-US',
            'ATProto-Accept-Labelers': 'did:web:a.pp;redact',
            'Authorization': 'Bearer nope',
            'Cookie': 'sekret',
        })

        headers = mock_request.call_args.kwargs['headers']
        self.assertEqual('en-US', headers['Accept-Language'])
        self.assertEqual('did:web:a.pp;redact', headers['ATProto-Accept-Labelers'])
        self.assertNotIn('Cookie', headers)
        self.assertNotEqual('Bearer nope', headers['Authorization'])

    def test_did_relative_service_id(self, mock_request, mock_resolve):
        mock_resolve.return_value = {
        'id': 'did:web:a.pp',
        'service': [{
            'id': 'did:web:a.pp#foo',
            'serviceEndpoint': 'https://a.pp',
        }],
        }
        resp = self.client.get('/xrpc/x.y.query',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})
        self.assertEqual(200, resp.status_code)

    def test_query_string_ending_in_question_mark(self, mock_request, _):
        self.client.get('/xrpc/x.y.query?q=what?',
                        headers={'atproto-proxy': 'did:web:a.pp#foo'})
        self.assertEqual('https://a.pp/xrpc/x.y.query?q=what?',
                         mock_request.call_args.args[1])

    def test_endpoint_path_prefix_ignored(self, mock_request, mock_resolve):
        # "Paths must always be top-level, not below a prefix."
        # https://atproto.com/specs/xrpc#lexicon-http-endpoints
        mock_resolve.return_value = {
        'id': 'did:web:a.pp',
        'service': [{
            'id': '#foo',
            'serviceEndpoint': 'https://a.pp/base/',
        }],
        }
        self.client.get('/xrpc/x.y.query',
                        headers={'atproto-proxy': 'did:web:a.pp#foo'})
        self.assertEqual('https://a.pp/xrpc/x.y.query',
                         mock_request.call_args.args[1])

    def test_response_headers(self, mock_request, _):
        body = gzip.compress(b'{"feed": []}')
        mock_request.side_effect = lambda *args, **kwargs: requests_response(
            body, headers={'Content-Encoding': 'gzip',
                           'Content-Length': str(len(body)),
                           'Transfer-Encoding': 'chunked',
                           'Trailer': 'Expires',
                           'Connection': 'keep-alive',
                           'X-Ratelimit-Limit': '3000'})
        resp = self.client.get('/xrpc/x.y.query',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})
        self.assertEqual(200, resp.status_code)
        # we stream the body through undecoded, so these stay accurate
        self.assertEqual(body, resp.get_data(as_text=False))
        self.assertEqual('gzip', resp.headers['Content-Encoding'])
        self.assertEqual(str(len(body)), resp.headers['Content-Length'])
        # ...but these describe our connection to the appview, not the body
        self.assertNotIn('Transfer-Encoding', resp.headers)
        self.assertNotIn('Connection', resp.headers)
        # wsgiref's hop-by-hop list misspells this one, so we check it ourselves
        self.assertNotIn('Trailer', resp.headers)
        self.assertEqual('3000', resp.headers['X-Ratelimit-Limit'])

    def test_override_cors_headers(self, mock_request, _):
        """The appview lowercases its headers; ours are title cased."""
        mock_request.side_effect = [
            requests_response({'feed': []},
                              headers={'access-control-allow-origin': 'https://a.pp'})
        ]

        resp = self.client.get('/xrpc/x.y.query',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})
        self.assertEqual(200, resp.status_code)
        self.assertEqual(['*'], resp.headers.get_all('Access-Control-Allow-Origin'))

    def test_forwards_accept_encoding(self, mock_request, _):
        self.client.get('/xrpc/x.y.query', headers={
            'atproto-proxy': 'did:web:a.pp#foo',
            'Accept-Encoding': 'br',
        })
        self.assertEqual(
            'br', mock_request.call_args.kwargs['headers']['Accept-Encoding'])

    def test_no_accept_encoding_asks_for_identity(self, mock_request, _):
        self.client.get('/xrpc/x.y.query',
                        headers={'atproto-proxy': 'did:web:a.pp#foo'})
        self.assertEqual('identity',
                         mock_request.call_args.kwargs['headers']['Accept-Encoding'])

    def test_stream_response(self, mock_request, _):
        body = b'\xde\xad\xbe\xef' * 100_000
        mock_request.side_effect = lambda *args, **kwargs: requests_response(
            body, headers={'Content-Type': 'application/vnd.ipld.car'})
        resp = self.client.get('/xrpc/com.atproto.sync.getRepo',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})

        self.assertEqual(200, resp.status_code)
        self.assertTrue(mock_request.call_args.kwargs['stream'])
        self.assertEqual(body, resp.get_data(as_text=False))
        self.assertEqual('application/vnd.ipld.car', resp.headers['Content-Type'])

    def test_passes_through_error_status(self, mock_request, _):
        mock_request.side_effect = lambda *args, **kwargs: requests_response(
            {'error': 'InvalidRequest', 'message': 'nope'}, status=400)
        resp = self.client.get('/xrpc/x.y.query',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})

        self.assertEqual(400, resp.status_code)
        self.assertEqual({'error': 'InvalidRequest', 'message': 'nope'}, resp.json)

    def test_server_methods_not_proxied(self, mock_request, _):
        """com.atproto.server.* is the PDS's own account and session surface."""
        resp = self.client.get('/xrpc/com.atproto.server.getSession',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})
        self.assertEqual(501, resp.status_code)
        self.assertEqual('MethodNotImplemented', resp.json['error'])
        mock_request.assert_not_called()

    def test_no_header(self, mock_request, _):
        resp = self.client.get('/xrpc/x.y.query')
        self.assertEqual(501, resp.status_code)
        self.assertEqual('MethodNotImplemented', resp.json['error'])
        mock_request.assert_not_called()

    def test_bad_header(self, mock_request, _):
        for bad in ('did:web:a.pp', '#baz', 'baz'):
            with self.subTest(bad=bad):
                resp = self.client.get('/xrpc/x.y.query',
                                       headers={'atproto-proxy': bad})
                self.assertEqual(400, resp.status_code)
                self.assertEqual('InvalidRequest', resp.json['error'])

        mock_request.assert_not_called()

    def test_not_authenticated(self, mock_request, _):
        """The default auth raises ValueError for a missing or bad token."""
        def err():
            raise ValueError('nope')
        server.authenticate = err

        resp = self.client.get('/xrpc/x.y.query',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})

        self.assertEqual(401, resp.status_code)
        self.assertEqual('AuthMissing', resp.json['error'])
        mock_request.assert_not_called()

    def test_all_repos_auth_cant_proxy(self, mock_request, _):
        """eg REPO_TOKEN. We need a user to sign the service auth JWT as."""
        self.authed_did = server.ALL_REPOS
        self.authed_scopes = server.ALL_SCOPES
        resp = self.client.get('/xrpc/x.y.query',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})

        self.assertEqual(401, resp.status_code)
        self.assertEqual('AuthMissing', resp.json['error'])
        mock_request.assert_not_called()

    def test_rpc_scope(self, mock_request, _):
        for scopes in (
            ['atproto', 'rpc:x.y.query?aud=did:web:a.pp%23foo'],
            ['atproto', 'rpc:*?aud=did:web:a.pp%23foo'],
            ['atproto', 'rpc:x.y.query?aud=*'],
        ):
            with self.subTest(scopes=scopes):
                self.authed_scopes = scopes
                resp = self.client.get('/xrpc/x.y.query',
                                       headers={'atproto-proxy': 'did:web:a.pp#foo'})
                self.assertEqual(200, resp.status_code)

    def test_insufficient_scope(self, mock_request, _):
        for scopes in (
            ['atproto'],
            ['atproto', 'repo:app.bsky.feed.post'],
            ['atproto', 'rpc:x.y.other?aud=did:web:a.pp%23foo'],
            ['atproto', 'rpc:x.y.query?aud=did:web:a.pp%23bar'],
            ['atproto', 'rpc:x.y.query?aud=did:web:oth.er%23foo'],
        ):
            with self.subTest(scopes=scopes):
                self.authed_scopes = scopes
                resp = self.client.get('/xrpc/x.y.query',
                                       headers={'atproto-proxy': 'did:web:a.pp#foo'})
                self.assertEqual(403, resp.status_code)
                self.assertEqual('insufficient_scope', resp.json['error'])
                self.assertEqual(
                    'DPoP error="insufficient_scope", error_description="Missing scope rpc:x.y.query?aud=did:web:a.pp%23foo"',
                    resp.headers['WWW-Authenticate'])

        mock_request.assert_not_called()

    def test_auth_http_exception_passes_through(self, mock_request, _):
        """eg OAuth errors, which carry headers like WWW-Authenticate."""
        def err():
            raise HTTPException(response=Response(status=401, headers={
                'WWW-Authenticate': 'DPoP error="use_dpop_nonce"',
            }))
        server.authenticate = err

        resp = self.client.get('/xrpc/x.y.query',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})

        self.assertEqual(401, resp.status_code)
        self.assertEqual('DPoP error="use_dpop_nonce"',
                         resp.headers['WWW-Authenticate'])
        mock_request.assert_not_called()

    def test_unknown_repo(self, mock_request, _):
        self.authed_did = 'did:web:nope.com'
        resp = self.client.get('/xrpc/x.y.query',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})

        self.assertEqual(400, resp.status_code)
        self.assertEqual('RepoNotFound', resp.json['error'])
        mock_request.assert_not_called()

    def test_unresolvable_did(self, mock_request, mock_resolve):
        mock_resolve.side_effect = ValueError('nope')

        resp = self.client.get('/xrpc/x.y.query',
                               headers={'atproto-proxy': 'did:web:nope.com#foo'})

        self.assertEqual(400, resp.status_code)
        self.assertEqual('InvalidRequest', resp.json['error'])
        mock_request.assert_not_called()

    def test_no_matching_service(self, mock_request, _):
        resp = self.client.get('/xrpc/x.y.query',
                               headers={'atproto-proxy': 'did:web:a.pp#other'})

        self.assertEqual(400, resp.status_code)
        self.assertEqual('InvalidRequest', resp.json['error'])
        mock_request.assert_not_called()

    def test_options_not_proxied(self, mock_request, _):
        resp = self.client.options('/xrpc/x.y.query',
                                   headers={'atproto-proxy': 'did:web:a.pp#foo'})

        self.assertEqual(200, resp.status_code)
        self.assertEqual('*', resp.headers['Access-Control-Allow-Origin'])
        mock_request.assert_not_called()

    def test_closes_upstream_response(self, mock_request, _):
        upstream = requests_response({'feed': []})
        mock_request.side_effect = lambda *args, **kwargs: upstream

        self.client.get('/xrpc/x.y.query',
                        headers={'atproto-proxy': 'did:web:a.pp#foo'})
        self.assertTrue(upstream.raw.closed)

    def test_signing_key_cached(self, mock_request, _):
        for i in range(3):
            self.client.get('/xrpc/x.y.query',
                            headers={'atproto-proxy': 'did:web:a.pp#foo'})

        self.assertEqual(3, mock_request.call_count)
        self.assertEqual(1, xrpc_proxy.signing_key.cache.currsize)


@patch('arroba.did.resolve', return_value=APPVIEW_DID_DOC)
class ReadAfterWriteTest(testutil.TestCase):

    def setUp(self):
        super().setUp()
        server.server._validate = True

        auth = patch.object(server, 'authenticate', return_value=(
            'did:web:user.com', ['atproto', 'transition:generic']))
        auth.start()
        self.addCleanup(auth.stop)

        app = Flask(__name__, static_folder=None)
        init_flask(Server(validate=False, require_lexicons=False), app,
                   fallback=xrpc_proxy.handler)
        self.client = app.test_client()
        self.client.environ_base['HTTP_ATPROTO_PROXY'] = 'did:web:a.pp#foo'

        self.repo = Repo.create(self.storage, 'did:web:user.com', handle='han.dull',
                                signing_key=self.key)
        xrpc_proxy.signing_key.cache.clear()

    def commit(self, collection, rkey, record=None):
        action = Action.CREATE if record else Action.DELETE
        self.storage.commit(self.repo, [Write(action, collection, rkey, record)])

    def get(self, nsid, **params):
        """Makes a request and checks that the output validates."""
        resp = self.client.get(f'/xrpc/{nsid}', query_string=params)
        self.assertEqual(200, resp.status_code)
        server.server.validate(nsid, 'output', resp.json)
        return resp

    @patch('webutil.util.session.request',
           return_value=requests_response(OTHER_FEED, headers=REV_HEADER))
    def test_timeline(self, mock_request, _):
        self.commit('app.bsky.feed.post', '3kmine', MY_POST)

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({'feed': [
            {'post': MY_POST_VIEW},
            {'post': OTHER_POST},
        ]}, resp.json)
        # upstream's rev would be wrong, since we've added newer records, and the
        # reference PDS doesn't send its own
        self.assertNotIn('Atproto-Repo-Rev', resp.headers)
        self.assertFalse(mock_request.call_args.kwargs['stream'])

    @patch('webutil.util.session.request', return_value=requests_response({
        'feed': [{'post': {**MY_POST_VIEW, 'likeCount': 5}}, {'post': OTHER_POST}],
    }, headers=REV_HEADER))
    def test_timeline_already_has_post(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine', MY_POST)

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({'feed': [
            {'post': {**MY_POST_VIEW, 'likeCount': 5}},
            {'post': OTHER_POST},
        ]}, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response({
        'feed': [{'post': MY_POST_VIEW}, {'post': OTHER_POST}],
    }, headers=REV_HEADER))
    def test_timeline_deleted_post(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine', MY_POST)
        self.commit('app.bsky.feed.post', '3kmine')

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual(OTHER_FEED, resp.json)

    @patch('webutil.util.session.request',
           return_value=requests_response(OTHER_FEED, headers=REV_HEADER))
    def test_timeline_new_posts_sorted_by_created_at(self, _, __):
        newer = {**MY_POST, 'createdAt': '2022-01-01T02:00:00.000Z'}
        older = {**MY_POST, 'createdAt': '2022-01-01T01:00:00.000Z'}
        self.commit('app.bsky.feed.post', '3knewer', newer)
        self.commit('app.bsky.feed.post', '3kolder', older)

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual([
            'at://did:web:user.com/app.bsky.feed.post/3knewer',
            'at://did:web:user.com/app.bsky.feed.post/3kolder',
            OTHER_POST['uri'],
        ], [item['post']['uri'] for item in resp.json['feed']])

    @patch('webutil.util.session.request', return_value=requests_response(
        {**OTHER_FEED, 'cursor': 'abc'}, headers=REV_HEADER))
    def test_timeline_new_post_older_than_page_not_inserted(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine',
                    {**MY_POST, 'createdAt': '2021-01-01T00:00:00.000Z'})

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({**OTHER_FEED, 'cursor': 'abc'}, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response({
        'feed': [{
            'post': OTHER_POST,
            'reason': {
                '$type': 'app.bsky.feed.defs#reasonRepost',
                'by': {'did': 'did:plc:reposter', 'handle': 're.poster'},
                'indexedAt': '2020-01-01T00:00:00.000Z',
            },
        }],
        'cursor': 'abc',
    }, headers=REV_HEADER))
    def test_timeline_oldest_in_page_is_repost(self, _, __):
        """Reposts are sorted by when they were reposted, not the post's createdAt."""
        self.commit('app.bsky.feed.post', '3kmine',
                    {**MY_POST, 'createdAt': '2021-01-01T00:00:00.000Z'})

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual([MY_POST_URI, OTHER_POST['uri']],
                         [item['post']['uri'] for item in resp.json['feed']])

    @patch('webutil.util.session.request',
           return_value=requests_response(OTHER_FEED, headers=REV_HEADER))
    def test_timeline_new_post_older_than_last_page_inserted(self, _, __):
        old_post = {**MY_POST, 'createdAt': '2021-01-01T00:00:00.000Z'}
        self.commit('app.bsky.feed.post', '3kmine', old_post)

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({'feed': [
            {'post': {
                **MY_POST_VIEW,
                'cid': dag_cbor_cid(old_post).encode('base32'),
                'record': old_post,
            }},
            {'post': OTHER_POST},
        ]}, resp.json)

    @patch('webutil.util.session.request')
    def test_timeline_updated_old_post_not_inserted(self, mock_request, _):
        old_post = {**MY_POST, 'createdAt': '2021-01-01T00:00:00.000Z'}
        self.commit('app.bsky.feed.post', '3kmine', old_post)
        mock_request.return_value = requests_response(
            {**OTHER_FEED, 'cursor': 'abc'},
            headers={'Atproto-Repo-Rev': self.repo.head.decoded['rev']})

        self.storage.commit(self.repo, [Write(Action.UPDATE, 'app.bsky.feed.post',
                                              '3kmine', {**old_post, 'text': 'edited'})])

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({**OTHER_FEED, 'cursor': 'abc'}, resp.json)

    @patch('webutil.util.session.request',
           return_value=requests_response(OTHER_FEED, headers=REV_HEADER))
    def test_timeline_later_page(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine', MY_POST)

        resp = self.get('app.bsky.feed.getTimeline', cursor='123')
        self.assertEqual(OTHER_FEED, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response(
        {'feed': []}, headers=REV_HEADER))
    def test_author_feed(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine', MY_POST)

        for actor in 'did:web:user.com', 'han.dull':
            with self.subTest(actor=actor):
                resp = self.get('app.bsky.feed.getAuthorFeed', actor=actor)
                self.assertEqual({'feed': [{'post': MY_POST_VIEW}]}, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response(
        {'feed': []}, headers=REV_HEADER))
    def test_author_feed_other_actor(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine', MY_POST)

        resp = self.get('app.bsky.feed.getAuthorFeed', actor='did:plc:other')
        self.assertEqual({'feed': []}, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response(
        {'feed': [{'post': MY_POST_VIEW}]}, headers=REV_HEADER))
    def test_author_feed_filter_still_removes_deleted_post(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine', MY_POST)
        self.commit('app.bsky.feed.post', '3kmine')

        resp = self.get('app.bsky.feed.getAuthorFeed', actor='did:web:user.com',
                        filter='posts_with_media')
        self.assertEqual({'feed': []}, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response(
        {'feed': []}, headers=REV_HEADER))
    def test_author_feed_filters(self, _, __):
        other_ref = {'uri': OTHER_POST['uri'], 'cid': OTHER_CID}
        my_ref = {
            'uri': 'at://did:web:user.com/app.bsky.feed.post/3kplain',
            'cid': dag_cbor_cid(MY_POST).encode('base32'),
        }
        images = {
            '$type': 'app.bsky.embed.images',
            'images': [{'image': BLOB, 'alt': ''}],
        }

        # in commit order, so the feed has them in reverse
        for rkey, fields in (
            ('3kplain', {}),
            ('3kreply', {'reply': {'root': other_ref, 'parent': other_ref}}),
            ('3kthread', {'reply': {'root': my_ref, 'parent': my_ref}}),
            ('3kimages', {'embed': images}),
            ('3kgallery', {'embed': {
                '$type': 'app.bsky.embed.gallery',
                'items': [{
                    '$type': 'app.bsky.embed.gallery#image',
                    'image': BLOB,
                    'alt': '',
                    'aspectRatio': {'width': 4, 'height': 3},
                }],
            }}),
            ('3kvideo', {'embed': {
                '$type': 'app.bsky.embed.video',
                'video': {**BLOB, 'mimeType': 'video/mp4'},
            }}),
            ('3kquote', {'embed': {
                '$type': 'app.bsky.embed.recordWithMedia',
                'record': {'$type': 'app.bsky.embed.record', 'record': other_ref},
                'media': images,
            }}),
        ):
            self.commit('app.bsky.feed.post', rkey, {**MY_POST, **fields})

        for filter, expected in (
            ('posts_with_replies', ['quote', 'video', 'gallery', 'images', 'thread',
                                    'reply', 'plain']),
            ('posts_no_replies', ['quote', 'video', 'gallery', 'images', 'plain']),
            ('posts_with_media', ['quote', 'gallery', 'images']),
            ('posts_with_video', ['video']),
            ('posts_and_author_threads', ['quote', 'video', 'gallery', 'images',
                                          'thread', 'plain']),
            ('unknown', []),
        ):
            with self.subTest(filter=filter):
                resp = self.get('app.bsky.feed.getAuthorFeed',
                                actor='did:web:user.com', filter=filter)
                self.assert_equals(
                    [f'at://did:web:user.com/app.bsky.feed.post/3k{rkey}'
                     for rkey in expected],
                    [item['post']['uri'] for item in resp.json['feed']])

    @patch('webutil.util.session.request', return_value=requests_response(
        {'feed': []}, headers=REV_HEADER))
    def test_post_author_profile(self, _, __):
        self.commit('app.bsky.actor.profile', 'self', {
            '$type': 'app.bsky.actor.profile',
            'displayName': 'Me',
            'avatar': BLOB,
        })
        self.commit('app.bsky.feed.post', '3kmine', MY_POST)

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({'feed': [{'post': {
            **MY_POST_VIEW,
            'author': {
                'did': 'did:web:user.com',
                'handle': 'han.dull',
                'displayName': 'Me',
                'avatar': GET_BLOB_URL,
            },
        }}]}, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response(
        {'feed': []}, headers=REV_HEADER))
    def test_post_images_embed(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine', {**MY_POST, 'embed': {
            '$type': 'app.bsky.embed.images',
            'images': [{
                'image': BLOB,
                'alt': 'a pic',
                'aspectRatio': {'width': 4, 'height': 3},
            }],
        }})

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({
            '$type': 'app.bsky.embed.images#view',
            'images': [{
                'thumb': GET_BLOB_URL,
                'fullsize': GET_BLOB_URL,
                'alt': 'a pic',
                'aspectRatio': {'width': 4, 'height': 3},
            }],
        }, resp.json['feed'][0]['post']['embed'])

    @patch.dict(os.environ, {'APPVIEW_HOST': 'a.pp'})
    @patch('webutil.util.session.request', return_value=requests_response(
        {'feed': []}, headers=REV_HEADER))
    def test_post_images_embed_cdn(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine', {**MY_POST, 'embed': {
            '$type': 'app.bsky.embed.images',
            'images': [{'image': BLOB, 'alt': ''}],
        }})

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({
            '$type': 'app.bsky.embed.images#view',
            'images': [{
                'thumb': f'https://cdn.bsky.app/img/feed_thumbnail/plain/did:web:user.com/{BLOB_CID.encode("base32")}@jpeg',
                'fullsize': f'https://cdn.bsky.app/img/feed_fullsize/plain/did:web:user.com/{BLOB_CID.encode("base32")}@jpeg',
                'alt': '',
            }],
        }, resp.json['feed'][0]['post']['embed'])

    @patch('webutil.util.session.request', return_value=requests_response(
        {'feed': []}, headers=REV_HEADER))
    def test_post_external_embed(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine', {**MY_POST, 'embed': {
            '$type': 'app.bsky.embed.external',
            'external': {
                'uri': 'https://li.nk/',
                'title': 'a link',
                'description': 'about it',
                'thumb': BLOB,
            },
        }})

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({
            '$type': 'app.bsky.embed.external#view',
            'external': {
                'uri': 'https://li.nk/',
                'title': 'a link',
                'description': 'about it',
                'thumb': GET_BLOB_URL,
            },
        }, resp.json['feed'][0]['post']['embed'])

    @patch('webutil.util.session.request', return_value=requests_response(
        {'feed': []}, headers=REV_HEADER))
    def test_post_quote_embed_omitted(self, _, __):
        """Quote post views need the quoted post's view, which only the appview has."""
        self.commit('app.bsky.feed.post', '3kmine', {**MY_POST, 'embed': {
            '$type': 'app.bsky.embed.record',
            'record': {'uri': OTHER_POST['uri'], 'cid': OTHER_CID},
        }})

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertNotIn('embed', resp.json['feed'][0]['post'])

    @patch('webutil.util.session.request',
           return_value=requests_response(MY_PROFILE_VIEW, headers=REV_HEADER))
    def test_profile(self, _, __):
        self.commit('app.bsky.actor.profile', 'self', {
            '$type': 'app.bsky.actor.profile',
            'displayName': 'New',
            'avatar': BLOB,
            'banner': BLOB,
        })

        resp = self.get('app.bsky.actor.getProfile', actor='did:web:user.com')
        self.assertEqual({
            'did': 'did:web:user.com',
            'handle': 'han.dull',
            'displayName': 'New',
            'avatar': GET_BLOB_URL,
            'banner': GET_BLOB_URL,
        }, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response({
        'profiles': [
            {'did': 'did:plc:other', 'handle': 'oth.er', 'displayName': 'Other'},
            MY_PROFILE_VIEW,
        ],
    }, headers=REV_HEADER))
    def test_profiles(self, _, __):
        self.commit('app.bsky.actor.profile', 'self', {
            '$type': 'app.bsky.actor.profile',
            'displayName': 'New',
        })

        resp = self.get('app.bsky.actor.getProfiles',
                        actors=['did:plc:other', 'did:web:user.com'])
        self.assertEqual({'profiles': [
            {'did': 'did:plc:other', 'handle': 'oth.er', 'displayName': 'Other'},
            {'did': 'did:web:user.com', 'handle': 'han.dull', 'displayName': 'New'},
        ]}, resp.json)

    @patch('webutil.util.session.request',
           return_value=requests_response({'feed': []}, headers=REV_HEADER))
    def test_like_own_new_post(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine', MY_POST)
        self.commit('app.bsky.feed.like', '3klike', {
            **MY_LIKE,
            'subject': {'uri': MY_POST_URI, 'cid': MY_POST_VIEW['cid']},
        })

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({'feed': [{'post': {
            **MY_POST_VIEW,
            'likeCount': 1,
            'viewer': {'like': MY_LIKE_URI},
        }}]}, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response({
        'feed': [{
            'post': OTHER_REPLY,
            'reply': {
                'root': {'$type': 'app.bsky.feed.defs#postView', **OTHER_POST},
                'parent': {'$type': 'app.bsky.feed.defs#postView', **OTHER_POST},
            },
        }],
    }, headers=REV_HEADER))
    def test_like_reply_root_and_parent_in_feed(self, _, __):
        self.commit('app.bsky.feed.like', '3klike', MY_LIKE)

        resp = self.get('app.bsky.feed.getTimeline')
        liked = {
            '$type': 'app.bsky.feed.defs#postView',
            **OTHER_POST,
            'likeCount': 1,
            'viewer': {'like': MY_LIKE_URI},
        }
        self.assertEqual({'feed': [{
            'post': OTHER_REPLY,
            'reply': {'root': liked, 'parent': liked},
        }]}, resp.json)

    @patch('webutil.util.session.request',
           return_value=requests_response(OTHER_FEED, headers=REV_HEADER))
    def test_like_in_other_actors_author_feed(self, _, __):
        self.commit('app.bsky.feed.like', '3klike', MY_LIKE)

        resp = self.get('app.bsky.feed.getAuthorFeed', actor='did:plc:other')
        self.assertEqual({'feed': [{'post': {
            **OTHER_POST,
            'likeCount': 1,
            'viewer': {'like': MY_LIKE_URI},
        }}]}, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response({
        'posts': [OTHER_POST, OTHER_REPLY],
    }, headers=REV_HEADER))
    def test_posts_like(self, _, __):
        self.commit('app.bsky.feed.like', '3klike', MY_LIKE)

        resp = self.get('app.bsky.feed.getPosts', uris=[OTHER_POST['uri'],
                                                        OTHER_REPLY['uri']])
        self.assertEqual({'posts': [
            {**OTHER_POST, 'likeCount': 1, 'viewer': {'like': MY_LIKE_URI}},
            OTHER_REPLY,
        ]}, resp.json)

    @patch('webutil.util.session.request',
           return_value=requests_response(OTHER_FEED, headers=REV_HEADER))
    def test_like(self, _, __):
        self.commit('app.bsky.feed.like', '3klike', MY_LIKE)

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({'feed': [{'post': {
            **OTHER_POST,
            'likeCount': 1,
            'viewer': {'like': MY_LIKE_URI},
        }}]}, resp.json)

    @patch('webutil.util.session.request')
    def test_unlike(self, mock_request, _):
        self.commit('app.bsky.feed.like', '3klike', MY_LIKE)
        mock_request.return_value = requests_response({'feed': [{'post': {
            **OTHER_POST,
            'likeCount': 1,
            'viewer': {'like': MY_LIKE_URI},
        }}]}, headers={'Atproto-Repo-Rev': self.repo.head.decoded['rev']})

        self.commit('app.bsky.feed.like', '3klike')

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({'feed': [{'post': {
            **OTHER_POST,
            'likeCount': 0,
            'viewer': {},
        }}]}, resp.json)

    @patch('webutil.util.session.request',
           return_value=requests_response(OTHER_FEED, headers=REV_HEADER))
    def test_like_then_unlike(self, _, __):
        self.commit('app.bsky.feed.like', '3klike', MY_LIKE)
        self.commit('app.bsky.feed.like', '3klike')

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual(OTHER_FEED, resp.json)

    @patch('webutil.util.session.request',
           return_value=requests_response(OTHER_FEED, headers=REV_HEADER))
    def test_repost(self, _, __):
        self.commit('app.bsky.feed.repost', '3krepost', {
            '$type': 'app.bsky.feed.repost',
            'subject': {'uri': OTHER_POST['uri'], 'cid': OTHER_CID},
            'createdAt': '2022-01-02T03:04:05.000Z',
        })

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual({'feed': [{'post': {
            **OTHER_POST,
            'repostCount': 1,
            'viewer': {
                'repost': 'at://did:web:user.com/app.bsky.feed.repost/3krepost',
            },
        }}]}, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response({
        'thread': {
            '$type': 'app.bsky.feed.defs#threadViewPost',
            'post': OTHER_POST,
            'replies': [{
                '$type': 'app.bsky.feed.defs#threadViewPost',
                'post': {**OTHER_REPLY, 'record': {
                    **OTHER_POST['record'],
                    'reply': {
                        'root': {'uri': OTHER_POST['uri'], 'cid': OTHER_CID},
                        'parent': {'uri': OTHER_POST['uri'], 'cid': OTHER_CID},
                    },
                }},
            }],
        },
    }, headers=REV_HEADER))
    def test_post_thread_like_reply(self, _, __):
        self.commit('app.bsky.feed.like', '3klike', {
            **MY_LIKE,
            'subject': {'uri': OTHER_REPLY['uri'], 'cid': OTHER_CID},
        })

        resp = self.get('app.bsky.feed.getPostThread', uri=OTHER_POST['uri'])
        self.assertEqual(OTHER_POST, resp.json['thread']['post'])
        reply = resp.json['thread']['replies'][0]['post']
        self.assertEqual(1, reply['likeCount'])
        self.assertEqual({'like': MY_LIKE_URI}, reply['viewer'])

    @patch('webutil.util.session.request', return_value=requests_response({
        'thread': [{
            'uri': OTHER_POST['uri'],
            'depth': 0,
            'value': {
                '$type': 'app.bsky.unspecced.defs#threadItemPost',
                'post': OTHER_POST,
                'moreParents': False,
                'moreReplies': 0,
                'opThread': False,
                'hiddenByThreadgate': False,
                'mutedByViewer': False,
            },
        }],
        'hasOtherReplies': False,
    }, headers=REV_HEADER))
    def test_post_thread_v2_like(self, _, __):
        self.commit('app.bsky.feed.like', '3klike', MY_LIKE)

        resp = self.get('app.bsky.unspecced.getPostThreadV2', anchor=OTHER_POST['uri'])
        self.assertEqual({
            **OTHER_POST,
            'likeCount': 1,
            'viewer': {'like': MY_LIKE_URI},
        }, resp.json['thread'][0]['value']['post'])

    @patch('webutil.util.session.request', return_value=requests_response({'feed': [{
        'post': {**OTHER_REPLY, 'embed': {
            '$type': 'app.bsky.embed.record#view',
            'record': {
                '$type': 'app.bsky.embed.record#viewRecord',
                'uri': OTHER_POST['uri'],
                'cid': OTHER_CID,
                'author': OTHER_POST['author'],
                'value': OTHER_POST['record'],
                'indexedAt': OTHER_POST['indexedAt'],
            },
        }},
    }]}, headers=REV_HEADER))
    def test_like_ignores_quoted_view_record(self, mock_request, _):
        self.commit('app.bsky.feed.like', '3klike', MY_LIKE)

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual(mock_request.return_value.json(), resp.json)

    @patch('webutil.util.session.request',
           return_value=requests_response(OTHER_FEED))
    def test_no_rev_header(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine', MY_POST)

        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual(OTHER_FEED, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response(
        OTHER_FEED, headers={
            **REV_HEADER,
            # requests has already decoded the body by the time we see it
            'Content-Encoding': 'gzip',
            'Content-Length': '999',
        }))
    def test_no_local_records(self, mock_request, _):
        resp = self.get('app.bsky.feed.getTimeline')
        self.assertEqual(OTHER_FEED, resp.json)
        self.assertEqual(INIT_REV, resp.headers['Atproto-Repo-Rev'])
        self.assertNotIn('Content-Encoding', resp.headers)
        self.assertEqual(str(len(resp.get_data())), resp.headers['Content-Length'])
        self.assertFalse(mock_request.call_args.kwargs['stream'])

    @patch('webutil.util.session.request', return_value=requests_response(
        {'error': 'InvalidRequest', 'message': 'nope'}, status=400,
        headers=REV_HEADER))
    def test_error_status(self, _, __):
        self.commit('app.bsky.feed.post', '3kmine', MY_POST)

        resp = self.client.get('/xrpc/app.bsky.feed.getTimeline')
        self.assertEqual(400, resp.status_code)
        self.assertEqual({'error': 'InvalidRequest', 'message': 'nope'}, resp.json)

    @patch('webutil.util.session.request', return_value=requests_response(
        {'feed': 'not a list'}, headers=REV_HEADER))
    def test_invalid_upstream_output(self, mock_request, _):
        self.commit('app.bsky.feed.post', '3kmine', MY_POST)

        resp = self.client.get('/xrpc/app.bsky.feed.getTimeline')
        self.assertEqual(200, resp.status_code)
        self.assertEqual(mock_request.return_value.content, resp.get_data())
