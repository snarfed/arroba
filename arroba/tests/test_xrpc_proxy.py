"""Unit tests for xrpc_proxy.py."""
import gzip
from unittest.mock import patch

from flask import Flask
import jwt
from lexrpc.flask_server import init_flask
from lexrpc.server import Server
from webutil.testutil import requests_response
from webutil.util import HTTP_TIMEOUT

# testutil must come first; it breaks the repo/storage circular import
from . import testutil
from ..repo import Repo
from .. import server
from .. import xrpc_proxy

APPVIEW_DID_DOC = {
    'id': 'did:web:a.pp',
    'service': [{
        'id': '#foo',
        'type': 'Foo',
        'serviceEndpoint': 'https://a.pp',
    }],
}



@patch('arroba.did.resolve', return_value=APPVIEW_DID_DOC)
@patch('webutil.util.session.request',
       side_effect=lambda *args, **kwargs: requests_response({'feed': []}))
class XrpcProxyTest(testutil.TestCase):
    authed_did = 'did:web:user.com'

    def setUp(self):
        super().setUp()

        self.server = Server(validate=False, require_lexicons=False)
        self.app = Flask(__name__, static_folder=None)
        fallback = xrpc_proxy.handler(auth=lambda: self.authed_did)
        init_flask(self.server, self.app, fallback=fallback)

        self.repo = Repo.create(self.storage, 'did:web:user.com', handle='han.dull',
                                signing_key=self.key)
        self.client = self.app.test_client()
        xrpc_proxy.signing_key.cache.clear()

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

    def test_default_service(self, mock_request, _):
        fallback = xrpc_proxy.handler(auth=lambda: self.authed_did,
                                      default_service='did:web:a.pp#foo')
        app = Flask(__name__, static_folder=None)
        init_flask(self.server, app, fallback=fallback)

        resp = app.test_client().get('/xrpc/x.y.query')
        self.assertEqual(200, resp.status_code)
        self.assert_jwt(mock_request, lxm='x.y.query')

    def test_server_methods_not_proxied(self, mock_request, _):
        """com.atproto.server.* is the PDS's own account and session surface."""
        resp = self.client.get('/xrpc/com.atproto.server.getSession',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})
        self.assertEqual(501, resp.status_code)
        self.assertEqual('MethodNotImplemented', resp.json['error'])
        mock_request.assert_not_called()

    def test_no_header_no_default_service(self, mock_request, _):
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
        self.authed_did = None
        resp = self.client.get('/xrpc/x.y.query',
                               headers={'atproto-proxy': 'did:web:a.pp#foo'})

        self.assertEqual(401, resp.status_code)
        self.assertEqual('AuthMissing', resp.json['error'])
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
