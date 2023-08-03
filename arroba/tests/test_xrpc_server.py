"""Unit tests for xrpc_server.py."""
from flask import Flask, request

from .. import server
from .. import xrpc_server
from . import testutil

app = Flask(__name__, static_folder=None)


class XrpcServerTest(testutil.TestCase):

    def setUp(self):
        super().setUp()
        server.init()

        self.request_context = app.test_request_context('/')
        self.request_context.push()

        request.headers = {}

    def tearDown(self):
        self.request_context.pop()
        super().tearDown()

    # based on atproto/packages/pds/tests/account.test.ts
    def test_create_session(self):
        resp = xrpc_server.create_session({
            'identifier': 'user.com',
            'password': 'sooper-sekret',
        })
        self.assertEqual({
            'handle': 'user.com',
            'did': 'did:web:user.com',
            'accessJwt': 'towkin',
            'refreshJwt': 'towkin',
        }, resp)

        request.headers['Authorization'] = 'Bearer towkin'
        resp = xrpc_server.get_session({})
        self.assertEqual({
            'handle': 'user.com',
            'did': 'did:web:user.com',
        }, resp)

    def test_create_session_fail(self):
        with self.assertRaises(ValueError):
            resp = xrpc_server.create_session({
                'identifier': 'nope.com',
                'password': 'sooper-sekret',
            })

        with self.assertRaises(ValueError):
            resp = xrpc_server.create_session({
                'identifier': 'user.com',
                'password': 'nope',
            })

    def test_get_session_not_logged_in(self):
        with self.assertRaises(ValueError):
            resp = xrpc_server.get_session({})

        request.headers['Authorization'] = 'Bearer nope'
        with self.assertRaises(ValueError):
            resp = xrpc_server.get_session({})
