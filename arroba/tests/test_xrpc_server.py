"""Unit tests for xrpc_server.py."""
from flask import request

from .. import xrpc_server
from . import testutil


class XrpcServerTest(testutil.XrpcTestCase):

    def setUp(self):
        super().setUp()
        request.headers = {}

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
