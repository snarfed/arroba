"""Unit tests for xrpc_server.py."""
from flask import request

from ..repo import Repo
from .. import server
from .. import xrpc_server
from . import testutil


class XrpcServerTest(testutil.XrpcTestCase):

    def setUp(self):
        super().setUp()
        request.headers = {}
        # TODO: remove once we're generating and parsing tokens to load repos
        server.repo = self.repo

    # based on atproto/packages/pds/tests/account.test.ts
    def test_create_session(self):
        resp = xrpc_server.create_session({
            'identifier': 'did:web:user.com',
            'password': 'sooper-sekret',
        })
        self.assertEqual({
            'handle': 'han.dull',
            'did': 'did:web:user.com',
            'accessJwt': 'towkin',
            'refreshJwt': 'towkin',
        }, resp)

        request.headers['Authorization'] = 'Bearer towkin'
        resp = xrpc_server.get_session({})
        self.assertEqual({
            'handle': 'han.dull',
            'did': 'did:web:user.com',
        }, resp)

    def test_create_session_handle(self):
        Repo.create(server.storage, 'did:plc:abc123', signing_key=self.key,
                    handle='user.handle')

        resp = xrpc_server.create_session({
            'identifier': 'user.handle',
            'password': 'sooper-sekret',
        })
        self.assertEqual({
            'handle': 'user.handle',
            'did': 'did:plc:abc123',
            'accessJwt': 'towkin',
            'refreshJwt': 'towkin',
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
