"""Unit tests for xrpc_identity.py.

TODO:
* did:plc support
"""
from arroba import xrpc_identity

from . import testutil


class XrpcIdentityTest(testutil.XrpcTestCase):

    # based on atproto/packages/pds/tests/handles.test.ts
    def test_resolve_handle(self):
        resp = xrpc_identity.resolve_handle({},
            handle='user.com',
        )
        self.assertEqual({'did': 'did:web:user.com'}, resp)

    def test_resolve_non_normalized_handle(self):
        resp = xrpc_identity.resolve_handle({},
            handle='uSeR.cOm',
        )
        self.assertEqual({'did': 'did:web:user.com'}, resp)

    def test_resolve_handle_not_found(self):
        with self.assertRaises(ValueError):
            xrpc_identity.resolve_handle({},
                handle='eve.net',
            )
