"""Unit tests for xrpc_identity.py."""
from unittest.mock import patch

import dns.resolver
from lexrpc.base import XrpcError
from webutil.testutil import requests_response
from webutil import util

from . import testutil
from .. import xrpc_identity

DID = 'did:plc:123'


# skip the DNS method so that resolution falls through to HTTPS
@patch('dns.resolver.resolve', side_effect=dns.resolver.NXDOMAIN())
class XrpcIdentityTest(testutil.XrpcTestCase):

    @patch.object(util.session, 'get', return_value=requests_response(DID))
    def test_resolve_handle(self, mock_get, _):
        self.assertEqual({'did': 'did:plc:123'},
                         xrpc_identity.resolve_handle({}, handle='foo.com'))
        mock_get.assert_called_with('https://foo.com/.well-known/atproto-did')

    @patch.object(util.session, 'get', return_value=requests_response('', status=404))
    def test_resolve_handle_not_found(self, mock_get, _):
        with self.assertRaises(XrpcError) as cm:
            xrpc_identity.resolve_handle({}, handle='foo.com')

        self.assertEqual('HandleNotFound', cm.exception.name)

        mock_get.assert_called_with('https://foo.com/.well-known/atproto-did')

    def test_resolve_handle_invalid(self, mock_dns):
        with self.assertRaises(ValueError):
            xrpc_identity.resolve_handle({}, handle='not a handle')

        mock_dns.assert_not_called()
