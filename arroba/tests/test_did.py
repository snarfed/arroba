"""Unit tests for did.py."""
from unittest.mock import patch

import requests

from .. import did

from .testutil import requests_response, TestCase


class DidTest(TestCase):

    @patch('requests.get', return_value=requests_response({'foo': 'bar'}))
    def test_resolve_plc(self, mock_get):
        self.assertEqual({'foo': 'bar'}, did.resolve_plc('did:plc:123'))
        mock_get.assert_called_with('https://plc.bsky-sandbox.dev/did:plc:123')

    def test_resolve_plc_bad_input(self):
        for bad in None, 1, 'foo', 'did:web:x':
            with self.assertRaises(ValueError):
                did.resolve_plc(bad)
