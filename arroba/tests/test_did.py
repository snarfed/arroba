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

    @patch('requests.get', return_value=requests_response({'foo': 'bar'}))
    def test_resolve_web_no_path(self, mock_get):
        self.assertEqual({'foo': 'bar'}, did.resolve_web('did:web:abc.com'))
        mock_get.assert_called_with('https://abc.com/.well-known/did.json')

    @patch('requests.get', return_value=requests_response({'foo': 'bar'}))
    def test_resolve_web_path(self, mock_get):
        self.assertEqual({'foo': 'bar'}, did.resolve_web('did:web:abc.com:def'))
        mock_get.assert_called_with('https://abc.com/def/did.json')

    @patch('requests.get', return_value=requests_response({'foo': 'bar'}))
    def test_resolve_web_port(self, mock_get):
        self.assertEqual({'foo': 'bar'}, did.resolve_web('did:web:abc.com%3A99'))
        mock_get.assert_called_with('https://abc.com:99/.well-known/did.json')

    def test_resolve_web_bad_input(self):
        for bad in None, 1, 'foo', 'did:plc:x':
            with self.assertRaises(ValueError):
                did.resolve_web(bad)
