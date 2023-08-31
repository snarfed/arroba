"""Unit tests for did.py."""
from unittest.mock import MagicMock, patch

from cryptography.hazmat.primitives.asymmetric import ec
import requests

from .. import did
from .. import util

from .testutil import requests_response, TestCase


class DidTest(TestCase):

    def setUp(self):
        super().setUp()
        self.mock_get = MagicMock(return_value=requests_response({'foo': 'bar'}))

    def test_resolve_plc(self):
        doc = did.resolve_plc('did:plc:123', get_fn=self.mock_get)
        self.assertEqual({'foo': 'bar'}, doc)
        self.mock_get.assert_called_with('https://plc.bsky-sandbox.dev/did:plc:123')

    def test_resolve_plc_bad_input(self):
        for bad in None, 1, 'foo', 'did:web:x':
            with self.assertRaises(ValueError):
                did.resolve_plc(bad)

    def test_resolve_web_no_path(self):
        doc = did.resolve_web('did:web:abc.com', get_fn=self.mock_get)
        self.assertEqual({'foo': 'bar'}, doc)
        self.mock_get.assert_called_with('https://abc.com/.well-known/did.json')

    def test_resolve_web_path(self):
        doc = did.resolve_web('did:web:abc.com:def', get_fn=self.mock_get)
        self.assertEqual({'foo': 'bar'}, doc)
        self.mock_get.assert_called_with('https://abc.com/def/did.json')

    def test_resolve_web_port(self):
        doc = did.resolve_web('did:web:abc.com%3A99', get_fn=self.mock_get)
        self.assertEqual({'foo': 'bar'}, doc)
        self.mock_get.assert_called_with('https://abc.com:99/.well-known/did.json')

    def test_resolve_web_bad_input(self):
        for bad in None, 1, 'foo', 'did:plc:x':
            with self.assertRaises(ValueError):
                did.resolve_web(bad)

    def test_resolve(self):
        doc = did.resolve('did:plc:123', get_fn=self.mock_get)
        self.assertEqual({'foo': 'bar'}, doc)
        self.mock_get.assert_called_with('https://plc.bsky-sandbox.dev/did:plc:123')

        doc = did.resolve('did:web:abc.com', get_fn=self.mock_get)
        self.assertEqual({'foo': 'bar'}, doc)
        self.mock_get.assert_called_with('https://abc.com/.well-known/did.json')

    def test_create_plc(self):
        mock_post = MagicMock(return_value=requests_response('OK'))
        did_plc = did.create_plc('han.dull', post_fn=mock_post)
        mock_post.assert_called_with(f'https://plc.bsky-sandbox.dev/{did_plc.did}',
                                     json=did_plc.doc)

        self.assertTrue(did_plc.did.startswith('did:plc:'))
        self.assertEqual(32, len(did_plc.did))
        self.assertIsInstance(did_plc.privkey, ec.EllipticCurvePrivateKey)

        self.assertTrue(did_plc.doc.pop('rotationKeys')[0].startswith('did:key:'))
        self.assertTrue(did_plc.doc.pop('verificationMethods')['atproto']
                        .startswith('did:key:'))

        util.verify_sig(did_plc.doc, did_plc.privkey.public_key())
        did_plc.doc.pop('sig')

        self.assertEqual({
            'type': 'plc_operation',
            'alsoKnownAs': [
                'at://han.dull',
            ],
            'services': {
                'atproto_pds': {
                    'type': 'AtprotoPersonalDataServer',
                    'endpoint': 'https://localhost:8080',
                }
            },
            'prev': None,
        }, did_plc.doc)
