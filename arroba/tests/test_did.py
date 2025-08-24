"""Unit tests for did.py."""
import base64
import copy
from unittest.mock import MagicMock, patch

from cryptography.hazmat.primitives.asymmetric import ec
from dns.rdatatype import TXT
import dns.resolver
import requests

from .. import did
from .. import util

from .testutil import dns_answer, requests_response, TestCase


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

        self.assertTrue(did_plc.did.startswith('did:plc:'))
        self.assertEqual(32, len(did_plc.did))
        self.assertIsInstance(did_plc.signing_key, ec.EllipticCurvePrivateKey)
        self.assertIsInstance(did_plc.rotation_key, ec.EllipticCurvePrivateKey)
        self.assertNotEqual(did_plc.rotation_key, did_plc.signing_key)

        mock_post.assert_called_once()
        self.assertEqual((f'https://plc.bsky-sandbox.dev/{did_plc.did}',),
                         mock_post.call_args.args)

        genesis_op = mock_post.call_args.kwargs['json']
        self.assertEqual(did_plc.did, genesis_op.pop('did'))
        genesis_op['sig'] = base64.urlsafe_b64decode(
            genesis_op['sig'] + '=' * (4 - len(genesis_op['sig']) % 4))  # padding
        assert util.verify_sig(genesis_op, did_plc.rotation_key.public_key())
        del genesis_op['sig']

        signing_did_key = did.encode_did_key(did_plc.signing_key.public_key())
        rotation_did_key = did.encode_did_key(did_plc.rotation_key.public_key())
        self.assertEqual({
            'type': 'plc_operation',
            'verificationMethods': {
                'atproto': signing_did_key,
            },
            'rotationKeys': [rotation_did_key],
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
        }, genesis_op)

        self.assertEqual({
            '@context': [
                'https://www.w3.org/ns/did/v1',
                'https://w3id.org/security/multikey/v1',
                'https://w3id.org/security/suites/secp256k1-2019/v1',
            ],
            'id': did_plc.did,
            'alsoKnownAs': ['at://han.dull'],
            'verificationMethod': [{
                'id': f'{did_plc.did}#atproto',
                'type': 'EcdsaSecp256r1VerificationKey2019',
                'controller': did_plc.did,
                'publicKeyMultibase': signing_did_key.removeprefix('did:key:'),
            }],
            'service': [{
                'id': '#atproto_pds',
                'type': 'AtprotoPersonalDataServer',
                'serviceEndpoint': 'https://localhost:8080',
            }],
        }, did_plc.doc)

    def test_create_plc_also_known_as(self):
        mock_post = MagicMock(return_value=requests_response('OK'))
        did_plc = did.create_plc('han.dull', also_known_as=['abc', 'xyz'],
                                 post_fn=mock_post)

        mock_post.assert_called_once()
        self.assertEqual((f'https://plc.bsky-sandbox.dev/{did_plc.did}',),
                         mock_post.call_args.args)

        self.assertTrue(did_plc.did.startswith('did:plc:'))
        self.assertEqual(['at://han.dull', 'abc', 'xyz'], did_plc.doc['alsoKnownAs'])
        genesis_op = mock_post.call_args.kwargs['json']
        self.assertEqual(['at://han.dull', 'abc', 'xyz'], genesis_op['alsoKnownAs'])

    def test_update_plc(self):
        did_key = did.encode_did_key(self.key.public_key())
        op = {
            'type': 'plc_operation',
            'services': {
                'atproto_pds': {
                    'type': 'AtprotoPersonalDataServer',
                    'endpoint': 'https://pds',
                },
            },
            'alsoKnownAs': ['at://han.dull'],
            'rotationKeys': [did_key],
            'verificationMethods': {'atproto': did_key}
        }
        mock_get = MagicMock(return_value=requests_response([{
            'did': 'did:plc:xyz',
            'operation': {**op, 'prev': None},
            'cid': 'orig',
            'nullified': False,
        }]))
        mock_post = MagicMock(return_value=requests_response('OK'))

        did_plc = did.update_plc('did:plc:xyz', get_fn=mock_get, post_fn=mock_post,
                                 signing_key=self.key, rotation_key=self.key)
        self.assertEqual('did:plc:xyz', did_plc.did)
        self.assertEqual(self.key, did_plc.signing_key)
        self.assertEqual(self.key, did_plc.rotation_key)
        self.assertEqual(['at://han.dull'], did_plc.doc['alsoKnownAs'])
        self.assertEqual([{
            'id': '#atproto_pds',
            'type': 'AtprotoPersonalDataServer',
            'serviceEndpoint': 'https://localhost:8080',
        }], did_plc.doc['service'])

        mock_post.assert_called_once()
        self.assertEqual((f'https://plc.bsky-sandbox.dev/{did_plc.did}',),
                         mock_post.call_args.args)

        update_op = mock_post.call_args.kwargs['json']
        self.assertEqual('did:plc:xyz', update_op.pop('did'))

        update_op['sig'] = base64.urlsafe_b64decode(
            update_op['sig'] + '=' * (4 - len(update_op['sig']) % 4))  # padding
        assert util.verify_sig(update_op, self.key.public_key())
        del update_op['sig']

        expected = copy.deepcopy(op)
        expected['prev'] = 'orig'
        expected['services']['atproto_pds']['endpoint'] = 'https://localhost:8080'
        self.assertEqual(expected, update_op)

    def test_update_plc_new_handle_pds(self):
        mock_get = MagicMock(return_value=requests_response([{
            'operation': {
                'alsoKnownAs': ['at://han.dull', 'http://han.dy'],
            },
            'cid': 'orig',
        }]))
        mock_post = MagicMock(return_value=requests_response('OK'))

        did_plc = did.update_plc('did:plc:xyz', get_fn=mock_get, post_fn=mock_post,
                                 handle='new.ie', pds_url='http://sur.vur',
                                 signing_key=self.key, rotation_key=self.key)
        self.assertEqual('did:plc:xyz', did_plc.did)
        self.assertEqual(self.key, did_plc.signing_key)
        self.assertEqual(self.key, did_plc.rotation_key)
        self.assertEqual(['at://new.ie', 'http://han.dy'], did_plc.doc['alsoKnownAs'])
        self.assertEqual([{
            'id': '#atproto_pds',
            'type': 'AtprotoPersonalDataServer',
            'serviceEndpoint': 'http://sur.vur',
        }], did_plc.doc['service'])

        mock_post.assert_called_once()
        self.assertEqual((f'https://plc.bsky-sandbox.dev/{did_plc.did}',),
                         mock_post.call_args.args)

        update_op = mock_post.call_args.kwargs['json']
        self.assertEqual('did:plc:xyz', update_op.pop('did'))

        update_op['sig'] = base64.urlsafe_b64decode(
            update_op['sig'] + '=' * (4 - len(update_op['sig']) % 4))  # padding
        assert util.verify_sig(update_op, self.key.public_key())
        del update_op['sig']

        did_key = did.encode_did_key(self.key.public_key())
        self.assertEqual({
            'type': 'plc_operation',
            'prev': 'orig',
            'services': {
                'atproto_pds': {
                    'type': 'AtprotoPersonalDataServer',
                    'endpoint': 'http://sur.vur',
                },
            },
            'alsoKnownAs': ['at://new.ie', 'http://han.dy'],
            'rotationKeys': [did_key],
            'verificationMethods': {'atproto': did_key}
        }, update_op)

    def test_encode_decode_did_key(self):
        did_key = did.encode_did_key(self.key.public_key())
        self.assertTrue(did_key.startswith('did:key:'))
        decoded = did.decode_did_key(did_key)
        self.assertEqual(self.key.public_key(), decoded)

    def test_get_handle(self):
        for doc in {}, {'alsoKnownAs': []},  {'alsoKnownAs': ['asdf']}:
            self.assertIsNone(did.get_handle(doc))

        self.assertEqual('did:123', did.get_handle({
            'alsoKnownAs': ['foo', 'did:nope', 'at://did:123', 'bar'],
        }))

    def test_get_signing_key(self):
        self.assertIsNone(did.get_signing_key({}))

        self.assertIsNone(did.get_signing_key({
            'id': 'did:plc:123abc',
            'verificationMethod': [{
                'id': 'did:plc:other#atproto',
                'publicKeyMultibase': 'unused',
            }],
        }))

        got = did.get_signing_key({
            'id': 'did:plc:123abc',
            'verificationMethod': [{
                'id': 'did:plc:123abc#atproto',
                'type': 'Multikey',
                'controller': 'did:plc:5zspv27pk4iqtrl2ql2nykjh',
                'publicKeyMultibase': did.encode_did_key(self.key.public_key()),
            }],
        })
        self.assertEqual(self.key.public_key(), got)

    def test_plc_operation_to_did_doc(self):
        self.assertEqual({
            '@context': [
                'https://www.w3.org/ns/did/v1',
                'https://w3id.org/security/multikey/v1',
                'https://w3id.org/security/suites/secp256k1-2019/v1',
            ],
            'id': 'did:plc:123abc',
            'alsoKnownAs': ['at://alice.example'],
            'verificationMethod': [{
                'id': 'did:plc:123abc#atproto',
                'type': 'EcdsaSecp256r1VerificationKey2019',
                'controller': 'did:plc:123abc',
                'publicKeyMultibase': 'zDnaeh9v2RmcMo13Du2d6pjUf5bZwtauYxj3n9dYjw4EZUAR7',
            }],
            'service': [{
                'id': '#atproto_pds',
                'type': 'AtprotoPersonalDataServer',
                'serviceEndpoint': 'https://pds.example',
            }],
        }, did.plc_operation_to_did_doc({
            'type': 'plc_operation',
            'did': 'did:plc:123abc',
            'verificationMethods': {
                # signing key
                'atproto': 'did:key:zDnaeh9v2RmcMo13Du2d6pjUf5bZwtauYxj3n9dYjw4EZUAR7'
            },
            'rotationKeys': [
                'did:key:rotation-key',
            ],
            'alsoKnownAs': [
                'at://alice.example',
            ],
            'services': {
                'atproto_pds': {
                    'type': 'AtprotoPersonalDataServer',
                    'endpoint': 'https://pds.example',
                },
            },
            'prev': None,
        }))

    def test_resolve_handle_dns_plc(self):
        self._test_resolve_handle_dns('did:plc:123')

    def test_resolve_handle_dns_web(self):
        self._test_resolve_handle_dns('did:web:bar.com')

    @patch('dns.resolver.resolve')
    def _test_resolve_handle_dns(self, val, mock_resolve):
        mock_resolve.return_value = dns_answer(
            '_atproto.foo.com.', f'"did={val}"')
        got = did.resolve_handle('foo.com', get_fn=self.mock_get)
        self.assertEqual(val, got)
        mock_resolve.assert_called_once_with('_atproto.foo.com.', TXT)
        self.mock_get.assert_not_called()

    def test_resolve_handle_https_well_known_plc(self):
        self._test_resolve_handle_https_well_known('did:plc:123')

    def test_resolve_handle_https_well_known_web(self):
        self._test_resolve_handle_https_well_known('did:web:bar.com')

    @patch('dns.resolver.resolve')
    def _test_resolve_handle_https_well_known(self, val, mock_resolve):
        mock_resolve.return_value = dns_answer('foo.com.', 'nope')
        self.mock_get.return_value = requests_response(val)

        self.assertEqual(val, did.resolve_handle('foo.com', get_fn=self.mock_get))
        mock_resolve.assert_called_once_with('_atproto.foo.com.', TXT)
        self.mock_get.assert_called_with('https://foo.com/.well-known/atproto-did')

    @patch('dns.resolver.resolve')
    def test_resolve_handle_https_well_known_not_did(self, mock_resolve):
        mock_resolve.return_value = dns_answer('foo.com.', 'nope')
        self.mock_get.return_value = requests_response('nope')
        self.assertIsNone(did.resolve_handle('foo.com', get_fn=self.mock_get))
        self.mock_get.assert_called_with('https://foo.com/.well-known/atproto-did')

    @patch('dns.resolver.resolve')
    def test_resolve_handle_nothing(self, mock_resolve):
        mock_resolve.return_value = dns_answer('_atproto.foo.com.', 'nope')
        self.mock_get.return_value = requests_response('', status=404)

        self.assertIsNone(did.resolve_handle('foo.com', get_fn=self.mock_get))
        mock_resolve.assert_called_once_with('_atproto.foo.com.', TXT)
        self.mock_get.assert_called_with('https://foo.com/.well-known/atproto-did')

    @patch('dns.resolver.resolve', side_effect=dns.resolver.NXDOMAIN())
    def test_resolve_handle_nothing_dns_nxdomain_exception(self, mock_resolve):
        self.mock_get.return_value = requests_response('', status=404)

        self.assertIsNone(did.resolve_handle('foo.com', get_fn=self.mock_get))
        mock_resolve.assert_called_once_with('_atproto.foo.com.', TXT)
        self.mock_get.assert_called_with('https://foo.com/.well-known/atproto-did')

    @patch('dns.resolver.resolve', side_effect=dns.resolver.NXDOMAIN())
    def test_resolve_handle_request_exception(self, mock_resolve):
        self.mock_get.side_effect = requests.exceptions.InvalidURL('foo')

        self.assertIsNone(did.resolve_handle('.foo.com', get_fn=self.mock_get))

        mock_resolve.assert_called_once_with('_atproto..foo.com.', TXT)
        self.mock_get.assert_called_with('https://.foo.com/.well-known/atproto-did')
