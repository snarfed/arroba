"""Unit tests for did.py."""
import base64
from unittest.mock import MagicMock, patch

from cryptography.hazmat.primitives.asymmetric import ec
import dns.message
import dns.name
from dns.rdataclass import IN
from dns.rdatatype import TXT
from dns.resolver import Answer
import dns.rrset
import requests

from .. import did
from .. import util

from .testutil import requests_response, TestCase


class DidTest(TestCase):

    def setUp(self):
        super().setUp()
        self.mock_get = MagicMock(return_value=requests_response({'foo': 'bar'}))

    def dns_answer(self, name, value):
        qname = dns.name.from_text(name)
        query = dns.message.make_query(qname=qname, rdclass=IN, rdtype=TXT)
        resp = dns.message.make_response(query)
        answer = Answer(qname=qname, rdtype=TXT, rdclass=IN, response=resp)
        answer.rrset = dns.rrset.from_text_list(
            name=qname, rdclass=IN, rdtype=TXT, ttl=300, text_rdatas=[value])
        return answer

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
        genesis_op['sig'] = base64.urlsafe_b64decode(genesis_op['sig'])
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

    def test_encode_decode_did_key(self):
        did_key = did.encode_did_key(self.key.public_key())
        self.assertTrue(did_key.startswith('did:key:'))
        decoded = did.decode_did_key(did_key)
        self.assertEqual(self.key.public_key(), decoded)

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

    @patch('dns.resolver.resolve')
    def test_resolve_handle_dns(self, mock_resolve):
        mock_resolve.return_value = self.dns_answer(
            '_atproto.foo.com.', '"did=did:plc:123abc"')

        self.assertEqual('did:plc:123abc',
                         did.resolve_handle('foo.com', get_fn=self.mock_get))
        mock_resolve.assert_called_once_with('_atproto.foo.com.', TXT)
        self.mock_get.assert_not_called()

    @patch('dns.resolver.resolve')
    def test_resolve_handle_https_well_known(self, mock_resolve):
        mock_resolve.return_value = self.dns_answer('foo.com.', 'nope')

        self.mock_get.return_value = requests_response('did:plc:123abc')
        self.mock_get.return_value.headers['Content-Type'] ='text/plain'

        self.assertEqual('did:plc:123abc',
                         did.resolve_handle('foo.com', get_fn=self.mock_get))
        mock_resolve.assert_called_once_with('_atproto.foo.com.', TXT)
        self.mock_get.assert_called_with('https://foo.com/.well-known/atproto-did')

    @patch('dns.resolver.resolve')
    def test_resolve_handle_nothing(self, mock_resolve):
        mock_resolve.return_value = self.dns_answer('_atproto.foo.com.', 'nope')
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

    @patch('requests.get', return_value = requests_response({'did': 'did:plc:123abc'}))
    def test_resolve_handle_bsky_social_special_case(self, mock_get):
        self.assertEqual('did:plc:123abc', did.resolve_handle('foo.bsky.social'))
        mock_get.assert_called_with(
            'https://bsky.social/xrpc/com.atproto.identity.resolveHandle?handle=foo.bsky.social',
            json=None,
            headers={
                'User-Agent': util.USER_AGENT,
                'Content-Type': 'application/json',
            },
        )

    @patch('requests.get', return_value = requests_response({}, status=400))
    def test_resolve_handle_bsky_social_special_case_not_Found(self, mock_get):
        self.assertIsNone(did.resolve_handle('foo.bsky.social'))
        mock_get.assert_called_with(
            'https://bsky.social/xrpc/com.atproto.identity.resolveHandle?handle=foo.bsky.social',
            json=None,
            headers={
                'User-Agent': util.USER_AGENT,
                'Content-Type': 'application/json',
            },
        )
