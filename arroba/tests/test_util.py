"""Unit tests for util.py."""
from datetime import timedelta

import jwt
from multiformats import CID

from ..util import (
    at_uri,
    dag_cbor_cid,
    datetime_to_tid,
    next_tid,
    parse_at_uri,
    int_to_tid,
    service_jwt,
    sign,
    tid_to_datetime,
    tid_to_int,
    verify_sig,
)
from .testutil import NOW, TestCase


class UtilTest(TestCase):

    def test_dag_cbor_cid(self):
        self.assertEqual(
            CID.decode('bafyreiblaotetvwobe7cu2uqvnddr6ew2q3cu75qsoweulzku2egca4dxq'),
            dag_cbor_cid({'foo': 'bar'}))

    def test_datetime_to_tid(self):
        self.assertEqual('3iom4o4g6u2l2', datetime_to_tid(NOW))

    def test_tid_to_datetime(self):
        self.assertEqual(NOW, tid_to_datetime('3iom4o4g6u2l2'))

    def test_int_to_tid(self):
        self.assertEqual('22222222222l2', int_to_tid(0))
        self.assertEqual('2222222222222', int_to_tid(0, clock_id=0))
        self.assertEqual('2222222223el2', int_to_tid(42))
        self.assertEqual('3iom4o4g6u2l2',
                         int_to_tid(int(NOW.timestamp() * 1000 * 1000)))

    def test_tid_to_int(self):
        self.assertEqual(0, tid_to_int('22222222222l2'))
        self.assertEqual(0, tid_to_int('2222222222222'))
        self.assertEqual(42, tid_to_int('2222222223el2'))
        self.assertEqual(int(NOW.timestamp() * 1000 * 1000),
                         tid_to_int('3iom4o4g6u2l2'))

    def test_sign_and_verify(self):
        commit = {'foo': 'bar'}
        sign(commit, self.key)
        assert verify_sig(commit, self.key.public_key())

    def test_verify_sig_error(self):
        with self.assertRaises(KeyError):
            self.assertFalse(verify_sig({'foo': 'bar'}, self.key.public_key()))

    def test_verify_sig_fail(self):
        self.assertFalse(verify_sig({'foo': 'bar', 'sig': 'nope'},
                                    self.key.public_key()))

    def test_next_tid(self):
        self.assertEqual('3iom4o4g6u2l2', next_tid())
        self.assertEqual('3iom4o4g6u3l2', next_tid())

    def test_at_uri(self):
        with self.assertRaises(AssertionError):
            at_uri(None, '', None)

        uri = at_uri('did:web:user.com', 'app.bsky.feed.post', 123)
        self.assertEqual('at://did:web:user.com/app.bsky.feed.post/123', uri)

    def test_parse_at_uri(self):
        for bad in None, '', 'http://foo':
            with self.assertRaises(ValueError):
                parse_at_uri(bad)

        for uri, expected in [
                ('at://did:foo/co.ll/123', ('did:foo', 'co.ll', '123')),
                ('at://did:foo/co.ll/', ('did:foo', 'co.ll', '')),
                ('at://did:foo', ('did:foo', '', '')),
        ]:
            self.assertEqual(expected, parse_at_uri(uri))

    def test_service_jwt(self):
        token = service_jwt('relay.local', 'did:web:user.com', self.key)
        decoded = jwt.decode(token, self.key, algorithms=['ES256K'],
                             audience='did:web:relay.local',
                             leeway=timedelta(weeks=9999))
        self.assertEqual({
            'alg': 'ES256K',
            'aud': 'did:web:relay.local',
            'exp': 1641093245,
            'iss': 'did:web:user.com',
        }, decoded)

    def test_service_jwt_aud(self):
        token = service_jwt('relay.local', 'did:web:user.com', self.key,
                            aud='did:plc:aud')
        decoded = jwt.decode(token, self.key, algorithms=['ES256K'],
                             audience='did:plc:aud',
                             leeway=timedelta(weeks=9999))
        self.assertEqual({
            'alg': 'ES256K',
            'aud': 'did:plc:aud',
            'exp': 1641093245,
            'iss': 'did:web:user.com',
        }, decoded)

