"""Unit tests for util.py."""
from multiformats import CID

from ..util import (
    at_uri,
    dag_cbor_cid,
    datetime_to_tid,
    new_key,
    next_tid,
    sign,
    tid_to_datetime,
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

    def test_sign_and_verify(self):
        key = new_key()
        commit = {'foo': 'bar'}
        sign(commit, key)
        assert verify_sig(commit, key.public_key())

    def test_verify_sig_error(self):
        key = new_key()
        with self.assertRaises(KeyError):
            self.assertFalse(verify_sig({'foo': 'bar'}, key.public_key()))

    def test_verify_sig_fail(self):
        key = new_key()
        self.assertFalse(verify_sig({'foo': 'bar', 'sig': 'nope'},
                                    key.public_key()))

    def test_next_tid(self):
        first = next_tid()
        second = next_tid()
        self.assertGreater(second, first)

    def test_at_uri(self):
        with self.assertRaises(AssertionError):
            at_uri(None, '', None)

        uri = at_uri('did:web:user.com', 'app.bsky.feed.post', 123)
        self.assertEqual('at://did:web:user.com/app.bsky.feed.post/123', uri)
