"""Unit tests for util.py."""
from Crypto.PublicKey import ECC
from datetime import datetime, timezone
from multiformats import CID

from arroba.util import (
    dag_cbor_cid,
    datetime_to_tid,
    new_p256_key,
    sign_commit,
    tid_to_datetime,
    verify_commit_sig,
)
from . import testutil


NOW = datetime(2022, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

class UtilTest(testutil.TestCase):

    def test_dag_cbor_cid(self):
        self.assertEqual(
            CID.decode('bafyreiblaotetvwobe7cu2uqvnddr6ew2q3cu75qsoweulzku2egca4dxq'),
            dag_cbor_cid({'foo': 'bar'}))

    def test_datetime_to_tid(self):
        self.assertEqual('3iom4o4g6u2l2', datetime_to_tid(NOW))

    def test_tid_to_datetime(self):
        self.assertEqual(NOW, tid_to_datetime('3iom4o4g6u2l2'))

    def test_sign_commit_and_verify(self):
        key = new_p256_key()
        commit = {'foo': 'bar'}
        sign_commit(commit, key)
        assert verify_commit_sig(commit, key)

    def test_verify_commit_error(self):
        key = new_p256_key()
        with self.assertRaises(KeyError):
            self.assertFalse(verify_commit_sig({'foo': 'bar'}, key))

    def test_verify_commit_fail(self):
        key = new_p256_key()
        self.assertFalse(verify_commit_sig({'foo': 'bar', 'sig': 'nope'}, key))
