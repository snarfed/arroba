"""Unit tests for memcache.py."""
from datetime import timedelta
from unittest.mock import patch

from pymemcache.test.utils import MockMemcacheClient

from .. import memcache
from ..memcache import Lease
from .testutil import NOW, TestCase


@patch('time.sleep')
class LeaseTest(TestCase):
    def test_acquire_and_release(self, _):
        lease = Lease(self.memcache, 'kee')
        lease.acquire()
        self.assertAlmostEqual(NOW + timedelta(minutes=5), lease.expires_at,
                               delta=timedelta(seconds=1))
        self.assertEqual(b'locked', self.memcache.get('kee'))

        lease.release()
        self.assertIsNone(self.memcache.get('kee'))

    def test_context_manager(self, _):
        with Lease(self.memcache, 'kee') as lease:
            self.assertAlmostEqual(NOW + timedelta(minutes=5), lease.expires_at,
                                   delta=timedelta(seconds=1))
            self.assertEqual(b'locked', self.memcache.get('kee'))

        self.assertIsNone(self.memcache.get('kee'))

    def test_acquire_retry_succeeds(self, _):
        # another worker holds the lease
        self.memcache.add('kee', 'locked')

        # simulate expiration by deleting after first attempt
        original_add = self.memcache.add
        attempts = [0]
        def mock_add(key, value, **kwargs):
            attempts[0] += 1
            if attempts[0] > 1:  # second attempt
                self.memcache.delete('kee')
            return original_add(key, value, **kwargs)

        with patch.object(self.memcache, 'add', side_effect=mock_add):
            lease = Lease(self.memcache, 'kee', retries=2,
                          initial_retry_delay=timedelta(seconds=0.1))
            lease.acquire()

        self.assertIsNotNone(NOW, lease.expires_at)
        self.assertEqual(b'locked', self.memcache.get('kee'))
        lease.release()

    def test_acquire_retry_fails(self, _):
        # another worker holds the lease with long expiration
        self.assertTrue(self.memcache.add('kee', 'locked', expire=999))

        lease = Lease(self.memcache, 'kee', retries=2)

        with self.assertRaises(RuntimeError) as ctx:
            lease.acquire()

        self.assertIn("couldn't acquire memcache lease kee after 3 attempts",
                      str(ctx.exception))
        self.assertIsNone(lease.expires_at)

    def test_release_without_acquire(self, _):
        lease = Lease(self.memcache, 'kee')
        with self.assertRaises(AssertionError):
            lease.release()

    def test_release_after_expiration(self, _):
        lease = Lease(self.memcache, 'kee', retries=1,
                      initial_retry_delay=timedelta(seconds=0.1))
        lease.acquire()

        # simulate expiration by setting expires_at far in the past
        lease.expires_at = NOW - timedelta(seconds=9999)

        # another worker could have acquired it
        self.memcache.set('kee', 'locked')

        lease.release()  # should not delete vals2's lease
        self.assertEqual(b'locked', self.memcache.get('kee'))

