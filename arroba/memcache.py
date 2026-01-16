"""Utilities for caching data in memcache."""
from datetime import timedelta
import logging
import time

from . import util

logger = logging.getLogger(__name__)


class Lease:
    """Memcache-based advisory lease.

    Uses memcache's atomic add operation to implement a lease with blocking acquire
    and automatic expiration. Can be used as a context manager. Not reusable.

    Example:
        lease = Lease('key', 'val')
        with lease:
            # ...

    Attributes:
      client (pymemcache.client.base.Client)
      key (str)
      retries (int)
      initial_retry_delay (timedelta)
      expiration (timedelta): how long to wait before automatically releasing
      expires_at (datetime): if the lease is held, when it will expire

    """
    def __init__(self, client, key, expiration=timedelta(minutes=5), retries=6,
                 initial_retry_delay=timedelta(seconds=5)):
        """Constructor.

        Args:
          client (pymemcache.client.base.Client)
          key (str): memcache key to use for the lease
          expiration (timedelta): how long to wait before automatically releasing
          expires_at (datetime): if the lease is held, when it will expire
          retries (int): number of times to retry acquiring the lease
          initial_retry_delay (timedelta): initial delay between retries; doubles
            each retry
        """
        self.client = client
        self.key = key
        self.expiration = expiration
        self.retries = retries
        self.initial_retry_delay = initial_retry_delay
        self.expires_at = None

    def acquire(self):
        """Acquire the lease, retrying with exponential backoff if necessary.

        Raises:
          RuntimeError: if the lease could not be acquired after all retries
        """
        assert not self.expires_at

        delay = self.initial_retry_delay

        for attempt in range(self.retries + 1):
            # add returns True if the key didn't exist and was set,
            # False if it already existed
            if self.client.add(self.key, 'locked', noreply=False,
                               expire=int(self.expiration.total_seconds())):
                self.expires_at = util.now() + self.expiration
                logger.info(f'acquired memcache lease {self.key}')
                return

            if attempt < self.retries:
                logger.info(f'memcache lease {self.key} already held, sleeping {delay}s')
                time.sleep(delay.total_seconds())
                delay *= 2

        raise RuntimeError(f"couldn't acquire memcache lease {self.key} after {self.retries + 1} attempts")

    def release(self):
        """Release the lease if we still hold it (hasn't expired)."""
        assert self.expires_at

        if util.now() <= self.expires_at:
            self.client.delete(self.key)
            logger.info(f'released memcache lease {self.key}')
        else:
            logger.warning(f'memcache lease {self.key} expired before release')

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False



