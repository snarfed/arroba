"""Common test utility code."""
from datetime import datetime, timezone
from multiformats import CID
import random
import unittest
from unittest.mock import ANY, call

from Crypto.PublicKey import ECC
import dag_cbor.random

from .. import util
from ..util import datetime_to_tid

NOW = datetime(2022, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

# always show base32 CIDs in test output
CID.__str__ = CID.__repr__ = lambda cid: cid.encode('base32')


class TestCase(unittest.TestCase):
    maxDiff = None
    key = None

    def setUp(self):
        super().setUp()

        util.now = lambda **kwargs: NOW
        util.time_ns = lambda: int(NOW.timestamp() * 1000 * 1000)

        # make random test data deterministic
        util._clockid = 17
        random.seed(1234567890)
        dag_cbor.random.set_options(seed=1234567890)
        util._randfunc = random.randbytes

        # reuse this because it's expensive to generate
        if not TestCase.key:
            TestCase.key = ECC.generate(curve='P-256', randfunc=random.randbytes)


    @staticmethod
    def random_keys_and_cids(num):
        return [(f'com.example.record/{TestCase.random_tid()}', cid)
                for cid in dag_cbor.random.rand_cid(num)]

    @staticmethod
    def random_tid():
        ms = random.randint(datetime(2020, 1, 1).timestamp() * 1000,
                            datetime(2024, 1, 1).timestamp() * 1000)
        return datetime_to_tid(datetime.fromtimestamp(float(ms) / 1000))
