"""Common test utility code."""
from datetime import datetime, timezone
import random
import unittest
from unittest.mock import ANY, call

import dag_cbor.random

from .. import util
from ..util import datetime_to_tid

NOW = datetime(2022, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


class TestCase(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        super().setUp()

        # make random test data deterministic
        util._clockid = 17
        random.seed(1234567890)
        dag_cbor.random.set_options(seed=1234567890)
        util._randfunc = random.randbytes

    @staticmethod
    def random_keys_and_cids(num):
        def tid():
            ms = random.randint(datetime(2020, 1, 1).timestamp() * 1000,
                                datetime(2024, 1, 1).timestamp() * 1000)
            return datetime_to_tid(datetime.fromtimestamp(float(ms) / 1000))

        return [(f'com.example.record/{tid()}', cid)
                for cid in dag_cbor.random.rand_cid(num)]

    def random_tid(num):
        ms = random.randint(datetime(2020, 1, 1).timestamp() * 1000,
                            datetime(2024, 1, 1).timestamp() * 1000)
        tid = datetime_to_tid(datetime.fromtimestamp(float(ms) / 1000))
        return f'com.example.record/{tid}'
