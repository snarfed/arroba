"""Common test utility code."""
from datetime import datetime, timezone
import random
import os
import unittest
from unittest.mock import ANY, call

from Crypto.PublicKey import ECC
import dag_cbor.random
from flask import Flask, request
from multiformats import CID

from .. import server
from .. import util
from ..util import datetime_to_tid, next_tid

NOW = datetime(2022, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

# render just base32 suffix of CIDs for readability in test output
CID.__str__ = CID.__repr__ = lambda cid: '…' + cid.encode('base32')[-7:]

# don't truncate assertion error diffs
import unittest.util
unittest.util._MAX_LENGTH = 999999


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
        timestamps = random.choices(range(int(datetime(2020, 1, 1).timestamp()) * 1000,
                                          int(datetime(2100, 1, 1).timestamp()) * 1000),
                                    k=num)
        cids = set()
        for cid in dag_cbor.random.rand_cid():
            cids.add(cid)
            if len(cids) == num:
                break

        return [(f'com.example.record/{datetime_to_tid(datetime.fromtimestamp(float(ts) / 1000))}', cid)
                for ts, cid in zip(timestamps, cids)]

    @staticmethod
    def random_objects(num):
        return {next_tid(): {'foo': random.randint(0, 1000)} for i in range(num)}


class XrpcTestCase(TestCase):
    app = Flask(__name__, static_folder=None)

    def setUp(self):
        super().setUp()
        server.init()

        self.request_context = self.app.test_request_context('/')
        self.request_context.push()

        os.environ.update({
            'ARROBA_PASSWORD': 'sooper-sekret',
            'ARROBA_JWT': 'towkin',
        })
        request.headers = {'Authorization': 'Bearer towkin'}

    def tearDown(self):
        self.request_context.pop()
        super().tearDown()

