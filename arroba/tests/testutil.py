"""Common test utility code."""
from datetime import datetime, timezone
import json
import random
import os
import unittest
from unittest.mock import ANY, call

import dag_cbor.random
from flask import Flask, request
from google.auth.credentials import AnonymousCredentials
from google.cloud import ndb
from multiformats import CID
import requests

from ..datastore_storage import DatastoreStorage
from ..repo import Repo
from .. import server
from ..storage import MemoryStorage
from .. import util
from ..util import datetime_to_tid, next_tid, new_key

NOW = datetime(2022, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

# render just base32 suffix of CIDs for readability in test output
CID.__str__ = CID.__repr__ = lambda cid: '…' + cid.encode('base32')[-7:]

# don't truncate assertion error diffs
import unittest.util
unittest.util._MAX_LENGTH = 999999

os.environ.setdefault('DATASTORE_EMULATOR_HOST', 'localhost:8089')


def requests_response(body, status=200):
    """
    Args:
      body: dict or list, JSON response

    Returns:
      :class:`requests.Response`
    """
    resp = requests.Response()

    if isinstance(body, (dict, list)):
        resp.headers['content-type'] = 'application/json'
        resp._text = json.dumps(body, indent=2)
    else:
        resp._text = body

    resp._content = resp._text.encode()
    resp.encoding = 'utf-8'
    resp.status_code = status
    return resp


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

        # reuse this because it's expensive to generate
        if not TestCase.key:
            TestCase.key = util.new_key()

        os.environ.setdefault('PDS_HOST', 'localhost:8080')
        os.environ.setdefault('PLC_HOST', 'plc.bsky-sandbox.dev')
        os.environ.setdefault('REPO_PASSWORD', 'sooper-sekret')
        os.environ.setdefault('REPO_TOKEN', 'towkin')

    @staticmethod
    def random_keys_and_cids(num):
        timestamps = random.choices(
            range(int(datetime(2020, 1, 1).timestamp()) * 1000,
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
        return {next_tid(): {'foo': random.randint(1, 999999999)} for i in range(num)}


class DatastoreTest(TestCase):
    ndb_client = ndb.Client(project='app', credentials=AnonymousCredentials())

    def setUp(self):
        super().setUp()
        self.storage = DatastoreStorage()

        # clear datastore
        requests.post(f'http://{self.ndb_client.host}/reset')

        # disable in-memory cache
        # https://github.com/googleapis/python-ndb/issues/888
        self.ndb_context = self.ndb_client.context(cache_policy=lambda key: False)
        self.ndb_context.__enter__()

    def tearDown(self):
        self.ndb_context.__exit__(None, None, None)
        super().tearDown()


class XrpcTestCase(TestCase):
    STORAGE_CLS = MemoryStorage
    app = Flask(__name__, static_folder=None)

    def setUp(self):
        super().setUp()

        server.storage = self.STORAGE_CLS()
        self.repo = Repo.create(server.storage, 'did:web:user.com',
                                signing_key=self.key)

        self.request_context = self.app.test_request_context('/')
        self.request_context.push()

        os.environ.update({
            'REPO_PASSWORD': 'sooper-sekret',
            'REPO_TOKEN': 'towkin',
        })
        request.headers = {'Authorization': 'Bearer towkin'}

    def tearDown(self):
        self.request_context.pop()
        super().tearDown()
