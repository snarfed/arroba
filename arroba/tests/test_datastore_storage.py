"""Unit tests for ndb_storage.py."""
import os

from google.cloud import ndb
import requests

from arroba.datastore_storage import AtpNode, DatastoreStorage, WriteOnceBlobProperty
from arroba.storage import BlockMap

from .testutil import TestCase

os.environ.setdefault('DATASTORE_EMULATOR_HOST', 'localhost:8089')


class DatastoreStorageTest(TestCase):
    ndb_client = ndb.Client()

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

    def test_write_once(self):
        class Foo(ndb.Model):
            prop = WriteOnceBlobProperty()

        foo = Foo(prop=b'x')
        with self.assertRaises(ndb.ReadonlyPropertyError):
            foo.prop = b'y'
        with self.assertRaises(ndb.ReadonlyPropertyError):
            foo.prop = None

        foo = Foo()
        foo.prop = b'x'
        with self.assertRaises(ndb.ReadonlyPropertyError):
            foo.prop = b'y'

        foo.put()
        foo = foo.key.get()
        with self.assertRaises(ndb.ReadonlyPropertyError):
            foo.prop = b'y'

    def test_read_write_has(self):
        self.assertIsNone(self.storage.read('abc123'))
        self.assertFalse(self.storage.has('abc123'))

        data = {'foo': 'bar'}
        cid = self.storage.write(data)
        self.assertEqual(data, self.storage.read(cid))
        self.assertTrue(self.storage.has(cid))

    def test_read_many(self):
        self.assertEqual((BlockMap(), ['a', 'b', 'c']),
                         self.storage.read_many(['a', 'b', 'c']))

        data = [{'foo': 'bar'}, {'baz': 'biff'}]
        cids = [self.storage.write(d) for d in data]

        map = BlockMap()
        map.add(data[0])
        map.add(data[1])
        self.assertEqual((map, []), self.storage.read_many(cids))
        self.assertEqual((map, ['b']),
                         self.storage.read_many([cids[0], 'b', cids[1]]))

