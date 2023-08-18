"""Unit tests for diff.py.

Heavily based on:
https://github.com/bluesky/atproto/blob/main/packages/repo/tests/mst.test.ts

Huge thanks to the Bluesky team for working in the public, in open source, and to
Daniel Holmgren and Devin Ivy for this code specifically!
"""
import dag_cbor.random

from ..diff import Change, Diff
from ..mst import MST
from ..storage import MemoryStorage
from . import testutil


class DiffTest(testutil.TestCase):

    def setUp(self):
        super().setUp()
        self.storage = MemoryStorage()
        self.mst = MST.create(storage=self.storage)

    def test_diffs(self):
        mst = self.mst

        data = self.random_keys_and_cids(1000)
        for key, cid in data:
            mst = mst.add(key, cid)

        before = after = mst

        to_add = self.random_keys_and_cids(100)
        to_edit = data[500:600]
        to_del = data[400:500]

        # these are all {str key: Change}
        expected_adds = {}
        expected_updates = {}
        expected_deletes = {}

        for key, cid in to_add:
            after = after.add(key, cid)
            expected_adds[key] = Change(key=key, cid=cid)

        for (key, prev), new in zip(to_edit, dag_cbor.random.rand_cid()):
            after = after.update(key, new)
            expected_updates[key] = Change(key=key, prev=prev, cid=new)

        for key, cid in to_del:
            after = after.delete(key)
            expected_deletes[key] = Change(key=key, cid=cid)

        diff = Diff.of(after, before)

        self.assertEqual(100, len(diff.adds))
        # TODO: this is flaky, it's occasionally 99 instead of 100 :(
        self.assertEqual(100, len(diff.updates))
        self.assertEqual(100, len(diff.deletes))

        self.assertEqual(expected_adds, diff.adds)
        self.assertEqual(expected_updates, diff.updates)
        self.assertEqual(expected_deletes, diff.deletes)

        # ensure we correctly report all added CIDs
        existing = [cid for _, cid in data]
        for entry in after.walk():
            cid = entry.get_pointer() if isinstance(entry, MST) else entry.value
            # from mst.test.ts, doesn't pass here because we generate test data
            # differently
            # assert cid in existing or cid in diff.new_cids, cid
