from typing import List, Tuple, BinaryIO

import os
import io
import json

import dag_cbor
import dag_cbor.random
from google.cloud import ndb
from multiformats import CID, varint

from ..diff import Change, Diff, null_diff
from ..mst import MST
from ..storage import MemoryStorage, Block, Storage
from ..datastore_storage import AtpBlock, AtpRepo, DatastoreStorage
from . import testutil


class MSTSuiteTest:
    """Abstract base class. Concrete subclasses are below, one for each storage."""
    STORAGE_CLASS = None

    def setUp(self):
        super().setUp()
        # recursively search for test cases in JSON format.
        # for now we only know how to process "mst-diff" test cases - more types will be added
        # in the future
        self.test_suite_base = "./mst-test-suite/"
        diff_testcases = {}
        for path in [os.path.join(dp, f) for dp, _, fn in os.walk(self.test_suite_base + "/tests/") for f in fn]:
            if not path.endswith(".json"):
                continue
            with open(path) as json_file:
                testcase = json.load(json_file)
            if testcase.get("$type") == "mst-diff":
                diff_testcases[path] = testcase
        self.diff_testcases = dict(sorted(diff_testcases.items())) # sort them because os.walk() uses a weird order

    def parse_car(self, stream: BinaryIO) -> Tuple[CID, List[Tuple[CID, bytes]]]:
        car_header = dag_cbor.decode(stream.read(varint.decode(stream)))
        blocks = []
        while True:
            try:
                block = stream.read(varint.decode(stream))
            except ValueError:
                break
            blocks.append((CID.decode(block[:36]), block[36:]))
        return car_header["roots"][0], blocks

    def populate_storage_from_car(self, storage: Storage, car_path: str) -> CID:
        with open(self.test_suite_base + car_path, "rb") as carfile:
            root, blocks = self.parse_car(carfile)
            self.store_blocks(storage, blocks)
            return root

    def store_blocks(self, storage: Storage, blocks: List[Tuple[CID, bytes]]):
        """Abstract, implemented by subclasses."""
        raise NotImplementedError()

    def serialise_canonical_car(self, root: CID, blocks: List[Tuple[CID, bytes]]) -> bytes:
        car = io.BytesIO()
        header = dag_cbor.encode({"version": 1, "roots": [root]})
        car.write(varint.encode(len(header)) + header)
        for cid, value in sorted(blocks, key=lambda x: bytes(x[0])):
            entry = bytes(cid) + value
            car.write(varint.encode(len(entry)) + entry)
        return car.getvalue()

    def test_diffs(self):
        for testname, testcase in self.diff_testcases.items():
            storage = self.STORAGE_CLASS()
            root_a = self.populate_storage_from_car(storage, testcase["inputs"]["mst_a"])
            root_b = self.populate_storage_from_car(storage, testcase["inputs"]["mst_b"])
            mst_a = MST.load(storage=storage, cid=root_a)
            mst_b = MST.load(storage=storage, cid=root_b)

            diff: Diff = Diff.of(mst_b, mst_a)

            ops_list = []
            for created in diff.adds.values():
                ops_list.append({
                    "rpath": created.key,
                    "old_value": None,
                    "new_value": created.cid.encode("base32")
                })
            for updated in diff.updates.values():
                ops_list.append({
                    "rpath": updated.key,
                    "old_value": updated.prev.encode("base32"),
                    "new_value": updated.cid.encode("base32")
                })
            for removed in diff.deletes.values():
                ops_list.append({
                    "rpath": removed.key,
                    "old_value": removed.cid.encode("base32"),
                    "new_value": None
                })

            # sort the lists for comparison, per mst-test-suite's rules.
            # NOTE: maybe we should just compare set()s instead?
            created_list = sorted(cid.encode("base32") for cid in diff.new_cids)
            deleted_list = sorted(cid.encode("base32") for cid in diff.removed_cids)
            ops_list.sort(key=lambda x: x["rpath"])

            with self.subTest(testcase["description"] + ": record_ops"):
                self.assertEqual(ops_list, testcase["results"]["record_ops"])
            with self.subTest(testcase["description"] + ": created_nodes"):
                self.assertEqual(created_list, testcase["results"]["created_nodes"]) # currently fails!
            with self.subTest(testcase["description"] + ": deleted_nodes"):
                self.assertEqual(deleted_list, testcase["results"]["deleted_nodes"])
                # TODO: implement checks for proof_nodes, firehose_cids (test data hasn't been generated yet)

    def test_diffs_inverse(self):
        # we re-use the diff test cases but "backwards" - applying the op list
        # to the initial MST see if we end up at the correct final MST
        for testname, testcase in self.diff_testcases.items():
            storage = self.STORAGE_CLASS()
            root_a = self.populate_storage_from_car(storage, testcase["inputs"]["mst_a"])
            mst = MST.load(storage=storage, cid=root_a)

            for op in testcase["results"]["record_ops"]:
                if op["old_value"] and op["new_value"]: # update
                    mst = mst.update(op["rpath"], CID.decode(op["new_value"]))
                elif op["old_value"]: # delete
                    mst = mst.delete(op["rpath"])
                else: # create
                    mst = mst.add(op["rpath"], CID.decode(op["new_value"]))

            diff = null_diff(mst) # should get us a map of the complete new mst
            root_b = mst.get_pointer()

            with open(self.test_suite_base  + testcase["inputs"]["mst_b"], "rb") as car_b:
                reference_root, reference_blocks = self.parse_car(car_b)

            reference_cid_set = set(x[0] for x in reference_blocks) # just look at the cids from the car

            with self.subTest(testcase["description"] + " (inverse): new root"):
                self.assertEqual(root_b, reference_root) # fails occasionally
            with self.subTest(testcase["description"] + " (inverse): new cid set"):
                self.assertEqual(diff.new_cids, reference_cid_set) # basically always fails, I think I'm doing something wrong


class MemoryMSTSuiteTest(MSTSuiteTest, testutil.TestCase):
    STORAGE_CLASS = MemoryStorage

    def store_blocks(self, storage: Storage, blocks: List[Tuple[CID, bytes]]):
        for cid, value in blocks:
            storage.blocks[cid] = Block(cid=cid, encoded=value)


class DatastoreMSTSuiteTest(MSTSuiteTest, testutil.DatastoreTest):
    STORAGE_CLASS = DatastoreStorage

    def store_blocks(self, storage: Storage, blocks: List[Tuple[CID, bytes]]):
        ndb.put_multi(AtpBlock(id=cid.encode('base32'), encoded=value, seq=0,
                               repo=AtpRepo(id='unused').key)
                      for cid, value in blocks)
