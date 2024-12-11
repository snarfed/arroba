import os
import json

import dag_cbor
import dag_cbor.random
from multiformats import CID, varint

from tqdm import tqdm

from ..diff import Change, Diff
from ..mst import MST
from ..storage import MemoryStorage, Block
from . import testutil

class MSTSuiteTest(testutil.TestCase):

    def setUp(self):
        super().setUp()
        self.diff_testcases = {}
        # recursively search for test cases in JSON format.
        # for now we only know how to process "mst-diff" test cases - more types will be added
        # in the future
        self.test_suite_base = "./mst-test-suite/"
        for path in [os.path.join(dp, f) for dp, _, fn in os.walk(self.test_suite_base + "/tests/") for f in fn]:
            if not path.endswith(".json"):
                continue
            with open(path) as json_file:
                testcase = json.load(json_file)
            if testcase.get("$type") == "mst-diff":
                self.diff_testcases[path] = testcase

    def populate_storage_from_car(self, storage: MemoryStorage, car_path: str) -> CID:
        # ad-hoc CAR parser, returns the root CID
        with open(self.test_suite_base + car_path, "rb") as carfile:
            car_header = dag_cbor.decode(carfile.read(varint.decode(carfile)))
            while True:
                try:
                    block = carfile.read(varint.decode(carfile))
                except ValueError:
                    break
                cid = CID.decode(block[:36])
                storage.blocks[cid] = Block(cid=cid, encoded=block[36:])
            return car_header["roots"][0]

    def test_diffs(self):
        for testname, testcase in tqdm(self.diff_testcases.items()):
            storage = MemoryStorage()
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

            # sort the lists for comparison, per mst-test-suite's rules
            created_list = sorted(cid.encode("base32") for cid in diff.new_cids)
            deleted_list = sorted(cid.encode("base32") for cid in diff.removed_cids)
            ops_list.sort(key=lambda x: x["rpath"])

            self.assertEqual(ops_list, testcase["results"]["record_ops"], f"{testname} record_ops")
            self.assertEqual(created_list, testcase["results"]["created_nodes"], f"{testname} created_nodes") # currently fails!
            self.assertEqual(deleted_list, testcase["results"]["deleted_nodes"], f"{testname} deleted_nodes")
            # TODO: implement checks for proof_nodes, firehose_cids (test data hasn't been generated yet)
