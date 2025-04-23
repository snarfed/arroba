"""Unit tests for canned data in testdata/."""
import json
import os
from pathlib import Path

from multiformats import CID

from .. import mst
from ..storage import Action, Block, CommitData, CommitOp, MemoryStorage

from .testutil import TestCase, XrpcTestCase


def load_json_list(filename):
  """Reads a JSON file with an outer list and returns its elements.

  Args:
    file (str): filename inside ``testdata/`` dir

  Returns:
    set
  """
  return json.load((Path(os.path.dirname(__file__)) / 'testdata' / filename).open())


tests = {}

for e in load_json_list('common_prefix.json'):
    def test_fn(e):
        def test(self):
            self.assertEqual(e['len'], mst.common_prefix_len(e['left'], e['right']))
        return test

    tests[f'test_prefix_{e["left"]}_{e["right"]}'] = test_fn(e)


for e in load_json_list('key_heights.json'):
    def test_fn(e):
        def test(self):
            self.assertEqual(e['height'], mst.leading_zeros_on_hash(e['key']))
        return test

    tests[f'test_key_height_{e["key"]}'] = test_fn(e)


TestDataTest = type('TestDataTest', (TestCase,), tests)


tests = {}

# for sync v1.1 inductive firehose aka inverted commits
# https://github.com/bluesky-social/proposals/blob/main/0006-sync-iteration/README.md
for e in load_json_list('commit-proof-fixtures.json'):
    def test_fn(e):
        def test(self):
            # set up MST
            storage = MemoryStorage()
            tree = mst.MST.create(storage=storage)
            val = CID.decode(e['leafValue'])
            for key in e['keys']:
              tree = tree.add(key, val)

            prev_head = self.repo.head
            self.assertEqual(e['rootBeforeCommit'],
                             tree.get_pointer().encode('base32'))

            # apply writes
            ops = []

            for key in e['adds']:
                tree = tree.add(key, val)
                ops.append(CommitOp(action=Action.CREATE, path=key, cid=val))

            for key in e['dels']:
                tree = tree.delete(key)
                ops.append(CommitOp(action=Action.DELETE, path=key, cid=None,
                                    prev_cid=val))

            self.assertEqual(e['rootAfterCommit'], tree.get_pointer().encode('base32'))
            _, blocks = tree.get_unstored_blocks()
            storage.write_blocks(blocks.values())

            # check add_covering_proofs
            commit = CommitData(commit=Block(decoded={'x': 'y'}, ops=ops),
                                blocks={val: Block(decoded={'a': 'b'})})
            proofs = tree.add_covering_proofs(commit)
            self.assertCountEqual([CID.decode(cid) for cid in e['blocksInProof']],
                                  proofs.keys())

        return test

    name = f'test_commit_fixture_{e.get("comment", "").replace(" ", "_").replace("-", "_")}'
    tests[name] = test_fn(e)


XrpcTestDataTest = type('XrpcTestDataTest', (XrpcTestCase,), tests)
