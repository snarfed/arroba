"""Unit tests for canned data in testdata/."""
import json
import os
from pathlib import Path
from unittest import TestCase

from arroba import mst


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
