"""Property-based tests for mst.py, using hypothesis."""
from string import ascii_letters, digits

from hypothesis import given, strategies as st

from ...mst import (
    cid_for_entries,
    deserialize_node_data,
    ensure_valid_key,
    Leaf,
    MST,
    serialize_node_data,
)
from ...util import dag_cbor_cid
from ..testutil import TestCase

# _VALID_KEY_RE, ie the characters ensure_valid_key allows in each of the two
# '/'-separated parts of a key
KEY_PART = st.text(alphabet=ascii_letters + digits + '_-:.', min_size=1, max_size=32)
KEYS = st.builds(lambda collection, rkey: f'{collection}/{rkey}', KEY_PART, KEY_PART)

CIDS = st.integers(min_value=0, max_value=2 ** 32).map(
    lambda num: dag_cbor_cid({'val': num}))

# a node's entries: an optional left subtree, then leaves in ascending key
# order, each optionally followed by a subtree
ENTRIES = st.builds(
    lambda leaves, left, subtrees: (
        ([MST(pointer=left)] if left else [])
        + [entry for leaf, subtree in zip(sorted(leaves), subtrees)
           for entry in ([Leaf(*leaf)] + ([MST(pointer=subtree)] if subtree else []))]),
    st.lists(st.tuples(KEYS, CIDS), min_size=1, max_size=8,
             unique_by=lambda leaf: leaf[0]),
    CIDS | st.none(),
    st.lists(CIDS | st.none(), min_size=8, max_size=8))


class MstHypothesisTest(TestCase):

    @given(ENTRIES)
    def test_serialize_then_deserialize_node_data(self, entries):
        data = serialize_node_data(entries)
        self.assertEqual(entries, deserialize_node_data(data=data))

    @given(ENTRIES)
    def test_cid_for_entries_is_deterministic(self, entries):
        self.assertEqual(cid_for_entries(entries), cid_for_entries(entries))

    @given(KEYS)
    def test_ensure_valid_key_accepts_generated_keys(self, key):
        ensure_valid_key(key)
