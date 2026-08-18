"""Property-based tests for util.py, using hypothesis."""
from datetime import datetime, timezone
from string import ascii_letters, ascii_lowercase, digits

from hypothesis import given, strategies as st

from ...util import (
    at_uri,
    dag_cbor_cid,
    datetime_to_tid,
    int_to_tid,
    new_key,
    parse_at_uri,
    s32decode,
    s32encode,
    sign,
    tid_to_datetime,
    tid_to_int,
    verify_sig,
)
from ..testutil import TestCase

# int_to_tid asserts that the TID is at most 13 chars, ie 11 chars of timestamp
# plus 2 of clock id
TID_INTS = st.integers(min_value=0, max_value=32 ** 11 - 1)
CLOCK_IDS = st.integers(min_value=0, max_value=31)

# https://atproto.com/specs/did
DIDS = (st.text(alphabet=ascii_lowercase + digits, min_size=1, max_size=24)
        .map(lambda suffix: f'did:plc:{suffix}'))

# https://atproto.com/specs/nsid
NSIDS = st.lists(st.text(alphabet=ascii_letters, min_size=1, max_size=8),
                 min_size=3, max_size=5).map('.'.join)

# https://atproto.com/specs/record-key
RKEYS = st.text(alphabet=ascii_letters + digits + '.-_:~', min_size=1,
                max_size=64).filter(lambda rkey: rkey not in ('.', '..'))

# DAG-CBOR encodable values. ints are limited to 64 bits, floats must be finite,
# and map keys must be strings.
CIDS = st.integers(min_value=0, max_value=2 ** 32).map(
    lambda num: dag_cbor_cid({'val': num}))

# https://atproto.com/specs/data-model
SCALARS = (st.none() | st.booleans()
           | st.integers(min_value=-2 ** 63, max_value=2 ** 63 - 1)
           | st.text() | st.binary() | CIDS)
NESTED = st.recursive(
    SCALARS,
    lambda children: st.lists(children) | st.dictionaries(st.text(), children),
    max_leaves=10)
OBJECTS = st.dictionaries(st.text(), NESTED)

# generating EC keys is expensive, so reuse a handful
KEYS = st.sampled_from([new_key(seed=seed) for seed in
                        (2349872879569, 8675309, 1618033988749)])


class UtilHypothesisTest(TestCase):

    @given(st.integers(min_value=0))
    def test_s32_round_trips(self, num):
        self.assertEqual(num, s32decode(s32encode(num)))

    @given(TID_INTS, CLOCK_IDS)
    def test_int_to_tid_round_trips(self, num, clock_id):
        tid = int_to_tid(num, clock_id=clock_id)
        self.assertEqual(13, len(tid))
        self.assertEqual(num, tid_to_int(tid))

    @given(TID_INTS, TID_INTS, CLOCK_IDS)
    def test_tids_sort_like_their_ints(self, a, b, clock_id):
        """TIDs are specified to be lexically sortable.

        https://atproto.com/specs/record-key#record-key-type-tid
        """
        tid_a = int_to_tid(a, clock_id=clock_id)
        tid_b = int_to_tid(b, clock_id=clock_id)
        self.assertEqual(a < b, tid_a < tid_b)
        self.assertEqual(a == b, tid_a == tid_b)

    @given(st.datetimes(min_value=datetime(1970, 1, 1),
                        max_value=datetime(2100, 1, 1),
                        timezones=st.just(timezone.utc)),
           CLOCK_IDS)
    def test_datetime_to_tid_round_trips(self, dt, clock_id):
        self.assertEqual(dt, tid_to_datetime(datetime_to_tid(dt, clock_id=clock_id)))

    @given(DIDS, NSIDS, RKEYS)
    def test_at_uri_round_trips(self, did, collection, rkey):
        self.assertEqual((did, collection, rkey),
                         parse_at_uri(at_uri(did, collection, rkey)))

    @given(OBJECTS, KEYS)
    def test_sign_then_verify_sig(self, obj, key):
        signed = sign(dict(obj), key)
        self.assertTrue(verify_sig(signed, key.public_key()))

    @given(OBJECTS, KEYS, st.text(min_size=1))
    def test_verify_sig_fails_on_tampered_obj(self, obj, key, field):
        signed = sign(dict(obj), key)
        signed[field] = 'tampered'
        self.assertFalse(verify_sig(signed, key.public_key()))
