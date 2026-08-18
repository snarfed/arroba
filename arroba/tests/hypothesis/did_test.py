"""Property-based tests for did.py, using hypothesis."""
from cryptography.hazmat.primitives.asymmetric import ec
from hypothesis import given, strategies as st

from ...did import decode_did_key, encode_did_key
from ..testutil import TestCase

CURVES = st.sampled_from([ec.SECP256K1, ec.SECP256R1])
SEEDS = st.integers(min_value=1, max_value=2 ** 200)


class DidHypothesisTest(TestCase):

    @given(SEEDS, CURVES)
    def test_did_key_round_trips(self, seed, curve):
        """https://atproto.com/specs/did#public-key-encoding"""
        pubkey = ec.derive_private_key(seed, curve()).public_key()
        decoded = decode_did_key(encode_did_key(pubkey))

        self.assertEqual(curve, type(decoded.curve))
        self.assertEqual(pubkey.public_numbers(), decoded.public_numbers())

    @given(SEEDS, CURVES)
    def test_encode_did_key_is_did_key_uri(self, seed, curve):
        pubkey = ec.derive_private_key(seed, curve()).public_key()
        did_key = encode_did_key(pubkey)

        self.assertTrue(did_key.startswith('did:key:z'), did_key)
        self.assertEqual(did_key, encode_did_key(pubkey))
