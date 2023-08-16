"""Stolen from https://github.com/DavidBuchanan314/picopds . Thank you David!

extremely cursed: monkeypatch pyjwt to always produce low-s secp256k1 ECDSA signatures

https://atproto.com/specs/cryptography#ecdsa-signature-malleability
"""
from jwt import *
from jwt import algorithms
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature, encode_dss_signature

CURVE_ORDER = {
	ec.SECP256R1: 0xFFFFFFFF_00000000_FFFFFFFF_FFFFFFFF_BCE6FAAD_A7179E84_F3B9CAC2_FC632551,
	ec.SECP256K1: 0xFFFFFFFF_FFFFFFFF_FFFFFFFF_FFFFFFFE_BAAEDCE6_AF48A03B_BFD25E8C_D0364141
}


def apply_low_s_mitigation(signature: bytes, curve: ec.EllipticCurve) -> bytes:
	r, s = decode_dss_signature(signature)
	n = CURVE_ORDER[type(curve)]
	if s > n // 2:
		s = n - s
	return encode_dss_signature(r, s)


def raw_sign(privkey: ec.EllipticCurvePrivateKey, data: bytes) -> bytes:
	r, s = decode_dss_signature(
		apply_low_s_mitigation(
			privkey.sign(data, ec.ECDSA(hashes.SHA256())),
			privkey.curve
		)
	)
	signature = r.to_bytes(32, "big") + s.to_bytes(32, "big")
	return signature


orig_der_to_raw_signature = algorithms.der_to_raw_signature

def low_s_patched_der_to_raw_signature(der_sig: bytes, curve: ec.EllipticCurve) -> bytes:
	return orig_der_to_raw_signature(apply_low_s_mitigation(der_sig, curve), curve)

algorithms.der_to_raw_signature = low_s_patched_der_to_raw_signature
