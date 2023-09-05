"""Stolen from https://github.com/DavidBuchanan314/picopds . Thank you David!

extremely cursed: monkeypatch pyjwt to always produce low-s secp256k1 ECDSA signatures

https://atproto.com/specs/cryptography#ecdsa-signature-malleability
"""
from cryptography.hazmat.primitives.asymmetric import ec
from jwt import *
from jwt import algorithms

from . import util

orig_der_to_raw_signature = algorithms.der_to_raw_signature

def low_s_patched_der_to_raw_signature(der_sig: bytes, curve: ec.EllipticCurve) -> bytes:
    return orig_der_to_raw_signature(util.apply_low_s_mitigation(der_sig, curve), curve)

algorithms.der_to_raw_signature = low_s_patched_der_to_raw_signature
