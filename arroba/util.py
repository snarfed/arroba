"""Misc AT Protocol utils. TIDs, CIDs, etc."""
import copy
from datetime import datetime, timezone
import logging
from numbers import Integral
import random
import time

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
    encode_dss_signature,
)
from cryptography.hazmat.primitives import hashes
import dag_cbor
from multiformats import CID, multicodec, multihash

logger = logging.getLogger(__name__)

# the bottom 32 clock ids can be randomized & are not guaranteed to be collision
# resistant. we use the same clockid for all TIDs coming from this runtime.
_clockid = random.randint(0, 31)
_tid_last = 0  # microseconds

S32_CHARS = '234567abcdefghijklmnopqrstuvwxyz'

# for low-S signing
# https://atproto.com/specs/cryptography
CURVE_ORDER = {
    ec.SECP256R1: 0xFFFFFFFF_00000000_FFFFFFFF_FFFFFFFF_BCE6FAAD_A7179E84_F3B9CAC2_FC632551,
    ec.SECP256K1: 0xFFFFFFFF_FFFFFFFF_FFFFFFFF_FFFFFFFE_BAAEDCE6_AF48A03B_BFD25E8C_D0364141
}


def now(tz=timezone.utc, **kwargs):
    """Wrapper for datetime.now that allows us to mock it out in tests."""
    return datetime.now(tz=tz, **kwargs)


def time_ns():
    """Wrapper for time.time_ns that allows us to mock it out in tests."""
    return time.time_ns()


def dag_cbor_cid(obj):
    """Returns the DAG-CBOR CID for a given object.

    Args:
      obj: CBOR-compatible native object or value

    Returns:
      :class:`CID`
    """
    encoded = dag_cbor.encode(obj)
    digest = multihash.digest(encoded, 'sha2-256')
    return CID('base58btc', 1, multicodec.get('dag-cbor'), digest)


def s32encode(num):
    """Base32 encode with encoding variant sort.

    Based on https://github.com/bluesky-social/atproto/blob/main/packages/common-web/src/tid.ts

    Args:
      num: int or Integral

    Returns:
      str
    """
    assert isinstance(num, Integral)

    encoded = []
    while num > 0:
        c = num % 32
        num = num // 32
        encoded.insert(0, S32_CHARS[c])

    return ''.join(encoded)


def s32decode(val):
    """Base32 decode with encoding variant sort.

    Based on https://github.com/bluesky-social/atproto/blob/main/packages/common-web/src/tid.ts

    Args:
      val: str

    Returns:
      int or Integral
    """
    i = 0
    for c in val:
        i = i * 32 + S32_CHARS.index(c)

    return i


def datetime_to_tid(dt):
    """Converts a datetime to an ATProto TID.

    https://atproto.com/guides/data-repos#identifier-types

    Args:
      dt: :class:`datetime.datetime`

    Returns:
      str, base32-encoded TID
    """
    tid = (s32encode(int(dt.timestamp() * 1000 * 1000)) +
           s32encode(_clockid).ljust(2, '2'))
    assert len(tid) == 13, tid
    return tid


def tid_to_datetime(tid):
    """Converts an ATProto TID to a datetime.

    https://atproto.com/guides/data-repos#identifier-types

    Args:
      tid: bytes, base32-encoded TID

    Returns:
      :class:`datetime.datetime`

    Raises:
      ValueError if tid is not bytes or not 13 characters long
    """
    if not isinstance(tid, (str, bytes)) or len(tid) != 13:
        raise ValueError(f'Expected 13-character str or bytes; got {tid}')

    encoded = tid.replace('-', '')[:-2]  # strip clock id
    return datetime.fromtimestamp(s32decode(encoded) / 1000 / 1000, timezone.utc)


def next_tid():
    """Returns the TID corresponding to the current time.

    A TID is UNIX timestamp (ie time since the epoch) in microseconds.
    Returned tids are guaranteed to monotonically increase across calls.

    https://atproto.com/specs/atp#timestamp-ids-tid
    https://github.com/bluesky-social/atproto/blob/main/packages/common-web/src/tid.ts

    Returns:
      str, TID
    """
    global _tid_last

    # enforce that we're at least 1us after the last TID to prevent TIDs moving
    # backwards if system clock drifts backwards
    _tid_last = max(time_ns() // 1000, _tid_last + 1)
    return str(_tid_last)


def at_uri(did, collection, rkey):
    """Returns the at:// URI for a given DID, collection, and rkey.

    Args:
      did: str
      collection: str
      rkey: str

    Returns:
      str, at:// URI
    """
    assert did
    assert collection
    assert rkey
    return f'at://{did}/{collection}/{rkey}'


def new_key():
    """Generates a new ECC P-256 keypair.

    Returns:
      :class:`ec.EllipticCurvePrivateKey`
    """
    return ec.generate_private_key(ec.SECP256K1())


def sign_commit(commit, private_key):
    """Signs a repo commit.

    Adds the signature in the `sig` field.

    https://atproto.com/specs/cryptography

    The signature is ECDSA around SHA-256 of the input, including a custom
    second pass to enforce that it's the "low-S" variant:
    https://atproto.com/specs/cryptography#ecdsa-signature-malleability

    Args:
      commit: dict, repo commit
      private_key: :class:`ec.EllipticCurvePrivateKey`

    Returns:
      dict, repo commit
    """
    orig_sig = private_key.sign(dag_cbor.encode(commit), ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(apply_low_s_mitigation(orig_sig, private_key.curve))
    commit['sig'] = r.to_bytes(32, 'big') + s.to_bytes(32, 'big')
    return commit

    # old, using pycryptodome
    # signer = DSS.new(private_key, 'fips-186-3', randfunc=_randfunc)
    # commit['sig'] = signer.sign(SHA256.new(dag_cbor.encode(commit)))
    # return commit


def apply_low_s_mitigation(signature: bytes, curve: ec.EllipticCurve) -> bytes:
    """Low-S signature mitigation.

    https://atproto.com/specs/cryptography#ecdsa-signature-malleability

    From picopds. Thank you David!
    https://github.com/DavidBuchanan314/picopds/blob/main/signing.py
    """
    r, s = decode_dss_signature(signature)
    n = CURVE_ORDER[type(curve)]
    if s > n // 2:
        s = n - s
    return encode_dss_signature(r, s)


def verify_commit_sig(commit, public_key):
    """Returns true if the commit's signature is valid, False otherwise.

    See :func:`sign_commit` for more background.

    Args:
      commit: dict repo commit
      public_key: :class:`ec.EllipticCurvePublicKey`

    Raises:
      KeyError if the commit isn't signed, ie doesn't have a `sig` field
    """
    commit = copy.copy(commit)
    sig = commit.pop('sig')

    if len(sig) != 64:
        logger.debug('Expected signature to be 64 bytes, got {len(sig)}')
        return False

    r = int.from_bytes(sig[:32], 'big')
    s = int.from_bytes(sig[32:], 'big')
    der_sig = encode_dss_signature(r, s)

    try:
        public_key.verify(der_sig, dag_cbor.encode(commit),
                          ec.ECDSA(hashes.SHA256()))
        return True
    except InvalidSignature:
        logger.debug("Couldn't verify signature", exc_info=True)
        return False

