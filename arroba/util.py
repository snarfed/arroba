"""Misc AT Protocol utils. TIDs, CIDs, etc."""
import copy
from datetime import datetime, timedelta, timezone
import json
import logging
from numbers import Integral
import random
import re
import time
from urllib.parse import urlparse

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import (
    decode_dss_signature,
    encode_dss_signature,
)
from cryptography.hazmat.primitives import hashes
import dag_cbor
import jwt
from multiformats import CID, multicodec, multihash

logger = logging.getLogger(__name__)

USER_AGENT = 'arroba (https://arroba.readthedocs.io/)'

DOMAIN_RE = re.compile(r'^[^/:;@?!\']+\.[^/:@_?!\']+$')

# the bottom 32 clock ids can be randomized & are not guaranteed to be collision
# resistant. we use the same clockid for all TIDs coming from this runtime.
_clockid = random.randint(0, 31)
_tid_ts_last = 0  # microseconds

S32_CHARS = '234567abcdefghijklmnopqrstuvwxyz'

# for low-S signing
# https://atproto.com/specs/cryptography
CURVE_ORDER = {
    ec.SECP256R1: 0xFFFFFFFF_00000000_FFFFFFFF_FFFFFFFF_BCE6FAAD_A7179E84_F3B9CAC2_FC632551,
    ec.SECP256K1: 0xFFFFFFFF_FFFFFFFF_FFFFFFFF_FFFFFFFE_BAAEDCE6_AF48A03B_BFD25E8C_D0364141
}

DEACTIVATED = 'deactivated'
DELETED = 'deleted'
TOMBSTONED = 'tombstoned'


class InactiveRepo(ValueError):
    """Raised when loading a repo that's not active.

    Attributes:
      status (str)
    """
    def __init__(self, did, status, *args, **kwargs):
        assert status
        self.status = status
        super().__init__(f'Repo {did} is {status}', *args, **kwargs)


def now(tz=timezone.utc, **kwargs):
    """Wrapper for :meth:`datetime.datetime.now` that lets us mock it out in tests."""
    return datetime.now(tz=tz, **kwargs)


def time_ns():
    """Wrapper for :func:`time.time_ns` that lets us mock it out in tests."""
    return time.time_ns()


def dag_cbor_cid(obj):
    """Returns the DAG-CBOR CID for a given object.

    Args:
      obj: CBOR-compatible native object or value

    Returns:
      CID:
    """
    encoded = dag_cbor.encode(obj)
    digest = multihash.digest(encoded, 'sha2-256')
    return CID('base58btc', 1, 'dag-cbor', digest)


def s32encode(num):
    """Base32 encode with encoding variant sort.

    Based on https://github.com/bluesky-social/atproto/blob/main/packages/common-web/src/tid.ts

    Args:
      num (int or Integral)

    Returns:
      str:
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
      val (str)

    Returns:
      int or Integral:
    """
    i = 0
    for c in val:
        i = i * 32 + S32_CHARS.index(c)

    return i


def datetime_to_tid(dt, clock_id=None):
    """Converts a datetime to a TID.

    https://atproto.com/specs/record-key#record-key-type-tid

    Args:
      dt (datetime.datetime):
      clock_id: 0, optional. If not specified, uses this runtime's clock id

    Returns:
      str: base32-encoded TID
    """
    return int_to_tid(int(dt.timestamp() * 1000 * 1000), clock_id=clock_id)


def int_to_tid(num, clock_id=None):
    """Converts an integer to a TID.

    https://atproto.com/specs/record-key#record-key-type-tid

    Args:
      seq (int)
      clock_id (int): optional. If not specified, uses this runtime's clock id

    Returns:
      str: base32-encoded TID
    """
    if clock_id is None:
        clock_id = _clockid

    tid = (s32encode(num) +
           s32encode(clock_id).ljust(2, '2'))  # clock id
    assert len(tid) <= 13, tid
    return tid.rjust(13, '2')  # pad up to 13 chars; '2' decodes to 0


def tid_to_datetime(tid):
    """Converts an TID to a datetime.

    https://atproto.com/specs/record-key#record-key-type-tid

    Args:
      tid (bytes): base32-encoded TID

    Returns:
      datetime.datetime:

    Raises:
      ValueError: if tid is not bytes or not 13 characters long
    """
    return datetime.fromtimestamp(tid_to_int(tid) / 1000 / 1000, timezone.utc)


def tid_to_int(tid):
    """Converts an TID to an integer.

    https://atproto.com/specs/record-key#record-key-type-tid

    Args:
      tid (bytes): base32-encoded TID

    Returns:
      int:

    Raises:
      ValueError: if tid is not bytes or not 13 characters long
    """
    if not isinstance(tid, (str, bytes)) or len(tid) != 13:
        raise ValueError(f'Expected 13-character str or bytes; got {tid}')

    encoded = tid.replace('-', '')[:-2]  # strip clock id
    return s32decode(encoded)


def next_tid():
    """Returns the TID corresponding to the current time.

    A TID is a base32-sortable-encoded UNIX timestamp (ie time since the epoch)
    in microseconds. Returned tids are guaranteed to monotonically increase
    across calls.

    https://atproto.com/specs/atp#timestamp-ids-tid
    https://atproto.com/specs/record-key#record-key-type-tid
    https://github.com/bluesky-social/atproto/blob/main/packages/common-web/src/tid.ts

    Returns:
      str: TID
    """
    global _tid_ts_last

    # enforce that we're at least 1us after the last TID to prevent TIDs moving
    # backwards if system clock drifts backwards
    _tid_ts_last = max(time_ns() // 1000, _tid_ts_last + 1)
    return int_to_tid(_tid_ts_last)


def at_uri(did, collection, rkey):
    """Returns the ``at://`` URI for a given DID, collection, and rkey.

    https://atproto.com/specs/at-uri-scheme

    Args:
      did (str)
      collection (str)
      rkey (str)

    Returns:
      str: ``at://`` URI
    """
    assert did
    assert collection
    assert rkey
    return f'at://{did}/{collection}/{rkey}'


def parse_at_uri(uri):
    """Parses the repo DID, collection, and rkey out of an at:// URI.

    https://atproto.com/specs/at-uri-scheme

    Args:
      uri (str)

    Returns:
      tuple of str: ``(did, collection, rkey)``
    """
    if not uri or not uri.startswith('at://'):
        raise ValueError(f"{uri} isn't an at:// URI")

    parsed = urlparse(uri)
    rkey = parsed.path.strip('/').split('/', maxsplit=1)
    if len(rkey) < 2:
        rkey.append('')
    return parsed.netloc, *rkey


def new_key(seed=None):
    """Generates a new ECC K-256 keypair.

    https://atproto.com/specs/cryptography

    Args:
      seed (int): optional deterministic value to derive private key from.
        Don't use in production!

    Returns:
      ec.EllipticCurvePrivateKey:
    """
    if seed:
        return ec.derive_private_key(seed, ec.SECP256K1())
    else:
        return ec.generate_private_key(ec.SECP256K1())


def sign(obj, private_key):
    """Signs an object, eg a repo commit or DID document.

    Adds the signature in the ``sig`` field.

    https://atproto.com/specs/cryptography

    The signature is ECDSA around SHA-256 of the input, including a custom
    second pass to enforce that it's the "low-S" variant:
    https://atproto.com/specs/cryptography#ecdsa-signature-malleability

    Args:
      obj (dict)
      private_key (cryptography.hazmat.primitives.asymmetric.ec.EllipticCurvePrivateKey)

    Returns:
      dict: ``obj`` with new ``sig`` field
    """
    orig_sig = private_key.sign(dag_cbor.encode(obj), ec.ECDSA(hashes.SHA256()))
    r, s = decode_dss_signature(apply_low_s_mitigation(orig_sig, private_key.curve))
    obj['sig'] = r.to_bytes(32, 'big') + s.to_bytes(32, 'big')
    return obj

    # old, using pycryptodome
    # signer = DSS.new(private_key, 'fips-186-3', randfunc=_randfunc)
    # commit['sig'] = signer.sign(SHA256.new(dag_cbor.encode(commit)))
    # return commit


def apply_low_s_mitigation(signature, curve):
    """Low-S signature mitigation.

    This prevents signature malleability. (It *doesn't* guarantee deterministic
    signatures though!)

    https://atproto.com/specs/cryptography#ecdsa-signature-malleability

    From picopds. Thank you David!
    https://github.com/DavidBuchanan314/picopds/blob/main/signing.py

    Args:
      signature (bytes)
      curve (ec.EllipticCurve)

    Returns:
      bytes:
    """
    r, s = decode_dss_signature(signature)
    n = CURVE_ORDER[type(curve)]
    if s > n // 2:
        s = n - s
    return encode_dss_signature(r, s)


def verify_sig(obj, public_key):
    """Returns True if obj's signature is valid, False otherwise.

    See :func:`sign` for more background.

    Args:
      obj (dict): repo commit
      public_key (ec.EllipticCurvePublicKey):

    Raises:
      KeyError: if ``obj`` isn't signed, ie doesn't have a ``sig`` field
    """
    obj = copy.copy(obj)
    sig = obj.pop('sig')

    if len(sig) != 64:
        logger.debug(f'Expected signature to be 64 bytes, got {len(sig)}')
        return False

    r = int.from_bytes(sig[:32], 'big')
    s = int.from_bytes(sig[32:], 'big')
    der_sig = encode_dss_signature(r, s)

    try:
        public_key.verify(der_sig, dag_cbor.encode(obj), ec.ECDSA(hashes.SHA256()))
        return True
    except InvalidSignature:
        logger.debug("Couldn't verify signature", exc_info=True)
        return False


def service_jwt(host, repo_did, privkey, expiration=timedelta(minutes=10),
                aud=None, **claims):
    """Generates an inter-service JWT, eg for a relay or AppView.

    https://atproto.com/specs/xrpc#inter-service-authentication-temporary-specification

    Args:
      host (str): hostname of the service this JWT is for, eg ``bsky.network``
      repo_did (str): DID of the repo this JWT is for
      privkey (ec.EllipticCurvePrivateKey): repo's signing key
      expiration (timedelta): length of time this JWT will be valid, defaults to 10m
      aud (str): JWT audience. Default is ``did:web:[host]``, which works for relays
        and AppViews, but others (eg mod services) have ``did:plc`` instead.
      claims (dict): additional claims to include in the JWT, eg ``lxm``

    Returns:
      str: JWT
    """
    assert host
    assert repo_did
    assert expiration
    data = {
        'iss': repo_did,
        'aud': aud or f'did:web:{host}',
        'alg': 'ES256K',  # k256
        'exp': int((now() + expiration).timestamp()),
        **claims,
    }
    logger.info(f'Generating ATProto inter-service JWT: {data}')
    return jwt.encode(data, privkey, algorithm='ES256K')
