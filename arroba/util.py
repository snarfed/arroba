"""Misc AT Protocol utils. TIDs, CIDs, etc."""
import copy
from datetime import datetime, timezone
import logging
from numbers import Integral
import random
import time

from Crypto.Hash import SHA256
from Crypto.PublicKey import ECC
from Crypto.Signature import DSS
import dag_cbor
from multiformats import CID, multicodec, multihash

logger = logging.getLogger(__name__)

# used as pycryptodome's randfunc in various places. None defaults to
# pycryptodome's internal RNG. This constant is overridden in tests to use
# random.randbytes with a fixed seed.
_randfunc = None

# the bottom 32 clock ids can be randomized & are not guaranteed to be collision
# resistant. we use the same clockid for all TIDs coming from this runtime.
_clockid = random.randint(0, 31)
_tid_last = 0  # microseconds

S32_CHARS = '234567abcdefghijklmnopqrstuvwxyz'


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


def new_p256_key():
    """Generates a new ECC P-256 keypair.

    Returns:
      :class:`Crypto.PublicKey.ECC.EccKey`
    """
    return ECC.generate(curve='P-256', randfunc=_randfunc)


def sign_commit(commit, key):
    """Signs a repo commit.

    Adds the signature in the `sig` field.

    Signing isn't yet in the atproto.com docs, this setup is taken from the TS
    code and conversations with @why on #bluesky-dev:matrix.org.

    * https://matrix.to/#/!vpdMrhHjzaPbBUSgOs:matrix.org/$Xaf4ugYks-iYg7Pguh3dN8hlsvVMUOuCQo3fMiYPXTY?via=matrix.org&via=minds.com&via=envs.net
    * https://github.com/bluesky-social/atproto/blob/384e739a3b7d34f7a95d6ba6f08e7223a7398995/packages/repo/src/util.ts#L238-L248
    * https://github.com/bluesky-social/atproto/blob/384e739a3b7d34f7a95d6ba6f08e7223a7398995/packages/crypto/src/p256/keypair.ts#L66-L73
    * https://github.com/bluesky-social/indigo/blob/f1f2480888ab5d0ac1e03bd9b7de090a3d26cd13/repo/repo.go#L64-L70
    * https://github.com/whyrusleeping/go-did/blob/2146016fc220aa1e08ccf26aaa762f5a11a81404/key.go#L67-L91

    The signature is ECDSA around SHA-256 of the input. We currently use P-256
    keypairs. Context:
    * Go supports P-256, ED25519, SECP256K1 keys
    * TS supports P-256, SECP256K1 keys
    * this recommends ED25519, then P-256:
      https://soatok.blog/2022/05/19/guidance-for-choosing-an-elliptic-curve-signature-algorithm-in-2022/

    Args:
      commit: dict, repo commit
      key: :class:`Crypto.PublicKey.ECC.EccKey`

    Returns:
      dict, repo commit
    """
    signer = DSS.new(key, 'fips-186-3', randfunc=_randfunc)
    commit['sig'] = signer.sign(SHA256.new(dag_cbor.encoding.encode(commit)))
    return commit


def verify_commit_sig(commit, key):
    """Returns true if the commit's signature is valid, False otherwise.

    See :func:`sign_commit` for more background.

    Args:
      commit: dict repo commit
      key: :class:`Crypto.PublicKey.ECC.EccKey`

    Raises:
      KeyError if the commit isn't signed, ie doesn't have a `sig` field
    """
    commit = copy.copy(commit)
    sig = commit.pop('sig')

    verifier = DSS.new(key.public_key(), 'fips-186-3', randfunc=_randfunc)
    try:
        verifier.verify(SHA256.new(dag_cbor.encode(commit)), sig)
        return True
    except ValueError:
        logger.debug("Couldn't verify signature", exc_info=True)
        return False

