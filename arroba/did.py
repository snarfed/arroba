"""Utilities to create and resolve did:plcs, did:webs, and handles.

* https://www.w3.org/TR/did-core/
* https://atproto.com/specs/did-plc
* https://github.com/bluesky-social/did-method-plc
* https://w3c-ccg.github.io/did-method-web/
* https://atproto.com/specs/handle#handle-resolution
"""
import base64
from collections import namedtuple
from datetime import timedelta
import json
import logging
import os
import re
import urllib.parse

from cachetools import cached, TTLCache
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.hashes import Hash, SHA256
from cryptography.hazmat.primitives import serialization
from dns.exception import DNSException
from dns.rdatatype import TXT
import dns.resolver
import dag_cbor
from multiformats import multibase, multicodec
import requests

from . import util

DidPlc = namedtuple('DidPlc', [
    'did',           # str
    'signing_key',   # ec.EllipticCurvePrivateKey
    'rotation_key',  # ec.EllipticCurvePrivateKey
    'doc',           # dict, DID document
])

logger = logging.getLogger(__name__)

CACHE_SIZE = 5000
CACHE_TTL = timedelta(hours=6)

# from https://atproto.com/specs/handle#handle-identifier-syntax
HANDLE_RE = re.compile(
    r'^([a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$')

# used as get_fn below. wrap so that we can mock requests.get in tests
requests_get = lambda *args, **kwargs: requests.get(*args, **kwargs)


def resolve(did, **kwargs):
    """Resolves a ``did:plc`` or ``did:web``.

    Args:
      did (str):
      kwargs: passed through to :func:`resolve_plc`/:func:`resolve_web`

    Returns:
      dict: JSON DID document

    Raises:
      ValueError: if the input is not a ``did:plc`` or ``did:web``
      requests.RequestException: if an HTTP request fails
    """
    if did:
        if did.startswith('did:plc:'):
            return resolve_plc(did, **kwargs)
        elif did.startswith('did:web:'):
            return resolve_web(did, **kwargs)

    raise ValueError(f'{did} is not a did:plc or did:web')


@cached(TTLCache(maxsize=CACHE_SIZE, ttl=CACHE_TTL.total_seconds()))
def resolve_plc(did, get_fn=requests_get):
    """Resolves a ``did:plc`` by fetching its DID document from a PLC directory.

    The PLC directory hostname is specified in the ``PLC_HOST`` environment
    variable.

    ``did:plc`` background:

    * https://atproto.com/specs/did-plc
    * https://github.com/bluesky-social/did-method-plc

    Args:
      did (str)
      get_fn (callable): for making HTTP GET requests

    Returns:
      dict: JSON DID document

    Raises:
      ValueError: if the input did is not a ``did:plc`` str
      requests.RequestException: if the HTTP request fails
    """
    if not isinstance(did, str) or not did.startswith('did:plc:'):
        raise ValueError(f'{did} is not a did:plc')

    resp = get_fn(f'https://{os.environ["PLC_HOST"]}/{did}')
    resp.raise_for_status()
    return resp.json()


def create_plc(handle, **kwargs):
    """Creates a new ``did:plc`` in a PLC directory.

    Args are documented in :func:`write_plc`.
    """
    assert 'did' not in kwargs
    assert 'prev' not in kwargs
    return write_plc(handle=handle, **kwargs)


def update_plc(did, **kwargs):
    """Updates an existing ``did:plc`` in a PLC directory.

    Args are documented in :func:`write_plc`.
    """
    assert 'rotation_key' in kwargs
    assert 'signing_key' in kwargs

    # get CID of previous head operation for this DID from the directory
    # response is a JSON list with operations from earliest to latest
    # https://github.com/did-method-plc/did-method-plc#audit-logs
    get_fn = kwargs.get('get_fn') or requests_get
    resp = get_fn(f'https://{os.environ["PLC_HOST"]}/{did}/log/audit')
    last_op = resp.json()[-1]

    # merge new data into existing data
    handle = last_op['operation']['alsoKnownAs'].pop(0)
    assert handle.startswith('at://')
    handle = handle.removeprefix('at://')
    kwargs.setdefault('handle', handle)

    kwargs.setdefault('also_known_as', last_op['operation']['alsoKnownAs'])

    # write update operation
    return write_plc(did=did, prev=last_op['cid'], **kwargs)


def write_plc(did=None, handle=None, signing_key=None, rotation_key=None,
              pds_url=None, also_known_as=None, prev=None,
              get_fn=requests_get, post_fn=requests.post):
    """Writes a PLC operation to a PLC directory.

    Generally used to create a new ``did:plc`` or update an existing one.

    The PLC directory hostname is specified in the ``PLC_HOST`` environment
    variable.

    ``did:plc`` background:

    * https://atproto.com/specs/did-plc
    * https://github.com/bluesky-social/did-method-plc

    The DID document in the returned value is the *new format* DID doc, with the
    fully qualified ``verificationMethod.id`` and ``Multikey`` key encoding, ie
    ``did:key`` without the prefix. Details:
    https://github.com/bluesky-social/atproto/discussions/1510

    Args:
      did (str): if provided, updates an existing DID, otherwise creates a new one.
      handle (str): domain handle to associate with this DID
      signing_key (ec.EllipticCurvePrivateKey): The curve must be SECP256K1.
        If omitted, a new keypair will be created.
      rotation_key (ec.EllipticCurvePrivateKey): The curve must be SECP256K1.
        If omitted, a new keypair will be created.
      pds_url (str): PDS base URL to associate with this DID. If omitted,
        defaults to ``https://[PDS_HOST]``
      also_known_as (str or sequence of str): additional URI or URIs to add to
        ``alsoKnownAs``
      prev (str): if an update, the CID of the previous operation for this DID
      get_fn (callable): for making HTTP GET requests
      post_fn (callable): for making HTTP POST requests

    Returns:
      DidPlc: with the newly created ``did:plc``, keys, and DID document

    Raises:
      ValueError: if any inputs are invalid
      requests.RequestException: if the HTTP request to the PLC directory fails
    """
    plc_host = os.environ['PLC_HOST']

    if not isinstance(handle, str) or not handle:
        raise ValueError(f'{handle} is not a valid handle')

    if not pds_url:
        pds_url = f'https://{os.environ["PDS_HOST"]}'
    assert not pds_url.endswith('/')

    for key in signing_key, rotation_key:
        if key and not isinstance(key.curve, ec.SECP256K1):
            raise ValueError(f'Expected SECP256K1 key; got {key.curve}')

    if not signing_key:
        logger.info('Generating new k256 signing key')
        signing_key = util.new_key()

    if not rotation_key:
        logger.info('Generating new k256 rotation key')
        rotation_key = util.new_key()

    if not also_known_as:
        also_known_as = []
    elif isinstance(also_known_as, str):
        also_known_as = [also_known_as]

    logger.info('Generating and signing PLC directory operation...')
    # this is a PLC directory genesis operation for creating or updating a DID.
    # it's *not* a DID document. similar but not the same!
    # https://github.com/bluesky-social/did-method-plc#presentation-as-did-document
    op = {
        'type': 'plc_operation',
        'rotationKeys': [encode_did_key(rotation_key.public_key())],
        'verificationMethods': {
            'atproto': encode_did_key(signing_key.public_key()),
        },
        'alsoKnownAs': [f'at://{handle}'] + list(also_known_as),
        'services': {
            'atproto_pds': {
                'type': 'AtprotoPersonalDataServer',
                'endpoint': f'{pds_url}',
            }
        },
        'prev': prev,
    }
    op = util.sign(op, rotation_key)
    op['sig'] = base64.urlsafe_b64encode(op['sig']).decode().rstrip('=')

    if did:
        logger.info(f'Updating existing DID {did}')
    else:
        sha256 = Hash(SHA256())
        sha256.update(dag_cbor.encode(op))
        hash = sha256.finalize()
        did = 'did:plc:' + base64.b32encode(hash)[:24].lower().decode()
        logger.info(f'Creating new DID {did}')

    plc_url = f'https://{plc_host}/{did}'
    logger.info(f'Publishing to {plc_url}  ...')
    resp = post_fn(plc_url, json=op)
    logger.info(f'{resp} {resp.content}')
    resp.raise_for_status()

    op['did'] = did
    return DidPlc(did=did, doc=plc_operation_to_did_doc(op),
                  signing_key=signing_key, rotation_key=rotation_key)


def encode_did_key(pubkey):
    """Encodes an :class:`ec.EllipticCurvePublicKey` into a ``did:key`` string.

    https://atproto.com/specs/did#public-key-encoding

    Args:
      pubkey (ec.EllipticCurvePublicKey)

    Returns:
      str: encoded ``did:key``
    """
    if isinstance(pubkey.curve, ec.SECP256K1):
        codec = 'secp256k1-pub'
    elif isinstance(pubkey.curve, ec.SECP256R1):
        codec = 'p256-pub'
    else:
        raise ValueError(f'Expected secp256k1 or secp256r1 curve, got {pubkey.curve}')

    pubkey_bytes = pubkey.public_bytes(serialization.Encoding.X962,
                                       serialization.PublicFormat.CompressedPoint)
    pubkey_multibase = multibase.encode(multicodec.wrap(codec, pubkey_bytes),
                                        'base58btc')
    return f'did:key:{pubkey_multibase}'


def decode_did_key(did_key):
    """Decodes a ``did:key`` string into an :class:`ec.EllipticCurvePublicKey`.

    https://atproto.com/specs/did#public-key-encoding

    Args:
      did_key (str)

    Returns:
      ec.EllipticCurvePublicKey
    """
    wrapped_bytes = multibase.decode(did_key.removeprefix('did:key:'))
    codec, data = multicodec.unwrap(wrapped_bytes)

    if codec.name == 'secp256k1-pub':
        curve = ec.SECP256K1()
    elif codec.name == 'p256-pub':
        curve = ec.SECP256R1()
    else:
        raise ValueError(f'Expected secp256k1 or secp256r1 curve, got {codec.name}')

    return ec.EllipticCurvePublicKey.from_encoded_point(curve, data)


def get_handle(did_doc):
    """Extracts and returns a DID's handle.

    Doesn't do bidirectional handle resolution! Just returns the handle in the
    first ``at://`` URI in ``alsoKnownAs``.

    Args:
      did_doc (dict): DID document

    Returns:
      str: handle, or None if the DID doc doens't have one
    """
    for aka in did_doc.get('alsoKnownAs', []):
        if aka.startswith('at://'):
            handle, _, _ = util.parse_at_uri(aka)
            if handle:
                return handle


def get_signing_key(did_doc):
    """Extracts and returns a DID's signing key.

    Args:
      did_doc (dict): DID document

    Returns:
      ec.EllipticCurvePublicKey, or None if the DID doc has no ATProto signing key
    """
    if not (did := did_doc.get('id')):
        return None

    for method in did_doc.get('verificationMethod', []):
        if method.get('id') == f'{did}#atproto':
            if key := method.get('publicKeyMultibase'):
                return decode_did_key(key)

def plc_operation_to_did_doc(op):
    """Converts a PLC directory operation to a DID document.

    https://github.com/bluesky-social/did-method-plc#presentation-as-did-document

    The DID document in the returned value is the *new format* DID doc, with the
    fully qualified ``verificationMethod.id`` and ``Multikey`` key encoding, ie
    ``did:key`` without the prefix. Details:
    https://github.com/bluesky-social/atproto/discussions/1510

    Args:
      op: dict, PLC operation, https://github.com/did-method-plc/did-method-plc#operation-serialization-signing-and-validation

    Returns:
      dict: DID document, https://www.w3.org/TR/did-core/#data-model
    """
    assert op

    signing_did_key = op['verificationMethods']['atproto']
    return {
        '@context': [
            'https://www.w3.org/ns/did/v1',
            'https://w3id.org/security/multikey/v1',
            'https://w3id.org/security/suites/secp256k1-2019/v1',
        ],
        'id': op['did'],
        'alsoKnownAs': op['alsoKnownAs'],
        'verificationMethod': [{
            'id': f'{op["did"]}#atproto',
            'type': 'EcdsaSecp256r1VerificationKey2019',
            'controller': op['did'],
            'publicKeyMultibase': signing_did_key.removeprefix('did:key:'),
        }],
        'service': [{
            'id': '#atproto_pds',
            'type': 'AtprotoPersonalDataServer',
            'serviceEndpoint': op['services']['atproto_pds']['endpoint'],
        }],
    }


@cached(TTLCache(maxsize=CACHE_SIZE, ttl=CACHE_TTL.total_seconds()))
def resolve_web(did, get_fn=requests_get):
    """Resolves a ``did:web`` by fetching its DID document.

    ``did:web`` spec: https://w3c-ccg.github.io/did-method-web/

    Args:
      did (str)
      get_fn (callable): for making HTTP GET requests

    Returns:
      dict: JSON DID document

    Raises:
      ValueError: if the input did is not a ``did:web`` str
      requests.RequestException: if the HTTP request fails
    """
    if not isinstance(did, str) or not did.startswith('did:web:'):
        raise ValueError(f'{did} is not a did:web')

    did = did.removeprefix('did:web:')
    if ':' in did:
        did = did.replace(':', '/')
    else:
        did += '/.well-known'

    resp = get_fn(f'https://{urllib.parse.unquote(did)}/did.json')
    resp.raise_for_status()
    return resp.json()


@cached(TTLCache(maxsize=CACHE_SIZE, ttl=CACHE_TTL.total_seconds()))
def resolve_handle(handle, get_fn=requests_get):
    """Resolves an ATProto handle to a DID.

    Supports the DNS TXT record and HTTPS well-known methods.

    https://atproto.com/specs/handle#handle-resolution

    Args:
      handle (str)
      get_fn (callable): for making HTTP GET requests

    Returns:
      str or None: DID, or None if the handle can't be resolved

    Raises:
      ValueError: if handle is not a domain
    """
    if not handle or not isinstance(handle, str) or not util.DOMAIN_RE.match(handle):
        raise ValueError(f"{handle} doesn't look like a domain")

    logger.info(f'Resolving handle {handle}')

    # DNS method
    name = f'_atproto.{handle}.'
    try:
        logger.info(f'Querying DNS TXT for {name}')
        answer = dns.resolver.resolve(name, TXT)
        logger.info(f'Got: {answer.response}')
        if answer.canonical_name.to_text() == name:
            for rdata in answer:
                if rdata.rdtype == TXT:
                    text = rdata.to_text()
                    if text.startswith('"did=did:'):
                        return text.strip('"').removeprefix('did=')
    except DNSException as e:
        logger.info(repr(e))

    # HTTPS well-known method
    try:
        resp = get_fn(f'https://{handle}/.well-known/atproto-did')
    except requests.RequestException as e:
        logger.info(f'HTTPS handle resolution failed: {e}')
        return None

    if resp.ok:
        did = resp.text.strip()
        if did.startswith('did:plc:') and len(did.removeprefix('did:plc:')) <= 24:
            return did

    return None
