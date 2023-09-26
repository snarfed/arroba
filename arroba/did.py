"""Utilities to create and resolve did:plcs, did:webs, and handles.

* https://www.w3.org/TR/did-core/
* https://atproto.com/specs/did-plc
* https://github.com/bluesky-social/did-method-plc
* https://w3c-ccg.github.io/did-method-web/
* https://atproto.com/specs/handle#handle-resolution
"""
import base64
from collections import namedtuple
import json
import logging
import os
import urllib.parse

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


def resolve(did, **kwargs):
    """Resolves a did:plc or did:web.

    Args:
      did: str
      kwargs: passed through to :meth:`resolve_plc`/:meth:`resolve_web`

    Returns:
      dict, JSON DID document

    Raises:
      ValueError, if the input did is not a did:plc or did:web str
      requests.RequestException, if an HTTP request fails
    """
    if did:
        if did.startswith('did:plc:'):
            return resolve_plc(did, **kwargs)
        elif did.startswith('did:web:'):
            return resolve_web(did, **kwargs)

    raise ValueError(f'{did} is not a did:plc or did:web')


def resolve_plc(did, get_fn=requests.get):
    """Resolves a did:plc by fetching its DID document from a PLC registry.

    The PLC registry hostname is specified in the PLC_HOST environment variable.

    did:plc background:
    * https://atproto.com/specs/did-plc
    * https://github.com/bluesky-social/did-method-plc

    Args:
      did: str
      get_fn: callable for making HTTP GET requests

    Returns:
      dict, JSON DID document

    Raises:
      ValueError, if the input did is not a did:plc str
      requests.RequestException, if the HTTP request fails
    """
    if not isinstance(did, str) or not did.startswith('did:plc:'):
        raise ValueError(f'{did} is not a did:plc')

    resp = get_fn(f'https://{os.environ["PLC_HOST"]}/{did}')
    resp.raise_for_status()
    return resp.json()


def create_plc(handle, signing_key=None, rotation_key=None, pds_url=None,
               post_fn=requests.post):
    """Creates a new did:plc in a PLC registry.

    The PLC registry hostname is specified in the PLC_HOST environment variable.

    did:plc background:

    * https://atproto.com/specs/did-plc
    * https://github.com/bluesky-social/did-method-plc

    The DID document in the returned value is the *new format* DID doc, with the
    fully qualified `verificationMethod.id` and `Multikey` key encoding, ie
    `did:key` without the prefix. Details:
    https://github.com/bluesky-social/atproto/discussions/1510

    Args:
      handle: str, domain handle to associate with this DID
      signing_key: :class:`ec.EllipticCurvePrivateKey`. The curve must be SECP256K1.
        If omitted, a new keypair will be created.
      rotation_key: :class:`ec.EllipticCurvePrivateKey`. The curve must be SECP256K1.
        If omitted, a new keypair will be created.
      pds_url: str, PDS base URL to associate with this DID. If omitted,
        defaults to `https://[PDS_HOST]`
      post_fn: callable for making HTTP POST requests

    Returns:
      :class:`DidPlc` with the newly created did:plc, keys, and DID document

    Raises:
      ValueError, if any inputs are invalid
      :class:`requests.RequestException`, if the HTTP request to the PLC
        registry fails
    """
    assert os.environ["PLC_HOST"]

    if not isinstance(handle, str) or not handle:
        raise ValueError(f'{handle} is not a valid handle')

    if not pds_url:
        pds_url = f'https://{os.environ["PDS_HOST"]}'

    for key in signing_key, rotation_key:
        if key and not isinstance(key.curve, ec.SECP256K1):
            raise ValueError(f'Expected SECP256K1 key; got {key.curve}')

    if not signing_key:
        logger.info('Generating new k256 signing key')
        signing_key = util.new_key()

    if not rotation_key:
        logger.info('Generating new k256 rotation key')
        rotation_key = util.new_key()

    logger.info('Generating and signing PLC directory genesis operation...')
    # this is a PLC directory genesis operation for creating a new DID.
    # it's *not* a DID document. similar but not the same!
    # https://github.com/bluesky-social/did-method-plc#presentation-as-did-document
    create = {
        'type': 'plc_operation',
        'rotationKeys': [encode_did_key(rotation_key.public_key())],
        'verificationMethods': {
            'atproto': encode_did_key(signing_key.public_key()),
        },
        'alsoKnownAs': [
            f'at://{handle}',
        ],
        'services': {
            'atproto_pds': {
                'type': 'AtprotoPersonalDataServer',
                'endpoint': f'{pds_url}',
            }
        },
        'prev': None,
    }
    create = util.sign(create, rotation_key)
    create['sig'] = base64.urlsafe_b64encode(create['sig']).decode()
    sha256 = Hash(SHA256())
    sha256.update(dag_cbor.encode(create))
    hash = sha256.finalize()
    did_plc = 'did:plc:' + base64.b32encode(hash)[:24].lower().decode()
    logger.info(f'  {did_plc}')

    plc_url = f'https://{os.environ["PLC_HOST"]}/{did_plc}'
    logger.info(f'Publishing to {plc_url}  ...')
    resp = post_fn(plc_url, json=create)
    resp.raise_for_status()
    logger.info(f'{resp} {resp.content}')

    create['did'] = did_plc
    return DidPlc(did=did_plc, doc=plc_operation_to_did_doc(create),
                  signing_key=signing_key, rotation_key=rotation_key)


def encode_did_key(pubkey):
    """Encodes a :class:`ec.EllipticCurvePublicKey` into a `did:key` string.

    https://atproto.com/specs/did#public-key-encoding

    Args:
      pubkey: :class:`ec.EllipticCurvePublicKey`

    Returns:
      str, `did:key`
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
    """Decodes a `did:key` string into a :class:`ec.EllipticCurvePublicKey`.

    https://atproto.com/specs/did#public-key-encoding

    Args:
      did_key: str

    Returns:
      :class:`ec.EllipticCurvePublicKey`
    """
    assert did_key.startswith('did:key:')
    wrapped_bytes = multibase.decode(did_key.removeprefix('did:key:'))
    codec, data = multicodec.unwrap(wrapped_bytes)

    if codec.name == 'secp256k1-pub':
        curve = ec.SECP256K1()
    elif codec.name == 'p256-pub':
        curve = ec.SECP256R1()
    else:
        raise ValueError(f'Expected secp256k1 or secp256r1 curve, got {codec.name}')

    return ec.EllipticCurvePublicKey.from_encoded_point(curve, data)


def plc_operation_to_did_doc(op):
    """Converts a PLC directory operation to a DID document.

    https://github.com/bluesky-social/did-method-plc#presentation-as-did-document

    The DID document in the returned value is the *new format* DID doc, with the
    fully qualified `verificationMethod.id` and `Multikey` key encoding, ie
    `did:key` without the prefix. Details:
    https://github.com/bluesky-social/atproto/discussions/1510
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


def resolve_web(did, get_fn=requests.get):
    """Resolves a did:web by fetching its DID document.

    did:web spec: https://w3c-ccg.github.io/did-method-web/

    Args:
      did: str
      get_fn: callable for making HTTP GET requests

    Returns:
      dict, JSON DID document

    Raises:
      ValueError, if the input did is not a did:web str
      :class:`requests.RequestException`, if the HTTP request fails
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


def resolve_handle(handle, get_fn=requests.get):
    """Resolves an ATProto handle to a DID.

    Supports the DNS TXT record and HTTPS well-known methods.

    https://atproto.com/specs/handle#handle-resolution

    Args:
      handle: str
      get_fn: callable for making HTTP GET requests

    Returns:
      str, DID, or None if the handle can't be resolved

    Raises:
      ValueError, if handle is not a domain
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
    resp = get_fn(f'https://{handle}/.well-known/atproto-did')
    if resp.ok and resp.headers.get('Content-Type', '').split(';')[0] == 'text/plain':
        return resp.text.strip()

    return None
