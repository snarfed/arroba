"""Utilities to create and resolve did:plcs and resolve did:webs.

* https://www.w3.org/TR/did-core/
* https://atproto.com/specs/did-plc
* https://github.com/bluesky-social/did-method-plc
* https://w3c-ccg.github.io/did-method-web/
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
import dag_cbor
from multiformats import multibase, multicodec
import requests

from . import util

DidPlc = namedtuple('DidPlc', [
    'did',      # str
    'privkey',  # ec.EllipticCurvePrivateKey
    'doc',      # dict, DID document
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


def create_plc(handle, privkey=None, pds_url=None, post_fn=requests.post):
    """Creates a new did:plc in a PLC registry.

    The PLC registry hostname is specified in the PLC_HOST environment variable.

    did:plc background:
    * https://atproto.com/specs/did-plc
    * https://github.com/bluesky-social/did-method-plc

    Args:
      handle: str, domain handle to associate with this DID
      privkey: :class:`ec.EllipticCurvePrivateKey`. The curve must be SECP256K1.
        If omitted, a new keypair will be created.
      pds_url: str, PDS base URL to associate with this DID. If omitted,
        defaults to `https://[PDS_HOST]`
      post_fn: callable for making HTTP POST requests

    Returns:
      :class:`DidPlc` with the newly created did:plc, keypair, and DID document

    Raises:
      ValueError, if any inputs are invalid
      :class:`requests.RequestException`, if the HTTP request to the PLC
        registry fails
    """
    assert os.environ["PLC_HOST"]

    if not isinstance(handle, str) or not handle:
        raise ValueError(f'{handle} is not a valid handle')

    if privkey:
        if not isinstance(privkey.curve, ec.SECP256K1):
            raise ValueError(
                f'Expected privkey to have SECP256K1 curve; got {privkey.curve}')
    else:
        logger.info('Generating new k256 keypair')
        privkey = util.new_key()

    if not pds_url:
        pds_url = f'https://{os.environ["PDS_HOST"]}'

    logger.info('Creating new did:plc for {handle} {pds_url}')
    pubkey = privkey.public_key()
    # https://atproto.com/specs/did#public-key-encoding
    pubkey_bytes = pubkey.public_bytes(serialization.Encoding.X962,
                                       serialization.PublicFormat.CompressedPoint)
    pubkey_multibase = multibase.encode(
        multicodec.wrap('secp256k1-pub', pubkey_bytes),
        'base58btc')
    did_key = f'did:key:{pubkey_multibase}'
    logger.info(f'  {did_key}')

    logger.info('Generating and signing DID document...')
    doc = {
        'type': 'plc_operation',
        'rotationKeys': [did_key],
        'verificationMethods': {
            'atproto': did_key,
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
    doc = util.sign(doc, privkey)
    doc['sig'] = base64.urlsafe_b64encode(doc['sig']).decode()
    sha256 = Hash(SHA256())
    sha256.update(dag_cbor.encode(doc))
    hash = sha256.finalize()
    did_plc = 'did:plc:' + base64.b32encode(hash)[:24].lower().decode()
    logger.info(f'  {did_plc}')

    plc_url = f'https://{os.environ["PLC_HOST"]}/{did_plc}'
    logger.info(f'Publishing to {plc_url}  ...')
    resp = post_fn(plc_url, json=doc)
    resp.raise_for_status()
    logger.info(f'{resp} {resp.content}')

    return DidPlc(did=did_plc, privkey=privkey, doc=doc)


def resolve_web(did, get_fn=requests.get):
    """Resolves a did:web by fetching its DID document.

    did:web spec: https://w3c-ccg.github.io/did-method-web/

    Args:
      did: str

    Returns:
      dict, JSON DID document
      get_fn: callable for making HTTP GET requests

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
