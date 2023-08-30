"""Utilities to create and resolve did:plcs and resolve did:webs.

* https://www.w3.org/TR/did-core/
* https://atproto.com/specs/did-plc
* https://github.com/bluesky-social/did-method-plc
* https://w3c-ccg.github.io/did-method-web/
"""
import os
import urllib.parse

import requests


def resolve(did):
    """Resolves a did:plc or did:web.

    Args:
      did: str

    Returns:
      dict, JSON DID document

    Raises:
      ValueError, if the input did is not a did:plc or did:web str
      requests.RequestException, if an HTTP request fails
    """
    if did:
        if did.startswith('did:plc:'):
            return resolve_plc(did)
        elif did.startswith('did:web:'):
            return resolve_web(did)

    raise ValueError(f'{did} is not a did:plc or did:web')


def resolve_plc(did):
    """Resolves a did:plc by fetching its DID document from a PLC registry.

    did:plc background:
    * https://atproto.com/specs/did-plc
    * https://github.com/bluesky-social/did-method-plc

    Args:
      did: str

    Returns:
      dict, JSON DID document

    Raises:
      ValueError, if the input did is not a did:plc str
      requests.RequestException, if the HTTP request fails
    """
    if not isinstance(did, str) or not did.startswith('did:plc:'):
        raise ValueError(f'{did} is not a did:plc')

    resp = requests.get(f'https://{os.environ["PLC_HOST"]}/{did}')
    resp.raise_for_status()
    return resp.json()


def resolve_web(did):
    """Resolves a did:web by fetching its DID document.

    did:web spec: https://w3c-ccg.github.io/did-method-web/

    Args:
      did: str

    Returns:
      dict, JSON DID document

    Raises:
      ValueError, if the input did is not a did:web str
      requests.RequestException, if the HTTP request fails
    """
    if not isinstance(did, str) or not did.startswith('did:web:'):
        raise ValueError(f'{did} is not a did:web')

    did = did.removeprefix('did:web:')
    if ':' in did:
        did = did.replace(':', '/')
    else:
        did += '/.well-known'

    resp = requests.get(f'https://{urllib.parse.unquote(did)}/did.json')
    resp.raise_for_status()
    return resp.json()
