"""Utilities to create and resolve did:plcs and resolve did:webs.

* https://www.w3.org/TR/did-core/
* https://atproto.com/specs/did-plc
* https://github.com/bluesky-social/did-method-plc
* https://w3c-ccg.github.io/did-method-web/
"""
import os

import requests


def resolve_plc(did):
    """Resolves a did:plc by fetching its DID document from a PLC registry.

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
