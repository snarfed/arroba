"""com.atproto.identity.* XRPC methods."""
import logging

from . import server

logger = logging.getLogger(__name__)


@server.server.method('com.atproto.server.resolveHandle')
def resolve_handle(input, handle=None):
    """
    """
    assert handle
    handle = handle.lower()

    if server.repo.did == f'did:web:{handle}':
        return {'did': server.repo.did}

    raise ValueError(f'{handle} not found')
