"""com.atproto.identity.* XRPC methods."""
import logging

from . import server

logger = logging.getLogger(__name__)


@server.server.method('com.atproto.server.resolveHandle')
def resolve_handle(input, handle=None):
    """
    """
