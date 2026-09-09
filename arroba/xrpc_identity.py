"""``com.atproto.identity.*`` XRPC methods."""
import logging

from lexrpc.base import XrpcError
from webutil import util

from . import did
from . import server

logger = logging.getLogger(__name__)


@server.server.method('com.atproto.identity.resolveHandle')
def resolve_handle(input, handle=None):
    """Handler for ``com.atproto.identity.resolveHandle`` XRPC method."""
    if not (resolved := did.resolve_handle(handle, get_fn=util.session.get)):
        raise XrpcError(f"Couldn't resolve handle {handle}", name='HandleNotFound')

    return {'did': resolved}
