"""com.atproto.identity.* XRPC methods."""
import logging
import os

from lexrpc.client import Client

from . import server

logger = logging.getLogger(__name__)


# STATE: get rid of this, not needed
@server.server.method('com.atproto.identity.resolveHandle')
def resolve_handle(input, handle=None):
    """Proxies to the appview."""
    appview = Client('https://' + os.environ['APPVIEW_HOST'], server.lexicons)
    return appview.com.atproto.identity.resolveHandle(input, handle=handle)
