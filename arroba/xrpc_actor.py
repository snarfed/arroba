"""``com.atproto.server.*`` XRPC methods."""
import logging

from . import server

logger = logging.getLogger(__name__)


@server.server.method('app.bsky.actor.getPreferences')
def get_preferences(input):
    """Stub for ``app.bsky.actor.getPreferences``. Returns an empty array."""
    return {'preferences': []}
