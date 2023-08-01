"""com.atproto.server.* XRPC methods."""
import logging

from . import server

logger = logging.getLogger(__name__)


@server.server.method('com.atproto.server.createAccount')
def create_account(input):
    """
    """
    # input: {email, handle, did, inviteCode, password, recoveryKey}
    # output: {accessJwt, refreshJwt, handle, did}


@server.server.method('com.atproto.server.createSession')
def create_session(input):
    """
    """
    # input: {identifier, password}
    # output: {accessJwt, refreshJwt, handle, did, email}


@server.server.method('com.atproto.server.getSession')
def get_session(input):
    """
    """
    # output: {handle, did, email}


@server.server.method('com.atproto.server.refreshSession')
def refresh_session(input, did=None, commit=None):
    """
    """
    # output: {accessJwt, refreshJwt, handle, did}
