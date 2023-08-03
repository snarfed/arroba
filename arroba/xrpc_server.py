"""com.atproto.server.* XRPC methods."""
import logging
import os

from . import server

logger = logging.getLogger(__name__)


# @server.server.method('com.atproto.server.createAccount')
# def create_account(input):
#     """
#     """


@server.server.method('com.atproto.server.createSession')
def create_session(input):
    """
    """
    repo_handle = server.repo.did.removeprefix('did:web:')
    input_handle = input['identifier'].removeprefix('did:web:')

    if (input_handle == repo_handle
            and input['password'] == os.environ['ARROBA_PASSWORD']):
        jwt = os.environ['ARROBA_JWT']
        return {
            'handle': server.repo.did.removeprefix('did:web:'),
            'did': server.repo.did,
            'accessJwt': jwt,
            'refreshJwt': jwt,
        }

    raise ValueError('Bad user or password')


@server.server.method('com.atproto.server.getSession')
def get_session(input):
    """
    """
    server.auth()

    return {
        'handle': server.repo.did.removeprefix('did:web:'),
        'did': server.repo.did,
    }


@server.server.method('com.atproto.server.refreshSession')
def refresh_session(input, did=None, commit=None):
    """
    """
    server.auth()

    return {
        'handle': server.repo.did.removeprefix('did:web:'),
        'did': server.repo.did,
        'accessJwt': jwt,
        'refreshJwt': jwt,
    }


@server.server.method('com.atproto.server.describeServer')
def describe_server(input):
    """
    """
    return {'availableUserDomains': []}
