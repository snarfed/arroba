"""com.atproto.server.* XRPC methods."""
import logging
import os

from . import server

logger = logging.getLogger(__name__)


@server.server.method('com.atproto.server.createSession')
def create_session(input):
    """
    """
    id = input['identifier']

    logger.debug(f'Expecting {server.repo.handle} {server.repo.did}')
    if (id and id in (server.repo.did, server.repo.handle)
            and input['password'] == os.environ['REPO_PASSWORD']):
        token = os.environ['REPO_TOKEN']
        return {
            'handle': server.repo.handle,
            'did': server.repo.did,
            'accessJwt': token,
            'refreshJwt': token,
        }

    raise ValueError('Bad user or password')


@server.server.method('com.atproto.server.getSession')
def get_session(input):
    """
    """
    server.auth()

    return {
        'handle': server.repo.handle,
        'did': server.repo.did,
    }


@server.server.method('com.atproto.server.refreshSession')
def refresh_session(input, did=None, commit=None):
    """
    """
    server.auth()

    token = os.environ['REPO_TOKEN']
    return {
        'handle': server.repo.handle,
        'did': server.repo.did,
        'accessJwt': token,
        'refreshJwt': token,
    }


@server.server.method('com.atproto.server.describeServer')
def describe_server(input):
    """
    """
    return {'availableUserDomains': []}
