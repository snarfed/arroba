"""``com.atproto.server.*`` XRPC methods."""
import logging
import os

from . import server

logger = logging.getLogger(__name__)


@server.server.method('com.atproto.server.createSession')
def create_session(input):
    """Handler for ``com.atproto.server.createSession`` XRPC method."""
    id = input['identifier']
    repo = server.storage.load_repo(id)
    if not repo:
        raise ValueError(f'Repo {id} not found')

    # TODO: generate JWT
    token = os.environ['REPO_TOKEN']
    return {
        'handle': repo.handle,
        'did': repo.did,
        'accessJwt': token,
        'refreshJwt': token,
    }


@server.server.method('com.atproto.server.getSession')
def get_session(input):
    """Handler for ``com.atproto.server.getSession`` XRPC method."""
    server.auth()

    # TODO: parse JWT, extract repo DID
    # decoded = jwt.decode(data, server.repo.privkey, algorithm='ES256K')
    return {
        'handle': server.repo.handle,
        'did': server.repo.did,
    }


@server.server.method('com.atproto.server.refreshSession')
def refresh_session(input, did=None, commit=None):
    """Handler for ``com.atproto.server.refreshSession`` XRPC method."""
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
    """Handler for ``com.atproto.server.describeServer`` XRPC method."""
    return {
        'availableUserDomains': [],
        # what is this for?! bsky.social sets it to did:web:bsky.social
        # https://github.com/bluesky-social/atproto/pull/2170#pullrequestreview-1889553896
        'did': f'did:web:{os.environ["PDS_HOST"]}',
    }


@server.server.method('com.atproto.server.getAccountInviteCodes')
def get_account_invite_codes(input, includeUsed=None, createAvailable=None):
    """Handler for ``com.atproto.server.getAccountInviteCodes`` XRPC method."""
    return {'codes': []}


@server.server.method('com.atproto.server.listAppPasswords')
def list_app_passwords(input):
    """Handler for ``com.atproto.server.listAppPasswords`` XRPC method."""
    return {'passwords': []}
