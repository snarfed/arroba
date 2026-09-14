"""Minimal top-level server instance.

Globals that clients can override:
* server (:class:`lexrpc.server.Server`)
* storage (:class:`storage.Storage`)
* auth (callable, () => str authenticated DID or `ALL_REPOS`)
"""
import os

from lexrpc.base import XrpcError
from lexrpc.server import Server

try:
    import flask
except ImportError:
    flask = None

from .util import parse_at_uri


# XRPC server
server = Server(validate=True, require_lexicons=False)

# initialized in app.py, testutil.XrpcTestCase.setUp
storage = None

# returned by auth for credentials that aren't tied to a user and can write to
# every repo, eg REPO_TOKEN.
ALL_REPOS = object()


def repo_token_auth():
    """Authenticates the current request. Default implementation of :func:`auth`.

    Checks that the `Authorization` header contains the ``$REPO_TOKEN`` bearer token.

    Returns:
      :data:`ALL_REPOS`

    Raises:
      ValueError: if the request isn't authenticated
      NotImplementedError: if ``$REPO_TOKEN`` isn't set
    """
    if not flask or not (token := os.environ.get('REPO_TOKEN')):
        raise NotImplementedError(
            'Authenticated XRPC methods are not currently supported')

    if flask.request.headers.get('Authorization') != f'Bearer {token}':
        raise ValueError('Invalid bearer token in Authorization header')

    return ALL_REPOS


# apps can replace this with their own function, eg arroba.server.auth = my_auth,
# with the same signature as repo_token_auth
auth = repo_token_auth


def auth_repo(did):
    """Authenticates the current request and checks that it can write to a repo.

    Args:
      did (str): the repo's DID

    Raises:
      XrpcError: if the credential can't write to the repo
      Exception: anything :func:`auth` raises
    """
    authed = auth()
    if authed is not ALL_REPOS and authed != did:
        raise XrpcError(f'Not authorized to write to {did}', name='AuthRequired')


def load_repo(did_or_at_uri):
    if did_or_at_uri.startswith('at://'):
        did_or_handle, _, _ = parse_at_uri(did_or_at_uri)
    else:
        did_or_handle = did_or_at_uri

    repo = storage.load_repo(did_or_handle)
    if not repo:
        raise XrpcError(f'Repo {did_or_handle} not found', name='RepoNotFound')
    elif repo.status:
        raise XrpcError(f'Repo {did_or_handle} is {repo.status}',
                        name='RepoDeactivated')

    return repo
