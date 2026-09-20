"""Minimal top-level server instance.

Globals that clients can override:
* server (:class:`lexrpc.server.Server`)
* storage (:class:`storage.Storage`)
* authenticate (callable, () => (str authenticated DID or `ALL_REPOS`,
                         sequence of str OAuth scopes or `ALL_SCOPES`))
"""
import os

from lexrpc.base import XrpcError
from lexrpc.server import Server

try:
    import flask
except ImportError:
    flask = None

from . import permissions
from .util import parse_at_uri


# XRPC server
server = Server(validate=True, require_lexicons=False)

# initialized in app.py, testutil.XrpcTestCase.setUp
storage = None

ALL_REPOS = object()
"""Permission that allows access to every repo. Returned by :func:`global_token_auth`."""

ALL_SCOPES = object()
"""Permission that allows all OAuth scopes."""


def global_token_auth():
    """Checks that the current request includes the ``$REPO_TOKEN`` global token.

    ...in the `Authorization` header.

    Returns:
      (:data:`ALL_REPOS`, :data:`ALL_SCOPES`) tuple

    Raises:
      ValueError: if the request isn't authenticated
      NotImplementedError: if ``$REPO_TOKEN`` isn't set
    """
    if not flask or not (token := os.environ.get('REPO_TOKEN')):
        raise NotImplementedError(
            'Authenticated XRPC methods are not currently supported')

    if flask.request.headers.get('Authorization') != f'Bearer {token}':
        raise ValueError('Invalid bearer token in Authorization header')

    return ALL_REPOS, ALL_SCOPES


authenticate = global_token_auth
"""Callable that authenticates the current request.

Determines whether the current request has a logged in user, and if so,
what their permissions are.

Defaults to :func:`global_token_auth`. Clients should replace this with their own.

Returns:
  tuple: (str authenticated DID or `ALL_REPOS`,
          sequence of str OAuth scopes or `ALL_SCOPES`))

Raises:
  ValueError: if the request isn't authenticated
"""


def authorize(did, writes):
    """Authenticates the current request and checks that it can write to a repo.

    Calls :func:`authenticate` exactly once.

    Args:
      did (str): the repo's DID
      writes (sequence of :class:`util.Write`): the writes to check against
        the credential's OAuth scopes

    Returns:
      str or :data:`ALL_REPOS`: the authenticated DID

    Raises:
      XrpcError: if the credential can't write to the repo, or its scopes
        don't allow one of ``writes``
      ValueError: if the request isn't authenticated
    """
    authed_did, scopes = authenticate()
    if authed_did not in (did, ALL_REPOS):
        raise XrpcError(f'Not authenticated as {did}', name='AuthRequired')

    if scopes is not ALL_SCOPES:
        for write in writes:
            perm = permissions.Repo((write.collection,), (write.action,))
            if not permissions.allows(scopes, perm):
                msg = f'Missing scope repo:{write.collection}?action={write.action.name.lower()}'
                header = f'DPoP error="insufficient_scope", error_description="{msg}"'
                raise XrpcError(msg, name='insufficient_scope', status=403,
                                headers={'WWW-Authenticate': header})

    return authed_did


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
