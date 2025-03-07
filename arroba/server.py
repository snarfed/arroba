"""Temporary!"""
import os

from lexrpc.base import XrpcError
from lexrpc.server import Server

try:
    import flask
except ImportError:
    flask = None

from .util import parse_at_uri


# XRPC server
server = Server(validate=True)

# initialized in app.py, testutil.XrpcTestCase.setUp
storage = None


def auth():
    token = os.environ.get('REPO_TOKEN')
    if not token:
        raise NotImplementedError(
            'Authenticated XRPC methods are not currently supported')

    if flask and flask.request.headers.get('Authorization') != f'Bearer {token}':
        raise ValueError('Invalid bearer token in Authorization header')


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
