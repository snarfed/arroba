"""Temporary!"""
import os

from flask import request
from lexrpc.server import Server

from .util import parse_at_uri


# XRPC server
server = Server(validate=False)

# initialized in app.py, testutil.XrpcTestCase.setUp
storage = None


def auth():
    if request.headers.get('Authorization') != f'Bearer {os.environ["REPO_TOKEN"]}':
        raise ValueError('Invalid bearer token in Authorization header')


def load_repo(did_or_at_uri):
    if did_or_at_uri.startswith('at://'):
        did_or_handle, _, _ = parse_at_uri(did_or_at_uri)
    else:
        did_or_handle = did_or_at_uri

    repo = storage.load_repo(did_or_handle)
    if not repo:
        raise ValueError(f'Repo {repo} not found')
    return repo
