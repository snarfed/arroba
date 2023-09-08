"""Temporary!"""
import os

from flask import request
from lexrpc.server import Server


# XRPC server
server = Server(validate=False)

# these are initialized in app.py, testutil.XrpcTestCase.setUp
storage = None
repo = None


def auth():
    if request.headers.get('Authorization') != f'Bearer {os.environ["REPO_TOKEN"]}':
        raise ValueError('Invalid bearer token in Authorization header')
