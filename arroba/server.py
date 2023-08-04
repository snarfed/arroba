"""Temporary!"""
import json
import os
from pathlib import Path
import random

from Crypto.PublicKey import ECC
from flask import request
from lexrpc.server import Server

from .mst import MST
from .repo import Repo
from .storage import MemoryStorage


# duplicates testutil
random.seed(1234567890)

# XRPC server
lexicons = []
for filename in (Path(__file__).parent / 'lexicons').glob('**/*.json'):
    with open(filename) as f:
        lexicons.append(json.load(f))

server = Server(lexicons, validate=False)

# repo
key = ECC.generate(curve='P-256', randfunc=random.randbytes)
storage = None
repo = None

def init():
    global repo, storage
    storage = MemoryStorage()
    repo = Repo.create(storage, 'did:web:user.com', key)


def auth():
    if request.headers.get('Authorization') != f'Bearer {os.environ["ARROBA_JWT"]}':
        raise ValueError('Invalid bearer token in Authorization header')
