"""Minimal subscribeRepos (firehose) client.

Outputs DAG-JSON encoded payloads to stdout in JSON Lines format, one object per
line.

Usage: subscribe.py [BGS_HOST [CURSOR]]
"""
import json
import os
from pathlib import Path
import sys

import dag_json
from lexrpc.client import Client

lexicons = []
for filename in (Path(__file__).parent / 'arroba/lexicons').glob('**/*.json'):
    with open(filename) as f:
        lexicons.append(json.load(f))

if __name__ == '__main__':
    assert len(sys.argv) <= 3
    host = sys.argv[1] if len(sys.argv) >= 2 else 'bgs.bsky-sandbox.dev'
    client = Client(f'https://{host}', lexicons)
    kwargs = {'cursor': sys.argv[2]} if len(sys.argv) == 3 else {}
    for msg in client.com.atproto.sync.subscribeRepos(**kwargs):
        print(dag_json.encode(msg).decode(), file=sys.stdout)
