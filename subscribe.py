"""Minimal subscribeRepos (firehose) client.

Outputs DAG-JSON encoded payloads to stdout in JSON Lines format, one object per
line.

Usage: subscribe.py [RELAY_HOST [CURSOR]]
"""
import json
import os
import sys

import dag_json
from lexrpc.client import Client


if __name__ == '__main__':
    assert len(sys.argv) <= 3
    host = sys.argv[1] if len(sys.argv) >= 2 else 'bgs.bsky-sandbox.dev'
    scheme = 'http' if host.split(':')[0] == 'localhost' else 'https'
    client = Client(f'{scheme}://{host}')
    kwargs = {'cursor': sys.argv[2]} if len(sys.argv) == 3 else {}
    for msg in client.com.atproto.sync.subscribeRepos(**kwargs):
        print(dag_json.encode(msg).decode(), file=sys.stdout, flush=True)
