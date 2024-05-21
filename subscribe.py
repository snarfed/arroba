"""Minimal subscribeRepos (firehose) client.

Outputs DAG-JSON encoded payloads to stdout in JSON Lines format, one object per
line.

Usage: subscribe.py [RELAY_HOST [CURSOR]]
"""
import json
import os
import sys

from carbox.car import Block, read_car, write_car
from carbox.message import read_event_pair
import dag_cbor
import dag_json
from lexrpc.client import Client


if __name__ == '__main__':
    assert len(sys.argv) <= 3
    host = sys.argv[1] if len(sys.argv) >= 2 else 'bgs.bsky-sandbox.dev'
    scheme = 'http' if host.split(':')[0] == 'localhost' else 'https'
    client = Client(f'{scheme}://{host}')
    kwargs = {'cursor': sys.argv[2]} if len(sys.argv) == 3 else {}

    for header, payload in client.com.atproto.sync.subscribeRepos(**kwargs):
        output = json.loads(dag_json.encode(payload).decode())
        output['blocks'] = output['blocks']['/']['bytes'][:32] + '…'
        print(output.get('seq'), header, output,
              file=sys.stdout, flush=True)

        roots, blocks = read_car(payload['blocks'])
        if blocks:
            blocks = {block.cid: block for block in blocks}
            for op in payload.get('ops', []):
                record = blocks[op['cid']].decoded if op['cid'] else ''
                print('    ', op['action'], op['path'], record,
                      file=sys.stdout, flush=True)

        print()
