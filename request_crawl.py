"""Run to connect your PDS to the BGS/AppView, once you've started it running."""
import logging
import os
from datetime import timedelta
from pathlib import Path

from lexrpc.client import Client

from arroba import util

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

repo_did = os.environ.get('REPO_DID')
if not repo_did:
    did_docs = list(Path(__file__).parent.glob('did:plc:*.json'))
    assert len(did_docs) == 1, f'Expected one DID doc file; got {did_docs}'
    repo_did = did_docs[0].name.removesuffix('.json')

privkey_pem = os.environ.get('REPO_PRIVKEY') or open('privkey.pem').read().strip()

bgs = os.environ.get('BGS_HOST', 'bgs.bsky-sandbox.dev')
scheme = ('http' if bgs.startswith('localhost') or bgs.startswith('127.0.0.1')
          else 'https')
token = util.service_jwt(host=bgs, repo_did=repo_did, privkey=privkey_pem,
                         expiration=timedelta(days=999))

client = Client(f'{scheme}://{bgs}', access_token=token)
pds = os.environ.get('PDS_HOST') or open('pds_host').read().strip()
client.com.atproto.sync.requestCrawl({'hostname': pds})

print('OK')
