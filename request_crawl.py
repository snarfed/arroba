"""Run to connect your PDS to the BGS/AppView, once you've started it running."""
import logging
import os
from datetime import timedelta
from pathlib import Path

from lexrpc.client import Client

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

bgs = os.environ.get('BGS_HOST', 'bgs.bsky-sandbox.dev')
scheme = ('http' if bgs.startswith('localhost') or bgs.startswith('127.0.0.1')
          else 'https')
client = Client(f'{scheme}://{bgs}')
pds = os.environ.get('PDS_HOST') or open('pds_host').read().strip()
client.com.atproto.sync.requestCrawl({'hostname': pds})

print('OK')
