"""Run to connect your PDS to the relay/AppView, once you've started it running."""
import logging
import os
from datetime import timedelta
from pathlib import Path

from lexrpc.client import Client

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

relay = os.environ.get('RELAY_HOST', os.environ.get('BGS_HOST') or 'bgs.bsky-sandbox.dev')
scheme = ('http' if relay.startswith('localhost') or relay.startswith('127.0.0.1')
          else 'https')
client = Client(f'{scheme}://{relay}')
pds = os.environ.get('PDS_HOST') or open('pds_host').read().strip()
client.com.atproto.sync.requestCrawl({'hostname': pds})

print('OK')
