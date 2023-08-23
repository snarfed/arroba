"""Run to connect your PDS to the BGS/AppView, once you've started it running."""
import os
from pathlib import Path
from datetime import datetime, timedelta

import requests

from arroba import jwt_monkeypatch as jwt


os.environ.setdefault('BGS_HOST', 'bgs.bsky-sandbox.dev')
os.environ.setdefault('PDS_HOST', open('pds_host').read().strip())
os.environ.setdefault('REPO_PRIVKEY', open('privkey.pem').read().strip())

did_docs = list(Path(__file__).parent.glob('did:plc:*.json'))
assert len(did_docs) == 1, f'Expected one DID doc file; got {did_docs}'
os.environ.setdefault('REPO_DID', did_docs[0].name.removesuffix('.json'))


# https://atproto.com/specs/xrpc#inter-service-authentication-temporary-specification
# https://atproto.com/specs/cryptography
token = jwt.encode({
    'iss': os.environ['REPO_DID'],
    'aud': f'did:web:{os.environ["BGS_HOST"]}',
    'exp': int((datetime.now() + timedelta(days=999)).timestamp()),  # 😎
}, os.environ['REPO_PRIVKEY'], algorithm='ES256K')

url = f'https://{os.environ["BGS_HOST"]}/xrpc/com.atproto.sync.requestCrawl'
print(f'Fetching {url}')
resp = requests.get(url, params={'hostname': os.environ['PDS_HOST']},
                    headers={'Authorization': f'Bearer {token}'})
print(resp.content)
resp.raise_for_status()
print('OK')
