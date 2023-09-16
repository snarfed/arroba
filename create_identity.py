"""Based on DavidBuchanan314/picopds/create_identity.py. Thank you David!

Run inside the arroba virtualenv. Requires the REPO_HANDLE, PDS_HOST, and
PLC_HOST environment variables. Example usage:

env REPO_HANDLE=arroba1.snarfed.org \
  PDS_HOST=arroba-pds.appspot.com \
  PLC_HOST=plc.bsky-sandbox.dev \
  python ./create_identity.py

Notes:
* Uses K-256 (SECP256K1) keypair. If privkey.pem exists, uses it as the private
  key. Otherwise, generates a new keypair and writes it to privkey.pem.
* Generates and writes a DID document to [did].json
* Publishes the DID document to $PLC_HOST
"""
import json
import logging
import os
import sys

from cryptography.hazmat.primitives import serialization

from arroba.did import create_plc

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)

privkey = None
if os.path.exists('privkey.pem'):
    print('Loading k256 key from privkey.pem...')
    with open('privkey.pem', 'rb') as f:
        privkey = serialization.load_pem_private_key(f.read(), password=None)
else:
    print('Generating new k256 keypair into privkey.pem...')
    # https://atproto.com/specs/cryptography
    privkey = new_key()
    with open('privkey.pem', 'wb') as f:
        f.write(privkey.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ))

did = create_plc(os.environ['REPO_HANDLE'], signing_key=privkey, rotation_key=privkey)

filename = f'{did.did}.json'
print(f'Writing DID document to {filename}...')
with open(filename, 'w') as f:
    json.dump(did.doc, f, indent=2)

print('Done.')
