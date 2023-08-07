"""Based on DavidBuchanan314/picopds/create_identity.py. Thank you David!

Run inside the arroba virtualenv. Requires the REPO_HANDLE, PDS_HOST, and
PLC_HOST environment variables. Example usage:

env REPO_HANDLE=arroba1.snarfed.org \
  PDS_HOST=arroba-pds.appspot.com \
  PLC_HOST=plc.bsky-sandbox.dev \
  python ./create_identity.py

Notes:
* Generates a P-256 (not SECP256K1) keypair. Writes the private key to
  privkey.pem.
* Generates and writes a DID document to [did].json
* Publishes the DID document to $PLC_HOST
"""
import base64
import json
import os
import sys

from Crypto.Hash import SHA256
from Crypto.PublicKey import ECC
from Crypto.Signature import DSS
import dag_cbor
from multiformats import multibase, multicodec
import requests

from arroba.util import new_p256_key, sign_commit


assert os.environ['REPO_HANDLE']
assert os.environ['PDS_HOST']
assert os.environ['PLC_HOST']

print('Generating new P-256 keypair...')
privkey = new_p256_key()
pubkey = privkey.public_key()
# https://atproto.com/specs/did#public-key-encoding
# https://www.pycryptodome.org/src/public_key/ecc#Crypto.PublicKey.ECC.EccKey.export_key
pubkey_bytes = pubkey.export_key(format='raw', compress=True)
pubkey_multibase = multibase.encode(multicodec.wrap('p256-pub', pubkey_bytes),
                                    'base58btc')
did_key = f'did:key:{pubkey_multibase}'
print(f'  {did_key}')

print('Writing private key to privkey.pem...')
with open('privkey.pem', 'w') as f:
    f.write(privkey.export_key(format='PEM'))

# https://atproto.com/specs/did#did-documents
print('Generating and signing DID document...')
genesis = {
    'type': 'plc_operation',
    'rotationKeys': [did_key],
    'verificationMethods': {
        'atproto': did_key,
    },
    'alsoKnownAs': [
        f'at://{os.environ["REPO_HANDLE"]}',
    ],
    'services': {
        'atproto_pds': {
            'type': 'AtprotoPersonalDataServer',
            'endpoint': f'https://{os.environ["PDS_HOST"]}',
        }
    },
    'prev': None,
}
genesis = sign_commit(genesis, privkey)
genesis['sig'] = base64.urlsafe_b64encode(genesis['sig']).decode()
hash = SHA256.new(dag_cbor.encode(genesis)).digest()
did_plc = 'did:plc:' + base64.b32encode(hash)[:24].lower().decode()
print('  ', did_plc)

filename = f'{did_plc}.json'
print(f'Writing DID document to {filename}...')
with open(filename, 'w') as f:
    json.dump(genesis, f, indent=2)

json.dump(genesis, sys.stdout, indent=2)
print()

# https://atproto.com/specs/did#public-key-encoding
# https://www.pycryptodome.org/src/public_key/ecc#Crypto.PublicKey.ECC.EccKey.export_key
pubkey_bytes = pubkey.export_key(format='raw')
pubkey_multibase = multibase.encode(pubkey_bytes, 'base58btc') + 'z'
did_key_obj = {
    'id': '#atproto',
    'type': 'p256',
    'controller': did_plc,
    'publicKeyMultibase': pubkey_multibase,
}
print(f'did:key object:')
json.dump(did_key_obj, sys.stdout, indent=2)
print()

plc_url = f'https://{os.environ["PLC_HOST"]}/{did_plc}'
print(f'Publishing to {plc_url}  ...')
resp = requests.post(plc_url, json=genesis)
resp.raise_for_status()
print(resp, resp.content)

print(f'{did_plc} should now be live at', plc_url)
