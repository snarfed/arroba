"""Based on DavidBuchanan314/picopds/create_identity.py. Thank you David!

Run inside the arroba virtualenv. Requires the REPO_HANDLE, PDS_HOST, and
PLC_HOST environment variables. Example usage:

env REPO_HANDLE=arroba3.snarfed.org \
  PDS_HOST=arroba-pds.appspot.com \
  PLC_HOST=plc.bsky-sandbox.dev \
  python ./create_identity.py

Notes:
* Generates a K-256 (SECP256K1) keypair. Writes the private key to privkey.pem.
* Generates and writes a DID document to [did].json
* Publishes the DID document to $PLC_HOST
"""
import base64
import json
import os
import sys

from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.hashes import Hash, SHA256
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature

import dag_cbor
from multiformats import multibase, multicodec
import requests

from arroba.util import new_key, sign_commit


assert os.environ['REPO_HANDLE']
assert os.environ['PDS_HOST']
assert os.environ['PLC_HOST']

print('Generating new k256 keypair...')
# https://atproto.com/specs/cryptography
privkey = new_key()
pubkey = privkey.public_key()
# https://atproto.com/specs/did#public-key-encoding
pubkey_bytes = pubkey.public_bytes(serialization.Encoding.X962,
                                   serialization.PublicFormat.CompressedPoint)
pubkey_multibase = multibase.encode(multicodec.wrap('secp256k1-pub', pubkey_bytes),
                                    'base58btc')
did_key = f'did:key:{pubkey_multibase}'
print(f'  {did_key}')

print('Writing private key to privkey.pem...')
with open('privkey.pem', 'wb') as f:
    f.write(privkey.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ))

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
sha256 = Hash(SHA256())
sha256.update(dag_cbor.encode(genesis))
hash = sha256.finalize()
did_plc = 'did:plc:' + base64.b32encode(hash)[:24].lower().decode()
print('  ', did_plc)

filename = f'{did_plc}.json'
print(f'Writing DID document to {filename}...')
with open(filename, 'w') as f:
    json.dump(genesis, f, indent=2)

json.dump(genesis, sys.stdout, indent=2)
print()

# https://atproto.com/specs/did#public-key-encoding
# https://cryptography.io/en/latest/hazmat/primitives/asymmetric/serialization/#cryptography.hazmat.primitives.serialization.Encoding

# TODO: how to get uncompressed public key bytes as required by ATProto
# for the `publicKeyMultibase` field here?
# https://atproto.com/specs/did#public-key-encoding
# pubkey_bytes = pubkey.public_bytes(serialization.Encoding.Raw,  # is this right?!
#                                    serialization.PublicFormat.Raw)
# pubkey_multibase = multibase.encode(pubkey_bytes, 'base58btc') + 'z'
pubkey_multibase = 'TODO'
did_key_obj = {
    'id': '#atproto',
    'type': 'k256',
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
