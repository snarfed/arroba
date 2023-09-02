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

did = create_plc(os.environ['REPO_HANDLE'], privkey=privkey)

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
    'controller': did.did,
    'publicKeyMultibase': pubkey_multibase,
}
print(f'did:key object:')
json.dump(did_key_obj, sys.stdout, indent=2)
print()
