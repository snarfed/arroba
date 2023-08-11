"""Demo PDS app."""
from datetime import datetime, timedelta
import logging
import os
from urllib.parse import urljoin

from Crypto.PublicKey import ECC
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from flask import Flask, make_response, redirect, request
import google.cloud.logging
from google.cloud import ndb
import jwt
import lexrpc.flask_server

logger = logging.getLogger(__name__)
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
for logger in ('google.cloud', 'oauthlib', 'requests', 'requests_oauthlib',
               'urllib3'):
  logging.getLogger(logger).setLevel(logging.INFO)
# logging.getLogger('lexrpc').setLevel(logging.INFO)

from arroba.repo import Repo
from arroba import server
from arroba.datastore_storage import DatastoreStorage
from arroba import xrpc_identity, xrpc_repo, xrpc_server, xrpc_sync

os.environ.setdefault('APPVIEW_HOST', 'api.bsky-sandbox.dev')
os.environ.setdefault('BGS_HOST', 'bgs.bsky-sandbox.dev')
os.environ.setdefault('PLC_HOST', 'plc.bsky-sandbox.dev')
os.environ.setdefault('PDS_HOST', open('pds_host').read().strip())
os.environ.setdefault('REPO_DID', open('repo_did').read().strip())
os.environ.setdefault('REPO_HANDLE', open('repo_handle').read().strip())
os.environ.setdefault('REPO_PRIVKEY', open('privkey.pem').read().strip())
os.environ.setdefault('REPO_PASSWORD', open('repo_password').read().strip())
os.environ.setdefault('REPO_TOKEN', open('repo_token').read().strip())

if os.environ.get('GAE_ENV') == 'standard':
    # prod App Engine
    logging_client = google.cloud.logging.Client()
    logging_client.setup_logging(log_level=logging.DEBUG)
else:
    # local
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(
        os.path.dirname(__file__), 'fake_user_account.json')
    os.environ.setdefault('CLOUDSDK_CORE_PROJECT', 'app')
    os.environ.setdefault('DATASTORE_DATASET', 'app')
    os.environ.setdefault('GOOGLE_CLOUD_PROJECT', 'app')
    os.environ.setdefault('DATASTORE_EMULATOR_HOST', 'localhost:8089')

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ['REPO_TOKEN']
app.json.compact = False

# redirect app.bsky.* XRPCs to sandbox AppView
# https://atproto.com/blog/federation-developer-sandbox#bluesky-app-view
#
# WARNING: this only works for GETs, but we're doing it for POSTs too. should be
# ok as long as client apps don't send us app.bsky POSTs. we'll see.
@app.route(f'/xrpc/app.bsky.<nsid_rest>', methods=['GET', 'OPTIONS'])
def proxy_appview(nsid_rest=None):
    if request.method == 'GET':
        resp = redirect(urljoin('https://' + os.environ['APPVIEW_HOST'],
                                request.full_path))
    else:
        resp = make_response('')

    resp.headers.update(lexrpc.flask_server.RESPONSE_HEADERS)
    return resp

lexrpc.flask_server.init_flask(server.server, app)

server.key = ECC.import_key(os.environ['REPO_PRIVKEY'])

ndb_client = ndb.Client()

with ndb_client.context():
    server.storage = DatastoreStorage()
    server.repo = Repo.create(server.storage, os.environ['REPO_DID'], server.key)

server.server.register('com.atproto.sync.subscribeRepos', xrpc_sync.subscribe_repos)
server.repo.callback = xrpc_sync.enqueue_commit

# https://atproto.com/specs/xrpc#inter-service-authentication-temporary-specification
privkey_bytes = server.key = load_pem_private_key(
    os.environ['REPO_PRIVKEY'].encode(), password=None)
APPVIEW_JWT = jwt.encode({
    'iss': os.environ['REPO_DID'],
    'aud': f'did:web:{os.environ["APPVIEW_HOST"]}',
    'alg': 'ES256',  # p256
    'exp': int((datetime.now() + timedelta(days=7)).timestamp()),  # 😎
}, privkey_bytes, algorithm='ES256')

def ndb_context_middleware(wsgi_app):
    """WSGI middleware to add an NDB context per request.

    Copied from oauth_dropins.webutil.flask_util.
    """
    def wrapper(environ, start_response):
        with ndb_client.context():
            return wsgi_app(environ, start_response)

    return wrapper


app.wsgi_app = ndb_context_middleware(app.wsgi_app)
