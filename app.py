"""Demo PDS app."""
import logging
import os

logger = logging.getLogger(__name__)
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
# logging.getLogger('lexrpc').setLevel(logging.INFO)

from flask import Flask
from google.cloud import ndb
import lexrpc.flask_server

from arroba import server
from arroba import xrpc_identity, xrpc_repo, xrpc_server, xrpc_sync


if os.environ.get('GAE_ENV') != 'standard':
    os.environ.setdefault('ARROBA_PASSWORD', 'sooper-sekret')
    os.environ.setdefault('ARROBA_JWT', 'towkin')


app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ['ARROBA_JWT']
app.json.compact = False

server.init()
lexrpc.flask_server.init_flask(server.server, app)


ndb_client = ndb.Client()

def ndb_context_middleware(wsgi_app):
    """WSGI middleware to add an NDB context per request.

    Copied from oauth_dropins.webutil.flask_util.
    """
    def wrapper(environ, start_response):
        with ndb_client.context():
            return wsgi_app(environ, start_response)

    return wrapper


app.wsgi_app = ndb_context_middleware(app.wsgi_app)
