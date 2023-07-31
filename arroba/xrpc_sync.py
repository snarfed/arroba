"""com.atproto.sync.* XRPC methods."""
import logging
from pathlib import Path

from carbox.car import Block, write_car
import dag_cbor
from lexrpc.server import Server

from arroba.mst import MST
from arroba.repo import Repo
from arroba.storage import MemoryStorage

logger = logging.getLogger(__name__)


# XRPC server
lexicons = []
# TODO: vendor in lexicons
for filename in (Path(__file__).parent.parent / 'atproto/lexicons/com/atproto').glob('**/*.json'):
    with open(filename) as f:
        lexicons.append(json.load(f))

xrpc_server = Server(lexicons, validate=False)


# repo
storage = MemoryStorage()

def init(key):
    global repo
    repo = Repo.create(storage, 'did:web:user.com', key)


@xrpc_server.method('com.atproto.sync.getCheckout')
def get_checkout(input, did=None, commit=None):
    """Gets a repo's state, optionally at a specific commit."""
    if not commit:
        commit = repo.cid

    blocks, missing = storage.read_blocks([commit])
    if commit not in blocks:
        raise ValueError(f'{commit} not found in {did}')

    # TODO
    # mst = MST.load(storage=storage, cid=commit)
    return write_car(
        [commit],
        (Block(cid=cid, data=data) for cid, data in repo.mst.load_all()))


@xrpc_server.method('com.atproto.sync.getRepo')
def get_repo(input, did=None, earliest=None, latest=None):
    """
    """


@xrpc_server.method('com.atproto.sync.subscribeRepos')
def subscribe_repos(input, uri=None, cid=None, limit=None, before=None):
    """
    """
