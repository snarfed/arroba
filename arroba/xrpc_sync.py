"""com.atproto.sync.* XRPC methods."""
import logging

from carbox.car import Block, write_car

from . import server

logger = logging.getLogger(__name__)


@server.server.method('com.atproto.sync.getCheckout')
def get_checkout(input, did=None, commit=None):
    """Gets a checkout, either head or a specific commit."""
    if not commit:
        commit = server.repo.cid

    blocks, missing = server.storage.read_blocks([commit])
    if commit not in blocks:
        raise ValueError(f'{commit} not found in {did}')

    # TODO
    # mst = MST.load(storage=storage, cid=commit)
    return write_car(
        [commit],
        (Block(cid=cid, data=data) for cid, data in server.repo.mst.load_all()))


@server.server.method('com.atproto.sync.getRepo')
def get_repo(input, did=None, earliest=None, latest=None):
    """
    """
    blocks, missing = server.storage.read_blocks([server.repo.cid])
    return write_car(
        [server.repo.cid],
        (Block(cid=cid, data=data) for cid, data in server.repo.mst.load_all()))


@server.server.method('com.atproto.sync.listRepos')
def list_repos(input, limit=None, cursor=None):
    """
    """
    return [{
        'did': server.repo.did,
        'head': server.repo.cid.encode('base32'),
    }]


@server.server.method('com.atproto.sync.subscribeRepos')
def subscribe_repos(input, cursor=None):  # int, seq # ?
    """
    """
    # subscription


@server.server.method('com.atproto.sync.getBlocks')
def get_blocks(input, did=None, cids=None):
    """
    """
    # output: CAR


@server.server.method('com.atproto.sync.getCommitPath')
def get_commit_path(input, did=None, earliest=None, latest=None):
    """
    """
    # output: {'commits': [CID]}


@server.server.method('com.atproto.sync.getHead')
def get_head(input, did=None):
    """
    """
    # output: {'root': CID}


@server.server.method('com.atproto.sync.getRecord')
def get_record(input, did=None, collection=None, rkey=None, commit=None):
    """
    """
    # output: CAR


@server.server.method('com.atproto.sync.notifyOfUpdate')
def notify_of_update(input, did=None, earliest=None, latest=None):
    """
    """
    # input: {'hostname': ...}
    # no output


@server.server.method('com.atproto.sync.requestCrawl')
def request_crawl(input):
    """
    """
    # input: {'hostname': ...}
    # no output


@server.server.method('com.atproto.sync.getBlob')
def get_blob(input, did=None, cid=None):
    """
    """
    # output: binary

@server.server.method('com.atproto.sync.listBlobs')
def list_blobs(input, did=None, earliest=None, latest=None):
    """
    """
    # output: {'cids': [CID, ...]}
