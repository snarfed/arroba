"""com.atproto.sync.* XRPC methods.

TODO:
* getBlocks?
* getCommitPath?
"""
import logging

from carbox.car import Block, write_car
import dag_cbor

from . import server
from . import xrpc_repo
from .util import dag_cbor_cid

logger = logging.getLogger(__name__)


def validate(did=None, collection=None, rkey=None):
    if did != server.repo.did:
        raise ValueError(f'Unknown DID: {did}')


@server.server.method('com.atproto.sync.getCheckout')
def get_checkout(input, did=None, commit=None):
    """Gets a checkout, either head or a specific commit."""
    validate(did=did)

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
    validate(did=did)

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


# @server.server.method('com.atproto.sync.getBlocks')
# def get_blocks(input, did=None, cids=None):
#     """
#     """
#     # TODO
#     return b''


# @server.server.method('com.atproto.sync.getCommitPath')
# def get_commit_path(input, did=None, earliest=None, latest=None):
#     """
#     """
#     # TODO


@server.server.method('com.atproto.sync.getHead')
def get_head(input, did=None):
    """
    """
    validate(did=did)

    return {
        'root': server.repo.cid.encode('base32'),
    }


@server.server.method('com.atproto.sync.getRecord')
def get_record(input, did=None, collection=None, rkey=None, commit=None):
    """
    """
    # Largely duplicates xrpc_repo.get_record
    validate(did=did, collection=collection, rkey=rkey)

    if commit:
        raise ValueError('commit not supported yet')

    record = server.repo.get_record(collection, rkey)
    if record is None:
        raise ValueError(f'{collection} {rkey} not found')

    block = Block(decoded=record)
    return write_car([block.cid], [block])

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
