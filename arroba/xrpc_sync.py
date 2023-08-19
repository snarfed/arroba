"""com.atproto.sync.* XRPC methods.

TODO:
* getBlocks?
* getCommitPath?
* blobs
"""
import logging
from queue import Queue
from threading import Lock

from carbox.car import Block, write_car
import dag_cbor

from . import server
from . import util
from . import xrpc_repo

logger = logging.getLogger(__name__)

# used by subscribe_repos and enqueue_commit
subscribers = set()  # stores Queue, one per subscriber
# TODO: do we need this?
# _subscribers_lock = Lock()


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


def enqueue_commit(commit_data):
    """
    Args:
      did: str
      commit_data: :class:`CommitData`
    """
    logger.debug(f'New commit {commit_data.cid}')
    if subscribers:
        logger.debug(f'Enqueueing for {len(subscribers)} subscribers')

    for subscriber in subscribers:
        subscriber.put(commit_data)


@server.server.method('com.atproto.sync.subscribeRepos')
def subscribe_repos(cursor=None):
    """Firehose event stream XRPC (ie type: subscription) for all new commits.

    Event stream details: https://atproto.com/specs/event-stream#framing

    This function serves forever, which ties up a runtime context, so it's not
    automatically registered with the XRPC server. Instead, clients should
    choose how to register and serve it themselves, eg asyncio vs threads vs
    WSGI workers.

    See :func:`enqueue_commit` for an example thread-based callback to register
    with :class:`Repo` to deliver all new commits. Here's how to register that
    callback and this XRPC method in a threaded context:

      server.server.register('com.atproto.sync.subscribeRepos',
                             xrpc_sync.subscribe_repos)
      server.repo.set_callback(xrpc_sync.enqueue_commit)

    Returns:
      (dict header, dict payload)
    """
    assert not cursor, 'cursor not implemented yet'

    queue = Queue()
    subscribers.add(queue)

    while True:
        commit_data = queue.get()
        cid = commit_data.cid
        commit = dag_cbor.decode(commit_data.blocks[cid])
        car_blocks = [Block(cid=cid, data=data)
                      for cid, data in commit_data.blocks.items()]

        yield ({  # header
          'op': 1,
          't': '#commit',
        }, {  # payload
            'repo': commit['did'],
            'ops': [{
                'action': 'create',  # TODO: update, delete
                'path': 'TODO!',
                'cid': b.cid,
            } for b in car_blocks],
            'commit': cid,
            'blocks': write_car([cid], car_blocks),
            'time': util.now().isoformat(),
            'seq': commit_data.seq,
            # TODO
            'prev': None,
            'rebase': False,
            'tooBig': False,
            'blobs': [],
        })

    # TODO: this is never reached, so we currently slowly leak queues. fix that
    subscribers.remove(queue)


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

# @server.server.method('com.atproto.sync.notifyOfUpdate')
# def notify_of_update(input, did=None, earliest=None, latest=None):
#     """
#     """
#     # input: {'hostname': ...}
#     # no output


# @server.server.method('com.atproto.sync.requestCrawl')
# def request_crawl(input):
#     """
#     """
#     # input: {'hostname': ...}
#     # no output


# @server.server.method('com.atproto.sync.getBlob')
# def get_blob(input, did=None, cid=None):
#     """
#     """
#     # output: binary

# @server.server.method('com.atproto.sync.listBlobs')
# def list_blobs(input, did=None, earliest=None, latest=None):
#     """
#     """
#     # output: {'cids': [CID, ...]}
