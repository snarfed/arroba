"""com.atproto.sync.* XRPC methods.

TODO:
* getBlocks?
* getCommitPath?
* blobs
"""
import logging
from queue import Queue
from threading import Lock

from carbox import car
import dag_cbor

from . import server
from .storage import CommitData, SUBSCRIBE_REPOS_NSID
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
        commit = server.repo.head.cid

    if not server.storage.has(commit):
        raise ValueError(f'{commit} not found in {did}')

    # TODO
    # mst = MST.load(storage=storage, cid=commit)
    return car.write_car(
        [commit],
        (car.Block(cid=cid, data=data) for cid, data in server.repo.mst.load_all()))


@server.server.method('com.atproto.sync.getRepo')
def get_repo(input, did=None, earliest=None, latest=None):
    """
    """
    validate(did=did)

    return car.write_car(
        [server.repo.head.cid],
        (car.Block(cid=cid, data=data) for cid, data in server.repo.mst.load_all()))


@server.server.method('com.atproto.sync.listRepos')
def list_repos(input, limit=None, cursor=None):
    """
    """
    return [{
        'did': server.repo.did,
        'head': server.repo.head.cid.encode('base32'),
    }]


def enqueue_commit(commit_data):
    """
    Args:
      did: str
      commit_data: :class:`CommitData`
    """
    logger.debug(f'New commit {commit_data.commit.cid}')
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

    Args:
      cursor: integer, try to serve commits from this sequence number forward

    Returns:
      (dict header, dict payload)
    """
    def header_payload(commit_data):
        commit = commit_data.commit.decoded
        car_blocks = [car.Block(cid=block.cid, data=block.encoded,
                                decoded=block.decoded)
                      for block in commit_data.blocks.values()]
        return ({  # header
          'op': 1,
          't': '#commit',
        }, {  # payload
            'repo': commit['did'],
            'ops': [{
                'action': op.action.name.lower(),
                'path': op.path,
                'cid': op.cid,
            } for op in (commit_data.commit.ops or [])],
            'commit': commit_data.commit.cid,
            'blocks': car.write_car([commit_data.commit.cid], car_blocks),
            'time': util.now().isoformat(),
            'seq': commit_data.commit.seq,
            # omit prev for now to help BGS skip bad commits that it didn't ingest
            # TODO: this should go away with repo v3?
            'prev': None, #commit['prev'],
            'rebase': False,
            'tooBig': False,
            'blobs': [],
        })

    if cursor is not None:
        assert cursor >= 0

    queue = Queue()
    subscribers.add(queue)

    # fetch existing blocks starting at seq, collect into commits
    if cursor is not None:
        logger.info(f'subscribeRepos: fetching existing commits from seq {cursor}')
        last_seq = server.repo.storage.last_seq(SUBSCRIBE_REPOS_NSID)
        if cursor > last_seq:
            yield ({
                'op': -1,
            }, {
                'error': 'FutureCursor',
                'message': f'Cursor {cursor} is past current sequence number {last_seq}',
            })
            return

        seq = commit_block = blocks = None
        for block in server.repo.storage.read_from_seq(cursor):
            assert block.seq
            if block.seq != seq:  # switching to a new commit's blocks
                if commit_block:
                    assert blocks
                    commit_data = CommitData(blocks=blocks, commit=commit_block,
                                             prev=commit_block.decoded['prev'])
                    yield header_payload(commit_data)
                else:
                    assert blocks is None  # only the first commit
                seq = block.seq
                blocks = {}  # maps CID to Block
                commit_block = None

            blocks[block.cid] = block
            if block.decoded.keys() == set(['version', 'did', 'prev', 'data', 'sig']):
                commit_block = block

        # final commit
        assert blocks and commit_block
        commit_data = CommitData(blocks=blocks, commit=commit_block,
                                 prev=commit_block.decoded['prev'])
        yield header_payload(commit_data)

    # serve new commits as they happen
    logger.info(f'subscribeRepos: serving new commits')
    while True:
        commit_data = queue.get()
        yield header_payload(commit_data)

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
        'root': server.repo.head.cid.encode('base32'),
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

    block = car.Block(decoded=record)
    return car.write_car([block.cid], [block])

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
