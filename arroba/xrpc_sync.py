"""``com.atproto.sync.*`` XRPC methods.

TODO:

* getBlocks?
* blobs
"""
from datetime import timedelta
import logging
from queue import Queue
from threading import Condition

from carbox import car
import dag_cbor

from . import server
from .storage import CommitData, SUBSCRIBE_REPOS_NSID
from . import util
from . import xrpc_repo

logger = logging.getLogger(__name__)

# used by subscribe_repos and send_new_commits
NEW_COMMITS_TIMEOUT = timedelta(seconds=60)
new_commits = Condition()


@server.server.method('com.atproto.sync.getCheckout')
def get_checkout(input, did=None):
    """Handler for ``com.atproto.sync.getCheckout`` XRPC method.

    Deprecated! Use ``getRepo`` instead.

    Gets a checkout, either head or a specific commit.
    """
    return get_repo(input, did=did)


@server.server.method('com.atproto.sync.getRepo')
def get_repo(input, did=None, since=None):
    """Handler for ``com.atproto.sync.getRepo`` XRPC method.

    TODO: implement ``since``
    """
    if since:
        raise ValueError('since is not implemented yet')

    repo = server.load_repo(did)
    return car.write_car(
        [repo.head.cid],
        (car.Block(cid=cid, data=data) for cid, data in repo.mst.load_all()))


# @server.server.method('com.atproto.sync.listRepos')
# def list_repos(input, limit=None, cursor=None):
#     """Handler for ``com.atproto.sync.listRepos`` XRPC method.

#     TODO: implement. needs new Storage.list_repos method or similar
#     TODO: implement cursor
#     """
#     if cursor:
#         raise ValueError('cursor is not implemented yet')

#     return [{
#         'did': repo.did,
#         'head': repo.head.cid.encode('base32'),
#     }]


def send_new_commits():
    """Triggers ``subscribeRepos`` to deliver new commits from storage to subscribers.
    """
    logger.debug(f'Triggering subscribeRepos to look for new commits')
    with new_commits:
        new_commits.notify_all()


@server.server.method('com.atproto.sync.subscribeRepos')
def subscribe_repos(cursor=None):
    """Firehose event stream XRPC (ie ``type: subscription``) for all new commits.

    Event stream details: https://atproto.com/specs/event-stream#framing

    This function serves forever, which ties up a runtime context, so it's not
    automatically registered with the XRPC server. Instead, clients should
    choose how to register and serve it themselves, eg asyncio vs threads vs
    WSGI workers.

    See :func:`send_new_commits` for an example thread-based callback to
    register with :class:`Repo` to deliver all new commits to subscribers.
    Here's how to register that callback and this XRPC method in a threaded
    context::

        server.repo.callback = lambda commit_data: xrpc_sync.send_new_commits()
        server.server.register('com.atproto.sync.subscribeRepos',
                               xrpc_sync.subscribe_repos)

    Args:
      cursor (int): try to serve commits from this sequence number forward

    Returns:
      (dict, dict) tuple: (header, payload)
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
            'rev': util.int_to_tid(commit_data.commit.seq),
            'since': None,  # TODO: load commit_data.commit['prev']'s CID
            'rebase': False,
            'tooBig': False,
            'blobs': [],
        })

    if cursor is not None:
        assert cursor >= 0

    # fetch existing commits
    last_seq = server.storage.last_seq(SUBSCRIBE_REPOS_NSID)
    if cursor is not None:
        logger.info(f'subscribeRepos: fetching existing commits from seq {cursor}')
        if cursor > last_seq:
            yield ({
                'op': -1,
            }, {
                'error': 'FutureCursor',
                'message': f'Cursor {cursor} is past current sequence number {last_seq}',
            })
            return

        for commit_data in server.storage.read_commits_by_seq(start=cursor):
            yield header_payload(commit_data)
            last_seq = commit_data.commit.seq

    # serve new commits as they happen
    logger.info(f'subscribeRepos: serving new commits')
    while True:
        with new_commits:
            new_commits.wait(NEW_COMMITS_TIMEOUT.total_seconds())

        for commit_data in server.storage.read_commits_by_seq(start=last_seq + 1):
            yield header_payload(commit_data)
            last_seq = commit_data.commit.seq


# @server.server.method('com.atproto.sync.getBlocks')
# def get_blocks(input, did=None, cids=None):
#     """Handler for ``com.atproto.sync.getBlocks`` XRPC method."""
#     # TODO
#     return b''


@server.server.method('com.atproto.sync.getHead')
def get_head(input, did=None):
    """Handler for ``com.atproto.sync.getHead`` XRPC method.

    Deprecated! Use getLatestCommit instead.
    """
    repo = server.load_repo(did)
    return {
        'root': repo.head.cid.encode('base32'),
    }


@server.server.method('com.atproto.sync.getLatestCommit')
def get_latest_commit(input, did=None):
    """Handler for ``com.atproto.sync.getLatestCommit`` XRPC method."""
    repo = server.load_repo(did)
    return {
        'cid': repo.head.cid.encode('base32'),
        'rev': repo.head.decoded['rev'],
    }


@server.server.method('com.atproto.sync.getRecord')
def get_record(input, did=None, collection=None, rkey=None, commit=None):
    """Handler for ``com.atproto.sync.getRecord`` XRPC method.

    TODO:

    * implement commit
    * merge with xrpc_repo.get_record?
    """
    if commit:
        raise ValueError('commit not supported yet')

    repo = server.load_repo(did)
    record = repo.get_record(collection, rkey)
    if record is None:
        raise ValueError(f'{collection} {rkey} not found')

    block = car.Block(decoded=record)
    return car.write_car([block.cid], [block])

# @server.server.method('com.atproto.sync.notifyOfUpdate')
# def notify_of_update(input, did=None, earliest=None, latest=None):
#     """Handler for ``com.atproto.sync.notifyOfUpdate`` XRPC method."""
#     # input: {'hostname': ...}
#     # no output


# @server.server.method('com.atproto.sync.requestCrawl')
# def request_crawl(input):
#     """Handler for ``com.atproto.sync.requestCrawl`` XRPC method."""
#     # input: {'hostname': ...}
#     # no output


# @server.server.method('com.atproto.sync.getBlob')
# def get_blob(input, did=None, cid=None):
#     """Handler for ``com.atproto.sync.getBlob`` XRPC method."""
#     # output: binary

# @server.server.method('com.atproto.sync.listBlobs')
# def list_blobs(input, did=None, earliest=None, latest=None):
#     """Handler for ``com.atproto.sync.listBlobs`` XRPC method."""
#     # output: {'cids': [CID, ...]}
