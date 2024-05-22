"""``com.atproto.sync.*`` XRPC methods.

TODO:

* getBlocks?
* blobs
"""
from datetime import timedelta, timezone
import itertools
import logging
import os
from threading import Condition

from carbox import car
import dag_cbor
from lexrpc.server import Redirect

from . import server
from .datastore_storage import AtpRemoteBlob
from .storage import CommitData, SUBSCRIBE_REPOS_NSID
from . import util
from . import xrpc_repo

logger = logging.getLogger(__name__)

# used by subscribe_repos and send_events
NEW_COMMITS_TIMEOUT = timedelta(seconds=60)
new_commits = Condition()

ROLLBACK_WINDOW = None
if 'ROLLBACK_WINDOW' in os.environ:
    ROLLBACK_WINDOW = int(os.environ['ROLLBACK_WINDOW'])


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
    repo = server.load_repo(did)
    return car.write_car(
        [repo.head.cid],
        (car.Block(cid=cid, data=data) for cid, data in itertools.chain(
            [(repo.head.cid, repo.head.encoded)], repo.mst.load_all())))


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


def send_events():
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

    See :func:`send_events` for an example thread-based callback to
    register with :class:`Repo` to deliver all new commits to subscribers.
    Here's how to register that callback and this XRPC method in a threaded
    context:

        server.repo.callback = lambda commit_data: xrpc_sync.send_events()
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
            'time': commit_data.commit.time.replace(tzinfo=timezone.utc).isoformat(),
            'seq': commit_data.commit.seq,
            'rev': util.int_to_tid(commit_data.commit.seq, clock_id=0),
            'since': None,  # TODO: load commit_data.commit['prev']'s CID
            'rebase': False,
            'tooBig': False,
            'blobs': [],
        })

    if cursor is not None:
        assert cursor >= 0

    last_seq = server.storage.last_seq(SUBSCRIBE_REPOS_NSID)
    if cursor is not None:
        # validate cursor
        if cursor > last_seq:
            msg = f'Cursor {cursor} is past our current sequence number {last_seq}'
            logger.warning(msg)
            yield ({'op': -1}, {'error': 'FutureCursor', 'message': msg})
            return

        if ROLLBACK_WINDOW is not None:
            rollback_start = max(last_seq - ROLLBACK_WINDOW - 1, 0)
            if cursor < rollback_start:
                logger.warning(f'Cursor {cursor} is before our rollback window; starting at {rollback_start}')
                yield ({'op': 1, 't': '#info'}, {'name': 'OutdatedCursor'})
                cursor = rollback_start

        logger.info(f'fetching existing events from seq {cursor}')
        for event in server.storage.read_events_by_seq(start=cursor):
            if isinstance(event, CommitData):
                yield header_payload(event)
                last_seq = event.commit.seq
            elif isinstance(event, dict):
                type = event.pop('$type')
                type_fragment = type.removeprefix('com.atproto.sync.subscribeRepos')
                assert type_fragment != type, type
                yield {'op': 1, 't': type_fragment}, event
                last_seq = event['seq']
            else:
                raise RuntimeError(f'unexpected event type {event.__class__} {event}')

    # serve new events as they happen
    logger.info(f'serving new events')
    while True:
        with new_commits:
            new_commits.wait(NEW_COMMITS_TIMEOUT.total_seconds())

        for commit_data in server.storage.read_events_by_seq(start=last_seq + 1):
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


@server.server.method('com.atproto.sync.getBlob')
def get_blob(input, did=None, cid=None):
    r"""Handler for ``com.atproto.sync.getBlob`` XRPC method.

    Right now only supports redirecting to "remote" blobs based on stored
    :class:`AtpRemoteBlob`\s.
    """
    blob = AtpRemoteBlob.query(AtpRemoteBlob.cid == cid).get()
    if blob:
        raise Redirect(to=blob.key.id())

    raise ValueError(f'No blob found for CID {cid}')


# @server.server.method('com.atproto.sync.listBlobs')
# def list_blobs(input, did=None, earliest=None, latest=None):
#     """Handler for ``com.atproto.sync.listBlobs`` XRPC method."""
#     # output: {'cids': [CID, ...]}
