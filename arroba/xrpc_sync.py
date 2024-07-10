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
from lexrpc.base import XrpcError
from lexrpc.server import Redirect
from multiformats import CID
from multiformats.multibase import MultibaseKeyError, MultibaseValueError

from .datastore_storage import AtpBlock, AtpRemoteBlob, AtpRepo, DatastoreStorage
from . import server
from .storage import CommitData, SUBSCRIBE_REPOS_NSID
from . import util
from . import xrpc_repo

logger = logging.getLogger(__name__)

# used by subscribe_repos and send_events
NEW_EVENTS_TIMEOUT = timedelta(seconds=60)
new_events = Condition()

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
    """Handler for ``com.atproto.sync.getRepo`` XRPC method."""

    # HACK, TODO: remove
    # https://github.com/snarfed/bridgy-fed/issues/1151
    if did in (
            # 'did:plc:2ixmtcwjcnp4dh5drqxegvac',
            'did:plc:expkm6j5nfdzwhvrzhjjm5fm',
            'did:plc:lxf6nbzgcphkzhbjzdhz24wa',
            'did:plc:rcalkk4q6f6b7vrr7ib6a3om',
            'did:plc:s73ruhnbvdumlqsrx7qxkpfp',
            'did:plc:tdcbyc2ccsidtqtpue723zl5',
            'did:plc:wsnriqelojaz3bzrlhy6jols',
    ):
        import json
        from werkzeug.exceptions import TooManyRequests
        from werkzeug.wrappers import Response

        resp = Response(content_type='application/json', response=json.dumps({
            'error': 'Other',
            'message': "This repo is big! We're still working on properly implementing `since` to handle it."
        }))
        raise TooManyRequests(response=resp)

    repo = server.load_repo(did)

    start = util.tid_to_int(since) if since else 0

    blocks_and_head = itertools.chain(
        [car.Block(repo.head.cid, repo.head.encoded)],
        (car.Block(cid, data) for cid, data in repo.mst.load_all(start=start)))

    return car.write_car([repo.head.cid], blocks_and_head)


@server.server.method('com.atproto.sync.getRepoStatus')
def get_repo_status(input, did=None):
    """Handler for ``com.atproto.sync.getRepoStatus`` XRPC method."""
    try:
        repo = server.load_repo(did)
    except XrpcError as e:
        if e.name == 'RepoDeactivated':
            return {
                'did': did,
                'active': False,
                'status': 'deactivated',
            }
        raise

    return {
        'did': did,
        'active': True,
    }


@server.server.method('com.atproto.sync.listRepos')
def list_repos(input, limit=500, cursor=None):
    """Handler for ``com.atproto.sync.listRepos`` XRPC method."""
    STATUSES = {'tombstoned': 'deactivated'}

    repos = [{
        'did': repo.did,
        'head': repo.head.cid.encode('base32'),
        'rev': repo.head.seq,
        'active': repo.status is None,
        'status': STATUSES.get(repo.status),
    } for repo in server.storage.load_repos(limit=limit, after=cursor)]

    ret = {'repos': repos}
    if len(repos) == limit:
        ret['cursor'] = repos[-1]['did']

    return ret


def send_events():
    """Triggers ``subscribeRepos`` to deliver new commits from storage to subscribers.
    """
    logger.debug(f'Triggering subscribeRepos to look for new commits')
    with new_events:
        new_events.notify_all()


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
    last_seq = server.storage.last_seq(SUBSCRIBE_REPOS_NSID)

    def handle(event):
        nonlocal last_seq

        if isinstance(event, dict):  # non-commit event
            last_seq = event['seq']
            type = event.pop('$type')
            type_fragment = type.removeprefix('com.atproto.sync.subscribeRepos')
            assert type_fragment != type, type
            return ({'op': 1, 't': type_fragment}, event)

        assert isinstance(event, CommitData), \
            f'unexpected event type {event.__class__} {event}'

        last_seq = event.commit.seq
        commit = event.commit.decoded
        car_blocks = [car.Block(cid=block.cid, data=block.encoded,
                                decoded=block.decoded)
                      for block in event.blocks.values()]
        return ({  # header
          'op': 1,
          't': '#commit',
        }, {  # payload
            'repo': commit['did'],
            'ops': [{
                'action': op.action.name.lower(),
                'path': op.path,
                'cid': op.cid,
            } for op in (event.commit.ops or [])],
            'commit': event.commit.cid,
            'blocks': car.write_car([event.commit.cid], car_blocks),
            'time': event.commit.time.replace(tzinfo=timezone.utc).isoformat(),
            'seq': event.commit.seq,
            'rev': util.int_to_tid(event.commit.seq, clock_id=0),
            'since': None,  # TODO: load event.commit['prev']'s CID
            'rebase': False,
            'tooBig': False,
            'blobs': [],
        })

    if cursor is not None:
        assert cursor >= 0

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
            yield handle(event)

    # serve new events as they happen
    logger.info(f'serving new events')
    while True:
        with new_events:
            new_events.wait(NEW_EVENTS_TIMEOUT.total_seconds())

        for commit_data in server.storage.read_events_by_seq(start=last_seq + 1):
            yield handle(commit_data)


@server.server.method('com.atproto.sync.getBlocks')
def get_blocks(input, did=None, cids=()):
    """Handler for ``com.atproto.sync.getBlocks`` XRPC method."""
    repo = server.load_repo(did)

    try:
        cids = [CID.decode(cid) for cid in cids]
    except (MultibaseKeyError, MultibaseValueError):
        raise XrpcError('Invalid CID', name='BlockNotFound')

    car_blocks = []
    blocks = server.storage.read_many(cids)

    for cid in cids:
        block = blocks[cid]
        if block is None:
            raise XrpcError(f'No block found for CID {cid.encode("base32")}',
                            name='BlockNotFound')
        car_blocks.append(car.Block(cid=block.cid, data=block.encoded))

    return car.write_car([server.storage.head], car_blocks)


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
    """Handler for ``com.atproto.sync.getBlob`` XRPC method.

    Right now only supports redirecting to "remote" blobs based on stored
    :class:`AtpRemoteBlob`\s.
    """
    blob = AtpRemoteBlob.query(AtpRemoteBlob.cid == cid).get()
    if blob:
        raise Redirect(to=blob.key.id(), status=301)

    raise ValueError(f'No blob found for CID {cid}')


# @server.server.method('com.atproto.sync.listBlobs')
# def list_blobs(input, did=None, since=None, limit=500):
#     """Handler for ``com.atproto.sync.listBlobs`` XRPC method.
#
#     TODO. The difficulty with this one is that AtpRemoteBlob is
#     repo-independent. Hrm.
#     """
#     # output: {'cids': [CID, ...]}
