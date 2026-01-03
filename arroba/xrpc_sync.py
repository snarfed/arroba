"""``com.atproto.sync.*`` XRPC methods."""
from datetime import timedelta, timezone
import itertools
import logging
import os

from carbox import car
import dag_cbor
from lexrpc.base import XrpcError
from lexrpc.server import Redirect
from multiformats import CID
from multiformats.multibase import MultibaseKeyError, MultibaseValueError
import requests
from werkzeug.exceptions import TooManyRequests

from .datastore_storage import AtpBlock, AtpRemoteBlob, AtpRepo, DatastoreStorage
from . import firehose
from .mst import MST
from . import server
from .storage import Action, Block, CommitData, CommitOp, SUBSCRIBE_REPOS_NSID
from . import util
from . import xrpc_repo

logger = logging.getLogger(__name__)

GET_BLOB_CACHE_CONTROL = {'Cache-Control': 'public, max-age=86400'}  # 1 day


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

    repos = []
    for repo in server.storage.load_repos(limit=limit, after=cursor):
        repo_obj = {
            'did': repo.did,
            'head': repo.head.cid.encode('base32'),
            'rev': util.int_to_tid(repo.head.seq, clock_id=0),
            'active': repo.status is None,
        }
        if repo.status:
            repo_obj['status'] = STATUSES.get(repo.status) or repo.status
        repos.append(repo_obj)

    ret = {'repos': repos}
    if len(repos) == limit:
        ret['cursor'] = repos[-1]['did']

    return ret


@server.server.method('com.atproto.sync.subscribeRepos')
def subscribe_repos(cursor=None):
    """Firehose event stream XRPC (ie ``type: subscription``) for all new commits.

    Event stream details: https://atproto.com/specs/event-stream#framing

    This function serves forever, which ties up a runtime context, so it's not
    automatically registered with the XRPC server. Instead, clients should
    choose how to register and serve it themselves, eg asyncio vs threads vs
    WSGI workers.

    See :func:`firehose.send_events` for an example thread-based callback to
    register with :class:`repo.Repo` to deliver all new commits to subscribers.
    Here's how to register that callback and this XRPC method in a threaded
    context:

        server.repo.callback = lambda commit_data: firehose.send_events()
        server.server.register('com.atproto.sync.subscribeRepos', xrpc_sync.subscribe_repos)

    Args:
      cursor (int): try to serve commits from this sequence number forward

    Returns:
      (dict, dict) tuple: (header, payload)
    """
    cur_seq = server.storage.sequences.last(SUBSCRIBE_REPOS_NSID)
    assert cur_seq is not None

    if cursor is not None:
        # validate cursor
        if cursor > cur_seq:
            msg = f'Cursor {cursor} is past our current sequence number {cur_seq}'
            logger.info(msg)
            yield ({'op': -1}, {'error': 'FutureCursor', 'message': msg})
            return

        # Check if cursor is outside of our rollback window
        rollback_start = max(cur_seq - firehose.ROLLBACK_WINDOW, 0)
        if cursor < rollback_start:
            logger.info(f'Cursor {cursor} is before our rollback window; starting at {rollback_start}')
            yield ({'op': 1, 't': '#info'}, {'name': 'OutdatedCursor'})
            cursor = rollback_start

    yield from firehose.subscribe(cursor)


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

    return car.write_car([repo.head.cid], car_blocks)


@server.server.method('com.atproto.sync.getHead')
def get_head(input, did=None):
    """Handler for ``com.atproto.sync.getHead`` XRPC method.

    Deprecated! Use ``getLatestCommit`` instead.
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
def get_record(input, did=None, collection=None, rkey=None):
    """Handler for ``com.atproto.sync.getRecord`` XRPC method."""
    repo = server.load_repo(did)
    record = repo.get_record(collection, rkey)
    if record is None:
        raise ValueError(f'{collection} {rkey} not found')

    block = car.Block(decoded=record)

    # include covering proof for a create of this record
    create = Block(decoded=record,
                   ops=[CommitOp(Action.CREATE, collection, block.cid)])
    proofs = repo.mst.add_covering_proofs(CommitData(commit=create, blocks={}))
    proof_blocks = [car.Block(data=val.encoded) for val in proofs.values()]

    return car.write_car([block.cid], [block] + proof_blocks)



@server.server.method('com.atproto.sync.getBlob')
def get_blob(input, did=None, cid=None):
    r"""Handler for ``com.atproto.sync.getBlob`` XRPC method.

    Right now only supports redirecting to "remote" blobs based on stored
    :class:`AtpRemoteBlob`\s.
    """
    for blob in AtpRemoteBlob.query(AtpRemoteBlob.cid == cid
                                    ).order(-AtpRemoteBlob.updated):
        if blob.status:
            continue

        try:
            blob.maybe_fetch(get_fn=requests.get)
        except requests.RequestException:
            continue

        if not blob.status:
            raise Redirect(to=blob.url or blob.key.id(), status=301,
                           headers=GET_BLOB_CACHE_CONTROL)

    err = ValueError(f'No blob found for CID {cid}')
    err.headers = GET_BLOB_CACHE_CONTROL
    raise err


@server.server.method('com.atproto.sync.listBlobs')
def list_blobs(input, did=None, since=None, limit=500, cursor=None):
    """Handler for ``com.atproto.sync.listBlobs`` XRPC method."""
    if since:
        raise ValueError('since parameter is not implemented')

    server.load_repo(did)  # raises if the repo doesn't exist or is deactivated

    query = AtpRemoteBlob.query(AtpRemoteBlob.repos == AtpRepo(id=did).key)
    if cursor:
        query = query.filter(AtpRemoteBlob.key > AtpRemoteBlob(id=cursor).key)

    blobs = query.fetch(limit=limit)

    ret = {'cids': [blob.cid for blob in blobs if blob.cid]}
    if len(blobs) == limit:
        ret['cursor'] = blobs[-1].key.id()

    return ret
