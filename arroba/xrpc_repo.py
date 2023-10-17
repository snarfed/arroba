"""``com.atproto.repo.*`` XRPC methods."""
import logging

from .repo import Repo, Write
from .storage import Action
from . import server
from .util import at_uri, dag_cbor_cid, next_tid

logger = logging.getLogger(__name__)


def validate(input, **params):
    input.update(params)

    for field in 'swapCommit', 'swapRecord':
        if input.get(field):
            raise ValueError(f'{field} not supported yet')

    if not input.get('repo'):
        raise ValueError('Missing repo param')

    server.auth()


@server.server.method('com.atproto.repo.createRecord')
def create_record(input):
    """Handler for ``com.atproto.repo.createRecord`` XRPC method."""
    validate(input)
    repo = server.load_repo(input['repo'])
    input.setdefault('rkey', next_tid())
    return put_record(input)


@server.server.method('com.atproto.repo.getRecord')
def get_record(input, repo=None, collection=None, rkey=None, cid=None):
    """Handler for `com.atproto.repo.getRecord` XRPC method."""
    # Largely duplicates xrpc_sync.get_record
    validate(input, repo=repo, collection=collection, rkey=rkey, cid=cid)
    if cid:
        raise ValueError(f'cid not supported yet')

    repo = server.load_repo(input['repo'])

    record = repo.get_record(collection, rkey)
    if record is None:
        raise ValueError(f'{collection} {rkey} not found')

    return {
        'uri': at_uri(repo.did, collection, rkey),
        'cid': dag_cbor_cid(record).encode('base32'),
        'value': record,
    }


@server.server.method('com.atproto.repo.deleteRecord')
def delete_record(input):
    """Handler for ``com.atproto.repo.deleteRecord`` XRPC method."""
    validate(input)
    repo = server.load_repo(input['repo'])

    record = repo.get_record(input['collection'], input['rkey'])
    if record is None:
        return  # noop

    repo.apply_writes([Write(
        action=Action.DELETE,
        collection=input['collection'],
        rkey=input['rkey'],
    )])


@server.server.method('com.atproto.repo.listRecords')
def list_records(input, repo=None, collection=None, limit=None, cursor=None,
                 reverse=None,
                 # DEPRECATED
                 rkeyStart=None, rkeyEnd=None):
    """Handler for `com.atproto.repo.listRecords` XRPC method."""
    validate(input, repo=repo, collection=collection, limit=limit, cursor=cursor)
    if rkeyStart or rkeyEnd:
        raise ValueError(f'rkeyStart/rkeyEnd not supported')
    repo = server.load_repo(input['repo'])

    records = [{
        'uri': at_uri(repo.did, collection, rkey),
        'cid': dag_cbor_cid(record).encode('base32'),
        'value': record,
    } for rkey, record in repo.get_contents()[collection].items()]
    if reverse:
        records.reverse()

    return {'records': records}


@server.server.method('com.atproto.repo.putRecord')
def put_record(input):
    """Handler for ``com.atproto.repo.putRecord`` XRPC method."""
    validate(input)
    repo = server.load_repo(input['repo'])

    existing = repo.get_record(input['collection'], input['rkey'])

    repo.apply_writes([Write(
        action=Action.CREATE if existing is None else Action.UPDATE,
        collection=input['collection'],
        rkey=input['rkey'],
        record=input['record'],
    )])

    return {
        'uri': at_uri(repo.did, input['collection'], input['rkey']),
        'cid': dag_cbor_cid(input['record']).encode('base32'),
    }


@server.server.method('com.atproto.repo.describeRepo')
def describe_repo(input, repo=None):
    """Handler for ``com.atproto.repo.describeRepo`` XRPC method."""
    validate(input, repo=repo)
    repo = server.load_repo(input['repo'])

    return {
        'did': repo.did,
        'handle': repo.handle,
        'didDoc': {'TODO': 'TODO'},
        # TODO
        'collections': [
            'app.bsky.actor.profile',
            'app.bsky.feed.posts',
            'app.bsky.feed.likes',
        ],
        'handleIsCorrect': True,
    }


@server.server.method('com.atproto.repo.applyWrites')
def apply_writes(input):
    """Handler for ``com.atproto.repo.applyWrites`` XRPC method."""
    validate(input)
    return 'Not implemented yet', 501


@server.server.method('com.atproto.repo.uploadBlob')
def upload_blob(input):
    """Handler for ``com.atproto.repo.uploadBlob`` XRPC method."""
    # input: binary
    validate({})
    return 'Not implemented yet', 501
