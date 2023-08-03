"""com.atproto.repo.* XRPC methods.

TODO:
* auth
* cid in getRecord, listRecords output
* apply_writes
* rebase_repo
"""
import logging

from .repo import Action, Repo, Write
from . import server
from .util import at_uri, next_tid

logger = logging.getLogger(__name__)


def validate(input, **params):
    input.update(params)

    if input['repo'] != server.repo.did:
        raise ValueError(f'Unknown repo: {input["repo"]}')

    for field in 'swapCommit', 'swapRecord':
        if input.get(field):
            raise ValueError(f'{field} not supported yet')


@server.server.method('com.atproto.repo.createRecord')
def create_record(input):
    """
    """
    input['rkey'] = next_tid()
    return put_record(input)


@server.server.method('com.atproto.repo.getRecord')
def get_record(input, repo=None, collection=None, rkey=None, cid=None):
    """
    """
    # Largely duplicates xrpc_sync.get_record
    validate(input, repo=repo, collection=collection, rkey=rkey, cid=cid)
    if cid:
        raise ValueError(f'cid not supported yet')

    record = server.repo.get_record(collection, rkey)
    if record is None:
        raise ValueError(f'{collection} {rkey} not found')

    return {
        'uri': at_uri(repo, collection, rkey),
        'cid': 'TODO',
        'value': record,
    }


@server.server.method('com.atproto.repo.deleteRecord')
def delete_record(input):
    """
    """
    server.auth()
    validate(input)

    record = server.repo.get_record(input['collection'], input['rkey'])
    if record is None:
        return  # noop

    repo = server.repo = server.repo.apply_writes([Write(
        action=Action.DELETE,
        collection=input['collection'],
        rkey=input['rkey'],
    )], server.key)


@server.server.method('com.atproto.repo.listRecords')
def list_records(input, repo=None, collection=None, limit=None, cursor=None,
                 reverse=None,
                 # DEPRECATED
                 rkeyStart=None, rkeyEnd=None):
    """
    """
    validate(input, repo=repo, collection=collection, limit=limit, cursor=cursor)
    if rkeyStart or rkeyEnd:
        raise ValueError(f'rkeyStart/rkeyEnd not supported')

    records = [{
        'uri': at_uri(repo, collection, rkey),
        'cid': 'TODO',
        'value': record,
    } for rkey, record in server.repo.get_contents()[collection].items()]
    if reverse:
        records.reverse()

    return {'records': records}


@server.server.method('com.atproto.repo.putRecord')
def put_record(input):
    """
    """
    server.auth()
    validate(input)

    existing = server.repo.get_record(input['collection'], input['rkey'])

    repo = server.repo = server.repo.apply_writes([Write(
        action=Action.CREATE if existing is None else Action.UPDATE,
        collection=input['collection'],
        rkey=input['rkey'],
        record=input['record'],
    )], server.key)

    return {
        'uri': at_uri(repo.did, input['collection'], input['rkey']),
        'cid': repo.commit['data'],
    }


@server.server.method('com.atproto.repo.describeRepo')
def describe_repo(input, repo=None):
    """
    """
    handle = server.repo.did.removeprefix('did:web:')
    if repo not in (server.repo.did, handle):
        raise ValueError(f'Unknown DID or handle: {repo}')

    return {
        'did': server.repo.did,
        'handle': handle,
        'didDoc': {'TODO': 'TODO'},
        # TODO
        'collections': [
            'app.bsky.actor.profile',
            'app.bsky.feed.posts',
            'app.bsky.feed.likes',
        ],
        'handleIsCorrect': True,
    }


@server.server.method('com.atproto.repo.rebaseRepo')
def rebase_repo(input):
    """
    """
    server.auth()
    validate(input)


@server.server.method('com.atproto.repo.applyWrites')
def apply_writes(input):
    """
    """
    server.auth()
    validate(input)


@server.server.method('com.atproto.repo.uploadBlob')
def upload_blob(input):
    """
    """
    # input: binary
    server.auth()
    validate(input)
