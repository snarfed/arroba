"""com.atproto.repo.* XRPC methods."""
import logging

from .repo import Action, Repo, Write
from . import server
from .util import at_uri, next_tid

logger = logging.getLogger(__name__)


def auth():
    pass


def validate(input, **params):
    input.update(params)

    if input['repo'] != server.repo.did:
        raise ValueError(f'Unknown repo: {input["repo"]}')

    if input.get('swapCommit'):
        raise ValueError('swapCommit not supported yet')


@server.server.method('com.atproto.repo.createRecord')
def create_record(input):
    """
    """
    auth()
    validate(input)

    rkey = input.get('rkey') or next_tid()
    repo = server.repo = server.repo.apply_writes([Write(
        action=Action.CREATE,
        collection=input['collection'],
        rkey=rkey,
        record=input['record'],
    )], server.key)

    return {
        'uri': at_uri(did=server.repo.did, collection=input['collection'], rkey=rkey),
        'cid': repo.commit['data'],
    }


@server.server.method('com.atproto.repo.getRecord')
def get_record(input, repo=None, nsid=None, cid=None):
    """
    """


@server.server.method('com.atproto.repo.deleteRecord')
def delete_record(input):
    """
    """


@server.server.method('com.atproto.repo.listRecords')
def list_records(input, repo=None, collection=None, limit=None, cursor=None,
                 reverse=None,
                 # DEPRECATED
                 rkeyStart=None, rkeyEnd=None):
    """
    """


@server.server.method('com.atproto.repo.putRecord')
def put_record(input):
    """
    """


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


@server.server.method('com.atproto.repo.applyWrites')
def apply_writes(input):
    """
    """


@server.server.method('com.atproto.repo.uploadBlob')
def upload_blob(input):
    """
    """
    # input: binary
