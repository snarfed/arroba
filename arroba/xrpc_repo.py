"""``com.atproto.repo.*`` XRPC methods."""
import itertools
import json
import logging
import os

from carbox import read_car
import dag_json
from multiformats import CID
from requests import RequestException
import webutil.util

from . import did
from .mst import MST
from .repo import Repo, Write
from . import server
from .storage import Action, Block
from . import server
from . import util
from .util import at_uri, dag_cbor_cid, new_key, next_tid, verify_sig

SUPPORTED_COLLECTIONS = frozenset(
    coll.strip() for coll in os.environ.get('SUPPORTED_COLLECTIONS', '').split(',')
    if coll.strip())

logger = logging.getLogger(__name__)


def validate(input, collection=None, **params):
    input.update(params)

    for field in 'swapCommit', 'swapRecord':
        if input.get(field):
            raise ValueError(f'{field} not supported yet')

    if not input.get('repo'):
        raise ValueError('Missing repo param')

    for coll in collection, input.get('collection'):
        if coll and SUPPORTED_COLLECTIONS and coll not in SUPPORTED_COLLECTIONS:
            raise ValueError(f'{coll} not supported')


@server.server.method('com.atproto.repo.createRecord')
def create_record(input):
    """Handler for ``com.atproto.repo.createRecord`` XRPC method."""
    validate(input, collection=input.get('collection')
                                or (input.get('record') or {}).get('$type'))
    server.auth()

    repo = server.load_repo(input['repo'])
    # TODO: check the lexicon's key field first
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
    if not (record := repo.get_record(collection, rkey)):
        raise ValueError(f'{collection} {rkey} not found')

    return json.loads(dag_json.encode({
        'uri': at_uri(repo.did, collection, rkey),
        'cid': dag_cbor_cid(record).encode('base32'),
        'value': record,
    }, dialect='atproto'))


@server.server.method('com.atproto.repo.deleteRecord')
def delete_record(input):
    """Handler for ``com.atproto.repo.deleteRecord`` XRPC method."""
    validate(input)
    server.auth()

    repo = server.load_repo(input['repo'])
    record = repo.get_record(input['collection'], input['rkey'])
    if record is None:
        return  # noop

    server.storage.commit(repo, [Write(
        action=Action.DELETE,
        collection=input['collection'],
        rkey=input['rkey'],
    )])


@server.server.method('com.atproto.repo.listRecords')
def list_records(input, repo=None, collection=None, limit=50, cursor=None,
                 reverse=None,
                 # DEPRECATED
                 rkeyStart=None, rkeyEnd=None):
    """Handler for `com.atproto.repo.listRecords` XRPC method.

    KNOWN ISSUE: cursor is interpreted as inclusive, so whenever a cursor is
    used, the response includes the last record returned in the previous
    response.
    """
    if SUPPORTED_COLLECTIONS and collection not in SUPPORTED_COLLECTIONS:
        return {'records': []}

    validate(input, repo=repo, collection=collection, limit=limit, cursor=cursor)

    if rkeyStart or rkeyEnd:
        raise ValueError(f'rkeyStart/rkeyEnd not supported')
    elif reverse:
        raise ValueError(f'reverse not supported yet')
    elif not collection:
        raise ValueError(f'collection is required')

    repo = server.load_repo(input['repo'])

    start = cursor or f'{collection}/'
    entries = list(itertools.islice(
        itertools.takewhile(lambda entry: entry.key.startswith(f'{collection}/'),
                            repo.mst.walk_leaves_from(key=start)),
        limit))
    blocks = server.storage.read_many([e.value for e in entries])
    records = [blocks[e.value].decoded for e in entries]


    records = [
        json.loads(dag_json.encode({
            'uri': at_uri(repo.did, *entry.key.split('/', 2)),  # collection, rkey
            'cid': dag_cbor_cid(record).encode('base32'),
            'value': record,
        }, dialect='atproto'))
        for entry, record in zip(entries, records)]

    ret = {'records': records}
    if len(entries) == limit:
        ret['cursor'] = entries[-1].key

    return ret


@server.server.method('com.atproto.repo.putRecord')
def put_record(input):
    """Handler for ``com.atproto.repo.putRecord`` XRPC method."""
    validate(input)
    server.auth()

    repo = server.load_repo(input['repo'])
    existing = repo.get_record(input['collection'], input['rkey'])

    server.storage.commit(repo, [Write(
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

    try:
        did_doc = did.resolve(repo.did, get_fn=webutil.util.session.get)
    except (ConnectionError, OSError, RequestException, TimeoutError) as e:
        raise ValueError(f"Couldn't resolve {repo.did}")

    # optimization since repo.get_contents() is slow
    collections = (sorted(SUPPORTED_COLLECTIONS) if SUPPORTED_COLLECTIONS
                   else list(repo.get_contents().keys()))

    return {
        'did': repo.did,
        'handle': repo.handle,
        'didDoc': did_doc,
        'collections': collections,
        'handleIsCorrect': True,
    }


@server.server.method('com.atproto.repo.importRepo')
def import_repo(input):
    """Handler for ``com.atproto.repo.importRepo`` XRPC method.

    Requires that a repo doesn't already exist for this DID.
    """
    server.auth()

    roots, car_blocks = read_car(input)
    if not roots:
        raise ValueError("CAR missing root CID")
    head_cid = roots[0]

    # read and prepare blocks
    blocks = []
    repo_did = None
    head = None
    for car_block in car_blocks:
        # note seq 0, since we won't emit these over the firehose
        block = Block(cid=car_block.cid, encoded=car_block.data, seq=0)
        blocks.append(block)

        if block.cid == head_cid:
            # this is the commit. note that its signature is generated by the
            # old PDS's signing key. that doesn't matter since we make a new
            # commit below when we create the repo.
            head = block
            repo_did = car_block.decoded['did']
            if server.storage.load_repo(repo_did):
                raise ValueError(f'repo already exists for DID {repo_did}')

            did_doc = did.resolve(repo_did, get_fn=webutil.util.session.get)
            signing_key = did.get_signing_key(did_doc)
            if not signing_key or not verify_sig(car_block.decoded, signing_key):
                raise ValueError(f"Couldn't verify signature on head commit {head_cid.encode('base32')}")

    if not head:
        raise ValueError("Couldn't find head commit block")
    elif not repo_did:
        raise ValueError("Head commit block missing DID")

    logger.info(f'importing repo for {repo_did}')

    for block in blocks:
        block.repo = repo_did
    server.storage.write_blocks(blocks)

    mst = MST.load(storage=server.storage, cid=head.decoded['data'])

    handle = did.get_handle(did_doc)
    repo = Repo(storage=server.storage, mst=mst, head=head, handle=handle,
                status='deactivated', signing_key=new_key(), rotation_key=new_key())
    server.storage.create_repo(repo)


@server.server.method('com.atproto.repo.applyWrites')
def apply_writes(input):
    """Handler for ``com.atproto.repo.applyWrites`` XRPC method."""
    validate(input)
    server.auth()
    return 'Not implemented', 501


@server.server.method('com.atproto.repo.uploadBlob')
def upload_blob(input):
    """Handler for ``com.atproto.repo.uploadBlob`` XRPC method."""
    # input: binary
    validate({})
    server.auth()
    return 'Not implemented', 501
