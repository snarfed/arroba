"""com.atproto.repo.* XRPC methods."""
import logging

from . import server

logger = logging.getLogger(__name__)


@server.server.method('com.atproto.repo.createRecord')
def create_record(input):
    """
    """


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
