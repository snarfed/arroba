"""ATProto XRPC service proxying, including read-after-write.

* https://atproto.com/specs/xrpc#service-proxying
* https://atproto.com/guides/writing-data#read-after-write
"""
from datetime import timedelta
import json
import logging
import os
import threading
from urllib.parse import urljoin, urlparse
from wsgiref.util import is_hop_by_hop

from cachetools import cached, TTLCache
import dag_json
from flask import request
from lexrpc.base import ValidationError, XrpcError
from lexrpc.flask_server import RESPONSE_HEADERS
import requests
from requests.structures import CaseInsensitiveDict
import webutil.util
from webutil.util import HTTP_TIMEOUT, parse_iso8601, session

from . import did
from . import server
from .util import at_uri, dag_cbor_cid, service_jwt, USER_AGENT

logger = logging.getLogger(__name__)

CACHE_SIZE = 5000
CACHE_TTL = timedelta(hours=1)

# service auth JWTs are single use, so they don't need to live long
# https://atproto.com/specs/xrpc#inter-service-authentication-temporary-specification
JWT_EXPIRATION = timedelta(minutes=5)

# request headers to pass through to the upstream service. notably not
# Authorization, which we replace with our own service auth JWT, or
# Accept-Encoding, which we handle separately below.
# https://atproto.com/specs/xrpc#service-proxying#summary-of-http-headers
FORWARD_REQUEST_HEADERS = (
    'Accept',
    'Accept-Language',
    'ATProto-Accept-Labelers',
    'Content-Type',
)

CHUNK_SIZE = 64 * 1024

# caps how far back read-after-write looks for local writes, so that an appview
# that's far behind can't turn a request into a scan of the whole repo
READ_AFTER_WRITE_MAX_EVENTS = 100

BLUESKY_CDN = 'https://cdn.bsky.app'

# maps NSID to function that applies the user's local writes to its output.
# populated by @read_after_write_fn.
read_after_write_fns = {}


def read_after_write_fn(nsid):
    """Decorator that registers a read-after-write function for an XRPC method.

    Args:
      nsid (str)
    """
    def decorator(fn):
        read_after_write_fns[nsid] = fn
        return fn

    return decorator


@cached(TTLCache(maxsize=CACHE_SIZE, ttl=CACHE_TTL.total_seconds()),
        lock=threading.Lock())
def signing_key(repo_did):
    """Returns a repo's signing key, cached.

    Loading a repo hits storage, and decoding its PEM private key costs about as
    much as the signature we're loading it for, so this is worth caching even
    though it means a rotated key can take up to :const:`CACHE_TTL` to take
    effect.

    Args:
      repo_did (str)

    Returns:
      ec.EllipticCurvePrivateKey:

    Raises:
      XrpcError: if the repo doesn't exist or isn't active
    """
    return server.load_repo(repo_did).signing_key


def error(name, message, status=400):
    """Returns an XRPC error as a Flask response.

    Args:
      name (str)
      message (str)
      status (int)
    """
    logger.info(f'{status} {name}: {message}')
    return {'error': name, 'message': message}, status, RESPONSE_HEADERS


def handler(nsid):
    """Service proxying handler for :func:`lexrpc.flask_server.init_flask`.

    Pass as ``fallback`` so that methods we don't implement ourselves get proxied
    to the service the client asks for in the ``atproto-proxy`` header.

    Authenticates requests with :func:`server.auth`. Exceptions from it that are
    werkzeug ``HTTPException``\\s, eg OAuth errors, pass through as is.

    Requests without an ``atproto-proxy`` header get ``MethodNotImplemented``.

    Args:
      nsid (str)

    Returns:
      Flask response
    """
    # don't proxy com.atproto.server.*; the PDS owns those
    if nsid.startswith('com.atproto.server.'):
        return error('MethodNotImplemented', f'{nsid} not implemented',
                     status=501)

    if not (target := request.headers.get('atproto-proxy')):
        return error('MethodNotImplemented',
                     f'{nsid} not implemented, and no atproto-proxy header',
                     status=501)

    target_did, _, service_id = target.partition('#')
    if not target_did or not service_id:
        return error('InvalidRequest', f'Bad atproto-proxy header {target}')

    try:
        user_did = server.auth()
    except (NotImplementedError, ValueError) as e:
        return error('AuthMissing', f'Proxying {nsid} requires authentication: {e}',
                     status=401)

    # ALL_REPOS credentials don't have a user to sign service auth JWTs as
    if not isinstance(user_did, str):
        return error('AuthMissing', f'Proxying {nsid} requires user authentication',
                     status=401)

    try:
        doc = did.resolve(target_did)
    except (ValueError, requests.RequestException) as e:
        return error('InvalidRequest', f"Couldn't resolve {target_did}: {e}")

    endpoint = None
    for service in (doc or {}).get('service', []):
        # DID docs may use either a relative or absolute service id
        if service.get('id') in (f'#{service_id}', f'{target_did}#{service_id}'):
            endpoint = service.get('serviceEndpoint')
            break

    if not endpoint:
        return error('InvalidRequest',
                     f'{target_did} has no #{service_id} serviceEndpoint')

    try:
        key = signing_key(user_did)
    except XrpcError as e:
        return error(e.name, e.message)

    token = service_jwt(host=urlparse(endpoint).netloc, repo_did=user_did,
                        privkey=key, aud=target_did, lxm=nsid,
                        expiration=JWT_EXPIRATION)
    headers = {
        'User-Agent': USER_AGENT,
        'Authorization': f'Bearer {token}',
        # we stream the body back in its original encoding. no Accept-Encoding
        # isn't the same as "any encoding is fine," so default to none explicitly
        # rather than letting requests default to gzip.
        'Accept-Encoding': request.headers.get('Accept-Encoding', 'identity'),
        **{name: val for name in FORWARD_REQUEST_HEADERS
           if (val := request.headers.get(name))},
    }

    # not request.full_path; it appends a ? even with no query params
    url = urljoin(endpoint, request.path)
    if request.query_string:
        url += '?' + request.query_string.decode()

    # read-after-write needs to parse and modify the response, so buffer it
    buffer = nsid in read_after_write_fns

    logger.info(f'Proxying {request.method} {url} for {user_did}')
    resp = session.request(request.method, url, headers=headers,
                           data=request.get_data(), stream=not buffer,
                           timeout=HTTP_TIMEOUT)
    logger.info(f'Got {resp.status_code}')

    def stream():
        # make sure we close the response's stream
        with resp:
            yield from resp.raw.stream(CHUNK_SIZE, decode_content=False)

    # case insensitive so that the upstream service's headers collide with
    # ours instead of both ending up in the response
    headers = CaseInsensitiveDict(
        (name, val) for name, val in resp.headers.items()
        # hop-by-hop trailers are connection-specific. trailer header
        # is due to a historic typo in the HTTP RFC.
        # https://datatracker.ietf.org/doc/html/rfc2616#section-13.5.1
        if not is_hop_by_hop(name) and name.lower() != 'trailer')

    # last, so that they win: we're the origin the browser is talking to, so
    # CORS is ours to decide, not the upstream service's
    headers.update(RESPONSE_HEADERS)

    if not buffer:
        return stream(), resp.status_code, headers

    # requests has already decoded the body
    headers.pop('Content-Encoding', None)
    headers.pop('Content-Length', None)

    if (resp.status_code == 200
            and resp.headers.get('Content-Type').startswith('application/json')
            and (rev := resp.headers.get('Atproto-Repo-Rev'))
            and (output := read_after_write(nsid, resp.json(), rev, user_did,
                                            service_did=target_did))):
        # upstream's rev is stale now that we've added newer writes. the
        # reference PDS doesn't send its own rev here either.
        headers.pop('Atproto-Repo-Rev', None)
        return output, 200, headers

    return resp.content, resp.status_code, headers


def read_after_write(nsid, output, rev, repo_did, service_did):
    """Applies a user's recent writes to an upstream response.

    https://atproto.com/guides/writing-data#read-after-write

    Args:
      nsid (str): method NSID, must be in :data:`read_after_write_fns`
      output (dict): upstream response
      rev (str): upstream's ``Atproto-Repo-Rev`` header, the latest rev it has
        indexed from the user's repo
      repo_did (str): the user's repo DID
      service_did (str): DID of the service we're proxying to

    Returns:
      dict: the modified output, or None if there are no local writes since
      ``rev``, or ``output`` doesn't validate
    """
    try:
        writes = list(server.storage.read_writes_since(
            repo_did, rev, limit=READ_AFTER_WRITE_MAX_EVENTS))
    except ValueError as e:
        logger.info(f'Bad Atproto-Repo-Rev {rev}: {e}')
        return None

    if not writes:
        return None

    # upstream is the authority on its own schemas, and ours may have drifted,
    # so if its output doesn't validate, skip read-after-write instead of failing
    try:
        server.server.validate(nsid, 'output', output)
    except ValidationError as e:
        logger.info(f"{nsid} output doesn't validate, skipping read-after-write: {e}")
        return None

    repo = server.load_repo(repo_did)

    # maps collection to dict that maps rkey to latest record, or None if deleted
    records = {}
    for write in writes:
        records.setdefault(write.collection, {})[write.rkey] = write.record

    read_after_write_fns[nsid](output, records, repo, service_did)

    return output


def blob_url(did, blob, preset, service_did):
    """Returns a URL for a blob in a local record.

    Uses Bluesky's CDN if we're proxying to ``$APPVIEW_HOST``, otherwise our own
    ``com.atproto.sync.getBlob``.

    Args:
      did (str): repo DID
      blob (dict): blob object from a record
      preset (str): CDN image preset, eg ``avatar`` or ``feed_thumbnail``
      service_did (str): DID of the service we're proxying to

    Returns:
      str: URL, or None if ``blob`` is empty
    """
    cid = blob['ref'].encode('base32')
    if ((appview := os.environ.get('APPVIEW_HOST'))
            and service_did == f'did:web:{appview}'):
        return f'{BLUESKY_CDN}/img/{preset}/plain/{did}/{cid}@jpeg'

    return f'https://{os.environ["PDS_HOST"]}/xrpc/com.atproto.sync.getBlob?did={did}&cid={cid}'


def apply_viewer_state(post, records, did):
    """Applies a user's local likes and reposts to a post view.

    Sets and clears ``viewer.like`` and ``viewer.repost`` and adjusts the
    counts.

    Args:
      post (dict): ``app.bsky.feed.defs#postView``
      records (dict): from :func:`read_after_write`, maps collection to dict
        that maps rkey to latest record, or None if deleted
      did (str): repo DID
    """
    viewer = post.setdefault('viewer', {})
    for collection, field, count in (
            ('app.bsky.feed.like', 'like', 'likeCount'),
            ('app.bsky.feed.repost', 'repost', 'repostCount'),
    ):
        for rkey, record in records.get(collection, {}).items():
            uri = at_uri(did, collection, rkey)
            if not record and viewer.get(field) == uri:
                del viewer[field]
                post[count] = max(post.get(count, 0) - 1, 0)
                logger.info(f'read-after-write: undoing {uri} for {post["uri"]}')
            elif (record and record['subject']['uri'] == post['uri']
                  and not viewer.get(field)):
                viewer[field] = uri
                post[count] = post.get(count, 0) + 1
                logger.info(f'read-after-write: adding {uri} for {post["uri"]}')


def apply_feed_viewer_state(feed, records, did):
    """Applies a user's local likes and reposts to the post views in a feed.

    Includes reply roots and parents.

    Args:
      feed (list of dict): ``app.bsky.feed.defs#feedViewPost`` s
      records (dict): from :func:`read_after_write`
      did (str): repo DID
    """
    for item in feed:
        apply_viewer_state(item['post'], records, did)
        reply = item.get('reply', {})
        for field in 'root', 'parent':
            if reply.get(field, {}).get('$type') == 'app.bsky.feed.defs#postView':
                apply_viewer_state(reply[field], records, did)


def apply_posts(output, posts, repo, service_did):
    """Removes a user's deleted posts from a feed and inserts new ones.

    Args:
      output (dict): upstream response with ``feed``, a list of
        ``app.bsky.feed.defs#feedViewPost``
      posts (dict): maps rkey to latest ``app.bsky.feed.post`` record, or None
        if deleted
      repo (Repo)
      service_did (str): DID of the service we're proxying to
    """
    posts = {at_uri(repo.did, 'app.bsky.feed.post', rkey): record
             for rkey, record in posts.items()}

    for uri, record in posts.items():
        verb = 'adding' if record else 'removing'
        logger.info(f'read-after-write: {verb} {uri}')

    # remove deleted posts
    output['feed'] = [item for item in output['feed']
                      if item['post']['uri'] not in posts
                      or posts[item['post']['uri']]]

    # insert new posts if we're on the first page
    if request.args.get('cursor'):
        return

    existing = {item['post']['uri'] for item in output['feed']}
    new = {uri: record for uri, record in posts.items()
           if record and uri not in existing}

    # sort reverse chronologically; cut off at end of this page to remove
    # eg backdated or updated posts
    if output.get('cursor') and output['feed']:
        last = output['feed'][-1]
        oldest = parse_iso8601(last.get('reason', {}).get('indexedAt')
                               or last['post']['record']['createdAt'])
        new = {uri: record for uri, record in new.items()
               if parse_iso8601(record['createdAt']) >= oldest}

    if not new:
        return

    author = {
        'did': repo.did,
        'handle': repo.handle or 'handle.invalid',
    }
    if profile := repo.get_record('app.bsky.actor.profile', 'self'):
        if name := profile.get('displayName'):
            author['displayName'] = name
        if avatar := profile.get('avatar'):
            author['avatar'] = blob_url(repo.did, avatar, 'avatar', service_did)

    views = []
    new_reverse_chron = sorted(new.items(), reverse=True,
                              key=lambda item: parse_iso8601(item[1]['createdAt']))
    for uri, record in new_reverse_chron:
        view = {
            'uri': uri,
            'cid': dag_cbor_cid(record).encode('base32'),
            'author': author,
            # convert blobs, CIDs, etc to ATProto-flavored JSON
            'record': json.loads(dag_json.encode(record, dialect='atproto')),
            'replyCount': 0,
            'repostCount': 0,
            'likeCount': 0,
            'quoteCount': 0,
            'indexedAt': webutil.util.now().isoformat(),
        }
        if embed := embed_view(repo.did, record.get('embed', {}), service_did):
            view['embed'] = embed
        views.append({'post': view})

    output['feed'] = views + output['feed']


def embed_view(did, embed, service_did):
    """Returns the view for a local post's embed.

    ``app.bsky.embed.record`` and ``recordWithMedia`` need the embedded record's
    view, which only the appview has, and ``app.bsky.embed.video`` needs the
    appview's HLS playlist, so those return None.

    Args:
      did (str): repo DID
      embed (dict): ``embed`` field from an ``app.bsky.feed.post`` record
      service_did (str): DID of the service we're proxying to

    Returns:
      dict: embed view, or None
    """
    type = embed.get('$type')

    if type == 'app.bsky.embed.images':
        view = {
            '$type': 'app.bsky.embed.images#view',
            'images': [],
        }
        for img in embed['images']:
            view_img = {
                'thumb': blob_url(did, img['image'], 'feed_thumbnail', service_did),
                'fullsize': blob_url(did, img['image'], 'feed_fullsize', service_did),
                'alt': img['alt'],
            }
            if aspect := img.get('aspectRatio'):
                view_img['aspectRatio'] = aspect
            view['images'].append(view_img)

        return view

    elif type == 'app.bsky.embed.external':
        external = embed['external']
        view = {
            '$type': 'app.bsky.embed.external#view',
            'external': {
                'uri': external['uri'],
                'title': external['title'],
                'description': external['description'],
            },
        }
        if thumb := external.get('thumb'):
            view['external']['thumb'] = blob_url(did, thumb, 'feed_thumbnail',
                                                 service_did)
        return view


@read_after_write_fn('app.bsky.feed.getTimeline')
def get_timeline(output, records, repo, service_did):
    """Read-after-write for ``app.bsky.feed.getTimeline``."""
    apply_posts(output, records.get('app.bsky.feed.post', {}), repo, service_did)
    apply_feed_viewer_state(output['feed'], records, repo.did)


@read_after_write_fn('app.bsky.feed.getAuthorFeed')
def get_author_feed(output, records, repo, service_did):
    """Read-after-write for ``app.bsky.feed.getAuthorFeed``.

    Only updates posts when ``actor`` is the user themselves.
    """
    filter = request.args.get('filter', 'posts_with_replies')

    # matches the appview's filters, in getAuthorFeed in
    # https://github.com/bluesky-social/atproto/blob/main/packages/bsky/src/data-plane/server/routes/feeds.ts
    def matches(record):
        reply = record.get('reply')
        embed = record.get('embed') or {}
        if embed.get('$type') == 'app.bsky.embed.recordWithMedia':
            embed = embed['media']
        embed_type = embed.get('$type')

        match filter:
            case 'posts_with_replies':
                return True
            case 'posts_no_replies':
                return not reply
            case 'posts_with_media':
                return embed_type in ('app.bsky.embed.images',
                                      'app.bsky.embed.gallery')
            case 'posts_with_video':
                return embed_type == 'app.bsky.embed.video'
            case 'posts_and_author_threads':
                return (not reply
                        or reply['root']['uri'].startswith(f'at://{repo.did}/'))

        return False

    if request.args.get('actor') in (repo.did, repo.handle):
        posts = {rkey: record
                 for rkey, record in records.get('app.bsky.feed.post', {}).items()
                 if not record or matches(record)}
        apply_posts(output, posts, repo, service_did)

    apply_feed_viewer_state(output['feed'], records, repo.did)


@read_after_write_fn('app.bsky.actor.getProfile')
def get_profile(output, records, repo, service_did):
    """Read-after-write for ``app.bsky.actor.getProfile``.

    Only for the the user's ``app.bsky.actor.profile``. Sets or removes
    ``displayName``, ``description``, ``avatar``, and ``banner``.
    """
    if output['did'] != repo.did:
        return

    if not (profile := records.get('app.bsky.actor.profile', {}).get('self') or {}):
        return

    logger.info(f'read-after-write: applying updated app.bsky.actor.profile/self')

    for field in 'displayName', 'description':
        if val := profile.get(field):
            output[field] = val
        else:
            output.pop(field, None)

    for field in 'avatar', 'banner':
        if blob := profile.get(field):
            output[field] = blob_url(repo.did, blob, field, service_did)
        else:
            output.pop(field, None)


@read_after_write_fn('app.bsky.actor.getProfiles')
def get_profiles(output, records, repo, service_did):
    """Read-after-write for ``app.bsky.actor.getProfiles``.

    Updates the user's own entry in ``profiles``, if any, the same way as
    :func:`get_profile`.
    """
    for profile in output['profiles']:
        get_profile(profile, records, repo, service_did)


@read_after_write_fn('app.bsky.feed.getPosts')
def get_posts(output, records, repo, service_did):
    """Read-after-write for ``app.bsky.feed.getPosts``.

    Applies likes and reposts to ``posts``.
    """
    for post in output['posts']:
        apply_viewer_state(post, records, repo.did)


@read_after_write_fn('app.bsky.feed.getPostThread')
def get_post_thread(output, records, repo, service_did):
    """Read-after-write for ``app.bsky.feed.getPostThread``.

    Applies likes and reposts to ``thread`` and its ``parent`` s and
    ``replies``, recursively.
    """
    def apply(thread):
        if thread.get('$type') != 'app.bsky.feed.defs#threadViewPost':
            return

        apply_viewer_state(thread['post'], records, repo.did)
        if parent := thread.get('parent'):
            apply(parent)
        for reply in thread.get('replies', []):
            apply(reply)

    apply(output['thread'])


@read_after_write_fn('app.bsky.unspecced.getPostThreadV2')
def get_post_thread_v2(output, records, repo, service_did):
    """Read-after-write for ``app.bsky.unspecced.getPostThreadV2``.

    Applies likes and reposts to the posts in ``thread``.
    """
    for item in output['thread']:
        if item['value'].get('$type') == 'app.bsky.unspecced.defs#threadItemPost':
            apply_viewer_state(item['value']['post'], records, repo.did)
