"""ATProto XRPC service proxying.

https://atproto.com/specs/xrpc#service-proxying
"""
from datetime import timedelta
import logging
import threading
from urllib.parse import urljoin, urlparse
from wsgiref.util import is_hop_by_hop

from cachetools import cached, TTLCache
from flask import request
from lexrpc.base import XrpcError
from lexrpc.flask_server import RESPONSE_HEADERS
import requests
from webutil.util import HTTP_TIMEOUT, session

from . import did
from . import server
from .util import service_jwt, USER_AGENT

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


def handler(auth, default_service=None):
    """Generates a service proxying handler for :func:`lexrpc.flask_server.init_flask`.

    Pass the returned callable as ``fallback`` so that methods we don't
    implement ourselves get proxied to the service the client asks for in the
    ``atproto-proxy`` header.

    Args:
      auth (callable: => str): returns the DID of the user who authenticated the
        current request, or None if it's unauthenticated
      default_service (str): ``[DID]#[service id]`` to proxy to when a request
        has no ``atproto-proxy`` header, eg ``did:web:api.bsky.app#bsky_appview``.
        If unset, those requests get ``MethodNotImplemented``.

    Returns:
      callable: str NSID => Flask response
    """
    def proxy(nsid):
        target = request.headers.get('atproto-proxy') or default_service
        if not target:
            return error('MethodNotImplemented',
                         f'{nsid} not implemented, and no atproto-proxy header',
                         status=501)

        target_did, _, service_id = target.partition('#')
        if not target_did or not service_id:
            return error('InvalidRequest', f'Bad atproto-proxy header {target}')

        if not (user_did := auth()):
            return error('AuthMissing', f'Proxying {nsid} requires authentication',
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

        logger.info(f'Proxying {request.method} {url} for {user_did}')
        resp = session.request(request.method, url, headers=headers,
                               data=request.get_data(), stream=True,
                               timeout=HTTP_TIMEOUT)
        logger.info(f'Got {resp.status_code}')

        def stream():
            # make sure we close the response's stream
            with resp:
                yield from resp.raw.stream(CHUNK_SIZE, decode_content=False)

        return stream(), resp.status_code, {
            **RESPONSE_HEADERS,
            **{name: val for name, val in resp.headers.items()
               # hop-by-hop trailers are connection-specific. trailer header
               # is due to a historic typo in the HTTP RFC.
               # https://datatracker.ietf.org/doc/html/rfc2616#section-13.5.1
               if not is_hop_by_hop(name) and name.lower() != 'trailer'},
        }

    return proxy
