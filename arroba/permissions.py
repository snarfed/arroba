"""ATProto OAuth permissions, aka scopes.

https://atproto.com/specs/permission

Only supports the ``repo``, ``rpc``, and ``blob`` resources so far, along with the
``atproto`` and ``transition:generic`` scopes. Other resources, ``include:``
permission sets, and other transitional scopes are rejected.

Permissions are namedtuples whose fields are all tuples of allowed values, where
``*`` is a wildcard. A request to check is a permission of the same type holding
the single concrete values it uses, eg ``Repo(('app.bsky.feed.post',),
(Action.CREATE,))``, which lets :func:`allows` match any two of them field by
field.
"""
from collections import namedtuple
from fnmatch import fnmatchcase
import logging
import re
from urllib.parse import parse_qs, quote, unquote

from lexrpc.base import NSID_RE, XrpcError

from .util import Action

logger = logging.getLogger(__name__)

# maps action names in scope strings to Actions. case-sensitive.
SCOPE_ACTIONS = {action.name.lower(): action for action in Action}

# printable, non-whitespace ASCII
SCOPE_RE = re.compile(r'[!-~]+')

# DID service reference, ie DID with required service type fragment
AUD_RE = re.compile(r'did:[a-z]+:[^#\s]+#[^#\s]+')

# MIME type or glob pattern, eg image/png, image/*, */*
ACCEPT_RE = re.compile(r'\*/\*|[^*/\s]+/(\*|[^*/\s]+)')

Repo = namedtuple('Repo', [
    'collection',
    'action',
], defaults=(tuple(Action),))
"""
Attributes:
  collection (tuple of strings): lexicon NSIDs
  action (tuple of Action)
"""

Rpc = namedtuple('Rpc', [
    'method',
    'service',
])
"""
Attributes:
  method (tuple of string): method NSIDs; the scope's ``lxm`` param
  service (1-tuple of string): DID service reference; the scope's ``aud`` param,
    eg ``did:web:api.bsky.app#bsky_appview``
"""

Blob = namedtuple('Blob', [
    'accept',
])
"""
Attributes:
  accept (tuple of string): MIME types or glob patterns, eg ``image/*`` or ``*/*``
"""


def parse(scope):
    """Parses one scope string into permissions.

    Scope syntax is https://atproto.com/specs/permission#scope-string-syntax

    Syntax is RESOURCE[:POSITIONAL][?KEY=VAL&KEY=VAL...]

    Examples:
    * repo:*
    * repo:app.bsky.feed.post
    * repo:app.bsky.feed.post?action=create&action=update
    * repo?action=update
    * repo?collection=app.bsky.actor.profile&collection=app.bsky.feed.post
    * rpc:app.bsky.feed.getTimeline?aud=did:web:api.bsky.app%23bsky_appview
    * rpc?lxm=*&aud=did:web:api.bsky.app%23bsky_appview
    * blob:*/*
    * blob?accept=video/*&accept=text/html

    Args:
      scope (str)

    Returns:
      tuple of Repo, Rpc, and Blob: usually one, but ``transition:generic``
      expands to all of them, and ``atproto`` expands to none

    Raises:
      ValueError: if ``scope`` is malformed or not supported
    """
    if scope == 'atproto':
        return ()
    elif scope == 'transition:generic':
        return (Repo(('*',)), Rpc(('*',), ('*',)), Blob(('*/*',)))
    elif not SCOPE_RE.fullmatch(scope):
        raise ValueError(f'Invalid scope {scope!r}')

    before, _, query = scope.partition('?')
    resource, colon, positional = before.partition(':')
    if resource == 'repo':
        param, other = 'collection', 'action'
    elif resource == 'rpc':
        param, other = 'lxm', 'aud'
    elif resource == 'blob':
        param, other = 'accept', None
    else:
        raise ValueError(f'Unsupported scope {scope}')

    if colon and not positional:
        raise ValueError(f'Empty positional parameter in {scope}')

    params = parse_qs(query, keep_blank_values=True, strict_parsing=True)
    if unknown := params.keys() - {param, other}:
        raise ValueError(f'Invalid parameter(s) {unknown} in {scope}')
    elif positional and param in params:
        raise ValueError(f'{param} is both positional and named in {scope}')

    # parse_qs already unquoted
    vals = [unquote(positional)] if positional else params.get(param, [])
    if not vals:
        raise ValueError(f'Missing {param} in {scope}')

    if resource == 'blob':
        for val in vals:
            if not ACCEPT_RE.fullmatch(val):
                raise ValueError(f'Invalid accept(s) {val} in {scope}')
        return (Blob(tuple(vals)),)

    # both collection and lxm are NSIDs
    if bad := [v for v in vals if v != '*' and not NSID_RE.fullmatch(v)]:
        raise ValueError(f'Invalid {param}(s) {bad} in {scope}')

    if resource == 'repo':
        actions = params.get('action') or SCOPE_ACTIONS.keys()
        if bad := set(actions) - SCOPE_ACTIONS.keys():
            raise ValueError(f'Invalid action(s) {bad} in {scope}')
        return (Repo(tuple(vals), tuple(SCOPE_ACTIONS[a] for a in actions)),)

    auds = params.get('aud', [])
    if len(auds) != 1:
        raise ValueError(f'Expected exactly one aud in {scope}')
    elif auds[0] != '*' and not AUD_RE.fullmatch(auds[0]):
        raise ValueError(f'Invalid aud {auds[0]} in {scope}')
    elif auds == ['*'] and vals == ['*']:
        # https://atproto.com/specs/permission#rpc
        raise ValueError(f"lxm and aud can't both be * in {scope}")

    return (Rpc(tuple(vals), tuple(auds)),)


def supported(scopes):
    """Filters requested scope strings to the ones we'll grant.

    Args:
      scopes (sequence of str)

    Returns:
      list of str: the scopes in ``scopes`` that :func:`parse` accepts, unchanged
    """
    ret = []
    for scope in scopes:
        try:
            parse(scope)
            ret.append(scope)
        except ValueError as e:
            logger.warning(f'Not granting scope: {e}')

    return ret


def allows(scopes, permission):
    """Returns whether a set of scopes allows a given request.

    Args:
      scopes (sequence of str)
      permission (Repo, Rpc, or Blob): the request, with single concrete values

    Returns:
      bool:
    """
    assert isinstance(permission, (Repo, Rpc, Blob)), permission

    for scope in supported(scopes):
        for granted in parse(scope):
            if type(granted) is not type(permission):
                continue
            elif isinstance(permission, Blob):
                if all(any(fnmatchcase(want, have) for have in granted.accept)
                       for want in permission.accept):
                    return True
            elif all(set(want) <= set(have) or '*' in have
                     for want, have in zip(permission, granted)):
                return True

    return False


def insufficient_scope(permission):
    """Returns an error for a request that a credential's scopes don't allow.

    Args:
      permission (Repo, Rpc, or Blob): the request, with single concrete values

    Returns:
      XrpcError: with the scope the credential would have needed
    """
    if isinstance(permission, Repo):
        action = permission.action[0].name.lower()
        scope = f'repo:{permission.collection[0]}?action={action}'
    elif isinstance(permission, Rpc):
        aud = quote(permission.service[0], safe=':')
        scope = f'rpc:{permission.method[0]}?aud={aud}'
    else:
        assert isinstance(permission, Blob)
        scope = f'blob:{permission.accept[0]}'

    msg = f'Missing scope {scope}'
    return XrpcError(msg, name='insufficient_scope', status=403, headers={
        'WWW-Authenticate': f'DPoP error="insufficient_scope", error_description="{msg}"',
    })


def describe(scope):
    """Describes a scope in English, eg for a user to approve or deny.

    Args:
      scope (str)

    Returns:
      list of str: one description per permission the scope grants

    Raises:
      ValueError: if ``scope`` is malformed or not supported
    """
    if scope == 'atproto':
        return ['Know which account is yours']

    descriptions = []
    for perm in parse(scope):
        if isinstance(perm, Repo):
            actions = _join([action.name.lower()
                             for action in perm.action]).capitalize()
            collections = 'all' if '*' in perm.collection else _join(perm.collection)
            descriptions.append(f'{actions} {collections} records')
        elif isinstance(perm, Blob):
            accept = 'any' if '*/*' in perm.accept else _join(perm.accept)
            descriptions.append(f'Upload {accept} files')
        else:
            methods = 'any' if '*' in perm.method else _join(perm.method)
            if '*' in perm.service:
                service = 'any service'
            else:
                # hostname is friendlier than the full DID service reference
                service = perm.service[0].partition('#')[0].removeprefix('did:web:')
            descriptions.append(f'Make {methods} requests to {service} as you')

    return descriptions


def _join(words):
    """Joins words into an English list, eg ``a, b, and c``.

    TODO: replace this with ``humanize.natural_list`` if we ever add humanize
    as a dependency. https://humanize.readthedocs.io/en/stable/lists/
    """
    if len(words) <= 2:
        return ' and '.join(words)
    return f'{", ".join(words[:-1])}, and {words[-1]}'
