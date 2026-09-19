"""ATProto OAuth permissions, aka scopes.

https://atproto.com/specs/permission

Only supports the ``repo`` resource so far, along with the ``atproto`` and
``transition:generic`` scopes. Other resources, ``include:`` permission sets, and
other transitional scopes are rejected.
"""
from collections import namedtuple
import logging
import re
from urllib.parse import parse_qs, unquote

from lexrpc.base import NSID_RE

from .util import Action

logger = logging.getLogger(__name__)

# maps action names in scope strings to Actions. case-sensitive.
SCOPE_ACTIONS = {action.name.lower(): action for action in Action}

# printable, non-whitespace ASCII
SCOPE_RE = re.compile(r'[!-~]+')

Repo = namedtuple('Repo', [
    'collection',  # tuple of string lexicon NSIDs
    'action',      # tuple of Actions
], defaults=(tuple(Action),))


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

    Args:
      scope (str)

    Returns:
      tuple of Repo: usually one, but ``transition:generic`` expands to
      ``Repo('*')``, and ``atproto`` expands to none

    Raises:
      ValueError: if ``scope`` is malformed or not supported
    """
    if scope == 'atproto':
        return ()
    elif scope == 'transition:generic':
        return (Repo(('*',)),)
    elif not SCOPE_RE.fullmatch(scope):
        raise ValueError(f'Invalid scope {scope!r}')

    before, _, query = scope.partition('?')
    resource, colon, positional = before.partition(':')
    if resource != 'repo':
        raise ValueError(f'Unsupported scope {scope}')
    elif colon and not positional:
        raise ValueError(f'Empty positional parameter in {scope}')

    params = parse_qs(query, keep_blank_values=True, strict_parsing=True)
    if unknown := params.keys() - {'collection', 'action'}:
        raise ValueError(f'Invalid parameter(s) {unknown} in {scope}')
    elif positional and 'collection' in params:
        raise ValueError(f'collection is both positional and named in {scope}')

    collections = ([unquote(positional)] if positional
                   else params.get('collection', []))  # parse_qs already unquoted
    if not collections:
        raise ValueError(f'Missing collection in {scope}')
    elif bad := [c for c in collections if c != '*' and not NSID_RE.fullmatch(c)]:
        raise ValueError(f'Invalid collection(s) {bad} in {scope}')

    actions = params.get('action') or SCOPE_ACTIONS.keys()
    if bad := set(actions) - SCOPE_ACTIONS.keys():
        raise ValueError(f'Invalid action(s) {bad} in {scope}')

    return (Repo(tuple(collections), tuple(SCOPE_ACTIONS[a] for a in actions)),)


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


def allows(scopes, collection, action):
    """Returns whether a set of scopes allows a given repo write.

    Args:
      scopes (sequence of str)
      collection (str): NSID
      action (Action)

    Returns:
      bool:
    """
    assert isinstance(action, Action), action
    return any((collection in perm.collection or '*' in perm.collection)
               and action in perm.action
               for scope in supported(scopes)
               for perm in parse(scope))


def describe(scope):
    """Describes a scope in English, eg for a user to approve or deny.

    Args:
      scope (str)

    Returns:
      str:

    Raises:
      ValueError: if ``scope`` is malformed or not supported
    """
    if scope == 'atproto':
        return 'Know which account is yours'

    perm, = parse(scope)
    actions = _join([action.name.lower() for action in perm.action]).capitalize()
    collections = 'all' if '*' in perm.collection else _join(perm.collection)
    return f'{actions} {collections} records'


def _join(words):
    """Joins words into an English list, eg ``a, b, and c``.

    TODO: replace this with ``humanize.natural_list`` if we ever add humanize
    as a dependency. https://humanize.readthedocs.io/en/stable/lists/
    """
    if len(words) <= 2:
        return ' and '.join(words)
    return f'{", ".join(words[:-1])}, and {words[-1]}'
