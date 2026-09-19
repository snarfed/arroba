"""Unit tests for permissions.py."""
from ..permissions import allows, describe, supported, parse, Repo
from ..storage import Action
from .testutil import TestCase


class PermissionsTest(TestCase):

    def test_parse(self):
        for scope, expected in (
            ('atproto', ()),
            ('transition:generic', (Repo(('*',)),)),
            ('repo:*', (Repo(('*',)),)),
            ('repo:app.example.profile', (Repo(('app.example.profile',)),)),
            ('repo:app.example.profile?action=create&action=update&action=delete',
             (Repo(('app.example.profile',), tuple(Action)),)),
            ('repo?collection=app.example.profile&collection=app.example.post',
             (Repo(('app.example.profile', 'app.example.post')),)),
            ('repo:*?action=delete', (Repo(('*',), (Action.DELETE,)),)),
            ('repo?action=create&collection=app.example.post',
             (Repo(('app.example.post',), (Action.CREATE,)),)),
            ('repo:app.example%2Epost', (Repo(('app.example.post',)),)),
            ('repo?collection=app.example%2Epost&action=%64elete',
             (Repo(('app.example.post',), (Action.DELETE,)),)),
            ('repo:app.example.post?%61ction=create',
             (Repo(('app.example.post',), (Action.CREATE,)),)),
        ):
            with self.subTest(scope=scope):
                self.assertEqual(expected, parse(scope))

    def test_parse_invalid(self):
        for scope in (
            '',
            'repo',
            'repo:',
            'repo?',
            'repo:?',
            'repo:&',
            'repo app.example.post',
            'repo:app.example.*',
            'repo:foo',
            'repo:app.example.post?collection=app.example.other',
            'repo:app.example.post?action=read',
            # case-sensitive
            'repo:app.example.post?action=CREATE',
            'repo:app.example.post?action=Delete',
            'REPO:app.example.post',
            'repo:app.example.post?action',
            'repo:app.example.post?foo=bar',
            'repo:app.example.québec',
            # percent-decoded once, not twice
            'repo:app.example%252Epost',
            'repo?collection=app.example%252Epost',
            # we don't support these (yet)
            'transition:chat.bsky',
            'transition:email',
            'rpc:app.example.moderation.createReport?aud=*',
            'blob:*/*',
            'account:email',
            'identity:handle',
            'include:app.example.authFull',
            'resource:positional?key=val',
        ):
            with self.subTest(scope=scope):
                with self.assertRaises(ValueError):
                    parse(scope)

    def test_supported(self):
        self.assertEqual([], supported([]))
        self.assertEqual([
            'atproto',
            'repo:app.example.post',
            'transition:generic',
        ], supported([
            'atproto',
            'repo:app.example.post',
            'identity:handle',
            'repo:app.example.*',
            'transition:generic',
            'transition:email',
        ]))

    def test_allows(self):
        for scopes, collection, action, expected in (
            ([], 'app.example.post', Action.CREATE, False),
            (['atproto'], 'app.example.post', Action.CREATE, False),
            (['atproto', 'transition:generic'], 'app.example.post', Action.CREATE, True),
            (['atproto', 'transition:generic'], 'app.example.post', Action.DELETE, True),
            (['repo:*'], 'app.example.post', Action.UPDATE, True),
            (['repo:app.example.post'], 'app.example.post', Action.CREATE, True),
            (['repo:app.example.post'], 'app.example.post', Action.UPDATE, True),
            (['repo:app.example.post'], 'app.example.post', Action.DELETE, True),
            (['repo:app.example.post'], 'app.example.like', Action.CREATE, False),
            (['repo:app.example.post?action=create'], 'app.example.post', Action.CREATE,
             True),
            (['repo:app.example.post?action=create'], 'app.example.post', Action.DELETE,
             False),
            (['repo:*?action=delete'], 'app.example.post', Action.DELETE, True),
            (['repo:*?action=delete'], 'app.example.post', Action.CREATE, False),
            (['repo?collection=app.example.post&collection=app.example.like'],
             'app.example.like', Action.CREATE, True),
            (['repo:app.example.post?action=create',
              'repo:app.example.post?action=delete'],
             'app.example.post', Action.DELETE, True),

            # invalid and unsupported scopes are ignored
            (['repo:app.example.*'], 'app.example.post', Action.CREATE, False),
            (['rpc:*?aud=*'], 'app.example.post', Action.CREATE, False),
        ):
            with self.subTest(scopes=scopes, collection=collection, action=action):
                self.assertEqual(expected, allows(scopes, collection, action))

    def test_describe(self):
        for scope, expected in (
            ('atproto', 'Know which account is yours'),
            ('transition:generic', 'Create, update, and delete all records'),
            ('repo:*', 'Create, update, and delete all records'),
            ('repo:app.example.post', 'Create, update, and delete app.example.post records'),
            ('repo:*?action=delete', 'Delete all records'),
            ('repo:app.example.post?action=create&action=update',
             'Create and update app.example.post records'),
            ('repo?collection=app.example.post&collection=app.example.like&collection=app.example.repost',
             'Create, update, and delete app.example.post, app.example.like, and app.example.repost records'),
        ):
            with self.subTest(scope=scope):
                self.assertEqual(expected, describe(scope))

    def test_describe_invalid(self):
        with self.assertRaises(ValueError):
            describe('identity:handle')
