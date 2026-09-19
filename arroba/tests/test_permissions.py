"""Unit tests for permissions.py."""
from ..permissions import ACTIONS, allows, describe, supported, parse, Repo
from .testutil import TestCase


class PermissionsTest(TestCase):

    def test_parse(self):
        for scope, expected in (
            ('atproto', ()),
            ('transition:generic', (Repo(('*',)),)),
            ('repo:*', (Repo(('*',)),)),
            ('repo:app.example.profile', (Repo(('app.example.profile',)),)),
            ('repo:app.example.profile?action=create&action=update&action=delete',
             (Repo(('app.example.profile',), ACTIONS),)),
            ('repo?collection=app.example.profile&collection=app.example.post',
             (Repo(('app.example.profile', 'app.example.post')),)),
            ('repo:*?action=delete', (Repo(('*',), ('delete',)),)),
            ('repo?action=create&collection=app.example.post',
             (Repo(('app.example.post',), ('create',)),)),
            ('repo:app.example%2Epost', (Repo(('app.example.post',)),)),
            ('repo?collection=app.example%2Epost&action=%64elete',
             (Repo(('app.example.post',), ('delete',)),)),
            ('repo:app.example.post?%61ction=create',
             (Repo(('app.example.post',), ('create',)),)),
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
            ([], 'app.example.post', 'create', False),
            (['atproto'], 'app.example.post', 'create', False),
            (['atproto', 'transition:generic'], 'app.example.post', 'create', True),
            (['atproto', 'transition:generic'], 'app.example.post', 'delete', True),
            (['repo:*'], 'app.example.post', 'update', True),
            (['repo:app.example.post'], 'app.example.post', 'create', True),
            (['repo:app.example.post'], 'app.example.post', 'update', True),
            (['repo:app.example.post'], 'app.example.post', 'delete', True),
            (['repo:app.example.post'], 'app.example.like', 'create', False),
            (['repo:app.example.post?action=create'], 'app.example.post', 'create',
             True),
            (['repo:app.example.post?action=create'], 'app.example.post', 'delete',
             False),
            (['repo:*?action=delete'], 'app.example.post', 'delete', True),
            (['repo:*?action=delete'], 'app.example.post', 'create', False),
            (['repo?collection=app.example.post&collection=app.example.like'],
             'app.example.like', 'create', True),
            (['repo:app.example.post?action=create',
              'repo:app.example.post?action=delete'],
             'app.example.post', 'delete', True),

            # invalid and unsupported scopes are ignored
            (['repo:app.example.*'], 'app.example.post', 'create', False),
            (['rpc:*?aud=*'], 'app.example.post', 'create', False),
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
