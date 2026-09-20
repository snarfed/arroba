"""Unit tests for permissions.py."""
from ..permissions import allows, describe, supported, parse, Repo, Rpc
from ..storage import Action
from .testutil import TestCase


class PermissionsTest(TestCase):

    def test_parse(self):
        for scope, expected in (
            ('atproto', ()),
            ('transition:generic', (Repo(('*',)), Rpc(('*',), ('*',)))),
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
            ('rpc:app.example.moderation.createReport?aud=*',
             (Rpc(('app.example.moderation.createReport',), ('*',)),)),
            ('rpc:*?aud=did:web:ho.st%23svc', (Rpc(('*',), ('did:web:ho.st#svc',)),)),
            # an unencoded # parses the same way
            ('rpc:*?aud=did:web:ho.st#svc', (Rpc(('*',), ('did:web:ho.st#svc',)),)),
            ('rpc?lxm=*&aud=did:web:ho.st#svc', (Rpc(('*',), ('did:web:ho.st#svc',)),)),
            ('rpc?lxm=app.example.getFoo&lxm=app.example.getBar&aud=did:web:ho.st#svc',
             (Rpc(('app.example.getFoo', 'app.example.getBar'), ('did:web:ho.st#svc',)),)),
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
            'rpc',
            'rpc:',
            'rpc:app.example.getFoo',
            'rpc:app.example.*?aud=did:web:ho.st#svc',
            'rpc:app.example.getFoo?lxm=app.example.getBar&aud=did:web:ho.st#svc',
            'rpc:app.example.getFoo?aud=did:web:ho.st#svc&aud=*',
            'rpc:app.example.getFoo?aud=',
            'rpc:app.example.getFoo?aud=did:web:ho.st',
            'rpc:app.example.getFoo?aud=not-a-did#svc',
            'rpc:app.example.getFoo?aud=did:web:ho.st#svc&action=create',
            # too broad, https://atproto.com/specs/permission#rpc
            'rpc:*?aud=*',
            # we don't support these (yet)
            'transition:chat.bsky',
            'transition:email',
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
            'rpc:app.example.getFoo?aud=did:web:ho.st#svc',
            'transition:generic',
        ], supported([
            'atproto',
            'repo:app.example.post',
            'identity:handle',
            'repo:app.example.*',
            'rpc:app.example.getFoo?aud=did:web:ho.st#svc',
            'rpc:*?aud=*',
            'transition:generic',
            'transition:email',
        ]))

    def test_allows(self):
        post_create = Repo(('app.example.post',), (Action.CREATE,))
        get_foo = Rpc(('app.example.getFoo',), ('did:web:ho.st#svc',))

        for scopes, permission, expected in (
            ([], post_create, False),
            (['atproto'], post_create, False),
            (['atproto', 'transition:generic'], post_create, True),
            (['atproto', 'transition:generic'],
             Repo(('app.example.post',), (Action.DELETE,)), True),
            (['repo:*'], Repo(('app.example.post',), (Action.UPDATE,)), True),
            (['repo:app.example.post'], post_create, True),
            (['repo:app.example.post'],
             Repo(('app.example.post',), (Action.UPDATE,)), True),
            (['repo:app.example.post'],
             Repo(('app.example.post',), (Action.DELETE,)), True),
            (['repo:app.example.post'], Repo(('app.example.like',), (Action.CREATE,)),
             False),
            (['repo:app.example.post?action=create'], post_create, True),
            (['repo:app.example.post?action=create'],
             Repo(('app.example.post',), (Action.DELETE,)), False),
            (['repo:*?action=delete'],
             Repo(('app.example.post',), (Action.DELETE,)), True),
            (['repo:*?action=delete'], post_create, False),
            (['repo?collection=app.example.post&collection=app.example.like'],
             Repo(('app.example.like',), (Action.CREATE,)), True),
            (['repo:app.example.post?action=create',
              'repo:app.example.post?action=delete'],
             Repo(('app.example.post',), (Action.DELETE,)), True),

            ([], get_foo, False),
            (['atproto'], get_foo, False),
            (['atproto', 'transition:generic'], get_foo, True),
            (['rpc:app.example.getFoo?aud=did:web:ho.st#svc'], get_foo, True),
            (['rpc:*?aud=did:web:ho.st#svc'], get_foo, True),
            (['rpc:app.example.getFoo?aud=*'], get_foo, True),
            (['rpc:app.example.getBar?aud=did:web:ho.st#svc'], get_foo, False),
            (['rpc:app.example.getFoo?aud=did:web:other.ho.st%23svc'], get_foo, False),

            # scopes for one resource don't allow another
            (['rpc:*?aud=did:web:ho.st#svc'], post_create, False),
            (['repo:*'], get_foo, False),

            # invalid and unsupported scopes are ignored
            (['repo:app.example.*'], post_create, False),
            (['rpc:*?aud=*'], get_foo, False),
            (['identity:handle'], post_create, False),
        ):
            with self.subTest(scopes=scopes, permission=permission):
                self.assertEqual(expected, allows(scopes, permission))

    def test_describe(self):
        for scope, expected in (
            ('atproto', ['Know which account is yours']),
            ('transition:generic', ['Create, update, and delete all records',
                                    'Make any requests to any service as you']),
            ('repo:*', ['Create, update, and delete all records']),
            ('repo:app.example.post',
             ['Create, update, and delete app.example.post records']),
            ('repo:*?action=delete', ['Delete all records']),
            ('repo:app.example.post?action=create&action=update',
             ['Create and update app.example.post records']),
            ('repo?collection=app.example.post&collection=app.example.like&collection=app.example.repost',
             ['Create, update, and delete app.example.post, app.example.like, and app.example.repost records']),
            ('rpc:app.example.getFoo?aud=did:web:ho.st#svc',
             ['Make app.example.getFoo requests to ho.st as you']),
            ('rpc:*?aud=did:web:ho.st#svc',
             ['Make any requests to ho.st as you']),
            ('rpc:app.example.getFoo?aud=*',
             ['Make app.example.getFoo requests to any service as you']),
            ('rpc:app.example.getFoo?aud=did:plc:123abc%23svc_appview',
             ['Make app.example.getFoo requests to did:plc:123abc as you']),
            ('rpc?lxm=app.example.getFoo&lxm=app.example.getBar&aud=did:web:ho.st#svc',
             ['Make app.example.getFoo and app.example.getBar requests to ho.st as you']),
        ):
            with self.subTest(scope=scope):
                self.assertEqual(expected, describe(scope))

    def test_describe_invalid(self):
        with self.assertRaises(ValueError):
            describe('identity:handle')
