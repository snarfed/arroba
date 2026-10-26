"""Unit tests for server.py."""
from unittest.mock import patch

from flask import request
from lexrpc.base import XrpcError

from .. import server
from ..permissions import Blob, Repo
from ..storage import Action
from .testutil import XrpcTestCase


class ServerTest(XrpcTestCase):

    def test_global_token_auth(self):
        self.prepare_auth()
        self.assertEqual((server.ALL_REPOS, server.ALL_SCOPES),
                         server.global_token_auth())

    def test_global_token_auth_bad_token(self):
        self.prepare_auth()
        request.headers['Authorization'] = 'Bearer nope'
        with self.assertRaises(ValueError):
            server.global_token_auth()

    @patch.object(server, 'authenticate',
                  return_value=('did:web:user.com', ['atproto']))
    def test_authorize_no_perms(self, mock_auth):
        self.assertEqual('did:web:user.com', server.authorize('did:web:user.com', ()))
        mock_auth.assert_called_once_with()

    @patch.object(server, 'authenticate', return_value=(
        'did:web:user.com', ['atproto', 'repo:app.bsky.feed.post',
                             'repo:app.bsky.feed.like?action=delete']))
    def test_authorize_writes(self, mock_auth):
        self.assertEqual('did:web:user.com', server.authorize('did:web:user.com', [
            Repo(('app.bsky.feed.post',), (Action.CREATE,)),
            Repo(('app.bsky.feed.post',), (Action.UPDATE,)),
            Repo(('app.bsky.feed.like',), (Action.DELETE,)),
        ]))
        mock_auth.assert_called_once_with()

    @patch.object(server, 'authenticate', return_value=(
        'did:web:user.com', ['atproto', 'repo:app.bsky.feed.post']))
    def test_authorize_insufficient_scope(self, mock_auth):
        with self.assertRaises(XrpcError) as e:
            server.authorize('did:web:user.com', [
                Repo(('app.bsky.feed.post',), (Action.CREATE,)),
                Repo(('app.bsky.feed.like',), (Action.CREATE,)),
            ])

        self.assertEqual('insufficient_scope', e.exception.name)
        self.assertEqual(403, e.exception.status)
        self.assertEqual({
            'WWW-Authenticate': 'DPoP error="insufficient_scope", error_description="Missing scope repo:app.bsky.feed.like?action=create"',
        }, e.exception.headers)
        self.assertEqual('Missing scope repo:app.bsky.feed.like?action=create',
                         e.exception.message)
        mock_auth.assert_called_once_with()

    @patch.object(server, 'authenticate',
                  return_value=('did:web:user.com', ['atproto']))
    def test_authorize_atproto_scope_only(self, _):
        with self.assertRaises(XrpcError) as e:
            server.authorize('did:web:user.com', [Repo(('app.bsky.feed.post',), (Action.CREATE,))])
        self.assertEqual(403, e.exception.status)

    @patch.object(server, 'authenticate', return_value=(
        'did:web:user.com', ['atproto', 'transition:generic']))
    def test_authorize_transition_generic(self, _):
        self.assertEqual('did:web:user.com', server.authorize('did:web:user.com', [
            Repo(('app.bsky.feed.post',), (Action.CREATE,)),
            Repo(('app.bsky.graph.follow',), (Action.DELETE,)),
        ]))

    @patch.object(server, 'authenticate', return_value=(
        'did:web:user.com', ['atproto', 'blob:image/*']))
    def test_authorize_blob(self, _):
        self.assertEqual('did:web:user.com', server.authorize('did:web:user.com', [
            Blob(('image/png',)),
        ]))

        with self.assertRaises(XrpcError) as e:
            server.authorize('did:web:user.com', [Blob(('video/mp4',))])
        self.assertEqual('Missing scope blob:video/mp4', e.exception.message)

    @patch.object(server, 'authenticate', return_value=(
        'did:web:user.com', ['atproto', 'blob:image/*']))
    def test_authorize_no_did(self, _):
        self.assertEqual('did:web:user.com',
                         server.authorize(None, [Blob(('image/png',))]))

    @patch.object(server, 'authenticate', return_value=(None, ['atproto', 'blob:*/*']))
    def test_authorize_no_did_auth_returns_no_did_fails_closed(self, _):
        with self.assertRaises(XrpcError) as e:
            server.authorize(None, [Blob(('image/png',))])
        self.assertEqual('AuthRequired', e.exception.name)

    @patch.object(server, 'authenticate', return_value=(
        'did:web:other.com', ['atproto', 'transition:generic']))
    def test_authorize_other_repo(self, _):
        with self.assertRaises(XrpcError) as e:
            server.authorize('did:web:user.com', [Repo(('app.bsky.feed.post',), (Action.CREATE,))])
        self.assertEqual('AuthRequired', e.exception.name)

    @patch.object(server, 'authenticate',
                  return_value=(server.ALL_REPOS, server.ALL_SCOPES))
    def test_authorize_all_repos_all_scopes(self, _):
        self.assertIs(server.ALL_REPOS, server.authorize('did:web:user.com', [
            Repo(('app.bsky.feed.post',), (Action.CREATE,)),
        ]))

    def test_authorize_unexpected_auth_return_fails_closed(self):
        """eg an auth function that forgets to return."""
        with patch.object(server, 'authenticate', return_value=(None, None)):
            with self.assertRaises(XrpcError) as e:
                server.authorize('did:web:user.com', [Repo(('app.bsky.x',), (Action.CREATE,))])
            self.assertEqual('AuthRequired', e.exception.name)

        with patch.object(server, 'authenticate',
                          return_value=('did:web:user.com', 'atproto')):
            with self.assertRaises(XrpcError) as e:
                server.authorize('did:web:user.com', [Repo(('app.bsky.x',), (Action.CREATE,))])
            self.assertEqual('insufficient_scope', e.exception.name)
