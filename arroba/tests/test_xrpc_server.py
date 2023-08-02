"""Unit tests for xrpc_server.py."""
from arroba import xrpc_server

from .. import server
from . import testutil


class XrpcServerTest(testutil.TestCase):

    def setUp(self):
        super().setUp()
        server.init()

    def test_create_account(self):
        pass

    def test_create_session(self):
        pass

    def test_get_session(self):
        pass

    def test_refresh_session(self):
        pass

    # # atproto/packages/pds/tests/account.test.ts
    # def test_login(self):
    #     res = xrpc_server.createSession({
    #         identifier: handle,
    #         password,
    #     })
    #     jwt = res.data.accessJwt
    #     self.assertEqual('string', typeof jwt)
    #     self.assertEqual('alice.test', res.data.handle)
    #     self.assertEqual(did, res.data.did)
    #     self.assertEqual(email, res.data.email)

    # def test_can_perform_authenticated_requests(self):
    #     agent.api.setHeader('authorization', f'Bearer {jwt}')
    #     res = xrpc_server.getSession({})
    #     self.assertEqual(did, res.data.did)
    #     self.assertEqual(handle, res.data.handle)
    #     self.assertEqual(email, res.data.email)
