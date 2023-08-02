"""Unit tests for xrpc_identity.py."""
from arroba import xrpc_identity

from . import testutil


class XrpcIdentityTest(testutil.TestCase):

    def setUp(self):
        super().setUp()

    def test_resolve_handle(self):
        pass

    # # atproto/packages/pds/tests/handles.test.ts
    # def test_resolves_handles(self):
    #     res = xrpc_identity.resolveHandle({
    #         handle: 'alice.test',
    #     })
    #     expect(res.data.did).toBe(alice)

    # def test_resolves_non_normalized_handles(self):
    #     res = xrpc_identity.resolveHandle({
    #         handle: 'aLicE.tEst',
    #     })
    #     expect(res.data.did).toBe(alice)
