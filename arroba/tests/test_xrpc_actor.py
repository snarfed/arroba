"""Unit tests for xrpc_actor.py."""
from .. import xrpc_actor
from . import testutil


class XrpcActorTest(testutil.XrpcTestCase):

    def test_get_preferences(self):
        resp = xrpc_actor.get_preferences({})
        self.assertEqual({'preferences': []}, resp)
