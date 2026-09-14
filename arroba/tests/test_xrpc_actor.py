"""Unit tests for xrpc_actor.py."""
from .. import xrpc_actor
from . import testutil


class XrpcActorTest(testutil.XrpcTestCase):

    def test_get_preferences(self):
        resp = xrpc_actor.get_preferences({})
        self.assertEqual({'preferences': []}, resp)

    def test_put_preferences(self):
        resp = xrpc_actor.put_preferences({
            'preferences': [{
                '$type': 'app.bsky.actor.defs#adultContentPref',
                'enabled': True,
            }],
        })
        self.assertIsNone(resp)

        # it's a stub, so it doesn't store anything
        self.assertEqual({'preferences': []}, xrpc_actor.get_preferences({}))
