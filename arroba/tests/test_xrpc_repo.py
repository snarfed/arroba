"""Unit tests for xrpc_repo.py."""
from arroba import xrpc_repo

from . import testutil


class XrpcRepoTest(testutil.TestCase):

    def setUp(self):
        super().setUp()

    def test_create_record(self):
        pass

    def test_get_record(self):
        pass

    def test_delete_record(self):
        pass

    def test_list_records(self):
        pass

    def test_put_record(self):
        pass

    def test_describe_repo(self):
        with self.assertRaises(ValueError):
            xrpc_repo.describe_repo({}, repo='unknown')

        resp = xrpc_repo.describe_repo({}, repo='user.com')
        self.assertEqual('did:web:user.com', resp['did'])
        self.assertEqual('user.com', resp['handle'])

    def test_rebase_repo(self):
        pass

    def test_apply_writes(self):
        pass

    def test_upload_blob(self):
        pass
