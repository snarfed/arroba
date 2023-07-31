"""Unit tests for xrpc_sync.py."""
from carbox.car import Block, read_car

from arroba.repo import Action, Repo, Write
from arroba.storage import MemoryStorage
from arroba.util import next_tid
from arroba import xrpc_sync

from . import testutil


def load_checkout(blocks):
    """
    Args:
      root: CID
      blocks: sequence of Block

    Returns:
      dict mapping str path (collection/rkey) to JSON object
    """
    decoded = {b.cid: b.decoded for b in blocks}
    objs = {}

    for block in blocks:
        path = b''
        for entry in block.decoded.get('e', []):
            path = path[:entry['p']] + entry['k']
            objs[path.decode()] = decoded[entry['v']]

    return objs


class XrpcSyncTest(testutil.TestCase):

    def setUp(self):
        super().setUp()

        xrpc_sync.init(self.key)

        self.data = {}  # maps path to obj
        writes = []
        for coll in 'com.example.posts', 'com.example.likes':
            for rkey, obj in self.random_objects(5).items():
                writes.append(Write(Action.CREATE, coll, rkey, obj))
                self.data[f'{coll}/{rkey}'] = obj

        xrpc_sync.repo = xrpc_sync.repo.apply_writes(writes, self.key)

    def test_get_checkout(self):
        resp = xrpc_sync.get_checkout({}, did='did:web:user.com')
                                      # TODO
                                      # commit=xrpc_sync.repo.cid)
        roots, blocks = read_car(resp)
        self.assertEqual(self.data, load_checkout(blocks))

  # def test_sync_checkout_skips_existing_blocks(self):
  #   const commitPath = await storage.getCommitPath(repo.cid, null)
  #   if (!commitPath) {
  #     throw new Error('Could not get commitPath')
  #   }
  #   const hasGenesisCommit = await syncStorage.has(commitPath[0])
  #   expect(hasGenesisCommit).toBeFalsy()
  # })

  # it('does not sync duplicate blocks', async () => {
  #   const carBytes = await streamToBuffer(sync.getCheckout(storage, repo.cid))
  #   const car = await CarReader.fromBytes(carBytes)
  #   const cids = new CidSet()
  #   for await (const block of car.blocks()) {
  #     if (cids.has(block.cid)) {
  #       throw new Error(`duplicate block: :${block.cid.toString()}`)
  #     }
  #     cids.add(block.cid)
  #   }
  # })

  # it('throws on a bad signature', async () => {
  #   const badRepo = await util.addBadCommit(repo, keypair)
  #   const checkoutCar = await streamToBuffer(
  #     sync.getCheckout(storage, badRepo.cid),
  #   )
  #   await expect(
  #     sync.loadCheckout(syncStorage, checkoutCar, repoDid, keypair.did()),
  #   ).rejects.toThrow(RepoVerificationError)
  # })
