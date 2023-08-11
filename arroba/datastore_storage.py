"""Google Cloud Datastore implementation of repo storage."""
import json

import dag_cbor
import dag_json
from google.cloud import ndb
from multiformats import CID, multicodec, multihash

from .storage import BlockMap, Storage
from .util import dag_cbor_cid


class WriteOnce:
    """:class:`ndb.Property` mix-in, prevents changing it once it's set."""
    def _set_value(self, entity, value):
        existing = self._get_value(entity)
        if existing is not None and value != existing:
            raise ndb.ReadonlyPropertyError(f"{self._name} can't be changed")

        return super()._set_value(entity, value)


class JsonProperty(ndb.TextProperty):
    """Fork of ndb's that subclasses TextProperty instead of BlobProperty.

    This makes values show up as normal, human-readable, serialized JSON in the
    web console.
    https://github.com/googleapis/python-ndb/issues/874#issuecomment-1442753255

    Duplicated in oauth-dropins/webutil:
    https://github.com/snarfed/webutil/blob/main/models.py
    """
    def _validate(self, value):
        if not isinstance(value, dict):
            raise TypeError('JSON property must be a dict')

    def _to_base_type(self, value):
        as_str = json.dumps(value, separators=(',', ':'), ensure_ascii=True)
        return as_str.encode('ascii')

    def _from_base_type(self, value):
        if not isinstance(value, str):
            value = value.decode('ascii')
        return json.loads(value)


class ComputedJsonProperty(JsonProperty, ndb.ComputedProperty):
    """Custom ComputedProperty for JSON values that stores them as strings.

    ...instead of like StructuredProperty, with "entity" type, which bloats them
    unnecessarily in the datastore.
    """
    def __init__(self, *args, **kwargs):
        kwargs['indexed'] = False
        super().__init__(*args, **kwargs)


class WriteOnceBlobProperty(WriteOnce, ndb.BlobProperty):
    pass


class AtpNode(ndb.Model):
    """A data record, MST node, or commit.

    Key name is the DAG-CBOR base32 CID of the data.

    Properties:
    * dag_cbor: bytes, DAG-CBOR encoded value
    * data: dict, DAG-JSON value, only used for human debugging
    """
    dag_cbor = WriteOnceBlobProperty(required=True)

    @ComputedJsonProperty
    def data(self):
        return json.loads(dag_json.encode(dag_cbor.decode(self.dag_cbor)))

    @staticmethod
    def create(data):
        """Writes a new AtpNode to the datastore.

        Args:
          data: dict value

        Returns:
          :class:`AtpNode`
        """
        encoded = dag_cbor.encode(data)
        digest = multihash.digest(encoded, 'sha2-256')
        cid = CID('base58btc', 1, multicodec.get('dag-cbor'), digest)

        node = AtpNode(id=cid.encode('base32'), dag_cbor=encoded)
        node.put()
        return node


class DatastoreStorage(Storage):
    """Google Cloud Datastore implementation of :class:`Storage`.

    See :class:`Storage` for method details
    """
    def read(self, cid):
        node = AtpNode.get_by_id(cid.encode('base32'))
        if node:
            return dag_cbor.decode(node.dag_cbor)

    def read_many(self, cids):
        found, missing = self._read_nodes(cids)
        found_objs = {cid: dag_cbor.decode(node.dag_cbor)
                      for cid, node in found.items()}
        return found_objs, missing

    def read_blocks(self, cids):
        found, missing = self._read_nodes(cids)
        blocks = BlockMap((cid, node.dag_cbor) for cid, node in found.items())
        return blocks, missing

    def _read_nodes(self, cids):
        """Internal helper, loads AtpNodes for a set of cids.

        Args:
          cids: sequence of :class:`CID`

        Returns:
          tuple, (dict mapping found CIDs to AtpNodes, list of CIDs not found)
        """
        keys = [ndb.Key(AtpNode, cid.encode('base32')) for cid in cids]
        got = list(zip(cids, ndb.get_multi(keys)))
        found = {CID.decode(node.key.id()): node
                 for _, node in got if node is not None}
        missing = [cid for cid, node in got if node is None]
        return found, missing

    def has(self, cid):
        return self.read(cid) is not None

    def write(self, node):
        return CID.decode(AtpNode.create(node).key.id())

    def apply_commit(self, commit):
        ndb.put_multi(AtpNode(id=cid.encode('base32'), dag_cbor=block)
                      for cid, block in commit.blocks.items())
        self.head = commit.cid
