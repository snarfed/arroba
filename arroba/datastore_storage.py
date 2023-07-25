"""Google Cloud Datastore implementation of repo storage."""
import json

import dag_cbor.decoding, dag_cbor.encoding
from google.cloud import ndb
from multiformats import CID, multicodec, multihash

from arroba.storage import Storage
from arroba.util import dag_cbor_cid


class WriteOnce:
    """:class:`ndb.Property` mix-in, prevents changing it once it's set."""
    def _set_value(self, entity, value):
        existing = self._get_value(entity)
        if existing is not None and value != existing:
            raise ndb.ReadonlyPropertyError(f"{self._name} can't be changed")

        return super()._set_value(entity, value)


class WriteOnceJsonProperty(WriteOnce, ndb.TextProperty):
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


class WriteOnceBlobProperty(WriteOnce, ndb.BlobProperty):
    pass


class AtpNode(ndb.Model):
    """A data record, MST node, or commit.

    Key name is the DAG-CBOR base32 CID of the data.

    Properties:
    * data: JSON-decoded DAG-JSON value of this node
    """
    data = WriteOnceJsonProperty(required=True)
    dag_cbor = WriteOnceBlobProperty(required=True)

    @staticmethod
    def create(data):
        """Writes a new AtpNode to the datastore.

        Args:
          data: dict value

        Returns:
          :class:`AtpNode`
        """
        encoded = dag_cbor.encoding.encode(data)
        digest = multihash.digest(encoded, 'sha2-256')
        cid = CID('base58btc', 1, multicodec.get('dag-cbor'), digest)

        node = AtpNode(id=cid.encode('base32'), data=data, dag_cbor=encoded)
        node.put()
        return node


class DatastoreStorage(Storage):
    """Google Cloud Datastore implementation of :class:`Storage`.

    See :class:`Storage` for method details
    """
    def read(self, cid):
        node = AtpNode.get_by_id(cid)
        if node:
            return node.data

    def read_many(self, cids):
        keys = [ndb.Key(AtpNode, cid) for cid in cids]
        got = list(zip(cids, ndb.get_multi(keys)))
        found = {CID.decode(node.key.id()): node.dag_cbor
                 for _, node in got if node is not None}
        missing = [cid for cid, node in got if node is None]
        return found, missing

    def read_blocks(self, cids):
        pass

    def has(self, cid):
        return self.read(cid) is not None

    def write(self, node):
        return AtpNode.create(node).key.id()

    def apply_commit(self, commit_data):
        pass
