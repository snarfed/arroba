"""Bluesky / AT Protocol Merkle search tree implementation.

* https://atproto.com/guides/data-repos
* https://atproto.com/lexicons/com-atproto-sync
* https://hal.inria.fr/hal-02303490/document

Heavily based on:
https://github.com/bluesky-social/atproto/blob/main/packages/repo/src/mst/mst.ts

Huge thanks to the Bluesky team for working in the public, in open source, and to
Daniel Holmgren and Devin Ivy for this code specifically!

From that file:

This is an implementation of a Merkle Search Tree (MST)
The data structure is described here: https://hal.inria.fr/hal-02303490/document
The MST is an ordered, insert-order-independent, deterministic tree.
Data keys are laid out in alphabetic order.
The key insight of an MST is that each key is hashed and starting 0s are counted
to determine which layer it falls on (5 zeros for ~32 fanout).
This is a merkle tree, so each subtree is referred to by it's hash (CID).
When a leaf is changed, ever tree on the path to that leaf is changed as well,
thereby updating the root hash.

For atproto, we use SHA-256 as the key hashing algorithm, and ~4 fanout
(2-bits of zero per layer).

A couple notes on CBOR encoding:

There are never two neighboring subtrees.
Therefore, we can represent a node as an array of
leaves & pointers to their right neighbor (possibly null),
along with a pointer to the left-most subtree (also possibly null).

Most keys in a subtree will have overlap.
We do compression on prefixes by describing keys as:
* the length of the prefix that it shares in common with the preceding key
* the rest of the string

For example:

If the first leaf in a tree is ``bsky/posts/abcdefg`` and the second is
``bsky/posts/abcdehi``, then the first will be described as ``prefix: 0, key:
'bsky/posts/abcdefg'``, and the second will be described as ``prefix: 16, key:
'hi'``.
"""
from collections import namedtuple
import copy
from hashlib import sha256
import logging
from os.path import commonprefix
import re

import dag_cbor
from multiformats import CID

from .storage import Block, Storage
from .util import dag_cbor_cid

logger = logging.getLogger(__name__)

# this is treeEntry in mst.ts
Entry = namedtuple('Entry', [
    'p',  # int, length of prefix that this data key shares with the prev data key
    'k',  # bytes, the rest of the data key outside the shared prefix
    'v',  # str CID, value
    't',  # str CID, next subtree (to the right of leaf), or None
])

Data = namedtuple('Data', [
    'l',  # str CID, left-most subtree, or None
    'e',  # list of Entry
])

Leaf = namedtuple('Leaf', [
    'key',    # str, data key (collection + record key aka rkey)
    'value',  # CID
])


class MST:
    """Merkle search tree class.

    Attributes:
      storage (Storage):
      entries (sequence of MST and Leaf)
      layer (int): this MST's layer in the root MST
      pointer (CID):
      outdated_pointer (bool): whether pointer needs to be recalculated
    """
    storage = None
    entries = None
    layer = None
    pointer = None
    outdated_pointer = False

    def __init__(self, *, storage=None, entries=None, pointer=None, layer=None):
        """Constructor.

        Args:
          storage (Storage)
          entries (sequence of MST and Leaf)
          pointer (CID)
          layer (int)

        Returns:
          MST:
        """
        self.storage = storage
        self.entries = entries
        self.pointer = pointer
        self.layer = layer

    @classmethod
    def load(cls, *, storage=None, cid=None):
        return MST(storage=storage, entries=None, pointer=cid, layer=None)

    @classmethod
    def create(cls, *, storage=None, entries=None, layer=None):
        """

        Args:
          storage (Storage)
          entries (sequence of MST and Leaf)
          layer (int)

        Returns:
          MST
        """
        if not entries:
            entries = []
        pointer = cid_for_entries(entries)
        return MST(storage=storage, entries=entries, pointer=pointer, layer=layer)

#     def from_data(storage, data, opts):
#         """
#         Returns:
#           MST:
#         """
#         entries = deserialize_node_data(data)
#         pointer = cid_for_cbor(data)
#         return MST(entries=entries, pointer=pointer)

    def __eq__(self, other):
        if isinstance(other, MST):
            return self.get_pointer() == other.get_pointer()

    def __unicode__(self):
        return f'MST with pointer {self.get_pointer()}'

    def __repr__(self):
        return f'MST(storage={self.storage}, entries=..., pointer={self.get_pointer()}, layer={self.get_layer()})'

    # Immutability
    # -------------------
    def new_tree(self, entries):
        """We never mutate an MST, we just return a new MST with updated values.

        Args:
            entries (sequence of MST and Leaf)

        Returns:
            MST:
        """
        mst = MST(storage=self.storage, entries=entries, pointer=self.pointer,
                  layer=self.layer)
        mst.outdated_pointer = True
        return mst


#     Getters (lazy load)
#     -------------------

    def get_entries(self):
        """

        We don't want to load entries of every subtree, just the ones we need.

        Returns:
          sequence of MST and Leaf:
        """
        if self.entries is not None:
            return copy.copy(self.entries)

        if self.pointer:
            data = Data(**self.storage.read(self.pointer).decoded)
            first_leaf = layer = None
            if data.e:
                layer = leading_zeros_on_hash(data.e[0]['k'])

            self.entries = deserialize_node_data(storage=self.storage, data=data,
                                                 layer=layer)
            return self.entries

        raise RuntimeError('No entries or CID provided')

    def get_pointer(self):
        """Returns this MST's root CID pointer. Calculates it if necessary.

        We don't hash the node on every mutation for performance reasons.
        Instead we keep track of whether the pointer is outdated and only
        (recursively) calculate when needed.

        Returns:
          CID:
        """
        if not self.outdated_pointer:
            return self.pointer

        outdated = False
        entries = self.get_entries()
        for e in entries:
            if isinstance(e, MST) and e.outdated_pointer:
                outdated = True
                e.get_pointer()

        if outdated:
            entries = self.get_entries()

        self.pointer = cid_for_entries(entries)
        self.outdated_pointer = False
        return self.pointer

    def get_layer(self):
        """Returns this MST's layer, and sets ``self.layer``.

        In most cases, we get the layer of a node from a hint on creation. In the
        case of the topmost node in the tree, we look for a key in the node &
        determine the layer. In the case where we don't find one, we recurse down
        until we do. If we still can't find one, then we have an empty tree and the
        node is layer 0.

        Returns:
          int:
        """
        self.layer = self.attempt_get_layer()
        if self.layer is None:
            self.layer = 0

        return self.layer

    def attempt_get_layer(self):
        """Returns this MST's layer, and sets ``self.layer``.

        Returns:
          int or None:
        """
        if self.layer is not None:
            return self.layer

        entries = self.get_entries()
        layer = layer_for_entries(entries)
        if layer is None:
            for entry in entries:
                if isinstance(entry, MST):
                    child_layer = entry.attempt_get_layer()
                    if child_layer is not None:
                        layer = child_layer + 1
                        break

        if layer is not None:
            self.layer = layer

        return layer


    # Core functionality
    # -------------------

    def get_unstored_blocks(self):
        """Return the necessary blocks to persist the MST to repo storage.

        Returns:
          (CID root, dict mapping CID to Block) tuple:
        """
        unstored = {}
        pointer = self.get_pointer()

        if self.storage.has(pointer):
            return pointer, unstored

        entries = self.get_entries()
        data = serialize_node_data(entries)
        block = Block(decoded=data._asdict())
        unstored[block.cid] = block

        for entry in entries:
            if isinstance(entry, MST):
                _, blocks = entry.get_unstored_blocks()
                unstored.update(blocks)

        return pointer, unstored

    def add(self, key, value=None, known_zeros=None):
        """Adds a new leaf for the given key/value pair.

        Args:
          key (str)
          value (CID)
          known_zeros (int)

        Returns:
          MST:

        Raises:
          ValueError: if a leaf with that key already exists
        """
        ensure_valid_key(key)
        key_zeros = known_zeros or leading_zeros_on_hash(key)
        layer = self.get_layer()
        new_leaf = Leaf(key=key, value=value)

        if key_zeros == layer:
            # it belongs in self layer
            index = self.find_gt_or_equal_leaf_index(key)
            found = self.at_index(index)
            if isinstance(found, Leaf) and found.key == key:
                raise ValueError(f'There is already a value at key: {key}')
            prev_node = self.at_index(index - 1)
            if not prev_node or isinstance(prev_node, Leaf):
                # if entry before is a leaf, (or we're on far left) we can just splice in
                return self.splice_in(new_leaf, index)
            else:
                # else we try to split the subtree around the key
                left, right = prev_node.split_around(key)
                return self.replace_with_split(index - 1, left, new_leaf, right)

        elif key_zeros < layer:
            # it belongs on a lower layer
            index = self.find_gt_or_equal_leaf_index(key)
            prev_node = self.at_index(index - 1)
            if prev_node and isinstance(prev_node, MST):
                # if entry before is a tree, we add it to that tree
                new_subtree = prev_node.add(key, value, key_zeros)
                return self.update_entry(index - 1, new_subtree)
            else:
                sub_tree = self.create_child()
                new_subtree = sub_tree.add(key, value, key_zeros)
                return self.splice_in(new_subtree, index)

        else:  # key_zeros > layer
            # it belongs on a higher layer, push the rest of the tree down
            left, right = self.split_around(key)
            # if the newly added key has >=2 more leading zeros than the current
            # highest layer then we need to add structural nodes between as well
            layer = self.get_layer()
            extra_layers_to_add = key_zeros - layer
            # intentionally starting at 1, first layer is taken care of by split
            for i in range(1, extra_layers_to_add):
                if left:
                    left = left.create_parent()
                if right:
                    right = right.create_parent()

            updated = []
            if left:
                updated.append(left)
            updated.append(Leaf(key=key, value=value))
            if right:
                updated.append(right)

            new_root = MST.create(storage=self.storage, entries=updated, layer=key_zeros)
            new_root.outdated_pointer = True
            return new_root

    def get(self, key):
        """Gets the value at the given key.

        Args:
          key (str)

        Returns:
          CID or None:
        """
        index = self.find_gt_or_equal_leaf_index(key)
        found = self.at_index(index)
        if found and isinstance(found, Leaf) and found.key == key:
            return found.value

        prev = self.at_index(index - 1)
        if prev and isinstance(prev, MST):
            return prev.get(key)

    def update(self, key, value):
        """Edits the value at the given key.

        Args:
          key (str)
          value (CID)

        Returns:
          MST:

        Raises:
          KeyError: if key doesn't exist
        """
        ensure_valid_key(key)

        index = self.find_gt_or_equal_leaf_index(key)
        found = self.at_index(index)
        if found and isinstance(found, Leaf) and found.key == key:
            return self.update_entry(index, Leaf(key=key, value=value))

        prev = self.at_index(index - 1)
        if prev and isinstance(prev, MST):
            updated_tree = prev.update(key, value)
            return self.update_entry(index - 1, updated_tree)

        raise KeyError(f'Could not find a record with key: {key}')

    def delete(self, key):
        """Deletes the value at the given key.

        Args:
          key (str)

        Returns:
          MST

        Raises:
          KeyError: if key doesn't exist
        """
        return self.delete_recurse(key).trim_top()

    def delete_recurse(self, key):
        """Deletes the value and subtree, if any, at the given key.

        Args:
          key (str):

        Returns:
          MST
        """
        index = self.find_gt_or_equal_leaf_index(key)
        found = self.at_index(index)

        # if found, remove it on self level
        if isinstance(found, Leaf) and found.key == key:
            prev = self.at_index(index - 1)
            next = self.at_index(index + 1)
            if isinstance(prev, MST) and isinstance(next, MST):
                merged = prev.append_merge(next)
                return self.new_tree(
                    self.slice(0, index - 1) + [merged] + self.slice(index + 2)
                )
            else:
                return self.remove_entry(index)

        # else recurse down to find it
        prev = self.at_index(index - 1)
        if isinstance(prev, MST):
            subtree = prev.delete_recurse(key)
            if subtree.get_entries():
                return self.update_entry(index - 1, subtree)
            else:
                return self.remove_entry(index - 1)

        raise KeyError(f'Could not find a record with key: {key}')


#     Simple Operations
#     -------------------

    def update_entry(self, index, entry):
        """Updates an entry in place.

        Args:
          index (int)
          entry (MST or Leaf)

        Returns:
          MST:
        """
        return self.new_tree(
            entries=self.slice(0, index) + [entry] + self.slice(index + 1))

    def remove_entry(self, index):
        """Removes the entry at a given index.

        Args:
          index (int)

        Returns:
          MST:
        """
        return self.new_tree(entries=self.slice(0, index) + self.slice(index + 1))

    def append(self, entry):
        """Appends an entry to the end of the node.

        Args:
          entry (MST or Leaf)

        Returns:
          MST:
        """
        return self.new_tree(self.get_entries() + [entry])

    def prepend(self, entry):
        """Prepends an entry to the start of the node.

        Args:
          entry (MST or Leaf)

        Returns:
          MST:
        """
        return self.new_tree([entry] + self.get_entries())

    def at_index(self, index):
        """Returns the entry at a given index.

        Args:
          index (int)

        Returns:
          MST or Leaf or None:
        """
        entries = self.get_entries()
        if 0 <= index < len(entries):
            return entries[index]

    def slice(self, start=None, end=None):
        """Returns a slice of this node.

        Args:
          start (int): optional, inclusive
          end (int): optional, exclusive

        Returns:
          sequence of MST and Leaf:
        """
        return self.get_entries()[start:end]

    def splice_in(self, entry, index):
        """Inserts an entry at a given index.

        Args:
          entry (MST or Leaf)
          index (int)

        Returns:
          MST:
        """
        return self.new_tree(self.slice(0, index) + [entry] + self.slice(index))

    def replace_with_split(self, index, left=None, leaf=None, right=None):
        """Replaces an entry with [ Maybe(tree), Leaf, Maybe(tree) ].

        Args:
          index (int):
          left (MST or Leaf):
          leaf (Leaf):
          right (MST or Leaf):

        Returns:
          MST:
        """
        updated = self.slice(0, index)
        if left:
            updated.append(left)
        updated.append(leaf)
        if right:
            updated.append(right)
        updated.extend(self.slice(index + 1))
        return self.new_tree(updated)

    def trim_top(self):
        """Trims the top and return its subtree, if necessary.

        Only if the topmost node in the tree only points to another tree.
        Otherwise, does nothing.

        Returns:
          MST:
        """
        entries = self.get_entries()
        if len(entries) == 1 and isinstance(entries[0], MST):
            return entries[0].trim_top()
        else:
            return self


#     Subtree & Splits
#     -------------------

    def split_around(self, key):
        """Recursively splits a subtree around a given key.

        Args:
          key (str)

        Returns:
          (MST or None, MST or None) tuple:
        """
        index = self.find_gt_or_equal_leaf_index(key)
        # split tree around key
        left_data = self.slice(0, index)
        right_data = self.slice(index)
        left = self.new_tree(left_data)
        right = self.new_tree(right_data)

        # if the far right of the left side is a subtree,
        # we need to split it on the key as well
        last_in_left = left_data[-1] if left_data else None
        if isinstance(last_in_left, MST):
            left = left.remove_entry(len(left_data) -1)
            split = last_in_left.split_around(key)
            if split[0]:
                left = left.append(split[0])
            if split[1]:
                right = right.prepend(split[1])

        return [
            left if left.get_entries() else None,
            right if right.get_entries() else None,
        ]

    def append_merge(self, to_merge):
        """Merges another tree with this one.

        The simple merge case where every key in the right tree is greater than
        every key in the left tree. Used primarily for deletes.

        Args:
          to_merge (MST)

        Returns:
          MST:
        """
        assert self.get_layer() == to_merge.get_layer(), \
            'Trying to merge two nodes from different layers of the MST'

        self_entries = self.get_entries()
        to_merge_entries = to_merge.get_entries()
        last_in_left = self_entries[-1]
        first_in_right = to_merge_entries[0]

        if isinstance(last_in_left, MST) and isinstance(first_in_right, MST):
            merged = last_in_left.append_merge(first_in_right)
            return self.new_tree(
                list(self_entries[:-1]) + [merged] + to_merge_entries[1:])
        else:
            return self.new_tree(self_entries + to_merge_entries)


    # Create relatives
    # -------------------

    def create_child(self):
        """
        Returns:
          MST:
        """
        return MST.create(storage=self.storage, entries=[],
                          layer=self.get_layer() - 1)

    def create_parent(self):
        """
        Returns:
          MST:
        """
        parent = MST.create(storage=self.storage, entries=[self],
                            layer=self.get_layer() + 1)
        parent.outdated_pointer = True
        return parent


#     Finding insertion points
#     -------------------

    def find_gt_or_equal_leaf_index(self, key):
        """Finds the index of the first leaf node greater than or equal to value.

        Args:
          key (str)

        Returns:
          int:
        """
        entries = self.get_entries()
        for i, entry in enumerate(entries):
            if isinstance(entry, Leaf) and entry.key >= key:
                return i

        # if we can't find it, we're on the end
        return len(entries)


#     List operations (partial tree traversal)
#     -------------------

    def walk_leaves_from(self, key):
        """Walk tree starting at key.

        Generator for leaves in the tree, starting at a given key.

        Args:
          key (str):

        Generates:
          Leaf
        """
        index = self.find_gt_or_equal_leaf_index(key)
        entries = self.get_entries()

        if index > 0:
            prev = entries[index - 1]
            if prev and isinstance(prev, MST):
                for e in prev.walk_leaves_from(key):
                    yield e

        for entry in entries[index:]:
            if isinstance(entry, Leaf):
                yield entry
            else:
                for e in entry.walk_leaves_from(key):
                    yield e

    def list(self, after=None, before=None):
        """Returns entries, optionally bounded within a key range.

        Args:
          after (str): key, optional
          before (str): key, optional

        Returns:
          sequence of Leaf:
        """
        vals = []

        for leaf in self.walk_leaves_from(after or ''):
            if leaf.key == after:
                continue
            if before and leaf.key >= before:
                break
            vals.append(leaf)

        return vals

    def list_with_prefix(self, prefix):
        """Returns entries with a given key prefix.

        Args:
          prefix (str): key prefix

        Returns:
          sequence of Leaf
        """
        vals = []

        for leaf in self.walk_leaves_from(prefix):
            if not leaf.key.startswith(prefix):
                break
            vals.append(leaf)

        return vals

#     Full tree traversal
#     -------------------

    def walk(self):
        """Walk full tree, depth first, and emit nodes.

        Returns:
          generator of MST and Leaf:
        """
        yield self

        for entry in self.get_entries():
            if isinstance(entry, MST):
                for e in entry.walk():
                    yield e
            else:
                yield entry

#     Walk full tree & emit nodes, consumer can bail at any point by returning False
#     def paths():
#     """
#     Returns:
#       sequence of MST and Leaf
#     """
#         paths = []
#         for entry in self.get_entries():
#             if isinstance(entry, Leaf):
#                 paths.append([entry])
#             if isinstance(entry, MST):
#                 sub_paths = entry.paths()
#                 paths.extend([entry] + p for p in sub_paths)
#
#         return paths

    def all_nodes(self):
        """Walks the tree and returns all nodes.

        Returns:
          sequence of MST and Leaf:
        """
        return list(self.walk())

#     Walks tree & returns all cids
#     def all_cids():
#     """
#     Returns:
#       CidSet
#     """
#         cids = CidSet()
#         for entry in self.get_entries():
#             if isinstance(entry, Leaf):
#                 cids.add(entry.value)
#             else:
#                 subtree_cids = entry.all_cids()
#                 cids.add_set(subtree_cids)
#         cids.add(self.get_pointer())
#         return cids

    def leaves(self):
        """Walks tree and returns all leaves.

        Returns:
          sequence of Leaf:
        """
        return [entry for entry in self.walk() if isinstance(entry, Leaf)]

    def leaf_count(self):
        """Returns the total number of leaves in this MST.

        Returns:
          int:
        """
        return len(self.leaves())


#     Reachable tree traversal
#     -------------------

    # Walk reachable branches of tree & emit nodes, consumer can bail at any
    # point by returning False

#     def walk_reachable(): AsyncIterable<NodeEntry>:
#         yield self
#         for entry in self.get_entries():
#             if isinstance(entry, MST):
#                 try:
#                     for e in entry.walk_reachable():
#                         yield e
#                 catch (err):
#                     if err instanceof MissingBlockError:
#                         continue
#                     else:
#                         raise err
#             else:
#                 yield entry

#     def reachable_leaves():
#     """
#     Returns:
#       Leaf[]
#     """
#         leaves: Leaf[] = []
#         for entry in self.walk_reachable():
#             if isinstance(entry, Leaf):
#                 leaves.append(entry)
#         return leaves

#     Sync Protocol

    def load_all(self, start=0):
        """Generator. Used in :func:`xrpc_sync.get_repo`.

        (The bluesky-social/atproto TS code calls this ``writeToCarStream``.)

        Args:
          start (int): optional ``subscribeRepos`` sequence number to start from,
            inclusive. Defaults to 0.

        Returns:
          generator of (CID, bytes) tuples
        """
        leaves = set()   # CIDs
        to_fetch = set() # CIDs

        pointer = self.get_pointer()
        assert pointer
        to_fetch.add(pointer)

        while to_fetch:
            blocks = self.storage.read_many(to_fetch)
            to_fetch.clear()

            for cid, block in blocks.items():
                if block.seq < start:
                    continue

                yield cid, block.encoded
                entries = deserialize_node_data(storage=self.storage,
                                                data=Data(**block.decoded))

                for entry in entries:
                    if isinstance(entry, Leaf):
                        leaves.add(entry.value)
                    else:
                        to_fetch.add(entry.get_pointer())

        leaf_blocks = self.storage.read_many(leaves)
        for cid, block in leaf_blocks.items():
            yield cid, block.encoded

#     def cids_for_path(self, key):
#         """Returns the CIDs in a given key path. ???
#
#         Args:
#           key (str):
#
#         Returns:
#           sequence of :class:`CID`
#         """
#         cids: CID[] = [self.get_pointer()]
#         index = self.find_gt_or_equal_leaf_index(key)
#         found = self.at_index(index)
#         if found and isinstance(found, Leaf) and found.key == key:
#             return cids + [found.value]
#         prev = self.at_index(index - 1)
#         if prev and isinstance(prev, MST):
#             return cids + prev.cids_for_path(key)
#         return cids


def leading_zeros_on_hash(key):
    """Returns the number of leading zeros in a key's hash.

    Args:
      key (str or bytes)

    Returns:
      int:
    """
    if not isinstance(key, bytes):
        key = key.encode()  # ensure_valid_key enforces that this is ASCII only

    leading_zeros = 0
    for byte in sha256(key).digest():
        if byte < 64:
             leading_zeros += 1
        if byte < 16:
             leading_zeros += 1
        if byte < 4:
             leading_zeros += 1
        if byte == 0:
            leading_zeros += 1
        else:
            break

    return leading_zeros


def layer_for_entries(entries):
    """
    Args:
      entries (MST or Leaf)

    Returns:
      int or None:
    """
    for entry in entries:
        if isinstance(entry, Leaf):
            return leading_zeros_on_hash(entry.key)


def deserialize_node_data(*, storage=None, data=None, layer=None):
    """
    Args:
      storage (Storage)
      data (Data)

    Returns:
      sequence of MST and Leaf:
    """
    entries = []
    if (data.l is not None):
        entries.append(MST(storage=storage, pointer=data.l,
                           layer=layer - 1 if layer else None))

    last_key = ''
    for entry_data in data.e:
        entry = Entry(**entry_data)
        key_str = entry.k.decode()
        key = last_key[:entry.p] + key_str
        ensure_valid_key(key)
        entries.append(Leaf(key, entry.v))
        last_key = key
        if entry.t is not None:
            entries.append(MST(storage=storage, pointer=entry.t,
                               layer=layer - 1 if layer else None))

    return entries


def serialize_node_data(entries):
    """
    Args:
      entries (sequence of MST and Leaf)

    Returns:
      Data:
    """
    l = None
    i = 0
    if entries and isinstance(entries[0], MST):
        i += 1
        l = entries[0].get_pointer()

    data = Data(l=l, e=[])
    last_key = ''
    while i < len(entries):
        leaf = entries[i]
        next = entries[i + 1] if i < len(entries) - 1 else None

        if not isinstance(leaf, Leaf):
            raise ValueError('Not a valid node: two subtrees next to each other')
        i += 1

        subtree = None
        if next and isinstance(next, MST):
            subtree = next.get_pointer()
            i += 1

        ensure_valid_key(leaf.key)
        prefix_len = common_prefix_len(last_key, leaf.key)
        data.e.append(Entry(
            p=prefix_len,
            k=leaf.key[prefix_len:].encode('ascii'),
            v=leaf.value,
            t=subtree,
        )._asdict())

        last_key = leaf.key

    return data


def common_prefix_len(a, b):
    """
    Args:
      a (str)
      b (str)

    Returns:
      int:
    """
    return len(commonprefix((a, b)))


def cid_for_entries(entries):
    """
    Args:
      entries (sequence of MST and Leaf)

    Returns:
      CID
    """
    return dag_cbor_cid(serialize_node_data(entries)._asdict())


def ensure_valid_key(key):
    """
    Args:
      key (str)

    Raises:
      ValueError: if key is not a valid MST key
    """
    valid = re.compile('[a-zA-Z0-9_\-:.]*$')
    split = key.split('/')
    if not (len(key) <= 256 and
            len(split) == 2 and
            split[0] and
            split[1] and
            valid.match(split[0]) and
            valid.match(split[1])
            ):
        raise ValueError(f'Invalid MST key: {key}')


WalkStatus = namedtuple('WalkStatus', [
    'done',     # bool
    'cur',      # MST or Leaf
    'walking',  # MST or None if cur is the root of the tree
    'index',    # int
], defaults=[None, None, None, None])


class Walker:
    """Allows walking an MST manually.

    Attributes:
      stack (sequence of WalkStatus)
      status (WalkStatus): current
    """
    stack = None
    status = None

    def __init__(self, tree):
        """Constructor.

        Args:
          tree (MST)
        """
        self.stack = []
        self.status = WalkStatus(
            done=False,
            cur=tree,
            walking=None,
            index=0,
        )

    def layer(self):
        """Returns the curent layer of the node we're on."""
        assert not self.status.done, 'Walk is done'

        if self.status.walking:
            return self.status.walking.layer or 0

        # if cur is the root of the tree, add 1
        if isinstance(self.status.cur, MST):
            return (self.status.cur.layer or 0) + 1

        raise RuntimeError('Could not identify layer of walk')


    def step_over(self):
        """Moves to the next node in the subtree, skipping over the subtree."""
        if self.status.done:
            return

        # if stepping over the root of the node, we're done
        if not self.status.walking:
            self.status = WalkStatus(done=True)
            return

        entries = self.status.walking.get_entries()
        self.status = self.status._replace(index=self.status.index + 1)

        if self.status.index >= len(entries):
            if not self.stack:
                self.status = WalkStatus(done=True)
            else:
                self.status = self.stack.pop()
                self.step_over()
        else:
            self.status = self.status._replace(cur=entries[self.status.index])

    def step_into(self):
        """Steps into a subtree.

        Raises:
          RuntimeError: if curently on a leaf
        """
        if self.status.done:
            return

        # edge case for very start of walk
        if not self.status.walking:
            assert isinstance(self.status.cur, MST), \
                'The root of the tree cannot be a leaf'
            next = self.status.cur.at_index(0)
            if not next:
                self.status = WalkStatus(done=True)
            else:
                self.status = WalkStatus(
                    done=False,
                    walking=self.status.cur,
                    cur=next,
                    index=0,
                )
            return

        if not isinstance(self.status.cur, MST):
            raise RuntimeError('No tree at pointer, cannot step into')

        next = self.status.cur.at_index(0)
        assert next, 'Tried to step into a node with 0 entries which is invalid'

        self.stack.append(self.status)
        self.status = WalkStatus(
            walking=self.status.cur,
            cur=next,
            index=0,
            done=False,
        )

    def advance(self):
        """Advances to the next node in the tree.

        Steps into the curent node if necessary.
        """
        if self.status.done:
            return

        if isinstance(self.status.cur, Leaf):
            self.step_over()
        else:
            self.step_into()
