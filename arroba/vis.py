"""Visualizes an MST via GraphViz.

Usage: python -m arroba.vis [-v]
"""
import json
import os
from pathlib import Path
import subprocess
import sys

from oauth_dropins.webutil.appengine_config import ndb_client
from multiformats import CID

from .mst import Data, Entry, Leaf, MST
from .storage import Action, Block, CommitData, CommitOp, MemoryStorage
from .datastore_storage import DatastoreStorage


# render CIDS as last 7 chars of base32, for readability
CID.__str__ = CID.__repr__ = lambda cid: '…' + cid.encode('base32')[-7:]

# base32 CIDs
red = []
green = []


def render(tree):
    """Outputs GraphViz to render an MST.

    Args:
      tree (mst.MST)

    Returns:
      str: GraphViz rendering
    """
    out = 'digraph {\ngraph [ordering = out]\n'

    def name(node):
        if isinstance(node, MST):
            return node.get_pointer().encode("base32")
        else:
            return f'{node.value.encode("base32")}_{node.key.replace("/", "_").replace(".", "_")}'

    def color(cid):
        cid32 = cid.encode('base32')
        return 'red' if cid32 in red else 'green' if cid32 in green else 'black'

    for entry in tree.walk():
        print('.', end='', flush=True)

        if isinstance(entry, Leaf):
            cid = entry.value
            shape = 'box'
            info = f'\\n{entry.key}'
        else:
            assert isinstance(entry, MST)
            cid = entry.get_pointer()
            shape = 'ellipse'
            info = ''

            if sys.argv[-1] == '-v':
                data = Data(**tree.storage.read(cid).decoded)
                info = f'\\nl {data.l}'

                children = []
                for child in data.e:
                    child = Entry(**child)
                    children.append(f'p {child.p} k {child.k.decode()} v {child.v} t {child.t}')

                children_str = '\\n'.join(children)
                info += f'\\n[ {children_str} ]'

        label = f'{info}…{cid.encode("base32")[-7:]}'
        out += f'{name(entry)} [shape={shape}, label="{cid}{info}", color="{color(cid)}"]\n'

        if isinstance(entry, MST):
            for child in entry.get_entries():
                child_cid = (child.get_pointer() if isinstance(child, MST)
                             else child.value)
                out += f'{name(entry)} -> {name(child)} [color="{color(child_cid)}"]\n'

    out += '}'
    return out


def render_from_prod():
    storage = DatastoreStorage(ndb_client=ndb_client)
    # after
    tree = MST.load(storage=storage, cid=CID.decode('zdpuAocGr6kaqXV8bFEWSMy3s73gzwYxZ1YPYgWeSkTpdijcX'))
    # before
    # tree = MST.load(storage=storage, cid=CID.decode('bafyreiggb6d4klqfkk732fx7htpmdptvwdntjzszhaainwl7qc3bi2apeu'))

    dot = render(tree)
    with open('vis.after.verbose.dot', 'w') as f:
        print(dot, file=f)

    with open('vis.after.verbose.svg', 'wb') as f:
        subprocess.run(['dot', '-Tsvg'], input=dot.encode(), stdout=f)


def render_test_cases:
    for case in json.load((Path(os.path.dirname(__file__))
                           / 'tests/testdata/commit-proof-fixtures.json'
                           ).open()):
        storage = MemoryStorage()
        tree = MST.create(storage=storage)
        val = CID.decode(case['leafValue'])

        # initial tree
        for key in case['keys']:
          tree = tree.add(key, val)

        _, blocks = tree.get_unstored_blocks()
        storage.write_blocks(blocks.values())

        filename = f'vis_{case.get("comment", "").replace(" ", "_").replace("-", "_")}'
        with open(f'{filename}_before.png', 'wb') as f:
            subprocess.run(['dot', '-Tpng'], input=render(tree).encode(), stdout=f)

        # new writes
        for key in case['adds']:
            tree = tree.add(key, val)

        for key in case['dels']:
            tree = tree.delete(key)

        _, blocks = tree.get_unstored_blocks()
        storage.write_blocks(blocks.values())

        with open(f'{filename}_after.png', 'wb') as f:
            subprocess.run(['dot', '-Tpng'], input=render(tree).encode(), stdout=f)


if __name__ == '__main__':
    render_test_cases()
