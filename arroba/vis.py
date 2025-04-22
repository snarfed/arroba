"""Visualizes an MST via GraphViz.

Usage: python -m arroba.vis [-v]
"""
import json
import os
from pathlib import Path
import subprocess
import sys

from multiformats import CID

from .mst import Data, Entry, Leaf, MST
from .storage import Action, Block, CommitData, CommitOp, MemoryStorage


# render CIDS as last 7 chars of base32, for readability
CID.__str__ = CID.__repr__ = lambda cid: '…' + cid.encode('base32')[-7:]


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
            return f'{node.value.encode("base32")}_{node.key.replace("/", "_")}'

    for entry in tree.walk():
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
                    children.append(f'{child.p} of k {child.k.decode()} v {child.v} t {child.t}')

                children_str = '\\n'.join(children)
                info += f'\\n[ {children_str} ]'

        label = f'{key}…{cid.encode("base32")[-7:]}'
        out += f'{name(entry)} [shape = {shape}, label = "{cid}{info}"]\n'

        if isinstance(entry, MST):
            for child in entry.get_entries():
                out += f'{name(entry)} -> {name(child)}\n'

    out += '}'
    return out


if __name__ == '__main__':
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
