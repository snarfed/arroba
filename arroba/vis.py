"""Visualizes an MST via GraphViz.

Usage: python -m arroba.vis
"""
import json
import os
from pathlib import Path
import subprocess

from multiformats import CID

from .mst import Leaf, MST
from .storage import Action, Block, CommitData, CommitOp, MemoryStorage


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
            key = f'{entry.key}\\n'
        else:
            assert isinstance(entry, MST)
            cid = entry.get_pointer()
            shape = 'circle'
            key = ''

        label = f'{key}…{cid.encode("base32")[-7:]}'
        out += f'{name(entry)} [shape = {shape}, label = "{label}"]\n'

        if isinstance(entry, MST):
            for child in entry.get_entries():
                out += f'{name(entry)} -> {name(child)}\n'

    out += '}'
    return out


if __name__ == '__main__':
    for case in json.load((Path(os.path.dirname(__file__))
                           / 'tests/testdata/commit-proof-fixtures.json'
                           ).open()):
        tree = MST.create(storage=MemoryStorage())
        val = CID.decode(case['leafValue'])

        # initial tree
        for key in case['keys']:
          tree = tree.add(key, val)

        filename = f'vis_{case.get("comment", "").replace(" ", "_").replace("-", "_")}'
        with open(f'{filename}_before.png', 'wb') as f:
            subprocess.run(['dot', '-Tpng'], input=render(tree).encode(), stdout=f)

        # new writes
        for key in case['adds']:
            tree = tree.add(key, val)

        for key in case['dels']:
            tree = tree.delete(key)

        with open(f'{filename}_after.png', 'wb') as f:
            subprocess.run(['dot', '-Tpng'], input=render(tree).encode(), stdout=f)
