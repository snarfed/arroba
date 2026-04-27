#!/usr/bin/env python3
"""Benchmark for com.atproto.sync.getRepo.

Two subcommands:

  load   Reset the Datastore emulator and create repos of three sizes.
  bench  Call getRepo multiple times per repo and report timing breakdowns.

Usage:
    local/bin/python scripts/getRepo_benchmark.py load
    local/bin/python scripts/getRepo_benchmark.py bench [--runs N]

Requires the Datastore emulator running at localhost:8089:
    gcloud beta emulators datastore start --consistency=1.0 --no-store-on-disk
"""
import argparse
import humanize
import logging
import os
import sys
import time

# must be set before arroba imports so PROFILE_GETREPO constant is True
os.environ['PROFILE_GETREPO'] = '1'
os.environ.setdefault('DATASTORE_EMULATOR_HOST', 'localhost:8089')
os.environ.setdefault('CLOUDSDK_CORE_PROJECT', 'app')
os.environ.setdefault('DATASTORE_DATASET', 'app')
os.environ.setdefault('GOOGLE_CLOUD_PROJECT', 'app')
os.environ['GRPC_VERBOSITY'] = 'ERROR'

import requests
from google.auth.credentials import AnonymousCredentials
from google.cloud import ndb

from arroba.datastore_storage import DatastoreStorage
from arroba.repo import Repo, Write
import arroba.server
from arroba import util
from arroba import xrpc_sync  # noqa: F401 — registers XRPC handlers as side effect
from arroba.storage import Action, MAX_OPERATIONS_PER_COMMIT
from arroba.util import RepoProfile, getrepo_profile

DEFAULT_RUNS = 3
COLLECTION = 'com.example.record'

SIZES = [
    ('small',  100),
    ('medium', 1000),
    ('large',  10000),
]


# ---------------------------------------------------------------------------
# Repo creation
# ---------------------------------------------------------------------------

def create_repo(storage, did, n_records):
    """Creates a repo populated with n_records records in batches."""
    key = util.new_key()
    repo = Repo.create(storage, did, signing_key=key, handle=f'{did}.bench')

    remaining = n_records
    while remaining > 0:
        batch = min(remaining, MAX_OPERATIONS_PER_COMMIT)
        writes = [
            Write(Action.CREATE, COLLECTION, util.next_tid(),
                  {'$type': COLLECTION,
                   'text': f'record {n_records - remaining + i}',
                   'createdAt': '2024-01-01T00:00:00Z'})
            for i in range(batch)
        ]
        storage.commit(repo, writes)
        remaining -= batch
        print(f'    {n_records - remaining}/{n_records} records written\r',
              end='', flush=True)

    print()
    return repo


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def mean(values):
    return sum(values) / len(values) if values else 0.0


def fmt_s(value):
    return f'{value:6.3f}s'


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------

def cmd_load(args, ndb_client):
    emulator_host = os.environ['DATASTORE_EMULATOR_HOST']
    print(f'Resetting Datastore emulator at {emulator_host}...')
    requests.post(f'http://{emulator_host}/reset', timeout=5)

    for size_name, n_records in SIZES:
        did = f'did:web:{size_name}.bench'
        print(f'\nCreating {size_name} repo ({n_records} records) ...')
        t0 = time.perf_counter()
        create_repo(arroba.server.storage, did, n_records)
        print(f'  done in {time.perf_counter() - t0:.1f}s')


def cmd_bench(args, ndb_client):
    all_results = {}  # size_name -> list of RepoProfile

    for size_name, n_records in SIZES:
        did = f'did:web:{size_name}.bench'
        print(f'\n--- {size_name} ({n_records} records, {did}) ---')
        runs = []

        for i in range(args.runs):
            getrepo_profile.set(RepoProfile())
            xrpc_sync.get_repo({}, did=did)
            p = getrepo_profile.get()
            assert p
            runs.append(p)

            total_blocks = p.node_blocks + p.leaf_blocks
            print(f'  run {i + 1}: total={fmt_s(p.total)}  '
                  f'car={humanize.naturalsize(p.car_size)}  blocks={total_blocks}')

        all_results[size_name] = runs

    col = 9
    print('\n' + '=' * 93)
    print('SUMMARY  (means across runs)')
    print('=' * 93)
    print(f'{"size":<7} {"recs":>5} {"blks":>6} {"lvls":>5} {"car":>9}  '
          f'{"total":>{col}} {"load_all":>{col}} '
          f'{"node_io":>{col}} {"node_dec":>{col}} {"leaf_io":>{col}} '
          f'{"ndb_all":>{col}} {"to_blk":>{col}}')
    print('-' * 93)

    for size_name, n_records in SIZES:
        runs = all_results.get(size_name, [])
        if not runs:
            print(f'{size_name:<7}  (no successful runs)')
            continue

        def avg(attr):
            vals = [getattr(r, attr) for r in runs]
            return mean(vals)

        total_blocks = avg('node_blocks') + avg('leaf_blocks')
        load_all = avg('node_io') + avg('node_decode') + avg('leaf_io')
        print(
            f'{size_name:<7} {n_records:>5} {int(total_blocks):>6} '
            f'{int(avg("levels")):>5} {humanize.naturalsize(avg("car_size")):>9}  '
            f'{fmt_s(avg("total")):>{col}} {fmt_s(load_all):>{col}} '
            f'{fmt_s(avg("node_io")):>{col}} {fmt_s(avg("node_decode")):>{col}} '
            f'{fmt_s(avg("leaf_io")):>{col}} '
            f'{fmt_s(avg("ndb_total")):>{col}} {fmt_s(avg("to_block_total")):>{col}}'
        )

    print()
    print('Columns:')
    print('  total     = full get_repo time (load_repo + load_all + write_car)')
    print('  load_all  = node_io + node_decode + leaf_io (MST traversal + leaf fetch)')
    print('  node_io   = read_many time for all MST node levels combined')
    print('  node_dec  = dag_cbor decode + deserialize_node_data for node blocks')
    print('  leaf_io   = read_many time for the final leaf block batch')
    print('  ndb_all   = total ndb.get_multi time across all read_many calls')
    print('  to_blk    = total AtpBlock.to_block() time across all read_many calls')
    print()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    subparsers = parser.add_subparsers(dest='cmd', required=True)

    subparsers.add_parser('load', help='Reset emulator and create benchmark repos')

    bench_parser = subparsers.add_parser('bench', help='Run getRepo benchmark')
    bench_parser.add_argument('--runs', type=int, default=DEFAULT_RUNS,
                              help=f'getRepo calls per repo size (default: {DEFAULT_RUNS})')

    args = parser.parse_args()
    logging.basicConfig(level=logging.WARNING)

    ndb_client = ndb.Client(project='app', credentials=AnonymousCredentials())

    with ndb_client.context(cache_policy=lambda key: False):
        arroba.server.storage = DatastoreStorage(ndb_client=ndb_client)
        if args.cmd == 'load':
            cmd_load(args, ndb_client)
        elif args.cmd == 'bench':
            cmd_bench(args, ndb_client)


if __name__ == '__main__':
    main()
