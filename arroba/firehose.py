"""subscribeRepos firehose server."""
from collections import deque
from contextlib import contextmanager
from datetime import timedelta, timezone
import logging
import os
from queue import SimpleQueue
import threading
import time

from carbox import car
import dag_cbor
from google.auth.credentials import AnonymousCredentials
from google.cloud import ndb
from multiformats import CID

from .mst import MST
from . import server
from .storage import Action, CommitData, SUBSCRIBE_REPOS_NSID
from . import util

NEW_EVENTS_TIMEOUT = timedelta(seconds=20)
ROLLBACK_WINDOW = int(os.getenv('ROLLBACK_WINDOW', 50_000))
# 4000 seqs is ~1h as of May 2025, loads in prod in ~2m
PRELOAD_WINDOW = int(os.getenv('PRELOAD_WINDOW', 4000))
SUBSCRIBE_REPOS_BATCH_DELAY = timedelta(seconds=float(os.getenv('SUBSCRIBE_REPOS_BATCH_DELAY', 0)))

new_events = threading.Condition()
subscribers = []
collector = None  # Thread; initialized in start()
rollback = None   # deque of (dict header, dict payload); initialized in collect()
started = threading.Event()  # notified once the collecter has fully started

lock = threading.Lock()

logger = logging.getLogger(__name__)


def start(limit=None):
    logger.debug('> start')
    with lock:
        global collector
        if collector:
            return
        collector = threading.Thread(target=collect, name='firehose collector',
                                     daemon=True, kwargs={'limit': limit})
    logger.debug('< start')

    logger.info(f'Starting firehose collector with limit {limit}')
    collector.start()
    started.wait()


def reset():
    global new_events, subscribers, collector, rollback

    logger.debug('> reset')
    with lock:
        new_events = threading.Condition()
        started.clear()
        subscribers = []
        if collector:
            assert not collector.is_alive()
        collector = rollback = None
    logger.debug('< reset')


def send_events():
    """Trigger for when new event(s) are available."""
    with new_events:
        new_events.notify_all()


def subscribe(cursor=None):
    """Generator that returns firehose events.

    Args:
      cursor (int): optional cursor to start at

    Yields:
      sequence of (dict header, dict payload) tuples
    """
    started.wait()

    # XXX TODO: synchronize handoff between this and rollback window
    rollback_start = rollback[0][1]['seq']
    if cursor is not None and cursor < rollback_start:
        logger.info(f"cursor {cursor} is behind our preloaded rollback window's start {rollback_start}; loading initial remainder manually")
        for event in server.storage.read_events_by_seq(start=cursor):
            seq = event['seq'] if isinstance(event, dict) else event.commit.seq
            # rollback window may have advanced, check it again, fresh, each time!
            if seq >= rollback[0][1]['seq']:
                break
            yield process_event(event)

    events = SimpleQueue()
    try:
        logger.debug('> subscribe')
        with lock:
            logger.debug('  subscribed')
            if cursor is not None:
                logger.debug(f'subscribe: backfilling from rollback window with cursor {cursor}')
                for header, payload in rollback:
                    if payload['seq'] >= cursor:
                        logger.debug(f'Backfilled {payload["seq"]}')
                        yield (header, payload)
                logger.debug('  done')

            logger.debug(f'subscribe: subscribing to new events')
            subscribers.append(events)
        logger.debug('< subscribe')

        while True:
            yield events.get()

    finally:
        logger.debug('> subscribe 2')
        with lock:
            logger.debug('  subscribed')
            if events in subscribers:
                subscribers.remove(events)
        logger.debug('< subscribe 2')


def collect(limit=None):
    """Daemon thread. Collects new events and sends them to each subscriber.

    Args:
      limit (int): if provided, return after collecting this many *new* events. Only
        used in tests.
    """
    logger.info(f'collect: preloading rollback window ({PRELOAD_WINDOW})')
    cur_seq = server.storage.last_seq(SUBSCRIBE_REPOS_NSID)
    query = server.storage.read_events_by_seq(
        start=max(cur_seq - PRELOAD_WINDOW + 1, 0))

    logger.debug('> collect 1')
    with lock:
        logger.debug('  collected')
        global rollback
        rollback = deque((process_event(e) for e in query), maxlen=ROLLBACK_WINDOW)
    logger.debug('< collect 1')

    cur_seq = rollback[-1][1]['seq']
    logger.debug(f'  preloaded seqs {rollback[0][1]["seq"]}-{cur_seq}')

    started.set()

    logger.info(f'collecting new events')
    timeout_s = NEW_EVENTS_TIMEOUT.total_seconds()
    last_event = time.time()
    seen = 0

    while True:
        if limit is not None and seen >= limit:
            return

        with new_events:
            new_events.wait(timeout_s)

        for event in server.storage.read_events_by_seq(start=cur_seq + 1):
            last_seq = cur_seq
            cur_seq = event['seq'] if isinstance(event, dict) else event.commit.seq

            # if we see a sequence number skipped, wait for it up to
            # NEW_EVENTS_TIMEOUT before giving up on it and moving on
            waited_enough = time.time() - last_event > timeout_s
            if cur_seq == last_seq + 1 or waited_enough:
                if cur_seq > last_seq + 1:
                    logger.info(f'Gave up waiting for seqs {last_seq + 1} to {cur_seq - 1}!')

                last_event = time.time()
                frame = process_event(event)
                logger.debug(f'Emitting to {len(subscribers)} subscribers: {frame[1]["seq"]}')
                logger.debug('> collect 2')
                with lock:
                    logger.debug('  collected')
                    rollback.append(frame)
                    for subscriber in subscribers:
                        subscriber.put(frame)
                logger.debug('< collect 2')

                seen += 1

            else:
                logger.info(f'Waiting for seq {last_seq + 1}')
                cur_seq = last_seq
                break

        time.sleep(SUBSCRIBE_REPOS_BATCH_DELAY.total_seconds())


def process_event(event):
    """Process an event for the ``subscribeRepos`` stream.

    Args:
        event (dict or CommitData)

    Returns:
        (dict, dict) tuple: (header, payload) to emit
    """
    logger.debug('process_event')
    if isinstance(event, dict):  # non-commit event
        type = event.pop('$type')
        type_fragment = type.removeprefix('com.atproto.sync.subscribeRepos')
        assert type_fragment != type, type
        return ({'op': 1, 't': type_fragment}, event)

    assert isinstance(event, CommitData), \
        f'unexpected event type {event.__class__} {event}'

    commit = event.commit.decoded

    # sync v1.1 aka inductive firehose: including blocks for "covering proofs" of
    # new/updated/deleted records
    # https://github.com/bluesky-social/proposals/tree/main/0006-sync-iteration
    tree = MST(storage=server.storage, pointer=event.commit.decoded['data'])
    tree.add_covering_proofs(event, blocks=event.blocks)
    car_blocks = [car.Block(cid=block.cid, data=block.encoded, decoded=block.decoded)
                for block in event.blocks.values()]

    # previous commit's data CID goes into prevData field
    prev_data = None
    if prev_commit_cid := commit['prev']:
        if prev_commit := server.storage.read(prev_commit_cid):
            prev_data = prev_commit.decoded.get('data')

    # records' previous CIDs go into operations' prev fields
    ops = []
    if event.commit.ops:
        for op in event.commit.ops:
            event_op = {
                'action': op.action.name.lower(),
                'path': op.path,
                'cid': None if op.action == Action.DELETE else op.cid,
            }
            if op.action != Action.CREATE:
                event_op['prev'] = op.prev_cid
            ops.append(event_op)

    return ({  # header
        'op': 1,
        't': '#commit',
    }, {  # payload
        'repo': commit['did'],
        'ops': ops,
        'commit': event.commit.cid,
        'blocks': car.write_car([event.commit.cid], car_blocks),
        'time': event.commit.time.replace(tzinfo=timezone.utc).isoformat(),
        'seq': event.commit.seq,
        'rev': util.int_to_tid(event.commit.seq, clock_id=0),
        'since': None,  # TODO: load event.commit['prev']'s CID
        'rebase': False,
        'tooBig': False,
        'blobs': [],
        'prevData': prev_data,
    })
