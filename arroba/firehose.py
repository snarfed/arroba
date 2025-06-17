"""subscribeRepos firehose server."""
from collections import deque
from contextlib import contextmanager
import copy
from datetime import datetime, timedelta, timezone
import logging
from logging import DEBUG, INFO
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
# only wait for a skipped seq if we're within this many seqs of current
# https://github.com/snarfed/arroba/issues/56
WAIT_FOR_SKIPPED_SEQ_WINDOW = 300  # roughly 5m as of May 2025

new_events = threading.Condition()
subscribers = []
collector = None  # Thread; initialized in start()
rollback = None   # deque of (dict header, dict payload); initialized in collect()
started = threading.Event()  # notified once the collecter has fully started

lock = threading.RLock()  # TODO: RLock seems unneeded, switch back to Lock?

thread_local = threading.local()
logger = thread_local.logger = logging.getLogger(__name__)


def start(limit=None):
    with lock:
        global collector
        if collector:
            return
        collector = threading.Thread(target=collect, name='firehose collector',
                                     daemon=True, kwargs={'limit': limit})

    logger.info(f'Starting firehose collector with limit {limit}')
    collector.start()
    started.wait()


def reset():
    global new_events, subscribers, collector, rollback

    with lock:
        new_events = threading.Condition()
        started.clear()
        subscribers = []
        if collector:
            assert not collector.is_alive()
        collector = rollback = None


def send_events():
    """Trigger for when new event(s) are available."""
    with new_events:
        new_events.notify_all()


def subscribe(cursor=None):
    """Generator that returns firehose events.

    Args:
      cursor (int): optional cursor to start at. Must be within rollback window.

    Yields:
      sequence of (dict header, dict payload) tuples
    """
    started.wait()

    thread = threading.current_thread().name
    def log(msg, level=INFO):
        logger.log(level, f'subscriber {thread}: {msg}')

    log(f'starting with cursor {cursor}')
    if cursor:
        assert cursor >= rollback[-1][1]['seq'] - ROLLBACK_WINDOW, cursor

    # if this cursor behind our rollback window, load the window between the two
    # manually, for this subscriber
    handoff = None     # deque; copy of rollback window for when we transition to it
    rollback_start = rollback[0][1]['seq'] if rollback else 0
    if cursor is not None and cursor < rollback_start:
        log(f'cursor {cursor} is behind rollback start {rollback_start}; loading rest manually')
        pre_rollback = []  # events prior to rollback window that we load here
        for i, event in enumerate(server.storage.read_events_by_seq(start=cursor)):
            header, payload = process_event(event)
            # TODO: remove once https://github.com/snarfed/arroba/issues/57 is done
            if i % 10 == 0:
                time.sleep(.01)
            with lock:
                # rollback window may have changed, check it again, fresh, each time!
                if payload['seq'] >= rollback[0][1]['seq']:
                    cursor = rollback[0][1]['seq']
                    handoff = rollback.copy()

                    remaining_len = rollback.maxlen - len(rollback)
                    if remaining_len and pre_rollback:
                        # merge old events we've loaded onto the end of rollback
                        # extendleft reverses its argument; reverse again to undo that
                        pre_rollback = [e for e in pre_rollback
                                        if e[1]['seq'] < cursor]
                        pre_rollback = pre_rollback[-remaining_len:]
                        log(f'merging {pre_rollback[0][1]["seq"]}-{pre_rollback[-1][1]["seq"]} into rollback')
                        assert len(rollback) + len(pre_rollback) <= rollback.maxlen, \
                            (len(rollback), len(pre_rollback))
                        assert pre_rollback[-1][1]['seq'] < rollback[0][1]['seq'], \
                            (pre_rollback[-1][1]['seq'], rollback[0][1]['seq'])
                        rollback.extendleft(reversed(pre_rollback))

                    break

            pre_rollback.append((header, payload))
            yield (header, payload)

    # hand off to rollback window and new events
    subscriber = SimpleQueue()
    try:
        with lock:
            if cursor is not None:
                if handoff and handoff[0][1]['seq'] < rollback[0][1]['seq']:
                    log(f'backfilling from handoff from {handoff[0][1]["seq"]}')
                    for header, payload in handoff:
                        if payload['seq'] >= rollback[0][1]['seq']:
                            break
                        log(f'Backfilled handoff {payload["seq"]}', DEBUG)
                        subscriber.put_nowait((header, payload))

                log(f'backfilling from rollback from {cursor}')
                for header, payload in rollback:
                    if payload['seq'] >= cursor:
                        log(f'Backfilled rollback {payload["seq"]}', DEBUG)
                        subscriber.put_nowait((header, payload))

            log(f'streaming new events after {rollback[-1][1]["seq"] if rollback else 0}')
            subscribers.append(subscriber)

        # let these get garbage collected
        handoff = pre_rollback = None
        while True:
            yield subscriber.get()

    finally:
        log('removing subscriber')
        with lock:
            if subscriber in subscribers:
                subscribers.remove(subscriber)


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

    with lock:
        global rollback
        rollback = deque((process_event(e) for e in query), maxlen=ROLLBACK_WINDOW)

    if rollback:
        cur_seq = rollback[-1][1]['seq']
        logger.info(f'  preloaded seqs {rollback[0][1]["seq"]}-{cur_seq}')

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
            assert cur_seq > last_seq

            # if we see a sequence number skipped, and we're not too far behind, wait
            # up to NEW_EVENTS_TIMEOUT for it before giving up on it and moving on
            if cur_seq > last_seq + 1:
                if time.time() - last_event <= timeout_s:
                    seqs_behind = server.storage.last_seq(SUBSCRIBE_REPOS_NSID) - cur_seq
                    if seqs_behind <= WAIT_FOR_SKIPPED_SEQ_WINDOW:
                        logger.info(f'Waiting for seq {last_seq + 1}')
                        cur_seq = last_seq
                        break
                logger.info(f'Gave up waiting for seqs {last_seq + 1} to {cur_seq - 1}!')

            # emit event!
            last_event = time.time()
            header, payload = process_event(event)
            did = payload.get('did') or payload.get('repo')
            delay_s = int((util.now() - datetime.fromisoformat(payload['time']))\
                          .total_seconds())
            logger.info(f'Emitting to {len(subscribers)} subscribers: {payload["seq"]} {did} {header.get("t")} ({delay_s} s behind)')
            with lock:
                rollback.append((header, payload))
                for subscriber in subscribers:
                    # subscriber here is an unbounded SimpleQueue, so put should
                    # never block, but I want to be extra sure. (if put would
                    # block here, put_nowait will raise queue.Full instead.)
                    subscriber.put_nowait((header, payload))

            seen += 1

        time.sleep(SUBSCRIBE_REPOS_BATCH_DELAY.total_seconds())


def process_event(event):
    """Process an event for the ``subscribeRepos`` stream.

    Args:
        event (dict or CommitData)

    Returns:
        (dict, dict) tuple: (header, payload) to emit
    """
    if isinstance(event, dict):  # non-commit event
        payload = copy.copy(event)
        type = payload.pop('$type')
        type_fragment = type.removeprefix('com.atproto.sync.subscribeRepos')
        assert type_fragment != type, type
        return ({'op': 1, 't': type_fragment}, payload)

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
