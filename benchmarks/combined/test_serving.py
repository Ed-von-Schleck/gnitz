"""Low-latency serving: RYOW view point-seek under write load (multiworker).

A grouped-aggregate view keyed on U64 (R3) so seek(view, key) resolves by natural
key, plus a passthrough filter view seekable by base PK. A background writer
streams push() updates to the base while N reader clients push to their OWN
disjoint key and immediately seek it, asserting the seek reflects their own
just-ACKed write (read-your-writes, staleness 0) under the concurrent tick storm.
Reports seek p50/p99; staleness (reported as torn_reads) must be 0.
"""

from __future__ import annotations

import os
import random
import time

import pytest

import gnitz
from helpers.datagen import SERVING_SIZES, push_one
from helpers.timing import percentiles, run_htap

pytestmark = pytest.mark.multiworker

READER_GROUP_BASE = 10_000_000   # reader groups live above every base cust
PK_BASE = 100_000_000            # reader push PKs live above every base PK
DURATION = {"quick": 3.0, "full": 10.0}
READERS = 3


def _setup(client, sn, groups):
    client.execute_sql("CREATE TABLE o (pk BIGINT NOT NULL PRIMARY KEY, "
                       "cust BIGINT UNSIGNED NOT NULL, amt BIGINT NOT NULL)", schema_name=sn)
    # Natural-key view: group column cust is single non-nullable U64 (R3).
    client.execute_sql("CREATE VIEW rev AS SELECT cust, SUM(amt) AS s FROM o GROUP BY cust",
                       schema_name=sn)
    # Passthrough filter view, seekable by base PK.
    client.execute_sql("CREATE VIEW passthru AS SELECT * FROM o WHERE amt >= 0", schema_name=sn)
    tid, schema = client.resolve_table(sn, "o")
    b = gnitz.ZSetBatch(schema)
    for k in range(1, groups + 1):
        b.append(pk=k, cust=k, amt=(k % 100) + 1, weight=1)
    client.push(tid, b)


def _bg_writer(sn, groups):
    """Background load: stream fresh base rows to random base custs."""
    def writer_fn(conn, deadline):
        tid, schema = conn.resolve_table(sn, "o")
        rng = random.Random(os.getpid())
        pk = PK_BASE + os.getpid() * 100_000_000
        commits = 0
        lat = []
        while time.perf_counter() < deadline:
            start = time.perf_counter()
            push_one(conn, tid, schema, pk=pk, cust=rng.randint(1, groups), amt=1)
            pk += 1
            commits += 1
            lat.append((time.perf_counter() - start) * 1000.0)
        return {"commits": commits, "conflicts": 0, "latencies": lat}
    return writer_fn


def _record(bench_timer, res, dur):
    p50, _, p99 = percentiles(res["reader_latencies"])
    bench_timer.num_clients = READERS + 1
    bench_timer.add_latencies(res["reader_latencies"], rows=res["reader_ops"])
    bench_timer.extra.update(
        seek_p50_ms=round(p50, 3), seek_p99_ms=round(p99, 3),
        reader_ops=res["reader_ops"], staleness=res["torn_reads"],
        writer_pushes_per_sec=round(res["writer_commits"] / dur, 1) if dur else 0.0,
    )
    assert res["reader_ops"] > 0, "no reader seeks"
    assert res["torn_reads"] == 0, f"observed {res['torn_reads']} stale seeks (RYOW violated)"


def test_serving_natural(client, socket_path, schema_name, bench_timer, scale_mode):
    sn = schema_name
    sz = SERVING_SIZES[scale_mode]
    dur = DURATION[scale_mode]
    _setup(client, sn, sz["groups"])

    def reader_fn(conn, deadline):
        o_tid, o_sch = conn.resolve_table(sn, "o")
        vid, _ = conn.resolve_table(sn, "rev")
        mygroup = READER_GROUP_BASE + (os.getpid() % 5_000_000)
        pk = PK_BASE + os.getpid() * 100_000_000
        total = ops = stale = 0
        lat = []
        while time.perf_counter() < deadline:
            push_one(conn, o_tid, o_sch, pk=pk, cust=mygroup, amt=1)  # the reader's own write
            pk += 1
            total += 1
            start = time.perf_counter()
            res = conn.seek(vid, mygroup)
            lat.append((time.perf_counter() - start) * 1000.0)
            rows = [r for r in res if r.weight > 0]
            if len(rows) != 1 or rows[0].s != total:
                stale += 1  # seek did not reflect the reader's own just-ACKed write
            ops += 1
        return {"ops": ops, "latencies": lat, "torn_reads": stale}

    res = run_htap(socket_path, _bg_writer(sn, sz["groups"]), reader_fn, 1, READERS, dur)
    _record(bench_timer, res, dur)


def test_serving_passthrough(client, socket_path, schema_name, bench_timer, scale_mode):
    sn = schema_name
    sz = SERVING_SIZES[scale_mode]
    dur = DURATION[scale_mode]
    _setup(client, sn, sz["groups"])

    def reader_fn(conn, deadline):
        o_tid, o_sch = conn.resolve_table(sn, "o")
        vid, _ = conn.resolve_table(sn, "passthru")
        pk = PK_BASE + os.getpid() * 100_000_000
        ops = stale = 0
        lat = []
        while time.perf_counter() < deadline:
            push_one(conn, o_tid, o_sch, pk=pk, cust=1, amt=pk % 1000)  # the reader's own write
            start = time.perf_counter()
            res = conn.seek(vid, pk)
            lat.append((time.perf_counter() - start) * 1000.0)
            rows = [r for r in res if r.weight > 0]
            if len(rows) != 1 or rows[0].pk != pk:
                stale += 1  # seek did not reflect the reader's own just-ACKed row
            pk += 1
            ops += 1
        return {"ops": ops, "latencies": lat, "torn_reads": stale}

    res = run_htap(socket_path, _bg_writer(sn, sz["groups"]), reader_fn, 1, READERS, dur)
    _record(bench_timer, res, dur)
