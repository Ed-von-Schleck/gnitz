"""Writes that overlap in time.

Barrier-synced processes pushing disjoint PK ranges to one table and to four;
a scan of one table while another is being written; and the two shapes that
once deadlocked — an INSERT whose PK-rejection probe burst runs while an async
view tick is still in flight, reached both from a steady INSERT loop against a
table carrying a view and from a bulk push immediately followed by one.
"""

import os
import multiprocessing
import pytest
import gnitz
from _uid import uid as _uid

_NUM_WORKERS = int(os.environ.get("GNITZ_WORKERS", "1"))
_NEEDS_MULTI = pytest.mark.skipif(
    _NUM_WORKERS < 2, reason="requires GNITZ_WORKERS >= 2"
)


def _drop_all(client, sn, tables=(), views=(), indices=()):
    for idx in indices:
        try:
            client.execute_sql(f"DROP INDEX {idx}", schema_name=sn)
        except Exception:
            pass
    for v in views:
        try:
            client.execute_sql(f"DROP VIEW {v}", schema_name=sn)
        except Exception:
            pass
    for t in tables:
        try:
            client.execute_sql(f"DROP TABLE {t}", schema_name=sn)
        except Exception:
            pass
    client.drop_schema(sn)

def _concurrent_push_worker(server_path, tid, pk_start, pk_end, barrier):
    """Worker function: barrier sync then push a batch of rows."""
    import gnitz as _gnitz
    barrier.wait()
    cols = [_gnitz.ColumnDef("pk", _gnitz.TypeCode.U64, primary_key=True),
            _gnitz.ColumnDef("val", _gnitz.TypeCode.I64)]
    schema = _gnitz.Schema(cols)
    with _gnitz.connect(server_path) as c:
        batch = _gnitz.ZSetBatch(schema)
        for pk in range(pk_start, pk_end):
            batch.append(pk=pk, val=pk)
        c.push(tid, batch)


def _concurrent_multi_table_worker(server_path, tid, pk_start, pk_end, barrier):
    """Worker function for multi-table concurrent test."""
    import gnitz as _gnitz
    barrier.wait()
    cols = [_gnitz.ColumnDef("pk", _gnitz.TypeCode.U64, primary_key=True),
            _gnitz.ColumnDef("val", _gnitz.TypeCode.I64)]
    schema = _gnitz.Schema(cols)
    with _gnitz.connect(server_path) as c:
        batch = _gnitz.ZSetBatch(schema)
        for pk in range(pk_start, pk_end):
            batch.append(pk=pk, val=pk)
        c.push(tid, batch)

def _run_with_deadlock_timeout(target, args, seconds, label):
    """Run `target(*args)` in a subprocess; kill and fail on hang.

    `signal.alarm` does not interrupt blocking C-level socket reads in the
    gnitz client, so we isolate the work in a child process and use
    `Process.join(timeout=)` to detect deadlocks.
    """
    proc = multiprocessing.Process(target=target, args=args)
    proc.start()
    proc.join(seconds)
    if proc.is_alive():
        proc.terminate()
        proc.join(5)
        if proc.is_alive():
            proc.kill()
            proc.join()
        raise AssertionError(
            f"DEADLOCK: {label} did not complete within {seconds}s"
        )
    assert proc.exitcode == 0, (
        f"{label}: child exited with code {proc.exitcode}"
    )


def _insert_loop_with_view_child(server_path, sn):
    """Child process: INSERT VALUES in a loop on a table that has a view."""
    import gnitz as _gnitz
    with _gnitz.connect(server_path) as c:
        for i in range(20):
            vals = ",".join(
                f"({i * 50 + j}, {j * 10})" for j in range(1, 51)
            )
            c.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

def _push_then_insert_child(server_path, sn, tid, n_bulk):
    """Child process: bulk push (triggers async tick), then INSERT loop."""
    import gnitz as _gnitz
    cols = [_gnitz.ColumnDef("pk", _gnitz.TypeCode.U64, primary_key=True),
            _gnitz.ColumnDef("val", _gnitz.TypeCode.I64)]
    schema = _gnitz.Schema(cols)
    with _gnitz.connect(server_path) as c:
        batch = _gnitz.ZSetBatch(schema)
        for pk in range(1, n_bulk + 1):
            batch.append(pk=pk, val=pk * 100)
        c.push(tid, batch)

        pk_base = n_bulk + 1
        for i in range(50):
            vals = ",".join(
                f"({pk_base + i * 100 + j}, "
                f"{(pk_base + i * 100 + j) * 100})"
                for j in range(100)
            )
            c.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)












@_NEEDS_MULTI
def test_workers_concurrent_push_same_table(client, server):
    """8 processes barrier-synced push non-overlapping PK ranges; scan → all rows."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tid = client.create_table(sn, "t", cols)

        n_procs = 8
        rows_per_proc = 25
        total = n_procs * rows_per_proc
        barrier = multiprocessing.Barrier(n_procs)

        procs = []
        for i in range(n_procs):
            pk_start = i * rows_per_proc + 1
            pk_end = pk_start + rows_per_proc
            p = multiprocessing.Process(
                target=_concurrent_push_worker,
                args=(server, tid, pk_start, pk_end, barrier),
            )
            procs.append(p)

        for p in procs:
            p.start()
        for p in procs:
            p.join()

        for p in procs:
            assert p.exitcode == 0, f"Worker exited with code {p.exitcode}"

        result = client.scan(tid)
        pks = sorted(r.pk for r in result)
        assert pks == list(range(1, total + 1))
    finally:
        _drop_all(client, sn, tables=["t"])

@_NEEDS_MULTI
def test_workers_concurrent_push_multi_table(client, server):
    """12 processes push to 4 tables (3 each); scan each → all rows present."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        n_tables = 4
        n_procs_per_table = 3
        rows_per_proc = 20

        tids = []
        for i in range(n_tables):
            tid = client.create_table(sn, f"t{i}", cols)
            tids.append(tid)

        total_per_table = n_procs_per_table * rows_per_proc
        barrier = multiprocessing.Barrier(n_tables * n_procs_per_table)

        procs = []
        for ti, tid in enumerate(tids):
            for pi in range(n_procs_per_table):
                pk_start = pi * rows_per_proc + 1
                pk_end = pk_start + rows_per_proc
                p = multiprocessing.Process(
                    target=_concurrent_multi_table_worker,
                    args=(server, tid, pk_start, pk_end, barrier),
                )
                procs.append(p)

        for p in procs:
            p.start()
        for p in procs:
            p.join()

        for p in procs:
            assert p.exitcode == 0, f"Worker exited with code {p.exitcode}"

        for i, tid in enumerate(tids):
            pks = sorted(r.pk for r in client.scan(tid))
            assert len(pks) == total_per_table, (
                f"table t{i}: expected {total_per_table} rows, got {len(pks)}"
            )
    finally:
        _drop_all(client, sn, tables=[f"t{i}" for i in range(n_tables)])

def test_cross_table_push_scan_isolation(client):
    """Push to table A while scanning table B — scan should not
    cause unnecessary flush of table A's pending pushes.  Per-table
    read barriers ensure cross-table isolation."""
    sn = "w" + _uid()
    client.create_schema(sn)
    try:
        cols = [
            gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64),
        ]
        schema = gnitz.Schema(cols)
        tid_a = client.create_table(sn, "a", cols)
        tid_b = client.create_table(sn, "b", cols)

        batch_a = gnitz.ZSetBatch(schema)
        for i in range(10):
            batch_a.append(pk=i + 1, val=i * 10)
        client.push(tid_a, batch_a)

        # Scan table B (empty) — must succeed without affecting table A
        result_b = client.scan(tid_b)
        assert len(result_b) == 0

        # Scan table A — all 10 rows must be present
        result_a = client.scan(tid_a)
        pks = sorted(row.pk for row in result_a)
        assert len(pks) == 10
    finally:
        _drop_all(client, sn, tables=["a", "b"])

@_NEEDS_MULTI
def test_insert_sql_with_filter_view_no_deadlock(client, server):
    """Repeated INSERT VALUES on a unique-PK table while a filter view is
    propagating must not deadlock the master.

    Each INSERT triggers the PK-rejection probe in
    `validate_txn_distributed`, which resets the w2m cursors. The view present on the table forces async tick state.
    """
    sn = "vd" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, "
            "val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM t WHERE val > 100",
            schema_name=sn,
        )

        _run_with_deadlock_timeout(
            _insert_loop_with_view_child,
            (server, sn),
            seconds=30,
            label="INSERT VALUES loop with view",
        )

        tid, _ = client.resolve_table(sn, "t")
        rows = sorted(r.pk for r in client.scan(tid))
        assert len(rows) == 20 * 50
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])

@_NEEDS_MULTI
def test_push_then_insert_sql_with_view_no_deadlock(client, server):
    """Bulk push (kicks off async tick) immediately followed by INSERT VALUES.

    This is the exact pattern that hung in `test_incremental_cost[filter]`:
    a large `client.push()` triggers view propagation, then a sequence of
    `INSERT VALUES` SQL statements race against the still-running tick. Each
    INSERT goes through the new PK-rejection synchronous broadcast that
    pollutes the shared w2m cursors.

    Scale tuned to mimic the benchmark: 50k bulk push + 50 × 100-row INSERTs.
    """
    sn = "pd" + _uid()
    client.create_schema(sn)
    try:
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
        tid = client.create_table(sn, "t", cols)
        client.execute_sql(
            "CREATE VIEW v AS SELECT * FROM t WHERE val > 500000",
            schema_name=sn,
        )

        n_bulk = 50_000
        _run_with_deadlock_timeout(
            _push_then_insert_child,
            (server, sn, tid, n_bulk),
            seconds=60,
            label="bulk push + INSERT loop with view",
        )

        rows = sorted(r.pk for r in client.scan(tid))
        assert len(rows) == n_bulk + 50 * 100
    finally:
        _drop_all(client, sn, views=["v"], tables=["t"])
