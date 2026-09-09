"""Writes that overlap in time.

Barrier-synced processes pushing disjoint PK ranges to four tables at once, and
the shape where a view tick is still in flight when the next statement's
PK-rejection probe burst runs.
"""

import multiprocessing

import gnitz
from _serverproc import NEEDS_MULTI

pytestmark = NEEDS_MULTI

_COLS = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
         gnitz.ColumnDef("val", gnitz.TypeCode.I64)]


def _push_range(server_path, tid, pk_start, pk_end, barrier):
    """Child process: connect, build the batch, then release together and push.

    The barrier sits after the connect and the batch build so what the processes
    align on is the push itself; waiting first would spread them by whatever
    connection setup costs.
    """
    import gnitz as _gnitz
    schema = _gnitz.Schema(
        [_gnitz.ColumnDef("pk", _gnitz.TypeCode.U64, primary_key=True),
         _gnitz.ColumnDef("val", _gnitz.TypeCode.I64)])
    with _gnitz.connect(server_path) as c:
        batch = _gnitz.ZSetBatch(schema)
        for pk in range(pk_start, pk_end):
            batch.append(pk=pk, val=pk)
        barrier.wait()
        c.push(tid, batch)


def _push_then_insert_child(server_path, sn, tid, n_bulk):
    """Child process: a bulk push that crosses the tick-coalesce threshold, then
    an INSERT loop against the still-running tick."""
    import gnitz as _gnitz
    schema = _gnitz.Schema(
        [_gnitz.ColumnDef("pk", _gnitz.TypeCode.U64, primary_key=True),
         _gnitz.ColumnDef("val", _gnitz.TypeCode.I64)])
    with _gnitz.connect(server_path) as c:
        batch = _gnitz.ZSetBatch(schema)
        for pk in range(1, n_bulk + 1):
            batch.append(pk=pk, val=pk * 100)
        c.push(tid, batch)

        pk_base = n_bulk + 1
        for i in range(50):
            vals = ",".join(
                f"({pk_base + i * 100 + j}, {(pk_base + i * 100 + j) * 100})"
                for j in range(100)
            )
            c.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)


def _run_child(target, args, seconds, label):
    """Run `target(*args)` in a subprocess; kill and fail if it never returns.

    A deadlock has no observable other than not finishing, and the client's
    blocking socket reads sit in C, where `signal.alarm` cannot reach them — so
    the work goes in a child and `Process.join(timeout=)` is the detector.
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
        raise AssertionError(f"DEADLOCK: {label} did not finish within {seconds}s")
    assert proc.exitcode == 0, f"{label}: child exited with code {proc.exitcode}"


def test_barrier_released_pushes_land_whole_on_every_table(client, server, schema_name):
    """Twelve processes release together onto four tables, three disjoint PK
    ranges each. Every table must hold exactly its ranges at weight 1: a
    mis-routed or replayed commit group moves a payload or a weight, neither of
    which a row count would show."""
    n_tables, n_procs, per_proc = 4, 3, 20
    tids = [client.create_table(schema_name, f"t{i}", _COLS) for i in range(n_tables)]
    barrier = multiprocessing.Barrier(n_tables * n_procs)

    procs = [
        multiprocessing.Process(
            target=_push_range,
            args=(server, tid, pi * per_proc + 1, (pi + 1) * per_proc + 1, barrier),
        )
        for tid in tids
        for pi in range(n_procs)
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    assert [p.exitcode for p in procs] == [0] * len(procs)

    want = [(pk, pk) for pk in range(1, n_procs * per_proc + 1)]
    for i, tid in enumerate(tids):
        res = client.scan(tid)
        assert sorted((r.pk, r.val) for r in res) == want, f"table t{i}"
        assert set(res.weights) == {1}, f"table t{i}: weights {set(res.weights)}"


def test_probe_burst_against_an_in_flight_tick(client, server, schema_name):
    """A push that crosses the tick-coalesce threshold ACKs while the tick it
    triggered is still running; the INSERTs that follow each take the
    PK-rejection probe burst against those same workers. Neither may wedge the
    master, and the view the tick was maintaining must converge exactly.

    The bulk size is the race window — it is how long the tick stays in flight —
    so it is load-bearing, not a scale knob.
    """
    tid = client.create_table(schema_name, "t", _COLS)
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM t WHERE val > 500000", schema_name=schema_name)

    n_bulk = 50_000
    _run_child(_push_then_insert_child, (server, schema_name, tid, n_bulk),
               seconds=60, label="bulk push + INSERT loop with view")

    inserted = 50 * 100
    assert len(client.scan(tid)) == n_bulk + inserted

    # val = pk * 100 throughout, so the view holds exactly the pk > 5000 rows,
    # each at weight 1 — a tick applied twice keeps the row set and doubles them.
    vres = client.scan(client.resolve_table(schema_name, "v")[0])
    assert len(vres) == n_bulk + inserted - 5000
    assert set(vres.weights) == {1}, f"view weights {set(vres.weights)}"
