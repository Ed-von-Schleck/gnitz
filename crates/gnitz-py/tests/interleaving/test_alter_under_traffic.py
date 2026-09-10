"""`ALTER TABLE` landing while writes and reads are in flight.

Two orderings, reached two ways. The first is genuine concurrency: threads
hammer the table across the width change, and what must hold is that every
*acknowledged* INSERT is readable at the value it supplied and that no scan ever
sees a third column set. The second is exact: a seam parks a warm push, holding
no lock, until a DDL has moved the target's schema version, so the push resumes
against a descriptor it decoded before the ALTER.

Both are ordering claims, not timing ones — the concurrent test asserts the race
actually happened rather than trusting it to.
"""

import threading

import gnitz
from _read import bag, rows
from _serverproc import START_TIMEOUT, join_or_fail
from _uid import uid as _uid


def test_add_column_concurrent_with_inserts_and_scans(client, schema_name, server):
    """Every acknowledged INSERT must be readable at the value it supplied —
    never truncated to NULL by a worker whose stashed DdlSync had not drained —
    and every concurrent SELECT must return the pre-ALTER or the post-ALTER
    column set, never a torn row."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)",
        schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(f"({k}, {k})" for k in range(200)),
        schema_name=schema_name)

    stop = threading.Event()
    running = threading.Event()
    scan_widths = set()
    acked = {}
    acked_lock = threading.Lock()

    def scanner():
        with gnitz.connect(server) as conn:
            while not stop.is_set():
                try:
                    res = conn.execute_sql("SELECT * FROM t", schema_name=schema_name)
                    got = list(res[0]["rows"])
                    if got:
                        scan_widths.add(len(got[0]._fields))
                        running.set()
                except Exception:  # noqa: BLE001
                    # A clean rejection across the width change is allowed; a
                    # torn row is not, and would show as a third width.
                    running.set()

    def inserter():
        with gnitz.connect(server) as conn:
            for k in range(1000, 1100):
                # The width flips under us mid-run, so try the pre-ALTER shape
                # then the post-ALTER one. A rejection is fine; an *accepted*
                # INSERT is an ACK, and `a` must read back as `k` — the guard
                # this test exists for is a wider batch silently truncated
                # against a worker's stale schema and ACKed anyway.
                for sql, c in ((f"INSERT INTO t VALUES ({k}, {k})", None),
                               (f"INSERT INTO t VALUES ({k}, {k}, {k})", k)):
                    try:
                        conn.execute_sql(sql, schema_name=schema_name)
                    except Exception:  # noqa: BLE001
                        continue
                    # `c` is whichever shape the engine accepted; `a` must be `k`
                    # either way, and that is the truncation this guards.
                    with acked_lock:
                        acked[k] = c
                    break

    threads = [threading.Thread(target=scanner), threading.Thread(target=inserter)]
    for th in threads:
        th.start()
    try:
        assert running.wait(START_TIMEOUT), "the scanner never issued a statement"
        client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=schema_name)
    finally:
        stop.set()
        join_or_fail("a worker thread hung", *threads)

    # The race actually happened, and only the two legitimate column sets were
    # ever observed. An empty `scan_widths` or `acked` would make the assertions
    # below vacuous.
    assert scan_widths and scan_widths <= {2, 3}, f"torn scan widths: {scan_widths}"
    assert acked, "no concurrent INSERT was ever accepted"

    got = rows(client, schema_name, "SELECT * FROM t")
    assert got[0]._fields == ("id", "a", "c")
    want = {(k, k, None): 1 for k in range(200)}
    for k, c in acked.items():
        want[(k, k, c)] = 1
    assert bag(got, "id", "a", "c") == want


def test_a_warm_push_racing_an_alter_is_refused(push_hold_target):
    """A warm push decodes its batch against the catalog's descriptor BEFORE it
    takes the catalog read lock, and the lock is writer-preferring — so an
    `ALTER TABLE ADD COLUMN` queued in between is applied while the push is
    parked, and the descriptor the push decoded against is stale by the time it
    resumes. The push must be refused, not committed: its batch is laid out for
    the old width, and the commit path would re-frame it against the new one.

    The seam makes that interleaving certain: the first push holds, holding no
    lock, until a DDL has moved the target's schema version.
    """
    with gnitz.connect(push_hold_target) as client:
        sn = "race" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("id", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("a", gnitz.TypeCode.I64)]
        tid = client.create_table(sn, "t", cols)
        schema = gnitz.Schema(cols)

        outcome = {}
        pushed = threading.Event()

        def pusher():
            with gnitz.connect(push_hold_target) as conn:
                # Warm this connection's schema cache at the pre-ALTER version,
                # so the push below ships no schema block and takes the warm path.
                conn.scan(tid)
                batch = gnitz.ZSetBatch(schema)
                batch.append(id=1, a=7)
                pushed.set()
                try:
                    outcome["lsn"] = conn.push(tid, batch)
                except Exception as e:  # noqa: BLE001 — the outcome under test
                    outcome["err"] = e

        th = threading.Thread(target=pusher)
        th.start()
        try:
            assert pushed.wait(START_TIMEOUT), "the pusher thread never reached its push"
            client.execute_sql("ALTER TABLE t ADD COLUMN c BIGINT", schema_name=sn)
        finally:
            join_or_fail("the held push never returned", th)

        assert "err" in outcome, f"the racing push committed at LSN {outcome.get('lsn')}"
        # Nothing was written: the pre-ALTER-shaped batch never reached a store.
        assert list(client.scan(tid)) == []
        client.drop_schema(sn)
