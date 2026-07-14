"""E2E tests for optimistic-concurrency LSN preconditions on the user-table TXN
frame — the lost-update guard on read-modify-write SQL.

Every SQL mutation that reads before it writes (UPDATE / DELETE with a resolving
WHERE, INSERT ... ON CONFLICT) now commits through a one-precondition PUSH_TXN
frame: "commit only if this table has not been written since basis L". The master
checks it under the same table locks that serialize the commit and rejects a
racing write with a retryable conflict. Autocommit statements retry internally
(adopting the server's fresh basis); BEGIN/COMMIT surfaces the conflict.

Run with GNITZ_WORKERS=4 — the conflict window is a distributed commit path.
"""

import random
import threading

import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _schema(client):
    sn = "occ" + _uid()
    client.create_schema(sn)
    return sn


def _table(client, sn, name="t"):
    client.execute_sql(
        f"CREATE TABLE {name} (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=sn,
    )
    tid, _ = client.resolve_table(sn, name)
    return tid


def _scan(client, tid):
    """Sorted (pk, val) over positive-weight rows."""
    return sorted((r.pk, r.val) for r in client.scan(tid) if r.weight > 0)


def _retry(c, sn, sql):
    """Run an autocommit statement, absorbing OCC conflicts at the app level
    (what makes a tight race deterministic — the internal bound can exhaust)."""
    while True:
        try:
            return c.execute_sql(sql, schema_name=sn)
        except gnitz.GnitzConflictError:
            continue


# ---------------------------------------------------------------------------
# Basis seeding: HELLO-ACK watermark seeds last_seen_lsn, which advances on push
# ---------------------------------------------------------------------------


def test_basis_seeded_at_connect_and_advances_on_push(server):
    with gnitz.connect(server) as c:
        sn = _schema(c)
        try:
            _table(c, sn)
            before = c.last_seen_lsn
            c.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)
            after = c.last_seen_lsn
            # A committed push advances the basis past its pre-push value.
            assert after > before
        finally:
            c.drop_schema(sn)
    # A fresh connection is seeded from the HELLO ACK watermark, which now
    # reflects the committed insert — a non-zero, sound basis with no read op.
    with gnitz.connect(server) as c2:
        assert c2.last_seen_lsn >= after
        assert c2.last_seen_lsn > 0


# ---------------------------------------------------------------------------
# Lost-update closed: the headline guarantee
# ---------------------------------------------------------------------------


def test_lost_update_closed_under_race(server):
    n = 30
    with gnitz.connect(server) as setup:
        sn = _schema(setup)
        _table(setup, sn)
        setup.execute_sql("INSERT INTO t VALUES (1, 0)", schema_name=sn)
    try:
        def worker():
            with gnitz.connect(server) as c:
                for _ in range(n):
                    _retry(c, sn, "UPDATE t SET val = val + 1 WHERE pk = 1")

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with gnitz.connect(server) as c:
            tid, _ = c.resolve_table(sn, "t")
            # Exactly 2n increments landed — no increment silently overwritten.
            assert _scan(c, tid) == [(1, 2 * n)]
    finally:
        with gnitz.connect(server) as c:
            c.drop_schema(sn)


# ---------------------------------------------------------------------------
# Single transient collision self-heals via the fresh-basis refresh
# ---------------------------------------------------------------------------


def test_single_transient_collision_self_heals(server):
    with gnitz.connect(server) as setup:
        sn = _schema(setup)
        _table(setup, sn)
        setup.execute_sql("INSERT INTO t VALUES (1, 0)", schema_name=sn)
    a = gnitz.connect(server)  # A's basis is fixed at connect (< B's commit below)
    b = gnitz.connect(server)
    try:
        # B commits between A's connect-time basis and A's UPDATE.
        b.execute_sql("UPDATE t SET val = val + 100 WHERE pk = 1", schema_name=sn)
        # A's UPDATE conflicts once (its basis predates B's commit), then the
        # internal retry adopts B's fresh basis and succeeds with NO app-visible
        # error. Without the fresh-basis refresh this would deterministically
        # re-conflict until the bound exhausts and raise GnitzConflictError.
        a.execute_sql("UPDATE t SET val = val + 1 WHERE pk = 1", schema_name=sn)
        tid, _ = a.resolve_table(sn, "t")
        assert _scan(a, tid) == [(1, 101)]  # 0 + 100 (B) + 1 (A): no lost update
    finally:
        a.close()
        b.close()
        with gnitz.connect(server) as c:
            c.drop_schema(sn)


# ---------------------------------------------------------------------------
# Different rows conflict spuriously (table grain) but both still succeed
# ---------------------------------------------------------------------------


def test_different_rows_both_succeed(server):
    n = 25
    with gnitz.connect(server) as setup:
        sn = _schema(setup)
        _table(setup, sn)
        setup.execute_sql("INSERT INTO t VALUES (1, 0), (2, 0)", schema_name=sn)
    try:
        def worker(pk):
            with gnitz.connect(server) as c:
                for _ in range(n):
                    _retry(c, sn, f"UPDATE t SET val = val + 1 WHERE pk = {pk}")

        threads = [threading.Thread(target=worker, args=(pk,)) for pk in (1, 2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with gnitz.connect(server) as c:
            tid, _ = c.resolve_table(sn, "t")
            # Table-grain conflicts between pk=1 and pk=2 are absorbed by the app
            # retry; every increment on both rows still lands.
            assert _scan(c, tid) == [(1, n), (2, n)]
    finally:
        with gnitz.connect(server) as c:
            c.drop_schema(sn)


# ---------------------------------------------------------------------------
# Autocommit conflict surfaces under sustained contention, and names the table
# ---------------------------------------------------------------------------


def test_autocommit_conflict_surfaces_and_names_table(server):
    with gnitz.connect(server) as setup:
        sn = _schema(setup)
        _table(setup, sn)
        setup.execute_sql("INSERT INTO t VALUES (1, 0)", schema_name=sn)

    stop = threading.Event()

    def hammer():
        with gnitz.connect(server) as c:
            while not stop.is_set():
                _retry(c, sn, "UPDATE t SET val = val + 1 WHERE pk = 1")

    hammers = [threading.Thread(target=hammer) for _ in range(3)]
    for h in hammers:
        h.start()

    seen = None
    try:
        with gnitz.connect(server) as c:
            # Under three threads committing continuously, the four-attempt
            # internal bound exhausts for at least one of these statements.
            for _ in range(400):
                try:
                    c.execute_sql("UPDATE t SET val = val + 1 WHERE pk = 1", schema_name=sn)
                except gnitz.GnitzConflictError as e:
                    seen = str(e)
                    break
    finally:
        stop.set()
        for h in hammers:
            h.join()

    assert seen is not None, "sustained contention must eventually surface a conflict"
    # The client-synthesized message names the conflicting table.
    assert "t" in seen and "conflict" in seen.lower()

    with gnitz.connect(server) as c:
        tid, _ = c.resolve_table(sn, "t")
        rows = _scan(c, tid)
        # Exactly one live row, value a clean integer sum — never a torn write.
        assert len(rows) == 1 and rows[0][0] == 1 and rows[0][1] >= 0
        c.drop_schema(sn)


# ---------------------------------------------------------------------------
# BEGIN/COMMIT: first-committer-wins, no auto-retry, buffered writes discarded
# ---------------------------------------------------------------------------


def test_begin_commit_conflict_and_rerun(server):
    with gnitz.connect(server) as setup:
        sn = _schema(setup)
        _table(setup, sn)
        setup.execute_sql("INSERT INTO t VALUES (1, 0)", schema_name=sn)
    a = gnitz.connect(server)
    b = gnitz.connect(server)
    try:
        tid, _ = a.resolve_table(sn, "t")
        a.execute_sql("BEGIN", schema_name=sn)
        # Reads t (resolves rows), buffers the write, records t in the read-set.
        a.execute_sql("UPDATE t SET val = val + 1 WHERE pk = 1", schema_name=sn)
        # A concurrent connection commits to t between A's read and A's COMMIT.
        b.execute_sql("UPDATE t SET val = val + 100 WHERE pk = 1", schema_name=sn)
        # First-committer-wins: A's COMMIT fails its precondition.
        with pytest.raises(gnitz.GnitzConflictError):
            a.execute_sql("COMMIT", schema_name=sn)
        # A's buffered write is absent — only B's change is durable.
        assert _scan(a, tid) == [(1, 100)]
        # Re-running the whole transaction (no further contention) succeeds.
        a.execute_sql("BEGIN", schema_name=sn)
        a.execute_sql("UPDATE t SET val = val + 1 WHERE pk = 1", schema_name=sn)
        a.execute_sql("COMMIT", schema_name=sn)
        assert _scan(a, tid) == [(1, 101)]
    finally:
        a.close()
        b.close()
        with gnitz.connect(server) as c:
            c.drop_schema(sn)


# ---------------------------------------------------------------------------
# SELECT-only tables are out of scope (accepted write-skew): no precondition
# ---------------------------------------------------------------------------


def test_select_only_table_does_not_conflict(server):
    with gnitz.connect(server) as setup:
        sn = _schema(setup)
        _table(setup, sn, "t")
        _table(setup, sn, "u")
        setup.execute_sql("INSERT INTO t VALUES (1, 0)", schema_name=sn)
        setup.execute_sql("INSERT INTO u VALUES (1, 0)", schema_name=sn)
    a = gnitz.connect(server)
    b = gnitz.connect(server)
    try:
        u_tid, _ = a.resolve_table(sn, "u")
        t_tid, _ = a.resolve_table(sn, "t")
        a.execute_sql("BEGIN", schema_name=sn)
        # SELECT-only on t: read, but NOT recorded in the read-set.
        a.execute_sql("SELECT * FROM t", schema_name=sn)
        # Write u (this IS an RMW on u, recorded).
        a.execute_sql("UPDATE u SET val = val + 1 WHERE pk = 1", schema_name=sn)
        # Concurrent write to t — the SELECT-only table.
        b.execute_sql("UPDATE t SET val = val + 100 WHERE pk = 1", schema_name=sn)
        # A commits despite the concurrent write to t: t is not in A's read-set.
        a.execute_sql("COMMIT", schema_name=sn)
        assert _scan(a, u_tid) == [(1, 1)]
        assert _scan(a, t_tid) == [(1, 100)]
    finally:
        a.close()
        b.close()
        with gnitz.connect(server) as c:
            c.drop_schema(sn)


# ---------------------------------------------------------------------------
# DELETE is an RMW too: a stale-basis DELETE conflicts once, then self-heals
# ---------------------------------------------------------------------------


def test_delete_rmw_self_heals_after_conflict(server):
    with gnitz.connect(server) as setup:
        sn = _schema(setup)
        _table(setup, sn)
        setup.execute_sql("INSERT INTO t VALUES (1, 0), (2, 0)", schema_name=sn)
    a = gnitz.connect(server)  # A's basis is fixed at connect (< B's commit below)
    b = gnitz.connect(server)
    try:
        # B writes t (a different row) between A's connect-time basis and A's
        # DELETE, bumping t's commit LSN past A's basis.
        b.execute_sql("UPDATE t SET val = val + 100 WHERE pk = 2", schema_name=sn)
        # DELETE routes through the same RMW driver: it resolves target PKs at a
        # stale basis, conflicts once (table grain), then the internal retry adopts
        # B's fresh basis, re-resolves, and retracts pk=1 with NO app-visible error.
        a.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)
        tid, _ = a.resolve_table(sn, "t")
        # pk=1 gone; pk=2 carries B's write, never clobbered.
        assert _scan(a, tid) == [(2, 100)]
    finally:
        a.close()
        b.close()
        with gnitz.connect(server) as c:
            c.drop_schema(sn)


# ---------------------------------------------------------------------------
# INSERT ... ON CONFLICT DO UPDATE is an RMW: upsert-increment race stays exact
# ---------------------------------------------------------------------------


def test_insert_on_conflict_do_update_lost_update_closed(server):
    n = 25
    with gnitz.connect(server) as setup:
        sn = _schema(setup)
        _table(setup, sn)
        setup.execute_sql("INSERT INTO t VALUES (1, 0)", schema_name=sn)
    try:
        def worker():
            with gnitz.connect(server) as c:
                for _ in range(n):
                    # The DO UPDATE reads the existing `val` (merge base) and writes
                    # `val + 1`; without the OCC precondition two racing upserts both
                    # read the same `val` and one increment is lost.
                    _retry(
                        c,
                        sn,
                        "INSERT INTO t VALUES (1, 0) ON CONFLICT (pk) DO UPDATE SET val = val + 1",
                    )

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        with gnitz.connect(server) as c:
            tid, _ = c.resolve_table(sn, "t")
            # Exactly 2n upsert-increments landed — no lost update on the ON
            # CONFLICT path either.
            assert _scan(c, tid) == [(1, 2 * n)]
    finally:
        with gnitz.connect(server) as c:
            c.drop_schema(sn)


# ---------------------------------------------------------------------------
# Multi-table transaction: a conflict on ONE read-set table rejects the whole
# bundle (every family's buffered write is discarded)
# ---------------------------------------------------------------------------


def test_multi_table_transaction_conflict_rejects_whole_bundle(server):
    with gnitz.connect(server) as setup:
        sn = _schema(setup)
        _table(setup, sn, "t")
        _table(setup, sn, "u")
        setup.execute_sql("INSERT INTO t VALUES (1, 0)", schema_name=sn)
        setup.execute_sql("INSERT INTO u VALUES (1, 0)", schema_name=sn)
    a = gnitz.connect(server)
    b = gnitz.connect(server)
    try:
        t_tid, _ = a.resolve_table(sn, "t")
        u_tid, _ = a.resolve_table(sn, "u")
        a.execute_sql("BEGIN", schema_name=sn)
        # RMW both tables: COMMIT ships one precondition per read-set table.
        a.execute_sql("UPDATE u SET val = val + 1 WHERE pk = 1", schema_name=sn)
        a.execute_sql("UPDATE t SET val = val + 1 WHERE pk = 1", schema_name=sn)
        # A concurrent commit to just ONE of A's read-set tables (t).
        b.execute_sql("UPDATE t SET val = val + 100 WHERE pk = 1", schema_name=sn)
        # The failed t precondition rejects the entire transaction — no partial
        # commit of the (unconflicted) u family.
        with pytest.raises(gnitz.GnitzConflictError):
            a.execute_sql("COMMIT", schema_name=sn)
        assert _scan(a, u_tid) == [(1, 0)]  # A's +1 to u discarded
        assert _scan(a, t_tid) == [(1, 100)]  # only B's write durable
    finally:
        a.close()
        b.close()
        with gnitz.connect(server) as c:
            c.drop_schema(sn)


# ---------------------------------------------------------------------------
# The raw `client.transaction()` commit path surfaces the typed conflict too
# ---------------------------------------------------------------------------


def test_raw_transaction_context_manager_conflict_is_typed(server):
    # The context-manager commit (PyTxn.__exit__) routes an OCC conflict to the
    # dedicated GnitzConflictError, the same retryable contract as
    # execute_sql("COMMIT") — its shared core buffer records an SQL RMW's
    # read-set, so a concurrent write loses the race at block exit.
    with gnitz.connect(server) as setup:
        sn = _schema(setup)
        _table(setup, sn)
        setup.execute_sql("INSERT INTO t VALUES (1, 0)", schema_name=sn)
    a = gnitz.connect(server)
    b = gnitz.connect(server)
    try:
        tid, _ = a.resolve_table(sn, "t")
        with pytest.raises(gnitz.GnitzConflictError):
            with a.transaction():
                # RMW inside the raw transaction: reads t, records it in the
                # shared read-set, buffers the write.
                a.execute_sql("UPDATE t SET val = val + 1 WHERE pk = 1", schema_name=sn)
                # Concurrent commit to t before the block exits.
                b.execute_sql("UPDATE t SET val = val + 100 WHERE pk = 1", schema_name=sn)
            # Exiting the with-block commits → t precondition fails → conflict.
        # A's buffered write is discarded — only B's change is durable.
        assert _scan(a, tid) == [(1, 100)]
    finally:
        a.close()
        b.close()
        with gnitz.connect(server) as c:
            c.drop_schema(sn)
