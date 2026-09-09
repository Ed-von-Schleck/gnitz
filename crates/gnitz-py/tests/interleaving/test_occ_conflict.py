"""Optimistic-concurrency LSN preconditions on the user-table TXN frame — the
lost-update guard on read-modify-write SQL.

Every SQL mutation that reads before it writes (UPDATE / DELETE with a resolving
WHERE, INSERT ... ON CONFLICT) commits through a one-precondition PUSH_TXN
frame: "commit only if this table has not been written since basis L". The master
checks it under the same table locks that serialize the commit and rejects a
racing write with a retryable conflict. Autocommit statements retry internally
(adopting the server's fresh basis); BEGIN/COMMIT surfaces the conflict.

Run with GNITZ_WORKERS=4 — the conflict window is a distributed commit path.

The table is named `ledger` rather than `t` so an assertion that the conflict
message names the conflicting table cannot be satisfied by a letter occurring
somewhere else in the message.
"""

import contextlib
import threading

import pytest
import gnitz

# A conflict absorbed at the app level is expected; one that never clears is a
# stuck watermark. Bounded so that regression fails the test instead of hanging
# the run — there is no suite-wide test timeout.
_MAX_RETRIES = 200


@pytest.fixture
def occ(client, schema_name, server):
    """`ledger(pk BIGINT PK, val BIGINT)` seeded at (1, 0), plus a factory for
    extra connections closed at teardown. Yields `(schema, connect)`.

    A factory rather than pre-opened handles: a connection's OCC basis is fixed
    at connect, and the self-heal tests below turn on their connect landing
    *before* another connection's commit. Pre-opening here would move that
    ordering out of the tests that depend on it.
    """
    _table(client, schema_name, "ledger")
    client.execute_sql("INSERT INTO ledger VALUES (1, 0)", schema_name=schema_name)
    with contextlib.ExitStack() as stack:
        yield schema_name, lambda: stack.enter_context(gnitz.connect(server))


def _table(client, sn, name):
    client.execute_sql(
        f"CREATE TABLE {name} (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
        schema_name=sn,
    )
    tid, _ = client.resolve_table(sn, name)
    return tid


def _scan(client, tid):
    """Sorted (pk, val, weight) over the relation."""
    return sorted((r.pk, r.val, r.weight) for r in client.scan(tid))


def _retry(c, sn, sql):
    """Run an autocommit statement, absorbing OCC conflicts at the app level
    (what makes a tight race deterministic — the internal bound can exhaust)."""
    for _ in range(_MAX_RETRIES):
        try:
            return c.execute_sql(sql, schema_name=sn)
        except gnitz.GnitzConflictError:
            continue
    raise AssertionError(f"still conflicting after {_MAX_RETRIES} attempts: {sql}")


# ---------------------------------------------------------------------------
# Basis seeding: the premise the rest of the file rests on
# ---------------------------------------------------------------------------


def test_basis_seeded_at_connect_and_advances_on_push(client, schema_name, server):
    """A connection's basis is non-zero from the HELLO ACK watermark and advances
    on its own commits — without it, every precondition below would be checked
    against 0."""
    _table(client, schema_name, "ledger")
    before = client.last_seen_lsn
    client.execute_sql("INSERT INTO ledger VALUES (1, 10)", schema_name=schema_name)
    after = client.last_seen_lsn
    assert after > before

    # A fresh connection is seeded from the HELLO ACK watermark, which now
    # reflects the committed insert — a sound basis with no read op.
    with gnitz.connect(server) as c2:
        assert c2.last_seen_lsn >= after > 0


# ---------------------------------------------------------------------------
# Lost-update closed: the headline guarantee
# ---------------------------------------------------------------------------


def test_lost_update_closed_under_race(client, occ):
    sn, connect = occ
    n = 30

    def worker():
        c = connect()
        for _ in range(n):
            _retry(c, sn, "UPDATE ledger SET val = val + 1 WHERE pk = 1")

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    tid, _ = client.resolve_table(sn, "ledger")
    # Exactly 2n increments landed — no increment silently overwritten.
    assert _scan(client, tid) == [(1, 2 * n, 1)]


# ---------------------------------------------------------------------------
# Single transient collision self-heals via the fresh-basis refresh
# ---------------------------------------------------------------------------


def test_single_transient_collision_self_heals(occ):
    sn, connect = occ
    a = connect()  # A's basis is fixed at connect (< B's commit below)
    b = connect()
    # B commits between A's connect-time basis and A's UPDATE.
    b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 1", schema_name=sn)
    # A's UPDATE conflicts once (its basis predates B's commit), then the
    # internal retry adopts B's fresh basis and succeeds with NO app-visible
    # error. Without the fresh-basis refresh this would deterministically
    # re-conflict until the bound exhausts and raise GnitzConflictError.
    a.execute_sql("UPDATE ledger SET val = val + 1 WHERE pk = 1", schema_name=sn)
    tid, _ = a.resolve_table(sn, "ledger")
    assert _scan(a, tid) == [(1, 101, 1)]  # 0 + 100 (B) + 1 (A): no lost update


# ---------------------------------------------------------------------------
# Different rows conflict spuriously (table grain) but both still succeed
# ---------------------------------------------------------------------------


def test_different_rows_both_succeed(client, occ):
    sn, connect = occ
    n = 25
    client.execute_sql("INSERT INTO ledger VALUES (2, 0)", schema_name=sn)

    def worker(pk):
        c = connect()
        for _ in range(n):
            _retry(c, sn, f"UPDATE ledger SET val = val + 1 WHERE pk = {pk}")

    threads = [threading.Thread(target=worker, args=(pk,)) for pk in (1, 2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    tid, _ = client.resolve_table(sn, "ledger")
    # Table-grain conflicts between pk=1 and pk=2 are absorbed by the app
    # retry; every increment on both rows still lands.
    assert _scan(client, tid) == [(1, n, 1), (2, n, 1)]


# ---------------------------------------------------------------------------
# Autocommit conflict surfaces under sustained contention, and names the table
# ---------------------------------------------------------------------------


def test_autocommit_conflict_surfaces_and_names_table(occ):
    sn, connect = occ
    stop = threading.Event()

    def hammer():
        c = connect()
        while not stop.is_set():
            _retry(c, sn, "UPDATE ledger SET val = val + 1 WHERE pk = 1")

    hammers = [threading.Thread(target=hammer) for _ in range(3)]
    for h in hammers:
        h.start()

    seen = None
    attempts = 0
    try:
        c = connect()
        # Under three threads committing continuously, the four-attempt
        # internal bound exhausts for at least one of these statements.
        for attempts in range(1, 401):
            try:
                c.execute_sql("UPDATE ledger SET val = val + 1 WHERE pk = 1", schema_name=sn)
            except gnitz.GnitzConflictError as e:
                seen = str(e)
                break
    finally:
        stop.set()
        for h in hammers:
            h.join()

    assert seen is not None, (
        f"sustained contention must eventually surface a conflict ({attempts} statements "
        "committed without one)")
    # The client-synthesized message names the conflicting table.
    assert "ledger" in seen and "conflict" in seen.lower(), seen


# ---------------------------------------------------------------------------
# BEGIN/COMMIT: first-committer-wins, no auto-retry, buffered writes discarded
# ---------------------------------------------------------------------------


def test_begin_commit_conflict_and_rerun(occ):
    sn, connect = occ
    a, b = connect(), connect()
    tid, _ = a.resolve_table(sn, "ledger")
    a.execute_sql("BEGIN", schema_name=sn)
    # Reads ledger (resolves rows), buffers the write, records it in the read-set.
    a.execute_sql("UPDATE ledger SET val = val + 1 WHERE pk = 1", schema_name=sn)
    # A concurrent connection commits between A's read and A's COMMIT.
    b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 1", schema_name=sn)
    # First-committer-wins: A's COMMIT fails its precondition.
    with pytest.raises(gnitz.GnitzConflictError):
        a.execute_sql("COMMIT", schema_name=sn)
    # A's buffered write is absent — only B's change is durable.
    assert _scan(a, tid) == [(1, 100, 1)]
    # Re-running the whole transaction (no further contention) succeeds.
    a.execute_sql("BEGIN", schema_name=sn)
    a.execute_sql("UPDATE ledger SET val = val + 1 WHERE pk = 1", schema_name=sn)
    a.execute_sql("COMMIT", schema_name=sn)
    assert _scan(a, tid) == [(1, 101, 1)]


# ---------------------------------------------------------------------------
# SELECT-only tables are out of scope (accepted write-skew): no precondition
# ---------------------------------------------------------------------------


def test_select_only_table_does_not_conflict(client, occ):
    sn, connect = occ
    u_tid = _table(client, sn, "u")
    client.execute_sql("INSERT INTO u VALUES (1, 0)", schema_name=sn)
    a, b = connect(), connect()
    t_tid, _ = a.resolve_table(sn, "ledger")
    a.execute_sql("BEGIN", schema_name=sn)
    # SELECT-only on ledger: read, but NOT recorded in the read-set.
    a.execute_sql("SELECT * FROM ledger", schema_name=sn)
    # Write u (this IS an RMW on u, recorded).
    a.execute_sql("UPDATE u SET val = val + 1 WHERE pk = 1", schema_name=sn)
    # Concurrent write to ledger — the SELECT-only table.
    b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 1", schema_name=sn)
    # A commits despite the concurrent write: ledger is not in A's read-set.
    a.execute_sql("COMMIT", schema_name=sn)
    assert _scan(a, u_tid) == [(1, 1, 1)]
    assert _scan(a, t_tid) == [(1, 100, 1)]


# ---------------------------------------------------------------------------
# DELETE is an RMW too: a stale-basis DELETE conflicts once, then self-heals
# ---------------------------------------------------------------------------


def test_delete_rmw_self_heals_after_conflict(client, occ):
    sn, connect = occ
    client.execute_sql("INSERT INTO ledger VALUES (2, 0)", schema_name=sn)
    a = connect()  # A's basis is fixed at connect (< B's commit below)
    b = connect()
    # B writes ledger (a different row) between A's connect-time basis and A's
    # DELETE, bumping the table's commit LSN past A's basis.
    b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 2", schema_name=sn)
    # DELETE routes through the same RMW driver: it resolves target PKs at a
    # stale basis, conflicts once (table grain), then the internal retry adopts
    # B's fresh basis, re-resolves, and retracts pk=1 with NO app-visible error.
    a.execute_sql("DELETE FROM ledger WHERE pk = 1", schema_name=sn)
    tid, _ = a.resolve_table(sn, "ledger")
    # pk=1 gone; pk=2 carries B's write, never clobbered.
    assert _scan(a, tid) == [(2, 100, 1)]


# ---------------------------------------------------------------------------
# INSERT ... ON CONFLICT DO UPDATE is an RMW: upsert-increment race stays exact
# ---------------------------------------------------------------------------


def test_insert_on_conflict_do_update_lost_update_closed(client, occ):
    sn, connect = occ
    n = 25

    def worker():
        c = connect()
        for _ in range(n):
            # The DO UPDATE reads the existing `val` (merge base) and writes
            # `val + 1`; without the OCC precondition two racing upserts both
            # read the same `val` and one increment is lost.
            _retry(c, sn,
                   "INSERT INTO ledger VALUES (1, 0) "
                   "ON CONFLICT (pk) DO UPDATE SET val = val + 1")

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    tid, _ = client.resolve_table(sn, "ledger")
    # Exactly 2n upsert-increments landed — no lost update on the ON CONFLICT
    # path either.
    assert _scan(client, tid) == [(1, 2 * n, 1)]


# ---------------------------------------------------------------------------
# Multi-table transaction: a conflict on ONE read-set table rejects the whole
# bundle (every family's buffered write is discarded)
# ---------------------------------------------------------------------------


def test_multi_table_transaction_conflict_rejects_whole_bundle(client, occ):
    sn, connect = occ
    u_tid = _table(client, sn, "u")
    client.execute_sql("INSERT INTO u VALUES (1, 0)", schema_name=sn)
    a, b = connect(), connect()
    t_tid, _ = a.resolve_table(sn, "ledger")
    a.execute_sql("BEGIN", schema_name=sn)
    # RMW both tables: COMMIT ships one precondition per read-set table.
    a.execute_sql("UPDATE u SET val = val + 1 WHERE pk = 1", schema_name=sn)
    a.execute_sql("UPDATE ledger SET val = val + 1 WHERE pk = 1", schema_name=sn)
    # A concurrent commit to just ONE of A's read-set tables.
    b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 1", schema_name=sn)
    # The failed precondition rejects the entire transaction — no partial commit
    # of the (unconflicted) u family.
    with pytest.raises(gnitz.GnitzConflictError):
        a.execute_sql("COMMIT", schema_name=sn)
    assert _scan(a, u_tid) == [(1, 0, 1)]      # A's +1 to u discarded
    assert _scan(a, t_tid) == [(1, 100, 1)]    # only B's write durable


# ---------------------------------------------------------------------------
# The raw `client.transaction()` commit path surfaces the typed conflict too
# ---------------------------------------------------------------------------


def test_raw_transaction_context_manager_conflict_is_typed(occ):
    """The context-manager commit (PyTxn.__exit__) routes an OCC conflict to the
    dedicated GnitzConflictError, the same retryable contract as
    execute_sql("COMMIT") — its shared core buffer records an SQL RMW's
    read-set, so a concurrent write loses the race at block exit."""
    sn, connect = occ
    a, b = connect(), connect()
    tid, _ = a.resolve_table(sn, "ledger")
    with pytest.raises(gnitz.GnitzConflictError):
        with a.transaction():
            # RMW inside the raw transaction: reads ledger, records it in the
            # shared read-set, buffers the write.
            a.execute_sql("UPDATE ledger SET val = val + 1 WHERE pk = 1", schema_name=sn)
            # Concurrent commit before the block exits.
            b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 1", schema_name=sn)
        # Exiting the with-block commits → precondition fails → conflict.
    # A's buffered write is discarded — only B's change is durable.
    assert _scan(a, tid) == [(1, 100, 1)]


# ---------------------------------------------------------------------------
# A rename is not a drop: DDL reclamation must leave the watermark alone
# ---------------------------------------------------------------------------


def test_rename_between_read_and_commit_still_conflicts(occ):
    """A durable DROP reclaims the relation's OCC commit watermark. A rename
    reaches the same DDL path but must not: a cleared watermark would let A's
    stale precondition pass, silently losing B's update."""
    sn, connect = occ
    a, b = connect(), connect()
    tid, _ = a.resolve_table(sn, "ledger")
    a.execute_sql("BEGIN", schema_name=sn)
    a.execute_sql("UPDATE ledger SET val = val + 1 WHERE pk = 1", schema_name=sn)
    b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 1", schema_name=sn)
    b.execute_sql("ALTER TABLE ledger RENAME TO ledger2", schema_name=sn)
    with pytest.raises(gnitz.GnitzConflictError):
        a.execute_sql("COMMIT", schema_name=sn)
    # The rename really committed (so the DDL path above really ran), and the
    # tid outlives it: only B's write is durable.
    assert a.resolve_table(sn, "ledger2")[0] == tid
    assert _scan(a, tid) == [(1, 100, 1)]
