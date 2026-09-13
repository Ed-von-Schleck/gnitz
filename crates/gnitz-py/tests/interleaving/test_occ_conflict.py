"""Optimistic concurrency on read-modify-write SQL — the lost-update guard.

Every SQL mutation that reads before it writes (UPDATE / DELETE with a resolving
WHERE, INSERT ... ON CONFLICT) commits only if its table has not been written
since the connection's basis; a racing write is refused with a retryable
conflict. The grain is the table. Autocommit statements retry internally,
adopting the server's fresh basis; BEGIN/COMMIT surfaces the conflict.
"""

import contextlib
import threading

import pytest
import gnitz
from _read import bag, scanned
from _serverproc import join_or_fail

# A conflict absorbed at the app level is expected; one that never clears is a
# stuck watermark, and the bound fails the test instead of hanging the run.
_MAX_RETRIES = 200

_BUMP_1 = "UPDATE {} SET val = val + 1 WHERE pk = 1"


@pytest.fixture
def occ(client, schema_name, server):
    """`ledger` seeded at (1, 0), (2, 0) and `u` at (1, 0), both `(pk, val)`, plus
    a factory for extra connections closed at teardown. Yields `(schema, connect)`.

    A factory rather than pre-opened handles: a connection's OCC basis is fixed
    at connect, and the tests below turn on where that connect lands relative to
    another connection's commit."""
    for sql in ("CREATE TABLE ledger (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                "CREATE TABLE u (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                "INSERT INTO ledger VALUES (1, 0), (2, 0)",
                "INSERT INTO u VALUES (1, 0)"):
        client.execute_sql(sql, schema_name=schema_name)
    with contextlib.ExitStack() as stack:
        yield schema_name, lambda: stack.enter_context(gnitz.connect(server))


def _retry(c, sn, sql):
    """Run an autocommit statement, absorbing OCC conflicts at the app level —
    what makes a tight race deterministic, since the internal bound can exhaust."""
    for _ in range(_MAX_RETRIES):
        try:
            return c.execute_sql(sql, schema_name=sn)
        except gnitz.GnitzConflictError:
            continue
    raise AssertionError(f"still conflicting after {_MAX_RETRIES} attempts: {sql}")


def test_racing_read_modify_writes_lose_no_update(client, occ):
    """Three connections race increments: an UPDATE and an upsert on one row, and
    an UPDATE on the other. Two racers that read the same `val` both write
    `val + 1` unless the loser's commit is refused, so every increment landing is
    the guard; the table-grain conflicts between the two rows are absorbed by the
    retry."""
    sn, connect = occ
    n = 25
    errors = []

    def worker(c, sql):
        try:
            for _ in range(n):
                _retry(c, sn, sql)
        except Exception as e:  # noqa: BLE001 — surfaced after the join
            errors.append(e)

    threads = [threading.Thread(target=worker, args=(connect(), sql), daemon=True) for sql in (
        _BUMP_1.format("ledger"),
        "INSERT INTO ledger VALUES (1, 0) ON CONFLICT (pk) DO UPDATE SET val = val + 1",
        "UPDATE ledger SET val = val + 1 WHERE pk = 2",
    )]
    for t in threads:
        t.start()
    join_or_fail("a racing writer hung", *threads)
    assert not errors, errors
    assert bag(scanned(client, sn, "ledger"), "pk", "val") == {(1, 2 * n): 1, (2, n): 1}


@pytest.mark.parametrize("sql, want", [
    (_BUMP_1.format("ledger"), {(1, 1): 1, (2, 100): 1}),
    ("INSERT INTO ledger VALUES (1, 0) ON CONFLICT (pk) DO UPDATE SET val = val + 1",
     {(1, 1): 1, (2, 100): 1}),
    ("DELETE FROM ledger WHERE pk = 1", {(2, 100): 1}),
], ids=["update", "upsert", "delete"])
def test_a_stale_basis_self_heals(occ, sql, want):
    """A connects, then B commits to the same table, so A's statement conflicts on
    its first attempt. The internal retry adopts the fresh basis and re-resolves:
    no app-visible error, and B's write survives. Without that refresh A would
    re-conflict until the bound exhausted."""
    sn, connect = occ
    a, b = connect(), connect()
    b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 2", schema_name=sn)
    a.execute_sql(sql, schema_name=sn)
    assert bag(scanned(a, sn, "ledger"), "pk", "val") == want


def test_sustained_contention_surfaces_a_conflict_naming_the_table(occ):
    """The internal retry is bounded: under three connections committing
    continuously, some statement exhausts it and raises the typed conflict, which
    names the table it lost on."""
    sn, connect = occ
    stop = threading.Event()
    errors = []

    def hammer(c):
        try:
            while not stop.is_set():
                _retry(c, sn, _BUMP_1.format("ledger"))
        except Exception as e:  # noqa: BLE001 — surfaced after the join
            errors.append(e)

    hammers = [threading.Thread(target=hammer, args=(connect(),), daemon=True) for _ in range(3)]
    for h in hammers:
        h.start()
    seen = None
    try:
        c = connect()
        for _ in range(400):
            try:
                c.execute_sql(_BUMP_1.format("ledger"), schema_name=sn)
            except gnitz.GnitzConflictError as e:
                seen = str(e)
                break
    finally:
        stop.set()
        join_or_fail("a hammer thread hung", *hammers)
    assert not errors, errors
    assert seen is not None, "400 statements under sustained contention never surfaced a conflict"
    assert "ledger" in seen, seen


@pytest.mark.parametrize("via, ledger_read, rename", [
    ("sql", _BUMP_1.format("ledger"), False),
    ("context-manager", _BUMP_1.format("ledger"), False),
    ("sql", _BUMP_1.format("ledger"), True),
    ("sql", "SELECT * FROM ledger", False),
], ids=["commit", "context-manager", "rename-between", "select-only"])
def test_a_transaction_commits_only_if_no_table_it_modified_was_written(occ, via, ledger_read, rename):
    """A writes `u` and reads `ledger` in one transaction; B commits to `ledger`
    before A commits. When A's read was a read-modify-write, A's commit is
    refused whole — its unconflicted write to `u` is discarded too — and a rerun
    succeeds. A SELECT-only read records no precondition (accepted write skew),
    so A commits.

    The context manager commits through the same path as SQL COMMIT. A rename is
    not a drop: the DDL path that reclaims a dropped relation's commit watermark
    must leave a renamed one's, or A's stale precondition would pass and lose B's
    write."""
    sn, connect = occ
    a, b = connect(), connect()
    u_tid, ledger_tid = (a.resolve_table(sn, name)[0] for name in ("u", "ledger"))
    conflicts = ledger_read != "SELECT * FROM ledger"

    def body():
        a.execute_sql(_BUMP_1.format("u"), schema_name=sn)
        a.execute_sql(ledger_read, schema_name=sn)
        b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 1", schema_name=sn)
        if rename:
            b.execute_sql("ALTER TABLE ledger RENAME TO ledger2", schema_name=sn)

    with pytest.raises(gnitz.GnitzConflictError) if conflicts else contextlib.nullcontext():
        if via == "context-manager":
            with a.transaction():
                body()
        else:
            a.execute_sql("BEGIN", schema_name=sn)
            body()
            a.execute_sql("COMMIT", schema_name=sn)

    assert bag(a.scan(u_tid), "pk", "val") == {(1, 0 if conflicts else 1): 1}
    assert bag(a.scan(ledger_tid), "pk", "val") == {(1, 100): 1, (2, 0): 1}
    if rename:
        assert a.resolve_table(sn, "ledger2")[0] == ledger_tid
    if conflicts:
        a.execute_sql("BEGIN", schema_name=sn)
        a.execute_sql(_BUMP_1.format("u"), schema_name=sn)
        a.execute_sql(_BUMP_1.format("ledger2" if rename else "ledger"), schema_name=sn)
        a.execute_sql("COMMIT", schema_name=sn)
        assert bag(a.scan(u_tid), "pk", "val") == {(1, 1): 1}
        assert bag(a.scan(ledger_tid), "pk", "val") == {(1, 101): 1, (2, 0): 1}
