"""Optimistic concurrency on read-modify-write SQL — the lost-update guard.

Every SQL mutation that reads before it writes (UPDATE / DELETE with a resolving
WHERE, INSERT ... ON CONFLICT) commits only if its table has not been written
since the read it was built from: the basis is that read's watermark. A racing
write is refused with a retryable conflict; the grain is the table. Autocommit
statements retry internally on a real conflict, re-reading each time;
BEGIN/COMMIT surfaces the conflict.

The writes that take no basis — a duplicate-key INSERT's pre-flight, and an FK
check — hold their table guard instead, and their refusals must be exactly as
decisive while other writers run.
"""

import contextlib
import threading

import pytest
import gnitz
from _read import bag, scanned
from _serverproc import join_or_fail, spawn

# A conflict absorbed at the app level is expected; one that never clears is a
# stuck watermark, and the bound fails the test instead of hanging the run.
_MAX_RETRIES = 200

_BUMP_1 = "UPDATE {} SET val = val + 1 WHERE pk = 1"


@pytest.fixture
def connect(client, server):
    """`ledger` seeded at (1, 0), (2, 0) and `u` at (1, 0), both `(pk, val)`.
    Yields a factory for extra connections to them, closed at teardown."""
    for sql in ("CREATE TABLE ledger (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                "CREATE TABLE u (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                "INSERT INTO ledger VALUES (1, 0), (2, 0)",
                "INSERT INTO u VALUES (1, 0)"):
        client.execute_sql(sql)
    with contextlib.ExitStack() as stack:
        yield lambda: stack.enter_context(gnitz.connect(server, schema=client.schema))


def _retry(c, sql):
    """Run an autocommit statement, absorbing OCC conflicts at the app level —
    what makes a tight race deterministic, since the internal bound can exhaust."""
    for _ in range(_MAX_RETRIES):
        try:
            return c.execute_sql(sql)
        except gnitz.GnitzConflictError:
            continue
    raise AssertionError(f"still conflicting after {_MAX_RETRIES} attempts: {sql}")


def test_racing_read_modify_writes_lose_no_update(client, connect):
    """Three connections race increments: an UPDATE and an upsert on one row, and
    an UPDATE on the other. Two racers that read the same `val` both write
    `val + 1` unless the loser's commit is refused, so every increment landing is
    the guard; the table-grain conflicts between the two rows are absorbed by the
    retry."""
    n = 25

    def worker(c, sql):
        for _ in range(n):
            _retry(c, sql)

    join_or_fail("a racing writer hung", *[spawn(worker, connect(), sql) for sql in (
        _BUMP_1.format("ledger"),
        "INSERT INTO ledger VALUES (1, 0) ON CONFLICT (pk) DO UPDATE SET val = val + 1",
        "UPDATE ledger SET val = val + 1 WHERE pk = 2",
    )])
    assert bag(scanned(client, "ledger"), "pk", "val") == {(1, 2 * n): 1, (2, n): 1}


@pytest.mark.parametrize("sql, want", [
    (_BUMP_1.format("ledger"), {(1, 1): 1, (2, 100): 1}),
    ("INSERT INTO ledger VALUES (1, 0) ON CONFLICT (pk) DO UPDATE SET val = val + 1",
     {(1, 1): 1, (2, 100): 1}),
    ("DELETE FROM ledger WHERE pk = 1", {(2, 100): 1}),
], ids=["update", "upsert", "delete"])
def test_a_foreign_commit_costs_no_extra_round(connect, sql, want):
    """A connects, then B commits to the same table, then A runs its statement. A's
    read comes after B's ACK, so its watermark covers B's commit and the first
    attempt commits: one RESOLVE, one read, one push — and B's write survives."""
    a, b = connect(), connect()
    b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 2")
    before = a.requests_sent
    a.execute_sql(sql)
    assert a.requests_sent - before == 3, "RESOLVE, SCAN_SPEC and PUSH_TXN"
    assert bag(scanned(a, "ledger"), "pk", "val") == want


@pytest.mark.parametrize("between", [False, True], ids=["before-begin", "between-two-tables"])
def test_a_transaction_commits_over_a_write_its_reads_saw(connect, between):
    """B commits to `ledger` before A's transaction reads it — before BEGIN, or
    after A has already written `u` inside the transaction. Each family's basis is
    the watermark of its own read, which saw B's commit, so COMMIT passes and
    every write survives."""
    a, b = connect(), connect()

    def b_commits():
        b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 2")

    if not between:
        b_commits()
    a.execute_sql("BEGIN")
    a.execute_sql(_BUMP_1.format("u"))
    if between:
        b_commits()
    a.execute_sql(_BUMP_1.format("ledger"))
    a.execute_sql("COMMIT")
    assert bag(scanned(a, "u"), "pk", "val") == {(1, 1): 1}
    assert bag(scanned(a, "ledger"), "pk", "val") == {(1, 1): 1, (2, 100): 1}


@pytest.mark.parametrize("via, ledger_read, rename", [
    ("sql", _BUMP_1.format("ledger"), False),
    ("context-manager", _BUMP_1.format("ledger"), False),
    ("sql", _BUMP_1.format("ledger"), True),
    ("sql", "SELECT * FROM ledger", False),
], ids=["commit", "context-manager", "rename-between", "select-only"])
def test_a_transaction_commits_only_if_no_table_it_modified_was_written(connect, via, ledger_read, rename):
    """A writes `u` and reads `ledger` in one transaction; B commits to `ledger`
    before A commits. When A's read was a read-modify-write, A's commit is
    refused whole — its unconflicted write to `u` is discarded too — and a rerun
    succeeds. A SELECT-only read records no precondition (accepted write skew),
    so A commits.

    The context manager commits through the same path as SQL COMMIT. A rename is
    not a drop: the DDL path that reclaims a dropped relation's commit watermark
    must leave a renamed one's, or A's stale precondition would pass and lose B's
    write."""
    a, b = connect(), connect()
    (u_tid, u_schema), (ledger_tid, ledger_schema) = (a.resolve_table(name) for name in ("u", "ledger"))
    conflicts = ledger_read != "SELECT * FROM ledger"

    def body():
        a.execute_sql(_BUMP_1.format("u"))
        a.execute_sql(ledger_read)
        b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 1")
        if rename:
            b.execute_sql("ALTER TABLE ledger RENAME TO ledger2")

    with pytest.raises(gnitz.GnitzConflictError) if conflicts else contextlib.nullcontext():
        if via == "context-manager":
            with a.transaction():
                body()
        else:
            a.execute_sql("BEGIN")
            body()
            a.execute_sql("COMMIT")

    assert bag(a.scan(u_tid, u_schema), "pk", "val") == {(1, 0 if conflicts else 1): 1}
    assert bag(a.scan(ledger_tid, ledger_schema), "pk", "val") == {(1, 100): 1, (2, 0): 1}
    if rename:
        assert a.resolve_table("ledger2")[0] == ledger_tid
    if conflicts:
        a.execute_sql("BEGIN")
        a.execute_sql(_BUMP_1.format("u"))
        a.execute_sql(_BUMP_1.format("ledger2" if rename else "ledger"))
        a.execute_sql("COMMIT")
        assert bag(a.scan(u_tid, u_schema), "pk", "val") == {(1, 1): 1}
        assert bag(a.scan(ledger_tid, ledger_schema), "pk", "val") == {(1, 101): 1, (2, 0): 1}


# ── Refusals no concurrent writer may dilute ─────────────────────────────────

def test_insert_duplicate_key_raises_while_upserts_are_in_flight(server, client):
    """`INSERT` ships conflict mode `Error`, whose duplicate-key rejection is a
    master-side pre-flight against committed state — there is no apply-time
    backstop. It therefore keeps the exclusive table guard even though binary
    upserts to the same table hold the shared one, and its verdict must survive
    that concurrency: a duplicate `INSERT` still raises, and never degrades
    into a silent upsert."""
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
                       "INSERT INTO t VALUES (1, 10)")
    tid, schema = client.resolve_table("t")

    stop = threading.Event()

    def upserter(seed):
        # Disjoint PKs, so nothing here can be the source of a duplicate.
        with gnitz.connect(server) as c:
            i = 0
            while not stop.is_set():
                c.push(tid, gnitz.ZSetBatch(schema).extend([{"pk": 1000 * seed + (i % 64) + 1, "val": i}]))
                i += 1

    pushers = [spawn(upserter, s) for s in range(1, 5)]
    try:
        for _ in range(20):
            with pytest.raises(gnitz.GnitzIntegrityError, match="(?i)duplicate key"):
                client.execute_sql("INSERT INTO t VALUES (1, 20)")
    finally:
        stop.set()
        join_or_fail("a concurrent upsert hung", *pushers)

    # The rejected INSERTs left the original row untouched, at weight 1.
    assert {k: w for k, w in bag(client.scan(tid, schema)).items() if k[0] == 1} == {(1, 10): 1}


@pytest.mark.parametrize("ddl,parent_table,parent,child_table,child", [
    ("CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY); "
     "CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL REFERENCES parent(id))",
     "parent", {"id": 1}, "child", {"cid": 2, "pid": 1}),
    ("CREATE TABLE tree (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT REFERENCES tree(id))",
     "tree", {"id": 1, "parent_id": None}, "tree", {"id": 2, "parent_id": 1}),
], ids=["two-tables", "self-fk"])
def test_fk_enforced_under_concurrent_push_and_delete(
        server, client, ddl, parent_table, parent, child_table, child):
    """A binary push and a binary delete are both conflict-mode `Update`, so the
    only thing keeping them off the shared table lock — where they would run
    concurrently and each miss the other's uncommitted rows — is the FK terms of
    the validator's predicate.

    Each round races inserting a child against deleting its parent: exactly one
    is rejected, and what is left is exactly the winner's outcome. With a self-FK
    both endpoints are one table, so the lock set dedupes to a single tid — the
    writer must still take that one guard exclusively."""
    client.execute_sql(ddl)
    ptid, pschema = client.resolve_table(parent_table)
    ctid, cschema = client.resolve_table(child_table)
    parent_row, child_row = tuple(parent.values()), tuple(child.values())
    client.push(ptid, gnitz.ZSetBatch(pschema).extend([parent]))

    with gnitz.connect(server) as a, gnitz.connect(server) as b:
        for _ in range(20):
            refused = []
            start = threading.Barrier(2)

            def race(label, write):
                start.wait()
                try:
                    write()
                except gnitz.GnitzIntegrityError:
                    refused.append(label)

            join_or_fail(
                "a racing FK write hung",
                spawn(race, "push", lambda: a.push(ctid, gnitz.ZSetBatch(cschema).extend([child]))),
                spawn(race, "delete", lambda: b.delete(ptid, pschema, [parent_row[0]])))
            assert len(refused) == 1, f"expected exactly one of the two writes to be rejected, got {refused}"

            held = {**bag(scanned(client, parent_table)), **bag(scanned(client, child_table))}
            push_won = refused == ["delete"]
            assert held == (dict.fromkeys([parent_row, child_row], 1) if push_won else {}), held
            # Back to {parent present, child absent}, touching only what is
            # there — a retraction of an absent row is not a no-op.
            if push_won:
                client.delete(ctid, cschema, [child_row[0]])
            else:
                client.push(ptid, gnitz.ZSetBatch(pschema).extend([parent]))
