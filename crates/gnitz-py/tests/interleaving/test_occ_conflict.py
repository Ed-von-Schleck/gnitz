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
from _schemas import KV, kv
from _serverproc import join_or_fail, spawn

# A conflict absorbed at the app level is expected; one that never clears is a
# stuck watermark, and the bound fails the test instead of hanging the run.
_MAX_RETRIES = 200

_BUMP_1 = "UPDATE {} SET val = val + 1 WHERE pk = 1"


@pytest.fixture
def ledger(client):
    """`ledger` seeded at (1, 0), (2, 0) and `u` at (1, 0), both `(pk, val)`."""
    client.execute_sql(
        "CREATE TABLE ledger (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "CREATE TABLE u (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
        "INSERT INTO ledger VALUES (1, 0), (2, 0); "
        "INSERT INTO u VALUES (1, 0)")


def _retry(c, sql):
    """Run an autocommit statement, absorbing OCC conflicts at the app level —
    what makes a tight race deterministic, since the internal bound can exhaust."""
    for _ in range(_MAX_RETRIES):
        try:
            return c.execute_sql(sql)
        except gnitz.GnitzConflictError:
            continue
    raise AssertionError(f"still conflicting after {_MAX_RETRIES} attempts: {sql}")


def test_racing_read_modify_writes_lose_no_update(client, ledger, peer):
    """Connections race increments: an UPDATE and an upsert on one row, and an
    UPDATE on the other. Two racers that read the same `val` both write `val + 1`
    unless the loser's commit is refused, so every increment landing is the
    guard; the table-grain conflicts between the rows are absorbed by the retry.

    A third row races an UPDATE against its DELETE: an UPDATE built on a read
    from before the DELETE committed would bring the row back."""
    n = 25
    client.execute_sql("INSERT INTO ledger VALUES (3, 0)")

    def worker(c, sql):
        for _ in range(n):
            _retry(c, sql)

    join_or_fail("a racing writer hung", *[spawn(worker, peer(), sql) for sql in (
        _BUMP_1.format("ledger"),
        "INSERT INTO ledger VALUES (1, 0) ON CONFLICT (pk) DO UPDATE SET val = val + 1",
        "UPDATE ledger SET val = val + 1 WHERE pk = 2",
        "UPDATE ledger SET val = val + 1 WHERE pk = 3",
        "DELETE FROM ledger WHERE pk = 3",
    )])
    assert bag(scanned(client, "ledger")) == {(1, 2 * n): 1, (2, n): 1}


@pytest.mark.parametrize("sql, want", [
    (_BUMP_1.format("ledger"), {(1, 1): 1, (2, 100): 1}),
    ("INSERT INTO ledger VALUES (1, 0) ON CONFLICT (pk) DO UPDATE SET val = val + 1",
     {(1, 1): 1, (2, 100): 1}),
    ("DELETE FROM ledger WHERE pk = 1", {(2, 100): 1}),
], ids=["update", "upsert", "delete"])
def test_a_commit_before_the_read_is_no_conflict(ledger, peer, sql, want):
    """A connects, then B commits to the same table, then A runs its statement in
    a transaction, which surfaces a conflict an autocommit would retry away. A's
    read comes after B's ACK, so its watermark covers B's commit: A commits, and
    B's write survives."""
    a, b = peer(), peer()
    b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 2")
    a.execute_sql("BEGIN")
    a.execute_sql(sql)
    a.execute_sql("COMMIT")
    assert bag(scanned(a, "ledger")) == want


@pytest.mark.parametrize("b_at, via, rmw, rename", [
    ("before-begin", "sql", True, False),
    ("before-read", "sql", True, False),
    ("after-read", "sql", True, False),
    ("after-read", "context-manager", True, False),
    ("after-read", "sql", True, True),
    ("after-read", "sql", False, False),
], ids=["before-begin", "before-read", "after-read", "context-manager", "rename-between", "select-only"])
def test_a_transaction_commits_only_if_no_table_it_modified_was_written(ledger, peer, b_at, via, rmw, rename):
    """A writes `u` and reads `ledger` in one transaction; B commits to `ledger`
    before A's BEGIN, between A's write to `u` and its read of `ledger`, or after
    that read. Each family's basis is the watermark of its own read, so only a
    commit after a read-modify-write read refuses A's commit, and refuses it whole
    — its unconflicted write to `u` is discarded too — and a rerun succeeds. A
    SELECT-only read records no precondition (accepted write skew), so A commits.

    The context manager commits through the same path as SQL COMMIT. B renaming
    `ledger` and back leaves it resolving as A planned against, so only its commit
    watermark can refuse A: the DDL path that reclaims a dropped relation's
    watermark must leave a renamed one's, or A's stale precondition would pass and
    lose B's write."""
    a, b = peer(), peer()
    ledger_read = _BUMP_1.format("ledger") if rmw else "SELECT * FROM ledger"
    conflicts = rmw and b_at == "after-read"

    def b_commits_at(at):
        if at == b_at:
            b.execute_sql("UPDATE ledger SET val = val + 100 WHERE pk = 1")
            if rename:
                b.execute_sql("ALTER TABLE ledger RENAME TO ledger2; ALTER TABLE ledger2 RENAME TO ledger")

    def body():
        a.execute_sql(_BUMP_1.format("u"))
        b_commits_at("before-read")
        a.execute_sql(ledger_read)
        b_commits_at("after-read")

    b_commits_at("before-begin")
    # A stale descriptor is refused as a conflict too; the text pins the watermark's.
    with pytest.raises(gnitz.GnitzConflictError, match="written after the read") if conflicts \
            else contextlib.nullcontext():
        if via == "context-manager":
            with a.transaction():
                body()
        else:
            a.execute_sql("BEGIN")
            body()
            a.execute_sql("COMMIT")

    assert bag(scanned(a, "u")) == {(1, 0 if conflicts else 1): 1}
    assert bag(scanned(a, "ledger")) == {(1, 100 + (rmw and not conflicts)): 1, (2, 0): 1}
    if conflicts:
        a.execute_sql("BEGIN")
        a.execute_sql(_BUMP_1.format("u"))
        a.execute_sql(_BUMP_1.format("ledger"))
        a.execute_sql("COMMIT")
        assert bag(scanned(a, "u")) == {(1, 1): 1}
        assert bag(scanned(a, "ledger")) == {(1, 101): 1, (2, 0): 1}


# ── Refusals no concurrent writer may dilute ─────────────────────────────────

def _race(refusal, **writes):
    """Run each named write on a `spawn`ed thread, all released at once, and
    return the names of those refused. A refusal must be an integrity error
    reading `refusal`; any other error fails the test."""
    start = threading.Barrier(len(writes))
    refused = []

    def run(name, write):
        start.wait()
        try:
            write()
        except gnitz.GnitzIntegrityError as e:
            if refusal not in str(e):
                raise
            refused.append(name)

    join_or_fail("a racing write hung", *(spawn(run, name, write) for name, write in writes.items()))
    return refused


def test_a_duplicate_key_insert_is_decided_against_a_racing_upsert(client, peer):
    """`INSERT` ships conflict mode `Error`, whose duplicate-key rejection is a
    master-side pre-flight against committed state — there is no apply-time
    backstop. It therefore keeps the exclusive table guard even though a binary
    upsert holds the shared one.

    Each round races an INSERT of a fresh key against an upsert of it. Either the
    INSERT commits first and the upsert overwrites it, or the upsert does and the
    INSERT is refused: the key ends at the upsert's value, at weight 1, both ways.
    A pre-flight run beside the upsert would pass and commit the INSERT's row too."""
    tid = client.create_table("t", KV)
    a, b = peer(), peer()
    for k in range(1, 21):
        _race("duplicate key value",
              insert=lambda: a.execute_sql(f"INSERT INTO t VALUES ({k}, 1)"),
              upsert=lambda: b.push(tid, kv((k, 2))))
        assert bag(client.seek(tid, KV, pk=k)) == {(k, 2): 1}, k


@pytest.mark.parametrize("ddl,parent_table,parent,child_table,child", [
    ("CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY); "
     "CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY, pid BIGINT NOT NULL REFERENCES parent(id))",
     "parent", {"id": 1}, "child", {"cid": 2, "pid": 1}),
    ("CREATE TABLE tree (id BIGINT NOT NULL PRIMARY KEY, parent_id BIGINT REFERENCES tree(id))",
     "tree", {"id": 1, "parent_id": None}, "tree", {"id": 2, "parent_id": 1}),
], ids=["two-tables", "self-fk"])
def test_fk_enforced_under_concurrent_push_and_delete(
        client, peer, ddl, parent_table, parent, child_table, child):
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

    a, b = peer(), peer()
    for _ in range(20):
        refused = _race("Foreign Key violation",
                        push=lambda: a.push(ctid, gnitz.ZSetBatch(cschema).extend([child])),
                        delete=lambda: b.delete(ptid, pschema, [parent_row[0]]))
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
