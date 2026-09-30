"""Transactions: what a bundle of writes commits, and when.

SQL's BEGIN … COMMIT and the binding's `client.transaction()` fill one
client-side buffer and ship it as one atomic frame. How a statement inside a
transaction resolves its rows against that buffer is test_dml.py's; this file
is the bundle itself — what it stages, when it is refused whole, and the
per-frame conflict modes only the binding exposes.
"""

import pytest
import gnitz
from _read import bag, rows, scanned
from _schemas import KV

_TU = ("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
       "CREATE TABLE u (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)")


def test_a_transaction_stages_its_writes_until_commit(client, schema_name):
    """Every write inside BEGIN … COMMIT, and every SERIAL id RETURNING hands
    back, is staged: a read and a scan see only committed state, a refused
    statement stages nothing and leaves the transaction open, and COMMIT lands
    the rest. A ROLLBACK leaves no residue — the same key commits after it — but
    the ids it drew stay drawn."""
    sn = schema_name

    def q(sql):
        return client.execute_sql(sql, schema_name=sn)

    q("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL); "
      "CREATE TABLE s (id SERIAL PRIMARY KEY, name TEXT NOT NULL); "
      "INSERT INTO t VALUES (1, 10)")
    assert [(r["type"], r.get("lsn")) for r in q("BEGIN; COMMIT")] == [
        ("TransactionStarted", None), ("TransactionCommitted", 0)]

    q("BEGIN")
    assert bag(rows(client, sn, "INSERT INTO t VALUES (7, 70) RETURNING pk")) == {(7,): 1}
    [a] = rows(client, sn, "INSERT INTO s (name) VALUES ('a') RETURNING id")
    assert q("UPDATE t SET val = 99 WHERE pk = 1")[0]["count"] == 1
    for refused in ("UPDATE t SET nope = 1 WHERE pk = 1", "INSERT INTO t VALUES (2)"):
        with pytest.raises(gnitz.GnitzError):
            q(refused)
    assert bag(rows(client, sn, "SELECT * FROM t")) == bag(scanned(client, sn, "t")) == {(1, 10): 1}
    assert bag(scanned(client, sn, "s")) == {}
    assert q("COMMIT")[0]["lsn"] > 0
    assert bag(scanned(client, sn, "t")) == {(1, 99): 1, (7, 70): 1}

    q("BEGIN; INSERT INTO t VALUES (8, 80)")
    [rolled] = rows(client, sn, "INSERT INTO s (name) VALUES ('b') RETURNING id")
    q("ROLLBACK")
    q("BEGIN; INSERT INTO t VALUES (8, 81); COMMIT")
    [c] = rows(client, sn, "INSERT INTO s (name) VALUES ('c') RETURNING id")
    assert a.id < rolled.id < c.id
    assert bag(scanned(client, sn, "t")) == {(1, 99): 1, (7, 70): 1, (8, 81): 1}
    assert bag(scanned(client, sn, "s")) == {(a.id, "a"): 1, (c.id, "c"): 1}


_REFUSED = {
    "an insert of a committed key beside an unrelated update":
        "BEGIN; UPDATE t SET val = 99 WHERE pk = 2; INSERT INTO t VALUES (1, 100); COMMIT",
    "a duplicate key beside a write to another table":
        "BEGIN; INSERT INTO u VALUES (9, 90); INSERT INTO t VALUES (1, 20); COMMIT",
    "a statement error after a BEGIN in the same call":
        "BEGIN; INSERT INTO t VALUES (3, 30); UPDATE t SET nope = 1; COMMIT",
    "an autocommit INSERT with a bad RETURNING list":
        "INSERT INTO t VALUES (3, 30) RETURNING bogus",
}


@pytest.mark.parametrize("sql", _REFUSED.values(), ids=_REFUSED.keys())
def test_a_refused_write_commits_nothing_and_leaves_no_transaction_open(client, schema_name, sql):
    sn = schema_name
    client.execute_sql(f"{_TU}; INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=sn)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql(sql, schema_name=sn)
    assert (bag(scanned(client, sn, "t")), bag(scanned(client, sn, "u"))) == (
        {(1, 10): 1, (2, 20): 1}, {})
    with pytest.raises(gnitz.GnitzError, match="no transaction open"):
        client.execute_sql("ROLLBACK", schema_name=sn)


def test_a_bundle_over_two_tables_reaches_a_join_as_one_delta_each(client, schema_name):
    """Both sides of a join written, interleaved, in one bundle. Rolled back it
    leaves every relation empty; committed, each table holds its net rows and
    the join holds the pair once — a cross term emitted twice is weight 2."""
    sn = schema_name
    client.execute_sql(
        "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL); "
        "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL); "
        "CREATE VIEW v AS SELECT a.id AS id, a.x AS x, b.y AS y FROM a JOIN b ON a.id = b.id",
        schema_name=sn)
    body = ("BEGIN; INSERT INTO a VALUES (1, 10); INSERT INTO b VALUES (1, 100); "
            "UPDATE a SET x = x + 1 WHERE id = 1; INSERT INTO a VALUES (2, 20); ")

    client.execute_sql(body + "ROLLBACK", schema_name=sn)
    assert [bag(scanned(client, sn, n)) for n in "abv"] == [{}, {}, {}]

    client.execute_sql(body + "COMMIT", schema_name=sn)
    assert [bag(scanned(client, sn, n)) for n in "abv"] == [
        {(1, 11): 1, (2, 20): 1}, {(1, 100): 1}, {(1, 11, 100): 1}]


# (committed rows of `a`, frames in bundle order, the bags of `a` and `b` after
# the commit — or None where the bundle is refused and both keep what they
# held). A frame is `(table, rows, mode)`: `(pk, val, weight)` rows pushed in
# that conflict mode, or keys for `delete`.
_BUNDLES = {
    "an error-mode insert of a committed key aborts the sibling table":
        ([(1, 10)], [("b", [(9, 90, 1)], "update"), ("a", [(1, 99, 1)], "error")], None),
    "delete then error-mode insert is the replace idiom":
        ([(1, 10)], [("a", [1], "delete"), ("a", [(1, 20, 1)], "error")], ({(1, 20): 1}, {})),
    "an error-mode insert sees an earlier frame's insert of its key":
        ([], [("a", [(1, 10, 1)], "update"), ("a", [(1, 20, 1)], "error")], None),
    "a blind delete leaves an uncommitted key free for an error-mode insert":
        ([], [("a", [5], "delete"), ("a", [(5, 50, 1)], "error")], ({(5, 50): 1}, {})),
    "an error-mode frame's own delete does not hide its insert of a committed key":
        ([(1, 10)], [("a", [(1, 99, 1), (1, 98, -1)], "error")], None),
    "an error-mode frame may delete a key and re-insert it":
        ([(1, 10)], [("a", [(1, 98, -1), (1, 20, 1)], "error")], ({(1, 20): 1}, {})),
    "an error-mode re-insert after its own delete is no duplicate":
        ([], [("a", [(1, 10, 1), (1, 10, -1), (1, 20, 1)], "error")], ({(1, 20): 1}, {})),
    "an error-mode frame's own delete does not hide an earlier frame's insert":
        ([], [("a", [(1, 10, 1)], "update"), ("a", [(1, 20, 1), (1, 20, -1)], "error")], None),
    "an error-mode row at weight 2 is a duplicate":
        ([], [("a", [(1, 10, 2)], "error")], None),
    "a weight-0 row is no row":
        ([], [("a", [(1, 10, 1), (2, 20, 0)], "update"), ("b", [(3, 30, 1)], "update")],
         ({(1, 10): 1}, {(3, 30): 1})),
    "a system table is no target":
        ([], [("a", [(1, 10, 1)], "update"), (gnitz.TABLE_TAB, [(1, 10, 1)], "update")], None),
    "an exception inside the block discards every frame":
        ([], [("a", [(1, 10, 1)], "update"), ("b", [(2, 20, 1)], "update"), (None, None, "raise")], None),
}


@pytest.mark.parametrize("committed,frames,after", _BUNDLES.values(), ids=_BUNDLES.keys())
def test_a_binary_bundle_validates_its_frames_in_order(client, schema_name, committed, frames, after):
    tids = {n: client.create_table(schema_name, n, KV) for n in "ab"}

    def batch(rows):
        return gnitz.ZSetBatch(KV).extend([{"pk": p, "val": v, "_weight": w} for p, v, w in rows])

    if committed:
        client.push(tids["a"], batch([(p, v, 1) for p, v in committed]))

    def run():
        with client.transaction() as txn:
            for table, rows, mode in frames:
                tid = tids.get(table, table)
                if mode == "raise":
                    raise RuntimeError("abandon the bundle")
                if mode == "delete":
                    txn.delete(tid, KV, rows)
                else:
                    txn.push(tid, batch(rows), mode)

    if after is None:
        with pytest.raises((gnitz.GnitzError, RuntimeError)):
            run()
        after = (dict.fromkeys(committed, 1), {})
    else:
        run()
    assert (bag(scanned(client, schema_name, "a")), bag(scanned(client, schema_name, "b"))) == after


def test_a_bundle_into_a_table_dropped_before_commit_is_not_found(client, schema_name, server):
    """The commit is validated against the catalog it reaches, not the one the
    transaction began under, and the refusal names the relation it concerns."""
    tid = client.create_table(schema_name, "gone", KV)
    with pytest.raises(gnitz.GnitzNotFoundError, match=str(tid)):
        with client.transaction() as txn:
            txn.push(tid, gnitz.ZSetBatch(KV).extend([{"pk": 1, "val": 1}]), "update")
            with gnitz.connect(server) as other:
                other.execute_sql("DROP TABLE gone", schema_name=schema_name)


def test_a_bundle_the_sal_cannot_hold_rolls_back_whole(tiny_sal_server):
    """Each family fits the 16 MiB SAL alone, the two together do not: the
    second one's refusal takes back the first, already laid out, and the SAL is
    left able to commit the next write."""
    target, _proc = tiny_sal_server
    schema = gnitz.Schema([gnitz.ColumnDef("pk", gnitz.TypeCode.U64),
                           gnitz.ColumnDef("pad", gnitz.TypeCode.STRING)], [0])
    pad = "x" * 8_000

    def wide(n):  # ~10 MB of rows
        return gnitz.ZSetBatch(schema).extend([{"pk": i, "pad": pad} for i in range(n)])

    with gnitz.connect(target) as client:
        sn = "sal"
        client.create_schema(sn)
        a, b = (client.create_table(sn, n, schema) for n in "ab")
        with pytest.raises(gnitz.GnitzSalFullError):
            with client.transaction() as txn:
                txn.push(a, wide(1_300), "update")
                txn.push(b, wide(1_300), "update")
        assert (bag(scanned(client, sn, "a")), bag(scanned(client, sn, "b"))) == ({}, {})

        client.push(a, gnitz.ZSetBatch(schema).extend([{"pk": 1, "pad": "y"}]))
        assert bag(scanned(client, sn, "a")) == {(1, "y"): 1}
