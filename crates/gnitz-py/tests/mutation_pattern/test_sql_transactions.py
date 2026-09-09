"""E2E tests for SQL transactions: BEGIN / COMMIT / ROLLBACK.

The SQL front end (`execute_sql`) drives the client's transaction buffer:
`BEGIN` opens it, each DML statement buffers its write batch (and resolves
UPDATE/DELETE/ON CONFLICT against a read-your-own-writes overlay of the buffer),
`COMMIT` ships the whole bundle as one atomic frame, `ROLLBACK` discards it.

Distinct from `test_transactions.py`, which exercises the lower-level
`with client.transaction()` binding on the same core buffer.
"""

import re

import pytest
import gnitz


def _table(client, schema_name, name="t", cols="pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL"):
    client.execute_sql(f"CREATE TABLE {name} ({cols})", schema_name=schema_name)
    tid, _ = client.resolve_table(schema_name, name)
    return tid


def _scan(client, tid):
    """Sorted `(pk, val)`, asserting every base row sits at the clamped weight 1
    — so a bundle applied twice fails here instead of reading as correct."""
    rows = list(client.scan(tid))
    assert all(r.weight == 1 for r in rows), f"unclamped weight in {tid}"
    return sorted((r.pk, r.val) for r in rows)


def _sql(client, schema_name, *statements):
    return client.execute_sql("; ".join(statements), schema_name=schema_name)


# ---------------------------------------------------------------------------
# Atomicity: multi-table commit, rollback, empty


# ---------------------------------------------------------------------------


def test_two_table_atomic_commit(client, schema_name):
    a = _table(client, schema_name, "a")
    b = _table(client, schema_name, "b")
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO a VALUES (1, 10), (2, 20)",
         "INSERT INTO b VALUES (5, 50)",
         "COMMIT")
    assert _scan(client, a) == [(1, 10), (2, 20)]
    assert _scan(client, b) == [(5, 50)]


def test_rollback_discards_everything(client, schema_name):
    a = _table(client, schema_name, "a")
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO a VALUES (1, 10)",
         "INSERT INTO a VALUES (2, 20)",
         "ROLLBACK")
    assert _scan(client, a) == []


def test_empty_commit_lsn_zero(client, schema_name):
    res = _sql(client, schema_name, "BEGIN", "COMMIT")
    assert res[0]["type"] == "TransactionStarted"
    assert res[1]["type"] == "TransactionCommitted"
    assert res[1]["lsn"] == 0


def test_transaction_spans_multiple_execute_sql_calls(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("BEGIN", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    # Invisible until COMMIT (staged write-set).
    assert _scan(client, t) == []
    client.execute_sql("INSERT INTO t VALUES (2, 20)", schema_name=schema_name)
    res = client.execute_sql("COMMIT", schema_name=schema_name)
    assert res[0]["lsn"] > 0
    assert _scan(client, t) == [(1, 10), (2, 20)]


# ---------------------------------------------------------------------------
# Transaction control state machine


# ---------------------------------------------------------------------------


def test_begin_twice_errors(client, schema_name):
    client.execute_sql("BEGIN", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError) as exc:
        client.execute_sql("BEGIN", schema_name=schema_name)
    assert "already open" in str(exc.value).lower()
    client.execute_sql("ROLLBACK", schema_name=schema_name)


def test_commit_or_rollback_without_transaction_errors(client, schema_name):
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("COMMIT", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("ROLLBACK", schema_name=schema_name)


def test_rejected_transaction_clauses(client, schema_name):
    # Each clause names itself in the rejection: the guards live in the
    # statement's own dispatch arm, which has no other pure entry point.
    for bad, clause in [
        ("BEGIN READ ONLY", "transaction modes"),
        ("START TRANSACTION ISOLATION LEVEL SERIALIZABLE", "transaction modes"),
        ("BEGIN DEFERRED", "BEGIN modifier"),
        ("COMMIT AND CHAIN", "AND CHAIN"),
        ("ROLLBACK AND CHAIN", "AND CHAIN"),
        ("ROLLBACK TO SAVEPOINT sp", "TO SAVEPOINT"),
    ]:
        with pytest.raises(gnitz.GnitzError, match=re.escape(clause)):
            client.execute_sql(bad, schema_name=schema_name)
    # A rejected BEGIN opened no transaction, and the rejected COMMIT/ROLLBACK
    # closed none: a COMMIT now finds no transaction open.
    with pytest.raises(gnitz.GnitzError) as exc:
        client.execute_sql("COMMIT", schema_name=schema_name)
    assert "no transaction open" in str(exc.value).lower()


# ---------------------------------------------------------------------------
# Read-your-own-writes: UPDATE / DELETE resolve against the buffer


# ---------------------------------------------------------------------------


def test_update_compounds(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    res = _sql(client, schema_name,
               "BEGIN",
               "UPDATE t SET val = val + 1 WHERE pk = 1",
               "UPDATE t SET val = val + 1 WHERE pk = 1",
               "COMMIT")
    assert res[1]["count"] == 1
    assert res[2]["count"] == 1
    assert _scan(client, t) == [(1, 12)]


def test_delete_then_update_no_resurrection(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    res = _sql(client, schema_name,
               "BEGIN",
               "DELETE FROM t WHERE pk = 1",
               "UPDATE t SET val = 99 WHERE pk = 1",
               "COMMIT")
    assert res[1]["count"] == 1  # DELETE matched
    assert res[2]["count"] == 0  # UPDATE matched nothing (row is gone)
    assert _scan(client, t) == []


def test_double_delete_reports_one_then_zero(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    res = _sql(client, schema_name,
               "BEGIN",
               "DELETE FROM t WHERE pk = 1",
               "DELETE FROM t WHERE pk = 1",
               "COMMIT")
    assert res[1]["count"] == 1
    assert res[2]["count"] == 0
    assert _scan(client, t) == []


def test_delete_reinsert_then_update_sees_reinserted_payload(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    _sql(client, schema_name,
         "BEGIN",
         "DELETE FROM t WHERE pk = 1",
         "INSERT INTO t VALUES (1, 100)",
         "UPDATE t SET val = val + 1 WHERE pk = 1",  # over the reinserted 100
         "COMMIT")
    assert _scan(client, t) == [(1, 101)]


def test_transaction_born_row_discovered(client, schema_name):
    t = _table(client, schema_name, "t")
    # A row born in the transaction is found by UPDATE / DELETE / no-WHERE.
    res = _sql(client, schema_name,
               "BEGIN",
               "INSERT INTO t VALUES (5, 50)",
               "UPDATE t SET val = 99 WHERE pk = 5",
               "COMMIT")
    assert res[2]["count"] == 1
    assert _scan(client, t) == [(5, 99)]

    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO t VALUES (6, 60)",
         "DELETE FROM t WHERE pk = 6",
         "COMMIT")
    assert _scan(client, t) == [(5, 99)]  # 6 born then removed


def test_no_where_update_touches_committed_and_born_rows(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    res = _sql(client, schema_name,
               "BEGIN",
               "INSERT INTO t VALUES (2, 20)",
               "UPDATE t SET val = 0",  # no WHERE: committed 1 + born 2
               "COMMIT")
    assert res[2]["count"] == 2
    assert _scan(client, t) == [(1, 0), (2, 0)]


# ---------------------------------------------------------------------------
# WHERE re-evaluation against the effective (overlaid) payload


# ---------------------------------------------------------------------------


def test_where_reeval_non_indexed_column(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    # Buffer val = 5, then a WHERE on the committed value (10) must match
    # nothing — the effective value is 5.
    res = _sql(client, schema_name,
               "BEGIN",
               "UPDATE t SET val = 5 WHERE pk = 1",
               "UPDATE t SET val = 100 WHERE val = 10",
               "COMMIT")
    assert res[2]["count"] == 0
    assert _scan(client, t) == [(1, 5)]


def test_where_reeval_indexed_column(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("CREATE INDEX ON t (val)", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    # The buffered-row overlay is unindexed, so inclusion is exact regardless
    # of the index: a committed row moved out of the WHERE span by a buffered
    # override matches 0; the buffered value is what the predicate sees.
    res = _sql(client, schema_name,
               "BEGIN",
               "UPDATE t SET val = 5 WHERE pk = 1",
               "UPDATE t SET val = 100 WHERE val = 10",  # indexed col, old value
               "COMMIT")
    assert res[2]["count"] == 0
    assert _scan(client, t) == [(1, 5)]


# ---------------------------------------------------------------------------
# SELECT reads the committed basis only (writes staged until COMMIT)


# ---------------------------------------------------------------------------


def test_select_is_committed_only(client, schema_name):
    _table(client, schema_name, "t")
    client.execute_sql("BEGIN", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (5, 50)", schema_name=schema_name)
    # Buffered insert is invisible to SELECT until COMMIT.
    res = client.execute_sql("SELECT * FROM t WHERE pk = 5", schema_name=schema_name)
    assert res[0]["type"] == "Rows"
    assert len(list(res[0]["rows"])) == 0
    client.execute_sql("COMMIT", schema_name=schema_name)
    res = client.execute_sql("SELECT * FROM t WHERE pk = 5", schema_name=schema_name)
    assert len(list(res[0]["rows"])) == 1


def test_update_count_is_forward_looking_while_committed_unchanged(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    client.execute_sql("BEGIN", schema_name=schema_name)
    res = client.execute_sql("UPDATE t SET val = 99 WHERE pk = 1", schema_name=schema_name)
    assert res[0]["count"] == 1  # forward-looking
    # The committed row is unchanged mid-transaction: SELECT still reads 10.
    res = client.execute_sql("SELECT * FROM t WHERE pk = 1", schema_name=schema_name)
    rows = list(res[0]["rows"])
    assert len(rows) == 1 and rows[0].val == 10
    client.execute_sql("COMMIT", schema_name=schema_name)
    assert _scan(client, t) == [(1, 99)]


# ---------------------------------------------------------------------------
# Statement order through the buffer (run-splitting)


# ---------------------------------------------------------------------------


def test_delete_then_insert_replace_idiom(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (2, 20)", schema_name=schema_name)
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO t VALUES (1, 10)",         # new
         "DELETE FROM t WHERE pk = 2",           # committed
         "INSERT INTO t VALUES (2, 22)",         # replace
         "COMMIT")
    assert _scan(client, t) == [(1, 10), (2, 22)]


def test_unrelated_update_does_not_launder_duplicate_insert(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError):
        _sql(client, schema_name,
             "BEGIN",
             "UPDATE t SET val = 99 WHERE pk = 2",  # unrelated Update family
             "INSERT INTO t VALUES (1, 100)",       # pk=1 committed, no prior delete
             "COMMIT")
    # Whole transaction aborted: both rows unchanged, transaction closed.
    assert _scan(client, t) == [(1, 10), (2, 20)]
    client.execute_sql("INSERT INTO t VALUES (3, 30)", schema_name=schema_name)  # autocommit works
    assert _scan(client, t) == [(1, 10), (2, 20), (3, 30)]


# ---------------------------------------------------------------------------
# ON CONFLICT consults the buffered net state


# ---------------------------------------------------------------------------


def test_do_nothing_twice_new_pk_yields_one_row(client, schema_name):
    t = _table(client, schema_name, "t")
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO t VALUES (1, 10) ON CONFLICT (pk) DO NOTHING",
         "INSERT INTO t VALUES (1, 20) ON CONFLICT (pk) DO NOTHING",
         "COMMIT")
    # Second DO NOTHING sees the first row buffered → dropped; one row commits.
    assert _scan(client, t) == [(1, 10)]


def test_do_nothing_after_delete_inserts(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    _sql(client, schema_name,
         "BEGIN",
         "DELETE FROM t WHERE pk = 1",
         "INSERT INTO t VALUES (1, 99) ON CONFLICT (pk) DO NOTHING",
         "COMMIT")
    # Buffered-deleted PK is no conflict → the insert applies.
    assert _scan(client, t) == [(1, 99)]


def test_do_update_merges_against_buffered_row(client, schema_name):
    t = _table(client, schema_name, "t")
    # pk=1 is born in the transaction; the DO UPDATE must read the buffered
    # existing row (val=10), not fall through to a plain insert of val=5.
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO t VALUES (1, 10)",
         "INSERT INTO t VALUES (1, 5) ON CONFLICT (pk) DO UPDATE SET val = val + 100",
         "COMMIT")
    assert _scan(client, t) == [(1, 110)]


def test_do_update_merges_against_committed_row(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO t VALUES (1, 5) ON CONFLICT (pk) DO UPDATE SET val = val + 100",
         "COMMIT")
    assert _scan(client, t) == [(1, 110)]


def test_do_update_after_delete_applies_insert(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    _sql(client, schema_name,
         "BEGIN",
         "DELETE FROM t WHERE pk = 1",
         "INSERT INTO t VALUES (1, 7) ON CONFLICT (pk) DO UPDATE SET val = val + 100",
         "COMMIT")
    # Buffered-deleted PK ⇒ no conflict ⇒ plain insert of the excluded row
    # (val=7), NOT a merge against the committed row (which would give 110).
    assert _scan(client, t) == [(1, 7)]


# ---------------------------------------------------------------------------
# Constraint enforcement at COMMIT


# ---------------------------------------------------------------------------


def test_concurrent_commit_of_pk_fails_at_commit(client, server, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("BEGIN", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    # Another connection commits the same PK before this COMMIT.
    with gnitz.connect(server) as other:
        other.execute_sql("INSERT INTO t VALUES (1, 999)", schema_name=schema_name)
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("COMMIT", schema_name=schema_name)
    # The transaction is closed; the other connection's row stands.
    assert _scan(client, t) == [(1, 999)]


def test_fk_parent_and_child_in_one_transaction(client, schema_name):
    client.execute_sql("CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)", schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY, "
        "pid BIGINT NOT NULL REFERENCES parent(id))",
        schema_name=schema_name)
    parent, _ = client.resolve_table(schema_name, "parent")
    child, _ = client.resolve_table(schema_name, "child")
    # Child buffered before parent — FK is a post-transaction survivor check.
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO child VALUES (10, 1)",
         "INSERT INTO parent VALUES (1)",
         "COMMIT")
    assert sorted(r.id for r in client.scan(parent)) == [1]
    assert sorted(r.cid for r in client.scan(child)) == [10]


def test_fk_child_referencing_parent_deleted_in_txn_fails(client, schema_name):
    client.execute_sql("CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)", schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY, "
        "pid BIGINT NOT NULL REFERENCES parent(id))",
        schema_name=schema_name)
    client.execute_sql("INSERT INTO parent VALUES (1)", schema_name=schema_name)
    parent, _ = client.resolve_table(schema_name, "parent")
    with pytest.raises(gnitz.GnitzError):
        _sql(client, schema_name,
             "BEGIN",
             "DELETE FROM parent WHERE id = 1",
             "INSERT INTO child VALUES (10, 1)",  # references a parent gone post-txn
             "COMMIT")
    # Nothing committed: parent still holds row 1.
    assert sorted(r.id for r in client.scan(parent)) == [1]


# ---------------------------------------------------------------------------
# Statement errors, single-call rollback, RETURNING


# ---------------------------------------------------------------------------


def test_statement_error_multi_call_leaves_txn_open(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("BEGIN", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    # A non-constraint statement error does NOT poison the transaction.
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("UPDATE t SET nope = 1 WHERE pk = 1", schema_name=schema_name)
    # Prior buffered work survives; COMMIT commits it.
    client.execute_sql("COMMIT", schema_name=schema_name)
    assert _scan(client, t) == [(1, 10)]


def test_single_call_begin_error_auto_rolls_back(client, schema_name):
    t = _table(client, schema_name, "t")
    # BEGIN opened in THIS call + an error with the txn still open ⇒ auto
    # rollback before returning; nothing committed and no stranded buffer.
    with pytest.raises(gnitz.GnitzError):
        _sql(client, schema_name,
             "BEGIN",
             "INSERT INTO t VALUES (1, 10)",
             "UPDATE t SET nope = 1 WHERE pk = 1",  # errors
             "COMMIT")
    assert _scan(client, t) == []
    # The transaction did not linger: a following autocommit INSERT stands.
    client.execute_sql("INSERT INTO t VALUES (2, 20)", schema_name=schema_name)
    assert _scan(client, t) == [(2, 20)]


def test_returning_in_transaction_returns_ids_before_commit(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("BEGIN", schema_name=schema_name)
    res = client.execute_sql("INSERT INTO t VALUES (7, 70) RETURNING pk", schema_name=schema_name)
    assert res[0]["type"] == "Rows"
    assert sorted(r.pk for r in res[0]["rows"]) == [7]
    # Still staged: invisible to a committed scan until COMMIT.
    assert _scan(client, t) == []
    client.execute_sql("COMMIT", schema_name=schema_name)
    assert _scan(client, t) == [(7, 70)]


def test_returning_bad_projection_commits_nothing_autocommit(client, schema_name):
    """Autocommit RETURNING with a bad projection must not commit the row
    (projection now runs before the write)."""
    t = _table(client, schema_name, "t")
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("INSERT INTO t VALUES (1, 10) RETURNING bogus", schema_name=schema_name)
    assert _scan(client, t) == []


# ---------------------------------------------------------------------------
# DDL is rejected inside a transaction (non-poisoning)


# ---------------------------------------------------------------------------


def test_ddl_rejected_inside_transaction(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("BEGIN", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    for ddl in ["CREATE TABLE t2 (pk BIGINT NOT NULL PRIMARY KEY)",
                "CREATE VIEW v AS SELECT pk FROM t",
                "CREATE INDEX ON t (val)",
                "DROP TABLE t"]:
        with pytest.raises(gnitz.GnitzError) as exc:
            client.execute_sql(ddl, schema_name=schema_name)
        assert "not allowed inside a transaction" in str(exc.value).lower()
    # The transaction stayed open through every rejection: COMMIT lands.
    client.execute_sql("COMMIT", schema_name=schema_name)
    assert _scan(client, t) == [(1, 10)]


# ---------------------------------------------------------------------------
# Committed transaction propagates to a view


# ---------------------------------------------------------------------------


def test_committed_transaction_visible_in_view(client, schema_name):
    _table(client, schema_name, "t")
    client.execute_sql("CREATE VIEW v AS SELECT pk, val FROM t WHERE val > 0", schema_name=schema_name)
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO t VALUES (1, 10), (2, 20)",
         "COMMIT")
    res = client.execute_sql("SELECT * FROM v", schema_name=schema_name)
    assert sorted((r.pk, r.val) for r in res[0]["rows"]) == [(1, 10), (2, 20)]


def test_join_view_reflects_committed_transaction(client, schema_name):
    client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=schema_name)
    client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL)", schema_name=schema_name)
    client.execute_sql(
        "CREATE VIEW v AS SELECT a.id AS id, a.x AS x, b.y AS y FROM a JOIN b ON a.id = b.id",
        schema_name=schema_name)
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO a VALUES (1, 10)",
         "INSERT INTO b VALUES (1, 100)",
         "COMMIT")
    # Both sides landed atomically → the join view has the matched pair.
    rows = list(client.execute_sql("SELECT * FROM v", schema_name=schema_name)[0]["rows"])
    assert len(rows) == 1


# ---------------------------------------------------------------------------
# SERIAL id allocation inside a transaction


# ---------------------------------------------------------------------------


def _serial_table(client, schema_name):
    client.execute_sql("CREATE TABLE t (id SERIAL PRIMARY KEY, name TEXT NOT NULL)", schema_name=schema_name)
    tid, _ = client.resolve_table(schema_name, "t")
    return tid


def test_serial_ids_stamped_in_transaction(client, schema_name):
    t = _serial_table(client, schema_name)
    r1 = client.execute_sql("BEGIN", schema_name=schema_name)
    assert r1[0]["type"] == "TransactionStarted"
    res = client.execute_sql("INSERT INTO t (name) VALUES ('a') RETURNING id", schema_name=schema_name)
    first = list(res[0]["rows"])[0].id
    res = client.execute_sql("INSERT INTO t (name) VALUES ('b') RETURNING id", schema_name=schema_name)
    second = list(res[0]["rows"])[0].id
    assert second > first  # monotonic within the transaction
    client.execute_sql("COMMIT", schema_name=schema_name)
    got = sorted((r.id, r.name) for r in client.scan(t))
    assert got == [(first, "a"), (second, "b")]


def test_rollback_consumes_serial_ids(client, schema_name):
    t = _serial_table(client, schema_name)
    client.execute_sql("BEGIN", schema_name=schema_name)
    res = client.execute_sql("INSERT INTO t (name) VALUES ('a') RETURNING id", schema_name=schema_name)
    rolled = list(res[0]["rows"])[0].id
    client.execute_sql("ROLLBACK", schema_name=schema_name)
    # The id was consumed at statement time; the next autocommit insert gets a
    # higher id (a gap), and the rolled-back row is absent.
    res = client.execute_sql("INSERT INTO t (name) VALUES ('b') RETURNING id", schema_name=schema_name)
    after = list(res[0]["rows"])[0].id
    assert after > rolled
    assert sorted((r.id, r.name) for r in client.scan(t)) == [(after, "b")]


# ---------------------------------------------------------------------------
# Every UPDATE/DELETE access path resolves through the overlay in a txn


# ---------------------------------------------------------------------------


def test_pk_in_multiseek_update_in_txn(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=schema_name)
    res = _sql(client, schema_name,
               "BEGIN",
               "UPDATE t SET val = 0 WHERE pk IN (1, 3)",
               "COMMIT")
    assert res[1]["count"] == 2
    assert _scan(client, t) == [(1, 0), (2, 20), (3, 0)]


def test_pk_in_multiseek_picks_up_born_row(client, schema_name):
    t = _table(client, schema_name, "t")
    res = _sql(client, schema_name,
               "BEGIN",
               "INSERT INTO t VALUES (5, 50)",
               "UPDATE t SET val = 0 WHERE pk IN (5, 6)",  # 5 born, 6 absent
               "COMMIT")
    assert res[2]["count"] == 1
    assert _scan(client, t) == [(5, 0)]


def test_pk_in_multiseek_delete_in_txn(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=schema_name)
    _sql(client, schema_name,
         "BEGIN",
         "DELETE FROM t WHERE pk IN (1, 3)",
         "COMMIT")
    assert _scan(client, t) == [(2, 20)]


def test_pk_seek_residual_against_overlaid_payload(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    # The residual `val = 10` is checked against the EFFECTIVE row: after the
    # first UPDATE buffers val=5, the second (pk=1 AND val=10) matches nothing.
    res = _sql(client, schema_name,
               "BEGIN",
               "UPDATE t SET val = 5 WHERE pk = 1",
               "UPDATE t SET val = 99 WHERE pk = 1 AND val = 10",
               "COMMIT")
    assert res[2]["count"] == 0
    assert _scan(client, t) == [(1, 5)]


def test_scan_all_delete_removes_committed_and_born(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=schema_name)
    res = _sql(client, schema_name,
               "BEGIN",
               "INSERT INTO t VALUES (3, 30)",
               "DELETE FROM t",  # no WHERE: committed 1,2 + born 3
               "COMMIT")
    assert res[2]["count"] == 3
    assert _scan(client, t) == []


def test_indexed_where_finds_buffered_born_row(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("CREATE INDEX ON t (val)", schema_name=schema_name)
    # The born row is unindexed (not committed), so the index seek misses it;
    # the full net enumeration + predicate re-filter still discovers val=77.
    res = _sql(client, schema_name,
               "BEGIN",
               "INSERT INTO t VALUES (5, 77)",
               "UPDATE t SET val = 0 WHERE val = 77",
               "COMMIT")
    assert res[2]["count"] == 1
    assert _scan(client, t) == [(5, 0)]


def test_delete_reinsert_where_matches_reinserted_payload(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    # A WHERE only the reinserted payload (55) satisfies must match the row.
    res = _sql(client, schema_name,
               "BEGIN",
               "DELETE FROM t WHERE pk = 1",
               "INSERT INTO t VALUES (1, 55)",
               "UPDATE t SET val = 0 WHERE val = 55",
               "COMMIT")
    assert res[3]["count"] == 1
    assert _scan(client, t) == [(1, 0)]


def test_empty_update_and_delete_do_not_break_txn(client, schema_name):
    t = _table(client, schema_name, "t")
    res = _sql(client, schema_name,
               "BEGIN",
               "UPDATE t SET val = 0 WHERE pk = 999",   # no match
               "DELETE FROM t WHERE pk = 999",          # no match
               "INSERT INTO t VALUES (1, 10)",
               "COMMIT")
    assert res[1]["count"] == 0
    assert res[2]["count"] == 0
    assert _scan(client, t) == [(1, 10)]


# ---------------------------------------------------------------------------
# Compound PK and non-integer payloads through the overlay


# ---------------------------------------------------------------------------


def test_compound_pk_transaction(client, schema_name):
    client.execute_sql(
        "CREATE TABLE t (a BIGINT NOT NULL, b BIGINT NOT NULL, v BIGINT NOT NULL, PRIMARY KEY (a, b))",
        schema_name=schema_name)
    t, _ = client.resolve_table(schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 1, 10)", schema_name=schema_name)
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO t VALUES (2, 2, 20)",                    # born
         "UPDATE t SET v = v + 5 WHERE a = 1 AND b = 1",       # committed, compound seek
         "DELETE FROM t WHERE a = 2 AND b = 2",                # delete the born row
         "COMMIT")
    got = sorted((r.a, r.b, r.v) for r in client.scan(t))
    assert got == [(1, 1, 15)]


def test_text_payload_read_your_own_writes(client, schema_name):
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, name TEXT NOT NULL)", schema_name=schema_name)
    t, _ = client.resolve_table(schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 'alice')", schema_name=schema_name)
    _sql(client, schema_name,
         "BEGIN",
         "UPDATE t SET name = 'bob' WHERE pk = 1",
         "INSERT INTO t VALUES (2, 'carol')",
         "COMMIT")
    got = sorted((r.pk, r.name) for r in client.scan(t))
    assert got == [(1, "bob"), (2, "carol")]


def test_null_payload_through_transaction(client, schema_name):
    client.execute_sql("CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT)", schema_name=schema_name)
    t, _ = client.resolve_table(schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, NULL)", schema_name=schema_name)
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO t VALUES (2, NULL)",           # born, NULL payload carried
         "UPDATE t SET val = 9 WHERE pk = 1",        # committed NULL → 9
         "COMMIT")
    got = sorted((r.pk, r.val) for r in client.scan(t))
    assert got == [(1, 9), (2, None)]


# ---------------------------------------------------------------------------
# Structural: interleaving, sequential txns, rollback re-run, bulk


# ---------------------------------------------------------------------------


def test_cross_table_interleaved_writes(client, schema_name):
    a = _table(client, schema_name, "a")
    b = _table(client, schema_name, "b")
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO a VALUES (1, 10)",
         "INSERT INTO b VALUES (2, 20)",
         "UPDATE a SET val = val + 1 WHERE pk = 1",   # RYOW on born a-row
         "INSERT INTO a VALUES (3, 30)",
         "COMMIT")
    assert _scan(client, a) == [(1, 11), (3, 30)]
    assert _scan(client, b) == [(2, 20)]


def test_two_sequential_transactions_on_one_connection(client, schema_name):
    t = _table(client, schema_name, "t")
    _sql(client, schema_name, "BEGIN", "INSERT INTO t VALUES (1, 10)", "COMMIT")
    _sql(client, schema_name, "BEGIN", "INSERT INTO t VALUES (2, 20)", "COMMIT")
    assert _scan(client, t) == [(1, 10), (2, 20)]


def test_rollback_fully_clears_buffer(client, schema_name):
    t = _table(client, schema_name, "t")
    _sql(client, schema_name, "BEGIN", "INSERT INTO t VALUES (1, 10)", "ROLLBACK")
    # The rolled-back write left no residue: re-inserting the same PK is not a
    # duplicate and commits cleanly.
    _sql(client, schema_name, "BEGIN", "INSERT INTO t VALUES (1, 10)", "COMMIT")
    assert _scan(client, t) == [(1, 10)]


def test_large_multistatement_transaction(client, schema_name):
    t = _table(client, schema_name, "t")
    stmts = ["BEGIN"] + [f"INSERT INTO t VALUES ({i}, {i * 10})" for i in range(1, 21)] + ["COMMIT"]
    _sql(client, schema_name, *stmts)
    assert _scan(client, t) == [(i, i * 10) for i in range(1, 21)]


def test_multirow_insert_then_scan_all_update(client, schema_name):
    t = _table(client, schema_name, "t")
    res = _sql(client, schema_name,
               "BEGIN",
               "INSERT INTO t VALUES (1, 1), (2, 2), (3, 3)",
               "UPDATE t SET val = 0",
               "COMMIT")
    assert res[2]["count"] == 3
    assert _scan(client, t) == [(1, 0), (2, 0), (3, 0)]


def test_insert_then_delete_same_pk_removes(client, schema_name):
    t = _table(client, schema_name, "t")
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO t VALUES (1, 10)",
         "DELETE FROM t WHERE pk = 1",
         "COMMIT")
    assert _scan(client, t) == []


# ---------------------------------------------------------------------------
# More ON CONFLICT combinations


# ---------------------------------------------------------------------------


def test_do_update_mixed_conflict_and_new_row(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO t VALUES (1, 5), (2, 20) ON CONFLICT (pk) DO UPDATE SET val = val + 100",
         "COMMIT")
    # Row 1 conflicts committed(10) → 110; row 2 is new → inserted as-is.
    assert _scan(client, t) == [(1, 110), (2, 20)]


def test_do_nothing_committed_conflict_in_txn(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    res = _sql(client, schema_name,
               "BEGIN",
               "INSERT INTO t VALUES (1, 99), (2, 20) ON CONFLICT (pk) DO NOTHING",
               "COMMIT")
    assert res[1]["count"] == 1  # only the new row survives
    assert _scan(client, t) == [(1, 10), (2, 20)]


def test_do_update_result_then_plain_update_compounds(client, schema_name):
    t = _table(client, schema_name, "t")
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO t VALUES (1, 10)",
         "INSERT INTO t VALUES (1, 5) ON CONFLICT (pk) DO UPDATE SET val = val + 100",  # → 110
         "UPDATE t SET val = val + 1 WHERE pk = 1",                                      # → 111
         "COMMIT")
    assert _scan(client, t) == [(1, 111)]


# ---------------------------------------------------------------------------
# Error handling and transaction lifecycle


# ---------------------------------------------------------------------------


def test_build_error_arity_is_non_poisoning(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("BEGIN", schema_name=schema_name)
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    # Wrong VALUES arity is a build-time error that buffers nothing.
    with pytest.raises(gnitz.GnitzError):
        client.execute_sql("INSERT INTO t VALUES (2)", schema_name=schema_name)
    client.execute_sql("COMMIT", schema_name=schema_name)
    assert _scan(client, t) == [(1, 10)]


def test_failed_commit_closes_txn_and_next_begin_works(client, schema_name):
    t = _table(client, schema_name, "t")
    client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=schema_name)
    # A duplicate-PK COMMIT fails and closes the transaction.
    with pytest.raises(gnitz.GnitzError):
        _sql(client, schema_name, "BEGIN", "INSERT INTO t VALUES (1, 20)", "COMMIT")
    # A fresh transaction opens and commits normally afterward.
    _sql(client, schema_name, "BEGIN", "INSERT INTO t VALUES (2, 20)", "COMMIT")
    assert _scan(client, t) == [(1, 10), (2, 20)]


def test_returning_rows_absent_after_rollback(client, schema_name):
    t = _table(client, schema_name, "t")
    res = client.execute_sql(
        "BEGIN; INSERT INTO t VALUES (7, 70) RETURNING pk", schema_name=schema_name)
    assert sorted(r.pk for r in res[1]["rows"]) == [7]  # ids returned pre-commit
    client.execute_sql("ROLLBACK", schema_name=schema_name)
    assert _scan(client, t) == []  # after rollback the row is absent


def test_fk_parent_first_order_commits(client, schema_name):
    client.execute_sql("CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY)", schema_name=schema_name)
    client.execute_sql(
        "CREATE TABLE child (cid BIGINT NOT NULL PRIMARY KEY, "
        "pid BIGINT NOT NULL REFERENCES parent(id))",
        schema_name=schema_name)
    parent, _ = client.resolve_table(schema_name, "parent")
    child, _ = client.resolve_table(schema_name, "child")
    _sql(client, schema_name,
         "BEGIN",
         "INSERT INTO parent VALUES (1)",
         "INSERT INTO child VALUES (10, 1)",
         "COMMIT")
    assert sorted(r.id for r in client.scan(parent)) == [1]
    assert sorted(r.cid for r in client.scan(child)) == [10]
