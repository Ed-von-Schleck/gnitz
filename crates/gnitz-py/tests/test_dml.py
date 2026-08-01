import random
import threading

import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _setup(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "t", cols)
    return sn, tid, schema


def test_push_and_scan(client):
    sn, tid, schema = _setup(client)
    batch = gnitz.ZSetBatch(schema)
    for i in range(1, 6):
        batch.append(pk=i, val=i * 10)
    client.push(tid, batch)
    result = client.scan(tid)
    assert len(result) == 5
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_scan_empty_table(client):
    sn, tid, schema = _setup(client)
    result = client.scan(tid)
    assert len(result) == 0
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_empty_push_no_desync(client):
    """A push of an empty batch is a no-op push on the wire (FLAG_PUSH with no
    data rows) that the server ACKs with the "nothing written" LSN 0 — never a
    scan, whose streamed table dump would desync the one-frame push reply
    reader. A following non-empty push must return a genuine ingest LSN and
    scan must reflect exactly the real writes."""
    sn, tid, schema = _setup(client)

    # Seed a row so a mis-routed scan would actually stream table data.
    seed = gnitz.ZSetBatch(schema)
    seed.append(pk=1, val=10)
    client.push(tid, seed)

    empty = gnitz.ZSetBatch(schema)
    assert len(empty) == 0
    assert client.push(tid, empty) == 0

    # Connection is still aligned: a real push returns a real LSN and the scan
    # sees exactly the two written rows (seed + this one), nothing leaked.
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=2, val=20)
    assert client.push(tid, batch) > 0
    rows = {r.pk: r.val for r in client.scan(tid)}
    assert rows == {1: 10, 2: 20}
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_delete_rows(client):
    sn, tid, schema = _setup(client)
    batch = gnitz.ZSetBatch(schema)
    for pk, val in [(1, 10), (2, 20), (3, 30)]:
        batch.append(pk=pk, val=val)
    client.push(tid, batch)
    client.delete(tid, schema, [2])
    result = client.scan(tid)
    pks = sorted(row.pk for row in result)
    assert pks == [1, 3]
    client.drop_table(sn, "t")
    client.drop_schema(sn)


def test_nullable_string_columns(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("s", gnitz.TypeCode.STRING, is_nullable=True)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "strs", cols)
    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, s="hello")
    batch.append(pk=2, s=None)
    batch.append(pk=3, s="world")
    client.push(tid, batch)
    result = client.scan(tid)
    assert len(result) == 3
    client.drop_table(sn, "strs")
    client.drop_schema(sn)


def test_scan_values_correct(client):
    """Verify pushed values round-trip through scan correctly."""
    sn, tid, schema = _setup(client)
    rows_in = [(i, i * 100) for i in range(1, 11)]
    batch = gnitz.ZSetBatch(schema)
    for pk, val in rows_in:
        batch.append(pk=pk, val=val)
    client.push(tid, batch)
    result = client.scan(tid)
    assert len(result) == 10
    pairs = sorted((row.pk, row.val) for row in result)
    assert pairs == rows_in
    client.drop_table(sn, "t")
    client.drop_schema(sn)


# ---------------------------------------------------------------------------
# Concurrent binary pushes to one table
#
# A push whose validation reads no committed state (an unconstrained base
# table under the binary API's upsert mode) holds its table lock shared, so
# several such pushes reach the committer together and fold into ONE merged
# batch under one SAL zone and one fsync. Both tests below need enough
# connections for that fold to happen at all: the committer drains only what
# is already queued when it wakes, so two connections almost never have a
# second request enqueued and nothing merges. Eight is comfortably past the
# threshold, and each asserts the fold actually occurred — a push returns its
# zone LSN, so fewer distinct LSNs than pushes means pushes shared a zone.
# ---------------------------------------------------------------------------

_CONCURRENT_CONNS = 8
# Distinct zones as a fraction of pushes. Deliberately loose: the assertion is
# that pushes coalesce at all, not how far they coalesce.
_MERGE_BAR = 0.75


def _run_threads(fn, n):
    """Run `fn(i)` on `n` threads, join them, and re-raise the first failure."""
    errors = []

    def body(i):
        try:
            fn(i)
        except Exception as e:  # noqa: BLE001 — re-raised below
            errors.append(e)

    threads = [threading.Thread(target=body, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if errors:
        raise errors[0]


def test_concurrent_same_pk_upserts(server):
    """Eight connections upserting the same PK concurrently. Every push
    succeeds, the table holds exactly one row, and its value is one of the
    pushed ones — several same-PK rows land in one merged batch and the ingest
    fold keeps the last in batch order."""
    n = 40
    with gnitz.connect(server) as setup:
        sn, tid, schema = _setup(setup)
    lsns = [[] for _ in range(_CONCURRENT_CONNS)]

    def worker(w):
        with gnitz.connect(server) as c:
            for i in range(n):
                batch = gnitz.ZSetBatch(schema)
                batch.append(pk=1, val=w * 1000 + i)
                lsns[w].append(c.push(tid, batch))

    try:
        _run_threads(worker, _CONCURRENT_CONNS)
        with gnitz.connect(server) as c:
            rows = list(c.scan(tid))
            assert len(rows) == 1
            assert rows[0].pk == 1
            pushed = {w * 1000 + i for w in range(_CONCURRENT_CONNS) for i in range(n)}
            assert rows[0].val in pushed
        seen = [lsn for per_conn in lsns for lsn in per_conn]
        assert len(set(seen)) <= _MERGE_BAR * len(seen), (
            f"{len(set(seen))} zones for {len(seen)} pushes: pushes did not coalesce"
        )
    finally:
        with gnitz.connect(server) as c:
            c.drop_table(sn, "t")
            c.drop_schema(sn)


def test_concurrent_disjoint_pk_pushes_with_strings(server):
    """Eight connections pushing disjoint PKs to one table, long enough that the
    committer's pooled merge batch is reused across many merges. The STRING
    values mix inline (<= 12 B) and heap payloads, and every one is asserted:
    the merge relocates each German string into the destination's own blob
    heap, and a mis-relocated pointer or a mis-reused pool entry leaves the row
    count intact while the value is wrong."""
    rounds, per_push = 30, 4
    sn = "s" + _uid()
    with gnitz.connect(server) as setup:
        setup.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("s", gnitz.TypeCode.STRING, is_nullable=False)]
        schema = gnitz.Schema(cols)
        tid = setup.create_table(sn, "strs", cols)

    def value_of(pk):
        # Alternates short (inline) and long (blob heap) within one batch.
        return f"v{pk}" if pk % 2 == 0 else f"long-payload-for-row-{pk}"

    def pk_of(w, r, j):
        return (w * rounds + r) * per_push + j

    lsns = [[] for _ in range(_CONCURRENT_CONNS)]

    def worker(w):
        with gnitz.connect(server) as c:
            for r in range(rounds):
                batch = gnitz.ZSetBatch(schema)
                for j in range(per_push):
                    pk = pk_of(w, r, j)
                    batch.append(pk=pk, s=value_of(pk))
                lsns[w].append(c.push(tid, batch))

    try:
        _run_threads(worker, _CONCURRENT_CONNS)
        expected = {pk_of(w, r, j): value_of(pk_of(w, r, j))
                    for w in range(_CONCURRENT_CONNS)
                    for r in range(rounds)
                    for j in range(per_push)}
        with gnitz.connect(server) as c:
            got = {row.pk: row.s for row in c.scan(tid)}
        assert got == expected
        seen = [lsn for per_conn in lsns for lsn in per_conn]
        assert len(set(seen)) <= _MERGE_BAR * len(seen), (
            f"{len(set(seen))} zones for {len(seen)} pushes: pushes did not coalesce"
        )
    finally:
        with gnitz.connect(server) as c:
            c.drop_table(sn, "strs")
            c.drop_schema(sn)


# ---------------------------------------------------------------------------
# Helpers for SQL UPDATE / DELETE tests
# ---------------------------------------------------------------------------

_CREATE_T3 = (
    "CREATE TABLE t "
    "(pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL, cat_id BIGINT NOT NULL)"
)
_INSERT_3ROWS = "INSERT INTO t VALUES (1, 100, 10), (2, 200, 20), (3, 300, 30)"


def _rows_map(client, tid):
    """Scan table and return {pk: row} dict (only positive-weight rows)."""
    return {row.pk: row for row in client.scan(tid)}


def _drop_idx_and_table(client, sn, idx_name):
    for sql in [f"DROP INDEX {idx_name}", "DROP TABLE t"]:
        try:
            client.execute_sql(sql, schema_name=sn)
        except Exception:
            pass
    client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestUpdateSQL
# ---------------------------------------------------------------------------

class TestUpdateSQL:
    def test_update_pk_literal_set(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = 99 WHERE pk = 1", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 1

            rows = _rows_map(client, tid)
            assert rows[1].val == 99
            assert rows[2].val == 200
            assert rows[3].val == 300
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_pk_expr_set(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = val * 2 WHERE pk = 2", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 1

            rows = _rows_map(client, tid)
            assert rows[2].val == 400
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_unique_index_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        idx = f"{sn}__t__idx_cat_id"
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(cat_id)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = 0 WHERE cat_id = 20", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 1

            rows = _rows_map(client, tid)
            assert rows[1].val == 100
            assert rows[2].val == 0
            assert rows[3].val == 300
        finally:
            _drop_idx_and_table(client, sn, idx)

    def test_update_nonunique_index_scan(self, client):
        """Non-unique index → falls through to full scan; both matching rows updated."""
        sn = "s" + _uid()
        client.create_schema(sn)
        idx = f"{sn}__t__idx_cat_id"
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 100, 10), (2, 200, 10), (3, 300, 99)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(cat_id)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = 0 WHERE cat_id = 10", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 2

            rows = _rows_map(client, tid)
            assert rows[1].val == 0
            assert rows[2].val == 0
            assert rows[3].val == 300
        finally:
            _drop_idx_and_table(client, sn, idx)

    def test_update_full_scan_predicate(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = 0 WHERE val > 150", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 2

            rows = _rows_map(client, tid)
            assert rows[1].val == 100
            assert rows[2].val == 0
            assert rows[3].val == 0
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_no_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = 42", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 3

            rows = _rows_map(client, tid)
            assert all(r.val == 42 for r in rows.values())
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_pk_column_rejects(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 100, 10)", schema_name=sn)

            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("UPDATE t SET pk = 999 WHERE pk = 1", schema_name=sn)
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_row_not_found(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 100, 10)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("UPDATE t SET val = 99 WHERE pk = 9999", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 0

            rows = _rows_map(client, tid)
            assert rows[1].val == 100
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestDeleteSQL
# ---------------------------------------------------------------------------

class TestDeleteSQL:
    def test_delete_pk_eq(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("DELETE FROM t WHERE pk = 2", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 1

            rows = _rows_map(client, tid)
            assert 1 in rows and 3 in rows and 2 not in rows
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_delete_pk_in(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("DELETE FROM t WHERE pk IN (1, 3)", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 2

            rows = _rows_map(client, tid)
            assert list(rows.keys()) == [2]
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_delete_unique_index(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        idx = f"{sn}__t__idx_cat_id"
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(cat_id)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("DELETE FROM t WHERE cat_id = 20", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 1

            rows = _rows_map(client, tid)
            assert 1 in rows and 3 in rows and 2 not in rows
        finally:
            _drop_idx_and_table(client, sn, idx)

    def test_delete_nonunique_index_scan(self, client):
        """Non-unique index → falls through to full scan; all matching rows deleted."""
        sn = "s" + _uid()
        client.create_schema(sn)
        idx = f"{sn}__t__idx_cat_id"
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(
                "INSERT INTO t VALUES (1, 100, 10), (2, 200, 10), (3, 300, 99)",
                schema_name=sn,
            )
            client.execute_sql("CREATE INDEX ON t(cat_id)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("DELETE FROM t WHERE cat_id = 10", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 2

            rows = _rows_map(client, tid)
            assert list(rows.keys()) == [3]
        finally:
            _drop_idx_and_table(client, sn, idx)

    def test_delete_full_scan(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("DELETE FROM t WHERE val > 150", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 2

            rows = _rows_map(client, tid)
            assert list(rows.keys()) == [1]
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_delete_no_where(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql(_INSERT_3ROWS, schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            res = client.execute_sql("DELETE FROM t", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 3

            assert len(client.scan(tid)) == 0
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_delete_updates_index(self, client):
        """DELETE retracts the source row; secondary index seek returns empty after delete."""
        sn = "s" + _uid()
        client.create_schema(sn)
        idx = f"{sn}__t__idx_cat_id"
        try:
            client.execute_sql(_CREATE_T3, schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (1, 100, 10)", schema_name=sn)
            client.execute_sql("CREATE UNIQUE INDEX ON t(cat_id)", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")

            # Confirm row is found via index before delete
            before = client.seek_by_index(tid, [2], [10])
            assert before.schema is not None and len(before.pks) == 1

            client.execute_sql("DELETE FROM t WHERE pk = 1", schema_name=sn)

            # After delete the index should return empty
            after = client.seek_by_index(tid, [2], [10])
            empty = after.schema is None or len(after.pks) == 0
            assert empty
        finally:
            _drop_idx_and_table(client, sn, idx)

    def test_update_then_view_reflects(self, client):
        """UPDATE delta propagates through a DBSP view circuit."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM t WHERE val > 50",
                schema_name=sn,
            )
            vid, _ = client.resolve_table(sn, "v")

            # Insert after view creation so rows flow through the circuit
            client.execute_sql("INSERT INTO t VALUES (1, 100), (2, 30)", schema_name=sn)

            # View should contain only row 1 (val=100 > 50)
            assert len(client.scan(vid)) == 1

            # Update row 2: val 30 → 200; delta passes the view filter
            client.execute_sql("UPDATE t SET val = 200 WHERE pk = 2", schema_name=sn)

            # View should now contain both rows
            assert len(client.scan(vid)) == 2
        finally:
            for sql in ["DROP VIEW v", "DROP TABLE t"]:
                try:
                    client.execute_sql(sql, schema_name=sn)
                except Exception:
                    pass
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestStringEdgeCases
# ---------------------------------------------------------------------------

class TestStringEdgeCases:

    def _setup(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
                gnitz.ColumnDef("label", gnitz.TypeCode.STRING, is_nullable=True)]
        schema = gnitz.Schema(cols)
        tid = client.create_table(sn, "strs", cols)
        return sn, tid, schema

    def test_string_empty(self, client):
        """Push row with empty string; verify empty string round-trips."""
        sn, tid, schema = self._setup(client)
        try:
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, label="")
            client.push(tid, batch)
            rows = list(client.scan(tid))
            assert len(rows) == 1
            assert rows[0].label == ""
        finally:
            client.drop_table(sn, "strs")
            client.drop_schema(sn)

    def test_string_12byte_boundary(self, client):
        """Push row with exactly 12-char string (inline boundary); verify round-trip."""
        sn, tid, schema = self._setup(client)
        try:
            s12 = "abcdefghijkl"  # exactly 12 bytes
            assert len(s12) == 12
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, label=s12)
            client.push(tid, batch)
            rows = list(client.scan(tid))
            assert len(rows) == 1
            assert rows[0].label == s12
        finally:
            client.drop_table(sn, "strs")
            client.drop_schema(sn)

    def test_string_long(self, client):
        """Push row with string > 12 chars (heap allocation); verify round-trip."""
        sn, tid, schema = self._setup(client)
        try:
            long_s = "this_is_a_longer_string_value"
            assert len(long_s) > 12
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, label=long_s)
            client.push(tid, batch)
            rows = list(client.scan(tid))
            assert len(rows) == 1
            assert rows[0].label == long_s
        finally:
            client.drop_table(sn, "strs")
            client.drop_schema(sn)

    def test_string_null(self, client):
        """Push row with null label; verify row.label is None in scan result."""
        sn, tid, schema = self._setup(client)
        try:
            batch = gnitz.ZSetBatch(schema)
            batch.append(pk=1, label=None)
            client.push(tid, batch)
            rows = list(client.scan(tid))
            assert len(rows) == 1
            assert rows[0].label is None
        finally:
            client.drop_table(sn, "strs")
            client.drop_schema(sn)


# ---------------------------------------------------------------------------
# TestPkInMultiSeek — `pk IN (…)` fast-path parity across DELETE/UPDATE/SELECT,
# plus IN-list predicates that fall through to the residual scan
# ---------------------------------------------------------------------------

class TestPkInMultiSeek:
    def _setup_t3(self, client, sn):
        client.execute_sql(_CREATE_T3, schema_name=sn)
        client.execute_sql(_INSERT_3ROWS, schema_name=sn)
        tid, _ = client.resolve_table(sn, "t")
        return tid

    def test_update_pk_in(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid = self._setup_t3(client, sn)
            res = client.execute_sql("UPDATE t SET val = 0 WHERE pk IN (1, 3)", schema_name=sn)
            assert res[0]["type"] == "RowsAffected"
            assert res[0]["count"] == 2
            rows = _rows_map(client, tid)
            assert rows[1].val == 0
            assert rows[2].val == 200
            assert rows[3].val == 0
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_pk_in_repeated_and_absent_keys(self, client):
        """Repeated keys count once; absent keys contribute nothing."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid = self._setup_t3(client, sn)
            res = client.execute_sql(
                "UPDATE t SET val = val + 1 WHERE pk IN (2, 2, 999)", schema_name=sn
            )
            assert res[0]["count"] == 1
            rows = _rows_map(client, tid)
            assert rows[2].val == 201  # applied exactly once despite the repeat
            assert rows[1].val == 100 and rows[3].val == 300
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_select_pk_in(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_t3(client, sn)
            res = client.execute_sql("SELECT * FROM t WHERE pk IN (1, 3)", schema_name=sn)
            assert res[0]["type"] == "Rows"
            pairs = sorted((r.pk, r.val) for r in res[0]["rows"])
            assert pairs == [(1, 100), (3, 300)]
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_select_pk_in_repeated_absent_projection_limit(self, client):
        """Dedup + absent keys, flowing into the shared projection/LIMIT tail."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup_t3(client, sn)
            res = client.execute_sql(
                "SELECT * FROM t WHERE pk IN (3, 3, 1, 999)", schema_name=sn
            )
            pks = sorted(r.pk for r in res[0]["rows"])
            assert pks == [1, 3]
            res = client.execute_sql(
                "SELECT pk, val FROM t WHERE pk IN (1, 2, 3) LIMIT 2", schema_name=sn
            )
            assert len(list(res[0]["rows"])) == 2
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_uuid_pk_in_update_select_parity(self, client):
        """UUID string keys route through the same IN fast path as `=` seeks."""
        ua = '550e8400-e29b-41d4-a716-446655440000'
        ub = '6ba7b810-9dad-11d1-80b4-00c04fd430c8'
        uc = '01935000-0000-7000-8000-000000000001'
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (pk UUID NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            for u, v in [(ua, 1), (ub, 2), (uc, 3)]:
                client.execute_sql(f"INSERT INTO t VALUES ('{u}', {v})", schema_name=sn)

            res = client.execute_sql(
                f"UPDATE t SET v = 99 WHERE pk IN ('{ua}', '{uc}')", schema_name=sn
            )
            assert res[0]["count"] == 2

            res = client.execute_sql(
                f"SELECT * FROM t WHERE pk IN ('{ua}', '{ub}')", schema_name=sn
            )
            vals = sorted(r.v for r in res[0]["rows"])
            assert vals == [2, 99]

            res = client.execute_sql(
                f"DELETE FROM t WHERE pk IN ('{ub}', '{ub}')", schema_name=sn
            )
            assert res[0]["count"] == 1
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_pk_in_with_a_companion_conjunct(self, client):
        """A `pk IN (…)` conjunct still gathers those keys when the WHERE has more
        to it; the rest post-filters the gathered rows, so only pk 3 is touched."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid = self._setup_t3(client, sn)
            res = client.execute_sql(
                "UPDATE t SET val = 0 WHERE pk IN (1, 3) AND val > 150", schema_name=sn
            )
            assert res[0]["count"] == 1
            rows = _rows_map(client, tid)
            assert rows[3].val == 0
            assert rows[1].val == 100 and rows[2].val == 200
            # The SELECT mirror: a PkSet gather carrying a predicate.
            res = client.execute_sql(
                "SELECT pk FROM t WHERE pk IN (1, 2, 3) AND val > 150", schema_name=sn
            )
            assert sorted(r.pk for r in res[0]["rows"]) == [2]
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_delete_pk_one_key_in_matches_the_eq_spelling(self, client):
        """A one-element list is the equality it spells: one row deleted, the
        others untouched."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid = self._setup_t3(client, sn)
            res = client.execute_sql("DELETE FROM t WHERE pk IN (2)", schema_name=sn)
            assert res[0]["count"] == 1
            rows = _rows_map(client, tid)
            assert sorted(rows.keys()) == [1, 3]
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_update_pk_not_in_falls_to_scan(self, client):
        """NOT IN never matches the fast path; `Not(…)` over the membership test
        evaluates as a residual predicate on the full scan."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid = self._setup_t3(client, sn)
            res = client.execute_sql(
                "UPDATE t SET val = 0 WHERE pk NOT IN (2)", schema_name=sn
            )
            assert res[0]["count"] == 2
            rows = _rows_map(client, tid)
            assert rows[1].val == 0 and rows[3].val == 0 and rows[2].val == 200
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)

    def test_delete_non_pk_in_scan(self, client):
        """A non-PK IN list evaluates as a residual predicate over the full scan."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            tid = self._setup_t3(client, sn)
            res = client.execute_sql(
                "DELETE FROM t WHERE val IN (100, 300, 555)", schema_name=sn
            )
            assert res[0]["count"] == 2
            rows = _rows_map(client, tid)
            assert list(rows.keys()) == [2]
        finally:
            try:
                client.execute_sql("DROP TABLE t", schema_name=sn)
            except Exception:
                pass
            client.drop_schema(sn)
