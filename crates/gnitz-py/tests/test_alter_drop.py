"""End-to-end ALTER TABLE DROP COLUMN / ALTER COLUMN DROP NOT NULL tests.

Run with GNITZ_WORKERS=4 so the DROP NOT NULL comparator swap must reach every
partition and the wildcard/positional-remap fixes exercise the distributed read
and write paths, not just a single worker.

    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_alter_drop.py -v --tb=short
"""

import pytest
import gnitz
from _uid import uid as _uid




def _rows(client, sn, sql):
    res = client.execute_sql(sql, schema_name=sn)
    assert res[0]["type"] == "Rows", f"expected Rows, got {res[0]['type']}"
    # `rows` is a lazy ScanResult (iterable, not indexable) — materialize it so
    # tests can index and re-scan it.
    return list(res[0]["rows"])


# ── DROP COLUMN ─────────────────────────────────────────────────────────────


def test_drop_middle_column_wildcards_and_positional_remap(client):
    sn = "adrop" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10, 100)", schema_name=sn)

        client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=sn)

        # `SELECT *` excludes the dropped middle column, and the pre-DROP row reads
        # back unchanged on the visible columns.
        rows = _rows(client, sn, "SELECT * FROM t")
        assert len(rows) == 1
        assert "a" not in rows[0]._fields
        assert (rows[0]["id"], rows[0]["b"]) == (1, 100)

        # A new INSERT supplies only the visible columns; the value lands in `b`,
        # not the dropped slot (positional-remap regression).
        client.execute_sql("INSERT INTO t VALUES (2, 200)", schema_name=sn)
        got = {r["id"]: r["b"] for r in _rows(client, sn, "SELECT * FROM t")}
        assert got == {1: 100, 2: 200}

        # `RETURNING *` also excludes the dropped column (the other wildcard leak).
        ret = client.execute_sql("INSERT INTO t VALUES (3, 300) RETURNING *", schema_name=sn)
        assert ret[0]["type"] == "Rows"
        ret_rows = list(ret[0]["rows"])
        assert "a" not in ret_rows[0]._fields
        assert (ret_rows[0]["id"], ret_rows[0]["b"]) == (3, 300)

        # The dropped column is unnameable: an explicit-list INSERT and a projection
        # that reference it both error.
        with pytest.raises(Exception):
            client.execute_sql("INSERT INTO t (id, a, b) VALUES (4, 1, 2)", schema_name=sn)
        with pytest.raises(Exception):
            client.execute_sql("SELECT a FROM t", schema_name=sn)
    finally:
        client.drop_schema(sn)


def test_drop_column_index_covered_then_dropped(client):
    sn = "adrop" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10, 100)", schema_name=sn)
        client.execute_sql("CREATE INDEX ON t (a)", schema_name=sn)

        # DROP COLUMN of an index-covered column errors until the index is dropped.
        with pytest.raises(Exception):
            client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=sn)

        client.execute_sql(f"DROP INDEX {sn}__t__idx_a", schema_name=sn)
        client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=sn)
        rows = _rows(client, sn, "SELECT * FROM t")
        assert "a" not in rows[0]._fields
        assert (rows[0]["id"], rows[0]["b"]) == (1, 100)
    finally:
        client.drop_schema(sn)


def test_drop_column_update_carries_hidden_value(client):
    sn = "adrop" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)
        client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=sn)

        # UPDATE and DELETE address rows by PK / visible columns; the hidden slot
        # rides through UPDATE verbatim and DELETE retracts the right row.
        client.execute_sql("UPDATE t SET b = 999 WHERE id = 1", schema_name=sn)
        client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
        got = {r["id"]: r["b"] for r in _rows(client, sn, "SELECT * FROM t")}
        assert got == {1: 999}
    finally:
        client.drop_schema(sn)


# ── DROP NOT NULL ───────────────────────────────────────────────────────────


def test_drop_not_null_null_after_multiworker(client):
    sn = "adrop" + _uid()
    client.create_schema(sn)
    try:
        # All-fixed-int NOT NULL → the table is on the FixedIntNonnull fast
        # comparator; DROP NOT NULL forces the Generic swap that must reach every
        # partition (W=4).
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        # Spread rows across partitions.
        vals = ", ".join(f"({i}, {i * 10})" for i in range(1, 41))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)

        # NULL rejected before the ALTER.
        with pytest.raises(Exception):
            client.execute_sql("INSERT INTO t VALUES (100, NULL)", schema_name=sn)

        client.execute_sql("ALTER TABLE t ALTER COLUMN v DROP NOT NULL", schema_name=sn)

        # After the swap: a NULL and an explicit 0 coexist as *distinct* values on
        # distinct PKs — if any partition kept the stale FixedIntNonnull comparator
        # the NULL would sort/merge as a real 0 and corrupt weights.
        client.execute_sql("INSERT INTO t VALUES (100, NULL), (101, 0)", schema_name=sn)
        by_id = {r["id"]: r["v"] for r in _rows(client, sn, "SELECT * FROM t")}
        assert by_id[100] is None, "NULL must read back as NULL, not 0"
        assert by_id[101] == 0
        assert by_id[1] == 10, "pre-ALTER non-null rows unchanged"
        assert len(by_id) == 42

        # Total weight is exactly one per PK (no NULL-vs-0 consolidation error).
        tid = client.resolve_table(sn, "t")[0]
        assert all(r.weight == 1 for r in client.scan(tid))
    finally:
        client.drop_schema(sn)


def test_drop_not_null_secondary_indexed_column(client):
    sn = "adrop" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn)
        client.execute_sql("CREATE INDEX ix ON t (v)", schema_name=sn)

        client.execute_sql("ALTER TABLE t ALTER COLUMN v DROP NOT NULL", schema_name=sn)

        # Existing rows still seek by the indexed column; a later NULL is simply
        # absent from the index but present in the table.
        assert [r["id"] for r in _rows(client, sn, "SELECT id FROM t WHERE v = 20")] == [2]
        client.execute_sql("INSERT INTO t VALUES (4, NULL)", schema_name=sn)
        ids = sorted(r["id"] for r in _rows(client, sn, "SELECT * FROM t"))
        assert ids == [1, 2, 3, 4]
    finally:
        client.drop_schema(sn)


# ── RESTRICT on a dependent view ────────────────────────────────────────────


def test_drop_restrict_dependent_view_leaves_catalog_intact(client):
    sn = "adrop" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10, 100)", schema_name=sn)
        client.execute_sql("CREATE VIEW v AS SELECT id, b FROM t", schema_name=sn)

        with pytest.raises(Exception):
            client.execute_sql("ALTER TABLE t DROP COLUMN a", schema_name=sn)
        with pytest.raises(Exception):
            client.execute_sql("ALTER TABLE t ALTER COLUMN a DROP NOT NULL", schema_name=sn)

        # The catalog is unchanged: `a` is still visible/usable and the view works.
        rows = _rows(client, sn, "SELECT * FROM t")
        assert set(rows[0]._fields) == {"id", "a", "b"}
        assert [r["b"] for r in _rows(client, sn, "SELECT * FROM v")] == [100]
    finally:
        client.drop_schema(sn)
