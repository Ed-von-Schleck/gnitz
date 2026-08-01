"""End-to-end ALTER TABLE / ALTER VIEW tests.

Run with GNITZ_WORKERS=4 so the exchange/join-view rename and the FK/index
paths exercise the distributed engine, not just a single worker.
"""

import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100_000, 999_999))


def _live(client, tid):
    return list(client.scan(tid))


# ── Rename table ────────────────────────────────────────────────────────────


def test_rename_table_data_intact_and_second_conn_sees_it(client, server):
    sn = "alt" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20)", schema_name=sn)
        client.execute_sql("ALTER TABLE t RENAME TO t2", schema_name=sn)

        # Old name gone, new name resolves, data intact.
        with pytest.raises(Exception):
            client.resolve_table(sn, "t")
        tid2 = client.resolve_table(sn, "t2")[0]
        assert sorted(r["v"] for r in _live(client, tid2)) == [10, 20]

        # A second connection sees the rename on its next statement.
        with gnitz.connect(server) as c2:
            assert c2.resolve_table(sn, "t2")[0] == tid2
            with pytest.raises(Exception):
                c2.resolve_table(sn, "t")
    finally:
        client.drop_schema(sn)


def test_rename_indexed_column_keeps_index_working(client):
    sn = "alt" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE INDEX iv ON t(v)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 100), (2, 200)", schema_name=sn)
        # Rename the indexed column; the index binds by ordinal, so it survives.
        client.execute_sql("ALTER TABLE t RENAME COLUMN v TO w", schema_name=sn)
        # A predicate over the renamed column still resolves (and reads correctly).
        client.execute_sql("INSERT INTO t VALUES (3, 100)", schema_name=sn)
        tid = client.resolve_table(sn, "t")[0]
        w_vals = sorted(r["w"] for r in _live(client, tid))
        assert w_vals == [100, 100, 200]
    finally:
        client.drop_schema(sn)


def test_rename_fk_child_and_parent(client):
    sn = "alt" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE parent (id BIGINT NOT NULL PRIMARY KEY, name BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE child (id BIGINT NOT NULL PRIMARY KEY, "
            "ref BIGINT NOT NULL REFERENCES parent(id))",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO parent VALUES (1, 11)", schema_name=sn)
        client.execute_sql("INSERT INTO child VALUES (1, 1)", schema_name=sn)

        # Rename both endpoints; the FK (ordinal-bound) keeps validating.
        client.execute_sql("ALTER TABLE child RENAME TO child2", schema_name=sn)
        client.execute_sql("ALTER TABLE parent RENAME TO parent2", schema_name=sn)

        # A valid child insert still succeeds; a dangling FK still rejects.
        client.execute_sql("INSERT INTO child2 VALUES (2, 1)", schema_name=sn)
        with pytest.raises(Exception):
            client.execute_sql("INSERT INTO child2 VALUES (3, 999)", schema_name=sn)
    finally:
        client.drop_schema(sn)


def test_rename_populated_join_view_keeps_output(client):
    sn = "alt" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, av BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW j AS SELECT a.id AS id, a.av AS av, b.bv AS bv "
            "FROM a JOIN b ON a.id = b.id",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO a VALUES (1, 10), (2, 20)", schema_name=sn)
        client.execute_sql("INSERT INTO b VALUES (1, 100), (2, 200)", schema_name=sn)

        vid = client.resolve_table(sn, "j")[0]
        before = sorted((r["av"], r["bv"], r.weight) for r in _live(client, vid))
        assert len(before) == 2

        # Renaming a populated exchange-join view must not double or drop its
        # output weights (no re-backfill).
        client.execute_sql("ALTER TABLE j RENAME TO j2", schema_name=sn)
        vid2 = client.resolve_table(sn, "j2")[0]
        after = sorted((r["av"], r["bv"], r.weight) for r in _live(client, vid2))
        assert after == before, f"join-view rename changed output: {before} -> {after}"
    finally:
        client.drop_schema(sn)


# ── ADD / DROP CONSTRAINT ───────────────────────────────────────────────────


def test_add_drop_unique_constraint(client):
    sn = "alt" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("ALTER TABLE t ADD CONSTRAINT uq UNIQUE (v)", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 5)", schema_name=sn)
        with pytest.raises(Exception):
            client.execute_sql("INSERT INTO t VALUES (2, 5)", schema_name=sn)

        # Drop by name; the duplicate is then allowed.
        client.execute_sql("ALTER TABLE t DROP CONSTRAINT uq", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (2, 5)", schema_name=sn)

        # DROP CONSTRAINT IF EXISTS of a missing constraint no-ops.
        client.execute_sql("ALTER TABLE t DROP CONSTRAINT IF EXISTS nope", schema_name=sn)

        # An unnamed UNIQUE constraint creates.
        client.execute_sql("ALTER TABLE t ADD CONSTRAINT UNIQUE (pk)", schema_name=sn)
    finally:
        client.drop_schema(sn)


# ── ALTER VIEW ... AS ───────────────────────────────────────────────────────


def test_alter_view_replaces_and_backfills(client):
    sn = "alt" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)", schema_name=sn)
        client.execute_sql("CREATE VIEW vw AS SELECT pk, v FROM t WHERE v >= 20", schema_name=sn)
        vid = client.resolve_table(sn, "vw")[0]
        assert sorted(r["v"] for r in _live(client, vid)) == [20, 30]

        # Redefine with a stricter filter; the new definition is backfilled over
        # the populated base.
        client.execute_sql("ALTER VIEW vw AS SELECT pk, v FROM t WHERE v >= 30", schema_name=sn)
        vid2 = client.resolve_table(sn, "vw")[0]
        assert sorted(r["v"] for r in _live(client, vid2)) == [30]
        # A fresh vid (ids are never reused).
        assert vid2 != vid

        # New inserts flow through the new definition.
        client.execute_sql("INSERT INTO t VALUES (4, 40), (5, 5)", schema_name=sn)
        assert sorted(r["v"] for r in _live(client, vid2)) == [30, 40]
    finally:
        client.drop_schema(sn)


def test_alter_view_restrict_and_self_reference(client):
    sn = "alt" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW base AS SELECT pk, v FROM t", schema_name=sn)
        client.execute_sql("CREATE VIEW dependent AS SELECT pk, v FROM base", schema_name=sn)

        # Redefining `base` with a dependent view present is RESTRICTed.
        with pytest.raises(Exception):
            client.execute_sql("ALTER VIEW base AS SELECT pk, v FROM t WHERE v > 0", schema_name=sn)
        # `base` still exists and is unchanged.
        assert client.resolve_table(sn, "base")[0] > 0

        # Self-reference is rejected before any zone is issued.
        with pytest.raises(Exception):
            client.execute_sql("ALTER VIEW dependent AS SELECT pk, v FROM dependent", schema_name=sn)
        # `dependent` survives the rejected self-reference.
        assert client.resolve_table(sn, "dependent")[0] > 0
    finally:
        client.drop_schema(sn)


# ── Transaction on another connection is not aborted by a rename ────────────


def test_rename_does_not_abort_other_conn_transaction(client, server):
    sn = "alt" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10)", schema_name=sn)

        with gnitz.connect(server) as c2:
            # conn A opens a transaction and buffers a write.
            client.execute_sql("BEGIN", schema_name=sn)
            client.execute_sql("INSERT INTO t VALUES (2, 20)", schema_name=sn)
            # conn B renames a DIFFERENT relation (layout-preserving DDL).
            c2.execute_sql("ALTER TABLE t RENAME TO t2", schema_name=sn)
            # conn A commits — a rename is layout-preserving, so the txn is not
            # aborted (the schema pin that aborts on a layout change is plan 4's).
            client.execute_sql("COMMIT", schema_name=sn)

        tid = client.resolve_table(sn, "t2")[0]
        assert sorted(r["v"] for r in _live(client, tid)) == [10, 20]
    finally:
        client.drop_schema(sn)
