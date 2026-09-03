import pytest
import gnitz
from _uid import uid as _uid




def test_create_drop_view(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    tid = client.create_table(sn, "src", cols)
    schema = gnitz.Schema(cols)
    vid = client.create_view(sn, "v", tid, schema)
    assert vid > 0

    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=42)
    client.push(tid, batch)
    rows = list(client.scan(vid))
    assert len(rows) == 1
    assert rows[0].val == 42

    client.drop_view(sn, "v")
    client.drop_table(sn, "src")
    client.drop_schema(sn)


def test_view_on_view(client):
    """A SQL view reading from another SQL view propagates inserts end-to-end."""
    sn = "s" + _uid()
    client.create_schema(sn)
    try:
        client.execute_sql(
            "CREATE TABLE t (pk BIGINT NOT NULL PRIMARY KEY, val BIGINT NOT NULL, cat BIGINT NOT NULL)",
            schema_name=sn,
        )
        # v1: filter to cat=1
        client.execute_sql(
            "CREATE VIEW v1 AS SELECT * FROM t WHERE cat = 1",
            schema_name=sn,
        )
        # v2: filter v1 to val > 10
        client.execute_sql(
            "CREATE VIEW v2 AS SELECT * FROM v1 WHERE val > 10",
            schema_name=sn,
        )
        v2_id = client.resolve_table(sn, "v2")[0]

        client.execute_sql(
            "INSERT INTO t VALUES (1, 5, 1), (2, 20, 1), (3, 30, 2), (4, 15, 1)",
            schema_name=sn,
        )

        rows = client.scan(v2_id)
        # cat=1 AND val>10: rows 2 (val=20) and 4 (val=15)
        assert len(rows) == 2, f"expected 2 rows from view-on-view, got {len(rows)}: {rows}"
        vals = sorted(r["val"] for r in rows)
        assert vals == [15, 20]

        client.execute_sql("DROP VIEW v2", schema_name=sn)
        client.execute_sql("DROP VIEW v1", schema_name=sn)
        client.execute_sql("DROP TABLE t", schema_name=sn)
    finally:
        client.drop_schema(sn)


def test_view_scan_propagates_inserts(client):
    """Push rows to source; scan view returns same rows."""
    sn = "s" + _uid()
    client.create_schema(sn)
    cols = [gnitz.ColumnDef("pk", gnitz.TypeCode.U64, primary_key=True),
            gnitz.ColumnDef("val", gnitz.TypeCode.I64)]
    schema = gnitz.Schema(cols)
    tid = client.create_table(sn, "src", cols)
    vid = client.create_view(sn, "v", tid, schema)

    batch = gnitz.ZSetBatch(schema)
    batch.append(pk=1, val=10).append(pk=2, val=20).append(pk=3, val=30)
    client.push(tid, batch)

    result = client.scan(vid)
    assert len(result) == 3
    vals = sorted(r.val for r in result)
    assert vals == [10, 20, 30]

    client.drop_view(sn, "v")
    client.drop_table(sn, "src")
    client.drop_schema(sn)


class TestViewOutputColumnAliases:
    """`CREATE VIEW v (x, y) AS …` renames the body's visible output columns
    positionally — the same rule, through the same helper, that a CTE's
    `WITH d(a, b)` and a derived table's `AS d(a, b)` use."""

    def _setup(self, client, sn):
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 10, 100), (2, 20, 200)", schema_name=sn)

    def test_aliases_rename_the_output(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("CREATE VIEW v (x, y) AS SELECT id, a FROM t", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = client.scan(vid).mappings()
            assert sorted(rows[0].keys()) == ["x", "y"], rows[0]
            assert sorted((r["x"], r["y"]) for r in rows) == [(1, 10), (2, 20)]
        finally:
            client.drop_schema(sn)

    def test_aliases_skip_a_join_bodys_hidden_key_region(self, client):
        """A join body's synthetic `_join_pk` is hidden, so the alias list names
        exactly the columns a reader sees — no off-by-one against the key slot."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql("INSERT INTO u VALUES (7, 10)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW j (left_id, right_val) AS SELECT t.id, u.a FROM t JOIN u ON t.a = u.a",
                schema_name=sn,
            )
            rows = client.scan(client.resolve_table(sn, "j")[0]).mappings()
            assert sorted(rows[0].keys()) == ["left_id", "right_val"], rows[0]
            assert [(r["left_id"], r["right_val"]) for r in rows] == [(1, 10)]
        finally:
            client.drop_schema(sn)

    def test_aliases_rename_the_projection_but_do_not_disambiguate_it(self, client):
        """The alias list renames what the body produced; it does not stand in for
        naming the projection. A body that names one output twice is rejected
        where it is written, before any alias is applied — the fix is `AS` on the
        projection item."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE TABLE u (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql("INSERT INTO u VALUES (7, 10)", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="duplicate column name 'id' in CREATE VIEW projection"):
                client.execute_sql(
                    "CREATE VIEW j (x, y) AS SELECT t.id, u.id FROM t JOIN u ON t.a = u.a", schema_name=sn
                )
            client.execute_sql(
                "CREATE VIEW j (x, y) AS SELECT t.id AS l, u.id AS r FROM t JOIN u ON t.a = u.a",
                schema_name=sn,
            )
            assert sorted(client.scan(client.resolve_table(sn, "j")[0]).mappings()[0].keys()) == ["x", "y"]
        finally:
            client.drop_schema(sn)

    def test_alias_arity_and_duplicates_are_refused(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            with pytest.raises(gnitz.GnitzError, match="defines 3 column aliases but body returns 2"):
                client.execute_sql("CREATE VIEW v (x, y, z) AS SELECT id, a FROM t", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="duplicate column name 'x'"):
                client.execute_sql("CREATE VIEW v (x, x) AS SELECT id, a FROM t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_a_declared_type_on_an_alias_is_refused(self, client):
        """A view's column types come from its body; a declared one would have to
        be checked or ignored, and gnitz does neither."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE VIEW v (x BIGINT) AS SELECT id FROM t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_alter_view_honours_the_same_aliases(self, client):
        """One clause, one meaning: `ALTER VIEW v (x, y) AS …` renames the output
        exactly as the CREATE form does, through the same helper."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t", schema_name=sn)
            client.execute_sql("ALTER VIEW v (x, y) AS SELECT id, b FROM t", schema_name=sn)
            rows = client.scan(client.resolve_table(sn, "v")[0]).mappings()
            assert sorted(rows[0].keys()) == ["x", "y"], rows[0]
            assert sorted((r["x"], r["y"]) for r in rows) == [(1, 100), (2, 200)]
            with pytest.raises(gnitz.GnitzError, match="defines 1 column aliases but body returns 2"):
                client.execute_sql("ALTER VIEW v (only_one) AS SELECT id, a FROM t", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_alter_view_still_refuses_a_with_clause(self, client):
        """sqlparser reads a `WITH (…)` between ALTER VIEW's column list and its
        `AS`. gnitz rejects it: letting ALTER VIEW set a budget would make the
        clause-less form silently drop the one the view already had."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("CREATE VIEW v AS SELECT id, a FROM t", schema_name=sn)
            with pytest.raises(gnitz.GnitzError, match="ALTER VIEW: WITH options is not supported"):
                client.execute_sql(
                    "ALTER VIEW v WITH (capacity = '4 MB') AS SELECT id, b FROM t", schema_name=sn
                )
        finally:
            client.drop_schema(sn)

    def test_aliases_reach_the_catalog_and_resolve_downstream(self, client):
        """The rename lands on the registered view schema, not just the local
        plan: a second view selects the aliased names, and the body's own names
        no longer resolve."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql("CREATE VIEW v (x, y) AS SELECT id, a FROM t", schema_name=sn)
            client.execute_sql("CREATE VIEW downstream AS SELECT x, y FROM v", schema_name=sn)
            rows = client.scan(client.resolve_table(sn, "downstream")[0]).mappings()
            assert sorted((r["x"], r["y"]) for r in rows) == [(1, 10), (2, 20)]
            with pytest.raises(gnitz.GnitzError):
                client.execute_sql("CREATE VIEW nope AS SELECT id FROM v", schema_name=sn)
        finally:
            client.drop_schema(sn)

    def test_aliases_survive_a_with_clause(self, client):
        """The alias step runs over the final segment whatever budgets it carries,
        so a bounded view is registered under the aliased names too."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            self._setup(client, sn)
            client.execute_sql(
                "CREATE VIEW b (x, y) WITH (capacity = '4 MB') AS SELECT id, a FROM t", schema_name=sn
            )
            rows = client.scan(client.resolve_table(sn, "b")[0]).mappings()
            assert sorted(rows[0].keys()) == ["x", "y"], rows[0]
            # Still bounded: a bounded view is a leaf.
            with pytest.raises(gnitz.GnitzError, match="views cannot be created over it"):
                client.execute_sql("CREATE VIEW over_it AS SELECT x FROM b", schema_name=sn)
        finally:
            client.drop_schema(sn)
