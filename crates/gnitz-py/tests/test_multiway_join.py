"""E2E tests: multi-way joins expressed as chained JOIN-body subqueries.

A JOIN body (`WITH h0 AS (SELECT ... FROM a JOIN b ON ...) SELECT ... FROM h0 JOIN c
...`) compiles into a hidden join view. Its synthetic `_join_pk` needs NO special
handling downstream: a join view is a first-class relation whose registered schema
already carries `_join_pk`, so the final join resolves its columns like any other
relation. Nesting these gives arbitrary N-way joins, incrementally maintained.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_multiway_join.py -v --tb=short
"""
import random
import pytest


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn):
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _rows(client, sn, view, keys):
    vid = client.resolve_table(sn, view)[0]
    return sorted(tuple(r._asdict()[k] for k in keys) for r in client.scan(vid) if r.weight > 0)


def _rows_ns(client, sn, view, keys):
    """Like _rows but NULL-safe in the sort (NULLs sort last) — for outer joins."""
    vid = client.resolve_table(sn, view)[0]
    rows = [tuple(r._asdict()[k] for k in keys) for r in client.scan(vid) if r.weight > 0]
    return sorted(rows, key=lambda t: tuple((x is None, x) for x in t))


def _tables(client, sn, names):
    for t in names:
        client.execute_sql(
            f"CREATE TABLE {t} (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
            schema_name=sn,
        )


class TestMultiwayJoin:
    def test_three_way_via_join_body_cte(self, client):
        """h0 = a JOIN b (JOIN-body CTE) feeding the final h0 JOIN c — a 3-way join."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS "
                "WITH h0 AS (SELECT a.id AS aid, b.k AS bk FROM a JOIN b ON a.k = b.id) "
                "SELECT h0.aid AS aid, c.v AS cv FROM h0 JOIN c ON h0.bk = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500, 0)", schema_name=sn)  # b.id=10, b.k=500
            client.execute_sql("INSERT INTO c VALUES (500, 0, 99)", schema_name=sn)  # c.id=500, c.v=99
            client.execute_sql("INSERT INTO a VALUES (1, 10, 7)", schema_name=sn)  # a.k=10->b10; bk=500->c500
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99)]

            # A second `a` row flows through both joins.
            client.execute_sql("INSERT INTO a VALUES (2, 10, 8)", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99), (2, 99)]

            # Retract the bridge row in b — both output rows disappear.
            client.execute_sql("DELETE FROM b WHERE id = 10", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "cv"]) == []
        finally:
            _cleanup(client, sn)

    def test_three_way_via_join_body_derived(self, client):
        """The FROM-clause form: a JOIN-body derived table joined with a base."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT h0.aid AS aid, c.v AS cv "
                "FROM (SELECT a.id AS aid, b.k AS bk FROM a JOIN b ON a.k = b.id) h0 "
                "JOIN c ON h0.bk = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 0, 99)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 7)", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99)]
        finally:
            _cleanup(client, sn)

    def test_four_way_via_nested_join_ctes(self, client):
        """Nested JOIN CTEs: h0 = a⋈b, h1 = h0⋈c, final = h1⋈d — a 4-way join."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c", "d"])
            client.execute_sql(
                "CREATE VIEW v AS "
                "WITH h0 AS (SELECT a.id AS aid, b.k AS bk FROM a JOIN b ON a.k = b.id), "
                "h1 AS (SELECT h0.aid AS aid, c.k AS ck FROM h0 JOIN c ON h0.bk = c.id) "
                "SELECT h1.aid AS aid, d.v AS dv FROM h1 JOIN d ON h1.ck = d.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 20, 0)", schema_name=sn)  # b.id=10 b.k=20
            client.execute_sql("INSERT INTO c VALUES (20, 30, 0)", schema_name=sn)  # c.id=20 c.k=30
            client.execute_sql("INSERT INTO d VALUES (30, 0, 42)", schema_name=sn)  # d.id=30 d.v=42
            client.execute_sql("INSERT INTO a VALUES (1, 10, 0)", schema_name=sn)  # chains 10->20->30
            assert _rows(client, sn, "v", ["aid", "dv"]) == [(1, 42)]

            # Break the middle link (c row) — the whole chain retracts.
            client.execute_sql("DELETE FROM c WHERE id = 20", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "dv"]) == []
        finally:
            _cleanup(client, sn)

    def test_join_cte_with_filter_then_aggregate_final(self, client):
        """A JOIN-body CTE feeding a GROUP BY final — join-then-aggregate."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b"])
            client.execute_sql(
                "CREATE VIEW v AS "
                "WITH j AS (SELECT b.k AS grp, a.v AS av FROM a JOIN b ON a.k = b.id) "
                "SELECT grp, SUM(av) AS total FROM j GROUP BY grp",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 1, 0), (20, 2, 0)", schema_name=sn)  # id,grp
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10, 100), (2, 10, 50), (3, 20, 200)", schema_name=sn
            )
            # j: a1->grp1(av100), a2->grp1(av50), a3->grp2(av200). SUM by grp: grp1=150, grp2=200.
            assert _rows(client, sn, "v", ["grp", "total"]) == [(1, 150), (2, 200)]
        finally:
            _cleanup(client, sn)

    def test_join_cte_column_aliases_rejected(self, client):
        """Positional column aliases on a JOIN-body CTE are rejected (synthetic PK)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            with pytest.raises(Exception) as ei:
                client.execute_sql(
                    "CREATE VIEW v AS WITH h0(x, y) AS (SELECT a.id AS aid, b.k AS bk FROM a JOIN b ON a.k = b.id) "
                    "SELECT h0.x AS x FROM h0 JOIN c ON h0.y = c.id",
                    schema_name=sn,
                )
            assert "alias" in str(ei.value).lower(), str(ei.value)
        finally:
            _cleanup(client, sn)


class TestDirectMultiwayJoin:
    """Standard SQL multi-way join syntax `a JOIN b JOIN c ...`, auto-decomposed into a
    left-deep chain of hidden 2-way segments (no hand-nested CTEs). Original aliases are
    tracked through a provenance map, so a later ON/projection/WHERE reference to `a.x`
    resolves to the accumulated hidden segment's physical column."""

    def test_three_way_direct(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.v AS cv "
                "FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 0, 99)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 7)", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99)]

            client.execute_sql("INSERT INTO a VALUES (2, 10, 8)", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99), (2, 99)]

            client.execute_sql("DELETE FROM b WHERE id = 10", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "cv"]) == []
        finally:
            _cleanup(client, sn)

    def test_three_way_direct_with_where(self, client):
        """A WHERE over the whole join folds into the final segment's residual."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.v AS cv "
                "FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id WHERE c.v > 50",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500, 0), (20, 600, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 0, 99), (600, 0, 10)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 7), (2, 20, 8)", schema_name=sn)
            # a1 -> b10 -> c500(v99) kept; a2 -> b20 -> c600(v10) filtered by WHERE.
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99)]
        finally:
            _cleanup(client, sn)

    def test_second_on_references_first_table(self, client):
        """The 2nd ON references the leftmost table `a` (deep in the accumulator), not
        just the immediately-prior relation — provenance must resolve it."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.v AS cv "
                "FROM a JOIN b ON a.k = b.id JOIN c ON a.v = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 0, 0)", schema_name=sn)  # b.id=10
            client.execute_sql("INSERT INTO c VALUES (700, 0, 88)", schema_name=sn)  # c.id=700
            client.execute_sql("INSERT INTO a VALUES (1, 10, 700)", schema_name=sn)  # a.k=10->b; a.v=700->c
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 88)]
        finally:
            _cleanup(client, sn)

    def test_four_way_direct(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c", "d"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, d.v AS dv "
                "FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id JOIN d ON c.k = d.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 20, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (20, 30, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO d VALUES (30, 0, 42)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 0)", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "dv"]) == [(1, 42)]

            client.execute_sql("DELETE FROM c WHERE id = 20", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "dv"]) == []
        finally:
            _cleanup(client, sn)

    def test_star_projection_drops_intermediate_join_pk(self, client):
        """`SELECT *` over a multi-way join exposes only the final `_join_pk` plus real
        columns — the intermediate segment's synthetic PK never accumulates."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT * FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 0, 99)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 7)", schema_name=sn)
            vid = client.resolve_table(sn, "v")[0]
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert len(rows) == 1
            names = list(rows[0]._asdict().keys())
            # Exactly one `_join_pk` (the final segment's) — no accumulated intermediate PK.
            assert names.count("_join_pk") == 1, names
        finally:
            _cleanup(client, sn)


class TestMultiwayOuterJoin:
    """LEFT / RIGHT / FULL joins in a left-deep multi-way chain (syntactic order:
    `a LEFT JOIN b JOIN c` is `(a LEFT JOIN b) JOIN c`). Each step's emit is the
    standard 2-way outer emit; null-filled rows carry NULL keys that drop out of a
    later inner join, as SQL requires."""

    def test_left_join_first_step(self, client):
        """(a LEFT JOIN b) JOIN c — an a with no b still joins c via an a-column key."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid, c.v AS cv "
                "FROM a LEFT JOIN b ON a.k = b.id JOIN c ON a.v = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 0, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (700, 0, 88)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 700), (2, 99, 700)", schema_name=sn)  # a2 no b
            assert _rows_ns(client, sn, "v", ["aid", "bid", "cv"]) == [(1, 10, 88), (2, None, 88)]
        finally:
            _cleanup(client, sn)

    def test_left_join_last_step(self, client):
        """(a JOIN b) LEFT JOIN c — preserved rows null-fill when c has no match."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.id AS cid "
                "FROM a JOIN b ON a.k = b.id LEFT JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500, 0), (20, 999, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 0, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 0), (2, 20, 0)", schema_name=sn)  # a2 -> b20 -> no c
            assert _rows_ns(client, sn, "v", ["aid", "cid"]) == [(1, 500), (2, None)]
        finally:
            _cleanup(client, sn)

    def test_left_join_last_null_fill_retraction(self, client):
        """A ΔC that first satisfies a preserved row retracts its null-fill and emits the
        match — the incremental outer-join path through a chain."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.id AS cid "
                "FROM a JOIN b ON a.k = b.id LEFT JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (20, 999, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (2, 20, 0)", schema_name=sn)  # -> b20 -> no c yet
            assert _rows_ns(client, sn, "v", ["aid", "cid"]) == [(2, None)]
            client.execute_sql("INSERT INTO c VALUES (999, 0, 0)", schema_name=sn)  # b20.k=999 now matches
            assert _rows_ns(client, sn, "v", ["aid", "cid"]) == [(2, 999)]
        finally:
            _cleanup(client, sn)

    def test_full_join_last_step(self, client):
        """(a JOIN b) FULL JOIN c — unmatched c rows are preserved with a NULL left."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.id AS cid "
                "FROM a JOIN b ON a.k = b.id FULL JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 0, 0), (600, 0, 0)", schema_name=sn)  # c600 unmatched
            client.execute_sql("INSERT INTO a VALUES (1, 10, 0)", schema_name=sn)
            assert _rows_ns(client, sn, "v", ["aid", "cid"]) == [(1, 500), (None, 600)]
        finally:
            _cleanup(client, sn)


class TestMultiwayLifecycle:
    """Backfill (view created after data) and DROP-cascade of the hidden segments."""

    def test_backfill_view_after_data(self, client):
        """Data exists before CREATE VIEW — the whole chain must backfill."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql("INSERT INTO b VALUES (10, 500, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 0, 99)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 7), (2, 10, 8)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.v AS cv "
                "FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99), (2, 99)]
            # And still incremental after backfill.
            client.execute_sql("INSERT INTO a VALUES (3, 10, 9)", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99), (2, 99), (3, 99)]
        finally:
            _cleanup(client, sn)

    def test_drop_cascades_hidden_segments(self, client):
        """DROP VIEW retires every hidden join segment, freeing the base tables to drop."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("DROP VIEW v", schema_name=sn)
            # If a hidden segment were orphaned it would still depend on a base and RESTRICT.
            client.execute_sql("DROP TABLE a", schema_name=sn)
            client.execute_sql("DROP TABLE b", schema_name=sn)
            client.execute_sql("DROP TABLE c", schema_name=sn)
        finally:
            _cleanup(client, sn)

    def test_drop_base_under_chain_restricts(self, client):
        """A base table feeding a live multi-way chain cannot be dropped."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            with pytest.raises(Exception):
                client.execute_sql("DROP TABLE b", schema_name=sn)
        finally:
            _cleanup(client, sn)


class TestMultiwayEdgeCases:
    def test_range_predicate_in_second_join(self, client):
        """A band/range predicate (`<`) in the second join of a multi-way chain."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.id AS cid "
                "FROM a JOIN b ON a.k = b.id JOIN c ON a.v < c.k",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 0, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (100, 50, 0), (200, 5, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 10)", schema_name=sn)  # a.v=10 < 50, not < 5
            assert _rows(client, sn, "v", ["aid", "cid"]) == [(1, 100)]
        finally:
            _cleanup(client, sn)

    def test_compound_pk_table_in_chain(self, client):
        """A compound-PK table participating in a multi-way join."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (x BIGINT NOT NULL, y BIGINT NOT NULL, k BIGINT NOT NULL, PRIMARY KEY (x, y))",
                schema_name=sn,
            )
            client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.x AS ax, c.v AS cv "
                "FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 99)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 2, 10)", schema_name=sn)
            assert _rows(client, sn, "v", ["ax", "cv"]) == [(1, 99)]
        finally:
            _cleanup(client, sn)

    def test_integer_width_promotion_on_join_key(self, client):
        """INT join key on one side, BIGINT on the other, chained through a third table."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k INT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE b (id INT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.v AS cv "
                "FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 99)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10)", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99)]
        finally:
            _cleanup(client, sn)

    def test_null_join_key_never_matches(self, client):
        """A NULL join key (nullable column) matches nothing, even mid-chain."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql("CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT)", schema_name=sn)
            client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.v AS cv "
                "FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 99)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10), (2, NULL)", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99)]  # a2's NULL key drops out
        finally:
            _cleanup(client, sn)

    def test_project_columns_from_all_relations(self, client):
        """The final projection pulls a column from each of the three joined relations."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.v AS av, b.v AS bv, c.v AS cv "
                "FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500, 11)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 0, 22)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 33)", schema_name=sn)
            assert _rows(client, sn, "v", ["av", "bv", "cv"]) == [(33, 11, 22)]
        finally:
            _cleanup(client, sn)

    def test_multi_column_equi_key(self, client):
        """A 2-column equijoin key (`a.k1 = b.k1 AND a.k2 = b.k2`) in a multi-way chain."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k1 BIGINT NOT NULL, k2 BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (k1 BIGINT NOT NULL, k2 BIGINT NOT NULL, w BIGINT NOT NULL, PRIMARY KEY (k1, k2))",
                schema_name=sn,
            )
            client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, v BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.v AS cv "
                "FROM a JOIN b ON a.k1 = b.k1 AND a.k2 = b.k2 JOIN c ON b.w = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (5, 6, 500)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 99)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 5, 6), (2, 5, 7)", schema_name=sn)  # a2 (5,7) no match
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99)]
        finally:
            _cleanup(client, sn)

    def test_update_reroutes_through_chain(self, client):
        """An UPDATE to a mid-chain join key reroutes the row to a different final match."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.v AS cv "
                "FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500, 0), (20, 600, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 0, 99), (600, 0, 88)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 7)", schema_name=sn)  # a1 -> b10 -> c500 (99)
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99)]
            # Re-key a1 to route through b20 -> c600.
            client.execute_sql("UPDATE a SET k = 20 WHERE id = 1", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 88)]
        finally:
            _cleanup(client, sn)

    def test_deep_eight_way_join(self, client):
        """An 8-way join (7 hidden segments) — deep chains compile and evaluate."""
        sn = "s" + _uid()
        client.create_schema(sn)
        n = 8
        try:
            for i in range(n):
                client.execute_sql(
                    f"CREATE TABLE t{i} (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
                    schema_name=sn,
                )
            joins = " ".join(f"JOIN t{i} ON t{i - 1}.k = t{i}.id" for i in range(1, n))
            client.execute_sql(f"CREATE VIEW v AS SELECT t0.id AS x, t{n - 1}.v AS y FROM t0 {joins}", schema_name=sn)
            # A single chain: t0.k -> t1.id, t1.k -> t2.id, ... all equal to 1 here.
            for i in range(n):
                nxt = 1 if i < n - 1 else 0
                client.execute_sql(f"INSERT INTO t{i} VALUES (1, {nxt}, {100 + i})", schema_name=sn)
            assert _rows(client, sn, "v", ["x", "y"]) == [(1, 100 + n - 1)]
        finally:
            _cleanup(client, sn)
