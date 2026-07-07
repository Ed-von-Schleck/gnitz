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

    def test_join_cte_column_aliases_apply_to_visible(self, client):
        """Positional column aliases on a JOIN-body CTE apply to its *visible*
        columns — the synthetic join PK is hidden and skipped, so `h0(x, y)`
        renames the two visible projected columns, and a downstream reference
        (`h0.y` in the ON, `h0.x` in the SELECT) resolves them by the new names."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            client.execute_sql(
                "CREATE VIEW v AS WITH h0(x, y) AS (SELECT a.id AS aid, b.k AS bk FROM a JOIN b ON a.k = b.id) "
                "SELECT h0.x AS x FROM h0 JOIN c ON h0.y = c.id",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # a.k=10 → b.id=10 (h0: x=a.id=1, y=b.k=20); h0.y=20 → c.id=20.
            client.execute_sql("INSERT INTO a VALUES (1, 10, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO b VALUES (10, 20, 2)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (20, 0, 3)", schema_name=sn)
            rows = [r for r in client.scan(vid) if r.weight > 0]
            assert [r["x"] for r in rows] == [1], [r._asdict() for r in rows]
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
            # The synthetic key is hidden: `SELECT *` exposes only the real columns.
            assert "_join_pk" not in rows[0]._asdict()
            # include_hidden surfaces the physical schema: exactly one `_join_pk`
            # (the final segment's) — no accumulated intermediate PK.
            raw = [r for r in client.scan(vid, include_hidden=True) if r.weight > 0]
            raw_names = list(raw[0]._asdict().keys())
            assert raw_names.count("_join_pk") == 1, raw_names
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


def _wrows(client, sn, view, keys):
    """Rows as (tuple, net_weight) pairs — NULL-safe sort — so a test can assert the
    multiplicity of a bag-valued (weight > 1) row, not just its presence."""
    vid = client.resolve_table(sn, view)[0]
    agg = {}
    for r in client.scan(vid):
        t = tuple(r._asdict()[k] for k in keys)
        agg[t] = agg.get(t, 0) + r.weight
    rows = [(t, w) for t, w in agg.items() if w != 0]
    return sorted(rows, key=lambda p: tuple((x is None, x) for x in p[0]))


class TestMultiwayPruning:
    """Intermediate segments project only the columns a later ON, the final
    projection, or the final WHERE still references. Correctness is unchanged; the
    tests exercise later-ON-only columns, wide tables, outer null-fills, and
    backfill parity under pruning."""

    def test_intermediate_retains_later_on_column(self, client):
        """A column referenced only by a LATER ON (and absent from the final
        projection) must survive the intermediate segment; a truly-dead column is
        pruned. Result matches a full recompute, under insert/update/delete."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL, "
                "dead BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, cv BIGINT NOT NULL)", schema_name=sn)
            # a.v is used ONLY by the 2nd ON; a.dead by nothing. Both flow through h0.
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.cv AS ccv "
                "FROM a JOIN b ON a.k = b.id JOIN c ON a.v = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 0)", schema_name=sn)  # b.id=10
            client.execute_sql("INSERT INTO c VALUES (700, 88)", schema_name=sn)  # c.id=700
            client.execute_sql("INSERT INTO a VALUES (1, 10, 700, 999)", schema_name=sn)  # a.k=10->b, a.v=700->c
            assert _rows(client, sn, "v", ["aid", "ccv"]) == [(1, 88)]

            # Update the dead column — no effect on the result.
            client.execute_sql("UPDATE a SET dead = 12345 WHERE id = 1", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "ccv"]) == [(1, 88)]

            # A second a whose later-ON key a.v matches a different c.
            client.execute_sql("INSERT INTO c VALUES (800, 77)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (2, 10, 800, 0)", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "ccv"]) == [(1, 88), (2, 77)]

            # Retract c(700) — a1's 2nd-ON match breaks.
            client.execute_sql("DELETE FROM c WHERE id = 700", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "ccv"]) == [(2, 77)]
        finally:
            _cleanup(client, sn)

    def test_four_way_wide_tables_prune_enables_plan(self, client):
        """A 4-way join of 18-column tables with a 2-column final SELECT. Unpruned the
        final overflow check is 1 + 55 + 18 = 74 > 65 (MAX_COLUMNS) and the view is
        rejected; pruning the accumulator to its live columns (h1 → ~3 cols) drops it
        far under the cap, so the view plans and maintains correctly."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            # 18 columns each: id (PK) + a key column + 16 pad columns.
            def wide(tab, keyname):
                pads = ", ".join(f"p{i} BIGINT NOT NULL" for i in range(16))
                client.execute_sql(
                    f"CREATE TABLE {tab} (id BIGINT NOT NULL PRIMARY KEY, {keyname} BIGINT NOT NULL, {pads})",
                    schema_name=sn,
                )

            wide("a", "k1")
            wide("b", "k2")
            wide("c", "k3")
            wide("d", "val")
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, d.val AS dval "
                "FROM a JOIN b ON a.k1 = b.id JOIN c ON b.k2 = c.id JOIN d ON c.k3 = d.id",
                schema_name=sn,
            )
            # The view must exist (unpruned it would be rejected at 74 > 65).
            client.resolve_table(sn, "v")

            padvals = ", ".join("0" for _ in range(16))
            client.execute_sql(f"INSERT INTO d VALUES (30, 42, {padvals})", schema_name=sn)  # d.id=30
            client.execute_sql(f"INSERT INTO c VALUES (20, 30, {padvals})", schema_name=sn)  # c.id=20, c.k3=30
            client.execute_sql(f"INSERT INTO b VALUES (10, 20, {padvals})", schema_name=sn)  # b.id=10, b.k2=20
            client.execute_sql(f"INSERT INTO a VALUES (1, 10, {padvals})", schema_name=sn)  # a.id=1, a.k1=10
            assert _rows(client, sn, "v", ["aid", "dval"]) == [(1, 42)]

            # Break the chain mid-way — the row drops.
            client.execute_sql("DELETE FROM c WHERE id = 20", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "dval"]) == []
            # Restore with a new d.val — re-derives.
            client.execute_sql(f"INSERT INTO c VALUES (20, 30, {padvals})", schema_name=sn)
            client.execute_sql("UPDATE d SET val = 99 WHERE id = 30", schema_name=sn)
            assert _rows(client, sn, "v", ["aid", "dval"]) == [(1, 99)]
        finally:
            _cleanup(client, sn)

    def test_left_step_midchain_nullfill_weight_exact(self, client):
        """A LEFT step mid-chain, pruned: two preserved rows that agree on every KEPT
        (non-dropped) column collapse to one bag-valued null-fill row of weight 2;
        retracting one leaves weight 1, both leaves it gone — the weight-exact
        positive_part null-fill survives pruning."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, junk BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY)", schema_name=sn)
            client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, cv BIGINT NOT NULL)", schema_name=sn)
            # a LEFT JOIN b (mid-chain) then JOIN c. `a.junk` is dead → pruned from h0,
            # so a1/a2 (same k, different junk) coincide in h0.
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.k AS ak, b.id AS bid, c.cv AS ccv "
                "FROM a LEFT JOIN b ON a.k = b.id JOIN c ON a.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10)", schema_name=sn)  # only k=10 matches b
            client.execute_sql("INSERT INTO c VALUES (5, 99), (10, 88)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO a VALUES (1, 5, 111), (2, 5, 222), (3, 10, 333)",
                schema_name=sn,
            )
            # a1,a2 (k=5): unmatched b -> bid NULL; c(5)=99. Same kept cols -> weight 2.
            # a3 (k=10): b(10) matched -> bid 10; c(10)=88.
            assert _wrows(client, sn, "v", ["ak", "bid", "ccv"]) == [
                ((5, None, 99), 2),
                ((10, 10, 88), 1),
            ]

            # Retract a1 -> the (5,None,99) bag drops to weight 1, still present.
            client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=sn)
            assert _wrows(client, sn, "v", ["ak", "bid", "ccv"]) == [
                ((5, None, 99), 1),
                ((10, 10, 88), 1),
            ]
            # Retract a2 too -> the null-fill bag is fully gone.
            client.execute_sql("DELETE FROM a WHERE id = 2", schema_name=sn)
            assert _wrows(client, sn, "v", ["ak", "bid", "ccv"]) == [((10, 10, 88), 1)]
        finally:
            _cleanup(client, sn)

    def test_full_step_midchain_pruned(self, client):
        """A FULL step mid-chain under pruning: both-side null-fills correct; the
        b-side null-fill carries a NULL join key that drops out of the next INNER join
        (as SQL requires), and a dead column is pruned."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, dead BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql("CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, bv BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql("CREATE TABLE c (id BIGINT NOT NULL PRIMARY KEY, cv BIGINT NOT NULL)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.k AS ak, b.bv AS bbv, c.cv AS ccv "
                "FROM a FULL JOIN b ON a.k = b.id JOIN c ON a.k = c.id",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO b VALUES (10, 500), (99, 600)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (10, 88), (5, 77)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 111), (2, 5, 222)", schema_name=sn)
            # a1 (k=10): b(10) matched bv=500; c(10)=88 -> (10,500,88).
            # a2 (k=5): unmatched b -> bbv NULL; c(5)=77 -> (5,None,77).
            # b(99) right-only null-fill has a NULL a-key -> no c match -> dropped.
            assert _rows_ns(client, sn, "v", ["ak", "bbv", "ccv"]) == [(5, None, 77), (10, 500, 88)]

            # Retract a1 -> its row drops; a2 unaffected.
            client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=sn)
            assert _rows_ns(client, sn, "v", ["ak", "bbv", "ccv"]) == [(5, None, 77)]
        finally:
            _cleanup(client, sn)

    def test_pruned_multiway_backfill_parity(self, client):
        """A pruned plain multiway view created over pre-populated tables (backfill)
        equals the fresh-insert run."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _tables(client, sn, ["a", "b", "c"])
            # Populate BEFORE the view exists.
            client.execute_sql("INSERT INTO b VALUES (10, 500, 0)", schema_name=sn)
            client.execute_sql("INSERT INTO c VALUES (500, 0, 99)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 10, 7), (2, 10, 8)", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, c.v AS cv "
                "FROM a JOIN b ON a.k = b.id JOIN c ON b.k = c.id",
                schema_name=sn,
            )
            # Both a rows chain a->b(10)->c(500,v99).
            assert _rows(client, sn, "v", ["aid", "cv"]) == [(1, 99), (2, 99)]
        finally:
            _cleanup(client, sn)

    def test_star_multiway_unpruned_identical(self, client):
        """`SELECT *` marks everything live: the multiway view keeps all real columns
        and the single final `_join_pk`, result-identical to pre-pruning. `SELECT
        DISTINCT *` over a join likewise keeps the wildcard H (no pruning)."""
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
            # The synthetic key is hidden: `SELECT *` exposes only the real columns.
            assert "_join_pk" not in rows[0]._asdict()
            # include_hidden surfaces the physical schema: one final `_join_pk`, no
            # accumulated intermediate PK (wildcard = no pruning).
            raw = [r for r in client.scan(vid, include_hidden=True) if r.weight > 0]
            raw_names = list(raw[0]._asdict().keys())
            assert raw_names.count("_join_pk") == 1, raw_names

            # Wildcard DISTINCT over a join keeps the wildcard H and dedups full rows.
            # Distinct column names so the DISTINCT output has no duplicate-name clash.
            client.execute_sql(
                "CREATE TABLE da (id BIGINT NOT NULL PRIMARY KEY, dfk BIGINT NOT NULL, dav BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE db (bid BIGINT NOT NULL PRIMARY KEY, dbv BIGINT NOT NULL)", schema_name=sn
            )
            client.execute_sql(
                "CREATE VIEW vd AS SELECT DISTINCT * FROM da JOIN db ON da.dfk = db.bid",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO db VALUES (5, 42)", schema_name=sn)
            # Two da rows both join db(5); their full rows differ (da.id) → two distinct rows.
            client.execute_sql("INSERT INTO da VALUES (1, 5, 7), (2, 5, 7)", schema_name=sn)
            vdid = client.resolve_table(sn, "vd")[0]
            drows = [r for r in client.scan(vdid) if r.weight > 0]
            assert len(drows) == 2, drows
        finally:
            _cleanup(client, sn)
