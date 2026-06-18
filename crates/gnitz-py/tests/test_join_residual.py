"""E2E tests: residual JOIN-ON predicates (a non-equi/non-range conjunct, or a
second range, evaluated as a linear post-join Filter) for INNER joins.

Run with multiple workers (the range path exchanges on the pair-PK; residual
correctness must hold across the fanout):
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_join_residual.py -v --tb=short
"""
import random

import pytest


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn, tables=None, views=None):
    for name in (views or []):
        try:
            client.execute_sql(f"DROP VIEW {name}", schema_name=sn)
        except Exception:
            pass
    for name in (tables or []):
        try:
            client.execute_sql(f"DROP TABLE {name}", schema_name=sn)
        except Exception:
            pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _rows(client, tid):
    """Current net (weight > 0) rows of a table/view, as dicts."""
    return [r._asdict() for r in client.scan(tid) if r.weight > 0]


def _view_pairs(client, vid):
    """The (aid, bid) multiset of a residual view as a set (INNER over unique-PK
    base tables emits each pair exactly once)."""
    return {(r["aid"], r["bid"]) for r in client.scan(vid) if r.weight > 0}


def _brute(a_rows, b_rows, pred):
    """Brute-force INNER join + residual: the (a.id, b.id) pairs where `pred`
    holds. The from-scratch oracle the incrementally-maintained view must equal."""
    return {
        (ar["id"], br["id"])
        for ar in a_rows
        for br in b_rows
        if pred(ar, br)
    }


class TestEquiResidual:
    def test_equijoin_inequality_incremental(self, client):
        """`a.k = b.k AND a.v <> b.v`: the equi rows minus those with a.v == b.v,
        maintained incrementally across inserts/deletes/updates that flip the
        residual outcome — verified each step against a from-scratch recompute."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                "FROM a JOIN b ON a.k = b.k AND a.v <> b.v",
                schema_name=sn,
            )
            a_tid = client.resolve_table(sn, "a")[0]
            b_tid = client.resolve_table(sn, "b")[0]
            vid = client.resolve_table(sn, "v")[0]

            def pred(ar, br):
                return ar["k"] == br["k"] and ar["v"] != br["v"]

            def check(ctx):
                exp = _brute(_rows(client, a_tid), _rows(client, b_tid), pred)
                got = _view_pairs(client, vid)
                assert got == exp, f"{ctx}: view {got} != recompute {exp}"

            client.execute_sql(
                "INSERT INTO b VALUES (10, 10, 5), (11, 10, 8), (12, 20, 9)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10, 5), (2, 10, 7), (3, 20, 9)",
                schema_name=sn,
            )
            # a1.v==b10.v (5) → excluded; a3.v==b12.v (9) → excluded.
            assert _view_pairs(client, vid) == {(1, 11), (2, 10), (2, 11)}
            check("seed")

            # Insert a row that matches and passes the residual.
            client.execute_sql("INSERT INTO a VALUES (4, 20, 1)", schema_name=sn)
            check("after insert a4 (k=20, v=1 <> 9)")

            # Update b11.v from 8 to 7 → a2(v=7) now equals b11 → (2,11) leaves.
            client.execute_sql("UPDATE b SET v = 7 WHERE id = 11", schema_name=sn)
            check("after update b11.v 8->7 (a2 now equal)")

            # Delete a2 → its surviving pair (2,10) leaves.
            client.execute_sql("DELETE FROM a WHERE id = 2", schema_name=sn)
            check("after delete a2")

            # Update a1.v 5->6 → a1 now differs from b10(5) → (1,10) enters.
            client.execute_sql("UPDATE a SET v = 6 WHERE id = 1", schema_name=sn)
            check("after update a1.v 5->6 (a1 now differs from b10)")
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    def test_equijoin_literal_residual(self, client):
        """`a.k = b.k AND a.region = 5`: a single-table column-vs-literal residual,
        evaluated post-join (not pushed into the a-side filter)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, region BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                "FROM a JOIN b ON a.k = b.k AND a.region = 5",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO b VALUES (10, 1), (11, 2)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO a VALUES (1, 1, 5), (2, 1, 6), (3, 2, 5)",
                schema_name=sn,
            )
            # a1 (region 5, k=1) × b10 (k=1) → in; a2 (region 6) excluded;
            # a3 (region 5, k=2) × b11 (k=2) → in.
            assert _view_pairs(client, vid) == {(1, 10), (3, 11)}
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])


class TestRangeResidual:
    def test_band_plus_second_range(self, client):
        """`a.k = b.k AND a.lo < b.hi AND a.x > b.y`: the eq pair + first range are
        the physical band; the second range is a residual. Compared against a
        brute-force INNER evaluation of all three conjuncts after a mixed workload."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                "lo BIGINT NOT NULL, x BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, "
                "hi BIGINT NOT NULL, y BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                "FROM a JOIN b ON a.k = b.k AND a.lo < b.hi AND a.x > b.y",
                schema_name=sn,
            )
            a_tid = client.resolve_table(sn, "a")[0]
            b_tid = client.resolve_table(sn, "b")[0]
            vid = client.resolve_table(sn, "v")[0]

            def pred(ar, br):
                return (ar["k"] == br["k"] and ar["lo"] < br["hi"] and ar["x"] > br["y"])

            def check(ctx):
                exp = _brute(_rows(client, a_tid), _rows(client, b_tid), pred)
                got = _view_pairs(client, vid)
                assert got == exp, f"{ctx}: view {got} != recompute {exp}"

            client.execute_sql(
                "INSERT INTO b VALUES (10, 1, 50, 5), (11, 1, 10, 5), (12, 2, 99, 0)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO a VALUES (1, 1, 20, 9), (2, 1, 5, 1), (3, 2, 1, 7)",
                schema_name=sn,
            )
            check("seed")

            client.execute_sql("INSERT INTO a VALUES (4, 1, 5, 100)", schema_name=sn)
            check("after insert a4")
            client.execute_sql("UPDATE a SET x = 0 WHERE id = 1", schema_name=sn)
            check("after update a1.x (fails x>y)")
            client.execute_sql("DELETE FROM b WHERE id = 12", schema_name=sn)
            check("after delete b12")
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    def test_pure_range_plus_residual(self, client):
        """`a.x < b.y AND a.r <> b.s`: pure range + inequality residual."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, x BIGINT NOT NULL, r BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, y BIGINT NOT NULL, s BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                "FROM a JOIN b ON a.x < b.y AND a.r <> b.s",
                schema_name=sn,
            )
            a_tid = client.resolve_table(sn, "a")[0]
            b_tid = client.resolve_table(sn, "b")[0]
            vid = client.resolve_table(sn, "v")[0]

            def pred(ar, br):
                return ar["x"] < br["y"] and ar["r"] != br["s"]

            def check(ctx):
                exp = _brute(_rows(client, a_tid), _rows(client, b_tid), pred)
                got = _view_pairs(client, vid)
                assert got == exp, f"{ctx}: view {got} != recompute {exp}"

            client.execute_sql(
                "INSERT INTO b VALUES (10, 100, 1), (11, 100, 9), (12, 5, 1)",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO a VALUES (1, 10, 1), (2, 10, 2), (3, 200, 3)",
                schema_name=sn,
            )
            check("seed")
            client.execute_sql("INSERT INTO a VALUES (4, 1, 7)", schema_name=sn)
            check("after insert a4")
            client.execute_sql("UPDATE b SET s = 2 WHERE id = 10", schema_name=sn)
            check("after update b10.s (a2.r now equals)")
            client.execute_sql("DELETE FROM a WHERE id = 1", schema_name=sn)
            check("after delete a1")
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])


class TestStringAndNull:
    def test_string_residual_content_comparison(self, client):
        """`a.k = b.k AND a.s <> b.s` (VARCHAR): comparison is by content, not by
        the German-string descriptor bytes. The pairs share a long common prefix
        and differ only in the out-of-line heap tail, so a descriptor-byte compare
        would wrongly equate them."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, s VARCHAR NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, s VARCHAR NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                "FROM a JOIN b ON a.k = b.k AND a.s <> b.s",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            # All strings share the 13-char prefix "commonprefix_" (out-of-line:
            # > 12 bytes), differing only in the heap tail.
            client.execute_sql(
                "INSERT INTO b VALUES (10, 1, 'commonprefix_BBBB'), (11, 1, 'commonprefix_SAME')",
                schema_name=sn,
            )
            client.execute_sql(
                "INSERT INTO a VALUES (1, 1, 'commonprefix_AAAA'), (2, 1, 'commonprefix_SAME')",
                schema_name=sn,
            )
            # a1(AAAA) vs b10(BBBB): differ → in; a1 vs b11(SAME): differ → in;
            # a2(SAME) vs b10(BBBB): differ → in; a2 vs b11(SAME): equal → out.
            assert _view_pairs(client, vid) == {(1, 10), (1, 11), (2, 10)}
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    def test_string_literal_residual(self, client):
        """`a.k = b.k AND a.s < 'mango'` (VARCHAR vs string literal): content
        comparison against a literal via the German-string opcodes (§6.6a)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, s VARCHAR NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                "FROM a JOIN b ON a.k = b.k AND a.s < 'mango'",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO b VALUES (10, 1)", schema_name=sn)
            client.execute_sql(
                "INSERT INTO a VALUES (1, 1, 'apple'), (2, 1, 'orange'), (3, 1, 'banana')",
                schema_name=sn,
            )
            # 'apple' < 'mango' and 'banana' < 'mango'; 'orange' is not.
            assert _view_pairs(client, vid) == {(1, 10), (3, 10)}
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    def test_null_3vl_residual(self, client):
        """A residual on a nullable column that is NULL excludes the pair (INNER
        3VL: a.v <> b.v is UNKNOWN → dropped); a later UPDATE making it non-NULL
        emits the row."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                "FROM a JOIN b ON a.k = b.k AND a.v <> b.v",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO b VALUES (10, 1, 5)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 1, NULL), (2, 1, 5)", schema_name=sn)
            # a1.v NULL → NULL <> 5 is UNKNOWN → excluded; a2.v == b10.v → excluded.
            assert _view_pairs(client, vid) == set()
            # Make a1.v non-NULL and != 5 → the pair (1, 10) enters.
            client.execute_sql("UPDATE a SET v = 9 WHERE id = 1", schema_name=sn)
            assert _view_pairs(client, vid) == {(1, 10)}
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    def test_is_null_residual_on_not_null_col_registers_empty(self, client):
        """`a.k = b.k AND a.x IS NULL` on a NOT NULL `a.x` folds to the constant
        false (§6.2 fold to LitInt(0)): the view registers and is always empty."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, x BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                "FROM a JOIN b ON a.k = b.k AND a.x IS NULL",
                schema_name=sn,
            )
            vid = client.resolve_table(sn, "v")[0]
            client.execute_sql("INSERT INTO b VALUES (10, 1)", schema_name=sn)
            client.execute_sql("INSERT INTO a VALUES (1, 1, 5)", schema_name=sn)
            # a.x is NOT NULL, so `a.x IS NULL` is constant false → empty result.
            assert _view_pairs(client, vid) == set()
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    def test_mixed_type_residual_rejected(self, client):
        """`a.k = b.k AND a.s <> b.n` (VARCHAR vs BIGINT) must fail at CREATE
        (§6.6b) — not silently miscompiled into a garbage integer load — and
        register no view."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, s VARCHAR NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, n BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(Exception):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                    "FROM a JOIN b ON a.k = b.k AND a.s <> b.n",
                    schema_name=sn,
                )
            with pytest.raises(Exception):
                client.resolve_table(sn, "v")
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])

    def test_left_join_residual_rejected(self, client):
        """LEFT JOIN + residual is rejected at CREATE (§6.3)."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE a (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
                schema_name=sn,
            )
            client.execute_sql(
                "CREATE TABLE b (id BIGINT NOT NULL PRIMARY KEY, k BIGINT NOT NULL, t BIGINT NOT NULL)",
                schema_name=sn,
            )
            with pytest.raises(Exception):
                client.execute_sql(
                    "CREATE VIEW v AS SELECT a.id AS aid, b.id AS bid "
                    "FROM a LEFT JOIN b ON a.k = b.k AND a.t <> b.t",
                    schema_name=sn,
                )
        finally:
            _cleanup(client, sn, tables=["a", "b"], views=["v"])
