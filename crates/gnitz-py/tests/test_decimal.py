"""E2E tests: DECIMAL(p, s) columns — fixed-point values stored as a scaled I64.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_decimal.py -v --tb=short
"""
from decimal import Decimal

import pytest
import gnitz
from _uid import uid as _uid


def _cleanup(client, sn, *names):
    for name in names:
        for kind in ("VIEW", "TABLE"):
            try:
                client.execute_sql(f"DROP {kind} {name}", schema_name=sn)
            except Exception:
                pass
    try:
        client.drop_schema(sn)
    except Exception:
        pass


def _rows(client, sn, q):
    res = client.execute_sql(q, schema_name=sn)[0]
    assert res["type"] == "Rows", res
    return list(res["rows"])


def _view(client, sn, name):
    return client.scan(client.resolve_table(sn, name)[0]).mappings()


def _seed(client, sn):
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL, "
        "price DECIMAL(10, 2) NOT NULL, qty NUMERIC(8, 3))",
        schema_name=sn,
    )
    client.execute_sql(
        "INSERT INTO t VALUES "
        "(1, 1, 12.50, 3), "
        "(2, 1, 0.10, 3.5), "
        "(3, 2, '7.125', '0.001'), "
        "(4, 2, -0.5, NULL), "
        "(5, 3, 1.005, 2)",
        schema_name=sn,
    )


class TestDecimalColumns:
    def test_ddl_literals_and_rendering(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _seed(client, sn)
            tid, schema = client.resolve_table(sn, "t")
            assert schema.columns[2].type_code == gnitz.TypeCode.DECIMAL
            assert schema.columns[2].scale == 2
            assert schema.columns[3].scale == 3
            assert schema.columns[1].scale == 0
            rows = {r["id"]: (r["price"], r["qty"]) for r in client.scan(tid).mappings()}
            assert rows == {
                1: (Decimal("12.50"), Decimal("3.000")),
                2: (Decimal("0.10"), Decimal("3.500")),
                # A string literal spells a DECIMAL; a longer fraction rounds
                # half away from zero, as 1.005 does below.
                3: (Decimal("7.13"), Decimal("0.001")),
                4: (Decimal("-0.50"), None),
                5: (Decimal("1.01"), Decimal("2.000")),
            }
            # `Decimal("12.50") == 12.5` is True, so the comparison above would
            # pass on a float too: the rendered type is its own assertion.
            assert isinstance(rows[1][0], Decimal) and isinstance(rows[1][1], Decimal)
            with pytest.raises(Exception, match="invalid DECIMAL literal"):
                client.execute_sql("INSERT INTO t VALUES (9, 1, 'abc', 1)", schema_name=sn)
            with pytest.raises(Exception, match="out of range"):
                client.execute_sql("INSERT INTO t VALUES (9, 1, 99999999999999999999, 1)", schema_name=sn)
        finally:
            _cleanup(client, sn, "t")

    def test_append_accepts_decimal_int_float_and_str(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            client.execute_sql(
                "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, v DECIMAL(9, 3) NOT NULL)",
                schema_name=sn,
            )
            tid, schema = client.resolve_table(sn, "t")
            batch = gnitz.ZSetBatch(schema)
            batch.append(id=1, v=Decimal("12.5"))
            batch.append(id=2, v=3)
            batch.append(id=3, v=1.1)
            batch.append(id=4, v="2.2505")
            batch.append(id=5, v=Decimal("1E+2"))
            client.push(tid, batch)
            rows = {r["id"]: r["v"] for r in client.scan(tid).mappings()}
            assert rows == {
                1: Decimal("12.500"),
                2: Decimal("3.000"),
                3: Decimal("1.100"),
                4: Decimal("2.251"),
                5: Decimal("100.000"),
            }
            with pytest.raises(ValueError):
                gnitz.ZSetBatch(schema).append(id=9, v="abc")
            with pytest.raises(OverflowError):
                gnitz.ZSetBatch(schema).append(id=9, v=10**16)
            # A client-authored schema carries the scale too.
            cols = [
                gnitz.ColumnDef("id", gnitz.TypeCode.I64, primary_key=True),
                gnitz.ColumnDef("v", gnitz.TypeCode.DECIMAL, scale=3),
            ]
            assert gnitz.Schema(cols).columns[1].scale == 3
        finally:
            _cleanup(client, sn, "t")

    def test_arithmetic_view_is_exact_and_maintained(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _seed(client, sn)
            client.execute_sql(
                "CREATE VIEW v AS SELECT id, price * qty AS total, price * 1.1 AS bumped, "
                "price + 0.005 AS p3, price / 4 AS quarter, ROUND(price * qty, 2) AS r2, "
                "CAST(price AS BIGINT) AS whole, FLOOR(price) AS fl, CEIL(price) AS ce, "
                "CAST(price AS DECIMAL(6, 1)) AS p1, CAST(price AS DOUBLE) AS pf, -price AS neg, "
                "price % 1 AS cents, GREATEST(price, qty) AS g, "
                "CASE WHEN price > 1 THEN price ELSE 0.5 END AS c "
                "FROM t",
                schema_name=sn,
            )
            vid, vs = client.resolve_table(sn, "v")
            # A computed column's scale reaches the catalog and comes back on
            # the wire. Which scale each operator produces is the planner's own
            # rule, asserted per node in `decimal_arithmetic_and_blend_typing`;
            # here one column of each declared class is what a server can add.
            scales = {c.name: (c.type_code, c.scale) for c in vs.columns}
            D, F, I = gnitz.TypeCode.DECIMAL, gnitz.TypeCode.F64, gnitz.TypeCode.I64
            assert scales["total"] == (D, 5)
            assert scales["quarter"] == (F, 0)
            assert scales["whole"] == (I, 0)
            rows = {r["id"]: r for r in client.scan(vid).mappings()}
            r1, r2, r3, r4 = rows[1], rows[2], rows[3], rows[4]
            assert r1["total"] == Decimal("37.50000") and r2["total"] == Decimal("0.35000")
            assert r1["bumped"] == Decimal("13.750") and r2["bumped"] == Decimal("0.110")
            assert r2["p3"] == Decimal("0.105") and r4["p3"] == Decimal("-0.495")
            assert r1["quarter"] == 3.125 and r4["quarter"] == -0.125
            assert r1["r2"] == Decimal("37.50") and r3["r2"] == Decimal("0.01")
            assert (r1["whole"], r3["whole"], r4["whole"]) == (13, 7, -1)
            assert (r1["fl"], r4["fl"], r1["ce"], r4["ce"]) == (Decimal(12), Decimal(-1), Decimal(13), Decimal(0))
            assert r1["p1"] == Decimal("12.5") and r3["p1"] == Decimal("7.1")
            assert r1["pf"] == 12.5 and r1["neg"] == Decimal("-12.50")
            assert r1["cents"] == Decimal("0.50") and r3["cents"] == Decimal("0.13")
            assert r1["g"] == Decimal("12.500") and r2["g"] == Decimal("3.500")
            assert r1["c"] == Decimal("12.50") and r2["c"] == Decimal("0.50")
            # GREATEST skips a NULL argument, as it does in PostgreSQL.
            assert r4["total"] is None and r4["r2"] is None and r4["g"] == Decimal("-0.500")
            # Maintained: an UPDATE retracts the old row and inserts the new.
            client.execute_sql("UPDATE t SET price = 0.20, qty = 0.5 WHERE id = 2", schema_name=sn)
            rows = {r["id"]: r for r in client.scan(vid).mappings()}
            assert rows[2]["total"] == Decimal("0.10000") and rows[2]["bumped"] == Decimal("0.220")
            assert len(rows) == 5
        finally:
            _cleanup(client, sn, "v", "t")

    def test_aggregates_keep_the_scale(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _seed(client, sn)
            client.execute_sql(
                "CREATE VIEW agg AS SELECT cat, SUM(price) AS s, MIN(price) AS lo, MAX(qty) AS hi, "
                "AVG(price) AS a, COUNT(*) AS n, SUM(price * qty) AS tot FROM t GROUP BY cat",
                schema_name=sn,
            )
            vid, vs = client.resolve_table(sn, "agg")
            scales = {c.name: (c.type_code, c.scale) for c in vs.columns}
            D, F = gnitz.TypeCode.DECIMAL, gnitz.TypeCode.F64
            assert scales["s"] == (D, 2) and scales["lo"] == (D, 2) and scales["hi"] == (D, 3)
            assert scales["a"] == (F, 0) and scales["tot"] == (D, 5)
            rows = {r["cat"]: r for r in client.scan(vid).mappings()}
            assert rows[1]["s"] == Decimal("12.60") and rows[1]["lo"] == Decimal("0.10")
            assert rows[1]["hi"] == Decimal("3.500") and rows[1]["a"] == 6.3 and rows[1]["n"] == 2
            assert rows[1]["tot"] == Decimal("37.85000")
            assert rows[2]["s"] == Decimal("6.63") and rows[2]["lo"] == Decimal("-0.50")
            assert rows[2]["tot"] == Decimal("0.00713")
            client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
            rows = {r["cat"]: r for r in client.scan(vid).mappings()}
            assert rows[1]["s"] == Decimal("0.10") and rows[1]["n"] == 1 and rows[1]["a"] == 0.1
            # GROUP BY a DECIMAL column: the key keeps its scale.
            client.execute_sql(
                "CREATE VIEW byp AS SELECT price, COUNT(*) AS n FROM t GROUP BY price",
                schema_name=sn,
            )
            bid, bs = client.resolve_table(sn, "byp")
            key = next(c for c in bs.columns if c.name == "price")
            assert (key.type_code, key.scale) == (D, 2)
            assert {r["price"]: r["n"] for r in client.scan(bid).mappings()} == {
                Decimal("0.10"): 1, Decimal("7.13"): 1, Decimal("-0.50"): 1, Decimal("1.01"): 1,
            }
            got = _rows(client, sn, "SELECT SUM(price) AS s, AVG(qty) AS a FROM t WHERE cat = 2")
            assert (got[0].s, got[0].a) == (Decimal("6.63"), 0.001)
        finally:
            _cleanup(client, sn, "byp", "agg", "t")

    def test_filters_keys_and_set(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _seed(client, sn)
            q = lambda sql: sorted(r.id for r in _rows(client, sn, sql))
            assert q("SELECT id FROM t WHERE price > 9.99") == [1]
            assert q("SELECT id FROM t WHERE price = 12.5") == [1]
            assert q("SELECT id FROM t WHERE price = 12.501") == []
            assert q("SELECT id FROM t WHERE price IN (12.5, 1.01, 7)") == [1, 5]
            assert q("SELECT id FROM t WHERE price < 1 AND qty >= 2") == [2]
            assert q("SELECT id FROM t WHERE price * qty > 30") == [1]
            assert q("SELECT id FROM t WHERE price BETWEEN 1 AND 8") == [3, 5]
            client.execute_sql("CREATE VIEW cheap AS SELECT id, price FROM t WHERE price <= 1.005", schema_name=sn)
            assert sorted(r["id"] for r in _view(client, sn, "cheap")) == [2, 4]
            client.execute_sql("UPDATE t SET price = 1.005 WHERE id = 2", schema_name=sn)
            client.execute_sql("UPDATE t SET price = price * 2 WHERE id = 1", schema_name=sn)
            client.execute_sql("UPDATE t SET qty = price WHERE id = 3", schema_name=sn)
            client.execute_sql("UPDATE t SET price = 3 WHERE id = 4", schema_name=sn)
            tid, _ = client.resolve_table(sn, "t")
            rows = {r["id"]: (r["price"], r["qty"]) for r in client.scan(tid).mappings()}
            assert rows[2][0] == Decimal("1.01") and rows[1][0] == Decimal("25.00")
            assert rows[3][1] == Decimal("7.130") and rows[4][0] == Decimal("3.00")
            assert sorted(r["id"] for r in _view(client, sn, "cheap")) == []

            # A DECIMAL primary key: routed, seeked and ranged on the stored integer.
            client.execute_sql(
                "CREATE TABLE p (amt DECIMAL(6, 2) NOT NULL PRIMARY KEY, v BIGINT NOT NULL)",
                schema_name=sn,
            )
            pid, ps = client.resolve_table(sn, "p")
            assert ps.columns[0].scale == 2
            client.execute_sql(
                "INSERT INTO p VALUES " + ", ".join(f"({i / 4}, {i})" for i in range(12)),
                schema_name=sn,
            )
            got = _rows(client, sn, "SELECT amt, v FROM p WHERE amt = 1.25")
            assert [(r.amt, r.v) for r in got] == [(Decimal("1.25"), 5)]
            assert _rows(client, sn, "SELECT v FROM p WHERE amt = 1.255") == []
            assert sorted(r.v for r in _rows(client, sn, "SELECT v FROM p WHERE amt >= 2 AND amt < 2.6")) == [8, 9, 10]
            assert sorted(r.v for r in _rows(client, sn, "SELECT v FROM p WHERE amt IN (0.25, 0.5, 9)")) == [1, 2]
            client.execute_sql("DELETE FROM p WHERE amt = 0.75", schema_name=sn)
            client.execute_sql("UPDATE p SET v = v + 100 WHERE amt > 2.5", schema_name=sn)
            assert {r["amt"]: r["v"] for r in client.scan(pid).mappings()} == {
                Decimal(f"{i / 4:.2f}"): i + (100 if i > 10 else 0) for i in range(12) if i != 3
            }
            with pytest.raises(Exception, match="not a valid DECIMAL"):
                client.execute_sql("INSERT INTO p VALUES ('x', 1)", schema_name=sn)
        finally:
            _cleanup(client, sn, "cheap", "p", "t")

    def test_join_on_a_decimal_key(self, client):
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _seed(client, sn)
            client.execute_sql(
                "CREATE TABLE tier (id BIGINT NOT NULL PRIMARY KEY, price DECIMAL(10, 2) NOT NULL, label VARCHAR)",
                schema_name=sn,
            )
            client.execute_sql("INSERT INTO tier VALUES (1, 12.5, 'big'), (2, 0.1, 'tiny')", schema_name=sn)
            client.execute_sql(
                "CREATE VIEW j AS SELECT t.id, tier.label, t.price * 2 AS dbl FROM t JOIN tier ON t.price = tier.price",
                schema_name=sn,
            )
            rows = {r["id"]: (r["label"], r["dbl"]) for r in _view(client, sn, "j")}
            assert rows == {1: ("big", Decimal("25.00")), 2: ("tiny", Decimal("0.20"))}
        finally:
            _cleanup(client, sn, "j", "tier", "t")

    @pytest.mark.parametrize(
        "stmt, needle",
        [
            ("CREATE VIEW v AS SELECT id, CAST('abc' AS DECIMAL(5, 2)) AS x FROM t", "invalid DECIMAL literal"),
            ("CREATE VIEW v AS SELECT id, qty * qty * qty * qty * qty * qty * qty AS x FROM t", "scale"),
            ("CREATE VIEW v AS SELECT id, price + qty * qty * qty * qty * qty * qty * qty AS x FROM t", "scale"),
            ("CREATE VIEW v AS SELECT price FROM t UNION SELECT qty FROM t", "type mismatch"),
            ("CREATE VIEW v AS SELECT a.id FROM t a JOIN t2 ON a.price = t2.qty", "same scale"),
            ("CREATE VIEW v AS SELECT a.id FROM t a JOIN t2 ON a.price = t2.id", "same scale"),
        ],
    )
    def test_rejections(self, client, stmt, needle):
        """A DDL-shape rejection (a bad precision or scale) is unit-tested in
        `decimal_up_to_18_digits_is_fixed_point`; these are the ones that need a
        catalog to reject against."""
        sn = "s" + _uid()
        client.create_schema(sn)
        try:
            _seed(client, sn)
            if "JOIN t2" in stmt:
                client.execute_sql(
                    "CREATE TABLE t2 (id BIGINT NOT NULL PRIMARY KEY, qty NUMERIC(8, 3) NOT NULL)",
                    schema_name=sn,
                )
            with pytest.raises(Exception, match=needle):
                client.execute_sql(stmt, schema_name=sn)
        finally:
            _cleanup(client, sn, "v", "t2", "t")
