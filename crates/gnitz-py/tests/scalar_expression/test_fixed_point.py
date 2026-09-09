"""The Dec register class: arithmetic that has to stay exact.

A DECIMAL is a scaled integer, so every operator here is integer arithmetic plus
a rule for what scale the result carries. The scale rule is the whole subject —
the arithmetic itself is the Int class's, already stated in `test_numeric.py`.
Each operator either keeps a scale, widens it (a product adds its operands'
scales), or leaves the class entirely (a division and a cast to DOUBLE produce a
float), and a result at the right value but the wrong scale renders identically
here while mis-keying a downstream join.

So every case asserts the **declared type and scale of the computed column**
beside its value. A value alone cannot distinguish `Decimal("37.50")` from
`Decimal("37.50000")` — Python compares them equal — which is exactly the
distinction the catalog has to carry.

That DECIMAL is a type — its literals, its storage integer, its key behaviour —
is `value_domain/test_decimal.py`.
"""

from decimal import Decimal

import pytest
import gnitz
from _read import bag, rows, scanned

DEC, F64, I64 = gnitz.TypeCode.DECIMAL, gnitz.TypeCode.F64, gnitz.TypeCode.I64


@pytest.fixture
def priced(client, schema_name):
    """`t(id, cat, price DECIMAL(10,2), qty NUMERIC(8,3))` — two scales, a
    negative, a NULL and a value whose product needs five decimal places."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, cat BIGINT NOT NULL, "
        "price DECIMAL(10, 2) NOT NULL, qty NUMERIC(8, 3))", schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES (1, 1, 12.50, 3), (2, 1, 0.10, 3.5), "
        "(3, 2, '7.125', '0.001'), (4, 2, -0.5, NULL), (5, 3, 1.005, 2)",
        schema_name=schema_name)
    return schema_name


def test_each_operator_carries_its_own_result_scale(client, priced):
    """One column per operator class, so the scale rule is one table. A product
    adds scales, a sum keeps the wider, a division and a float cast leave the
    class, and an integer cast truncates toward zero rather than rounding."""
    sn = priced
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, price * qty AS total, price * 1.1 AS bumped, "
        "price + 0.005 AS p3, price / 4 AS quarter, ROUND(price * qty, 2) AS r2, "
        "CAST(price AS BIGINT) AS whole, FLOOR(price) AS fl, CEIL(price) AS ce, "
        "CAST(price AS DECIMAL(6, 1)) AS p1, CAST(price AS DOUBLE) AS pf, "
        "-price AS neg, price % 1 AS cents, GREATEST(price, qty) AS g, "
        "CASE WHEN price > 1 THEN price ELSE 0.5 END AS c FROM t", schema_name=sn)

    _, vs = client.resolve_table(sn, "v")
    scales = {c.name: (c.type_code, c.scale) for c in vs.columns}
    # scale(price) + scale(qty) = 2 + 3; a division is not closed over the class.
    assert scales["total"] == (DEC, 5)
    assert scales["quarter"] == (F64, 0)
    assert scales["whole"] == (I64, 0)

    r = {x["id"]: x for x in scanned(client, sn, "v")}
    r1, r2, r3, r4 = r[1], r[2], r[3], r[4]
    assert r1["total"] == Decimal("37.50000") and r2["total"] == Decimal("0.35000")
    assert r1["bumped"] == Decimal("13.750") and r2["bumped"] == Decimal("0.110")
    assert r2["p3"] == Decimal("0.105") and r4["p3"] == Decimal("-0.495")
    assert r1["quarter"] == 3.125 and r4["quarter"] == -0.125
    assert r1["r2"] == Decimal("37.50") and r3["r2"] == Decimal("0.01")
    # A cast to an integer truncates toward zero: -0.50 becomes -1, not 0.
    assert (r1["whole"], r3["whole"], r4["whole"]) == (13, 7, -1)
    assert (r1["fl"], r4["fl"], r1["ce"], r4["ce"]) == \
        (Decimal(12), Decimal(-1), Decimal(13), Decimal(0))
    assert r1["p1"] == Decimal("12.5") and r3["p1"] == Decimal("7.1")
    assert r1["pf"] == 12.5 and r1["neg"] == Decimal("-12.50")
    assert r1["cents"] == Decimal("0.50") and r3["cents"] == Decimal("0.13")
    assert r1["g"] == Decimal("12.500") and r2["g"] == Decimal("3.500")
    assert r1["c"] == Decimal("12.50") and r2["c"] == Decimal("0.50")
    # An operator propagates a NULL operand; GREATEST instead skips it, as it
    # does in PostgreSQL, so a row with one NULL argument still has a result.
    assert r4["total"] is None and r4["r2"] is None and r4["g"] == Decimal("-0.500")


def test_a_computed_decimal_is_maintained_across_a_retraction(client, priced):
    """An UPDATE retracts the old computed row and inserts the new one, so the
    expression runs again on the retraction. A computed column recomputed only on
    the insert leaves the old row behind — visible here as a second row for the
    same id, which is why the whole bag is asserted rather than one lookup."""
    sn = priced
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, price * qty AS total FROM t", schema_name=sn)
    client.execute_sql("UPDATE t SET price = 0.20, qty = 0.5 WHERE id = 2",
                       schema_name=sn)

    assert bag(scanned(client, sn, "v"), "id", "total") == {
        (1, Decimal("37.50000")): 1,
        (2, Decimal("0.10000")): 1,
        (3, Decimal("0.00713")): 1,
        (4, None): 1,
        (5, Decimal("2.02000")): 1,
    }


def test_an_aggregate_over_a_decimal_keeps_the_class_or_leaves_it(client, priced):
    """SUM, MIN and MAX are closed over the class and keep the argument's scale;
    AVG divides and so lands in the float class. The grouped view is then
    maintained across a retraction, which is where a scale recomputed only on
    the insert path would drift."""
    sn = priced
    client.execute_sql(
        "CREATE VIEW agg AS SELECT cat, SUM(price) AS s, MIN(price) AS lo, "
        "MAX(qty) AS hi, AVG(price) AS a, COUNT(*) AS n, SUM(price * qty) AS tot "
        "FROM t GROUP BY cat", schema_name=sn)
    _, vs = client.resolve_table(sn, "agg")
    scales = {c.name: (c.type_code, c.scale) for c in vs.columns}
    assert (scales["s"], scales["lo"], scales["hi"]) == ((DEC, 2), (DEC, 2), (DEC, 3))
    assert (scales["a"], scales["tot"]) == ((F64, 0), (DEC, 5))

    r = {x["cat"]: x for x in scanned(client, sn, "agg")}
    assert (r[1]["s"], r[1]["lo"], r[1]["hi"], r[1]["a"], r[1]["n"]) == \
        (Decimal("12.60"), Decimal("0.10"), Decimal("3.500"), 6.3, 2)
    assert r[1]["tot"] == Decimal("37.85000")
    assert (r[2]["s"], r[2]["lo"], r[2]["tot"]) == \
        (Decimal("6.63"), Decimal("-0.50"), Decimal("0.00713"))

    client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)
    r = {x["cat"]: x for x in scanned(client, sn, "agg")}
    assert (r[1]["s"], r[1]["n"], r[1]["a"]) == (Decimal("0.10"), 1, 0.1)

    assert bag(rows(client, sn,
                    "SELECT SUM(price) AS s, AVG(qty) AS a FROM t WHERE cat = 2")) == \
        {(Decimal("6.63"), 0.001): 1}


def test_a_decimal_predicate_compares_at_the_columns_scale(client, priced):
    """A literal is parsed at the column's scale before the compare, so `12.5`
    and `12.50` name one value while `12.501` names none. A predicate that
    compared the literal at its own scale would answer all three the same."""
    sn = priced

    def ids(where):
        return bag(rows(client, sn, f"SELECT id FROM t WHERE {where}"))

    assert ids("price > 9.99") == {(1,): 1}
    assert ids("price = 12.5") == {(1,): 1}
    assert ids("price = 12.501") == {}
    assert ids("price IN (12.5, 1.01, 7)") == {(1,): 1, (5,): 1}
    assert ids("price < 1 AND qty >= 2") == {(2,): 1}
    assert ids("price * qty > 30") == {(1,): 1}
    assert ids("price BETWEEN 1 AND 8") == {(3,): 1, (5,): 1}


def test_a_filtered_decimal_view_follows_its_rows_across_the_threshold(client, priced):
    """Updates move rows over and back across the view's bound, including one
    that rounds onto the bound exactly. Each crossing must retract or admit
    exactly once."""
    sn = priced
    client.execute_sql(
        "CREATE VIEW cheap AS SELECT id, price FROM t WHERE price <= 1.005",
        schema_name=sn)
    assert bag(scanned(client, sn, "cheap"), "id") == {(2,): 1, (4,): 1}

    # 1.005 rounds to 1.01 at scale 2, so it lands just above the bound.
    client.execute_sql("UPDATE t SET price = 1.005 WHERE id = 2", schema_name=sn)
    client.execute_sql("UPDATE t SET price = price * 2 WHERE id = 1", schema_name=sn)
    client.execute_sql("UPDATE t SET qty = price WHERE id = 3", schema_name=sn)
    client.execute_sql("UPDATE t SET price = 3 WHERE id = 4", schema_name=sn)

    assert bag(scanned(client, sn, "cheap"), "id") == {}
    assert bag(scanned(client, sn, "t"), "id", "price", "qty") == {
        (1, Decimal("25.00"), Decimal("3.000")): 1,
        (2, Decimal("1.01"), Decimal("3.500")): 1,
        (3, Decimal("7.13"), Decimal("7.130")): 1,
        (4, Decimal("3.00"), None): 1,
        (5, Decimal("1.01"), Decimal("2.000")): 1,
    }


@pytest.mark.parametrize("body,message", [
    ("SELECT id, CAST('abc' AS DECIMAL(5, 2)) AS x FROM t", "invalid DECIMAL literal"),
    # Each product adds its operands' scales, so a chain of them runs past what
    # the scaled integer can hold — refused at compile time, not at the first row.
    ("SELECT id, qty * qty * qty * qty * qty * qty * qty AS x FROM t", "scale"),
    ("SELECT id, price + qty * qty * qty * qty * qty * qty * qty AS x FROM t", "scale"),
], ids=["bad-literal", "scale-overflow", "scale-overflow-in-a-sum"])
def test_an_expression_with_no_representable_scale_is_refused(
        client, priced, body, message):
    """The result scale is computed from the operand scales at plan time, so an
    expression whose scale cannot exist is rejected before any row is read."""
    with pytest.raises(gnitz.GnitzError, match=message):
        client.execute_sql(f"CREATE VIEW v AS {body}", schema_name=priced)
