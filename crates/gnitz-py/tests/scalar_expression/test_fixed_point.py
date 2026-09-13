"""The Dec register class: arithmetic that has to stay exact.

A DECIMAL is a scaled integer, so every operator here is integer arithmetic plus
a rule for what scale the result carries. The scale rule is the whole subject —
the arithmetic itself is the Int class's, already stated in `test_numeric.py`.
Each operator either keeps a scale, widens it (a product adds its operands'
scales), or leaves the class entirely (a division and a cast to DOUBLE produce a
float), and a result at the right value but the wrong scale renders identically
here while mis-keying a downstream join.

So the **declared type and scale of the computed column** is asserted beside its
value. A value alone cannot distinguish `Decimal("37.50")` from
`Decimal("37.50000")` — Python compares them equal — which is exactly the
distinction the catalog has to carry.

That DECIMAL is a type — its literals, its storage integer, its key behaviour —
is `value_domain/test_decimal.py`.
"""

from decimal import Decimal as D

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


def _declared(client, sn, name):
    """`{column name: (type code, scale)}` of relation `name`."""
    return {c.name: (c.type_code, c.scale) for c in client.resolve_table(sn, name)[1].columns}


# Each operator against its value on ids 1..5. An operator propagates a NULL
# operand; GREATEST instead skips it, as it does in PostgreSQL.
_OPERATORS = {
    # scale(price) + scale(qty) = 2 + 3.
    "price * qty": [D("37.50000"), D("0.35000"), D("0.00713"), None, D("2.02000")],
    "price * 1.1": [D("13.750"), D("0.110"), D("7.843"), D("-0.550"), D("1.111")],
    "price + 0.005": [D("12.505"), D("0.105"), D("7.135"), D("-0.495"), D("1.015")],
    # A division is not closed over the class.
    "price / 4": [3.125, 0.025, 1.7825, -0.125, 0.2525],
    "ROUND(price * qty, 2)": [D("37.50"), D("0.35"), D("0.01"), None, D("2.02")],
    # A cast to an integer rounds half away from zero: -0.50 becomes -1.
    "CAST(price AS BIGINT)": [13, 0, 7, -1, 1],
    "FLOOR(price)": [D(12), D(0), D(7), D(-1), D(1)],
    "CEIL(price)": [D(13), D(1), D(8), D(0), D(2)],
    "CAST(price AS DECIMAL(6, 1))": [D("12.5"), D("0.1"), D("7.1"), D("-0.5"), D("1.0")],
    "CAST(price AS DOUBLE)": [12.5, 0.1, 7.13, -0.5, 1.01],
    "-price": [D("-12.50"), D("-0.10"), D("-7.13"), D("0.50"), D("-1.01")],
    "price % 1": [D("0.50"), D("0.10"), D("0.13"), D("-0.50"), D("0.01")],
    "GREATEST(price, qty)": [D("12.500"), D("3.500"), D("7.130"), D("-0.500"), D("2.000")],
    "CASE WHEN price > 1 THEN price ELSE 0.5 END": [D("12.50"), D("0.50"), D("7.13"),
                                                    D("0.50"), D("1.01")],
}


def test_each_operator_carries_its_own_result_scale(client, priced):
    """One column per operator class, so the scale rule is one table — served
    alike by a maintained view and an ad-hoc read, and re-derived on the
    retraction an UPDATE makes. A predicate parses its literal at the column's
    scale, so `12.5` and `12.50` name one value while `12.501` names none."""
    sn = priced
    select = "SELECT id, " + ", ".join(
        f"{e} AS c{n}" for n, e in enumerate(_OPERATORS)) + " FROM t"
    client.execute_sql(f"CREATE VIEW v AS {select}", schema_name=sn)

    declared = _declared(client, sn, "v")
    assert [declared[f"c{list(_OPERATORS).index(e)}"] for e in
            ("price * qty", "price / 4", "CAST(price AS BIGINT)")] == [(DEC, 5), (F64, 0), (I64, 0)]

    expected = {r: 1 for r in zip([1, 2, 3, 4, 5], *_OPERATORS.values())}
    assert bag(scanned(client, sn, "v")) == expected
    assert bag(rows(client, sn, select)) == expected

    def ids(where):
        return bag(rows(client, sn, f"SELECT id FROM t WHERE {where}"))

    assert ids("price > 9.99") == {(1,): 1}
    assert ids("price = 12.5") == {(1,): 1}
    assert ids("price = 12.501") == {}
    assert ids("price IN (12.5, 1.01, 7)") == {(1,): 1, (5,): 1}
    assert ids("price < 1 AND qty >= 2") == {(2,): 1}
    assert ids("price * qty > 30") == {(1,): 1}
    assert ids("price BETWEEN 1 AND 8") == {(3,): 1, (5,): 1}

    client.execute_sql("UPDATE t SET price = 0.20, qty = 0.5 WHERE id = 2", schema_name=sn)
    after = bag(scanned(client, sn, "v"))
    assert after == bag(rows(client, sn, select))
    assert bag(scanned(client, sn, "v"), "id", "c0") == {
        (1, D("37.50000")): 1, (2, D("0.10000")): 1, (3, D("0.00713")): 1, (4, None): 1,
        (5, D("2.02000")): 1}


def test_an_aggregate_keeps_the_class_or_leaves_it_as_rows_cross_a_bound(client, priced):
    """SUM, MIN and MAX are closed over the class and keep the argument's scale;
    AVG divides and so lands in the float class. Updates then move rows over
    and back across a filtered view's bound — one rounding onto just past it —
    and a delete retracts a group's extremum, which is where a scale recomputed
    only on the insert path would drift."""
    sn = priced
    agg = ("SELECT cat, SUM(price) AS s, MIN(price) AS lo, MAX(qty) AS hi, AVG(price) AS a, "
           "COUNT(*) AS n, SUM(price * qty) AS tot FROM t GROUP BY cat")
    client.execute_sql(f"CREATE VIEW agg AS {agg}", schema_name=sn)
    client.execute_sql("CREATE VIEW cheap AS SELECT id, price FROM t WHERE price <= 1.005",
                       schema_name=sn)
    declared = _declared(client, sn, "agg")
    assert [declared[k] for k in ("s", "lo", "hi", "a", "tot")] == \
        [(DEC, 2), (DEC, 2), (DEC, 3), (F64, 0), (DEC, 5)]

    assert bag(scanned(client, sn, "agg")) == bag(rows(client, sn, agg)) == {
        (1, D("12.60"), D("0.10"), D("3.500"), 6.3, 2, D("37.85000")): 1,
        (2, D("6.63"), D("-0.50"), D("0.001"), 3.315, 2, D("0.00713")): 1,
        (3, D("1.01"), D("1.01"), D("2.000"), 1.01, 1, D("2.02000")): 1,
    }
    assert bag(scanned(client, sn, "cheap")) == {(2, D("0.10")): 1, (4, D("-0.50")): 1}

    # 1.005 rounds to 1.01 at scale 2, so it lands just above the bound.
    client.execute_sql("UPDATE t SET price = 1.005 WHERE id = 2", schema_name=sn)
    client.execute_sql("UPDATE t SET qty = price WHERE id = 3", schema_name=sn)
    client.execute_sql("UPDATE t SET price = 3 WHERE id = 4", schema_name=sn)
    client.execute_sql("UPDATE t SET price = 0.5 WHERE id = 5", schema_name=sn)
    client.execute_sql("DELETE FROM t WHERE id = 1", schema_name=sn)

    assert bag(scanned(client, sn, "t")) == {
        (2, 1, D("1.01"), D("3.500")): 1,
        (3, 2, D("7.13"), D("7.130")): 1,
        (4, 2, D("3.00"), None): 1,
        (5, 3, D("0.50"), D("2.000")): 1,
    }
    assert bag(scanned(client, sn, "cheap")) == {(5, D("0.50")): 1}
    assert bag(scanned(client, sn, "agg")) == bag(rows(client, sn, agg)) == {
        (1, D("1.01"), D("1.01"), D("3.500"), 1.01, 1, D("3.53500")): 1,
        (2, D("10.13"), D("3.00"), D("7.130"), 5.065, 2, D("50.83690")): 1,
        (3, D("0.50"), D("0.50"), D("2.000"), 0.5, 1, D("1.00000")): 1,
    }
