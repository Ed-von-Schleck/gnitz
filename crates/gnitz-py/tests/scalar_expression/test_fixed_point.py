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

import gnitz
from _exprs import declared, projected
from _read import bag, rows, scanned

TC = gnitz.TypeCode
F64, I64, BOOL = (TC.F64, 0), (TC.I64, 0), (TC.BOOLEAN, 0)


def DEC(scale):
    return (TC.DECIMAL, scale)


# Each operator against its value on ids 1..5. An operator propagates a NULL
# operand; GREATEST instead skips it, as it does in PostgreSQL.
_OPERATORS = {
    # scale(price) + scale(qty) = 2 + 3.
    "price * qty": (DEC(5), [D("37.50000"), D("0.35000"), D("0.00713"), None, D("2.02000")]),
    "price * 1.1": (DEC(3), [D("13.750"), D("0.110"), D("7.843"), D("-0.550"), D("1.111")]),
    "price + 0.005": (DEC(3), [D("12.505"), D("0.105"), D("7.135"), D("-0.495"), D("1.015")]),
    # A division is not closed over the class.
    "price / 4": (F64, [3.125, 0.025, 1.7825, -0.125, 0.2525]),
    "ROUND(price * qty, 2)": (DEC(2), [D("37.50"), D("0.35"), D("0.01"), None, D("2.02")]),
    # A cast to an integer rounds half away from zero: -0.50 becomes -1.
    "CAST(price AS BIGINT)": (I64, [13, 0, 7, -1, 1]),
    "FLOOR(price)": (DEC(0), [D(12), D(0), D(7), D(-1), D(1)]),
    "CEIL(price)": (DEC(0), [D(13), D(1), D(8), D(0), D(2)]),
    "CAST(price AS DECIMAL(6, 1))": (DEC(1), [D("12.5"), D("0.1"), D("7.1"), D("-0.5"),
                                              D("1.0")]),
    "CAST(price AS DOUBLE)": (F64, [12.5, 0.1, 7.13, -0.5, 1.01]),
    "-price": (DEC(2), [D("-12.50"), D("-0.10"), D("-7.13"), D("0.50"), D("-1.01")]),
    "price % 1": (DEC(2), [D("0.50"), D("0.10"), D("0.13"), D("-0.50"), D("0.01")]),
    "GREATEST(price, qty)": (DEC(3), [D("12.500"), D("3.500"), D("7.130"), D("-0.500"),
                                      D("2.000")]),
    "CASE WHEN price > 1 THEN price ELSE 0.5 END": (DEC(2), [D("12.50"), D("0.50"), D("7.13"),
                                                             D("0.50"), D("1.01")]),
    # A comparison against a computed scale, or a literal list and bounds each
    # parsed at the column's scale.
    "price IN (12.5, 1.01, 7)": (BOOL, [True, False, False, False, True]),
    "price < 1 AND qty >= 2": (BOOL, [False, True, False, None, False]),
    "price * qty > 30": (BOOL, [True, False, False, None, False]),
    "price BETWEEN 1 AND 8": (BOOL, [False, False, True, False, True]),
}


def test_each_operator_carries_its_own_result_scale(client, priced):
    projected(client, priced, _OPERATORS)


def test_an_aggregate_keeps_the_class_or_leaves_it_as_rows_cross_a_bound(client, priced):
    """SUM, MIN and MAX are closed over the class and keep the argument's scale;
    AVG divides and so lands in the float class. Updates then move rows over
    and back across a filtered view's bound — one rounding onto just past it —
    and a delete retracts a group's extremum, which is where a scale recomputed
    only on the insert path would drift."""
    agg = ("SELECT cat, SUM(price) AS s, MIN(price) AS lo, MAX(qty) AS hi, AVG(price) AS a, "
           "COUNT(*) AS n, SUM(price * qty) AS tot FROM t GROUP BY cat")
    client.execute_sql(f"CREATE VIEW agg AS {agg}")
    client.execute_sql("CREATE VIEW cheap AS SELECT id, price FROM t WHERE price <= 1.005")
    types = declared(client, "agg")
    assert [types[k] for k in ("s", "lo", "hi", "a", "tot")] == \
        [DEC(2), DEC(2), DEC(3), F64, DEC(5)]
    client.execute_sql(priced)

    assert bag(scanned(client, "agg")) == bag(rows(client, agg)) == {
        (1, D("12.60"), D("0.10"), D("3.500"), 6.3, 2, D("37.85000")): 1,
        (2, D("6.63"), D("-0.50"), D("0.001"), 3.315, 2, D("0.00713")): 1,
        (3, D("1.01"), D("1.01"), D("2.000"), 1.01, 1, D("2.02000")): 1,
    }
    assert bag(scanned(client, "cheap")) == {(2, D("0.10")): 1, (4, D("-0.50")): 1}

    # 1.005 rounds to 1.01 at scale 2, so it lands just above the bound.
    client.execute_sql("UPDATE t SET price = 1.005 WHERE id = 2")
    client.execute_sql("UPDATE t SET qty = price WHERE id = 3")
    client.execute_sql("UPDATE t SET price = 3 WHERE id = 4")
    client.execute_sql("UPDATE t SET price = 0.5 WHERE id = 5")
    client.execute_sql("DELETE FROM t WHERE id = 1")

    assert bag(scanned(client, "t")) == {
        (2, 1, D("1.01"), D("3.500")): 1,
        (3, 2, D("7.13"), D("7.130")): 1,
        (4, 2, D("3.00"), None): 1,
        (5, 3, D("0.50"), D("2.000")): 1,
    }
    assert bag(scanned(client, "cheap")) == {(5, D("0.50")): 1}
    assert bag(scanned(client, "agg")) == bag(rows(client, agg)) == {
        (1, D("1.01"), D("1.01"), D("3.500"), 1.01, 1, D("3.53500")): 1,
        (2, D("10.13"), D("3.00"), D("7.130"), 5.065, 2, D("50.83690")): 1,
        (3, D("0.50"), D("0.50"), D("2.000"), 0.5, 1, D("1.00000")): 1,
    }
