"""BOOLEAN inside an expression: the type of every test, and of nothing else.

A comparison, a null test, LIKE, IN, EXISTS and the connectives produce a
BOOLEAN, and a condition takes one: there is no number that is true. So a
projected test is a BOOLEAN column, a BOOLEAN column is a condition on its own,
and the two convert to and from a number only through a CAST.

Asserted on weights, and across a retraction: a verdict over a flag is
recomputed when the flag changes, and a stale one leaves the old row behind.
"""

import itertools

import gnitz
import pytest
from _read import bag, rows, scanned

_T = "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, b BOOLEAN, c BOOLEAN, x BIGINT)"
# Every pairing of TRUE / FALSE / NULL for `b` and `c`, ids 1 to 9 in that
# order with `c` varying fastest; `x` is the id.
_FLAGS = dict(enumerate(itertools.product((True, False, None), repeat=2), 1))
_ROWS = "INSERT INTO t VALUES " + ", ".join(
    "(%d, %s, %s, %d)" % (i, *("NULL" if v is None else str(v).upper() for v in bc), i) for i, bc in _FLAGS.items())

# Each condition against the ids it admits.
_CONDITIONS = {
    "b": {1, 2, 3},
    "NOT b": {4, 5, 6},
    "b AND c": {1},
    "b OR c": {1, 2, 3, 4, 7},
    "b AND x > 1": {2, 3},
    "b = TRUE": {1, 2, 3},
    "b = FALSE": {4, 5, 6},
    "b <> c": {2, 4},
    "b = c": {1, 5},
    # FALSE orders below TRUE.
    "b < c": {4},
    "b >= c": {1, 2, 5},
    "b = 'yes'": {1, 2, 3},
    "b IN (TRUE, FALSE)": {1, 2, 3, 4, 5, 6},
    # The IS tests are total: a NULL is not TRUE and not FALSE.
    "b IS TRUE": {1, 2, 3},
    "b IS NOT TRUE": {4, 5, 6, 7, 8, 9},
    "b IS FALSE": {4, 5, 6},
    "b IS NOT FALSE": {1, 2, 3, 7, 8, 9},
    "b IS UNKNOWN": {7, 8, 9},
    "b IS NOT UNKNOWN": {1, 2, 3, 4, 5, 6},
    "(b AND c) IS NOT TRUE": {2, 3, 4, 5, 6, 7, 8, 9},
    "b IS DISTINCT FROM c": {2, 3, 4, 6, 7, 8},
    "COALESCE(b, c)": {1, 2, 3, 7},
    "CASE WHEN x > 5 THEN c ELSE b END": {1, 2, 3, 7},
    "CAST(x - 1 AS BOOLEAN)": {2, 3, 4, 5, 6, 7, 8, 9},
    "CAST(b AS INT) = 1": {1, 2, 3},
    "CAST(b AS TEXT) = 'false'": {4, 5, 6},
    "TRUE": {1, 2, 3, 4, 5, 6, 7, 8, 9},
    "FALSE": set(),
}


def test_a_condition_reads_a_boolean_across_a_retraction(client):
    """One filter view per condition, and the same condition as a direct read.
    Swapping `b` and `c` moves rows across each condition's boundary, so the
    view's delta must retract exactly what the old flags admitted."""
    client.execute_sql(_T)
    views = {c: f"v{n}" for n, c in enumerate(_CONDITIONS)}
    for cond, v in views.items():
        client.execute_sql(f"CREATE VIEW {v} AS SELECT id FROM t WHERE {cond}")
    client.execute_sql(_ROWS)
    for cond, want in _CONDITIONS.items():
        assert bag(scanned(client, views[cond])) == {(k,): 1 for k in want}, cond
        assert bag(rows(client, f"SELECT id FROM t WHERE {cond}")) == {(k,): 1 for k in want}, cond

    # `b` and `c` trade places, which maps id 2 ↔ 4, 3 ↔ 7 and 6 ↔ 8 in the
    # pairing table and leaves `x` behind on the row it was written to.
    client.execute_sql("UPDATE t SET b = c, c = b")
    swap = {2: 4, 4: 2, 3: 7, 7: 3, 6: 8, 8: 6}
    for cond in ("b", "NOT b", "b AND c", "b OR c", "b = c", "b < c", "b IS NOT TRUE", "b IS UNKNOWN"):
        want = {swap.get(k, k) for k in _CONDITIONS[cond]}
        assert bag(scanned(client, views[cond])) == {(k,): 1 for k in want}, cond


def test_a_projected_test_is_a_boolean_column(client):
    """Every test, connective and boolean-valued CASE is declared BOOLEAN and
    stores the byte a declared column would, so a view over the view reads it as
    a condition. A NULL verdict is a NULL cell, not FALSE."""
    client.execute_sql(_T)
    items = {
        "gt": "x > 5",
        "both": "b AND c",
        "nb": "NOT b",
        "isn": "b IS NULL",
        "ist": "b IS TRUE",
        "pick": "CASE WHEN x > 5 THEN c ELSE b END",
        "co": "COALESCE(b, FALSE)",
        "fromint": "CAST(x - 1 AS BOOLEAN)",
        "lit": "TRUE",
        "inl": "x IN (1, 2, 9)",
        "lk": "CAST(x AS TEXT) LIKE '1%'",
    }
    client.execute_sql("CREATE VIEW v AS SELECT id, " + ", ".join(f"{e} AS {n}" for n, e in items.items()) + " FROM t")
    client.execute_sql("CREATE VIEW vv AS SELECT id FROM v WHERE both OR nb")
    _, schema = client.resolve_table("v")
    assert [c.type_code for c in schema.columns[1:]] == [gnitz.TypeCode.BOOLEAN] * len(items)
    client.execute_sql(_ROWS)

    b = {i: bc[0] for i, bc in _FLAGS.items()}
    c = {i: bc[1] for i, bc in _FLAGS.items()}
    and3 = lambda p, q: False if p is False or q is False else (None if p is None or q is None else True)
    want = {
        (i, i > 5, and3(b[i], c[i]), None if b[i] is None else not b[i], b[i] is None, b[i] is True,
         c[i] if i > 5 else b[i], bool(b[i]), i != 1, True, i in (1, 2, 9), i == 1): 1
        for i in range(1, 10)
    }
    got = scanned(client, "v")
    assert bag(got) == want
    assert {type(v) for r in got for v in tuple(r)[1:]} == {bool, type(None)}
    # `both OR nb`: TRUE where both are, or where b is FALSE.
    assert bag(scanned(client, "vv")) == {(1,): 1, (4,): 1, (5,): 1, (6,): 1}

    client.execute_sql("DELETE FROM t WHERE id IN (1, 4)")
    assert bag(scanned(client, "vv")) == {(5,): 1, (6,): 1}
    assert bag(scanned(client, "v"), "id") == {(i,): 1 for i in (2, 3, 5, 6, 7, 8, 9)}


def test_a_boolean_converts_to_a_number_and_text_only_by_cast(client):
    """`CAST(b AS INT)` is the 0 or 1 an aggregate can sum, `CAST(b AS TEXT)`
    its spelling, and each is NULL over a NULL. CONCAT takes the same text."""
    client.execute_sql(_T)
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, CAST(b AS INT) AS n, CAST(b AS TEXT) AS s, CONCAT('b=', b) AS cc FROM t")
    client.execute_sql(
        "CREATE VIEW tally AS SELECT SUM(CAST(b AS BIGINT)) AS yes, "
        "SUM(CASE WHEN b THEN 0 ELSE 1 END) AS not_yes, COUNT(b) AS known FROM t")
    client.execute_sql(_ROWS)
    assert bag(rows(client, "SELECT id, n, s, cc FROM v WHERE id IN (1, 4, 7)")) == {
        (1, 1, "true", "b=true"): 1, (4, 0, "false", "b=false"): 1, (7, None, None, "b="): 1,
    }
    assert bag(scanned(client, "tally")) == {(3, 6, 6): 1}
    client.execute_sql("UPDATE t SET b = TRUE WHERE id = 9")
    assert bag(scanned(client, "tally")) == {(4, 5, 7): 1}


@pytest.mark.parametrize("sql,needle", [
    # A number is not a condition.
    ("SELECT id FROM t WHERE x", "a condition must be BOOLEAN"),
    ("SELECT id FROM t WHERE b AND x", "a condition must be BOOLEAN"),
    ("SELECT id FROM t WHERE NOT x", "a condition must be BOOLEAN"),
    ("SELECT id, CASE WHEN x THEN 1 ELSE 0 END AS y FROM t", "a condition must be BOOLEAN"),
    # A BOOLEAN is not a number.
    ("SELECT id, b + 1 AS y FROM t", "not supported on a BOOLEAN operand"),
    ("SELECT id, -b AS y FROM t", "BOOLEAN"),
    ("SELECT id, ABS(b) AS y FROM t", "BOOLEAN"),
    ("SELECT id FROM t WHERE b = 1", "between BOOLEAN and"),
    ("SELECT id FROM t WHERE x = TRUE", "BOOLEAN"),
    ("SELECT id FROM t WHERE b IN (1, 0)", "between BOOLEAN and"),
    ("SELECT id FROM t WHERE b = 'maybe'", "invalid BOOLEAN literal"),
    ("SELECT id, CASE WHEN b THEN 1 ELSE FALSE END AS y FROM t", "BOOLEAN"),
    ("SELECT id, COALESCE(b, 0) AS y FROM t", "BOOLEAN"),
    ("SELECT id, GREATEST(b, x) AS y FROM t", "BOOLEAN"),
    ("SELECT id, CAST(b AS DOUBLE) AS y FROM t", "CAST of BOOLEAN to"),
    ("SELECT id, CAST(b AS DATE) AS y FROM t", "CAST of BOOLEAN to"),
    ("SELECT id, CAST(CAST(x AS TEXT) AS BOOLEAN) AS y FROM t", "literal only"),
    ("SELECT id, CAST('maybe' AS BOOLEAN) AS y FROM t", "invalid BOOLEAN literal"),
    ("SELECT id FROM t WHERE x IS UNKNOWN", "IS UNKNOWN takes a BOOLEAN"),
    ("SELECT SUM(b) AS s FROM t", "SUM: not supported on BOOLEAN"),
    ("SELECT AVG(b) AS s FROM t", "not supported on BOOLEAN"),
    ("SELECT SUM(x > 1) AS s FROM t", "BOOLEAN"),
    ("SELECT t.id FROM t JOIN u ON t.b = u.n", "BOOLEAN"),
    ("SELECT x FROM t UNION SELECT flag FROM u", "type mismatch"),
])
def test_a_boolean_and_a_number_do_not_mix(client, sql, needle):
    """Every mixed form is refused as a view body, naming the rule — where a
    silent coercion would make `b = 2` false for a true `b`."""
    client.execute_sql(_T)
    client.execute_sql("CREATE TABLE u (n BIGINT NOT NULL PRIMARY KEY, flag BOOLEAN)")
    with pytest.raises(gnitz.GnitzError, match=needle):
        client.execute_sql(f"CREATE VIEW bad AS {sql}")


@pytest.mark.parametrize("sql,needle", [
    ("INSERT INTO t VALUES (1, 1, NULL, 1)", "1 is not a BOOLEAN value"),
    ("INSERT INTO t VALUES (1, 'maybe', NULL, 1)", "invalid BOOLEAN literal"),
    ("INSERT INTO t VALUES (1, TRUE, NULL, TRUE)", "TRUE is not a"),
    ("UPDATE t SET b = 1", "is not a BOOLEAN value"),
    ("UPDATE t SET b = x", "cannot assign a"),
    ("UPDATE t SET x = b", "cannot assign a"),
    ("UPDATE t SET x = x > 1", "cannot assign a"),
    ("DELETE FROM t WHERE x", "a condition must be BOOLEAN"),
])
def test_a_write_takes_a_boolean_only_where_the_column_is_one(client, sql, needle):
    client.execute_sql(_T)
    client.execute_sql("INSERT INTO t VALUES (5, TRUE, FALSE, 5)")
    with pytest.raises(gnitz.GnitzError, match=needle):
        client.execute_sql(sql)
    assert bag(scanned(client, "t")) == {(5, True, False, 5): 1}
