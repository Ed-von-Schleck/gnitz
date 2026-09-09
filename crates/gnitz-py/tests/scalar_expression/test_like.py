"""LIKE / ILIKE: a matcher whose behaviour is fixed by compile-time literal data.

Every other operator in this directory decides what to do from its per-row
operands. A LIKE decides from its *pattern*, tokenized once when the program is
built, and the tokenization picks one of four specializations — exact, prefix,
suffix, contains — or the generic backtracking walk. That makes the pattern part
of the compiled circuit rather than part of the data.

The matcher's own semantics — which pattern reaches which specialization, the
escape rules, the byte-granular anchor walk, ASCII-only ILIKE folding — are
pinned directly in `gnitz-expr`'s kernel tests, over corpora no SQL literal can
express. What is left to assert here is what only a running engine can answer:
that the tokenized pattern survives into a maintained circuit and keeps
answering as rows arrive and leave, that the verdict is an ordinary nullable
boolean, and that a view filter and a DML residual cannot disagree about one.
"""

import pytest
from _read import bag, rows, scanned

# id 6 and 7 carry the characters the escape rules are about: a literal percent,
# and a trailing backslash that is only an escape if escaping is on.
_ROWS = [(1, "abc"), (2, "abyz"), (3, "xxmidxx"), (4, "exact"), (5, "ALICE"),
         (6, "100%"), (7, "100\\"), (8, None)]


@pytest.fixture
def lk(client, schema_name):
    """`t (id, s)` with `s` nullable, holding one value per matcher shape."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)",
        schema_name=schema_name)
    client.execute_sql(
        "INSERT INTO t VALUES " + ", ".join(
            f"({i}, " + ("NULL" if s is None else "'" + s.replace("'", "''") + "'") + ")"
            for i, s in _ROWS), schema_name=schema_name)
    return schema_name


@pytest.mark.parametrize("pred,want", [
    # One per specialization, then the generic walk.
    ("s LIKE 'exact'", {4}),
    ("s LIKE 'ab%'", {1, 2}),
    ("s LIKE '%yz'", {2}),
    ("s LIKE '%mid%'", {3}),
    ("s LIKE 'a_c%'", {1}),
    ("s LIKE '%'", {1, 2, 3, 4, 5, 6, 7}),
    # ILIKE folds ASCII case; LIKE does not.
    ("s ILIKE 'alice'", {5}),
    ("s LIKE 'alice'", set()),
    ("s ILIKE '%c%'", {1, 4, 5}),
    # NOT LIKE is `bool_not` over the verdict, so a NULL subject stays NULL and
    # is admitted by neither direction — the row is in neither view.
    ("s NOT LIKE 'ab%'", {3, 4, 5, 6, 7}),
    ("s NOT ILIKE 'A%'", {3, 4, 6, 7}),
    # The ESCAPE clause reaches the tokenizer: the default makes `\%` a literal
    # percent, an empty ESCAPE turns the backslash back into an ordinary byte and
    # leaves `%` a wildcard, and an alternate escape character works the same.
    (r"s LIKE '100\%'", {6}),
    (r"s LIKE '100\%' ESCAPE ''", {7}),
    ("s LIKE '100!%' ESCAPE '!'", {6}),
    # The subject is any string expression, not only a column.
    ("UPPER(s) LIKE 'AB%'", {1, 2}),
    ("TRIM(s) LIKE '%mid%'", {3}),
])
def test_a_pattern_answers_the_same_in_a_maintained_view_and_an_adhoc_read(
        client, lk, pred, want):
    """The pattern is compiled into the circuit at CREATE time and into a
    residual at read time; the two compilations must agree row for row."""
    client.execute_sql(f"CREATE VIEW v AS SELECT id FROM t WHERE {pred}",
                       schema_name=lk)
    expected = {(i,): 1 for i in want}
    assert bag(scanned(client, lk, "v"), "id") == expected
    assert bag(rows(client, lk, f"SELECT id FROM t WHERE {pred}")) == expected


def test_a_row_moves_between_pattern_views_as_its_value_changes(client, lk):
    """A tokenized pattern has to keep answering against rows that arrive after
    the view was built, and a row whose value changes must be retracted from
    every view it no longer matches and admitted to every one it now does — each
    exactly once."""
    sn = lk
    for name, pattern in [("v_prefix", "ab%"), ("v_suffix", "%yz"),
                          ("v_exact", "exact"), ("v_generic", "a_c%")]:
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT id FROM t WHERE s LIKE '{pattern}'",
            schema_name=sn)

    def ids(name):
        return bag(scanned(client, sn, name), "id")

    assert ids("v_prefix") == {(1,): 1, (2,): 1}
    assert ids("v_suffix") == {(2,): 1}

    # A delete retracts from every view that held the row.
    client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
    assert ids("v_prefix") == {(1,): 1}
    assert ids("v_suffix") == {}

    # An update moves a row into a view it did not match …
    client.execute_sql("UPDATE t SET s = 'abq' WHERE id = 5", schema_name=sn)
    assert ids("v_prefix") == {(1,): 1, (5,): 1}
    assert ids("v_generic") == {(1,): 1}, "`a_c%` needs a `c` in third place"
    # … and out of one it did.
    client.execute_sql("UPDATE t SET s = 'inexact' WHERE id = 4", schema_name=sn)
    assert ids("v_exact") == {}

    # A row that arrives only now is matched by the pattern compiled earlier.
    client.execute_sql("INSERT INTO t VALUES (9, 'abz')", schema_name=sn)
    assert ids("v_prefix") == {(1,): 1, (5,): 1, (9,): 1}


def test_the_verdict_is_an_ordinary_nullable_boolean_column(client, lk):
    """A LIKE is a value as much as a predicate, so it can be projected and
    stored — with the NULL a NULL subject produces surviving into the column
    rather than collapsing to false."""
    client.execute_sql(
        "CREATE VIEW v AS SELECT id, s LIKE 'a%' AS flag, s ILIKE 'A%' AS iflag FROM t",
        schema_name=lk)
    assert bag(scanned(client, lk, "v"), "id", "flag", "iflag") == {
        (1, 1, 1): 1, (2, 1, 1): 1, (3, 0, 0): 1, (4, 0, 0): 1,
        (5, 0, 1): 1, (6, 0, 0): 1, (7, 0, 0): 1,
        (8, None, None): 1,
    }


def test_a_pattern_in_a_dml_residual_matches_what_the_view_holds(client, lk):
    """A DML residual compiles the same program a view filter does, so what a
    LIKE view holds is exactly what a LIKE delete removes."""
    sn = lk
    client.execute_sql("CREATE VIEW v AS SELECT id FROM t WHERE s ILIKE 'a%'",
                       schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {(1,): 1, (2,): 1, (5,): 1}

    client.execute_sql("DELETE FROM t WHERE s ILIKE 'a%'", schema_name=sn)
    assert bag(scanned(client, sn, "v"), "id") == {}
    assert bag(scanned(client, sn, "t"), "id") == {
        (3,): 1, (4,): 1, (6,): 1, (7,): 1, (8,): 1}

    client.execute_sql("UPDATE t SET s = 'touched' WHERE s LIKE '100%'", schema_name=sn)
    assert bag(scanned(client, sn, "t"), "id", "s") == {
        (3, "xxmidxx"): 1, (4, "exact"): 1, (6, "touched"): 1,
        (7, "touched"): 1, (8, None): 1}
