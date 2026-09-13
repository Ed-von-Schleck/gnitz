"""LIKE / ILIKE: a matcher whose behaviour is fixed by compile-time literal data.

Every other operator in this directory decides what to do from its per-row
operands. A LIKE decides from its *pattern*, tokenized once when the program is
built, and the tokenization picks one of four specializations — exact, prefix,
suffix, contains — or the generic backtracking walk. That makes the pattern part
of the compiled circuit rather than part of the data.

What is asserted here is that the tokenized pattern survives into a maintained
circuit and keeps answering as rows arrive and leave, that the verdict is an
ordinary nullable boolean, and that a view filter, an ad-hoc read and a DML
residual cannot disagree about one.
"""

from _read import bag, rows, scanned

# id 6 and 7 carry the characters the escape rules are about: a literal percent,
# and a trailing backslash that is only an escape if escaping is on.
_ROWS = r"(1, 'abc'), (2, 'abyz'), (3, 'xxmidxx'), (4, 'exact'), (5, 'ALICE'), " \
        r"(6, '100%'), (7, '100\'), (8, NULL)"

_PATTERNS = {
    # One per specialization, then the generic walk.
    "s LIKE 'exact'": {4},
    "s LIKE 'ab%'": {1, 2},
    "s LIKE '%yz'": {2},
    "s LIKE '%mid%'": {3},
    "s LIKE 'a_c%'": {1},
    "s LIKE '%'": {1, 2, 3, 4, 5, 6, 7},
    # ILIKE folds ASCII case; LIKE does not.
    "s ILIKE 'alice'": {5},
    "s LIKE 'alice'": set(),
    "s ILIKE '%c%'": {1, 4, 5},
    # NOT LIKE is `bool_not` over the verdict, so a NULL subject stays NULL and
    # is admitted by neither direction.
    "s NOT LIKE 'ab%'": {3, 4, 5, 6, 7},
    "s NOT ILIKE 'A%'": {3, 4, 6, 7},
    # The ESCAPE clause reaches the tokenizer: the default makes `\%` a literal
    # percent, an empty ESCAPE turns the backslash back into an ordinary byte and
    # leaves `%` a wildcard, and an alternate escape character works the same.
    r"s LIKE '100\%'": {6},
    r"s LIKE '100\%' ESCAPE ''": {7},
    "s LIKE '100!%' ESCAPE '!'": {6},
    # The subject is any string expression, not only a column.
    "UPPER(s) LIKE 'AB%'": {1, 2},
    "TRIM(s) LIKE '%mid%'": {3},
}

_FLAGS = "SELECT id, s LIKE 'a%' AS flag, s ILIKE 'A%' AS iflag FROM t"


def test_a_pattern_answers_alike_on_every_surface_as_its_rows_move(client, schema_name):
    """The pattern is compiled into the circuit at CREATE time and into a
    residual at read and DML time; the compilations must agree row for row. A
    row whose value changes must be retracted from every view it no longer
    matches and admitted to every one it now does — each exactly once — and a
    row that arrives only later is matched by the pattern compiled earlier."""
    sn = schema_name
    client.execute_sql("CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)", schema_name=sn)
    views = {p: f"v{n}" for n, p in enumerate(_PATTERNS)}
    for p, v in views.items():
        client.execute_sql(f"CREATE VIEW {v} AS SELECT id FROM t WHERE {p}", schema_name=sn)
    # A LIKE is a value as much as a predicate, so it can be projected and
    # stored — with the NULL a NULL subject produces surviving into the column.
    client.execute_sql(f"CREATE VIEW flags AS {_FLAGS}", schema_name=sn)
    client.execute_sql(f"INSERT INTO t VALUES {_ROWS}", schema_name=sn)

    def ids(pred):
        return bag(scanned(client, sn, views[pred]))

    def every_surface_agrees():
        for p, v in views.items():
            assert bag(scanned(client, sn, v)) == \
                bag(rows(client, sn, f"SELECT id FROM t WHERE {p}")), p
        assert bag(scanned(client, sn, "flags")) == bag(rows(client, sn, _FLAGS))

    for p, want in _PATTERNS.items():
        assert ids(p) == {(i,): 1 for i in want}, p
    assert bag(scanned(client, sn, "flags")) == {
        (1, 1, 1): 1, (2, 1, 1): 1, (3, 0, 0): 1, (4, 0, 0): 1,
        (5, 0, 1): 1, (6, 0, 0): 1, (7, 0, 0): 1, (8, None, None): 1,
    }
    every_surface_agrees()

    client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
    client.execute_sql("UPDATE t SET s = 'abq' WHERE id = 5", schema_name=sn)
    client.execute_sql("UPDATE t SET s = 'inexact' WHERE id = 4", schema_name=sn)
    client.execute_sql("INSERT INTO t VALUES (9, 'abz')", schema_name=sn)
    assert ids("s LIKE 'ab%'") == {(1,): 1, (5,): 1, (9,): 1}
    assert ids("s LIKE '%yz'") == {}
    assert ids("s LIKE 'a_c%'") == {(1,): 1}, "`a_c%` needs a `c` in third place"
    assert ids("s LIKE 'exact'") == {}
    every_surface_agrees()

    # What a LIKE view holds is exactly what a LIKE delete removes.
    client.execute_sql("DELETE FROM t WHERE s ILIKE 'a%'", schema_name=sn)
    client.execute_sql("UPDATE t SET s = 'touched' WHERE s LIKE '100%'", schema_name=sn)
    assert bag(scanned(client, sn, "t")) == {
        (3, "xxmidxx"): 1, (4, "inexact"): 1, (6, "touched"): 1, (7, "touched"): 1,
        (8, None): 1}
    every_surface_agrees()
