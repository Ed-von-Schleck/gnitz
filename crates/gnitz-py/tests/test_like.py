"""E2E tests: LIKE / ILIKE, in incrementally-maintained views and in DML.

Every pattern shape the matcher specializes (exact, prefix, suffix, contains)
and the generic glob walk are driven through a view's insert/update/delete
maintenance, so each one is exercised on the path a query actually takes.

Run with GNITZ_WORKERS=4: a view filter is evaluated per worker partition, and a
single-worker run skips the fanout entirely.

    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_like.py -v
"""


import pytest
from _uid import uid as _uid




@pytest.fixture
def sn(client):
    """A private schema, dropped however the test leaves it."""
    name = "lk" + _uid()
    client.create_schema(name)
    yield name
    try:
        client.drop_schema(name)
    except Exception:
        pass


def _rows(client, sn, sql):
    res = client.execute_sql(sql, schema_name=sn)
    assert res[0]["type"] == "Rows", f"expected Rows, got {res[0]['type']}"
    return list(res[0]["rows"])


def _view(client, sn, name, col="s"):
    """The live values of `col` in view `name`, sorted."""
    vid = client.resolve_table(sn, name)[0]
    return sorted(r[col] for r in client.scan(vid).mappings())


def _lit(s):
    """A SQL string literal. Only the quote doubles — a backslash is an ordinary
    byte here, which is exactly what the escape tests need to store."""
    return "NULL" if s is None else "'" + s.replace("'", "''") + "'"


def _text_table(client, sn, rows, nullable=False):
    """`t (id BIGINT PK, s TEXT)` seeded with `rows`."""
    null = "" if nullable else " NOT NULL"
    client.execute_sql(
        f"CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT{null})",
        schema_name=sn,
    )
    values = ", ".join(f"({i}, {_lit(s)})" for i, s in rows)
    client.execute_sql(f"INSERT INTO t VALUES {values}", schema_name=sn)


def test_every_pattern_shape_maintains_incrementally(client, sn):
    """One view per matcher shape, through insert, update and delete."""
    client.execute_sql(
        "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
        schema_name=sn,
    )
    for name, pattern in [
        ("v_exact", "exact"),
        ("v_prefix", "ab%"),
        ("v_suffix", "%yz"),
        ("v_contains", "%mid%"),
        ("v_generic", "a_c%"),
    ]:
        client.execute_sql(
            f"CREATE VIEW {name} AS SELECT * FROM t WHERE s LIKE '{pattern}'",
            schema_name=sn,
        )

    client.execute_sql(
        "INSERT INTO t VALUES (1, 'abc'), (2, 'abyz'), (3, 'xxmidxx'), "
        "(4, 'exact'), (5, 'zzz')",
        schema_name=sn,
    )
    assert _view(client, sn, "v_exact") == ["exact"]
    assert _view(client, sn, "v_prefix") == ["abc", "abyz"]
    assert _view(client, sn, "v_suffix") == ["abyz"]
    assert _view(client, sn, "v_contains") == ["xxmidxx"]
    # `a_c%` is the glob walk: `abc` matches, `abyz` does not.
    assert _view(client, sn, "v_generic") == ["abc"]

    # A delete retracts from every view that held the row.
    client.execute_sql("DELETE FROM t WHERE id = 2", schema_name=sn)
    assert _view(client, sn, "v_prefix") == ["abc"]
    assert _view(client, sn, "v_suffix") == []

    # An update moves a row into a view it did not match before …
    client.execute_sql("UPDATE t SET s = 'abq' WHERE id = 5", schema_name=sn)
    assert _view(client, sn, "v_prefix") == ["abc", "abq"]
    assert _view(client, sn, "v_generic") == ["abc"]
    # … and out of one it did.
    client.execute_sql("UPDATE t SET s = 'inexact' WHERE id = 4", schema_name=sn)
    assert _view(client, sn, "v_exact") == []


def test_ilike_folds_case_where_like_does_not(client, sn):
    _text_table(client, sn, [(1, "ALICE"), (2, "alice"), (3, "AlIcE"), (4, "Bob")])
    client.execute_sql(
        "CREATE VIEW v_ci AS SELECT * FROM t WHERE s ILIKE 'alice'",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW v_cs AS SELECT * FROM t WHERE s LIKE 'alice'",
        schema_name=sn,
    )
    assert _view(client, sn, "v_ci") == ["ALICE", "AlIcE", "alice"]
    assert _view(client, sn, "v_cs") == ["alice"]

    # And through a wildcard shape, on a row that arrives after the view.
    client.execute_sql(
        "CREATE VIEW v_pre AS SELECT * FROM t WHERE s ILIKE 'a%e'",
        schema_name=sn,
    )
    client.execute_sql("INSERT INTO t VALUES (5, 'AXE')", schema_name=sn)
    assert _view(client, sn, "v_pre") == ["ALICE", "AXE", "AlIcE", "alice"]


def test_not_like_excludes_null_rows(client, sn):
    """`NOT LIKE` is `bool_not` over the verdict, so a NULL stays NULL and the
    row is excluded from both the positive and the negated view."""
    _text_table(client, sn, [(1, "abc"), (2, "xyz"), (3, None)], nullable=True)
    client.execute_sql(
        "CREATE VIEW v_yes AS SELECT * FROM t WHERE s LIKE 'a%'",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW v_no AS SELECT * FROM t WHERE s NOT LIKE 'a%'",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW v_no_ci AS SELECT * FROM t WHERE s NOT ILIKE 'A%'",
        schema_name=sn,
    )
    assert _view(client, sn, "v_yes") == ["abc"]
    assert _view(client, sn, "v_no") == ["xyz"]
    assert _view(client, sn, "v_no_ci") == ["xyz"]


def test_like_over_a_computed_operand(client, sn):
    """The subject is any string expression, not just a column."""
    _text_table(client, sn, [(1, "  abc  "), (2, "  xyz"), (3, "abc")])
    client.execute_sql(
        "CREATE VIEW v AS SELECT * FROM t WHERE TRIM(s) LIKE 'ab%'",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW v_up AS SELECT * FROM t WHERE UPPER(s) LIKE '%XYZ'",
        schema_name=sn,
    )
    assert _view(client, sn, "v") == ["  abc  ", "abc"]
    assert _view(client, sn, "v_up") == ["  xyz"]


def test_escape_clause(client, sn):
    r"""The default escape makes `\%` the literal `%`; `ESCAPE ''` turns the
    backslash back into an ordinary byte and leaves `%` a wildcard."""
    _text_table(
        client,
        sn,
        [(1, "100%"), (2, "1005"), (3, "100\\"), (4, "100\\abc")],
    )
    client.execute_sql(
        r"CREATE VIEW v_esc AS SELECT * FROM t WHERE s LIKE '100\%'",
        schema_name=sn,
    )
    client.execute_sql(
        r"CREATE VIEW v_raw AS SELECT * FROM t WHERE s LIKE '100\%' ESCAPE ''",
        schema_name=sn,
    )
    client.execute_sql(
        "CREATE VIEW v_alt AS SELECT * FROM t WHERE s LIKE '100!%' ESCAPE '!'",
        schema_name=sn,
    )
    assert _view(client, sn, "v_esc") == ["100%"]
    assert _view(client, sn, "v_raw") == ["100\\", "100\\abc"]
    assert _view(client, sn, "v_alt") == ["100%"]


def test_like_in_a_projection(client, sn):
    """The verdict is an ordinary boolean value, so it can be selected and
    materialized as a view column, not only tested in a WHERE."""
    _text_table(client, sn, [(1, "abc"), (2, "xyz")])
    rows = _rows(client, sn, "SELECT id, s LIKE 'a%' AS flag FROM t")
    assert sorted((r["id"], r["flag"]) for r in rows) == [(1, 1), (2, 0)]

    client.execute_sql(
        "CREATE VIEW v AS SELECT id, s ILIKE 'A%' AS flag FROM t", schema_name=sn
    )
    vid = client.resolve_table(sn, "v")[0]
    assert sorted((r["id"], r["flag"]) for r in client.scan(vid).mappings()) == [
        (1, 1),
        (2, 0),
    ]


def test_adhoc_select_with_like(client, sn):
    _text_table(client, sn, [(1, "abc"), (2, "xxmidxx"), (3, "ABC")])
    ids = [r["id"] for r in _rows(client, sn, "SELECT id FROM t WHERE s LIKE '%mid%'")]
    assert ids == [2]
    got = sorted(r["id"] for r in _rows(client, sn, "SELECT id FROM t WHERE s ILIKE 'abc'"))
    assert got == [1, 3]


def test_point_dml_by_like(client, sn):
    """A DML residual compiles the same program a view filter does, so the two
    surfaces cannot disagree about a pattern."""
    _text_table(
        client,
        sn,
        [(1, "del-one"), (2, "del-two"), (3, "keep"), (4, "UPme"), (5, "upYOU")],
    )
    res = client.execute_sql("DELETE FROM t WHERE s LIKE 'del%'", schema_name=sn)
    assert res[0]["count"] == 2
    assert sorted(r["s"] for r in _rows(client, sn, "SELECT s FROM t")) == [
        "UPme",
        "keep",
        "upYOU",
    ]

    res = client.execute_sql(
        "UPDATE t SET s = 'touched' WHERE s ILIKE 'up%'", schema_name=sn
    )
    assert res[0]["count"] == 2
    assert sorted(r["s"] for r in _rows(client, sn, "SELECT s FROM t")) == [
        "keep",
        "touched",
        "touched",
    ]
