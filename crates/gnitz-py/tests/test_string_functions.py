"""E2E: string values in expressions — UPPER/LOWER, LENGTH, SUBSTRING, TRIM,
CONCAT/||, string CASE/COALESCE, and CAST between STRING and the numeric types.

Every function here is a pure per-row transform, so these tests check more than
the value: a retraction must re-derive byte-identical output and cancel under
(PK, payload) consolidation. A computed string that differed by one byte between
an insert and its retraction would leave both rows in the trace forever, which is
why the update/delete cycles below check row *counts* as much as values.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_string_functions.py -v --tb=short
"""

import random
import pytest
import gnitz


def _uid():
    return str(random.randint(100000, 999999))


def _cleanup(client, sn):
    try:
        client.drop_schema(sn)
    except Exception:
        pass


@pytest.fixture
def schema_name(client):
    sn = "s" + _uid()
    client.create_schema(sn)
    yield sn
    _cleanup(client, sn)


def _dicts(client, vid):
    """Current (positive-state) view rows as dicts."""
    return client.scan(vid).mappings()


def _by_id(client, vid):
    return {r["id"]: r for r in _dicts(client, vid)}


def _rejects(client, sn, sql, want):
    """The statement must fail, and fail for the stated reason — a bare "raises"
    would also pass on a typo or a duplicate view name."""
    with pytest.raises(Exception) as ei:
        client.execute_sql(sql, schema_name=sn)
    assert want in str(ei.value), f"{sql!r} failed for the wrong reason: {ei.value}"


def _adhoc(client, sn, sql):
    res = client.execute_sql(sql, schema_name=sn)[0]
    assert res["type"] == "Rows", res
    return list(res["rows"])


# A value on each side of the 12-byte inline/heap boundary, plus non-ASCII and
# an empty string — the four cell shapes every kernel has to handle.
CORPUS = [
    (1, "abc"),
    (2, "exactly12chr"),
    (3, "thirteen-char"),
    (4, "Grüße-Österreich"),
    (5, ""),
]


def _corpus_values():
    return ", ".join(f"({i}, '{s}')" for i, s in CORPUS)


# ---------------------------------------------------------------------------
# Projections
# ---------------------------------------------------------------------------


class TestProjection:
    def test_every_function_over_a_nullable_column(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, "
            "UPPER(s) AS u, LOWER(s) AS l, "
            "LENGTH(s) AS n, CHAR_LENGTH(s) AS cn, OCTET_LENGTH(s) AS bn, "
            "SUBSTRING(s FROM 2 FOR 3) AS sub, "
            "TRIM(s) AS tr, LTRIM(s) AS lt, RTRIM(s) AS rt "
            "FROM t",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, '  Grüße  '), (2, 'abc'), (3, NULL)",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        rows = _by_id(client, vid)

        # ASCII-only folding: the umlaut and the ß are untouched, which is a
        # documented deviation from PostgreSQL under a UTF-8 locale.
        assert rows[1]["u"] == "  GRüßE  "
        assert rows[1]["l"] == "  grüße  "
        # LENGTH counts characters; OCTET_LENGTH counts bytes. 'Grüße' is 5
        # characters in 7 bytes, plus the four spaces.
        assert rows[1]["n"] == 9 and rows[1]["cn"] == 9 and rows[1]["bn"] == 11
        assert rows[1]["sub"] == " Gr"
        assert rows[1]["tr"] == "Grüße"
        assert rows[1]["lt"] == "Grüße  "
        assert rows[1]["rt"] == "  Grüße"

        assert rows[2]["u"] == "ABC" and rows[2]["sub"] == "bc"
        # NULL in, NULL out for every one of them.
        for c in ("u", "l", "n", "cn", "bn", "sub", "tr", "lt", "rt"):
            assert rows[3][c] is None, c

    def test_values_round_trip_across_the_inline_heap_boundary(self, client, schema_name):
        """A value of 12 bytes or less lives inside its 16-byte cell; a longer
        one lives in the blob heap. A map must relocate the second kind into its
        own heap, so both classes have to survive the same projection."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, UPPER(s) AS u, LENGTH(s) AS n FROM t",
            schema_name=sn,
        )
        client.execute_sql(f"INSERT INTO t VALUES {_corpus_values()}", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        rows = _by_id(client, vid)
        assert len(rows) == len(CORPUS)
        for i, s in CORPUS:
            assert rows[i]["n"] == len(s), f"row {i}"
            if s.isascii():
                assert rows[i]["u"] == s.upper(), f"row {i}"
        assert rows[3]["u"] == "THIRTEEN-CHAR"
        assert rows[5]["u"] == ""

    def test_a_view_whose_only_projected_column_is_a_computed_string(self, client, schema_name):
        """The compute kernel is gated on the emit lists being non-empty. A gate
        that counted only the scalar emits would drop the kernel here and ship
        the STRING column's uninitialized region — recycled heap bytes, so an
        insert and its retraction would never cancel."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT id, UPPER(s) AS u FROM t", schema_name=sn)
        client.execute_sql(f"INSERT INTO t VALUES {_corpus_values()}", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        rows = _by_id(client, vid)
        assert rows[1]["u"] == "ABC"
        assert rows[3]["u"] == "THIRTEEN-CHAR"

        # Re-scanning must give the identical bytes: an uninitialized region
        # would read differently across calls.
        assert _by_id(client, vid) == rows

    def test_a_computed_string_column_is_declared_a_string(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, n BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, UPPER(s) AS u, LENGTH(s) AS l, "
            "CAST(n AS TEXT) AS nt FROM t",
            schema_name=sn,
        )
        _vid, vschema = client.resolve_table(sn, "v")
        types = {c.name: c.type_code for c in vschema.columns}
        assert types["u"] == gnitz.TypeCode.STRING
        assert types["nt"] == gnitz.TypeCode.STRING
        assert types["l"] == gnitz.TypeCode.I64


# ---------------------------------------------------------------------------
# Retraction determinism
# ---------------------------------------------------------------------------


class TestRetraction:
    def test_update_and_delete_cancel_the_computed_row(self, client, schema_name):
        """An UPDATE is a retract plus an insert, so every computed string is
        re-derived. The old row only vanishes if the derivation is byte-stable;
        otherwise the view accumulates."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, k BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, UPPER(s) AS u, "
            "SUBSTRING(TRIM(s) FROM 1 FOR 4) AS head, "
            "CONCAT(s, '-', CAST(k AS TEXT)) AS tag FROM t",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, ' a-long-value-past-twelve ', 7), (2, 'abc', 9)",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        rows = _by_id(client, vid)
        assert len(rows) == 2
        assert rows[1]["head"] == "a-lo"
        assert rows[1]["tag"] == " a-long-value-past-twelve -7"
        assert rows[2]["u"] == "ABC" and rows[2]["tag"] == "abc-9"

        # Touch only `k`: the two columns derived from the untouched string must
        # cancel bit-exactly against their retraction.
        client.execute_sql("UPDATE t SET k = 42 WHERE id = 1", schema_name=sn)
        rows = _by_id(client, vid)
        assert len(rows) == 2, "the pre-update row must have cancelled, not accumulated"
        assert rows[1]["tag"] == " a-long-value-past-twelve -42"
        assert rows[1]["head"] == "a-lo"

        # And touch the string itself, crossing the inline/heap boundary in both
        # directions.
        client.execute_sql("UPDATE t SET s = 'xy' WHERE id = 1", schema_name=sn)
        client.execute_sql("UPDATE t SET s = 'another-long-one' WHERE id = 2", schema_name=sn)
        rows = _by_id(client, vid)
        assert len(rows) == 2
        assert rows[1]["u"] == "XY"
        assert rows[2]["u"] == "ANOTHER-LONG-ONE"

        client.execute_sql("DELETE FROM t", schema_name=sn)
        assert _dicts(client, vid) == []

    def test_unicode_corpus_cancels(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, UPPER(s) AS u, LENGTH(s) AS n, "
            "OCTET_LENGTH(s) AS b, SUBSTRING(s FROM 2) AS tail FROM t",
            schema_name=sn,
        )
        corpus = ["日本語テキスト", "é", "Ω-omega-Ω", "мир", "🙂🙃"]
        vals = ", ".join(f"({i}, '{s}')" for i, s in enumerate(corpus, start=1))
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        rows = _by_id(client, vid)
        for i, s in enumerate(corpus, start=1):
            assert rows[i]["n"] == len(s), f"{s!r} character count"
            assert rows[i]["b"] == len(s.encode()), f"{s!r} byte count"
            assert rows[i]["tail"] == s[1:], f"{s!r} substring"
        client.execute_sql("DELETE FROM t", schema_name=sn)
        assert _dicts(client, vid) == []


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


class TestFilter:
    def test_computed_string_predicates(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
            schema_name=sn,
        )
        cases = [
            ("vu", "UPPER(s) = 'ABC'", {1}),
            ("vl", "LENGTH(s) > 5", {2, 3}),
            ("vt", "TRIM(s) = ''", {4}),
            ("vs", "SUBSTRING(s FROM 1 FOR 1) = 'a'", {1, 3}),
        ]
        for name, pred, _want in cases:
            client.execute_sql(f"CREATE VIEW {name} AS SELECT id, s FROM t WHERE {pred}", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 'abc'), (2, 'ABCDEFGH'), (3, 'a-long-value'), (4, '   ')",
            schema_name=sn,
        )
        for name, pred, want in cases:
            vid = client.resolve_table(sn, name)[0]
            assert {r["id"] for r in _dicts(client, vid)} == want, pred

    def test_the_plain_column_predicate_still_works(self, client, schema_name):
        """`s = 'x'` keeps the specialized column-vs-constant opcodes; the
        register channel exists for computed operands only."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, u TEXT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT id FROM t WHERE s = 'x'", schema_name=sn)
        client.execute_sql("CREATE VIEW w AS SELECT id FROM t WHERE s < u", schema_name=sn)
        client.execute_sql("INSERT INTO t VALUES (1, 'x', 'y'), (2, 'z', 'a')", schema_name=sn)
        assert {r["id"] for r in _dicts(client, client.resolve_table(sn, "v")[0])} == {1}
        assert {r["id"] for r in _dicts(client, client.resolve_table(sn, "w")[0])} == {1}


# ---------------------------------------------------------------------------
# CONCAT, ||, CASE and COALESCE
# ---------------------------------------------------------------------------


class TestComposition:
    def test_concat_and_the_operator_differ_only_in_their_null_rule(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a TEXT, b TEXT, n BIGINT)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, a || b AS pipe, CONCAT(a, b) AS cat, "
            "CONCAT(a, '-', n) AS mixed, CONCAT(a) AS one FROM t",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, 'x', 'y', 7), (2, NULL, 'y', 7), "
            "(3, 'x', NULL, NULL), (4, NULL, NULL, NULL)",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        rows = _by_id(client, vid)

        assert rows[1]["pipe"] == "xy" and rows[1]["cat"] == "xy"
        assert rows[1]["mixed"] == "x-7" and rows[1]["one"] == "x"
        # `||` propagates NULL from either side; CONCAT treats it as empty.
        assert rows[2]["pipe"] is None and rows[2]["cat"] == "y"
        assert rows[3]["pipe"] is None and rows[3]["cat"] == "x"
        # An all-NULL CONCAT is the empty string, never NULL — including the
        # single-argument case, which the seeded fold makes non-NULL.
        assert rows[4]["cat"] == "" and rows[4]["one"] == ""
        assert rows[4]["pipe"] is None
        assert rows[4]["mixed"] == "-"

    def test_string_case_and_coalesce(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT, k BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, "
            "CASE WHEN k > 5 THEN 'big' WHEN k > 0 THEN 'small' ELSE 'none' END AS bucket, "
            "CASE WHEN k > 5 THEN s END AS maybe, "
            "COALESCE(s, 'default') AS c FROM t",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, 'here', 9), (2, NULL, 3), (3, 'x', 0)",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        rows = _by_id(client, vid)
        assert rows[1]["bucket"] == "big" and rows[2]["bucket"] == "small"
        assert rows[3]["bucket"] == "none"
        # A CASE with no ELSE is ELSE NULL, and in a string CASE that NULL must
        # itself be a string — otherwise the branches would mix register classes.
        assert rows[1]["maybe"] == "here"
        assert rows[2]["maybe"] is None and rows[3]["maybe"] is None
        assert rows[1]["c"] == "here" and rows[2]["c"] == "default"

    def test_chained_string_operations(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, UPPER(SUBSTRING(TRIM(s), 1, 3)) AS h FROM t",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, '  hello world  '), (2, ' ab '), (3, '     ')",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        rows = _by_id(client, vid)
        assert rows[1]["h"] == "HEL"
        assert rows[2]["h"] == "AB"
        assert rows[3]["h"] == ""


# ---------------------------------------------------------------------------
# CAST
# ---------------------------------------------------------------------------


class TestCast:
    def test_text_to_number_parses_or_yields_null(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, CAST(s AS BIGINT) AS i, "
            "CAST(s AS INT) AS i32, CAST(s AS DOUBLE) AS f FROM t",
            schema_name=sn,
        )
        cases = [
            (1, "42", 42, 42, 42.0),
            (2, " -7 ", -7, -7, -7.0),
            (3, "1.5", None, None, 1.5),
            (4, "", None, None, None),
            (5, "42abc", None, None, None),
            (6, "3000000000", 3000000000, None, 3000000000.0),  # out of INT range
            (7, "0x10", None, None, None),  # a documented narrowing vs PG 16+
        ]
        vals = ", ".join(f"({i}, '{s}')" for i, s, _a, _b, _c in cases)
        client.execute_sql(f"INSERT INTO t VALUES {vals}", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        rows = _by_id(client, vid)
        for i, s, want_i, want_i32, want_f in cases:
            assert rows[i]["i"] == want_i, f"{s!r} as BIGINT"
            assert rows[i]["i32"] == want_i32, f"{s!r} as INT"
            if want_f is None:
                assert rows[i]["f"] is None, f"{s!r} as DOUBLE"
            else:
                assert rows[i]["f"] == pytest.approx(want_f), f"{s!r} as DOUBLE"

    def test_number_to_text(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL, "
            "u BIGINT UNSIGNED NOT NULL, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, CAST(i AS TEXT) AS it, "
            "CAST(u AS TEXT) AS ut, CAST(f AS TEXT) AS ft FROM t",
            schema_name=sn,
        )
        big_u = 2**64 - 1
        client.execute_sql(
            f"INSERT INTO t VALUES (1, -9, {big_u}, 1.5), (2, 0, 0, 1e300)",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        rows = _by_id(client, vid)
        assert rows[1]["it"] == "-9"
        # The unsigned column's value is above 2^63, so a signed reading of the
        # register would print a negative number.
        assert rows[1]["ut"] == str(big_u)
        assert rows[1]["ft"] == "1.5"
        # Outside [1e-4, 1e15) the float form switches to scientific notation,
        # which is what keeps the output bounded.
        assert rows[2]["ft"] == "1e300"

    def test_round_trip_through_text(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, CAST(CAST(i AS TEXT) AS BIGINT) AS back FROM t",
            schema_name=sn,
        )
        vals = [-(2**63), -1, 0, 1, 2**63 - 1]
        rows_sql = ", ".join(f"({n}, {v})" for n, v in enumerate(vals, start=1))
        client.execute_sql(f"INSERT INTO t VALUES {rows_sql}", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        rows = _by_id(client, vid)
        for n, v in enumerate(vals, start=1):
            assert rows[n]["back"] == v


# ---------------------------------------------------------------------------
# Views over views, ad-hoc reads, and point DML
# ---------------------------------------------------------------------------


class TestSurfaces:
    def test_a_view_reads_another_view_s_computed_string(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v1 AS SELECT id, UPPER(s) AS u FROM t", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v2 AS SELECT id, LENGTH(u) AS n, LOWER(u) AS back FROM v1",
            schema_name=sn,
        )
        client.execute_sql(f"INSERT INTO t VALUES {_corpus_values()}", schema_name=sn)
        rows = _by_id(client, client.resolve_table(sn, "v2")[0])
        assert rows[1]["back"] == "abc" and rows[1]["n"] == 3
        assert rows[3]["back"] == "thirteen-char" and rows[3]["n"] == 13

    def test_adhoc_select_computes_strings(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 'abc'), (2, 'a-long-value-here')", schema_name=sn)
        rows = _adhoc(client, sn, "SELECT id, UPPER(s) AS u, LENGTH(s) AS n FROM t ORDER BY id")
        assert [(r.id, r.u, r.n) for r in rows] == [
            (1, "ABC", 3),
            (2, "A-LONG-VALUE-HERE", 17),
        ]
        got = _adhoc(client, sn, "SELECT id FROM t WHERE UPPER(s) = 'ABC'")
        assert [r.id for r in got] == [1]

    def test_point_dml_over_string_expressions(self, client, schema_name):
        """The residual filter and the SET right-hand side both run through the
        shared evaluator, client-side — the same code the engine runs."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT id, UPPER(s) AS u FROM t", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, '  padded  '), (2, 'keep'), (3, 'drop-me-please')",
            schema_name=sn,
        )
        client.execute_sql("UPDATE t SET s = TRIM(s) WHERE id = 1", schema_name=sn)
        client.execute_sql("UPDATE t SET s = CONCAT(s, '!') WHERE id = 2", schema_name=sn)
        client.execute_sql("DELETE FROM t WHERE UPPER(s) = 'DROP-ME-PLEASE'", schema_name=sn)

        rows = _by_id(client, client.resolve_table(sn, "v")[0])
        assert set(rows) == {1, 2}
        assert rows[1]["u"] == "PADDED"
        assert rows[2]["u"] == "KEEP!"

        got = _adhoc(client, sn, "SELECT id, s FROM t ORDER BY id")
        assert [(r.id, r.s) for r in got] == [(1, "padded"), (2, "keep!")]


# ---------------------------------------------------------------------------
# Rejections
# ---------------------------------------------------------------------------


class TestRejections:
    def test_strings_in_numeric_positions_are_rejected_at_plan_time(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, k BIGINT NOT NULL)",
            schema_name=sn,
        )
        # Each is rejected while planning, naming the string operand — not
        # accepted and then failing inside the engine's class validator, which
        # would surface as an opaque internal enum.
        for i, expr in enumerate(
            [
                "s + 1",
                "ABS(s)",
                "ROUND(s)",
                "GREATEST(s, 'x')",
                "-s",
                "s || 1",
                "s = k",
                "s AND k",
            ]
        ):
            _rejects(
                client,
                sn,
                f"CREATE VIEW bad{i} AS SELECT id, {expr} AS x FROM t",
                "string",
            )

    def test_a_non_literal_trim_set_is_rejected(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
            schema_name=sn,
        )
        _rejects(
            client,
            sn,
            "CREATE VIEW bad AS SELECT id, TRIM(s FROM s) AS x FROM t",
            "ASCII string literal",
        )

    def test_a_string_valued_set_against_an_integer_column_is_rejected(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL, k BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 'abc', 1)", schema_name=sn)
        _rejects(client, sn, "UPDATE t SET k = UPPER(s)", "cannot assign")
