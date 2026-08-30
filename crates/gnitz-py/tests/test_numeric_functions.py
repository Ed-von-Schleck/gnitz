"""E2E: numeric scalar functions (ABS/FLOOR/CEIL/ROUND/TRUNC/MOD/GREATEST/LEAST)
and numeric CAST.

Every function here is a pure per-row value transform — a linear map in DBSP
terms — so the load-bearing property is not just the value but that a retraction
re-running the same expression over the same payload cancels byte-exactly.

Run:
    cd crates/gnitz-py && GNITZ_WORKERS=4 uv run pytest tests/test_numeric_functions.py -v --tb=short
"""

import pytest
import gnitz
from _uid import uid as _uid

U64_HIGH = 2**63 + 5  # crosses the signed boundary: negative read as an i64




def _cleanup(client, sn):
    # drop_schema cascades: it drains every view, then every table, then the
    # schema row. Naming the members again only adds failing round-trips.
    try:
        client.drop_schema(sn)
    except Exception:
        pass


@pytest.fixture
def schema_name(client):
    sn = "n" + _uid()
    client.create_schema(sn)
    yield sn
    _cleanup(client, sn)


def _dicts(client, vid):
    """Current (positive-state) view rows as dicts."""
    return client.scan(vid).mappings()


def _rejects(client, sn, sql, want):
    """The statement must fail, and fail for the stated reason — a bare
    "raises" would also pass on a typo or a duplicate view name."""
    with pytest.raises(Exception) as ei:
        client.execute_sql(sql, schema_name=sn)
    assert want in str(ei.value), f"{sql!r} failed for the wrong reason: {ei.value}"


def _adhoc(client, sn, sql):
    """Rows of an ad-hoc SELECT, as attribute-bearing row objects."""
    res = client.execute_sql(sql, schema_name=sn)[0]
    assert res["type"] == "Rows", res
    return list(res["rows"])


# ---------------------------------------------------------------------------
# Projections: values, NULL propagation, and retraction cancellation
# ---------------------------------------------------------------------------


class TestProjection:
    def test_every_function_over_a_nullable_column(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT, f DOUBLE)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, "
            "ABS(i) AS ai, ABS(f) AS af, "
            "FLOOR(f) AS ff, CEIL(f) AS cf, CEILING(f) AS cgf, "
            "ROUND(f) AS rf, TRUNC(f) AS tf, "
            "MOD(i, 3) AS mi, "
            "GREATEST(i, 0) AS gi, LEAST(i, 0) AS li "
            "FROM t",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, -7, -2.5), (2, 7, 2.5), (3, NULL, NULL)",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        rows = {r["id"]: r for r in _dicts(client, vid)}

        assert rows[1]["ai"] == 7 and rows[2]["ai"] == 7
        assert rows[1]["af"] == pytest.approx(2.5)
        assert rows[1]["ff"] == pytest.approx(-3.0)
        assert rows[1]["cf"] == pytest.approx(-2.0)
        assert rows[1]["cgf"] == pytest.approx(-2.0)
        assert rows[1]["tf"] == pytest.approx(-2.0)  # toward zero, unlike FLOOR
        # round_ties_even: 2.5 -> 2, -2.5 -> -2 (not away from zero)
        assert rows[1]["rf"] == pytest.approx(-2.0)
        assert rows[2]["rf"] == pytest.approx(2.0)
        assert rows[1]["mi"] == -1 and rows[2]["mi"] == 1
        assert rows[1]["gi"] == 0 and rows[1]["li"] == -7
        assert rows[2]["gi"] == 7 and rows[2]["li"] == 0

        # NULL in, NULL out for every function that propagates.
        for c in ("ai", "af", "ff", "cf", "cgf", "rf", "tf", "mi"):
            assert rows[3][c] is None, c
        # GREATEST/LEAST skip a NULL operand instead of propagating it.
        assert rows[3]["gi"] == 0 and rows[3]["li"] == 0

    def test_update_and_delete_cancel_the_computed_row(self, client, schema_name):
        """The retraction-determinism proof for the whole opcode set: an UPDATE
        is a retract + insert, so the old computed row must cancel to weight
        zero and vanish — which it only does if the expression is bit-stable."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, ABS(i) AS ai, ROUND(f, 2) AS rf, "
            "GREATEST(i, 10, -3) AS g, CAST(f AS INT) AS ci FROM t",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, -5, 1.005), (2, 3, 9.9)", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        rows = {r["id"]: r for r in _dicts(client, vid)}
        assert len(rows) == 2
        assert rows[1]["ai"] == 5 and rows[1]["g"] == 10 and rows[1]["ci"] == 1
        assert rows[2]["ai"] == 3 and rows[2]["g"] == 10 and rows[2]["ci"] == 9

        # An UPDATE retracts and re-inserts the whole row, so every computed
        # column is recomputed — including the two derived from the untouched
        # float column, whose old values must cancel bit-exactly.
        client.execute_sql("UPDATE t SET i = 42 WHERE id = 1", schema_name=sn)
        rows = {r["id"]: r for r in _dicts(client, vid)}
        assert len(rows) == 2, "the pre-update row must have cancelled, not accumulated"
        assert rows[1]["ai"] == 42 and rows[1]["g"] == 42 and rows[1]["ci"] == 1

        client.execute_sql("DELETE FROM t", schema_name=sn)
        assert _dicts(client, vid) == []

    def test_integer_arguments_keep_their_type_and_precision(self, client, schema_name):
        """ROUND/FLOOR/CEIL/TRUNC of an integer fold away entirely. The fold is
        for correctness, not economy: an f64 lift would mangle any |x| >= 2^53,
        and rounding an integer to more decimal places is the identity."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, ROUND(i, 2) AS r2, ROUND(i) AS r, "
            "FLOOR(i) AS fl, TRUNC(i) AS tr, ABS(i) AS a FROM t",
            schema_name=sn,
        )
        big = 9007199254740993  # 2^53 + 1: not representable in f64
        client.execute_sql(f"INSERT INTO t VALUES (1, {big})", schema_name=sn)
        vid, vschema = client.resolve_table(sn, "v")
        types = {c.name: c.type_code for c in vschema.columns}
        assert types["r2"] == gnitz.TypeCode.I64
        row = _dicts(client, vid)[0]
        for c in ("r2", "r", "fl", "tr", "a"):
            assert row[c] == big, f"{c} lost precision"


# ---------------------------------------------------------------------------
# ROUND's committed semantics
# ---------------------------------------------------------------------------


class TestRound:
    def test_scaled_rounding(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, ROUND(f, 2) AS r2, ROUND(f, -5) AS rm5, "
            "ROUND(f, 0) AS r0 FROM t",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, 2.665), (2, 2.675), (3, 881469.0444980001)",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        rows = {r["id"]: r for r in _dicts(client, vid)}
        # Half-to-even at the scaled value, at every scale. 2.665 * 100 is an
        # exact tie and rounds DOWN to even; 2.675 * 100 rounds up to exactly
        # 267.5 first, which then ties to even (268) — the same one rule.
        assert rows[1]["r2"] == pytest.approx(2.66)
        assert rows[2]["r2"] == pytest.approx(2.68)
        assert rows[3]["rm5"] == pytest.approx(900000.0)
        # n == 0 IS the 1-arg ROUND, so it carries the same tie rule.
        assert rows[3]["r0"] == pytest.approx(881469.0)

    def test_ties_go_to_even(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT id, ROUND(f) AS r FROM t", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 2.5), (2, 3.5), (3, -2.5), (4, 0.5)",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        got = {r["id"]: r["r"] for r in _dicts(client, vid)}
        assert got[1] == pytest.approx(2.0)
        assert got[2] == pytest.approx(4.0)
        assert got[3] == pytest.approx(-2.0)
        assert got[4] == pytest.approx(0.0)

    def test_scale_literal_is_validated_at_create_time(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        for bad in ("ROUND(f, 16)", "ROUND(f, -16)", "ROUND(f, 2.5)", "ROUND(f, id)"):
            _rejects(
                client,
                sn,
                f"CREATE VIEW bad AS SELECT id, {bad} AS r FROM t",
                "scale must be an integer literal",
            )
        # Control: the boundary scales are accepted.
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, ROUND(f, 15) AS a, ROUND(f, -15) AS b FROM t",
            schema_name=sn,
        )


# ---------------------------------------------------------------------------
# MOD: the total IntMod opcode, not Rust's `%`
# ---------------------------------------------------------------------------


class TestMod:
    def test_zero_divisor_and_the_wrapping_boundary(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, b BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT id, MOD(a, b) AS m FROM t", schema_name=sn)
        client.execute_sql(
            f"INSERT INTO t VALUES (1, 7, 3), (2, 7, 0), (3, {-(2**63)}, -1)",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        got = {r["id"]: r["m"] for r in _dicts(client, vid)}
        assert got[1] == 1
        assert got[2] is None, "a zero divisor NULLs the row rather than faulting"
        # wrapping_rem: Rust's `%` would panic on i64::MIN % -1.
        assert got[3] == 0

    def test_float_mod_is_rejected(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        _rejects(
            client,
            sn,
            "CREATE VIEW bad AS SELECT id, MOD(f, 2) AS m FROM t",
            "modulo",
        )


# ---------------------------------------------------------------------------
# GREATEST / LEAST
# ---------------------------------------------------------------------------


class TestGreatestLeast:
    def test_null_skipping_and_computed_args(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT, b BIGINT, c BIGINT)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, GREATEST(a, b, c) AS g, LEAST(a, b, c) AS l, "
            "GREATEST(a + 1, b * 2) AS gc, GREATEST(a, NULL) AS gn FROM t",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, 1, 5, 3), (2, NULL, 5, 3), (3, NULL, NULL, NULL), (4, -9, NULL, -2)",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        rows = {r["id"]: r for r in _dicts(client, vid)}
        assert rows[1]["g"] == 5 and rows[1]["l"] == 1
        assert rows[2]["g"] == 5 and rows[2]["l"] == 3, "a NULL argument is skipped"
        assert rows[3]["g"] is None and rows[3]["l"] is None, "NULL only when every arg is"
        assert rows[4]["g"] == -2 and rows[4]["l"] == -9
        assert rows[1]["gc"] == 10
        assert rows[1]["gn"] == 1, "a NULL literal argument is skipped like any other"

    def test_mixed_u64_list_is_argument_order_independent(self, client, schema_name):
        """The engine's unsigned taint only exists from the first U64 operand
        onward, so an in-SQL-order fold would compare the early signed pairs
        signed. One U64 argument is rotated to the head of the fold to fix the
        compare domain for the whole fold."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, u BIGINT UNSIGNED NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, GREATEST(-1, 1, u) AS a, GREATEST(u, -1, 1) AS b FROM t",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 5)", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        row = _dicts(client, vid)[0]
        assert row["a"] == row["b"], "the written argument order must not decide the result"

    def test_float_extremum(self, client, schema_name):
        """A float argument list folds through the float opcodes, which order by
        `total_cmp` — the same total order rows sort by. (The NaN and signed-zero
        corners of that order are pinned in the engine's own kernel tests; SQL
        has no literal for either.)"""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, GREATEST(f, 5.0) AS g, LEAST(f, 5.0) AS l FROM t",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 2.0), (2, 9.0)", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        got = {r["id"]: (r["g"], r["l"]) for r in _dicts(client, vid)}
        assert got[1] == (pytest.approx(5.0), pytest.approx(2.0))
        assert got[2] == (pytest.approx(9.0), pytest.approx(5.0))

    def test_non_numeric_argument_is_rejected(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, s TEXT NOT NULL)",
            schema_name=sn,
        )
        # Every argument is read through the numeric operand helper, which names
        # the column and the surfaces a string value does have.
        _rejects(
            client,
            sn,
            "CREATE VIEW bad AS SELECT id, GREATEST(s, s) AS g FROM t",
            'column "s" is a string',
        )


# ---------------------------------------------------------------------------
# CAST
# ---------------------------------------------------------------------------


class TestCast:
    def test_computed_column_type_is_the_targets_register_image(self, client, schema_name):
        """EMIT stores a whole 8-byte register into an 8-byte slot, so a narrow
        target produces a wide column carrying the range-checked value."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, CAST(i AS SMALLINT) AS s, CAST(i AS BIGINT UNSIGNED) AS u, "
            "CAST(f AS FLOAT) AS ff, CAST(i AS DOUBLE) AS d FROM t",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 300, 0.1)", schema_name=sn)
        vid, vschema = client.resolve_table(sn, "v")
        types = {c.name: c.type_code for c in vschema.columns}
        assert types["s"] == gnitz.TypeCode.I64
        assert types["u"] == gnitz.TypeCode.U64
        assert types["ff"] == gnitz.TypeCode.F64
        assert types["d"] == gnitz.TypeCode.F64
        row = _dicts(client, vid)[0]
        assert row["s"] == 300 and row["u"] == 300

    def test_out_of_range_is_a_per_row_null(self, client, schema_name):
        """No fault-delivery mechanism exists at row granularity, so a failed
        cast is a NULL — including for a literal, which is not a CREATE-time
        error either."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, CAST(i AS TINYINT) AS b, CAST(300 AS TINYINT) AS lit, "
            "CAST(i AS BIGINT UNSIGNED) AS u FROM t",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 100), (2, 300), (3, -1)", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        rows = {r["id"]: r for r in _dicts(client, vid)}
        assert len(rows) == 3, "a failed cast NULLs the value, it does not drop the row"
        assert rows[1]["b"] == 100
        assert rows[2]["b"] is None
        assert rows[3]["b"] == -1
        assert rows[3]["u"] is None, "-1 has no BIGINT UNSIGNED image"
        assert all(r["lit"] is None for r in rows.values())

    def test_narrowing_a_u64_column_blanks_half_the_domain(self, client, schema_name):
        """The recorded risk, asserted rather than implied: a narrowing cast in
        a materialized view can blank a large fraction of a column with no error
        and no row-count change."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, u BIGINT UNSIGNED NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, u, CAST(u AS BIGINT) AS s FROM t", schema_name=sn
        )
        client.execute_sql(f"INSERT INTO t VALUES (1, 5), (2, {U64_HIGH})", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        rows = {r["id"]: r for r in _dicts(client, vid)}
        assert len(rows) == 2, "the row count is unchanged"
        assert rows[1]["s"] == 5
        assert rows[2]["s"] is None, "a value >= 2^63 has no BIGINT image"
        assert rows[2]["u"] == U64_HIGH, "the source column is untouched"

    def test_cast_to_unsigned_reseeds_the_compare_domain(self, client, schema_name):
        """The elision regression, end to end: eliding `u32 -> BIGINT UNSIGNED`
        would leave the register signed while the column is declared U64, and
        `> -1` would then answer TRUE for the U32 source and FALSE for the U64
        one."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, "
            "w INT UNSIGNED NOT NULL, u BIGINT UNSIGNED NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id FROM t WHERE CAST(w AS BIGINT UNSIGNED) > -1",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v2 AS SELECT id FROM t WHERE CAST(u AS BIGINT UNSIGNED) > -1",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 7, 7)", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        v2id = client.resolve_table(sn, "v2")[0]
        assert _dicts(client, vid) == [], "-1 reads as 2^64-1 in the unsigned domain"
        assert _dicts(client, v2id) == []
        # Control: the same predicate against 0 does match, so the empty results
        # above are the compare domain and not a broken view.
        assert _adhoc(client, sn, "SELECT id FROM t WHERE CAST(w AS BIGINT UNSIGNED) > 0")

    def test_float_to_int_truncates_toward_zero(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT id, CAST(f AS INT) AS i FROM t", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 2.7), (2, -2.7), (3, 1e300)", schema_name=sn
        )
        vid = client.resolve_table(sn, "v")[0]
        got = {r["id"]: r["i"] for r in _dicts(client, vid)}
        assert got[1] == 2 and got[2] == -2
        assert got[3] is None, "out of the target's domain"

    def test_cast_to_float_rounds_through_f32(self, client, schema_name):
        """Only a finite source whose rounded result is not finite is a NULL.
        Everything else the rounding does is precision loss and passes through —
        underflow to zero included."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT id, CAST(f AS FLOAT) AS g FROM t", schema_name=sn)
        client.execute_sql(
            "INSERT INTO t VALUES (1, 0.1), (2, 1e300), (3, 1e-300)", schema_name=sn
        )
        vid = client.resolve_table(sn, "v")[0]
        got = {r["id"]: r["g"] for r in _dicts(client, vid)}
        import struct

        f32_of_point_one = struct.unpack("f", struct.pack("f", 0.1))[0]
        assert got[1] == pytest.approx(f32_of_point_one)
        assert got[1] != 0.1, "the value really went through f32 precision"
        assert got[2] is None, "finite overflow of f32's range"
        assert got[3] == 0.0, "underflow flushes to zero, it is not a domain error"

    def test_unsupported_targets_and_sources_reject_at_create_time(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL)",
            schema_name=sn,
        )
        for expr, why in (
            ("CAST(i AS UUID)", "not supported"),
            ("CAST(i AS BOOLEAN)", "BOOLEAN"),
            # A wide integer literal has no register slot at all — the general
            # limitation of the 8-byte register file, not something CAST adds.
            ("CAST(18446744073709551615 AS BIGINT UNSIGNED)", "18446744073709551615"),
        ):
            _rejects(
                client, sn, f"CREATE VIEW bad AS SELECT id, {expr} AS c FROM t", why
            )

    def test_every_cast_spelling_works(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id, CAST(i AS INT) AS a, i::INT AS b, "
            "TRY_CAST(i AS INT) AS c, SAFE_CAST(i AS INT) AS d FROM t",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, 5)", schema_name=sn)
        vid = client.resolve_table(sn, "v")[0]
        row = _dicts(client, vid)[0]
        assert row["a"] == row["b"] == row["c"] == row["d"] == 5


# ---------------------------------------------------------------------------
# Predicates, aggregates, ad-hoc reads and DML — the surfaces the one shared
# evaluator reaches the moment the opcode exists.
# ---------------------------------------------------------------------------


class TestOtherSurfaces:
    def test_functions_in_a_view_where(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, a BIGINT NOT NULL, "
            "b BIGINT NOT NULL, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("CREATE VIEW v AS SELECT id FROM t WHERE ABS(a) > 5", schema_name=sn)
        client.execute_sql(
            "CREATE VIEW v2 AS SELECT id FROM t WHERE GREATEST(a, b) >= 10", schema_name=sn
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, -9, 1, 2.7), (2, 3, 11, 9.9), (3, 1, 1, 0.1)",
            schema_name=sn,
        )
        assert sorted(r["id"] for r in _dicts(client, client.resolve_table(sn, "v")[0])) == [1]
        assert sorted(r["id"] for r in _dicts(client, client.resolve_table(sn, "v2")[0])) == [2]

    def test_cast_in_a_where_excludes_the_failed_rows(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT id FROM t WHERE CAST(f AS INT) = 2", schema_name=sn
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, 2.7), (2, 1e300), (3, 5.0)", schema_name=sn
        )
        vid = client.resolve_table(sn, "v")[0]
        got = sorted(r["id"] for r in _dicts(client, vid))
        assert got == [1], "the NULL-cast row compares NULL and is excluded"

    def test_scalar_wrapper_over_an_aggregate_in_having(self, client, schema_name):
        """The view path and the ad-hoc path must agree: the scalar wrapper is
        consumed above the leaf on both, and its aggregate resolves through the
        leaf on the recursion."""
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, g BIGINT NOT NULL, val BIGINT NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql(
            "INSERT INTO t VALUES (1, 1, -20), (2, 1, -5), (3, 2, 4), (4, 2, 1)",
            schema_name=sn,
        )
        client.execute_sql(
            "CREATE VIEW v AS SELECT g, SUM(val) AS s FROM t GROUP BY g HAVING ABS(SUM(val)) > 10",
            schema_name=sn,
        )
        vid = client.resolve_table(sn, "v")[0]
        view_rows = sorted((r["g"], r["s"]) for r in _dicts(client, vid))
        assert view_rows == [(1, -25)]

        adhoc = _adhoc(
            client,
            sn,
            "SELECT g, SUM(val) AS s FROM t GROUP BY g HAVING ABS(SUM(val)) > 10",
        )
        assert sorted((r.g, r.s) for r in adhoc) == view_rows

    def test_adhoc_select_runs_the_same_expressions(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, i BIGINT NOT NULL, f DOUBLE NOT NULL)",
            schema_name=sn,
        )
        client.execute_sql("INSERT INTO t VALUES (1, -7, 2.5)", schema_name=sn)
        row = _adhoc(
            client,
            sn,
            "SELECT ABS(i) AS a, FLOOR(f) AS fl, ROUND(f) AS r, "
            "GREATEST(i, 0) AS g, CAST(f AS INT) AS c FROM t WHERE MOD(i, 7) = 0",
        )[0]
        assert row.a == 7
        assert row.fl == pytest.approx(2.0)
        assert row.r == pytest.approx(2.0)
        assert row.g == 0
        assert row.c == 2

    def test_point_dml_uses_the_functions(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, n BIGINT NOT NULL)",
            schema_name=sn,
        )
        tid = client.resolve_table(sn, "t")[0]
        client.execute_sql(
            "INSERT INTO t VALUES (1, -5), (2, -6), (3, 7), (4, 8)", schema_name=sn
        )
        client.execute_sql("UPDATE t SET n = ABS(n) WHERE id = 1", schema_name=sn)
        rows = {r["id"]: r["n"] for r in _dicts(client, tid)}
        assert rows[1] == 5

        client.execute_sql("DELETE FROM t WHERE MOD(n, 2) = 0", schema_name=sn)
        rows = {r["id"]: r["n"] for r in _dicts(client, tid)}
        assert sorted(rows) == [1, 3], "the even-valued rows are gone"

    def test_cast_residual_on_the_scan_fallback(self, client, schema_name):
        sn = schema_name
        client.execute_sql(
            "CREATE TABLE t (id BIGINT NOT NULL PRIMARY KEY, u BIGINT UNSIGNED NOT NULL)",
            schema_name=sn,
        )
        tid = client.resolve_table(sn, "t")[0]
        client.execute_sql(f"INSERT INTO t VALUES (1, 5), (2, {U64_HIGH})", schema_name=sn)
        # The high row's cast is NULL, so its predicate is NULL and it survives.
        client.execute_sql("DELETE FROM t WHERE CAST(u AS SMALLINT) = 5", schema_name=sn)
        assert sorted(r["id"] for r in _dicts(client, tid)) == [2]
