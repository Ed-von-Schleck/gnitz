# expr_test.py
#
# RPython-compiled test suite for the expression bytecode VM.

import sys
import os

from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.longlong2float import float2longlong, longlong2float

from gnitz.core import types, batch
from gnitz.core.batch import RowBuilder
from gnitz.dbsp.ops import linear
from gnitz.dbsp.expr import ExprBuilder, ExprMapFunction, eval_expr


# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------

def log(msg):
    os.write(1, msg + "\n")

def fail(msg):
    os.write(2, "CRITICAL FAILURE: " + msg + "\n")
    raise Exception(msg)

def assert_true(condition, msg):
    if not condition:
        fail(msg)

def assert_equal_i(expected, actual, msg):
    if expected != actual:
        fail(msg + " (Expected " + str(expected) + ", got " + str(actual) + ")")

def assert_equal_i64(expected, actual, msg):
    if expected != actual:
        fail(msg + " (i64 mismatch)")


def _make_int_schema():
    """Schema: pk(U64), col1(I64), col2(I64)."""
    cols = newlist_hint(3)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="col1"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="col2"))
    return types.TableSchema(cols, 0)


def _make_float_schema():
    """Schema: pk(U64), col1(F64)."""
    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_F64, name="col1"))
    return types.TableSchema(cols, 0)


def _make_nullable_schema():
    """Schema: pk(U64), col1(I64 nullable)."""
    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="col1", is_nullable=True))
    return types.TableSchema(cols, 0)


def _make_multi_schema():
    """Schema: pk(U64), col_a(I64), col_b(I64), col_c(I64)."""
    cols = newlist_hint(4)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="col_a"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="col_b"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="col_c"))
    return types.TableSchema(cols, 0)


# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------


def test_int_comparisons():
    log("[EXPR] Testing integer comparisons...")
    schema = _make_int_schema()

    # Batch: rows with col1 values 10, 20, 30, 40, 50
    b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)
    try:
        rb = RowBuilder(schema, b)
        for i in range(5):
            val = (i + 1) * 10
            rb.begin(r_uint128(i + 1), r_int64(1))
            rb.put_int(r_int64(val))
            rb.put_int(r_int64(0))
            rb.commit()

        # col1 > 25 => rows with 30, 40, 50
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        k = eb.load_const_int(25)
        r = eb.cmp_gt(c, k)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(3, b_out.length(), "GT filter")

        # col1 == 20 => 1 row
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        k = eb.load_const_int(20)
        r = eb.cmp_eq(c, k)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(1, b_out.length(), "EQ filter")

        # col1 != 30 => 4 rows
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        k = eb.load_const_int(30)
        r = eb.cmp_ne(c, k)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(4, b_out.length(), "NE filter")

        # col1 <= 30 => rows with 10, 20, 30
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        k = eb.load_const_int(30)
        r = eb.cmp_le(c, k)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(3, b_out.length(), "LE filter")

        # col1 >= 30 => rows with 30, 40, 50
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        k = eb.load_const_int(30)
        r = eb.cmp_ge(c, k)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(3, b_out.length(), "GE filter")

        # col1 < 30 => rows with 10, 20
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        k = eb.load_const_int(30)
        r = eb.cmp_lt(c, k)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(2, b_out.length(), "LT filter")

        log("  PASSED")
    finally:
        b.free()
        b_out.free()


def test_float_comparisons():
    log("[EXPR] Testing float comparisons...")
    schema = _make_float_schema()

    b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)
    try:
        rb = RowBuilder(schema, b)
        # rows with col1 values 1.5, 2.5, 3.5
        for i in range(3):
            rb.begin(r_uint128(i + 1), r_int64(1))
            rb.put_float(1.5 + float(i))
            rb.commit()

        # col1 > 2.0 => rows with 2.5, 3.5
        eb = ExprBuilder()
        c = eb.load_col_float(1)
        k = eb.load_const_float(2.0)
        r = eb.fcmp_gt(c, k)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(2, b_out.length(), "FCMP_GT filter")

        # col1 == 2.5 => 1 row
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_float(1)
        k = eb.load_const_float(2.5)
        r = eb.fcmp_eq(c, k)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(1, b_out.length(), "FCMP_EQ filter")

        # col1 <= 2.5 => rows with 1.5, 2.5
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_float(1)
        k = eb.load_const_float(2.5)
        r = eb.fcmp_le(c, k)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(2, b_out.length(), "FCMP_LE filter")

        log("  PASSED")
    finally:
        b.free()
        b_out.free()


def test_int_arithmetic():
    log("[EXPR] Testing integer arithmetic...")
    schema = _make_int_schema()

    b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)
    try:
        rb = RowBuilder(schema, b)
        # rows with col1 = 40, 50, 60
        for i in range(3):
            val = 40 + i * 10
            rb.begin(r_uint128(i + 1), r_int64(1))
            rb.put_int(r_int64(val))
            rb.put_int(r_int64(0))
            rb.commit()

        # (col1 + 10) > 55 => 50+10=60>55 yes, 60+10=70>55 yes => 2 rows
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        k10 = eb.load_const_int(10)
        s = eb.int_add(c, k10)
        k55 = eb.load_const_int(55)
        r = eb.cmp_gt(s, k55)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(2, b_out.length(), "INT_ADD + CMP_GT")

        # (col1 * 2) == 100 => 50*2=100 => 1 row
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        k2 = eb.load_const_int(2)
        m = eb.int_mul(c, k2)
        k100 = eb.load_const_int(100)
        r = eb.cmp_eq(m, k100)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(1, b_out.length(), "INT_MUL + CMP_EQ")

        # col1 - 40 == 0 => first row only
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        k40 = eb.load_const_int(40)
        d = eb.int_sub(c, k40)
        k0 = eb.load_const_int(0)
        r = eb.cmp_eq(d, k0)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(1, b_out.length(), "INT_SUB + CMP_EQ")

        # -col1 < -45 => col1 > 45 => 50, 60 => 2 rows
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        n = eb.int_neg(c)
        km45 = eb.load_const_int(-45)
        r = eb.cmp_lt(n, km45)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(2, b_out.length(), "INT_NEG + CMP_LT")

        log("  PASSED")
    finally:
        b.free()
        b_out.free()


def test_float_arithmetic():
    log("[EXPR] Testing float arithmetic...")
    schema = _make_float_schema()

    b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)
    try:
        rb = RowBuilder(schema, b)
        # rows with col1 = 1.0, 2.0, 3.0
        for i in range(3):
            rb.begin(r_uint128(i + 1), r_int64(1))
            rb.put_float(float(i + 1))
            rb.commit()

        # (col1 + 0.5) > 2.0 => 2.5>2 yes, 3.5>2 yes => 2 rows
        eb = ExprBuilder()
        c = eb.load_col_float(1)
        k = eb.load_const_float(0.5)
        s = eb.float_add(c, k)
        k2 = eb.load_const_float(2.0)
        r = eb.fcmp_gt(s, k2)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(2, b_out.length(), "FLOAT_ADD + FCMP_GT")

        # col1 * 2.0 == 4.0 => 2.0*2=4.0 => 1 row
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_float(1)
        k2 = eb.load_const_float(2.0)
        m = eb.float_mul(c, k2)
        k4 = eb.load_const_float(4.0)
        r = eb.fcmp_eq(m, k4)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(1, b_out.length(), "FLOAT_MUL + FCMP_EQ")

        # col1 - 1.0 > 0.5 => 2.0-1=1>0.5 yes, 3.0-1=2>0.5 yes => 2 rows
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_float(1)
        k1 = eb.load_const_float(1.0)
        d = eb.float_sub(c, k1)
        k05 = eb.load_const_float(0.5)
        r = eb.fcmp_gt(d, k05)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(2, b_out.length(), "FLOAT_SUB + FCMP_GT")

        # -col1 < -1.5 => col1 > 1.5 => 2.0, 3.0 => 2 rows
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_float(1)
        n = eb.float_neg(c)
        km = eb.load_const_float(-1.5)
        r = eb.fcmp_lt(n, km)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(2, b_out.length(), "FLOAT_NEG + FCMP_LT")

        log("  PASSED")
    finally:
        b.free()
        b_out.free()


def test_boolean_combinators():
    log("[EXPR] Testing boolean combinators...")
    schema = _make_int_schema()

    b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)
    try:
        rb = RowBuilder(schema, b)
        # col1 values: 5, 10, 50, 100
        for val in [5, 10, 50, 100]:
            rb.begin(r_uint128(val), r_int64(1))
            rb.put_int(r_int64(val))
            rb.put_int(r_int64(0))
            rb.commit()

        # (col1 > 10) AND (col1 < 100) => 50
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        k10 = eb.load_const_int(10)
        gt = eb.cmp_gt(c, k10)
        k100 = eb.load_const_int(100)
        lt = eb.cmp_lt(c, k100)
        r = eb.bool_and(gt, lt)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(1, b_out.length(), "AND combinator")
        acc = b_out.get_accessor(0)
        assert_equal_i64(r_int64(50), acc.get_int_signed(1), "AND result value")

        # (col1 == 5) OR (col1 == 10) => 2 rows
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        k5 = eb.load_const_int(5)
        eq5 = eb.cmp_eq(c, k5)
        k10 = eb.load_const_int(10)
        eq10 = eb.cmp_eq(c, k10)
        r = eb.bool_or(eq5, eq10)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(2, b_out.length(), "OR combinator")

        # NOT (col1 > 50) => 5, 10, 50
        b_out.clear()
        eb = ExprBuilder()
        c = eb.load_col_int(1)
        k50 = eb.load_const_int(50)
        gt = eb.cmp_gt(c, k50)
        r = eb.bool_not(gt)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(3, b_out.length(), "NOT combinator")

        log("  PASSED")
    finally:
        b.free()
        b_out.free()


def test_null_handling():
    log("[EXPR] Testing null handling...")
    schema = _make_nullable_schema()

    b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)
    try:
        rb = RowBuilder(schema, b)
        # Row 1: col1 = NULL
        rb.begin(r_uint128(1), r_int64(1))
        rb.put_null()
        rb.commit()
        # Row 2: col1 = 42
        rb.begin(r_uint128(2), r_int64(1))
        rb.put_int(r_int64(42))
        rb.commit()
        # Row 3: col1 = NULL
        rb.begin(r_uint128(3), r_int64(1))
        rb.put_null()
        rb.commit()

        # IS_NULL(col1) => 2 rows (rows 1, 3)
        eb = ExprBuilder()
        r = eb.is_null(1)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(2, b_out.length(), "IS_NULL filter")

        # IS_NOT_NULL(col1) => 1 row (row 2)
        b_out.clear()
        eb = ExprBuilder()
        r = eb.is_not_null(1)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(1, b_out.length(), "IS_NOT_NULL filter")
        acc = b_out.get_accessor(0)
        assert_equal_i64(r_int64(42), acc.get_int_signed(1), "IS_NOT_NULL value")

        log("  PASSED")
    finally:
        b.free()
        b_out.free()


def test_div_by_zero():
    log("[EXPR] Testing division by zero...")
    schema = _make_int_schema()

    b = batch.ArenaZSetBatch(schema)
    try:
        rb = RowBuilder(schema, b)
        rb.begin(r_uint128(1), r_int64(1))
        rb.put_int(r_int64(100))
        rb.put_int(r_int64(0))  # col2 = 0
        rb.commit()

        # col1 / col2 where col2 == 0 => result should be 0
        eb = ExprBuilder()
        c1 = eb.load_col_int(1)
        c2 = eb.load_col_int(2)
        d = eb.int_div(c1, c2)
        prog = eb.build(d)
        acc = b.get_accessor(0)
        result, _null = eval_expr(prog, acc)
        assert_equal_i64(r_int64(0), result, "INT div by zero")

        # col1 % col2 where col2 == 0 => result should be 0
        eb = ExprBuilder()
        c1 = eb.load_col_int(1)
        c2 = eb.load_col_int(2)
        m = eb.int_mod(c1, c2)
        prog = eb.build(m)
        result, _null = eval_expr(prog, acc)
        assert_equal_i64(r_int64(0), result, "INT mod by zero")

        # float div by zero
        cols = newlist_hint(3)
        cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
        cols.append(types.ColumnDefinition(types.TYPE_F64, name="a"))
        cols.append(types.ColumnDefinition(types.TYPE_F64, name="b"))
        fschema = types.TableSchema(cols, 0)
        fb = batch.ArenaZSetBatch(fschema)
        try:
            frb = RowBuilder(fschema, fb)
            frb.begin(r_uint128(1), r_int64(1))
            frb.put_float(10.0)
            frb.put_float(0.0)
            frb.commit()

            eb = ExprBuilder()
            c1 = eb.load_col_float(1)
            c2 = eb.load_col_float(2)
            d = eb.float_div(c1, c2)
            prog = eb.build(d)
            facc = fb.get_accessor(0)
            result, _null = eval_expr(prog, facc)
            assert_equal_i64(r_int64(0), result, "FLOAT div by zero")
        finally:
            fb.free()

        log("  PASSED")
    finally:
        b.free()


def test_complex_predicate():
    log("[EXPR] Testing complex predicate...")
    schema = _make_multi_schema()

    b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)
    try:
        rb = RowBuilder(schema, b)
        # (col_a, col_b, col_c) tuples:
        # Row 1: (20, 50, 99)   => a>10 AND b<100 => T, c==42 => F, result T
        # Row 2: (5, 50, 42)    => a>10 => F,          c==42 => T, result T
        # Row 3: (5, 50, 99)    => a>10 => F,          c==42 => F, result F
        # Row 4: (20, 200, 42)  => a>10 AND b<100 => F, c==42 => T, result T
        data = [(20, 50, 99), (5, 50, 42), (5, 50, 99), (20, 200, 42)]
        for i in range(len(data)):
            a, bb, c = data[i]
            rb.begin(r_uint128(i + 1), r_int64(1))
            rb.put_int(r_int64(a))
            rb.put_int(r_int64(bb))
            rb.put_int(r_int64(c))
            rb.commit()

        # (col_a > 10 AND col_b < 100) OR col_c == 42
        eb = ExprBuilder()
        ca = eb.load_col_int(1)
        k10 = eb.load_const_int(10)
        a_gt = eb.cmp_gt(ca, k10)

        cb = eb.load_col_int(2)
        k100 = eb.load_const_int(100)
        b_lt = eb.cmp_lt(cb, k100)

        lhs = eb.bool_and(a_gt, b_lt)

        cc = eb.load_col_int(3)
        k42 = eb.load_const_int(42)
        c_eq = eb.cmp_eq(cc, k42)

        r = eb.bool_or(lhs, c_eq)
        pred = eb.build_predicate(r)
        linear.op_filter(b, b_out, pred)
        assert_equal_i(3, b_out.length(), "Complex predicate")

        log("  PASSED")
    finally:
        b.free()
        b_out.free()


def test_integration_with_op_filter():
    log("[EXPR] Testing full integration with op_filter...")
    schema = _make_int_schema()

    b = batch.ArenaZSetBatch(schema)
    b_out = batch.ArenaZSetBatch(schema)
    try:
        rb = RowBuilder(schema, b)
        # 10 rows: col1 = 0..9, col2 = 90..81
        for i in range(10):
            rb.begin(r_uint128(i + 1), r_int64(1))
            rb.put_int(r_int64(i))
            rb.put_int(r_int64(90 - i))
            rb.commit()

        # WHERE col1 >= 5 AND col2 > 83
        # col1: 0,1,2,3,4,5,6,7,8,9
        # col2: 90,89,88,87,86,85,84,83,82,81
        # col1>=5: rows 5,6,7,8,9 (col2: 85,84,83,82,81)
        # col2>83: col2=85,84 => rows 5,6
        eb = ExprBuilder()
        c1 = eb.load_col_int(1)
        k5 = eb.load_const_int(5)
        ge = eb.cmp_ge(c1, k5)
        c2 = eb.load_col_int(2)
        k83 = eb.load_const_int(83)
        gt = eb.cmp_gt(c2, k83)
        r = eb.bool_and(ge, gt)
        pred = eb.build_predicate(r)

        linear.op_filter(b, b_out, pred)
        assert_equal_i(2, b_out.length(), "Integration row count")

        # Verify actual values
        acc0 = b_out.get_accessor(0)
        assert_equal_i64(r_int64(5), acc0.get_int_signed(1), "Integration row 0 col1")
        assert_equal_i64(r_int64(85), acc0.get_int_signed(2), "Integration row 0 col2")

        acc1 = b_out.get_accessor(1)
        assert_equal_i64(r_int64(6), acc1.get_int_signed(1), "Integration row 1 col1")
        assert_equal_i64(r_int64(84), acc1.get_int_signed(2), "Integration row 1 col2")

        log("  PASSED")
    finally:
        b.free()
        b_out.free()


def test_emit_null():
    """EXPR_EMIT_NULL writes a NULL at the specified payload column."""
    log("[EXPR] Testing EMIT_NULL in eval_expr_map...")

    # Input schema: pk(U64), col1(I64), col2(I64)
    in_schema = _make_int_schema()

    # Output schema: pk(U64), a(I64), b(I64 nullable), c(I64)
    # We'll MAP: COPY_COL col1→payload0, EMIT_NULL payload1, COPY_COL col2→payload2
    out_cols = newlist_hint(4)
    out_cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    out_cols.append(types.ColumnDefinition(types.TYPE_I64, name="a"))
    out_cols.append(types.ColumnDefinition(types.TYPE_I64, name="b", is_nullable=True))
    out_cols.append(types.ColumnDefinition(types.TYPE_I64, name="c"))
    out_schema = types.TableSchema(out_cols, 0)

    b = batch.ArenaZSetBatch(in_schema)
    b_out = batch.ArenaZSetBatch(out_schema)
    try:
        rb = RowBuilder(in_schema, b)
        rb.begin(r_uint128(1), r_int64(1))
        rb.put_int(r_int64(42))   # col1
        rb.put_int(r_int64(99))   # col2
        rb.commit()

        rb.begin(r_uint128(2), r_int64(1))
        rb.put_int(r_int64(7))
        rb.put_int(r_int64(13))
        rb.commit()

        # Build map program: copy col1 → payload 0, emit NULL → payload 1, copy col2 → payload 2
        # payload_col_idx is 0-based sequential (PK is handled separately by op_map)
        eb = ExprBuilder()
        eb.copy_col(types.TYPE_I64.code, 1, 0)  # src=col1, dst=payload0
        eb.emit_null(1)                           # NULL at payload1
        eb.copy_col(types.TYPE_I64.code, 2, 2)  # src=col2, dst=payload2
        prog = eb.build(0)  # result_reg unused for map
        map_func = ExprMapFunction(prog)

        linear.op_map(b, batch.BatchWriter(b_out), map_func, out_schema)
        assert_equal_i(2, b_out.length(), "EMIT_NULL row count")

        # Row 0: a=42, b=NULL, c=99
        acc0 = b_out.get_accessor(0)
        assert_equal_i64(r_int64(42), acc0.get_int_signed(1), "Row 0 col a")
        assert_true(acc0.is_null(2), "Row 0 col b should be NULL")
        assert_equal_i64(r_int64(99), acc0.get_int_signed(3), "Row 0 col c")

        # Row 1: a=7, b=NULL, c=13
        acc1 = b_out.get_accessor(1)
        assert_equal_i64(r_int64(7), acc1.get_int_signed(1), "Row 1 col a")
        assert_true(acc1.is_null(2), "Row 1 col b should be NULL")
        assert_equal_i64(r_int64(13), acc1.get_int_signed(3), "Row 1 col c")

        log("  PASSED")
    finally:
        b.free()
        b_out.free()


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------


def entry_point(argv):
    try:
        test_int_comparisons()
        test_float_comparisons()
        test_int_arithmetic()
        test_float_arithmetic()
        test_boolean_combinators()
        test_null_handling()
        test_div_by_zero()
        test_complex_predicate()
        test_integration_with_op_filter()
        test_emit_null()
        log("\nALL EXPR TESTS PASSED")
    except Exception as e:
        os.write(2, "FAILURE\n")
        return 1

    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
