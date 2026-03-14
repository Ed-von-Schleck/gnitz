# compile_graph_test.py
#
# Tests compile_from_graph for every operator type that can be compiled today.
# Exercises the full pipeline: CircuitBuilder -> create_view -> program_cache -> execute_epoch.

import sys
import os

from rpython.rlib import rposix
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.longlong2float import float2longlong

from gnitz.core import types, batch
from gnitz.core.batch import RowBuilder
from gnitz.core.errors import LayoutError
from gnitz.catalog import engine
from gnitz.catalog.registry import ingest_to_family
from gnitz.catalog.metadata import ensure_dir
from rpython_tests.helpers.circuit_builder import CircuitBuilder


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

def _recursive_delete(path):
    if not os.path.exists(path):
        return
    if os.path.isdir(path):
        items = os.listdir(path)
        for item in items:
            _recursive_delete(path + "/" + item)
        try:
            rposix.rmdir(path)
        except OSError:
            pass
    else:
        try:
            os.unlink(path)
        except OSError:
            pass

def cleanup(path):
    if os.path.exists(path):
        _recursive_delete(path)


def _make_i64_cols(names):
    """Helper: build column defs list of (name, TYPE_I64.code) tuples for output_col_defs."""
    result = newlist_hint(len(names))
    for n in names:
        result.append((n, types.TYPE_I64.code))
    return result


def _make_table_cols_i64(col_names):
    """Create ColumnDefinition list: pk(U64) + N I64 columns."""
    cols = newlist_hint(len(col_names) + 1)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    for n in col_names:
        cols.append(types.ColumnDefinition(types.TYPE_I64, name=n))
    return cols


def _add_int_row(rb, pk, vals, weight=1):
    """Add a row: pk(U64), vals(list of ints), weight."""
    rb.begin(r_uint128(pk), r_int64(weight))
    for v in vals:
        rb.put_int(r_int64(v))
    rb.commit()


# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------


def test_passthrough(base_dir):
    """input_delta -> sink: trivial pass-through (baseline)."""
    log("[COMPILE] Testing pass-through circuit...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    table_cols = _make_table_cols_i64(["val"])
    table = db.create_table("test.src", table_cols, 0)

    builder = CircuitBuilder(view_id=0, primary_source_id=table.table_id)
    src = builder.input_delta()
    builder.sink(src, target_table_id=0)
    out_cols = _make_i64_cols(["pk", "val"])
    # Use U64 for pk in output col defs
    out_cols[0] = ("pk", types.TYPE_U64.code)
    graph = builder.build(out_cols)
    db.create_view("test.v_pass", graph, "")

    view = db.get_table("test.v_pass")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "pass-through plan is None")

    in_batch = batch.ArenaZSetBatch(table.schema)
    rb = RowBuilder(table.schema, in_batch)
    _add_int_row(rb, 1, [100])
    _add_int_row(rb, 2, [200])

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(out is not None, "pass-through produced None")
    assert_equal_i(2, out.length(), "pass-through row count")
    out.free()

    db.drop_view("test.v_pass")
    db.drop_table("test.src")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_negate(base_dir):
    """input_delta -> negate -> sink: all weights should flip sign."""
    log("[COMPILE] Testing negate circuit...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    table_cols = _make_table_cols_i64(["val"])
    table = db.create_table("test.src", table_cols, 0)

    builder = CircuitBuilder(view_id=0, primary_source_id=table.table_id)
    src = builder.input_delta()
    neg = builder.negate(src)
    builder.sink(neg, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_neg", graph, "")

    view = db.get_table("test.v_neg")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "negate plan is None")

    in_batch = batch.ArenaZSetBatch(table.schema)
    rb = RowBuilder(table.schema, in_batch)
    _add_int_row(rb, 1, [100], weight=3)

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(out is not None, "negate produced None")
    assert_equal_i(1, out.length(), "negate row count")
    assert_equal_i64(r_int64(-3), out.get_weight(0), "negate weight")
    out.free()

    db.drop_view("test.v_neg")
    db.drop_table("test.src")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_filter_passthrough(base_dir):
    """input_delta -> filter(NULL_PREDICATE) -> sink: currently a pass-through since func resolution is stubbed."""
    log("[COMPILE] Testing filter (stubbed NULL_PREDICATE) circuit...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    table_cols = _make_table_cols_i64(["val"])
    table = db.create_table("test.src", table_cols, 0)

    builder = CircuitBuilder(view_id=0, primary_source_id=table.table_id)
    src = builder.input_delta()
    flt = builder.filter(src, func_id=1)
    builder.sink(flt, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_flt", graph, "")

    view = db.get_table("test.v_flt")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "filter plan is None")

    in_batch = batch.ArenaZSetBatch(table.schema)
    rb = RowBuilder(table.schema, in_batch)
    _add_int_row(rb, 1, [10])
    _add_int_row(rb, 2, [20])
    _add_int_row(rb, 3, [30])

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    # NULL_PREDICATE passes everything through
    assert_true(out is not None, "filter produced None")
    assert_equal_i(3, out.length(), "filter (stub) should pass all rows")
    out.free()

    db.drop_view("test.v_flt")
    db.drop_table("test.src")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_union(base_dir):
    """Two input_deltas -> union -> sink: output should have rows from both sides."""
    log("[COMPILE] Testing union circuit...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    table_cols = _make_table_cols_i64(["val"])
    table_a = db.create_table("test.src_a", table_cols, 0)
    table_b = db.create_table("test.src_b", table_cols, 0)

    # Build: input_a -> union(input_a, scan(src_b)) -> sink
    builder = CircuitBuilder(view_id=0, primary_source_id=table_a.table_id)
    src_a = builder.input_delta()
    src_b = builder.trace_scan(table_b.table_id)
    u = builder.union(src_a, src_b)
    builder.sink(u, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_union", graph, "")

    # Ingest data into src_b so the trace scan has something
    b_batch = batch.ArenaZSetBatch(table_b.schema)
    rb_b = RowBuilder(table_b.schema, b_batch)
    _add_int_row(rb_b, 10, [1000])
    _add_int_row(rb_b, 11, [1100])
    ingest_to_family(table_b, b_batch)
    b_batch.free()

    view = db.get_table("test.v_union")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "union plan is None")

    # Feed 1 row as delta for src_a
    in_batch = batch.ArenaZSetBatch(table_a.schema)
    rb = RowBuilder(table_a.schema, in_batch)
    _add_int_row(rb, 1, [100])

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(out is not None, "union produced None")
    # src_a delta has 1 row, src_b trace scan has 2 rows -> 3 total
    assert_equal_i(3, out.length(), "union row count")
    out.free()

    db.drop_view("test.v_union")
    db.drop_table("test.src_b")
    db.drop_table("test.src_a")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_delay(base_dir):
    """input_delta -> delay -> sink: output should match input."""
    log("[COMPILE] Testing delay circuit...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    table_cols = _make_table_cols_i64(["val"])
    table = db.create_table("test.src", table_cols, 0)

    builder = CircuitBuilder(view_id=0, primary_source_id=table.table_id)
    src = builder.input_delta()
    d = builder.delay(src)
    builder.sink(d, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_delay", graph, "")

    view = db.get_table("test.v_delay")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "delay plan is None")

    in_batch = batch.ArenaZSetBatch(table.schema)
    rb = RowBuilder(table.schema, in_batch)
    _add_int_row(rb, 1, [42])
    _add_int_row(rb, 2, [99])

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(out is not None, "delay produced None")
    assert_equal_i(2, out.length(), "delay row count")
    out.free()

    db.drop_view("test.v_delay")
    db.drop_table("test.src")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_join_delta_trace(base_dir):
    """input_delta -> join(delta, trace_table) -> sink."""
    log("[COMPILE] Testing join_delta_trace circuit...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    cols_l = _make_table_cols_i64(["val_l"])
    table_l = db.create_table("test.left", cols_l, 0)
    cols_r = _make_table_cols_i64(["val_r"])
    table_r = db.create_table("test.right", cols_r, 0)

    # Populate right table with trace data
    r_batch = batch.ArenaZSetBatch(table_r.schema)
    rb_r = RowBuilder(table_r.schema, r_batch)
    _add_int_row(rb_r, 10, [500])
    _add_int_row(rb_r, 20, [600])
    ingest_to_family(table_r, r_batch)
    r_batch.free()

    builder = CircuitBuilder(view_id=0, primary_source_id=table_l.table_id)
    src = builder.input_delta()
    j = builder.join(src, table_r.table_id)
    builder.sink(j, target_table_id=0)
    # Join output schema: pk_l, val_l, pk_r, val_r -> merged
    out_cols = [
        ("pk_l", types.TYPE_U64.code),
        ("val_l", types.TYPE_I64.code),
        ("pk_r", types.TYPE_U64.code),
        ("val_r", types.TYPE_I64.code),
    ]
    graph = builder.build(out_cols)
    db.create_view("test.v_join", graph, "")

    view = db.get_table("test.v_join")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "join plan is None")

    # Delta with pk=10 should match right table pk=10
    in_batch = batch.ArenaZSetBatch(table_l.schema)
    rb = RowBuilder(table_l.schema, in_batch)
    _add_int_row(rb, 10, [100])

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(out is not None, "join produced None")
    assert_equal_i(1, out.length(), "join should match 1 row")
    out.free()

    db.drop_view("test.v_join")
    db.drop_table("test.right")
    db.drop_table("test.left")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_anti_join_delta_trace(base_dir):
    """input_delta -> anti_join(delta, trace_table) -> sink.
    Anti-join returns rows from delta that do NOT match in the trace."""
    log("[COMPILE] Testing anti_join_delta_trace circuit...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    cols = _make_table_cols_i64(["val"])
    table_l = db.create_table("test.left", cols, 0)
    table_r = db.create_table("test.right", cols, 0)

    # Populate right table: pk=10, pk=20
    r_batch = batch.ArenaZSetBatch(table_r.schema)
    rb_r = RowBuilder(table_r.schema, r_batch)
    _add_int_row(rb_r, 10, [500])
    _add_int_row(rb_r, 20, [600])
    ingest_to_family(table_r, r_batch)
    r_batch.free()

    builder = CircuitBuilder(view_id=0, primary_source_id=table_l.table_id)
    src = builder.input_delta()
    aj = builder.anti_join(src, table_r.table_id)
    builder.sink(aj, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_aj", graph, "")

    view = db.get_table("test.v_aj")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "anti_join plan is None")

    # Delta with pk=10 (matches), pk=30 (no match), pk=40 (no match)
    in_batch = batch.ArenaZSetBatch(table_l.schema)
    rb = RowBuilder(table_l.schema, in_batch)
    _add_int_row(rb, 10, [100])
    _add_int_row(rb, 30, [300])
    _add_int_row(rb, 40, [400])

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(out is not None, "anti_join produced None")
    # pk=10 matches right, so excluded. pk=30, pk=40 don't match -> kept
    assert_equal_i(2, out.length(), "anti_join should keep 2 non-matching rows")
    out.free()

    db.drop_view("test.v_aj")
    db.drop_table("test.right")
    db.drop_table("test.left")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_semi_join_delta_trace(base_dir):
    """input_delta -> semi_join(delta, trace_table) -> sink.
    Semi-join returns rows from delta that DO match in the trace."""
    log("[COMPILE] Testing semi_join_delta_trace circuit...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    cols = _make_table_cols_i64(["val"])
    table_l = db.create_table("test.left", cols, 0)
    table_r = db.create_table("test.right", cols, 0)

    # Populate right table: pk=10, pk=20
    r_batch = batch.ArenaZSetBatch(table_r.schema)
    rb_r = RowBuilder(table_r.schema, r_batch)
    _add_int_row(rb_r, 10, [500])
    _add_int_row(rb_r, 20, [600])
    ingest_to_family(table_r, r_batch)
    r_batch.free()

    builder = CircuitBuilder(view_id=0, primary_source_id=table_l.table_id)
    src = builder.input_delta()
    sj = builder.semi_join(src, table_r.table_id)
    builder.sink(sj, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_sj", graph, "")

    view = db.get_table("test.v_sj")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "semi_join plan is None")

    # Delta with pk=10 (matches), pk=30 (no match), pk=40 (no match)
    in_batch = batch.ArenaZSetBatch(table_l.schema)
    rb = RowBuilder(table_l.schema, in_batch)
    _add_int_row(rb, 10, [100])
    _add_int_row(rb, 30, [300])
    _add_int_row(rb, 40, [400])

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(out is not None, "semi_join produced None")
    # pk=10 matches right, so kept. pk=30, pk=40 don't match -> excluded
    assert_equal_i(1, out.length(), "semi_join should keep 1 matching row")
    out.free()

    db.drop_view("test.v_sj")
    db.drop_table("test.right")
    db.drop_table("test.left")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_distinct(base_dir):
    """input_delta -> distinct -> sink: stateful operator with history table."""
    log("[COMPILE] Testing distinct circuit...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    table_cols = _make_table_cols_i64(["val"])
    table = db.create_table("test.src", table_cols, 0)

    builder = CircuitBuilder(view_id=0, primary_source_id=table.table_id)
    src = builder.input_delta()
    d = builder.distinct(src)
    builder.sink(d, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_dist", graph, "")

    view = db.get_table("test.v_dist")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "distinct plan is None")

    # Tick 1: insert pk=1 with weight=3 -> distinct should output w=1
    in_batch = batch.ArenaZSetBatch(table.schema)
    rb = RowBuilder(table.schema, in_batch)
    _add_int_row(rb, 1, [100], weight=3)

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(out is not None, "distinct tick 1 produced None")
    assert_equal_i(1, out.length(), "distinct tick 1 row count")
    assert_equal_i64(r_int64(1), out.get_weight(0), "distinct should clamp weight to 1")
    out.free()

    db.drop_view("test.v_dist")
    db.drop_table("test.src")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_join_delta_delta(base_dir):
    """Two deltas joined: delta_a -> join_delta_delta(a, scan_b) -> sink."""
    log("[COMPILE] Testing join_delta_delta circuit...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    cols_a = _make_table_cols_i64(["val_a"])
    table_a = db.create_table("test.src_a", cols_a, 0)
    cols_b = _make_table_cols_i64(["val_b"])
    table_b = db.create_table("test.src_b", cols_b, 0)

    # Populate table_b so its scan_trace emits delta rows
    b_batch = batch.ArenaZSetBatch(table_b.schema)
    rb_b = RowBuilder(table_b.schema, b_batch)
    _add_int_row(rb_b, 5, [50])
    _add_int_row(rb_b, 10, [100])
    _add_int_row(rb_b, 15, [150])
    ingest_to_family(table_b, b_batch)
    b_batch.free()

    builder = CircuitBuilder(view_id=0, primary_source_id=table_a.table_id)
    src_a = builder.input_delta()
    # scan_b is a non-trace SCAN_TRACE (table_id > 0, not connected to PORT_TRACE)
    # This means compile_from_graph will emit a scan_trace instruction to dump it into a delta register
    src_b = builder.trace_scan(table_b.table_id)
    j = builder.join_delta_delta(src_a, src_b)
    builder.sink(j, target_table_id=0)
    out_cols = [
        ("pk_a", types.TYPE_U64.code),
        ("val_a", types.TYPE_I64.code),
        ("pk_b", types.TYPE_U64.code),
        ("val_b", types.TYPE_I64.code),
    ]
    graph = builder.build(out_cols)
    db.create_view("test.v_jdd", graph, "")

    view = db.get_table("test.v_jdd")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "join_delta_delta plan is None")

    # Delta_a: pk=5 and pk=10 -> should match scan_b pk=5 and pk=10
    in_batch = batch.ArenaZSetBatch(table_a.schema)
    rb = RowBuilder(table_a.schema, in_batch)
    _add_int_row(rb, 5, [55])
    _add_int_row(rb, 10, [110])

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(out is not None, "join_delta_delta produced None")
    assert_equal_i(2, out.length(), "join_delta_delta should match 2 rows")
    out.free()

    db.drop_view("test.v_jdd")
    db.drop_table("test.src_b")
    db.drop_table("test.src_a")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_negate_union_annihilation(base_dir):
    """input -> union(input, negate(input)) -> sink: should annihilate to empty."""
    log("[COMPILE] Testing negate+union annihilation circuit...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    table_cols = _make_table_cols_i64(["val"])
    table = db.create_table("test.src", table_cols, 0)

    builder = CircuitBuilder(view_id=0, primary_source_id=table.table_id)
    src = builder.input_delta()
    neg = builder.negate(src)
    u = builder.union(src, neg)
    builder.sink(u, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_annihilate", graph, "")

    view = db.get_table("test.v_annihilate")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "annihilation plan is None")

    in_batch = batch.ArenaZSetBatch(table.schema)
    rb = RowBuilder(table.schema, in_batch)
    _add_int_row(rb, 1, [100])
    _add_int_row(rb, 2, [200])

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    # Union of input + negated input: rows exist but aren't consolidated here.
    # The output is the raw union (2 + 2 = 4 rows, with +1 and -1 weights).
    # Consolidation would produce empty, but op_union just concatenates.
    assert_true(out is not None, "annihilation produced None")
    assert_equal_i(4, out.length(), "union(input, negate(input)) should have 4 raw rows")
    out.free()

    db.drop_view("test.v_annihilate")
    db.drop_table("test.src")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_persistence_and_recovery(base_dir):
    """Create a view, close engine, reopen, verify plan can be recovered and executed."""
    log("[COMPILE] Testing persistence and recovery...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    table_cols = _make_table_cols_i64(["val"])
    table = db.create_table("test.src", table_cols, 0)
    table_id = table.table_id

    builder = CircuitBuilder(view_id=0, primary_source_id=table_id)
    src = builder.input_delta()
    neg = builder.negate(src)
    builder.sink(neg, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_persist", graph, "")
    view_id = db.get_table("test.v_persist").table_id
    db.close()

    # Reopen and verify recovery
    db2 = engine.open_engine(base_dir)
    assert_true(db2.registry.has("test", "v_persist"), "view not recovered")

    plan = db2.program_cache.get_program(view_id)
    assert_true(plan is not None, "recovered plan is None")

    # Execute the recovered plan
    src_schema = db2.get_table("test.src").schema
    in_batch = batch.ArenaZSetBatch(src_schema)
    rb = RowBuilder(src_schema, in_batch)
    _add_int_row(rb, 1, [42])

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(out is not None, "recovered plan produced None")
    assert_equal_i(1, out.length(), "recovered plan row count")
    assert_equal_i64(r_int64(-1), out.get_weight(0), "recovered negate weight")
    out.free()

    db2.drop_view("test.v_persist")
    db2.drop_table("test.src")
    db2.drop_schema("test")
    db2.close()
    log("  PASSED")


def test_anti_semi_complement(base_dir):
    """anti_join + semi_join should partition the input (complement property)."""
    log("[COMPILE] Testing anti/semi join complement property...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    cols = _make_table_cols_i64(["val"])
    table_l = db.create_table("test.left", cols, 0)
    table_r = db.create_table("test.right", cols, 0)

    # Populate right table: pk=1, pk=3, pk=5
    r_batch = batch.ArenaZSetBatch(table_r.schema)
    rb_r = RowBuilder(table_r.schema, r_batch)
    _add_int_row(rb_r, 1, [10])
    _add_int_row(rb_r, 3, [30])
    _add_int_row(rb_r, 5, [50])
    ingest_to_family(table_r, r_batch)
    r_batch.free()

    # Anti-join view
    builder_aj = CircuitBuilder(view_id=0, primary_source_id=table_l.table_id)
    src_aj = builder_aj.input_delta()
    aj = builder_aj.anti_join(src_aj, table_r.table_id)
    builder_aj.sink(aj, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph_aj = builder_aj.build(out_cols)
    db.create_view("test.v_aj", graph_aj, "")

    # Semi-join view
    builder_sj = CircuitBuilder(view_id=0, primary_source_id=table_l.table_id)
    src_sj = builder_sj.input_delta()
    sj = builder_sj.semi_join(src_sj, table_r.table_id)
    builder_sj.sink(sj, target_table_id=0)
    graph_sj = builder_sj.build(out_cols)
    db.create_view("test.v_sj", graph_sj, "")

    plan_aj = db.program_cache.get_program(db.get_table("test.v_aj").table_id)
    plan_sj = db.program_cache.get_program(db.get_table("test.v_sj").table_id)
    assert_true(plan_aj is not None, "anti_join plan is None")
    assert_true(plan_sj is not None, "semi_join plan is None")

    # Left delta: pk=1,2,3,4,5 (5 rows)
    # Right has pk=1,3,5
    # Semi should get pk=1,3,5 (3 rows)
    # Anti should get pk=2,4 (2 rows)
    in_batch_aj = batch.ArenaZSetBatch(table_l.schema)
    in_batch_sj = batch.ArenaZSetBatch(table_l.schema)
    rb_aj = RowBuilder(table_l.schema, in_batch_aj)
    rb_sj = RowBuilder(table_l.schema, in_batch_sj)
    for pk in range(1, 6):
        _add_int_row(rb_aj, pk, [pk * 10])
        _add_int_row(rb_sj, pk, [pk * 10])

    out_aj = plan_aj.execute_epoch(in_batch_aj)
    out_sj = plan_sj.execute_epoch(in_batch_sj)
    in_batch_aj.free()
    in_batch_sj.free()

    assert_true(out_aj is not None, "anti_join complement produced None")
    assert_true(out_sj is not None, "semi_join complement produced None")

    aj_count = out_aj.length()
    sj_count = out_sj.length()
    assert_equal_i(2, aj_count, "anti_join complement count")
    assert_equal_i(3, sj_count, "semi_join complement count")
    assert_equal_i(5, aj_count + sj_count, "anti + semi should equal input count")

    out_aj.free()
    out_sj.free()

    db.drop_view("test.v_sj")
    db.drop_view("test.v_aj")
    db.drop_table("test.right")
    db.drop_table("test.left")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_filter_with_expr(base_dir):
    """input_delta -> filter(col1 > 25) -> sink: expression-based filtering."""
    log("[COMPILE] Testing filter with expression bytecode...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    table_cols = _make_table_cols_i64(["val"])
    table = db.create_table("test.src", table_cols, 0)

    # Build expression: col1 > 25  (col1 is column index 1, the I64 "val" column)
    from gnitz.dbsp.expr import ExprBuilder
    eb = ExprBuilder()
    col1 = eb.load_col_int(1)       # reg 0 = row.col[1]
    const25 = eb.load_const_int(25) # reg 1 = 25
    result = eb.cmp_gt(col1, const25)  # reg 2 = col1 > 25
    prog = eb.build(result)

    builder = CircuitBuilder(view_id=0, primary_source_id=table.table_id)
    src = builder.input_delta()
    flt = builder.filter(src, expr_code=prog.code_as_ints(),
                         expr_num_regs=prog.num_regs,
                         expr_result_reg=prog.result_reg)
    builder.sink(flt, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_flt_expr", graph, "")

    view = db.get_table("test.v_flt_expr")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "filter_expr plan is None")

    in_batch = batch.ArenaZSetBatch(table.schema)
    rb = RowBuilder(table.schema, in_batch)
    _add_int_row(rb, 1, [10])
    _add_int_row(rb, 2, [20])
    _add_int_row(rb, 3, [30])
    _add_int_row(rb, 4, [40])
    _add_int_row(rb, 5, [50])

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(out is not None, "filter_expr produced None")
    assert_equal_i(3, out.length(), "filter_expr should keep 3 rows (30,40,50)")
    out.free()

    db.drop_view("test.v_flt_expr")
    db.drop_table("test.src")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_map_projection(base_dir):
    """input_delta -> map(projection=[2]) -> sink: column projection (payload cols only, PK auto-copied)."""
    log("[COMPILE] Testing map with projection...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    # Input schema: pk(U64), col_a(I64), col_b(I64)
    table_cols = _make_table_cols_i64(["col_a", "col_b"])
    table = db.create_table("test.src", table_cols, 0)

    # Output schema: pk(U64), col_b(I64) — projects only col_b from input payload
    # PK is auto-copied by op_map's commit_row, projection covers payload columns only
    out_col_defs = [("pk", types.TYPE_U64.code), ("col_b", types.TYPE_I64.code)]

    builder = CircuitBuilder(view_id=0, primary_source_id=table.table_id)
    src = builder.input_delta()
    m = builder.map(src, projection=[2])  # select col 2 (col_b) from input schema
    builder.sink(m, target_table_id=0)
    graph = builder.build(out_col_defs)
    db.create_view("test.v_map", graph, "")

    view = db.get_table("test.v_map")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "map_proj plan is None")

    in_batch = batch.ArenaZSetBatch(table.schema)
    rb = RowBuilder(table.schema, in_batch)
    _add_int_row(rb, 1, [100, 200])
    _add_int_row(rb, 2, [300, 400])

    out = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(out is not None, "map_proj produced None")
    assert_equal_i(2, out.length(), "map_proj row count")

    # Verify projected values: output col 1 should be input col 2 (col_b)
    acc = out.get_accessor(0)
    val0 = acc.get_int_signed(1)  # output col 1 = input col_b
    assert_equal_i64(r_int64(200), val0, "map_proj row 0 col_b")
    acc2 = out.get_accessor(1)
    val1 = acc2.get_int_signed(1)
    assert_equal_i64(r_int64(400), val1, "map_proj row 1 col_b")
    out.free()

    db.drop_view("test.v_map")
    db.drop_table("test.src")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_reduce_sum(base_dir):
    """input_delta -> reduce(AGG_SUM, group=[0], agg_col=1) -> sink."""
    log("[COMPILE] Testing reduce with SUM aggregation...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    # Input schema: pk(U64), val(I64)
    table_cols = _make_table_cols_i64(["val"])
    table = db.create_table("test.src", table_cols, 0)

    from gnitz.dbsp.functions import AGG_SUM
    builder = CircuitBuilder(view_id=0, primary_source_id=table.table_id)
    src = builder.input_delta()
    r = builder.reduce(src, agg_func_id=AGG_SUM, group_by_cols=[0], agg_col_idx=1)
    builder.sink(r, target_table_id=0)
    # Reduce output: group_col(U64), agg_result(I64)
    out_cols = [("pk", types.TYPE_U64.code), ("sum_val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_reduce", graph, "")

    view = db.get_table("test.v_reduce")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "reduce_sum plan is None")

    # 3 rows with same PK=1 but different values; weight=1 each
    in_batch = batch.ArenaZSetBatch(table.schema)
    rb = RowBuilder(table.schema, in_batch)
    _add_int_row(rb, 1, [10])
    _add_int_row(rb, 1, [20])
    _add_int_row(rb, 1, [30])

    # Plan now has exchange_post_plan due to auto-inserted EXCHANGE_SHARD
    pre_result = plan.execute_epoch(in_batch)
    in_batch.free()
    assert_true(pre_result is not None, "reduce_sum pre-plan produced None")
    if plan.exchange_post_plan is not None:
        out = plan.exchange_post_plan.execute_epoch(pre_result)
        pre_result.free()
    else:
        out = pre_result
    assert_true(out is not None, "reduce_sum produced None")
    # Should produce 1 group with sum = 10+20+30 = 60
    assert_equal_i(1, out.length(), "reduce_sum row count")
    acc = out.get_accessor(0)
    sum_val = acc.get_int_signed(1)
    assert_equal_i64(r_int64(60), sum_val, "reduce_sum value")
    out.free()

    db.drop_view("test.v_reduce")
    db.drop_table("test.src")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------

def entry_point(argv):
    base_dir = "compile_graph_test_data"
    cleanup(base_dir)
    ensure_dir(base_dir)

    try:
        test_passthrough(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_negate(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_filter_passthrough(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_union(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_delay(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_join_delta_trace(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_anti_join_delta_trace(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_semi_join_delta_trace(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_distinct(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_join_delta_delta(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_negate_union_annihilation(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_persistence_and_recovery(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_anti_semi_complement(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_filter_with_expr(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_map_projection(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_reduce_sum(base_dir)

        log("\nALL COMPILE_GRAPH TESTS PASSED")
    except Exception as e:
        os.write(2, "FAILURE\n")
        return 1
    finally:
        cleanup(base_dir)

    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
