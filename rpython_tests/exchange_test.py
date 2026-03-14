# exchange_test.py
#
# Tests for Phase 3 exchange operators:
# - hash_row_by_columns distribution
# - repartition_batch correctness
# - Split-plan compile: CircuitBuilder with SHARD node -> compile_from_graph

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

from gnitz.core import types, batch
from gnitz.core.batch import RowBuilder, ArenaZSetBatch
from gnitz.catalog import engine
from gnitz.catalog.registry import ingest_to_family
from gnitz.catalog.metadata import ensure_dir
from gnitz.dbsp.ops.exchange import hash_row_by_columns, repartition_batch
from gnitz.server.master import PartitionAssignment
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


def _make_table_cols_i64(col_names):
    cols = newlist_hint(len(col_names) + 1)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    for n in col_names:
        cols.append(types.ColumnDefinition(types.TYPE_I64, name=n))
    return cols


def _add_int_row(rb, pk, vals, weight=1):
    rb.begin(r_uint128(pk), r_int64(weight))
    for v in vals:
        rb.put_int(r_int64(v))
    rb.commit()


# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------


def test_hash_distribution():
    """hash_row_by_columns should produce a reasonable distribution across 256 buckets."""
    log("[EXCHANGE] Testing hash_row_by_columns distribution...")

    schema = types.TableSchema(
        _make_table_cols_i64(["val"]),
        pk_index=0,
    )
    b = batch.ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)

    # Insert 1000 rows with distinct PKs
    for i in range(1000):
        _add_int_row(rb, i, [i * 7])

    # Hash each row by column 1 (val)
    col_indices = [1]
    buckets = [0] * 256
    for i in range(b.length()):
        p = hash_row_by_columns(b, i, col_indices)
        assert_true(p >= 0 and p < 256, "hash out of range")
        buckets[p] += 1

    # Check distribution: at least 100 distinct buckets should be hit
    non_empty = 0
    for i in range(256):
        if buckets[i] > 0:
            non_empty += 1
    assert_true(non_empty >= 100,
                "hash distribution too skewed: only " + str(non_empty) + " of 256 buckets hit")

    b.free()
    log("  PASSED")


def test_repartition_batch():
    """repartition_batch should split rows correctly across workers."""
    log("[EXCHANGE] Testing repartition_batch...")

    schema = types.TableSchema(
        _make_table_cols_i64(["val"]),
        pk_index=0,
    )
    b = batch.ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)

    for i in range(100):
        _add_int_row(rb, i, [i * 3])

    assignment = PartitionAssignment(4)
    col_indices = [1]

    sub_batches = repartition_batch(b, col_indices, 4, assignment)

    # Verify: total rows across all sub-batches == 100
    total = 0
    for w in range(4):
        if sub_batches[w] is not None:
            total += sub_batches[w].length()
    assert_equal_i(100, total, "repartition total row count")

    # At least 2 workers should have rows (with 100 rows and 4 workers)
    non_empty_workers = 0
    for w in range(4):
        if sub_batches[w] is not None and sub_batches[w].length() > 0:
            non_empty_workers += 1
    assert_true(non_empty_workers >= 2,
                "repartition: only " + str(non_empty_workers) + " workers got rows")

    # Cleanup
    for w in range(4):
        if sub_batches[w] is not None:
            sub_batches[w].free()
    b.free()
    log("  PASSED")


def test_compile_split_plan(base_dir):
    """Build circuit with SHARD node, compile, verify exchange_post_plan is set."""
    log("[EXCHANGE] Testing compile_from_graph with SHARD node...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    table_cols = _make_table_cols_i64(["val"])
    table = db.create_table("test.src", table_cols, 0)

    # Circuit: input -> shard(col=1) -> sink
    builder = CircuitBuilder(view_id=0, primary_source_id=table.table_id)
    src = builder.input_delta()
    sharded = builder.shard(src, [1])  # shard by column 1
    builder.sink(sharded, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_shard", graph, "")

    view = db.get_table("test.v_shard")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "shard plan is None")
    assert_true(plan.exchange_post_plan is not None, "exchange_post_plan should be set")
    assert_true(plan.exchange_shard_cols is not None, "exchange_shard_cols should be set")
    assert_equal_i(1, len(plan.exchange_shard_cols), "should have 1 shard column")
    assert_equal_i(1, plan.exchange_shard_cols[0], "shard column should be 1")

    # Execute pre-plan in single-process mode (no exchange handler)
    in_batch = batch.ArenaZSetBatch(table.schema)
    rb = RowBuilder(table.schema, in_batch)
    _add_int_row(rb, 1, [100])
    _add_int_row(rb, 2, [200])

    # Pre-plan: just passes data through (input -> halt at shard boundary)
    pre_result = plan.execute_epoch(in_batch)
    assert_true(pre_result is not None, "pre-plan produced None")
    assert_equal_i(2, pre_result.length(), "pre-plan row count")

    # Post-plan: takes exchanged data, integrates to sink
    post_result = plan.exchange_post_plan.execute_epoch(pre_result)
    assert_true(post_result is not None, "post-plan produced None")
    assert_equal_i(2, post_result.length(), "post-plan row count")

    pre_result.free()
    post_result.free()
    in_batch.free()

    db.drop_view("test.v_shard")
    db.drop_table("test.src")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_compile_shard_with_negate(base_dir):
    """Circuit: input -> negate -> shard -> sink. Pre-plan includes negate."""
    log("[EXCHANGE] Testing shard with negate in pre-plan...")
    db = engine.open_engine(base_dir)

    db.create_schema("test")
    table_cols = _make_table_cols_i64(["val"])
    table = db.create_table("test.src", table_cols, 0)

    builder = CircuitBuilder(view_id=0, primary_source_id=table.table_id)
    src = builder.input_delta()
    neg = builder.negate(src)
    sharded = builder.shard(neg, [1])
    builder.sink(sharded, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_ns", graph, "")

    view = db.get_table("test.v_ns")
    plan = db.program_cache.get_program(view.table_id)
    assert_true(plan is not None, "negate+shard plan is None")
    assert_true(plan.exchange_post_plan is not None, "exchange_post_plan should be set")

    in_batch = batch.ArenaZSetBatch(table.schema)
    rb = RowBuilder(table.schema, in_batch)
    _add_int_row(rb, 1, [100], weight=3)

    # Pre-plan: input -> negate (weight should flip)
    pre_result = plan.execute_epoch(in_batch)
    assert_true(pre_result is not None, "pre-plan produced None")
    assert_equal_i(1, pre_result.length(), "pre-plan row count")

    w = pre_result.get_weight(0)
    assert_true(w == r_int64(-3), "pre-plan should negate weight")

    # Post-plan: sink
    post_result = plan.exchange_post_plan.execute_epoch(pre_result)
    assert_true(post_result is not None, "post-plan produced None")
    assert_equal_i(1, post_result.length(), "post-plan row count")

    pre_result.free()
    post_result.free()
    in_batch.free()

    db.drop_view("test.v_ns")
    db.drop_table("test.src")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------

def entry_point(argv):
    base_dir = "exchange_test_data"
    cleanup(base_dir)
    ensure_dir(base_dir)

    try:
        test_hash_distribution()

        test_repartition_batch()

        test_compile_split_plan(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_compile_shard_with_negate(base_dir)

        log("\nALL EXCHANGE TESTS PASSED")
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
