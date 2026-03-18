# exchange_test.py
#
# Tests for Phase 3 exchange operators:
# - hash_row_by_columns distribution
# - repartition_batch correctness
# - Split-plan compile: CircuitBuilder with SHARD node -> compile_from_graph

import sys
import os

from rpython.rlib import rposix, rsocket
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
from gnitz.core import opcodes
from gnitz.dbsp.ops.exchange import (
    hash_row_by_columns, repartition_batch, repartition_batches,
    repartition_batches_merged, PartitionAssignment,
)
from gnitz.storage.partitioned_table import _partition_for_key
from gnitz.server import ipc, ipc_ffi
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


def test_string_hash_consistency():
    """Two rows with the same string value should hash to the same partition."""
    log("[EXCHANGE] Testing string hash consistency...")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_STRING, name="name"))
    schema = types.TableSchema(cols, pk_index=0)

    b = batch.ArenaZSetBatch(schema)
    from gnitz.core.batch import RowBuilder
    rb = RowBuilder(schema, b)

    # Two rows with the same string "hello" but different PKs
    rb.begin(r_uint128(1), r_int64(1))
    rb.put_string("hello")
    rb.commit()

    rb.begin(r_uint128(2), r_int64(1))
    rb.put_string("hello")
    rb.commit()

    # A row with a different string
    rb.begin(r_uint128(3), r_int64(1))
    rb.put_string("world")
    rb.commit()

    col_indices = [1]
    p0 = hash_row_by_columns(b, 0, col_indices)
    p1 = hash_row_by_columns(b, 1, col_indices)
    p2 = hash_row_by_columns(b, 2, col_indices)

    assert_equal_i(p0, p1, "same string 'hello' should hash to same partition")
    # p2 may or may not differ, but at least the same-string case must match

    # Also test a longer string (> 12 bytes, heap-allocated)
    b2 = batch.ArenaZSetBatch(schema)
    rb2 = RowBuilder(schema, b2)

    rb2.begin(r_uint128(10), r_int64(1))
    rb2.put_string("this is a longer string for heap")
    rb2.commit()

    rb2.begin(r_uint128(11), r_int64(1))
    rb2.put_string("this is a longer string for heap")
    rb2.commit()

    q0 = hash_row_by_columns(b2, 0, col_indices)
    q1 = hash_row_by_columns(b2, 1, col_indices)
    assert_equal_i(q0, q1, "same long string should hash to same partition")

    b.free()
    b2.free()
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
    shard_cols = db.program_cache.get_shard_cols(view.table_id)
    assert_equal_i(1, len(shard_cols), "should have 1 shard column")
    assert_equal_i(1, shard_cols[0], "shard column should be 1")

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


def _make_table_cols_u128(col_names):
    cols = [types.ColumnDefinition(types.TYPE_U128, name="pk")]
    for n in col_names:
        cols.append(types.ColumnDefinition(types.TYPE_I64, name=n))
    return cols


def test_gather_deleted(base_dir):
    """Opcode 21 (old GATHER) is no longer a split marker; plan skips it."""
    log("[EXCHANGE] Testing GATHER opcode is dead...")

    # Verify OPCODE_EXCHANGE_SHARD is still 20
    assert_equal_i(20, opcodes.OPCODE_EXCHANGE_SHARD, "OPCODE_EXCHANGE_SHARD must be 20")

    # Build a circuit with raw opcode 21 in the node list (no gather() helper anymore).
    # compile_from_graph should silently skip it (not produce exchange_post_plan).
    db = engine.open_engine(base_dir)
    db.create_schema("test")
    table_cols = _make_table_cols_i64(["val"])
    table = db.create_table("test.src_g", table_cols, 0)

    # Build circuit by inserting opcode 21 manually via circuit_builder internals
    builder = CircuitBuilder(view_id=0, primary_source_id=table.table_id)
    src = builder.input_delta()
    # Manually allocate a node with raw opcode 21 (old GATHER) and connect it
    from rpython_tests.helpers.circuit_builder import NodeHandle
    nid = builder._next_node_id
    builder._next_node_id += 1
    builder._nodes.append((nid, 21))
    eid = builder._next_edge_id
    builder._next_edge_id += 1
    builder._edges.append((eid, src.node_id, nid, 0))
    gather_handle = NodeHandle(nid)
    builder.sink(gather_handle, target_table_id=0)
    out_cols = [("pk", types.TYPE_U64.code), ("val", types.TYPE_I64.code)]
    graph = builder.build(out_cols)
    db.create_view("test.v_gather", graph, "")

    view = db.get_table("test.v_gather")
    plan = db.program_cache.get_program(view.table_id)
    # Opcode 21 is silently skipped; plan compiles but has no exchange split
    assert_true(plan is not None, "plan for opcode-21 circuit must not be None")
    assert_true(plan.exchange_post_plan is None,
                "opcode 21 must not produce an exchange_post_plan")

    db.drop_view("test.v_gather")
    db.drop_table("test.src_g")
    db.drop_schema("test")
    db.close()
    log("  PASSED")


def test_hash_row_pk_col(base_dir):
    """repartition_batch with [pk_index] uses pk_lo/pk_hi, not col_bufs (U128 PK)."""
    log("[EXCHANGE] Testing hash_row_by_columns with U128 PK column...")

    cols = _make_table_cols_u128(["val"])
    schema = types.TableSchema(cols, pk_index=0)
    b = batch.ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)

    assignment = PartitionAssignment(4)

    # pk_lo and pk_hi values as separate lists (avoids prebuilt-long issues in RPython)
    # Rows: pk_hi=0 (U64-like) and pk_hi!=0 (true U128)
    pk_los = [1, 2, 999983, 305419896]
    pk_his = [0, 0, 1, 268435456]

    for i in range(len(pk_los)):
        pk_lo = pk_los[i]
        pk_hi = pk_his[i]
        pk = r_uint128((r_uint128(r_uint64(pk_hi)) << 64) | r_uint128(r_uint64(pk_lo)))
        rb.begin(pk, r_int64(1))
        rb.put_int(r_int64(42))
        rb.commit()

    # repartition by pk_index and verify each row lands on the expected worker
    sub_batches = repartition_batch(b, [schema.pk_index], 4, assignment)

    for i in range(len(pk_los)):
        pk_lo = pk_los[i]
        pk_hi = pk_his[i]
        expected_p = _partition_for_key(r_uint64(pk_lo), r_uint64(pk_hi))
        expected_w = assignment.worker_for_partition(expected_p)
        found = False
        for w in range(4):
            sb = sub_batches[w]
            if sb is None:
                continue
            for j in range(sb.length()):
                lo = sb._read_pk_lo(j)
                hi = sb._read_pk_hi(j)
                if lo == r_uint64(pk_lo) and hi == r_uint64(pk_hi):
                    assert_equal_i(expected_w, w,
                                   "U128 PK row " + str(i) + " on wrong worker")
                    found = True
                    break
        assert_true(found, "U128 PK row " + str(i) + " not found in any sub-batch")

    for w in range(4):
        if sub_batches[w] is not None:
            sub_batches[w].free()
    b.free()
    log("  PASSED")


def test_repartition_batch_pk_equals_split(base_dir):
    """repartition_batch([pk_index]) produces same assignment as manual _partition_for_key."""
    log("[EXCHANGE] Testing repartition_batch pk-routing golden values...")

    schema = types.TableSchema(
        _make_table_cols_i64(["val"]),
        pk_index=0,
    )
    b = batch.ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)

    pks = [1, 7, 42, 100, 255, 1024, 65537, 999983]
    for pk in pks:
        _add_int_row(rb, pk, [pk * 2])

    assignment = PartitionAssignment(4)
    sub_batches = repartition_batch(b, [schema.pk_index], 4, assignment)

    # Compute expected worker for each PK manually
    for i in range(len(pks)):
        pk_val = pks[i]
        expected_p = _partition_for_key(r_uint64(pk_val), r_uint64(0))
        expected_w = assignment.worker_for_partition(expected_p)
        found = False
        for w in range(4):
            sb = sub_batches[w]
            if sb is None:
                continue
            for j in range(sb.length()):
                lo = sb._read_pk_lo(j)
                if lo == r_uint64(pk_val):
                    assert_equal_i(expected_w, w,
                                   "PK " + str(pk_val) + " on wrong worker")
                    found = True
                    break
        assert_true(found, "PK " + str(pk_val) + " not found in any sub-batch")

    for w in range(4):
        if sub_batches[w] is not None:
            sub_batches[w].free()
    b.free()
    log("  PASSED")


def _make_simple_schema():
    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    return types.TableSchema(cols, 0)


def _make_simple_batch(schema):
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    _add_int_row(rb, 1, [100])
    _add_int_row(rb, 2, [200])
    return b


def test_batch_sorted_flag_roundtrip():
    """FLAG_BATCH_SORTED preserved through serialize_to_memfd / _recv_and_parse."""
    log("[EXCHANGE] Testing FLAG_BATCH_SORTED roundtrip...")
    schema = _make_simple_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
    try:
        b = _make_simple_batch(schema)
        b.mark_sorted(True)
        ipc.send_batch(s1.fd, 1, b, schema=schema)
        b.free()

        payload = ipc.receive_payload(s2.fd)
        assert_true(payload.batch is not None, "batch should be present")
        assert_true(payload.batch._sorted,
                    "FLAG_BATCH_SORTED: _sorted should be True after roundtrip")
        assert_true(not payload.batch._consolidated,
                    "sorted-only batch should not be consolidated")
        payload.close()
    finally:
        s1.close()
        s2.close()
    log("  PASSED")


def test_batch_consolidated_flag_roundtrip():
    """FLAG_BATCH_CONSOLIDATED preserved through IPC; implies _sorted."""
    log("[EXCHANGE] Testing FLAG_BATCH_CONSOLIDATED roundtrip...")
    schema = _make_simple_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
    try:
        b = _make_simple_batch(schema)
        b.mark_consolidated(True)
        ipc.send_batch(s1.fd, 1, b, schema=schema)
        b.free()

        payload = ipc.receive_payload(s2.fd)
        assert_true(payload.batch is not None, "batch should be present")
        assert_true(payload.batch._consolidated,
                    "FLAG_BATCH_CONSOLIDATED: _consolidated should be True")
        assert_true(payload.batch._sorted,
                    "consolidated batch must also be sorted")
        payload.close()
    finally:
        s1.close()
        s2.close()
    log("  PASSED")


def test_batch_no_flags_roundtrip():
    """Plain (unsorted) batch: neither _sorted nor _consolidated set after roundtrip."""
    log("[EXCHANGE] Testing no batch property flags roundtrip...")
    schema = _make_simple_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
    try:
        b = _make_simple_batch(schema)
        # default: _sorted=False, _consolidated=False
        ipc.send_batch(s1.fd, 1, b, schema=schema)
        b.free()

        payload = ipc.receive_payload(s2.fd)
        assert_true(payload.batch is not None, "batch should be present")
        assert_true(not payload.batch._sorted,
                    "plain batch: _sorted should be False")
        assert_true(not payload.batch._consolidated,
                    "plain batch: _consolidated should be False")
        payload.close()
    finally:
        s1.close()
        s2.close()
    log("  PASSED")


def test_repartition_does_not_propagate_consolidated():
    """repartition_batch sub-batches are NOT consolidated even if input is."""
    log("[EXCHANGE] Testing repartition_batch does not propagate _consolidated...")
    schema = _make_simple_schema()
    assignment = PartitionAssignment(4)

    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    for i in range(8):
        _add_int_row(rb, i + 1, [(i + 1) * 10])
    b.mark_consolidated(True)

    sub_batches = repartition_batch(b, [schema.pk_index], 4, assignment)
    b.free()

    for w in range(4):
        sb = sub_batches[w]
        if sb is not None:
            assert_true(not sb._consolidated,
                        "sub-batch should not be consolidated")
            sb.free()
    log("  PASSED")


def test_repartition_batches():
    """repartition_batches: sequential fallback, output NOT consolidated."""
    log("[EXCHANGE] Testing repartition_batches (fallback)...")
    schema = _make_simple_schema()
    assignment = PartitionAssignment(4)

    # 4 non-consolidated source batches, 8 rows each (PKs 1-8, 9-16, 17-24, 25-32)
    batches = [None] * 4
    for b_idx in range(4):
        sb = ArenaZSetBatch(schema)
        rb = RowBuilder(schema, sb)
        for i in range(8):
            pk = b_idx * 8 + i + 1
            _add_int_row(rb, pk, [pk * 10])
        batches[b_idx] = sb

    dest = repartition_batches(batches, [schema.pk_index], 4, assignment)

    # Total rows == 32
    total = 0
    for w in range(4):
        if dest[w] is not None:
            total += dest[w].length()
    assert_equal_i(32, total, "repartition_batches total row count")

    # No dest batch should be consolidated
    for w in range(4):
        if dest[w] is not None:
            assert_true(not dest[w]._consolidated,
                        "repartition_batches: dest[" + str(w) + "] must not be consolidated")

    # Each row in dest[w] must hash to w
    for w in range(4):
        if dest[w] is not None:
            for j in range(dest[w].length()):
                p = _partition_for_key(dest[w]._read_pk_lo(j), dest[w]._read_pk_hi(j))
                expected_w = assignment.worker_for_partition(p)
                assert_equal_i(expected_w, w,
                               "repartition_batches: row in dest[" + str(w) + "] routes to wrong worker")

    for b_idx in range(4):
        batches[b_idx].free()
    for w in range(4):
        if dest[w] is not None:
            dest[w].free()
    log("  PASSED")


def test_repartition_batches_merged():
    """repartition_batches_merged: k-way merge, output consolidated and PK-sorted."""
    log("[EXCHANGE] Testing repartition_batches_merged (k-way merge)...")
    schema = _make_simple_schema()
    assignment = PartitionAssignment(4)

    # 4 consolidated source batches, 8 rows each (PKs 1-8, 9-16, 17-24, 25-32)
    batches = [None] * 4
    for b_idx in range(4):
        sb = ArenaZSetBatch(schema)
        rb = RowBuilder(schema, sb)
        for i in range(8):
            pk = b_idx * 8 + i + 1
            _add_int_row(rb, pk, [pk * 10])
        sb.mark_consolidated(True)
        batches[b_idx] = sb

    dest = repartition_batches_merged(batches, [schema.pk_index], 4, assignment)

    # Total rows == 32
    total = 0
    for w in range(4):
        if dest[w] is not None:
            total += dest[w].length()
    assert_equal_i(32, total, "repartition_batches_merged total row count")

    # Every non-None dest batch must be consolidated
    for w in range(4):
        if dest[w] is not None:
            assert_true(dest[w]._consolidated,
                        "repartition_batches_merged: dest[" + str(w) + "] must be consolidated")

    # Within each non-None dest batch, PKs must be strictly increasing
    for w in range(4):
        if dest[w] is not None and dest[w].length() > 1:
            for j in range(dest[w].length() - 1):
                cur_hi = dest[w]._read_pk_hi(j)
                nxt_hi = dest[w]._read_pk_hi(j + 1)
                cur_lo = dest[w]._read_pk_lo(j)
                nxt_lo = dest[w]._read_pk_lo(j + 1)
                strictly_less = cur_hi < nxt_hi or (cur_hi == nxt_hi and cur_lo < nxt_lo)
                assert_true(strictly_less,
                            "repartition_batches_merged: PKs not strictly increasing in dest[" + str(w) + "]")

    # Each row in dest[w] must hash to w
    for w in range(4):
        if dest[w] is not None:
            for j in range(dest[w].length()):
                p = _partition_for_key(dest[w]._read_pk_lo(j), dest[w]._read_pk_hi(j))
                expected_w = assignment.worker_for_partition(p)
                assert_equal_i(expected_w, w,
                               "repartition_batches_merged: row in dest[" + str(w) + "] routes to wrong worker")

    for b_idx in range(4):
        batches[b_idx].free()
    for w in range(4):
        if dest[w] is not None:
            dest[w].free()
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

        test_string_hash_consistency()

        test_compile_split_plan(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_compile_shard_with_negate(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_gather_deleted(base_dir)
        cleanup(base_dir)
        ensure_dir(base_dir)

        test_hash_row_pk_col(base_dir)

        test_repartition_batch_pk_equals_split(base_dir)

        test_batch_sorted_flag_roundtrip()
        test_batch_consolidated_flag_roundtrip()
        test_batch_no_flags_roundtrip()
        test_repartition_does_not_propagate_consolidated()

        test_repartition_batches()
        test_repartition_batches_merged()

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
