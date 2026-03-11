# rpython_tests/partitioned_table_test.py

import sys
import os

from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)

from gnitz.core import types, batch, xxh
from gnitz.core.batch import RowBuilder, ArenaZSetBatch
from gnitz.storage.partitioned_table import (
    PartitionedTable,
    make_partitioned_persistent,
    make_partitioned_ephemeral,
    get_num_partitions,
    _partition_for_key,
    NUM_PARTITIONS,
)
from gnitz.catalog.system_tables import FIRST_USER_TABLE_ID


# -- Helpers -------------------------------------------------------------------


def cleanup_dir(path):
    if not os.path.exists(path):
        return
    for item in os.listdir(path):
        p = os.path.join(path, item)
        if os.path.isdir(p):
            cleanup_dir(p)
        else:
            os.unlink(p)
    os.rmdir(path)


def make_u64_i64_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    return types.TableSchema(cols, 0)


def assert_true(condition, msg):
    if not condition:
        raise Exception("Assertion Failed: " + msg)


def assert_equal_i(expected, actual, msg):
    if expected != actual:
        raise Exception(
            msg + " -> Expected: %d, Actual: %d" % (expected, actual)
        )


def log(msg):
    os.write(1, "[TEST] " + msg + "\n")


# -- Tests ---------------------------------------------------------------------


def test_get_num_partitions():
    log("test_get_num_partitions")
    # System tables (table_id < FIRST_USER_TABLE_ID) get 1 partition
    for tid in range(1, FIRST_USER_TABLE_ID):
        assert_equal_i(1, get_num_partitions(tid),
                       "system table %d should have 1 partition" % tid)
    # User tables get NUM_PARTITIONS
    assert_equal_i(NUM_PARTITIONS, get_num_partitions(FIRST_USER_TABLE_ID),
                   "first user table should have 256 partitions")
    assert_equal_i(NUM_PARTITIONS, get_num_partitions(1000),
                   "user table 1000 should have 256 partitions")
    log("  PASSED")


def test_partition_routing_determinism():
    log("test_partition_routing_determinism")
    # Same key always maps to the same partition
    for i in range(100):
        lo = r_uint64(i * 7 + 13)
        hi = r_uint64(i * 11 + 37)
        p1 = _partition_for_key(lo, hi)
        p2 = _partition_for_key(lo, hi)
        assert_equal_i(p1, p2, "partition must be deterministic for key %d" % i)
        assert_true(0 <= p1 < 256, "partition must be in [0, 256)")
    log("  PASSED")


def test_n1_persistent(base_dir):
    log("test_n1_persistent (system table behavior)")
    d = base_dir + "/n1_persist"
    os.mkdir(d)

    schema = make_u64_i64_schema()
    table_id = 5  # system table

    store = make_partitioned_persistent(d, "sys_test", schema, table_id, 1)

    # Ingest a few rows
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    for i in range(10):
        rb.begin(r_uint128(i + 1), r_int64(1))
        rb.put_int(r_int64(i * 100))
        rb.commit()

    store.ingest_batch(b)
    b.free()

    # Scan and verify count
    count = 0
    cur = store.create_cursor()
    while cur.is_valid():
        count += 1
        cur.advance()
    cur.close()
    assert_equal_i(10, count, "N=1 scan should see 10 rows")

    # has_pk
    assert_true(store.has_pk(r_uint128(1)), "pk=1 should exist")
    assert_true(not store.has_pk(r_uint128(999)), "pk=999 should not exist")

    # flush and re-scan
    store.flush()
    count = 0
    cur = store.create_cursor()
    while cur.is_valid():
        count += 1
        cur.advance()
    cur.close()
    assert_equal_i(10, count, "N=1 scan after flush should see 10 rows")

    store.close()
    log("  PASSED")


def test_n256_persistent(base_dir):
    log("test_n256_persistent (user table, 1000 rows)")
    d = base_dir + "/n256_persist"
    os.mkdir(d)

    schema = make_u64_i64_schema()
    table_id = FIRST_USER_TABLE_ID + 1

    store = make_partitioned_persistent(d, "user_test", schema, table_id, NUM_PARTITIONS)

    num_rows = 1000

    # Ingest 1000 rows
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    for i in range(num_rows):
        pk = r_uint128(i + 1)
        rb.begin(pk, r_int64(1))
        rb.put_int(r_int64(i * 10))
        rb.commit()

    store.ingest_batch(b)
    b.free()

    # Scan all — should see exactly 1000 rows
    count = 0
    cur = store.create_cursor()
    while cur.is_valid():
        count += 1
        cur.advance()
    cur.close()
    assert_equal_i(num_rows, count, "N=256 scan should see 1000 rows")

    # has_pk for each key
    for i in range(num_rows):
        pk = r_uint128(i + 1)
        assert_true(store.has_pk(pk), "pk=%d should exist" % (i + 1))

    assert_true(not store.has_pk(r_uint128(0)), "pk=0 should not exist")
    assert_true(not store.has_pk(r_uint128(num_rows + 1)),
                "pk=%d should not exist" % (num_rows + 1))

    log("  PASSED")


def test_n256_sorted_output(base_dir):
    log("test_n256_sorted_output (cursor merge produces global sort)")
    d = base_dir + "/n256_sorted"
    os.mkdir(d)

    schema = make_u64_i64_schema()
    table_id = FIRST_USER_TABLE_ID + 2

    store = make_partitioned_persistent(d, "sort_test", schema, table_id, NUM_PARTITIONS)

    num_rows = 500

    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    for i in range(num_rows):
        pk = r_uint128(i + 1)
        rb.begin(pk, r_int64(1))
        rb.put_int(r_int64(i))
        rb.commit()

    store.ingest_batch(b)
    b.free()

    # Verify cursor output is sorted
    cur = store.create_cursor()
    prev_key = r_uint128(0)
    count = 0
    while cur.is_valid():
        k = cur.key()
        assert_true(k > prev_key, "cursor output must be globally sorted")
        prev_key = k
        count += 1
        cur.advance()
    cur.close()
    assert_equal_i(num_rows, count, "sorted scan count")

    store.close()
    log("  PASSED")


def test_n256_flush_and_rescan(base_dir):
    log("test_n256_flush_and_rescan")
    d = base_dir + "/n256_flush"
    os.mkdir(d)

    schema = make_u64_i64_schema()
    table_id = FIRST_USER_TABLE_ID + 3

    store = make_partitioned_persistent(d, "flush_test", schema, table_id, NUM_PARTITIONS)

    # Ingest first batch
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    for i in range(100):
        rb.begin(r_uint128(i + 1), r_int64(1))
        rb.put_int(r_int64(i))
        rb.commit()
    store.ingest_batch(b)
    b.free()

    # Flush
    store.flush()

    # Ingest second batch
    b2 = ArenaZSetBatch(schema)
    rb2 = RowBuilder(schema, b2)
    for i in range(100, 200):
        rb2.begin(r_uint128(i + 1), r_int64(1))
        rb2.put_int(r_int64(i))
        rb2.commit()
    store.ingest_batch(b2)
    b2.free()

    # Should see all 200
    count = 0
    cur = store.create_cursor()
    while cur.is_valid():
        count += 1
        cur.advance()
    cur.close()
    assert_equal_i(200, count, "after flush + second ingest should see 200 rows")

    store.close()
    log("  PASSED")


def test_n256_ephemeral(base_dir):
    log("test_n256_ephemeral")
    d = base_dir + "/n256_eph"
    os.mkdir(d)

    schema = make_u64_i64_schema()
    table_id = FIRST_USER_TABLE_ID + 4

    store = make_partitioned_ephemeral(d, "eph_test", schema, table_id, NUM_PARTITIONS)

    num_rows = 300

    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    for i in range(num_rows):
        rb.begin(r_uint128(i + 1), r_int64(1))
        rb.put_int(r_int64(i))
        rb.commit()
    store.ingest_batch(b)
    b.free()

    count = 0
    cur = store.create_cursor()
    while cur.is_valid():
        count += 1
        cur.advance()
    cur.close()
    assert_equal_i(num_rows, count, "ephemeral N=256 should see all rows")

    store.close()
    log("  PASSED")


def test_create_child(base_dir):
    log("test_create_child")
    d = base_dir + "/n256_child"
    os.mkdir(d)

    schema = make_u64_i64_schema()
    table_id = FIRST_USER_TABLE_ID + 5

    store = make_partitioned_ephemeral(d, "child_test", schema, table_id, NUM_PARTITIONS)

    child = store.create_child("scratch", schema)
    # child should be usable as a store
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    rb.begin(r_uint128(42), r_int64(1))
    rb.put_int(r_int64(99))
    rb.commit()
    child.ingest_batch(b)
    b.free()

    assert_true(child.has_pk(r_uint128(42)), "child should have pk=42")

    child.close()
    store.close()
    log("  PASSED")


# -- Entry Point ---------------------------------------------------------------


def entry_point(argv):
    base_dir = "partitioned_table_test_data"
    if os.path.exists(base_dir):
        cleanup_dir(base_dir)
    os.mkdir(base_dir)

    try:
        test_get_num_partitions()
        test_partition_routing_determinism()
        test_n1_persistent(base_dir)
        test_n256_persistent(base_dir)
        test_n256_sorted_output(base_dir)
        test_n256_flush_and_rescan(base_dir)
        test_n256_ephemeral(base_dir)
        test_create_child(base_dir)
        os.write(1, "\nALL PARTITIONED TABLE TESTS PASSED\n")
    except Exception as e:
        os.write(2, "TEST FAILED: " + str(e) + "\n")
        return 1
    finally:
        cleanup_dir(base_dir)

    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
