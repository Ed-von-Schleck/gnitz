# rpython_tests/storage_comprehensive_test.py

import sys
import os
import errno

from rpython.rlib import rposix, rposix_stat
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import types, batch, xxh, errors
from gnitz.core import strings as string_logic
from gnitz.storage import (
    buffer,
    memtable,
    cursor,
    mmap_posix,
)
from gnitz.storage.ephemeral_table import EphemeralTable
from gnitz.storage.table import PersistentTable
from gnitz.storage.partitioned_table import (
    PartitionedTable, make_partitioned_persistent, make_partitioned_ephemeral,
    get_num_partitions, _partition_for_key, NUM_PARTITIONS,
)
from gnitz.core.batch import ArenaZSetBatch, ColumnarBatchAccessor, RowBuilder
from gnitz.catalog.registry import _enforce_unique_pk, TableFamily
from gnitz.catalog.system_tables import FIRST_USER_TABLE_ID
from rpython_tests.helpers.jit_stub import ensure_jit_reachable
from rpython_tests.helpers.assertions import (
    assert_true, assert_false, assert_equal_i, assert_equal_i64,
    assert_equal_u64, assert_equal_s, assert_equal_u128,
)
from rpython_tests.helpers.fs import cleanup_dir

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def make_u64_str_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="id"),
        types.ColumnDefinition(types.TYPE_STRING, name="val"),
    ]
    return types.TableSchema(cols, 0)


def make_u64_i64_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="id"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    return types.TableSchema(cols, 0)


def make_u128_i64_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U128, name="uuid_pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    return types.TableSchema(cols, 0)


def make_u64_u128_str_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_U128, name="uuid_payload"),
        types.ColumnDefinition(types.TYPE_STRING, name="label"),
    ]
    return types.TableSchema(cols, 0)



def corrupt_byte(filepath, offset):
    fd = rposix.open(filepath, os.O_RDWR, 0)
    try:
        rposix.lseek(fd, offset, 0)
        val_str = rposix.read(fd, 1)
        val = ord(val_str[0])
        rposix.lseek(fd, offset, 0)
        rposix.write(fd, chr(val ^ 0xFF))
    finally:
        rposix.close(fd)


def log(msg):
    os.write(1, "[TEST] " + msg + "\n")


def _make_pt_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="pk"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]
    return types.TableSchema(cols, 0)


# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------


def test_integrity_and_lowlevel(base_dir):
    os.write(1, "[Storage] Testing Integrity & Low-Level Buffers...\n")

    # 1. XXHash Primitives
    c1 = xxh.compute_checksum_bytes("GnitzDB")
    c2 = xxh.compute_checksum_bytes("GnitzDB")
    c3 = xxh.compute_checksum_bytes("gnitzdb")
    assert_true(c1 == c2, "XXH3-64 stability failed")
    assert_true(c1 != c3, "XXH3-64 collision on case shift")

    # 2. Raw pointers hashing
    size = 128
    buf = lltype.malloc(rffi.CCHARP.TO, size, flavor="raw")
    try:
        for i in range(size):
            buf[i] = chr(i % 256)
        rc1 = xxh.compute_checksum(buf, 64)
        rc2 = xxh.compute_checksum(rffi.ptradd(buf, 10), 64)
        assert_true(rc1 != rc2, "XXH raw pointer offset failed")
        assert_true(xxh.verify_checksum(buf, 64, rc1), "XXH verify failed")

        # Large buffer (exceeds XXH_STRIPE_LEN)
        large_size = 1024
        lbuf = lltype.malloc(rffi.CCHARP.TO, large_size, flavor="raw")
        try:
            for i in range(large_size):
                lbuf[i] = chr(i % 127)
            lc = xxh.compute_checksum(lbuf, large_size)
            assert_true(lc != r_uint64(0), "Large buffer checksum is zero")
        finally:
            lltype.free(lbuf, flavor="raw")
    finally:
        lltype.free(buf, flavor="raw")

    # 3. Buffer Bounds & Lifecycle
    arena = buffer.Buffer(1024, growable=False)
    try:
        p1 = arena.alloc(5, alignment=1)
        off1 = rffi.cast(lltype.Signed, p1) - rffi.cast(lltype.Signed, arena.base_ptr)
        assert_equal_i(0, off1, "First alloc should be at offset 0")

        p2 = arena.alloc(1, alignment=8)
        off2 = rffi.cast(lltype.Signed, p2) - rffi.cast(lltype.Signed, arena.base_ptr)
        assert_equal_i(8, off2, "8-byte alignment failed")

        raised = False
        try:
            arena.alloc(2048)
        except errors.MemTableFullError:
            raised = True
        assert_true(raised, "MemTableFullError not raised on overflow")
    finally:
        arena.free()

    os.write(1, "    [OK] Integrity & Low-Level passed.\n")




def make_compaction_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="id"),  # PK
        types.ColumnDefinition(types.TYPE_STRING, name="name"),  # Payload 1
        types.ColumnDefinition(types.TYPE_I64, name="val"),  # Payload 2
    ]
    return types.TableSchema(cols, pk_index=0)


def ingest_test_row(tbl, pk_val, weight, name_str, int_val):
    schema = tbl.get_schema()
    b = batch.ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    rb.begin(r_uint64(pk_val), r_uint64(0), r_int64(weight))
    rb.put_string(name_str)
    rb.put_int(r_int64(int_val))
    rb.commit()

    tbl.ingest_batch(b)
    # Flush immediately to create a physical shard
    tbl.flush()


def test_compaction(base_dir):
    """
    Tests the FLSM Compactor: N-way tournament merge + Algebraic Annihilation.
    """
    os.write(1, "[Storage] Testing Pure Z-Set Compaction...\n")
    schema = make_compaction_schema()
    path = os.path.join(base_dir, "compaction_test")

    # 1. Setup Table
    tbl = PersistentTable(path, "compact_me", schema, table_id=500)

    # 2. Create Overlapping State (5 flushes -> needs_compaction = True)
    # PK 100: (1 + 1 - 2) = 0  -> Should be GHOSTED
    # PK 200: 1                -> Should survive
    # PK 300: 1                -> Should survive
    ingest_test_row(tbl, 100, 1, "record_a", 1000)
    ingest_test_row(tbl, 200, 1, "record_b", 2000)
    ingest_test_row(tbl, 100, 1, "record_a", 1000)
    ingest_test_row(tbl, 300, 1, "record_c", 3000)
    ingest_test_row(tbl, 100, -2, "record_a", 1000)

    initial_shards = tbl.index.handles
    if len(initial_shards) < 5:
        raise Exception("Failed to create 5 shards for compaction test")

    # Record filenames for cleanup verification
    initial_filenames = newlist_hint(len(initial_shards))
    fi = 0
    while fi < len(initial_shards):
        initial_filenames.append(initial_shards[fi].filename)
        fi += 1

    if not tbl.index.needs_compaction:
        raise Exception("Expected needs_compaction=True before compaction")

    # 3. Execute Compaction via compact_if_needed
    tbl.compact_if_needed()

    # 4. Verify Index State: L0 cleared, L1 guards installed
    if len(tbl.index.handles) != 0:
        raise Exception(
            "Expected 0 L0 handles after L0->L1 compaction, got "
            + str(len(tbl.index.handles))
        )
    if len(tbl.index.levels) != 1:
        raise Exception(
            "Expected 1 L1 level after compaction, got "
            + str(len(tbl.index.levels))
        )
    if tbl.index.needs_compaction:
        raise Exception("needs_compaction must be False after compaction")

    # 5. Verify Content (The Ghost Property) via cursor scan
    c = tbl.create_cursor()
    row_count = 0
    pk100_weight = r_int64(0)
    while c.is_valid():
        k = c.key()
        w = c.weight()
        if k == r_uint128(100):
            pk100_weight = pk100_weight + w
        if w > r_int64(0):
            row_count += 1
        c.advance()
    c.close()

    if row_count != 2:
        raise Exception(
            "Expected 2 surviving rows (PK 200 and 300), got " + str(row_count)
        )
    if pk100_weight != r_int64(0):
        raise Exception("Ghost Property Violation: PK 100 net weight != 0")

    if tbl.has_pk(r_uint64(100), r_uint64(0)):
        raise Exception("Ghost Property Violation: has_pk(100) should be False")
    if not tbl.has_pk(r_uint64(200), r_uint64(0)):
        raise Exception("PK 200 should survive compaction")
    if not tbl.has_pk(r_uint64(300), r_uint64(0)):
        raise Exception("PK 300 should survive compaction")

    # 6. Verify Cleanup: old L0 shard files should be deleted
    fi = 0
    while fi < len(initial_filenames):
        if os.path.exists(initial_filenames[fi]):
            os.write(1, "[Warning] Old shard still on disk: " + initial_filenames[fi] + "\n")
        fi += 1

    tbl.close()
    os.write(1, "[Storage] Compaction Test Passed.\n")


def test_ephemeral_and_persistent_tables(base_dir):
    os.write(1, "[Storage] Testing Ephemeral & Persistent Tables...\n")
    schema = make_u64_str_schema()

    # 1. Ephemeral Table In-Memory & Summation
    eph_dir = os.path.join(base_dir, "eph")
    t_eph = EphemeralTable(eph_dir, "trace1", schema)

    pk1 = r_uint128(1)

    b_add = batch.ArenaZSetBatch(schema)
    rb_add = RowBuilder(schema, b_add)
    rb_add.begin(r_uint64(pk1), r_uint64(pk1 >> 64), r_int64(5))
    rb_add.put_string("sum_test")
    rb_add.commit()
    t_eph.ingest_batch(b_add)

    b_sub = batch.ArenaZSetBatch(schema)
    rb_sub = RowBuilder(schema, b_sub)
    rb_sub.begin(r_uint64(pk1), r_uint64(pk1 >> 64), r_int64(-3))
    rb_sub.put_string("sum_test")
    rb_sub.commit()
    t_eph.ingest_batch(b_sub)

    tmp_acc = batch.ArenaZSetBatch(schema)
    rb_acc = RowBuilder(schema, tmp_acc)
    rb_acc.begin(r_uint64(pk1), r_uint64(pk1 >> 64), r_int64(1))
    rb_acc.put_string("sum_test")
    rb_acc.commit()
    acc1 = tmp_acc.get_accessor(0)

    net_w = t_eph.get_weight(r_uint64(pk1), r_uint64(pk1 >> 64), acc1)
    assert_equal_i64(r_int64(2), net_w, "Ephemeral algebraic summation failed")

    # 2. Ephemeral Spill
    t_eph.flush()

    net_w2 = t_eph.get_weight(r_uint64(pk1), r_uint64(pk1 >> 64), acc1)
    assert_equal_i64(
        r_int64(2), net_w2, "Summation visibility lost after ephemeral flush"
    )

    # 3. Scratch Table creation
    t_scr = t_eph.create_child("rec_trace", schema)
    assert_true(
        t_scr.directory.find("scratch_rec_trace") >= 0,
        "Scratch directory path incorrect",
    )
    t_scr.close()

    # 4. Unified Cursor
    r_k2 = r_uint128(2)
    b2 = batch.ArenaZSetBatch(schema)
    rb2 = RowBuilder(schema, b2)
    rb2.begin(r_uint64(r_k2), r_uint64(r_k2 >> 64), r_int64(1))
    rb2.put_string("mem_only")
    rb2.commit()
    t_eph.ingest_batch(b2)

    uc = t_eph.create_cursor()
    assert_true(uc.is_valid(), "UnifiedCursor should be valid")
    assert_equal_u128(pk1, uc.key(), "UnifiedCursor seq 1 mismatch")
    uc.advance()
    assert_equal_u128(r_k2, uc.key(), "UnifiedCursor seq 2 mismatch")
    uc.advance()
    assert_false(uc.is_valid(), "UnifiedCursor should be exhausted")
    uc.close()

    tmp_acc.free()
    t_eph.close()
    b_add.free()
    b_sub.free()
    b2.free()

    os.write(1, "    [OK] Tables passed.\n")


def test_u128_payloads(base_dir):
    os.write(1, "[Storage] Testing TYPE_U128 Non-PK Payloads...\n")
    schema = make_u64_u128_str_schema()
    db_path = os.path.join(base_dir, "u128_db")

    t_u128 = PersistentTable(db_path, "u128_t", schema)

    pk1 = r_uint128(1)
    uuid_lo = r_uint64(0xCAFEBABEDEADBEEF)
    uuid_hi = r_uint64(0x0123456789ABCDEF)

    b = batch.ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    rb.begin(r_uint64(pk1), r_uint64(pk1 >> 64), r_int64(1))
    rb.put_u128(uuid_lo, uuid_hi)
    rb.put_string("a")
    rb.commit()

    t_u128.ingest_batch(b)
    t_u128.flush()

    c = t_u128.create_cursor()
    assert_true(c.is_valid(), "Cursor should be valid for u128 payload test")
    acc = c.get_accessor()

    # Reconstruct u128 and check
    val = acc.get_u128(1)  # Index 1 is uuid_payload
    expected = (r_uint128(uuid_hi) << 64) | r_uint128(uuid_lo)
    assert_equal_u128(expected, val, "U128 payload mismatch")

    c.close()
    t_u128.close()
    b.free()
    os.write(1, "    [OK] U128 Payloads passed.\n")


# ------------------------------------------------------------------------------
# retract_pk Tests
# ------------------------------------------------------------------------------


def test_retract_pk(base_dir):
    os.write(1, "  test_retract_pk ...\n")
    schema = make_u64_str_schema()

    # --- test_retract_pk_found_in_memtable ---
    eph_dir = os.path.join(base_dir, "retract_mt")
    t = EphemeralTable(eph_dir, "retract_mt", schema)
    b = batch.ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    rb.begin(r_uint64(10), r_uint64(0), r_int64(1))
    rb.put_string("hello")
    rb.commit()
    t.ingest_batch(b)
    b.free()

    out = batch.ArenaZSetBatch(schema)
    result = t.retract_pk(r_uint64(10), r_uint64(0), out)
    assert_true(result, "retract_pk memtable should return True")
    assert_equal_i(1, out.length(), "retract_pk memtable out should have 1 row")
    assert_true(out.get_weight(0) == r_int64(-1), "retract weight should be -1")
    assert_true(out.get_pk(0) == r_uint128(10), "retract pk should be 10")
    out.free()
    t.close()

    # --- test_retract_pk_found_in_shard ---
    eph_dir2 = os.path.join(base_dir, "retract_shard")
    t2 = EphemeralTable(eph_dir2, "retract_shard", schema)
    b2 = batch.ArenaZSetBatch(schema)
    rb2 = RowBuilder(schema, b2)
    rb2.begin(r_uint64(20), r_uint64(0), r_int64(1))
    rb2.put_string("world")
    rb2.commit()
    t2.ingest_batch(b2)
    b2.free()
    t2.flush()  # moves to shard

    out2 = batch.ArenaZSetBatch(schema)
    result2 = t2.retract_pk(r_uint64(20), r_uint64(0), out2)
    assert_true(result2, "retract_pk shard should return True")
    assert_equal_i(1, out2.length(), "retract_pk shard out should have 1 row")
    assert_true(out2.get_weight(0) == r_int64(-1), "retract shard weight should be -1")
    out2.free()
    t2.close()

    # --- test_retract_pk_absent ---
    eph_dir3 = os.path.join(base_dir, "retract_absent")
    t3 = EphemeralTable(eph_dir3, "retract_absent", schema)
    out3 = batch.ArenaZSetBatch(schema)
    result3 = t3.retract_pk(r_uint64(999), r_uint64(0), out3)
    assert_false(result3, "retract_pk absent should return False")
    assert_equal_i(0, out3.length(), "retract_pk absent out should be empty")
    out3.free()
    t3.close()

    # --- test_retract_pk_after_retraction (net=0) ---
    eph_dir4 = os.path.join(base_dir, "retract_net0")
    t4 = EphemeralTable(eph_dir4, "retract_net0", schema)
    b4a = batch.ArenaZSetBatch(schema)
    rb4a = RowBuilder(schema, b4a)
    rb4a.begin(r_uint64(30), r_uint64(0), r_int64(1))
    rb4a.put_string("ins")
    rb4a.commit()
    t4.ingest_batch(b4a)
    b4a.free()
    b4b = batch.ArenaZSetBatch(schema)
    rb4b = RowBuilder(schema, b4b)
    rb4b.begin(r_uint64(30), r_uint64(0), r_int64(-1))
    rb4b.put_string("ins")
    rb4b.commit()
    t4.ingest_batch(b4b)
    b4b.free()

    out4 = batch.ArenaZSetBatch(schema)
    result4 = t4.retract_pk(r_uint64(30), r_uint64(0), out4)
    assert_false(result4, "retract_pk net=0 should return False")
    assert_equal_i(0, out4.length(), "retract_pk net=0 out should be empty")
    out4.free()
    t4.close()

    # --- test_retract_pk_split_weight (shard+memtable both positive) ---
    eph_dir5 = os.path.join(base_dir, "retract_split")
    t5 = EphemeralTable(eph_dir5, "retract_split", schema)
    b5a = batch.ArenaZSetBatch(schema)
    rb5a = RowBuilder(schema, b5a)
    rb5a.begin(r_uint64(40), r_uint64(0), r_int64(1))
    rb5a.put_string("split")
    rb5a.commit()
    t5.ingest_batch(b5a)
    b5a.free()
    t5.flush()  # +1 in shard
    b5b = batch.ArenaZSetBatch(schema)
    rb5b = RowBuilder(schema, b5b)
    rb5b.begin(r_uint64(40), r_uint64(0), r_int64(1))
    rb5b.put_string("split")
    rb5b.commit()
    t5.ingest_batch(b5b)
    b5b.free()  # +1 in memtable

    out5 = batch.ArenaZSetBatch(schema)
    result5 = t5.retract_pk(r_uint64(40), r_uint64(0), out5)
    assert_true(result5, "retract_pk split weight should return True")
    assert_equal_i(1, out5.length(), "retract_pk split should emit exactly 1 retraction")
    assert_true(out5.get_weight(0) == r_int64(-1), "retract split weight should be -1")
    out5.free()
    t5.close()

    # --- test_retract_pk_weight_canceled (shard +1, memtable -1) ---
    eph_dir6 = os.path.join(base_dir, "retract_cancel")
    t6 = EphemeralTable(eph_dir6, "retract_cancel", schema)
    b6a = batch.ArenaZSetBatch(schema)
    rb6a = RowBuilder(schema, b6a)
    rb6a.begin(r_uint64(50), r_uint64(0), r_int64(1))
    rb6a.put_string("cancel")
    rb6a.commit()
    t6.ingest_batch(b6a)
    b6a.free()
    t6.flush()  # +1 in shard
    b6b = batch.ArenaZSetBatch(schema)
    rb6b = RowBuilder(schema, b6b)
    rb6b.begin(r_uint64(50), r_uint64(0), r_int64(-1))
    rb6b.put_string("cancel")
    rb6b.commit()
    t6.ingest_batch(b6b)
    b6b.free()  # -1 in memtable

    out6 = batch.ArenaZSetBatch(schema)
    result6 = t6.retract_pk(r_uint64(50), r_uint64(0), out6)
    assert_false(result6, "retract_pk canceled should return False")
    assert_equal_i(0, out6.length(), "retract_pk canceled out should be empty")
    out6.free()
    t6.close()

    # --- test_retract_pk_string_column (verify string data copied correctly) ---
    eph_dir7 = os.path.join(base_dir, "retract_str")
    t7 = EphemeralTable(eph_dir7, "retract_str", schema)
    test_str = "retract_string_payload"
    b7 = batch.ArenaZSetBatch(schema)
    rb7 = RowBuilder(schema, b7)
    rb7.begin(r_uint64(60), r_uint64(0), r_int64(1))
    rb7.put_string(test_str)
    rb7.commit()
    t7.ingest_batch(b7)

    # Build a reference row for comparison
    ref = batch.ArenaZSetBatch(schema)
    rb_ref = RowBuilder(schema, ref)
    rb_ref.begin(r_uint64(60), r_uint64(0), r_int64(-1))
    rb_ref.put_string(test_str)
    rb_ref.commit()

    out7 = batch.ArenaZSetBatch(schema)
    result7 = t7.retract_pk(r_uint64(60), r_uint64(0), out7)
    assert_true(result7, "retract_pk string should return True")
    assert_equal_i(1, out7.length(), "retract_pk string out should have 1 row")
    # Compare the retracted row's payload against the reference
    from gnitz.core import comparator as core_comparator
    acc_out = ColumnarBatchAccessor(schema)
    acc_out.bind(out7, 0)
    acc_ref = ColumnarBatchAccessor(schema)
    acc_ref.bind(ref, 0)
    cmp_result = core_comparator.compare_rows(schema, acc_out, acc_ref)
    assert_equal_i(0, cmp_result, "retract_pk string payload should match original")
    out7.free()
    ref.free()
    b7.free()
    t7.close()

    # --- test_has_pk_unchanged (verify has_pk still works after refactor) ---
    eph_dir8 = os.path.join(base_dir, "retract_has")
    t8 = EphemeralTable(eph_dir8, "retract_has", schema)
    b8 = batch.ArenaZSetBatch(schema)
    rb8 = RowBuilder(schema, b8)
    rb8.begin(r_uint64(70), r_uint64(0), r_int64(1))
    rb8.put_string("exists")
    rb8.commit()
    t8.ingest_batch(b8)
    b8.free()

    assert_true(t8.has_pk(r_uint64(70), r_uint64(0)), "has_pk should find pk=70")
    assert_false(t8.has_pk(r_uint64(71), r_uint64(0)), "has_pk should not find pk=71")

    t8.flush()
    assert_true(t8.has_pk(r_uint64(70), r_uint64(0)), "has_pk should find pk=70 after flush")
    assert_false(t8.has_pk(r_uint64(71), r_uint64(0)), "has_pk should not find pk=71 after flush")
    t8.close()

    os.write(1, "    [OK] retract_pk tests passed.\n")


def test_enforce_unique_pk(base_dir):
    os.write(1, "  test_enforce_unique_pk ...\n")
    schema = make_u64_i64_schema()

    # --- insert_new: new PK, no stored row → output = input row ---
    sub_dir1 = os.path.join(base_dir, "upk_t1")
    t1 = EphemeralTable(sub_dir1, "upk_t1", schema)
    family1 = TableFamily("s", "upk_t1", 1, 1, sub_dir1, 0, t1, unique_pk=True)
    b1 = batch.ArenaZSetBatch(schema)
    rb1 = RowBuilder(schema, b1)
    rb1.begin(r_uint64(1), r_uint64(0), r_int64(1))
    rb1.put_int(r_int64(10))
    rb1.commit()
    result1 = _enforce_unique_pk(family1, b1)
    assert_equal_i(1, result1.length(), "insert_new: should have 1 row")
    assert_true(result1.get_weight(0) == r_int64(1), "insert_new: weight should be +1")
    result1.free()
    b1.free()
    t1.close()

    # --- update_existing: stored row → retraction + new row ---
    sub_dir2 = os.path.join(base_dir, "upk_t2")
    t2 = EphemeralTable(sub_dir2, "upk_t2", schema)
    family2 = TableFamily("s", "upk_t2", 2, 1, sub_dir2, 0, t2, unique_pk=True)
    b2pre = batch.ArenaZSetBatch(schema)
    rb2pre = RowBuilder(schema, b2pre)
    rb2pre.begin(r_uint64(1), r_uint64(0), r_int64(1))
    rb2pre.put_int(r_int64(10))
    rb2pre.commit()
    t2.ingest_batch(b2pre)
    b2pre.free()
    b2 = batch.ArenaZSetBatch(schema)
    rb2 = RowBuilder(schema, b2)
    rb2.begin(r_uint64(1), r_uint64(0), r_int64(1))
    rb2.put_int(r_int64(20))
    rb2.commit()
    result2 = _enforce_unique_pk(family2, b2)
    assert_equal_i(2, result2.length(), "update_existing: should have 2 rows")
    assert_true(result2.get_weight(0) == r_int64(-1), "update_existing: retraction first w=-1")
    assert_true(result2.get_weight(1) == r_int64(1), "update_existing: insertion second w=+1")
    result2.free()
    b2.free()
    t2.close()

    # --- delete_existing: stored row + w=-1 input → retraction with stored payload ---
    sub_dir3 = os.path.join(base_dir, "upk_t3")
    t3 = EphemeralTable(sub_dir3, "upk_t3", schema)
    family3 = TableFamily("s", "upk_t3", 3, 1, sub_dir3, 0, t3, unique_pk=True)
    b3pre = batch.ArenaZSetBatch(schema)
    rb3pre = RowBuilder(schema, b3pre)
    rb3pre.begin(r_uint64(1), r_uint64(0), r_int64(1))
    rb3pre.put_int(r_int64(10))
    rb3pre.commit()
    t3.ingest_batch(b3pre)
    b3pre.free()
    b3 = batch.ArenaZSetBatch(schema)
    rb3 = RowBuilder(schema, b3)
    rb3.begin(r_uint64(1), r_uint64(0), r_int64(-1))
    rb3.put_int(r_int64(0))
    rb3.commit()
    result3 = _enforce_unique_pk(family3, b3)
    assert_equal_i(1, result3.length(), "delete_existing: should have 1 row")
    assert_true(result3.get_weight(0) == r_int64(-1), "delete_existing: weight should be -1")
    assert_true(result3.get_pk(0) == r_uint128(1), "delete_existing: pk should be 1")
    result3.free()
    b3.free()
    t3.close()

    # --- delete_absent: no stored row + w=-1 → empty output ---
    sub_dir4 = os.path.join(base_dir, "upk_t4")
    t4 = EphemeralTable(sub_dir4, "upk_t4", schema)
    family4 = TableFamily("s", "upk_t4", 4, 1, sub_dir4, 0, t4, unique_pk=True)
    b4 = batch.ArenaZSetBatch(schema)
    rb4 = RowBuilder(schema, b4)
    rb4.begin(r_uint64(99), r_uint64(0), r_int64(-1))
    rb4.put_int(r_int64(0))
    rb4.commit()
    result4 = _enforce_unique_pk(family4, b4)
    assert_equal_i(0, result4.length(), "delete_absent: should be empty")
    result4.free()
    b4.free()
    t4.close()

    # --- intra_batch_duplicate: two inserts for same PK → 3 rows (last wins) ---
    sub_dir5 = os.path.join(base_dir, "upk_t5")
    t5 = EphemeralTable(sub_dir5, "upk_t5", schema)
    family5 = TableFamily("s", "upk_t5", 5, 1, sub_dir5, 0, t5, unique_pk=True)
    b5 = batch.ArenaZSetBatch(schema)
    rb5a = RowBuilder(schema, b5)
    rb5a.begin(r_uint64(1), r_uint64(0), r_int64(1))
    rb5a.put_int(r_int64(10))
    rb5a.commit()
    rb5b = RowBuilder(schema, b5)
    rb5b.begin(r_uint64(1), r_uint64(0), r_int64(1))
    rb5b.put_int(r_int64(20))
    rb5b.commit()
    result5 = _enforce_unique_pk(family5, b5)
    assert_equal_i(3, result5.length(), "intra_batch_duplicate: should have 3 rows")
    result5.free()
    b5.free()
    t5.close()

    # --- intra_batch_insert_then_delete: insert then delete same PK → net zero ---
    sub_dir6 = os.path.join(base_dir, "upk_t6")
    t6 = EphemeralTable(sub_dir6, "upk_t6", schema)
    family6 = TableFamily("s", "upk_t6", 6, 1, sub_dir6, 0, t6, unique_pk=True)
    b6 = batch.ArenaZSetBatch(schema)
    rb6a = RowBuilder(schema, b6)
    rb6a.begin(r_uint64(1), r_uint64(0), r_int64(1))
    rb6a.put_int(r_int64(10))
    rb6a.commit()
    rb6b = RowBuilder(schema, b6)
    rb6b.begin(r_uint64(1), r_uint64(0), r_int64(-1))
    rb6b.put_int(r_int64(0))
    rb6b.commit()
    result6 = _enforce_unique_pk(family6, b6)
    # Insert then delete: output carries [+1, -1] pair that nets to zero
    assert_equal_i(2, result6.length(), "intra_batch_insert_then_delete: should have 2 rows")
    assert_true(result6.get_weight(0) == r_int64(1), "intra_batch_insert_then_delete: row0 w=+1")
    assert_true(result6.get_weight(1) == r_int64(-1), "intra_batch_insert_then_delete: row1 w=-1")
    result6.free()
    b6.free()
    t6.close()

    os.write(1, "    [OK] enforce_unique_pk tests passed.\n")


def test_ephemeral_compaction(base_dir):
    """Auto-compaction via create_cursor(): sawtooth pattern, ghost property."""
    os.write(1, "[Storage] Testing EphemeralTable auto-compaction...\n")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    tbl_dir = os.path.join(base_dir, "eph_compact")
    tbl = EphemeralTable(tbl_dir, "ec", schema)

    try:
        # Phase 1: 6 rows, one flush each -> 6 shards -> needs_compaction True
        i = 1
        while i <= 6:
            b = batch.ArenaZSetBatch(schema)
            rb = RowBuilder(schema, b)
            rb.begin(r_uint64(i), r_uint64(0), r_int64(1))
            rb.put_int(r_int64(i * 10))
            rb.commit()
            tbl.ingest_batch(b)
            tbl.flush()
            b.free()
            i += 1

        if not tbl.index.needs_compaction:
            raise Exception("needs_compaction should be True after 6 flushes")
        if len(tbl.index.handles) != 6:
            raise Exception(
                "Expected 6 shards, got " + str(len(tbl.index.handles))
            )

        # Record pre-compaction shard filenames to verify deletion afterward
        pre_compact_files = []
        h_idx = 0
        while h_idx < len(tbl.index.handles):
            pre_compact_files.append(tbl.index.handles[h_idx].filename)
            h_idx += 1

        # create_cursor() triggers compaction, routes L0 to L1 guards
        c = tbl.create_cursor()

        if len(tbl.index.handles) != 0:
            raise Exception(
                "Expected 0 L0 handles post-compaction, got "
                + str(len(tbl.index.handles))
            )
        if len(tbl.index.levels) != 1:
            raise Exception(
                "Expected 1 L1 level post-compaction, got "
                + str(len(tbl.index.levels))
            )
        if tbl.index.needs_compaction:
            raise Exception("needs_compaction should be False after compaction")

        # Verify all 6 rows present via cursor scan
        row_count = 0
        while c.is_valid():
            if c.weight() > r_int64(0):
                row_count += 1
            c.advance()
        c.close()
        if row_count != 6:
            raise Exception("Expected 6 rows post-compaction, got " + str(row_count))

        # Old shard files should be deleted (ref_counter.try_cleanup() inside run_compact)
        f_idx = 0
        while f_idx < len(pre_compact_files):
            if os.path.exists(pre_compact_files[f_idx]):
                os.write(1, "    [WARN] old shard not deleted: "
                         + pre_compact_files[f_idx] + "\n")
            f_idx += 1

        # Phase 2: retract PK=3 + add PKs 7-10 -> 5 more L0 shards -> second compaction
        b = batch.ArenaZSetBatch(schema)
        rb = RowBuilder(schema, b)
        rb.begin(r_uint64(3), r_uint64(0), r_int64(-1))
        rb.put_int(r_int64(30))
        rb.commit()
        tbl.ingest_batch(b)
        tbl.flush()
        b.free()

        i = 7
        while i <= 10:
            b = batch.ArenaZSetBatch(schema)
            rb = RowBuilder(schema, b)
            rb.begin(r_uint64(i), r_uint64(0), r_int64(1))
            rb.put_int(r_int64(i * 10))
            rb.commit()
            tbl.ingest_batch(b)
            tbl.flush()
            b.free()
            i += 1

        # 5 new L0 shards -> needs_compaction True again
        if not tbl.index.needs_compaction:
            raise Exception(
                "needs_compaction should be True before second compaction"
            )

        c = tbl.create_cursor()
        if len(tbl.index.handles) != 0:
            raise Exception(
                "Expected 0 L0 handles after second compaction, got "
                + str(len(tbl.index.handles))
            )
        c.close()

        # Ghost property: PK=3 net weight = +1 - 1 = 0, must be absent
        if tbl.has_pk(r_uint64(3), r_uint64(0)):
            raise Exception("Ghost property violation: PK=3 should be annihilated")

        # Row count: 6 original - 1 retracted + 4 new = 9
        c = tbl.create_cursor()
        row_count_2 = 0
        while c.is_valid():
            if c.weight() > r_int64(0):
                row_count_2 += 1
            c.advance()
        c.close()
        if row_count_2 != 9:
            raise Exception(
                "Expected 9 rows after second compaction, got " + str(row_count_2)
            )

    finally:
        tbl.close()

    os.write(1, "[Storage] EphemeralTable auto-compaction Test Passed.\n")


def test_compact_no_op_and_idempotent(base_dir):
    """compact_if_needed(): no-op on empty table, no-op at threshold, idempotent after compact."""
    os.write(1, "[Storage] Testing compaction no-op and idempotency...\n")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    tbl_dir = os.path.join(base_dir, "eph_noop")
    tbl = EphemeralTable(tbl_dir, "en", schema)

    try:
        # Phase 0 (Gap M): freshly created table (0 shards) -> compact_if_needed() is a no-op
        if tbl.index.needs_compaction:
            raise Exception("needs_compaction must be False for newly created table")
        tbl.compact_if_needed()
        if len(tbl.index.handles) != 0:
            raise Exception(
                "compact_if_needed() on empty table must leave 0 shards, got "
                + str(len(tbl.index.handles))
            )

        # Phase 1 (Gap A): exactly at threshold (4 shards) -> needs_compaction must be False
        i = 1
        while i <= 4:
            b = batch.ArenaZSetBatch(schema)
            rb = RowBuilder(schema, b)
            rb.begin(r_uint64(i), r_uint64(0), r_int64(1))
            rb.put_int(r_int64(i * 10))
            rb.commit()
            tbl.ingest_batch(b)
            tbl.flush()
            b.free()
            i += 1

        if tbl.index.needs_compaction:
            raise Exception(
                "needs_compaction must be False at threshold boundary (4 shards)"
            )
        tbl.compact_if_needed()
        if len(tbl.index.handles) != 4:
            raise Exception(
                "compact_if_needed() must be no-op at boundary; got "
                + str(len(tbl.index.handles)) + " shards"
            )

        # Phase 2 (Gap A): one more flush pushes past threshold (5 > 4) -> compaction fires
        b = batch.ArenaZSetBatch(schema)
        rb = RowBuilder(schema, b)
        rb.begin(r_uint64(5), r_uint64(0), r_int64(1))
        rb.put_int(r_int64(50))
        rb.commit()
        tbl.ingest_batch(b)
        tbl.flush()
        b.free()

        if not tbl.index.needs_compaction:
            raise Exception("needs_compaction must be True with 5 shards (> threshold 4)")

        tbl.compact_if_needed()

        if len(tbl.index.handles) != 0:
            raise Exception(
                "Expected 0 L0 handles after compaction, got "
                + str(len(tbl.index.handles))
            )
        if len(tbl.index.levels) != 1:
            raise Exception(
                "Expected 1 L1 level after compaction, got "
                + str(len(tbl.index.levels))
            )
        if tbl.index.needs_compaction:
            raise Exception("needs_compaction must be False after compaction")

        # Phase 3 (Gap J): second compact_if_needed() -> idempotent no-op
        tbl.compact_if_needed()
        if len(tbl.index.handles) != 0:
            raise Exception(
                "compact_if_needed() must be idempotent; got "
                + str(len(tbl.index.handles)) + " L0 handles after second call"
            )
        if len(tbl.index.levels) != 1:
            raise Exception(
                "compact_if_needed() must be idempotent; got "
                + str(len(tbl.index.levels)) + " levels after second call"
            )
        if tbl.index.needs_compaction:
            raise Exception(
                "needs_compaction must remain False after idempotent compact call"
            )

    finally:
        tbl.close()

    os.write(1, "[Storage] Compaction no-op and idempotency Test Passed.\n")


def test_compact_preserves_memtable(base_dir):
    """Shard compaction does not lose rows that are still in the memtable."""
    os.write(1, "[Storage] Testing compaction preserves memtable rows...\n")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    tbl_dir = os.path.join(base_dir, "eph_memtab")
    tbl = EphemeralTable(tbl_dir, "em", schema)

    try:
        # 5 flushed shards (> threshold 4) -> compaction fires on next create_cursor()
        i = 1
        while i <= 5:
            b = batch.ArenaZSetBatch(schema)
            rb = RowBuilder(schema, b)
            rb.begin(r_uint64(i), r_uint64(0), r_int64(1))
            rb.put_int(r_int64(i * 10))
            rb.commit()
            tbl.ingest_batch(b)
            tbl.flush()
            b.free()
            i += 1

        if not tbl.index.needs_compaction:
            raise Exception("Expected needs_compaction=True before memtable test")

        # PK=99 written to memtable only — deliberately NOT flushed
        b = batch.ArenaZSetBatch(schema)
        rb = RowBuilder(schema, b)
        rb.begin(r_uint64(99), r_uint64(0), r_int64(1))
        rb.put_int(r_int64(990))
        rb.commit()
        tbl.ingest_batch(b)
        b.free()

        # create_cursor() compacts the 5 shards into L1; memtable is untouched
        c = tbl.create_cursor()

        if len(tbl.index.handles) != 0:
            raise Exception(
                "Expected 0 L0 handles post-compaction, got "
                + str(len(tbl.index.handles))
            )
        if len(tbl.index.levels) != 1:
            raise Exception(
                "Expected 1 L1 level post-compaction, got "
                + str(len(tbl.index.levels))
            )

        # All 6 rows must be visible: 5 from L1 guards + 1 from memtable
        row_count = 0
        while c.is_valid():
            if c.weight() > r_int64(0):
                row_count += 1
            c.advance()
        c.close()

        if row_count != 6:
            raise Exception(
                "Expected 6 rows (5 shard + 1 memtable), got " + str(row_count)
            )

        # PK=99 (memtable-only row) must survive shard compaction
        if not tbl.has_pk(r_uint64(99), r_uint64(0)):
            raise Exception("PK=99 (memtable-only) must survive shard compaction")

    finally:
        tbl.close()

    os.write(1, "[Storage] Compaction preserves memtable Test Passed.\n")


def test_persistent_compact_if_needed(base_dir):
    """PersistentTable: create_cursor() bypasses compaction; compact_if_needed() compacts."""
    os.write(1, "[Storage] Testing PersistentTable.compact_if_needed()...\n")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    tbl_dir = os.path.join(base_dir, "persist_compact")
    tbl = PersistentTable(tbl_dir, "pc", schema)

    try:
        # 5 flushes -> 5 shards -> needs_compaction True
        i = 1
        while i <= 5:
            b = batch.ArenaZSetBatch(schema)
            rb = RowBuilder(schema, b)
            rb.begin(r_uint64(i), r_uint64(0), r_int64(1))
            rb.put_int(r_int64(i * 10))
            rb.commit()
            tbl.ingest_batch(b)
            tbl.flush()
            b.free()
            i += 1

        if not tbl.index.needs_compaction:
            raise Exception("Expected needs_compaction=True after 5 flushes")
        if len(tbl.index.handles) != 5:
            raise Exception(
                "Expected 5 shards before compact, got "
                + str(len(tbl.index.handles))
            )

        # PersistentTable.create_cursor() must NOT compact (concurrent cursor safety)
        c = tbl.create_cursor()
        c.close()

        if len(tbl.index.handles) != 5:
            raise Exception(
                "PersistentTable.create_cursor() must not compact; expected 5 shards, got "
                + str(len(tbl.index.handles))
            )

        # compact_if_needed() must run L0->L1 compaction with manifest update
        tbl.compact_if_needed()

        if len(tbl.index.handles) != 0:
            raise Exception(
                "PersistentTable.compact_if_needed() must compact; expected 0 L0 handles, got "
                + str(len(tbl.index.handles))
            )
        if len(tbl.index.levels) != 1:
            raise Exception(
                "PersistentTable.compact_if_needed() must create L1; got "
                + str(len(tbl.index.levels)) + " levels"
            )
        if tbl.index.needs_compaction:
            raise Exception("needs_compaction must be False after compaction")

        # All 5 rows accessible after compaction
        c = tbl.create_cursor()
        row_count = 0
        while c.is_valid():
            if c.weight() > r_int64(0):
                row_count += 1
            c.advance()
        c.close()

        if row_count != 5:
            raise Exception(
                "Expected 5 rows post-compaction, got " + str(row_count)
            )

    finally:
        tbl.close()

    os.write(1, "[Storage] PersistentTable compact_if_needed Test Passed.\n")


def test_compact_with_strings(base_dir):
    """Compaction correctly relocates string blob arenas."""
    os.write(1, "[Storage] Testing compaction with string columns...\n")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_STRING, name="name"))
    schema = types.TableSchema(cols, 0)

    tbl_dir = os.path.join(base_dir, "eph_strings")
    tbl = EphemeralTable(tbl_dir, "es", schema)

    try:
        strings = ["alpha", "bravo", "charlie", "delta", "echo"]
        i = 0
        while i < 5:
            b = batch.ArenaZSetBatch(schema)
            rb = RowBuilder(schema, b)
            rb.begin(r_uint64(i + 1), r_uint64(0), r_int64(1))
            rb.put_string(strings[i])
            rb.commit()
            tbl.ingest_batch(b)
            tbl.flush()
            b.free()
            i += 1

        if not tbl.index.needs_compaction:
            raise Exception("Expected needs_compaction=True after 5 flushes")

        # Compaction must relocate string blobs without corruption
        c = tbl.create_cursor()

        if len(tbl.index.handles) != 0:
            raise Exception(
                "Expected 0 L0 handles post-compaction, got "
                + str(len(tbl.index.handles))
            )
        if len(tbl.index.levels) != 1:
            raise Exception(
                "Expected 1 L1 level post-compaction, got "
                + str(len(tbl.index.levels))
            )

        # All 5 string rows must survive blob relocation
        row_count = 0
        while c.is_valid():
            if c.weight() > r_int64(0):
                row_count += 1
            c.advance()
        c.close()

        if row_count != 5:
            raise Exception(
                "Expected 5 string rows post-compaction, got " + str(row_count)
            )

        # PK lookups (which exercise the xor8 filter on the new shard) must all succeed
        i = 1
        while i <= 5:
            if not tbl.has_pk(r_uint64(i), r_uint64(0)):
                raise Exception(
                    "PK=" + str(i) + " missing after string shard compaction"
                )
            i += 1

    finally:
        tbl.close()

    os.write(1, "[Storage] String column compaction Test Passed.\n")


def test_partitioned_compact_if_needed(base_dir):
    """PartitionedTable.compact_if_needed() propagates to each partition."""
    os.write(1, "[Storage] Testing PartitionedTable.compact_if_needed() propagation...\n")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    tbl_dir = os.path.join(base_dir, "part_compact")
    store = make_partitioned_ephemeral(tbl_dir, "pct", schema, 200, 2)

    try:
        # Flush each partition 5 times directly (bypasses hash routing)
        p = 0
        while p < 2:
            part = store.partitions[p]
            i = 1
            while i <= 5:
                b = batch.ArenaZSetBatch(schema)
                rb = RowBuilder(schema, b)
                rb.begin(r_uint64(p * 100 + i), r_uint64(0), r_int64(1))
                rb.put_int(r_int64(i * 10))
                rb.commit()
                part.ingest_batch(b)
                part.flush()
                b.free()
                i += 1
            p += 1

        if not store.partitions[0].index.needs_compaction:
            raise Exception("partition 0 should need compaction after 5 flushes")
        if not store.partitions[1].index.needs_compaction:
            raise Exception("partition 1 should need compaction after 5 flushes")

        # compact_if_needed() on PartitionedTable must delegate to each partition
        store.compact_if_needed()

        if len(store.partitions[0].index.handles) != 0:
            raise Exception(
                "partition 0: expected 0 L0 handles post-compaction, got "
                + str(len(store.partitions[0].index.handles))
            )
        if len(store.partitions[0].index.levels) != 1:
            raise Exception(
                "partition 0: expected 1 L1 level post-compaction, got "
                + str(len(store.partitions[0].index.levels))
            )
        if len(store.partitions[1].index.handles) != 0:
            raise Exception(
                "partition 1: expected 0 L0 handles post-compaction, got "
                + str(len(store.partitions[1].index.handles))
            )
        if len(store.partitions[1].index.levels) != 1:
            raise Exception(
                "partition 1: expected 1 L1 level post-compaction, got "
                + str(len(store.partitions[1].index.levels))
            )
        if store.partitions[0].index.needs_compaction:
            raise Exception("partition 0: needs_compaction must be False after compaction")
        if store.partitions[1].index.needs_compaction:
            raise Exception("partition 1: needs_compaction must be False after compaction")

    finally:
        local = 0
        while local < len(store.partitions):
            store.partitions[local].close()
            local += 1

    os.write(1, "[Storage] PartitionedTable compaction propagation Test Passed.\n")


# ------------------------------------------------------------------------------
# Partitioned Table Tests
# ------------------------------------------------------------------------------


def test_partition_basics():
    log("test_partition_basics (num_partitions + routing determinism)")
    # System tables (table_id < FIRST_USER_TABLE_ID) get 1 partition
    for tid in range(1, FIRST_USER_TABLE_ID):
        assert_equal_i(1, get_num_partitions(tid),
                       "system table %d should have 1 partition" % tid)
    # User tables get NUM_PARTITIONS
    assert_equal_i(NUM_PARTITIONS, get_num_partitions(FIRST_USER_TABLE_ID),
                   "first user table should have 256 partitions")
    assert_equal_i(NUM_PARTITIONS, get_num_partitions(1000),
                   "user table 1000 should have 256 partitions")
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

    schema = _make_pt_schema()
    table_id = 5  # system table

    store = make_partitioned_persistent(d, "sys_test", schema, table_id, 1)

    # Ingest a few rows
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    for i in range(10):
        rb.begin(r_uint64(i + 1), r_uint64(0), r_int64(1))
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
    assert_true(store.has_pk(r_uint64(1), r_uint64(0)), "pk=1 should exist")
    assert_true(not store.has_pk(r_uint64(999), r_uint64(0)), "pk=999 should not exist")

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

    schema = _make_pt_schema()
    table_id = FIRST_USER_TABLE_ID + 1

    store = make_partitioned_persistent(d, "user_test", schema, table_id, NUM_PARTITIONS)

    num_rows = 1000

    # Ingest 1000 rows
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    for i in range(num_rows):
        rb.begin(r_uint64(i + 1), r_uint64(0), r_int64(1))
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
        assert_true(store.has_pk(r_uint64(i + 1), r_uint64(0)), "pk=%d should exist" % (i + 1))

    assert_true(not store.has_pk(r_uint64(0), r_uint64(0)), "pk=0 should not exist")
    assert_true(not store.has_pk(r_uint64(num_rows + 1), r_uint64(0)),
                "pk=%d should not exist" % (num_rows + 1))

    log("  PASSED")


def test_n256_sorted_output(base_dir):
    log("test_n256_sorted_output (cursor merge produces global sort)")
    d = base_dir + "/n256_sorted"
    os.mkdir(d)

    schema = _make_pt_schema()
    table_id = FIRST_USER_TABLE_ID + 2

    store = make_partitioned_persistent(d, "sort_test", schema, table_id, NUM_PARTITIONS)

    num_rows = 500

    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    for i in range(num_rows):
        rb.begin(r_uint64(i + 1), r_uint64(0), r_int64(1))
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

    schema = _make_pt_schema()
    table_id = FIRST_USER_TABLE_ID + 3

    store = make_partitioned_persistent(d, "flush_test", schema, table_id, NUM_PARTITIONS)

    # Ingest first batch
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    for i in range(100):
        rb.begin(r_uint64(i + 1), r_uint64(0), r_int64(1))
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
        rb2.begin(r_uint64(i + 1), r_uint64(0), r_int64(1))
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

    schema = _make_pt_schema()
    table_id = FIRST_USER_TABLE_ID + 4

    store = make_partitioned_ephemeral(d, "eph_test", schema, table_id, NUM_PARTITIONS)

    num_rows = 300

    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    for i in range(num_rows):
        rb.begin(r_uint64(i + 1), r_uint64(0), r_int64(1))
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

    schema = _make_pt_schema()
    table_id = FIRST_USER_TABLE_ID + 5

    store = make_partitioned_ephemeral(d, "child_test", schema, table_id, NUM_PARTITIONS)

    child = store.create_child("scratch", schema)
    # child should be usable as a store
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    rb.begin(r_uint64(42), r_uint64(0), r_int64(1))
    rb.put_int(r_int64(99))
    rb.commit()
    child.ingest_batch(b)
    b.free()

    assert_true(child.has_pk(r_uint64(42), r_uint64(0)), "child should have pk=42")

    child.close()
    store.close()
    log("  PASSED")


def test_retract_pk_partitioned(base_dir):
    log("test_retract_pk_partitioned")
    d = base_dir + "/retract_part"
    os.mkdir(d)

    schema = _make_pt_schema()
    table_id = FIRST_USER_TABLE_ID + 10

    store = make_partitioned_persistent(d, "retract_test", schema, table_id, NUM_PARTITIONS)

    # Insert rows
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    for i in range(20):
        rb.begin(r_uint64(i + 1), r_uint64(0), r_int64(1))
        rb.put_int(r_int64(i * 10))
        rb.commit()
    store.ingest_batch(b)
    b.free()

    # retract_pk for an existing key
    out = ArenaZSetBatch(schema)
    result = store.retract_pk(r_uint64(5), r_uint64(0), out)
    assert_true(result, "retract_pk partitioned should return True for pk=5")
    assert_equal_i(1, out.length(), "retract_pk partitioned should emit 1 row")
    assert_true(out.get_weight(0) == r_int64(-1), "retract weight should be -1")
    assert_true(out.get_pk(0) == r_uint128(5), "retract pk should be 5")
    out.free()

    store.close()
    log("  PASSED")


def test_retract_pk_partitioned_absent(base_dir):
    log("test_retract_pk_partitioned_absent")
    d = base_dir + "/retract_part_absent"
    os.mkdir(d)

    schema = _make_pt_schema()
    table_id = FIRST_USER_TABLE_ID + 11

    store = make_partitioned_persistent(d, "retract_absent", schema, table_id, NUM_PARTITIONS)

    out = ArenaZSetBatch(schema)
    result = store.retract_pk(r_uint64(12345), r_uint64(0), out)
    assert_true(not result, "retract_pk partitioned absent should return False")
    assert_equal_i(0, out.length(), "retract_pk partitioned absent out should be empty")
    out.free()

    store.close()
    log("  PASSED")


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------


def entry_point(argv):
    ensure_jit_reachable()
    os.write(1, "--- GnitzDB Comprehensive Storage Test ---\n")
    base_dir = "storage_test_data"
    cleanup_dir(base_dir)
    os.mkdir(base_dir)

    try:
        test_integrity_and_lowlevel(base_dir)
        test_ephemeral_and_persistent_tables(base_dir)
        test_u128_payloads(base_dir)
        test_retract_pk(base_dir)
        test_enforce_unique_pk(base_dir)
        test_partition_basics()
        test_n1_persistent(base_dir)
        test_n256_persistent(base_dir)
        test_n256_sorted_output(base_dir)
        test_n256_flush_and_rescan(base_dir)
        test_n256_ephemeral(base_dir)
        test_create_child(base_dir)
        test_retract_pk_partitioned(base_dir)
        test_retract_pk_partitioned_absent(base_dir)
        os.write(1, "\nALL STORAGE TEST PATHS PASSED\n")
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
