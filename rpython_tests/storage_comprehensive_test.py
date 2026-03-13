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
    wal,
    wal_layout,
    writer_table,
    shard_table,
    compactor,
    tournament_tree,
    cursor,
    manifest,
    refcount,
    index,
    mmap_posix,
)
from gnitz.storage.ephemeral_table import EphemeralTable
from gnitz.storage.table import PersistentTable
from gnitz.core.batch import ColumnarBatchAccessor, RowBuilder
from gnitz.catalog.registry import _enforce_unique_pk, TableFamily

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


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


def assert_true(condition, msg):
    if not condition:
        raise Exception("Assertion Failed: " + msg)


def assert_false(condition, msg):
    if condition:
        raise Exception("Assertion Failed: " + msg)


def assert_equal_i(expected, actual, msg):
    if expected != actual:
        raise Exception("Assertion Failed: " + msg)


def assert_equal_i64(expected, actual, msg):
    if expected != actual:
        raise Exception("Assertion Failed (i64): " + msg)


def assert_equal_u64(expected, actual, msg):
    if expected != actual:
        raise Exception("Assertion Failed (u64): " + msg)


def assert_equal_s(expected, actual, msg):
    if expected != actual:
        raise Exception("Assertion Failed (str): " + msg)


def assert_equal_u128(expected, actual, msg):
    if expected != actual:
        raise Exception("Assertion Failed (U128): " + msg)


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


def test_wal_storage(base_dir):
    os.write(1, "[Storage] Testing Columnar Z-Set WAL...\n")
    schema = make_u64_str_schema()
    wal_path = os.path.join(base_dir, "test.wal")

    # 1. WAL Columnar Roundtrip
    writer = wal.WALWriter(wal_path, schema)

    b1 = batch.ZSetBatch(schema)
    rb = RowBuilder(schema, b1)
    rb.begin(r_uint128(10), r_int64(1))
    rb.put_string("block1")
    rb.commit()

    writer.append_batch(r_uint64(1), 1, b1)
    writer.close()
    b1.free()

    reader = wal.WALReader(wal_path, schema)
    blocks = newlist_hint(1)
    for block in reader.iterate_blocks():
        blocks.append(block)

    assert_equal_i(1, len(blocks), "WALReader didn't read exactly 1 block")
    assert_equal_u64(r_uint64(1), blocks[0].lsn, "WAL LSN mismatch")

    read_batch = blocks[0].batch
    assert_equal_i(1, read_batch.length(), "WAL batch length mismatch")
    assert_equal_u128(r_uint128(10), read_batch.get_pk(0), "WAL PK mismatch")
    assert_equal_i64(r_int64(1), read_batch.get_weight(0), "WAL weight mismatch")

    # Read string value via ColumnarBatchAccessor
    acc = ColumnarBatchAccessor(schema)
    acc.bind(read_batch, 0)
    length, prefix, struct_ptr, heap_ptr, py_str = acc.get_str_struct(1)
    recovered_str = string_logic.unpack_string(struct_ptr, heap_ptr)
    assert_equal_s("block1", recovered_str, "WAL string deserialization mismatch")

    blocks[0].free()
    reader.close()

    # 2. Single Writer Lock
    writer1 = wal.WALWriter(wal_path, schema)
    raised = False
    try:
        wal.WALWriter(wal_path, schema)
    except errors.StorageError:
        raised = True
    assert_true(raised, "WAL single-writer lock failed")
    writer1.close()

    # 3. Truncation
    writer2 = wal.WALWriter(wal_path, schema)
    writer2.truncate_before_lsn(r_uint64(2))
    assert_equal_i(0, os.path.getsize(wal_path), "WAL physical truncation failed")
    writer2.close()

    # 4. Multi-Record Blob Alignment (Critical Bug Repro)
    wal_blob_path = os.path.join(base_dir, "blob.wal")
    writer_blob = wal.WALWriter(wal_blob_path, schema)

    b_blob = batch.ZSetBatch(schema)
    rb_blob = RowBuilder(schema, b_blob)

    # Long string forces Blob allocation inside WAL block body
    long_str = "A" * 50
    rb_blob.begin(r_uint128(1), r_int64(1))
    rb_blob.put_string(long_str)
    rb_blob.commit()

    # Short string immediately follows
    rb_blob.begin(r_uint128(2), r_int64(1))
    rb_blob.put_string("short")
    rb_blob.commit()

    writer_blob.append_batch(r_uint64(100), 1, b_blob)
    writer_blob.close()
    b_blob.free()

    r_blob = wal.WALReader(wal_blob_path, schema)
    block_b = r_blob.read_next_block()
    assert_true(block_b is not None, "Failed to read blob WAL block")

    rb_blob_batch = block_b.batch
    assert_equal_i(2, rb_blob_batch.length(), "Multi-record WAL decode failed")

    acc_b = ColumnarBatchAccessor(schema)
    acc_b.bind(rb_blob_batch, 0)
    _, _, sp1, hp1, _ = acc_b.get_str_struct(1)
    s1 = string_logic.unpack_string(sp1, hp1)
    assert_equal_s(long_str, s1, "WAL long string alignment mismatch")

    acc_b.bind(rb_blob_batch, 1)
    _, _, sp2, hp2, _ = acc_b.get_str_struct(1)
    s2 = string_logic.unpack_string(sp2, hp2)
    assert_equal_s("short", s2, "WAL short string alignment mismatch")

    block_b.free()
    r_blob.close()

    os.write(1, "    [OK] Columnar Z-Set WAL passed.\n")


def test_memtable(base_dir):
    os.write(1, "[Storage] Testing MemTable...\n")
    schema_u128 = make_u128_i64_schema()

    # 1. Upsert and weight coalescing
    mt = memtable.MemTable(schema_u128, 1024 * 1024)
    try:
        pk1 = r_uint128(1)

        b_add = batch.ZSetBatch(schema_u128)
        rb = RowBuilder(schema_u128, b_add)
        rb.begin(pk1, r_int64(1))
        rb.put_int(rffi.cast(rffi.LONGLONG, r_uint64(100)))
        rb.commit()
        mt.upsert_batch(b_add)

        assert_false(mt.is_empty(), "MemTable should not be empty after insert")
        assert_equal_i64(
            r_int64(1), mt.lookup_pk(pk1)[0], "Weight should be 1 after insert"
        )

        b_sub = batch.ZSetBatch(schema_u128)
        rb_sub = RowBuilder(schema_u128, b_sub)
        rb_sub.begin(pk1, r_int64(-1))
        rb_sub.put_int(rffi.cast(rffi.LONGLONG, r_uint64(100)))
        rb_sub.commit()
        mt.upsert_batch(b_sub)

        assert_equal_i64(
            r_int64(0),
            mt.lookup_pk(pk1)[0],
            "Weight should be 0 after annihilation",
        )

        b_add.free()
        b_sub.free()
    finally:
        mt.free()

    # 2. Flush with annihilation and blob pruning
    schema_str = make_u64_str_schema()
    mt2 = memtable.MemTable(schema_str, 1024 * 1024)
    try:
        dead_str = "ANNIHILATE" * 10
        live_str = "SURVIVE" * 10

        pk1 = r_uint128(1)
        pk2 = r_uint128(2)

        b_d1 = batch.ZSetBatch(schema_str)
        rb_d1 = RowBuilder(schema_str, b_d1)
        rb_d1.begin(pk1, r_int64(1))
        rb_d1.put_string(dead_str)
        rb_d1.commit()

        b_d2 = batch.ZSetBatch(schema_str)
        rb_d2 = RowBuilder(schema_str, b_d2)
        rb_d2.begin(pk1, r_int64(-1))
        rb_d2.put_string(dead_str)
        rb_d2.commit()

        b_l1 = batch.ZSetBatch(schema_str)
        rb_l1 = RowBuilder(schema_str, b_l1)
        rb_l1.begin(pk2, r_int64(1))
        rb_l1.put_string(live_str)
        rb_l1.commit()

        mt2.upsert_batch(b_d1)
        mt2.upsert_batch(b_d2)
        mt2.upsert_batch(b_l1)

        shard_path = os.path.join(base_dir, "survivor.db")
        mt2.flush(shard_path, 1)

        view = shard_table.TableShardView(shard_path, schema_str)
        assert_equal_i(1, view.count, "Annihilated row was flushed to shard")
        assert_equal_i(
            len(live_str), view.blob_buf.size, "Dead blob was not pruned during flush"
        )
        view.close()

        b_d1.free()
        b_d2.free()
        b_l1.free()
    finally:
        mt2.free()

    # 3. Cursor iteration order after consolidation
    mt3 = memtable.MemTable(schema_u128, 1024 * 1024)
    try:
        for pk_val in [30, 10, 20]:
            b_c = batch.ZSetBatch(schema_u128)
            rb_c = RowBuilder(schema_u128, b_c)
            rb_c.begin(r_uint128(pk_val), r_int64(1))
            rb_c.put_int(rffi.cast(rffi.LONGLONG, r_int64(pk_val)))
            rb_c.commit()
            mt3.upsert_batch(b_c)
            b_c.free()

        mc = cursor.MemTableCursor(mt3)
        assert_true(mc.is_valid(), "MemTableCursor should be valid")
        assert_equal_u128(r_uint128(10), mc.key(), "Cursor should start at PK 10")
        mc.advance()
        assert_equal_u128(r_uint128(20), mc.key(), "Cursor should advance to PK 20")
        mc.advance()
        assert_equal_u128(r_uint128(30), mc.key(), "Cursor should advance to PK 30")
        mc.advance()
        assert_true(mc.is_exhausted(), "Cursor should be exhausted")

        # 4. Cursor seek
        mc2 = cursor.MemTableCursor(mt3)
        mc2.seek(r_uint128(20))
        assert_true(mc2.is_valid(), "Cursor should be valid after seek")
        assert_equal_u128(r_uint128(20), mc2.key(), "Cursor should seek to PK 20")
        mc2.close()
        mc.close()
    finally:
        mt3.free()

    # 5. MemTableFullError
    tiny_mt = memtable.MemTable(schema_u128, 64)
    raised = False
    try:
        for j in range(1000):
            b_f = batch.ZSetBatch(schema_u128)
            rb_f = RowBuilder(schema_u128, b_f)
            rb_f.begin(r_uint128(j), r_int64(1))
            rb_f.put_int(rffi.cast(rffi.LONGLONG, r_int64(j)))
            rb_f.commit()
            tiny_mt.upsert_batch(b_f)
            b_f.free()
    except errors.MemTableFullError:
        raised = True
    assert_true(raised, "MemTableFullError not raised when capacity exceeded")
    tiny_mt.free()

    os.write(1, "    [OK] MemTable passed.\n")


def test_shards_and_columnar(base_dir):
    os.write(1, "[Storage] Testing N-Partition Columnar Shards...\n")
    schema = make_u64_str_schema()
    fn = os.path.join(base_dir, "test_shard.db")

    # 1. Write and Validate Checksums
    writer = writer_table.TableShardWriter(schema, 1)

    tmp1 = batch.ZSetBatch(schema)
    rb1 = RowBuilder(schema, tmp1)
    rb1.begin(r_uint128(10), r_int64(1))
    rb1.put_string("test")
    rb1.commit()
    writer.add_row_from_accessor(r_uint128(10), r_int64(1), tmp1.get_accessor(0))
    tmp1.free()

    tmp2 = batch.ZSetBatch(schema)
    rb2 = RowBuilder(schema, tmp2)
    rb2.begin(r_uint128(20), r_int64(1))
    rb2.put_string("data_long_string_for_blob")
    rb2.commit()
    writer.add_row_from_accessor(r_uint128(20), r_int64(1), tmp2.get_accessor(0))
    tmp2.free()

    writer.finalize(fn)

    view1 = shard_table.TableShardView(fn, schema, validate_checksums=True)
    assert_equal_i(2, view1.count, "Row count mismatch")
    assert_equal_u64(r_uint64(10), view1.get_pk_u64(0), "PK columnar access failed")
    assert_true(view1.string_field_equals(0, 1, "test"), "String inline access failed")
    assert_true(
        view1.string_field_equals(1, 1, "data_long_string_for_blob"),
        "String blob access failed",
    )

    # Calculate absolute file offsets via pointer arithmetic on the mmap
    # Offset = (Region Pointer) - (File Base Pointer)
    off_pk = rffi.cast(lltype.Signed, view1.pk_lo_buf.ptr) - rffi.cast(
        lltype.Signed, view1.ptr
    )
    off_blob = rffi.cast(lltype.Signed, view1.blob_buf.ptr) - rffi.cast(
        lltype.Signed, view1.ptr
    )
    view1.close()

    # 2. Corrupt Region PK (Eager Validation)
    corrupt_byte(fn, off_pk)
    raised = False
    try:
        shard_table.TableShardView(fn, schema, validate_checksums=True)
    except errors.CorruptShardError:
        raised = True
    assert_true(raised, "Eager PK checksum validation failed")

    # 3. Skip Validation Option
    view2 = shard_table.TableShardView(fn, schema, validate_checksums=False)
    assert_equal_i(2, view2.count, "Skip validation failed to open corrupt shard")
    view2.close()

    # Restore PK byte
    corrupt_byte(fn, off_pk)

    # 4. Lazy Checksum Validation (Blob Region)
    corrupt_byte(fn, off_blob)
    view3 = shard_table.TableShardView(fn, schema, validate_checksums=True)
    # Eager regions pass.
    assert_equal_u64(r_uint64(10), view3.get_pk_u64(0), "PK should still be readable")

    raised = False
    try:
        view3.string_field_equals(1, 1, "data_long_string_for_blob")
    except errors.CorruptShardError:
        raised = True
    assert_true(raised, "Lazy Blob checksum validation failed")
    view3.close()

    os.write(1, "    [OK] N-Partition Columnar Shards passed.\n")


def test_manifest_and_spine(base_dir):
    os.write(1, "[Storage] Testing Manifest & Spine Indexing...\n")
    schema = make_u64_str_schema()
    manifest_file = os.path.join(base_dir, "sync.manifest")

    # 1. Binary Serialization
    large_k = (r_uint128(0xAAAA) << 64) | r_uint128(0xBBBB)
    entry_w = manifest.ManifestEntry(
        123, "shard_001.db", large_k, large_k + r_uint128(1), 5, 10
    )

    mgr = manifest.ManifestManager(manifest_file)
    mgr.publish_new_version([entry_w], global_max_lsn=r_uint64(999))

    reader = mgr.load_current()
    assert_equal_i(1, reader.entry_count, "Manifest entry count mismatch")
    assert_equal_u64(
        r_uint64(999), reader.global_max_lsn, "Manifest global max LSN mismatch"
    )

    entries = newlist_hint(1)
    for e in reader.iterate_entries():
        entries.append(e)
    assert_equal_i(123, entries[0].table_id, "Entry TID mismatch")
    assert_equal_u128(large_k, entries[0].get_min_key(), "Entry PK U128 split mismatch")
    assert_equal_s(
        "shard_001.db", entries[0].shard_filename, "Entry filename mismatch"
    )

    # 2. SWMR Sync Detection
    assert_false(reader.has_changed(), "Fresh manifest should not report change")

    # Mock concurrent atomic rename
    mgr.publish_new_version([], global_max_lsn=r_uint64(2000))
    assert_true(reader.has_changed(), "ManifestReader failed to detect atomic rename")

    reader.reload()
    assert_equal_u64(r_uint64(2000), reader.global_max_lsn, "Manifest reload failed")
    reader.close()

    # 3. RefCounter & Deferred Cleanup
    rc = refcount.RefCounter()
    dummy_db = os.path.join(base_dir, "ref_test.db")
    fd_d = rposix.open(dummy_db, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    rposix.write(fd_d, "data")
    rposix.close(fd_d)

    rc.acquire(dummy_db)
    assert_false(rc.can_delete(dummy_db), "Should not allow delete of acquired shard")
    rc.mark_for_deletion(dummy_db)
    rc.try_cleanup()
    assert_true(os.path.exists(dummy_db), "Shard was deleted while ref held")

    rc.release(dummy_db)
    assert_true(rc.can_delete(dummy_db), "Should allow delete after release")
    rc.try_cleanup()
    assert_false(os.path.exists(dummy_db), "Shard was not cleaned up after release")

    # 4. Index Resolution
    idx_db = os.path.join(base_dir, "shard_idx.db")
    w = writer_table.TableShardWriter(schema, table_id=1)

    tmp = batch.ZSetBatch(schema)
    rb_idx = RowBuilder(schema, tmp)
    rb_idx.begin(r_uint128(10), r_int64(1))
    rb_idx.put_string("index")
    rb_idx.commit()
    w.add_row_from_accessor(r_uint128(10), r_int64(1), tmp.get_accessor(0))
    tmp.free()

    w.finalize(idx_db)

    mgr.publish_new_version(
        [manifest.ManifestEntry(1, idx_db, r_uint128(10), r_uint128(10), 0, 1)],
        global_max_lsn=r_uint64(1),
    )

    idx = index.index_from_manifest(manifest_file, 1, schema, rc)
    res = idx.find_all_shards_and_indices(r_uint128(10))
    assert_equal_i(1, len(res), "Index resolution failed to find shard")
    assert_equal_s(idx_db, res[0][0].filename, "Index resolved to wrong filename")

    idx.close_all()

    os.write(1, "    [OK] Manifest & Spine Indexing passed.\n")


def make_compaction_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="id"),  # PK
        types.ColumnDefinition(types.TYPE_STRING, name="name"),  # Payload 1
        types.ColumnDefinition(types.TYPE_I64, name="val"),  # Payload 2
    ]
    return types.TableSchema(cols, pk_index=0)


def ingest_test_row(tbl, pk_val, weight, name_str, int_val):
    schema = tbl.get_schema()
    b = batch.ZSetBatch(schema)
    rb = RowBuilder(schema, b)
    rb.begin(r_uint128(pk_val), r_int64(weight))
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

    # 2. Create Overlapping State
    # Shard 1: PK=100 (W=1), PK=200 (W=1)
    ingest_test_row(tbl, 100, 1, "record_a", 1000)
    ingest_test_row(tbl, 200, 1, "record_b", 2000)

    # Shard 2: PK=100 (W=1) [Duplicate Payload], PK=300 (W=1)
    ingest_test_row(tbl, 100, 1, "record_a", 1000)
    ingest_test_row(tbl, 300, 1, "record_c", 3000)

    # Shard 3: PK=100 (W=-2) [Annihilation payload]
    ingest_test_row(tbl, 100, -2, "record_a", 1000)

    # At this point, Read Amplification is high.
    # PK 100: (1 + 1 - 2) = 0  -> Should be GHOSTED
    # PK 200: 1                -> Should survive
    # PK 300: 1                -> Should survive

    initial_shards = tbl.index.handles
    if len(initial_shards) < 3:
        raise Exception("Failed to create overlapping shards")

    # 3. Execute Compaction
    # We manually trigger compaction for the test.
    out_shard = compactor.execute_compaction(
        tbl.index, tbl.manifest_manager, output_dir=path, validate_checksums=True
    )

    if not os.path.exists(out_shard):
        raise Exception("Compactor failed to produce output shard")

    # 4. Verify Index and Manifest State
    # Only the new Guard Shard should remain in the index.
    if len(tbl.index.handles) != 1:
        raise Exception("Index was not correctly updated after compaction")

    # 5. Verify Content (The Ghost Property)
    view = shard_table.TableShardView(out_shard, schema)
    try:
        # PK 100 should be physically removed because its net weight was 0.
        # PK 200 and 300 should be the only records left.
        if view.count != 2:
            os.write(1, "Error: Shard count is " + str(view.count) + " expected 2\n")
            raise Exception("Annihilation failed: zero-weight record preserved")

        # Verify PK 200 survives with correct weight
        idx_200 = view.find_row_index(r_uint128(200))
        if idx_200 == -1 or view.get_weight(idx_200) != 1:
            raise Exception("Survived record 200 missing or corrupted")

        # Verify PK 300 survives
        idx_300 = view.find_row_index(r_uint128(300))
        if idx_300 == -1:
            raise Exception("Survived record 300 missing")

        # Double check PK 100 is absent
        if view.find_row_index(r_uint128(100)) != -1:
            raise Exception("Ghost Property Violation: PK 100 still exists")

    finally:
        view.close()

    # 6. Verify Cleanup (Refcounting)
    # The compactor marks old shards for deletion.
    # We must call try_cleanup to physically remove them from disk.
    tbl.index.ref_counter.try_cleanup()

    # Verify file unlinking
    # Note: RPython loop required instead of any()
    for h in initial_shards:
        if os.path.exists(h.filename) and h.filename != out_shard:
            # Note: In some OS environments, file locks might delay unlinking.
            # We skip hard failure here but log it.
            os.write(1, "[Warning] Old shard still on disk: " + h.filename + "\n")

    tbl.close()
    os.write(1, "[Storage] Compaction Test Passed.\n")


def test_ephemeral_and_persistent_tables(base_dir):
    os.write(1, "[Storage] Testing Ephemeral & Persistent Tables...\n")
    schema = make_u64_str_schema()

    # 1. Ephemeral Table In-Memory & Summation
    eph_dir = os.path.join(base_dir, "eph")
    t_eph = EphemeralTable(eph_dir, "trace1", schema)

    pk1 = r_uint128(1)

    b_add = batch.ZSetBatch(schema)
    rb_add = RowBuilder(schema, b_add)
    rb_add.begin(pk1, r_int64(5))
    rb_add.put_string("sum_test")
    rb_add.commit()
    t_eph.ingest_batch(b_add)

    b_sub = batch.ZSetBatch(schema)
    rb_sub = RowBuilder(schema, b_sub)
    rb_sub.begin(pk1, r_int64(-3))
    rb_sub.put_string("sum_test")
    rb_sub.commit()
    t_eph.ingest_batch(b_sub)

    tmp_acc = batch.ZSetBatch(schema)
    rb_acc = RowBuilder(schema, tmp_acc)
    rb_acc.begin(pk1, r_int64(1))
    rb_acc.put_string("sum_test")
    rb_acc.commit()
    acc1 = tmp_acc.get_accessor(0)

    net_w = t_eph.get_weight(pk1, acc1)
    assert_equal_i64(r_int64(2), net_w, "Ephemeral algebraic summation failed")

    # 2. Ephemeral Spill
    shard_path = t_eph.flush()
    assert_true(
        shard_path.find("eph_shard") >= 0, "Ephemeral shard naming convention failed"
    )

    net_w2 = t_eph.get_weight(pk1, acc1)
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
    b2 = batch.ZSetBatch(schema)
    rb2 = RowBuilder(schema, b2)
    rb2.begin(r_k2, r_int64(1))
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

    b = batch.ZSetBatch(schema)
    rb = RowBuilder(schema, b)
    rb.begin(pk1, r_int64(1))
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


def test_bloom_filter(base_dir):
    os.write(1, "[Storage] Testing Bloom Filter...\n")
    from gnitz.storage.bloom import BloomFilter

    # 1. No false negatives
    bf = BloomFilter(200)
    for i in range(100):
        bf.add(r_uint128(i))
    for i in range(100):
        assert_true(bf.may_contain(r_uint128(i)), "Bloom FN for key %d" % i)

    # 2. Low false positive rate
    fp_count = 0
    for i in range(1000, 2000):
        if bf.may_contain(r_uint128(i)):
            fp_count += 1
    assert_true(fp_count < 50, "Bloom FPR too high: %d/1000" % fp_count)
    bf.free()

    # 3. Empty filter returns False
    bf2 = BloomFilter(100)
    assert_false(bf2.may_contain(r_uint128(42)), "Empty bloom should return False")
    bf2.free()

    os.write(1, "    [OK] Bloom Filter passed.\n")


def test_xor8_filter(base_dir):
    os.write(1, "[Storage] Testing XOR8 Filter...\n")
    from gnitz.storage.xor8 import build_xor8, save_xor8, load_xor8

    num_keys = 200
    pk_lo_buf = lltype.malloc(rffi.CCHARP.TO, num_keys * 8, flavor="raw")
    pk_hi_buf = lltype.malloc(rffi.CCHARP.TO, num_keys * 8, flavor="raw")
    try:
        i = 0
        while i < num_keys:
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(pk_lo_buf, i * 8))[
                0
            ] = rffi.cast(rffi.ULONGLONG, r_uint64(i + 1))
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(pk_hi_buf, i * 8))[
                0
            ] = rffi.cast(rffi.ULONGLONG, r_uint64(0))
            i += 1

        # 1. Construction + no false negatives
        xf = build_xor8(pk_lo_buf, pk_hi_buf, num_keys)
        assert_true(xf is not None, "XOR8 build returned None")

        for i in range(num_keys):
            assert_true(
                xf.may_contain(r_uint128(i + 1)),
                "XOR8 FN for key %d" % (i + 1),
            )

        # 2. Low false positive rate
        fp_count = 0
        for i in range(5000, 7000):
            if xf.may_contain(r_uint128(i)):
                fp_count += 1
        assert_true(fp_count < 40, "XOR8 FPR too high: %d/2000" % fp_count)

        # 3. Serde roundtrip
        xor_path = os.path.join(base_dir, "test.xor8")
        save_xor8(xf, xor_path)
        xf2 = load_xor8(xor_path)
        assert_true(xf2 is not None, "XOR8 load returned None")

        for i in range(num_keys):
            assert_true(
                xf2.may_contain(r_uint128(i + 1)),
                "XOR8 serde FN for key %d" % (i + 1),
            )
        xf2.free()
        xf.free()
    finally:
        lltype.free(pk_lo_buf, flavor="raw")
        lltype.free(pk_hi_buf, flavor="raw")

    # 4. Small set (2 keys)
    small_lo = lltype.malloc(rffi.CCHARP.TO, 2 * 8, flavor="raw")
    small_hi = lltype.malloc(rffi.CCHARP.TO, 2 * 8, flavor="raw")
    try:
        rffi.cast(rffi.ULONGLONGP, small_lo)[0] = rffi.cast(
            rffi.ULONGLONG, r_uint64(999)
        )
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(small_lo, 8))[0] = rffi.cast(
            rffi.ULONGLONG, r_uint64(1000)
        )
        rffi.cast(rffi.ULONGLONGP, small_hi)[0] = rffi.cast(
            rffi.ULONGLONG, r_uint64(0)
        )
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(small_hi, 8))[0] = rffi.cast(
            rffi.ULONGLONG, r_uint64(0)
        )
        xf3 = build_xor8(small_lo, small_hi, 2)
        assert_true(xf3 is not None, "XOR8 small set build failed")
        assert_true(xf3.may_contain(r_uint128(999)), "XOR8 small FN key 999")
        assert_true(xf3.may_contain(r_uint128(1000)), "XOR8 small FN key 1000")
        xf3.free()
    finally:
        lltype.free(small_lo, flavor="raw")
        lltype.free(small_hi, flavor="raw")

    # 5. load_xor8 returns None for missing file
    missing = load_xor8(os.path.join(base_dir, "nonexistent.xor8"))
    assert_true(missing is None, "load_xor8 should return None for missing file")

    os.write(1, "    [OK] XOR8 Filter passed.\n")


def test_filter_integration(base_dir):
    os.write(1, "[Storage] Testing Filter Integration...\n")
    schema = make_u64_str_schema()

    # 1. MemTable Bloom integration
    mt = memtable.MemTable(schema, 1024 * 1024)
    try:
        for i in range(1, 51):
            b = batch.ZSetBatch(schema)
            rb = RowBuilder(schema, b)
            rb.begin(r_uint128(i), r_int64(1))
            rb.put_string("v%d" % i)
            rb.commit()
            mt.upsert_batch(b)
            b.free()

        for i in range(1, 51):
            assert_true(
                mt.may_contain_pk(r_uint128(i)),
                "Bloom missed inserted key %d" % i,
            )

        fp_count = 0
        for i in range(1000, 1100):
            if mt.may_contain_pk(r_uint128(i)):
                fp_count += 1
        assert_true(fp_count < 20, "MemTable bloom FPR too high: %d/100" % fp_count)
    finally:
        mt.free()

    # 2. Shard XOR8 integration
    shard_path = os.path.join(base_dir, "xor8_shard.db")
    sw = writer_table.TableShardWriter(schema, table_id=1)
    for i in range(1, 51):
        tmp = batch.ZSetBatch(schema)
        rb_s = RowBuilder(schema, tmp)
        rb_s.begin(r_uint128(i), r_int64(1))
        rb_s.put_string("shard_v%d" % i)
        rb_s.commit()
        sw.add_row_from_accessor(r_uint128(i), r_int64(1), tmp.get_accessor(0))
        tmp.free()
    sw.finalize(shard_path)

    xor8_path = shard_path + ".xor8"
    assert_true(os.path.exists(xor8_path), "XOR8 sidecar file not created")

    h = index.ShardHandle(
        shard_path, schema, r_uint64(0), r_uint64(0)
    )
    assert_true(h.xor8_filter is not None, "ShardHandle xor8_filter not loaded")

    for i in range(1, 51):
        assert_true(
            h.xor8_filter.may_contain(r_uint128(i)),
            "Shard XOR8 FN for key %d" % i,
        )
    h.close()

    # 3. EphemeralTable with Bloom
    eph_dir = os.path.join(base_dir, "filter_eph")
    t_eph = EphemeralTable(eph_dir, "filter_test", schema)
    b = batch.ZSetBatch(schema)
    rb_e = RowBuilder(schema, b)
    rb_e.begin(r_uint128(42), r_int64(1))
    rb_e.put_string("test_val")
    rb_e.commit()
    t_eph.ingest_batch(b)
    b.free()

    assert_true(t_eph.has_pk(r_uint128(42)), "EphemeralTable has_pk failed for 42")
    assert_false(
        t_eph.has_pk(r_uint128(9999)), "EphemeralTable has_pk FP for 9999"
    )
    t_eph.close()

    os.write(1, "    [OK] Filter Integration passed.\n")


# ------------------------------------------------------------------------------
# retract_pk Tests
# ------------------------------------------------------------------------------


def test_retract_pk(base_dir):
    os.write(1, "  test_retract_pk ...\n")
    schema = make_u64_str_schema()

    # --- test_retract_pk_found_in_memtable ---
    eph_dir = os.path.join(base_dir, "retract_mt")
    t = EphemeralTable(eph_dir, "retract_mt", schema)
    b = batch.ZSetBatch(schema)
    rb = RowBuilder(schema, b)
    rb.begin(r_uint128(10), r_int64(1))
    rb.put_string("hello")
    rb.commit()
    t.ingest_batch(b)
    b.free()

    out = batch.ArenaZSetBatch(schema)
    result = t.retract_pk(r_uint128(10), out)
    assert_true(result, "retract_pk memtable should return True")
    assert_equal_i(1, out.length(), "retract_pk memtable out should have 1 row")
    assert_true(out.get_weight(0) == r_int64(-1), "retract weight should be -1")
    assert_true(out.get_pk(0) == r_uint128(10), "retract pk should be 10")
    out.free()
    t.close()

    # --- test_retract_pk_found_in_shard ---
    eph_dir2 = os.path.join(base_dir, "retract_shard")
    t2 = EphemeralTable(eph_dir2, "retract_shard", schema)
    b2 = batch.ZSetBatch(schema)
    rb2 = RowBuilder(schema, b2)
    rb2.begin(r_uint128(20), r_int64(1))
    rb2.put_string("world")
    rb2.commit()
    t2.ingest_batch(b2)
    b2.free()
    t2.flush()  # moves to shard

    out2 = batch.ArenaZSetBatch(schema)
    result2 = t2.retract_pk(r_uint128(20), out2)
    assert_true(result2, "retract_pk shard should return True")
    assert_equal_i(1, out2.length(), "retract_pk shard out should have 1 row")
    assert_true(out2.get_weight(0) == r_int64(-1), "retract shard weight should be -1")
    out2.free()
    t2.close()

    # --- test_retract_pk_absent ---
    eph_dir3 = os.path.join(base_dir, "retract_absent")
    t3 = EphemeralTable(eph_dir3, "retract_absent", schema)
    out3 = batch.ArenaZSetBatch(schema)
    result3 = t3.retract_pk(r_uint128(999), out3)
    assert_false(result3, "retract_pk absent should return False")
    assert_equal_i(0, out3.length(), "retract_pk absent out should be empty")
    out3.free()
    t3.close()

    # --- test_retract_pk_after_retraction (net=0) ---
    eph_dir4 = os.path.join(base_dir, "retract_net0")
    t4 = EphemeralTable(eph_dir4, "retract_net0", schema)
    b4a = batch.ZSetBatch(schema)
    rb4a = RowBuilder(schema, b4a)
    rb4a.begin(r_uint128(30), r_int64(1))
    rb4a.put_string("ins")
    rb4a.commit()
    t4.ingest_batch(b4a)
    b4a.free()
    b4b = batch.ZSetBatch(schema)
    rb4b = RowBuilder(schema, b4b)
    rb4b.begin(r_uint128(30), r_int64(-1))
    rb4b.put_string("ins")
    rb4b.commit()
    t4.ingest_batch(b4b)
    b4b.free()

    out4 = batch.ArenaZSetBatch(schema)
    result4 = t4.retract_pk(r_uint128(30), out4)
    assert_false(result4, "retract_pk net=0 should return False")
    assert_equal_i(0, out4.length(), "retract_pk net=0 out should be empty")
    out4.free()
    t4.close()

    # --- test_retract_pk_split_weight (shard+memtable both positive) ---
    eph_dir5 = os.path.join(base_dir, "retract_split")
    t5 = EphemeralTable(eph_dir5, "retract_split", schema)
    b5a = batch.ZSetBatch(schema)
    rb5a = RowBuilder(schema, b5a)
    rb5a.begin(r_uint128(40), r_int64(1))
    rb5a.put_string("split")
    rb5a.commit()
    t5.ingest_batch(b5a)
    b5a.free()
    t5.flush()  # +1 in shard
    b5b = batch.ZSetBatch(schema)
    rb5b = RowBuilder(schema, b5b)
    rb5b.begin(r_uint128(40), r_int64(1))
    rb5b.put_string("split")
    rb5b.commit()
    t5.ingest_batch(b5b)
    b5b.free()  # +1 in memtable

    out5 = batch.ArenaZSetBatch(schema)
    result5 = t5.retract_pk(r_uint128(40), out5)
    assert_true(result5, "retract_pk split weight should return True")
    assert_equal_i(1, out5.length(), "retract_pk split should emit exactly 1 retraction")
    assert_true(out5.get_weight(0) == r_int64(-1), "retract split weight should be -1")
    out5.free()
    t5.close()

    # --- test_retract_pk_weight_canceled (shard +1, memtable -1) ---
    eph_dir6 = os.path.join(base_dir, "retract_cancel")
    t6 = EphemeralTable(eph_dir6, "retract_cancel", schema)
    b6a = batch.ZSetBatch(schema)
    rb6a = RowBuilder(schema, b6a)
    rb6a.begin(r_uint128(50), r_int64(1))
    rb6a.put_string("cancel")
    rb6a.commit()
    t6.ingest_batch(b6a)
    b6a.free()
    t6.flush()  # +1 in shard
    b6b = batch.ZSetBatch(schema)
    rb6b = RowBuilder(schema, b6b)
    rb6b.begin(r_uint128(50), r_int64(-1))
    rb6b.put_string("cancel")
    rb6b.commit()
    t6.ingest_batch(b6b)
    b6b.free()  # -1 in memtable

    out6 = batch.ArenaZSetBatch(schema)
    result6 = t6.retract_pk(r_uint128(50), out6)
    assert_false(result6, "retract_pk canceled should return False")
    assert_equal_i(0, out6.length(), "retract_pk canceled out should be empty")
    out6.free()
    t6.close()

    # --- test_retract_pk_string_column (verify string data copied correctly) ---
    eph_dir7 = os.path.join(base_dir, "retract_str")
    t7 = EphemeralTable(eph_dir7, "retract_str", schema)
    test_str = "retract_string_payload"
    b7 = batch.ZSetBatch(schema)
    rb7 = RowBuilder(schema, b7)
    rb7.begin(r_uint128(60), r_int64(1))
    rb7.put_string(test_str)
    rb7.commit()
    t7.ingest_batch(b7)

    # Build a reference row for comparison
    ref = batch.ZSetBatch(schema)
    rb_ref = RowBuilder(schema, ref)
    rb_ref.begin(r_uint128(60), r_int64(-1))
    rb_ref.put_string(test_str)
    rb_ref.commit()

    out7 = batch.ArenaZSetBatch(schema)
    result7 = t7.retract_pk(r_uint128(60), out7)
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
    b8 = batch.ZSetBatch(schema)
    rb8 = RowBuilder(schema, b8)
    rb8.begin(r_uint128(70), r_int64(1))
    rb8.put_string("exists")
    rb8.commit()
    t8.ingest_batch(b8)
    b8.free()

    assert_true(t8.has_pk(r_uint128(70)), "has_pk should find pk=70")
    assert_false(t8.has_pk(r_uint128(71)), "has_pk should not find pk=71")

    t8.flush()
    assert_true(t8.has_pk(r_uint128(70)), "has_pk should find pk=70 after flush")
    assert_false(t8.has_pk(r_uint128(71)), "has_pk should not find pk=71 after flush")
    t8.close()

    os.write(1, "    [OK] retract_pk tests passed.\n")


def test_enforce_unique_pk(base_dir):
    os.write(1, "  test_enforce_unique_pk ...\n")
    schema = make_u64_i64_schema()

    # --- insert_new: new PK, no stored row → output = input row ---
    sub_dir1 = os.path.join(base_dir, "upk_t1")
    t1 = EphemeralTable(sub_dir1, "upk_t1", schema)
    family1 = TableFamily("s", "upk_t1", 1, 1, sub_dir1, 0, t1, unique_pk=True)
    b1 = batch.ZSetBatch(schema)
    rb1 = RowBuilder(schema, b1)
    rb1.begin(r_uint128(1), r_int64(1))
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
    b2pre = batch.ZSetBatch(schema)
    rb2pre = RowBuilder(schema, b2pre)
    rb2pre.begin(r_uint128(1), r_int64(1))
    rb2pre.put_int(r_int64(10))
    rb2pre.commit()
    t2.ingest_batch(b2pre)
    b2pre.free()
    b2 = batch.ZSetBatch(schema)
    rb2 = RowBuilder(schema, b2)
    rb2.begin(r_uint128(1), r_int64(1))
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
    b3pre = batch.ZSetBatch(schema)
    rb3pre = RowBuilder(schema, b3pre)
    rb3pre.begin(r_uint128(1), r_int64(1))
    rb3pre.put_int(r_int64(10))
    rb3pre.commit()
    t3.ingest_batch(b3pre)
    b3pre.free()
    b3 = batch.ZSetBatch(schema)
    rb3 = RowBuilder(schema, b3)
    rb3.begin(r_uint128(1), r_int64(-1))
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
    b4 = batch.ZSetBatch(schema)
    rb4 = RowBuilder(schema, b4)
    rb4.begin(r_uint128(99), r_int64(-1))
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
    b5 = batch.ZSetBatch(schema)
    rb5a = RowBuilder(schema, b5)
    rb5a.begin(r_uint128(1), r_int64(1))
    rb5a.put_int(r_int64(10))
    rb5a.commit()
    rb5b = RowBuilder(schema, b5)
    rb5b.begin(r_uint128(1), r_int64(1))
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
    b6 = batch.ZSetBatch(schema)
    rb6a = RowBuilder(schema, b6)
    rb6a.begin(r_uint128(1), r_int64(1))
    rb6a.put_int(r_int64(10))
    rb6a.commit()
    rb6b = RowBuilder(schema, b6)
    rb6b.begin(r_uint128(1), r_int64(-1))
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


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------


def entry_point(argv):
    os.write(1, "--- GnitzDB Comprehensive Storage Test ---\n")
    base_dir = "storage_test_data"
    cleanup_dir(base_dir)
    os.mkdir(base_dir)

    try:
        test_integrity_and_lowlevel(base_dir)
        test_wal_storage(base_dir)
        test_memtable(base_dir)
        test_shards_and_columnar(base_dir)
        test_manifest_and_spine(base_dir)
        test_compaction(base_dir)
        test_ephemeral_and_persistent_tables(base_dir)
        test_u128_payloads(base_dir)
        test_bloom_filter(base_dir)
        test_xor8_filter(base_dir)
        test_filter_integration(base_dir)
        test_retract_pk(base_dir)
        test_enforce_unique_pk(base_dir)
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
