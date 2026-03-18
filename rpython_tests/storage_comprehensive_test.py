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
    flsm,
    mmap_posix,
)
from gnitz.storage.ephemeral_table import EphemeralTable
from gnitz.storage.table import PersistentTable
from gnitz.storage.partitioned_table import make_partitioned_ephemeral
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

    b1 = batch.ArenaZSetBatch(schema)
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

    b_blob = batch.ArenaZSetBatch(schema)
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

        b_add = batch.ArenaZSetBatch(schema_u128)
        rb = RowBuilder(schema_u128, b_add)
        rb.begin(pk1, r_int64(1))
        rb.put_int(rffi.cast(rffi.LONGLONG, r_uint64(100)))
        rb.commit()
        mt.upsert_batch(b_add)

        assert_false(mt.is_empty(), "MemTable should not be empty after insert")
        assert_equal_i64(
            r_int64(1), mt.lookup_pk(pk1)[0], "Weight should be 1 after insert"
        )

        b_sub = batch.ArenaZSetBatch(schema_u128)
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

        b_d1 = batch.ArenaZSetBatch(schema_str)
        rb_d1 = RowBuilder(schema_str, b_d1)
        rb_d1.begin(pk1, r_int64(1))
        rb_d1.put_string(dead_str)
        rb_d1.commit()

        b_d2 = batch.ArenaZSetBatch(schema_str)
        rb_d2 = RowBuilder(schema_str, b_d2)
        rb_d2.begin(pk1, r_int64(-1))
        rb_d2.put_string(dead_str)
        rb_d2.commit()

        b_l1 = batch.ArenaZSetBatch(schema_str)
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
            b_c = batch.ArenaZSetBatch(schema_u128)
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
            b_f = batch.ArenaZSetBatch(schema_u128)
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

    tmp1 = batch.ArenaZSetBatch(schema)
    rb1 = RowBuilder(schema, tmp1)
    rb1.begin(r_uint128(10), r_int64(1))
    rb1.put_string("test")
    rb1.commit()
    writer.add_row_from_accessor(r_uint128(10), r_int64(1), tmp1.get_accessor(0))
    tmp1.free()

    tmp2 = batch.ArenaZSetBatch(schema)
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

    tmp = batch.ArenaZSetBatch(schema)
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

    idx = flsm.index_from_manifest(manifest_file, 1, schema, rc)
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
    b = batch.ArenaZSetBatch(schema)
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

    if tbl.has_pk(r_uint128(100)):
        raise Exception("Ghost Property Violation: has_pk(100) should be False")
    if not tbl.has_pk(r_uint128(200)):
        raise Exception("PK 200 should survive compaction")
    if not tbl.has_pk(r_uint128(300)):
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
    rb_add.begin(pk1, r_int64(5))
    rb_add.put_string("sum_test")
    rb_add.commit()
    t_eph.ingest_batch(b_add)

    b_sub = batch.ArenaZSetBatch(schema)
    rb_sub = RowBuilder(schema, b_sub)
    rb_sub.begin(pk1, r_int64(-3))
    rb_sub.put_string("sum_test")
    rb_sub.commit()
    t_eph.ingest_batch(b_sub)

    tmp_acc = batch.ArenaZSetBatch(schema)
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
    b2 = batch.ArenaZSetBatch(schema)
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

    b = batch.ArenaZSetBatch(schema)
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
            b = batch.ArenaZSetBatch(schema)
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
        tmp = batch.ArenaZSetBatch(schema)
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
    b = batch.ArenaZSetBatch(schema)
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
    b = batch.ArenaZSetBatch(schema)
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
    b2 = batch.ArenaZSetBatch(schema)
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
    b4a = batch.ArenaZSetBatch(schema)
    rb4a = RowBuilder(schema, b4a)
    rb4a.begin(r_uint128(30), r_int64(1))
    rb4a.put_string("ins")
    rb4a.commit()
    t4.ingest_batch(b4a)
    b4a.free()
    b4b = batch.ArenaZSetBatch(schema)
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
    b5a = batch.ArenaZSetBatch(schema)
    rb5a = RowBuilder(schema, b5a)
    rb5a.begin(r_uint128(40), r_int64(1))
    rb5a.put_string("split")
    rb5a.commit()
    t5.ingest_batch(b5a)
    b5a.free()
    t5.flush()  # +1 in shard
    b5b = batch.ArenaZSetBatch(schema)
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
    b6a = batch.ArenaZSetBatch(schema)
    rb6a = RowBuilder(schema, b6a)
    rb6a.begin(r_uint128(50), r_int64(1))
    rb6a.put_string("cancel")
    rb6a.commit()
    t6.ingest_batch(b6a)
    b6a.free()
    t6.flush()  # +1 in shard
    b6b = batch.ArenaZSetBatch(schema)
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
    b7 = batch.ArenaZSetBatch(schema)
    rb7 = RowBuilder(schema, b7)
    rb7.begin(r_uint128(60), r_int64(1))
    rb7.put_string(test_str)
    rb7.commit()
    t7.ingest_batch(b7)

    # Build a reference row for comparison
    ref = batch.ArenaZSetBatch(schema)
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
    b8 = batch.ArenaZSetBatch(schema)
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
    b1 = batch.ArenaZSetBatch(schema)
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
    b2pre = batch.ArenaZSetBatch(schema)
    rb2pre = RowBuilder(schema, b2pre)
    rb2pre.begin(r_uint128(1), r_int64(1))
    rb2pre.put_int(r_int64(10))
    rb2pre.commit()
    t2.ingest_batch(b2pre)
    b2pre.free()
    b2 = batch.ArenaZSetBatch(schema)
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
    b3pre = batch.ArenaZSetBatch(schema)
    rb3pre = RowBuilder(schema, b3pre)
    rb3pre.begin(r_uint128(1), r_int64(1))
    rb3pre.put_int(r_int64(10))
    rb3pre.commit()
    t3.ingest_batch(b3pre)
    b3pre.free()
    b3 = batch.ArenaZSetBatch(schema)
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
    b4 = batch.ArenaZSetBatch(schema)
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
    b5 = batch.ArenaZSetBatch(schema)
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
    b6 = batch.ArenaZSetBatch(schema)
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
            rb.begin(r_uint128(i), r_int64(1))
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
        rb.begin(r_uint128(3), r_int64(-1))
        rb.put_int(r_int64(30))
        rb.commit()
        tbl.ingest_batch(b)
        tbl.flush()
        b.free()

        i = 7
        while i <= 10:
            b = batch.ArenaZSetBatch(schema)
            rb = RowBuilder(schema, b)
            rb.begin(r_uint128(i), r_int64(1))
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
        if tbl.has_pk(r_uint128(3)):
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
            rb.begin(r_uint128(i), r_int64(1))
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
        rb.begin(r_uint128(5), r_int64(1))
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
            rb.begin(r_uint128(i), r_int64(1))
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
        rb.begin(r_uint128(99), r_int64(1))
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
        if not tbl.has_pk(r_uint128(99)):
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
            rb.begin(r_uint128(i), r_int64(1))
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
            rb.begin(r_uint128(i + 1), r_int64(1))
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
            if not tbl.has_pk(r_uint128(i)):
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
                rb.begin(r_uint128(p * 100 + i), r_int64(1))
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


def test_flsm_data_structures(base_dir):
    os.write(1, "[Storage] Testing FLSM data structures...\n")
    schema = make_u64_i64_schema()

    # 1. LevelGuard.guard_key()
    g = flsm.LevelGuard(r_uint64(100), r_uint64(0))
    assert_equal_u128(r_uint128(100), g.guard_key(), "LevelGuard.guard_key failed")

    # 2. FLSMLevel.find_guard_idx: empty level returns -1
    level = flsm.FLSMLevel(1)
    assert_equal_i(-1, level.find_guard_idx(r_uint128(50)), "empty level should return -1")

    # Insert guards at keys 0, 100, 200, 300
    g0 = flsm.LevelGuard(r_uint64(0), r_uint64(0))
    g1 = flsm.LevelGuard(r_uint64(100), r_uint64(0))
    g2 = flsm.LevelGuard(r_uint64(200), r_uint64(0))
    g3 = flsm.LevelGuard(r_uint64(300), r_uint64(0))
    level.insert_guard_sorted(g0)
    level.insert_guard_sorted(g1)
    level.insert_guard_sorted(g2)
    level.insert_guard_sorted(g3)

    assert_equal_i(0, level.find_guard_idx(r_uint128(50)),  "key=50 should be guard 0")
    assert_equal_i(1, level.find_guard_idx(r_uint128(100)), "key=100 should be guard 1")
    assert_equal_i(2, level.find_guard_idx(r_uint128(250)), "key=250 should be guard 2")
    assert_equal_i(3, level.find_guard_idx(r_uint128(350)), "key=350 should be guard 3")
    assert_equal_i(0, level.find_guard_idx(r_uint128(0)),   "key=0 should be guard 0")

    # 3. find_guards_for_range(80, 210) -> indices [0, 1, 2]
    indices = level.find_guards_for_range(r_uint128(80), r_uint128(210))
    assert_equal_i(3, len(indices), "find_guards_for_range(80,210) should return 3 indices")
    assert_equal_i(0, indices[0], "range[0] should be 0")
    assert_equal_i(1, indices[1], "range[1] should be 1")
    assert_equal_i(2, indices[2], "range[2] should be 2")

    # 4. FLSMIndex handle management: create 5 real shard files
    shard_dir = os.path.join(base_dir, "flsm_ds")
    rposix.mkdir(shard_dir, 0o755)
    rc = refcount.RefCounter()
    idx = flsm.FLSMIndex(99, schema, rc, shard_dir)
    assert_equal_i(0, len(idx.handles), "FLSMIndex initially empty")

    shard_paths = []
    i = 0
    while i < 5:
        shard_path = shard_dir + "/ds_shard_%d.db" % i
        sw = writer_table.TableShardWriter(schema, table_id=99)
        tmp = batch.ArenaZSetBatch(schema)
        rb = RowBuilder(schema, tmp)
        rb.begin(r_uint128(i * 10), r_int64(1))
        rb.put_int(r_int64(i))
        rb.commit()
        sw.add_row_from_accessor(r_uint128(i * 10), r_int64(1), tmp.get_accessor(0))
        tmp.free()
        sw.finalize(shard_path)
        shard_paths.append(shard_path)
        i += 1

    h0 = index.ShardHandle(shard_paths[0], schema, r_uint64(0), r_uint64(0))
    idx.add_handle(h0)
    if idx.needs_compaction:
        raise Exception("needs_compaction should be False with 1 handle")

    i = 1
    while i < 5:
        h = index.ShardHandle(shard_paths[i], schema, r_uint64(0), r_uint64(0))
        idx.add_handle(h)
        i += 1
    if not idx.needs_compaction:
        raise Exception("needs_compaction should be True with 5 handles (> threshold 4)")

    idx.close_all()

    os.write(1, "    [OK] FLSM data structures passed.\n")


def test_flsm_guard_read_path(base_dir):
    os.write(1, "[Storage] Testing FLSM guard-aware read path...\n")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="val"))
    schema = types.TableSchema(cols, 0)

    tbl_dir = os.path.join(base_dir, "flsm_grp")
    tbl = EphemeralTable(tbl_dir, "grp", schema)

    # 1. Insert 20 rows PKs 0-19, 4 rows per batch, flush after each batch
    batch_num = 0
    while batch_num < 5:
        b = batch.ArenaZSetBatch(schema)
        row_in_batch = 0
        while row_in_batch < 4:
            pk = r_uint128(batch_num * 4 + row_in_batch)
            rb = RowBuilder(schema, b)
            rb.begin(pk, r_int64(1))
            rb.put_int(r_int64(batch_num * 4 + row_in_batch))
            rb.commit()
            row_in_batch += 1
        tbl.ingest_batch(b)
        b.free()
        tbl.flush()
        batch_num += 1

    if len(tbl.index.handles) != 5:
        raise Exception("Expected 5 L0 handles, got " + str(len(tbl.index.handles)))

    # 2. Build L1 guards using actual pk_min of first handle placed in each
    handles = tbl.index.handles  # sorted: [0-3], [4-7], [8-11], [12-15], [16-19]
    level1 = tbl.index.get_or_create_level(1)

    guard0 = flsm.LevelGuard(handles[0].pk_min_lo, handles[0].pk_min_hi)
    guard1 = flsm.LevelGuard(handles[3].pk_min_lo, handles[3].pk_min_hi)

    # Directly append without add_handle: already refcounted by original add_handle
    guard0.handles.append(handles[0])
    guard0.handles.append(handles[1])
    guard1.handles.append(handles[3])
    guard1.handles.append(handles[4])

    level1.insert_guard_sorted(guard0)
    level1.insert_guard_sorted(guard1)

    # 3. L0 now holds only handles[2] (covers [8-11])
    new_l0 = newlist_hint(1)
    new_l0.append(handles[2])
    tbl.index.handles = new_l0

    # 4. create_cursor() — assert yields all 20 rows
    c = tbl.create_cursor()
    row_count = 0
    while c.is_valid():
        if c.weight() > r_int64(0):
            row_count += 1
        c.advance()
    c.close()
    if row_count != 20:
        raise Exception("Expected 20 rows from cursor, got " + str(row_count))

    # 5. has_pk for all k in 0..19 — assert True
    k = 0
    while k < 20:
        if not tbl.has_pk(r_uint128(k)):
            raise Exception("has_pk(%d) should be True" % k)
        k += 1

    # 6. has_pk(50) -> False (50 routes to guard1 but not in [12-15] or [16-19])
    if tbl.has_pk(r_uint128(50)):
        raise Exception("has_pk(50) should be False")

    # 7. all_handles_for_cursor() -> exactly 5 handles
    all_h = tbl.index.all_handles_for_cursor()
    if len(all_h) != 5:
        raise Exception("all_handles_for_cursor should return 5, got " + str(len(all_h)))

    tbl.close()
    os.write(1, "    [OK] FLSM guard-aware read path passed.\n")


def test_zero_copy_wal_recovery(base_dir):
    os.write(1, "[Storage] Testing zero-copy WAL recovery (mmap reader)...\n")
    schema = make_u64_i64_schema()
    wal_path = os.path.join(base_dir, "zc_recovery.wal")

    # 1. Write 3 blocks with distinct LSNs and table IDs
    writer = wal.WALWriter(wal_path, schema)
    written = []   # list of (lsn, tid, [(pk, val)])
    for i in range(3):
        b = batch.ArenaZSetBatch(schema)
        rb = RowBuilder(schema, b)
        for j in range(i + 1):
            rb.begin(r_uint128(i * 10 + j), r_int64(1))
            rb.put_int(r_int64(i * 100 + j))
            rb.commit()
        lsn = r_uint64(i + 1)
        tid = i + 1
        writer.append_batch(lsn, tid, b)
        rows = newlist_hint(i + 1)
        for j in range(i + 1):
            rows.append((i * 10 + j, i * 100 + j))
        written.append((i + 1, tid, rows))
        b.free()
    writer.close()

    # 2. Read back with mmap reader while reader is still open; verify all blocks
    reader = wal.WALReader(wal_path, schema)
    blocks = newlist_hint(3)
    for block in reader.iterate_blocks():
        blocks.append(block)

    assert_equal_i(3, len(blocks), "mmap WALReader: expected 3 blocks")
    for i in range(3):
        exp_lsn, exp_tid, exp_rows = written[i]
        assert_equal_u64(r_uint64(exp_lsn), blocks[i].lsn,
                         "block " + str(i) + " lsn mismatch")
        assert_equal_i(exp_tid, intmask(r_uint64(blocks[i].tid)),
                       "block " + str(i) + " tid mismatch")
        assert_equal_i(len(exp_rows), blocks[i].batch.length(),
                       "block " + str(i) + " row count mismatch")
        acc = ColumnarBatchAccessor(schema)
        for j in range(len(exp_rows)):
            exp_pk, exp_val = exp_rows[j]
            assert_equal_u128(r_uint128(exp_pk), blocks[i].batch.get_pk(j),
                              "block " + str(i) + " row " + str(j) + " pk mismatch")
            acc.bind(blocks[i].batch, j)
            assert_equal_i(exp_val, intmask(r_uint64(acc.get_int(1))),
                           "block " + str(i) + " row " + str(j) + " val mismatch")
        blocks[i].free()

    # Close reader last (mmap unmapped here; blocks already freed above)
    reader.close()

    # 3. Empty WAL: zero-size file -> read_next_block returns None immediately
    empty_path = os.path.join(base_dir, "empty.wal")
    fd = rposix.open(empty_path, os.O_WRONLY | os.O_CREAT, 0o644)
    rposix.close(fd)
    empty_reader = wal.WALReader(empty_path, schema)
    assert_true(empty_reader.read_next_block() is None,
                "empty WAL should return None")
    empty_reader.close()

    # 4. Corrupt block: truncate mid-block -> CorruptShardError
    trunc_path = os.path.join(base_dir, "trunc.wal")
    writer2 = wal.WALWriter(trunc_path, schema)
    b2 = batch.ArenaZSetBatch(schema)
    rb2 = RowBuilder(schema, b2)
    rb2.begin(r_uint128(999), r_int64(1))
    rb2.put_int(r_int64(42))
    rb2.commit()
    writer2.append_batch(r_uint64(10), 5, b2)
    b2.free()
    writer2.close()

    # Truncate the WAL to header_size + 4 (body present but incomplete)
    full_size = os.path.getsize(trunc_path)
    trunc_fd = rposix.open(trunc_path, os.O_WRONLY, 0o644)
    rposix.ftruncate(trunc_fd, wal_layout.WAL_BLOCK_HEADER_SIZE + 4)
    rposix.close(trunc_fd)

    corrupt_reader = wal.WALReader(trunc_path, schema)
    raised = False
    try:
        corrupt_reader.read_next_block()
    except errors.CorruptShardError:
        raised = True
    corrupt_reader.close()
    assert_true(raised, "truncated WAL block should raise CorruptShardError")

    os.write(1, "    [OK] Zero-copy WAL recovery passed.\n")


def test_flsm_l0_to_l1_compaction(base_dir):
    os.write(1, "[Storage] Testing FLSM L0->L1 compaction...\n")
    schema = make_u64_i64_schema()

    # --- Sub-test 1: basic L0->L1 compaction ---
    tbl_dir = os.path.join(base_dir, "flsm_l0l1")
    tbl = EphemeralTable(tbl_dir, "l0l1test", schema)

    pk = 1
    while pk <= 5:
        b = batch.ArenaZSetBatch(schema)
        rb = RowBuilder(schema, b)
        rb.begin(r_uint128(pk), r_int64(1))
        rb.put_int(r_int64(pk * 10))
        rb.commit()
        tbl.ingest_batch(b)
        b.free()
        tbl.flush()
        pk += 1

    assert_equal_i(5, len(tbl.index.handles), "Expected 5 L0 handles before compaction")

    # create_cursor() triggers compaction
    c = tbl.create_cursor()

    assert_equal_i(0, len(tbl.index.handles), "L0 handles should be 0 after compaction")
    assert_equal_i(1, len(tbl.index.levels), "Should have exactly 1 level after compaction")

    l1 = tbl.index.levels[0]
    guard_count = len(l1.guards)
    if guard_count < 1:
        raise Exception("Guard count should be >= 1, got 0")
    if guard_count > 5:
        raise Exception("Guard count %d should be <= 5" % guard_count)

    # Verify guards are sorted by guard_key
    gi = 1
    while gi < guard_count:
        if l1.guards[gi].guard_key() <= l1.guards[gi - 1].guard_key():
            raise Exception("Guards not sorted at index %d" % gi)
        gi += 1

    # Scan cursor: assert 5 positive-weight rows
    row_count = 0
    while c.is_valid():
        if c.weight() > r_int64(0):
            row_count += 1
        c.advance()
    c.close()
    assert_equal_i(5, row_count, "Expected 5 rows after L0->L1 compaction")

    tbl.close()

    # --- Sub-test 2: ghost elimination ---
    tbl2_dir = os.path.join(base_dir, "flsm_ghost")
    tbl2 = EphemeralTable(tbl2_dir, "ghosttest", schema)

    # flush A(+1), B(+1), A(-1), C(+1), D(+1) -> 5 L0 shards
    ghost_pks = [r_uint128(1), r_uint128(2), r_uint128(1), r_uint128(3), r_uint128(4)]
    ghost_weights = [r_int64(1), r_int64(1), r_int64(-1), r_int64(1), r_int64(1)]
    ghost_vals = [r_int64(100), r_int64(200), r_int64(100), r_int64(300), r_int64(400)]
    gi2 = 0
    while gi2 < 5:
        b = batch.ArenaZSetBatch(schema)
        rb = RowBuilder(schema, b)
        rb.begin(ghost_pks[gi2], ghost_weights[gi2])
        rb.put_int(ghost_vals[gi2])
        rb.commit()
        tbl2.ingest_batch(b)
        b.free()
        tbl2.flush()
        gi2 += 1

    assert_equal_i(5, len(tbl2.index.handles), "Expected 5 L0 handles before ghost compaction")

    c2 = tbl2.create_cursor()

    # pk=1 (A) should be absent (net weight 0)
    found_pk1 = False
    while c2.is_valid():
        if c2.key() == r_uint128(1) and c2.weight() > r_int64(0):
            found_pk1 = True
        c2.advance()
    c2.close()

    assert_false(found_pk1, "Ghost key A (pk=1) should be absent after compaction")
    assert_true(tbl2.has_pk(r_uint128(2)), "key B (pk=2) should be present")
    assert_true(tbl2.has_pk(r_uint128(3)), "key C (pk=3) should be present")

    tbl2.close()
    os.write(1, "    [OK] FLSM L0->L1 compaction passed.\n")


def test_flsm_manifest_persistence(base_dir):
    os.write(1, "[Storage] Testing FLSM manifest V3 persistence...\n")
    schema = make_u64_i64_schema()

    tbl_dir = os.path.join(base_dir, "flsm_persist")
    manifest_path = tbl_dir + "/MANIFEST"

    # 1. Create PersistentTable, flush 5 shards, compact into L1
    tbl = PersistentTable(tbl_dir, "fp", schema, table_id=42)
    i = 1
    while i <= 5:
        b = batch.ArenaZSetBatch(schema)
        rb = RowBuilder(schema, b)
        rb.begin(r_uint128(i), r_int64(1))
        rb.put_int(r_int64(i * 10))
        rb.commit()
        tbl.ingest_batch(b)
        tbl.flush()
        b.free()
        i += 1

    tbl.compact_if_needed()

    assert_equal_i(0, len(tbl.index.handles), "Expected 0 L0 after compact")
    assert_equal_i(1, len(tbl.index.levels), "Expected 1 L1 after compact")

    pre_guard_count = len(tbl.index.levels[0].guards)
    if pre_guard_count < 1:
        raise Exception("Expected at least 1 L1 guard after compaction")

    # Record guard keys for comparison
    pre_guard_keys_lo = newlist_hint(pre_guard_count)
    pre_guard_keys_hi = newlist_hint(pre_guard_count)
    gi = 0
    while gi < pre_guard_count:
        pre_guard_keys_lo.append(tbl.index.levels[0].guards[gi].guard_key_lo)
        pre_guard_keys_hi.append(tbl.index.levels[0].guards[gi].guard_key_hi)
        gi += 1

    tbl.close()

    # 2. Verify V3 manifest: at least one entry with non-zero level
    reader = manifest.ManifestReader(manifest_path)
    found_l1_entry = False
    for entry in reader.iterate_entries():
        if entry.level != 0:
            found_l1_entry = True
            break
    reader.close()

    assert_true(found_l1_entry, "V3 manifest should have at least one L1 entry")

    # 3. Reopen PersistentTable and verify L1 structure is restored
    tbl2 = PersistentTable(tbl_dir, "fp", schema, table_id=42)

    assert_equal_i(0, len(tbl2.index.handles), "Reopened table: expected 0 L0 handles")
    assert_equal_i(1, len(tbl2.index.levels), "Reopened table: expected 1 L1 level")
    assert_equal_i(pre_guard_count, len(tbl2.index.levels[0].guards),
                   "Reopened table: guard count mismatch")

    # Guard keys must match pre-close values
    gi = 0
    while gi < pre_guard_count:
        if tbl2.index.levels[0].guards[gi].guard_key_lo != pre_guard_keys_lo[gi]:
            raise Exception("Guard key_lo mismatch at index %d" % gi)
        if tbl2.index.levels[0].guards[gi].guard_key_hi != pre_guard_keys_hi[gi]:
            raise Exception("Guard key_hi mismatch at index %d" % gi)
        gi += 1

    # 4. Verify index_from_manifest produces same structure
    rc2 = refcount.RefCounter()
    idx2 = flsm.index_from_manifest(manifest_path, 42, schema, rc2,
                                     validate_checksums=False, output_dir=tbl_dir)
    assert_equal_i(0, len(idx2.handles), "index_from_manifest: expected 0 L0 handles")
    assert_equal_i(1, len(idx2.levels), "index_from_manifest: expected 1 L1 level")
    assert_equal_i(pre_guard_count, len(idx2.levels[0].guards),
                   "index_from_manifest: guard count mismatch")
    idx2.close_all()

    # 5. Scan all rows via cursor
    c = tbl2.create_cursor()
    row_count = 0
    while c.is_valid():
        if c.weight() > r_int64(0):
            row_count += 1
        c.advance()
    c.close()
    assert_equal_i(5, row_count, "Reopened table: expected 5 rows from cursor")

    tbl2.close()
    os.write(1, "    [OK] FLSM manifest V3 persistence passed.\n")


def test_flsm_horizontal_compaction(base_dir):
    os.write(1, "[Storage] Testing FLSM horizontal compaction...\n")
    schema = make_u64_i64_schema()

    shard_dir = os.path.join(base_dir, "flsm_hcomp")
    rposix.mkdir(shard_dir, 0o755)
    rc = refcount.RefCounter()
    idx = flsm.FLSMIndex(77, schema, rc, shard_dir)

    l1 = idx.get_or_create_level(1)
    guard = flsm.LevelGuard(r_uint64(0), r_uint64(0))
    l1.insert_guard_sorted(guard)

    # Create 6 shard files (10 distinct rows each, PKs 0-9, 10-19, ..., 50-59)
    i = 0
    while i < 6:
        shard_path = shard_dir + "/hcomp_src_%d.db" % i
        sw = writer_table.TableShardWriter(schema, table_id=77)
        row = 0
        while row < 10:
            pk = r_uint128(i * 10 + row)
            tmp = batch.ArenaZSetBatch(schema)
            rb = RowBuilder(schema, tmp)
            rb.begin(pk, r_int64(1))
            rb.put_int(r_int64(i * 10 + row))
            rb.commit()
            sw.add_row_from_accessor(pk, r_int64(1), tmp.get_accessor(0))
            tmp.free()
            row += 1
        sw.finalize(shard_path)
        h = index.ShardHandle(shard_path, schema, r_uint64(0), r_uint64(0))
        guard.add_handle(rc, h)
        i += 1

    assert_equal_i(6, len(guard.handles), "Expected 6 handles before compaction")
    assert_true(guard.needs_horizontal_compact(flsm.GUARD_FILE_THRESHOLD),
                "Guard should need horizontal compaction")

    idx.compact_guards_if_needed()

    assert_equal_i(1, len(guard.handles), "Expected 1 handle after horizontal compaction")
    assert_equal_i(1, len(l1.guards), "Guard count should be unchanged")
    assert_equal_i(1, len(idx.levels), "No new level should be created")

    # Verify data integrity
    out_view = shard_table.TableShardView(guard.handles[0].filename, schema)
    assert_equal_i(60, out_view.count, "Expected 60 rows in merged shard")
    out_view.close()

    idx.close_all()
    os.write(1, "    [OK] FLSM horizontal compaction passed.\n")


def test_flsm_horizontal_ghost_elimination(base_dir):
    os.write(1, "[Storage] Testing FLSM horizontal ghost elimination...\n")
    schema = make_u64_i64_schema()

    shard_dir = os.path.join(base_dir, "flsm_hghost")
    rposix.mkdir(shard_dir, 0o755)
    rc = refcount.RefCounter()
    idx = flsm.FLSMIndex(78, schema, rc, shard_dir)

    l1 = idx.get_or_create_level(1)
    guard = flsm.LevelGuard(r_uint64(0), r_uint64(0))
    l1.insert_guard_sorted(guard)

    # Shard 0: pk=10 (+1), pk=20 (+1), pk=30 (+1)
    # Shard 1: pk=10 (-1) — cancels shard 0's pk=10
    # Shard 2: pk=40 (+1), pk=50 (+1)
    # Shard 3: pk=20 (-1) — cancels shard 0's pk=20
    # Shard 4: pk=60 (+1)
    shard_specs = [
        [(r_uint128(10), r_int64(1)), (r_uint128(20), r_int64(1)), (r_uint128(30), r_int64(1))],
        [(r_uint128(10), r_int64(-1))],
        [(r_uint128(40), r_int64(1)), (r_uint128(50), r_int64(1))],
        [(r_uint128(20), r_int64(-1))],
        [(r_uint128(60), r_int64(1))],
    ]
    original_paths = newlist_hint(5)
    s = 0
    while s < 5:
        shard_path = shard_dir + "/ghost_src_%d.db" % s
        original_paths.append(shard_path)
        sw = writer_table.TableShardWriter(schema, table_id=78)
        for pk, w in shard_specs[s]:
            tmp = batch.ArenaZSetBatch(schema)
            rb = RowBuilder(schema, tmp)
            rb.begin(pk, w)
            rb.put_int(r_int64(intmask(pk)))
            rb.commit()
            sw.add_row_from_accessor(pk, w, tmp.get_accessor(0))
            tmp.free()
        sw.finalize(shard_path)
        h = index.ShardHandle(shard_path, schema, r_uint64(0), r_uint64(0))
        guard.add_handle(rc, h)
        s += 1

    idx.compact_guards_if_needed()

    assert_equal_i(1, len(guard.handles), "Expected 1 handle after ghost elimination")

    out_view = shard_table.TableShardView(guard.handles[0].filename, schema)
    assert_equal_i(4, out_view.count, "Expected 4 surviving rows (pk=30,40,50,60)")
    out_view.close()

    # Original shard files must be deleted
    p = 0
    while p < 5:
        if os.path.exists(original_paths[p]):
            raise Exception("Original shard %d should have been deleted" % p)
        p += 1

    idx.close_all()
    os.write(1, "    [OK] FLSM horizontal ghost elimination passed.\n")


def _make_single_row_shard(path, schema, table_id, pk_val):
    sw = writer_table.TableShardWriter(schema, table_id=table_id)
    tmp = batch.ArenaZSetBatch(schema)
    rb = RowBuilder(schema, tmp)
    rb.begin(r_uint128(pk_val), r_int64(1))
    rb.put_int(r_int64(pk_val))
    rb.commit()
    sw.add_row_from_accessor(r_uint128(pk_val), r_int64(1), tmp.get_accessor(0))
    tmp.free()
    sw.finalize(path)


def test_flsm_lmax_horizontal_threshold(base_dir):
    os.write(1, "[Storage] Testing FLSM Lmax horizontal threshold...\n")
    schema = make_u64_i64_schema()

    shard_dir = os.path.join(base_dir, "flsm_lmax")
    rposix.mkdir(shard_dir, 0o755)
    rc = refcount.RefCounter()
    idx = flsm.FLSMIndex(79, schema, rc, shard_dir)

    # Sub-test A: L2 fires at 2 handles (LMAX_FILE_THRESHOLD=1, trigger when > 1)
    l2 = idx.get_or_create_level(2)
    guard_a = flsm.LevelGuard(r_uint64(0), r_uint64(0))
    l2.insert_guard_sorted(guard_a)

    p0 = shard_dir + "/lmax_a0.db"
    p1 = shard_dir + "/lmax_a1.db"
    _make_single_row_shard(p0, schema, 79, 1)
    _make_single_row_shard(p1, schema, 79, 2)
    guard_a.add_handle(rc, index.ShardHandle(p0, schema, r_uint64(0), r_uint64(0)))
    guard_a.add_handle(rc, index.ShardHandle(p1, schema, r_uint64(0), r_uint64(0)))

    assert_true(guard_a.needs_horizontal_compact(flsm.LMAX_FILE_THRESHOLD),
                "L2 guard with 2 handles should need compaction (threshold=1)")
    idx.compact_guards_if_needed()
    assert_equal_i(1, len(guard_a.handles), "L2 guard should have 1 handle after compaction")

    # Sub-test B: L1 does NOT fire at exactly 4 handles (threshold=4, trigger when > 4)
    l1 = idx.get_or_create_level(1)
    guard_b = flsm.LevelGuard(r_uint64(1000), r_uint64(0))
    l1.insert_guard_sorted(guard_b)

    j = 0
    while j < 4:
        pb = shard_dir + "/lmax_b%d.db" % j
        _make_single_row_shard(pb, schema, 79, 1000 + j)
        guard_b.add_handle(rc, index.ShardHandle(pb, schema, r_uint64(0), r_uint64(0)))
        j += 1

    assert_true(not guard_b.needs_horizontal_compact(flsm.GUARD_FILE_THRESHOLD),
                "L1 guard with exactly 4 handles should NOT need compaction")
    idx.compact_guards_if_needed()
    assert_equal_i(4, len(guard_b.handles), "L1 guard with 4 handles should be unchanged")

    # Sub-test C: L1 fires at 5 handles
    pc = shard_dir + "/lmax_b4.db"
    _make_single_row_shard(pc, schema, 79, 1004)
    guard_b.add_handle(rc, index.ShardHandle(pc, schema, r_uint64(0), r_uint64(0)))

    assert_true(guard_b.needs_horizontal_compact(flsm.GUARD_FILE_THRESHOLD),
                "L1 guard with 5 handles should need compaction")
    idx.compact_guards_if_needed()
    assert_equal_i(1, len(guard_b.handles), "L1 guard should have 1 handle after compaction")

    idx.close_all()
    os.write(1, "    [OK] FLSM Lmax horizontal threshold passed.\n")


def test_flsm_multilevel_compaction(base_dir):
    os.write(1, "[Storage] Testing FLSM multilevel compaction...\n")
    schema = make_u64_i64_schema()

    shard_dir = os.path.join(base_dir, "flsm_ml")
    rposix.mkdir(shard_dir, 0o755)
    rc = refcount.RefCounter()
    idx = flsm.FLSMIndex(80, schema, rc, shard_dir)

    l1 = idx.get_or_create_level(1)
    guard_keys_lo = [100, 200, 300, 400, 500]
    g = 0
    while g < 5:
        gk_lo = guard_keys_lo[g]
        guard = flsm.LevelGuard(r_uint64(gk_lo), r_uint64(0))
        l1.insert_guard_sorted(guard)
        j = 0
        while j < 4:
            pk_val = gk_lo + 1 + j
            path = shard_dir + "/ml_g%d_s%d.db" % (g, j)
            _make_single_row_shard(path, schema, 80, pk_val)
            guard.add_handle(rc, index.ShardHandle(path, schema, r_uint64(0), r_uint64(0)))
            j += 1
        g += 1

    assert_equal_i(5, len(l1.guards), "Expected 5 L1 guards before vertical compaction")
    assert_equal_i(20, l1.total_file_count(), "Expected 20 L1 files before vertical compaction")

    idx.compact_guard_vertical_if_needed(1)

    assert_true(len(idx.levels) >= 2, "L2 should be created after vertical compaction")
    assert_true(len(idx.levels[1].guards) >= 1, "L2 should have at least one guard")
    assert_equal_i(4, len(l1.guards), "L1 should have 4 guards after promoting worst")
    assert_equal_i(16, l1.total_file_count(), "L1 should have 16 files after promoting one guard")

    # Scan all rows: no data loss
    all_handles = idx.all_handles_for_cursor()
    total_rows = 0
    hi = 0
    while hi < len(all_handles):
        v = shard_table.TableShardView(all_handles[hi].filename, schema)
        total_rows += v.count
        v.close()
        hi += 1
    assert_equal_i(20, total_rows, "Expected 20 rows total across all levels")

    # Point lookup for each PK
    gi = 0
    while gi < 5:
        gk_lo = guard_keys_lo[gi]
        j = 0
        while j < 4:
            pk_val = gk_lo + 1 + j
            results = idx.find_all_shards_and_indices(r_uint128(pk_val))
            assert_true(len(results) > 0, "PK %d should be present" % pk_val)
            j += 1
        gi += 1

    # Every L2 guard has exactly 1 handle (Z=1)
    l2 = idx.levels[1]
    gi2 = 0
    while gi2 < len(l2.guards):
        assert_equal_i(1, len(l2.guards[gi2].handles),
                       "L2 guard %d should have exactly 1 handle" % gi2)
        gi2 += 1

    # L2 guard keys are sorted ascending
    gi3 = 1
    while gi3 < len(l2.guards):
        if l2.guards[gi3].guard_key() <= l2.guards[gi3 - 1].guard_key():
            raise Exception("L2 guards not sorted at index %d" % gi3)
        gi3 += 1

    idx.close_all()
    os.write(1, "    [OK] FLSM multilevel compaction passed.\n")


def test_flsm_lazy_leveling_lmax(base_dir):
    os.write(1, "[Storage] Testing FLSM Lazy Leveling Lmax...\n")
    schema = make_u64_i64_schema()

    shard_dir = os.path.join(base_dir, "flsm_lazy")
    rposix.mkdir(shard_dir, 0o755)
    rc = refcount.RefCounter()
    idx = flsm.FLSMIndex(81, schema, rc, shard_dir)

    # Round 1: one L1 guard with 2 files → L2 created with 1 guard × 1 file
    l1 = idx.get_or_create_level(1)
    guard1 = flsm.LevelGuard(r_uint64(0), r_uint64(0))
    l1.insert_guard_sorted(guard1)

    p0 = shard_dir + "/lazy_r1_0.db"
    p1 = shard_dir + "/lazy_r1_1.db"
    _make_single_row_shard(p0, schema, 81, 1)
    _make_single_row_shard(p1, schema, 81, 2)
    guard1.add_handle(rc, index.ShardHandle(p0, schema, r_uint64(0), r_uint64(0)))
    guard1.add_handle(rc, index.ShardHandle(p1, schema, r_uint64(0), r_uint64(0)))

    idx.compact_guard_vertical_if_needed(1)

    assert_true(len(idx.levels) >= 2, "L2 should be created after round 1")
    assert_equal_i(0, l1.total_file_count(), "L1 should be empty after promoting only guard")

    l2 = idx.levels[1]
    gi = 0
    while gi < len(l2.guards):
        assert_equal_i(1, len(l2.guards[gi].handles),
                       "L2 guard should have exactly 1 handle after round 1")
        gi += 1

    # Round 2: new L1 guard same range with 3 files → merged with existing L2 guard
    guard2 = flsm.LevelGuard(r_uint64(0), r_uint64(0))
    l1.insert_guard_sorted(guard2)

    p2 = shard_dir + "/lazy_r2_0.db"
    p3 = shard_dir + "/lazy_r2_1.db"
    p4 = shard_dir + "/lazy_r2_2.db"
    _make_single_row_shard(p2, schema, 81, 10)
    _make_single_row_shard(p3, schema, 81, 20)
    _make_single_row_shard(p4, schema, 81, 30)
    guard2.add_handle(rc, index.ShardHandle(p2, schema, r_uint64(0), r_uint64(0)))
    guard2.add_handle(rc, index.ShardHandle(p3, schema, r_uint64(0), r_uint64(0)))
    guard2.add_handle(rc, index.ShardHandle(p4, schema, r_uint64(0), r_uint64(0)))

    idx.compact_guard_vertical_if_needed(1)

    # Z=1 still enforced after re-compaction
    gi2 = 0
    while gi2 < len(l2.guards):
        assert_equal_i(1, len(l2.guards[gi2].handles),
                       "L2 guard should have exactly 1 handle after round 2")
        gi2 += 1

    # All 5 rows present (pks 1, 2, 10, 20, 30)
    all_handles = idx.all_handles_for_cursor()
    total_rows = 0
    hi = 0
    while hi < len(all_handles):
        v = shard_table.TableShardView(all_handles[hi].filename, schema)
        total_rows += v.count
        v.close()
        hi += 1
    assert_equal_i(5, total_rows, "Expected 5 rows total after two compaction rounds")

    idx.close_all()
    os.write(1, "    [OK] FLSM Lazy Leveling Lmax passed.\n")


def test_flsm_guard_isolation(base_dir):
    os.write(1, "[Storage] Testing FLSM guard isolation...\n")
    schema = make_u64_i64_schema()

    shard_dir = os.path.join(base_dir, "flsm_iso")
    rposix.mkdir(shard_dir, 0o755)
    rc = refcount.RefCounter()
    idx = flsm.FLSMIndex(82, schema, rc, shard_dir)

    l1 = idx.get_or_create_level(1)

    # Guard A: 1 file at key 0
    guard_a = flsm.LevelGuard(r_uint64(0), r_uint64(0))
    l1.insert_guard_sorted(guard_a)
    fn_a_path = shard_dir + "/iso_a.db"
    _make_single_row_shard(fn_a_path, schema, 82, 50)
    guard_a.add_handle(rc, index.ShardHandle(fn_a_path, schema, r_uint64(0), r_uint64(0)))
    fn_a = guard_a.handles[0].filename

    # Guard B: 4 files at key 1000 (worst)
    guard_b = flsm.LevelGuard(r_uint64(1000), r_uint64(0))
    l1.insert_guard_sorted(guard_b)
    j = 0
    while j < 4:
        pb = shard_dir + "/iso_b%d.db" % j
        _make_single_row_shard(pb, schema, 82, 1001 + j)
        guard_b.add_handle(rc, index.ShardHandle(pb, schema, r_uint64(0), r_uint64(0)))
        j += 1

    # Guard C: 1 file at key 2000
    guard_c = flsm.LevelGuard(r_uint64(2000), r_uint64(0))
    l1.insert_guard_sorted(guard_c)
    fn_c_path = shard_dir + "/iso_c.db"
    _make_single_row_shard(fn_c_path, schema, 82, 2050)
    guard_c.add_handle(rc, index.ShardHandle(fn_c_path, schema, r_uint64(0), r_uint64(0)))
    fn_c = guard_c.handles[0].filename

    idx.compact_guard_vertical_if_needed(1)

    # L1 has exactly 2 guards (A and C remain; B promoted)
    assert_equal_i(2, len(l1.guards), "L1 should have 2 guards after promoting B")

    # A's and C's files are unchanged
    assert_equal_s(fn_a, l1.guards[0].handles[0].filename, "Guard A file should be unchanged")
    assert_equal_s(fn_c, l1.guards[1].handles[0].filename, "Guard C file should be unchanged")

    # L2 has exactly 1 guard covering key 1000
    assert_true(len(idx.levels) >= 2, "L2 should exist")
    l2 = idx.levels[1]
    assert_equal_i(1, len(l2.guards), "L2 should have exactly 1 guard")
    assert_equal_u64(r_uint64(1000), l2.guards[0].guard_key_lo, "L2 guard key should be 1000")

    # L2 guard has exactly 1 handle (Z=1)
    assert_equal_i(1, len(l2.guards[0].handles), "L2 guard should have exactly 1 handle")

    # Point lookups for all pks
    check_pks = [50, 1001, 1002, 1003, 1004, 2050]
    ci = 0
    while ci < len(check_pks):
        pk_val = check_pks[ci]
        results = idx.find_all_shards_and_indices(r_uint128(pk_val))
        assert_true(len(results) > 0, "PK %d should be present" % pk_val)
        ci += 1

    idx.close_all()
    os.write(1, "    [OK] FLSM guard isolation passed.\n")


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
        test_ephemeral_compaction(base_dir)
        test_compact_no_op_and_idempotent(base_dir)
        test_compact_preserves_memtable(base_dir)
        test_persistent_compact_if_needed(base_dir)
        test_compact_with_strings(base_dir)
        test_partitioned_compact_if_needed(base_dir)
        test_flsm_data_structures(base_dir)
        test_flsm_guard_read_path(base_dir)
        test_flsm_l0_to_l1_compaction(base_dir)
        test_flsm_manifest_persistence(base_dir)
        test_flsm_horizontal_compaction(base_dir)
        test_flsm_horizontal_ghost_elimination(base_dir)
        test_flsm_lmax_horizontal_threshold(base_dir)
        test_flsm_multilevel_compaction(base_dir)
        test_flsm_lazy_leveling_lmax(base_dir)
        test_flsm_guard_isolation(base_dir)
        test_zero_copy_wal_recovery(base_dir)
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
