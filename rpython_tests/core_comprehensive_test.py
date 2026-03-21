# core_comprehensive_test.py
import sys
import os

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import types, serialize, strings, comparator, batch, xxh, errors
from gnitz.core import strings as string_logic
from gnitz.core.batch import RowBuilder, ColumnarBatchAccessor
from rpython_tests.helpers.jit_stub import ensure_jit_reachable
from rpython_tests.helpers.assertions import (
    assert_true, assert_false, assert_equal_i, assert_equal_i64,
    assert_equal_u64, assert_equal_s, assert_equal_u128, assert_equal_f,
)


class DummyBlobAllocator(strings.BlobAllocator):
    """A mock allocator for testing string packing independently of MemTable/WAL."""

    def __init__(self, fixed_offset):
        self.fixed_offset = fixed_offset
        self.last_allocated = ""

    def allocate(self, string_data):
        self.last_allocated = string_data
        return r_uint64(self.fixed_offset)

    def allocate_from_ptr(self, src_ptr, length):
        self.last_allocated = rffi.charpsize2str(src_ptr, length)
        return r_uint64(self.fixed_offset)


# ------------------------------------------------------------------------------
# Test Suites
# ------------------------------------------------------------------------------


def test_types_and_schema():
    os.write(1, "[Core] Testing Schema & Layout Algebra...\n")

    cols = newlist_hint(4)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I8, name="i8_col"))
    cols.append(types.ColumnDefinition(types.TYPE_U128, name="uuid_col"))
    cols.append(types.ColumnDefinition(types.TYPE_STRING, name="str_col"))

    schema = types.TableSchema(cols, pk_index=0)

    # Max alignment across types is 16 (from U128). 32 + 16 (string size) = 48.
    # 48 is perfectly divisible by 16.
    assert_equal_i(48, schema.memtable_stride, "Schema stride calculation mismatch")

    # Validate Schema Algebra (Join Merge)
    right_cols = newlist_hint(2)
    right_cols.append(types.ColumnDefinition(types.TYPE_U64, name="right_pk"))
    right_cols.append(types.ColumnDefinition(types.TYPE_F64, name="right_f64"))
    schema_right = types.TableSchema(right_cols, pk_index=0)

    merged = types.merge_schemas_for_join(schema, schema_right)
    # Output Schema =[Left_PK] + [Left_Payloads...] + [Right_Payloads...]
    assert_equal_i(5, len(merged.columns), "Merged schema column count mismatch")
    assert_equal_i(
        types.TYPE_F64.code,
        merged.columns[4].field_type.code,
        "Merged payload type mismatch",
    )

    # Schema Enforcement Edge Cases
    raised = False
    try:
        bad_cols = newlist_hint(1)
        bad_cols.append(types.ColumnDefinition(types.TYPE_STRING, name="bad_pk"))
        types.TableSchema(bad_cols, pk_index=0)
    except errors.LayoutError:
        raised = True
    assert_true(raised, "Schema failed to reject non-integer PK type")

    os.write(1, "    [OK] Schema & Layout passed.\n")


def test_strings_german_layout():
    os.write(1, "[Core] Testing German String Optimization...\n")

    # 1. Prefix Calculation
    p_short = strings.compute_prefix("AB")
    p_long = strings.compute_prefix("ABCDEF")

    # "AB": A=65, B=66. Packed little endian: 65 | (66 << 8) = 65 | 16896 = 16961
    assert_equal_u64(
        r_uint64(16961),
        rffi.cast(rffi.ULONGLONG, p_short),
        "Short prefix packing mismatch",
    )

    # 2. Packing and Relocation
    allocator = DummyBlobAllocator(fixed_offset=4096)
    target = lltype.malloc(rffi.CCHARP.TO, 16, flavor="raw")

    try:
        # Scenario A: Short String (Inline)
        short_s = "ShortStr"
        strings.pack_and_write_blob(target, short_s, allocator)

        u32_ptr = rffi.cast(rffi.UINTP, target)
        assert_equal_i(8, intmask(u32_ptr[0]), "German struct length mismatch (short)")
        assert_equal_s(
            "tStr",
            rffi.charpsize2str(rffi.ptradd(target, 8), 4),
            "German struct inline payload mismatch",
        )

        # Scenario B: Long String (Heap allocated)
        long_s = "ThisIsALongStringForHeap"
        strings.pack_and_write_blob(target, long_s, allocator)

        assert_equal_i(24, intmask(u32_ptr[0]), "German struct length mismatch (long)")
        assert_equal_s(
            long_s,
            allocator.last_allocated,
            "Allocator didn't receive full long string",
        )

        u64_payload = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0]
        assert_equal_u64(
            r_uint64(4096),
            rffi.cast(rffi.ULONGLONG, u64_payload),
            "German struct heap offset mismatch",
        )

    finally:
        lltype.free(target, flavor="raw")

    # 3. O(1) Equality & Content Comparisons
    # Use rffi.cast(rffi.LONGLONG, ...) instead of r_uint32 to ensure RPython
    # unifies the prefix types as signed 64-bit integers uniformly across the program.
    res1 = strings.compare_structures(
        4,
        rffi.cast(rffi.LONGLONG, 100),
        lltype.nullptr(rffi.CCHARP.TO),
        lltype.nullptr(rffi.CCHARP.TO),
        "AAAA",
        4,
        rffi.cast(rffi.LONGLONG, 200),
        lltype.nullptr(rffi.CCHARP.TO),
        lltype.nullptr(rffi.CCHARP.TO),
        "BBBB",
    )
    assert_equal_i(-1, res1, "O(1) prefix rejection failed")

    # Tie-breaker logic (Length matching, prefixes match, diff char later)
    res2 = strings.compare_structures(
        5,
        rffi.cast(rffi.LONGLONG, 100),
        lltype.nullptr(rffi.CCHARP.TO),
        lltype.nullptr(rffi.CCHARP.TO),
        "TestA",
        5,
        rffi.cast(rffi.LONGLONG, 100),
        lltype.nullptr(rffi.CCHARP.TO),
        lltype.nullptr(rffi.CCHARP.TO),
        "TestB",
    )
    assert_equal_i(-1, res2, "Suffix tie-breaker loop failed")

    os.write(1, "    [OK] German Strings passed.\n")


def test_serialize_and_hash():
    os.write(1, "[Core] Testing Stable XXHash...\n")

    cols = newlist_hint(3)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="i64"))
    cols.append(types.ColumnDefinition(types.TYPE_STRING, name="str"))
    schema = types.TableSchema(cols, pk_index=0)

    # Base Row — build into a batch and get an accessor
    b1 = batch.ArenaZSetBatch(schema, initial_capacity=4)
    rb1 = RowBuilder(schema, b1)
    rb1.begin(r_uint64(1), r_uint64(0), r_int64(1))
    rb1.put_int(rffi.cast(rffi.LONGLONG, 12345))
    rb1.put_string("Deterministic")
    rb1.commit()

    accessor = b1.get_accessor(0)

    # 1. Test Stable Hashing
    hash_buf = lltype.nullptr(rffi.CCHARP.TO)
    try:
        # Call 1
        h1, hash_buf, cap1 = serialize.compute_hash(schema, accessor, hash_buf, 0)
        # Call 2 (tests reuse of same buffer without dirty padding bytes)
        h2, hash_buf, cap2 = serialize.compute_hash(schema, accessor, hash_buf, cap1)

        assert_equal_u64(h1, h2, "Serialization hash is unstable across sequential calls")

        # Prove alteration changes hash — build a mutated row in a new batch
        b2 = batch.ArenaZSetBatch(schema, initial_capacity=4)
        rb2 = RowBuilder(schema, b2)
        rb2.begin(r_uint64(1), r_uint64(0), r_int64(1))
        rb2.put_int(rffi.cast(rffi.LONGLONG, 12345))
        rb2.put_string("Mutated_Now_Invalidated")
        rb2.commit()

        accessor_mut = b2.get_accessor(0)

        h3, hash_buf, cap3 = serialize.compute_hash(
            schema, accessor_mut, hash_buf, cap2
        )
        assert_true(h1 != h3, "Serialization hash failed to detect mutation")

        b2.free()
    finally:
        if hash_buf:
            lltype.free(hash_buf, flavor="raw")

    b1.free()
    os.write(1, "    [OK] Stable Hash passed.\n")


def test_comparator():
    os.write(1, "[Core] Testing Cross-Format Comparator...\n")

    cols = newlist_hint(3)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, is_nullable=True, name="i64"))
    cols.append(types.ColumnDefinition(types.TYPE_F64, name="f64"))
    schema = types.TableSchema(cols, pk_index=0)

    b = batch.ArenaZSetBatch(schema, initial_capacity=4)
    rb = RowBuilder(schema, b)

    # Row A: null col 1, float -5.0 for col 2
    rb.begin(r_uint64(1), r_uint64(0), r_int64(1))
    rb.put_null()
    rb.put_float(-5.0)
    rb.commit()

    # Row B: int 10 for col 1, float 5.0 for col 2
    rb.begin(r_uint64(2), r_uint64(0), r_int64(1))
    rb.put_int(rffi.cast(rffi.LONGLONG, 10))
    rb.put_float(5.0)
    rb.commit()

    # Row C: int 10 for col 1, float -5.0 for col 2
    rb.begin(r_uint64(3), r_uint64(0), r_int64(1))
    rb.put_int(rffi.cast(rffi.LONGLONG, 10))
    rb.put_float(-5.0)
    rb.commit()

    acc_a = ColumnarBatchAccessor(schema)
    acc_a.bind(b, 0)
    acc_b = ColumnarBatchAccessor(schema)
    acc_b.bind(b, 1)
    acc_c = ColumnarBatchAccessor(schema)
    acc_c.bind(b, 2)

    # Nulls come first
    assert_equal_i(
        -1,
        comparator.compare_rows(schema, acc_a, acc_b),
        "Null comparison failure (Null < Val)",
    )
    assert_equal_i(
        1,
        comparator.compare_rows(schema, acc_b, acc_a),
        "Null comparison failure (Val > Null)",
    )

    # Float comparisons
    assert_equal_i(
        1,
        comparator.compare_rows(schema, acc_b, acc_c),
        "Float comparison failure (5.0 > -5.0)",
    )
    assert_equal_i(
        0,
        comparator.compare_rows(schema, acc_c, acc_c),
        "Equality failure",
    )

    b.free()
    os.write(1, "    [OK] Comparator passed.\n")


def test_batch_operations():
    os.write(1, "[Core] Testing ZSetBatch, Sorting & The Ghost Property...\n")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U128, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_STRING, name="payload"))
    schema = types.TableSchema(cols, pk_index=0)

    b = batch.ArenaZSetBatch(schema, initial_capacity=10)

    pk1 = r_uint128(100)
    pk2 = r_uint128(200)

    rb = RowBuilder(schema, b)

    # Insert unordered mix
    rb.begin(r_uint64(pk2), r_uint64(pk2 >> 64), r_int64(5))
    rb.put_string("Payload_B")
    rb.commit()

    rb.begin(r_uint64(pk1), r_uint64(pk1 >> 64), r_int64(10))
    rb.put_string("Payload_A")
    rb.commit()

    rb.begin(r_uint64(pk2), r_uint64(pk2 >> 64), r_int64(-5))
    rb.put_string("Payload_B")
    rb.commit()

    rb.begin(r_uint64(pk1), r_uint64(pk1 >> 64), r_int64(5))
    rb.put_string("Payload_A")
    rb.commit()

    assert_equal_i(4, b.length(), "Pre-sort length mismatch")
    assert_false(b.is_sorted(), "Batch erroneously flagged as sorted")

    # Sort (functional: to_sorted() returns a new batch, SortedScope frees it)
    with batch.SortedScope(b) as sorted_b:
        assert_true(sorted_b.is_sorted(), "Sort failed to flag batch as sorted")

        # Post sort, PK1 (100) should be before PK2 (200)
        assert_equal_u128(pk1, sorted_b.get_pk(0), "Sort order mismatch idx 0")
        assert_equal_u128(pk1, sorted_b.get_pk(1), "Sort order mismatch idx 1")
        assert_equal_u128(pk2, sorted_b.get_pk(2), "Sort order mismatch idx 2")
        assert_equal_u128(pk2, sorted_b.get_pk(3), "Sort order mismatch idx 3")

    # Consolidate (Algebraic Merge + Ghost Pruning)
    # ConsolidatedScope handles sorting internally and frees the temporary.
    #
    # Expected End State:
    # PK 100, "Payload_A": W = 10 + 5 = 15
    # PK 200, "Payload_B": W = 5 - 5 = 0 -> GHOSTED (Removed)
    with batch.ConsolidatedScope(b) as cb:
        assert_equal_i(
            1,
            cb.length(),
            "Ghost Property failed: zero-weight record was preserved",
        )
        assert_equal_u128(pk1, cb.get_pk(0), "Consolidated PK mismatch")
        assert_equal_i64(
            r_int64(15), cb.get_weight(0), "Algebraic summation mismatch"
        )

        # Read the row back to ensure blob strings survived the relocation passes
        acc = cb.get_accessor(0)
        length, prefix, sptr, hptr, py_s = acc.get_str_struct(1)
        if py_s is not None:
            s = py_s
        else:
            s = string_logic.unpack_string(sptr, hptr)
        assert_equal_s("Payload_A", s, "Consolidated payload string mismatch")

    b.free()
    os.write(1, "    [OK] ZSetBatch & Ghost Property passed.\n")


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------


def entry_point(argv):
    ensure_jit_reachable()
    os.write(1, "--- GnitzDB Comprehensive Core Package Test ---\n")

    try:
        test_types_and_schema()
        test_strings_german_layout()
        test_serialize_and_hash()
        test_comparator()
        test_batch_operations()
        os.write(1, "\nALL CORE TEST PATHS PASSED\n")
    except Exception as e:
        os.write(2, "TEST FAILED: " + str(e) + "\n")
        return 1

    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
