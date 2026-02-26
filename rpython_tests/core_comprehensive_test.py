# core_comprehensive_test.py
import sys
import os

from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import types, values, serialize, strings, comparator, batch, xxh, errors

# ------------------------------------------------------------------------------
# Helpers & Assertions
# ------------------------------------------------------------------------------

def assert_true(condition, msg):
    if not condition:
        raise Exception("Assertion Failed: " + msg)

def assert_false(condition, msg):
    if condition:
        raise Exception("Assertion Failed: " + msg)

def assert_equal_i(expected, actual, msg):
    if expected != actual:
        raise Exception("Assertion Failed (int): " + msg)

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

def assert_equal_f(expected, actual, msg):
    if expected != actual:
        raise Exception("Assertion Failed (float): " + msg)

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

    # Validate logical layout & physical offset calculation rules
    # PK (col 0) -> always -1 offset, stripped from packed row payload
    # I8 (col 1) -> offset 0
    # U128 (col 2) -> must align to 16. Current is 1 -> jumps to 16.
    # STR (col 3) -> size 16, aligns to 8. Current is 32 -> remains 32.
    assert_equal_i(-1, schema.get_column_offset(0), "PK offset must be -1")
    assert_equal_i(0, schema.get_column_offset(1), "I8 offset mismatch")
    assert_equal_i(16, schema.get_column_offset(2), "U128 alignment padding failed")
    assert_equal_i(32, schema.get_column_offset(3), "String offset mismatch")
    
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
    assert_equal_i(types.TYPE_F64.code, merged.columns[4].field_type.code, "Merged payload type mismatch")

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


def test_values_and_payload_row():
    os.write(1, "[Core] Testing Struct-of-Arrays PayloadRow...\n")

    cols = newlist_hint(6)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I32, name="i32"))
    cols.append(types.ColumnDefinition(types.TYPE_F64, name="f64"))
    cols.append(types.ColumnDefinition(types.TYPE_U128, name="u128"))
    cols.append(types.ColumnDefinition(types.TYPE_STRING, name="str"))
    cols.append(types.ColumnDefinition(types.TYPE_U64, is_nullable=True, name="nullable_u64"))
    
    schema = types.TableSchema(cols, pk_index=0)
    row = values.make_payload_row(schema)

    # Ensure derivation flags are correctly pulled
    assert_true(row._has_u128, "Schema derivation failed on has_u128")
    assert_true(row._has_string, "Schema derivation failed on has_string")
    assert_true(row._has_nullable, "Schema derivation failed on has_nullable")

    # 1. Integers & Sign Extension
    # Testing storage of negative numbers via unsigned bitcasts
    row.append_int(rffi.cast(rffi.LONGLONG, -9999))
    
    # 2. Exact Float Bit Representation
    row.append_float(3.14159)
    
    # 3. Native U128 split logic (avoiding > sys.maxint literal trap)
    hi_word = r_uint64(1000)
    lo_word = r_uint64(2000)
    row.append_u128(lo_word, hi_word)
    
    # 4. String Heap avoidance
    row.append_string("GnitzDB")

    # 5. Null scalar tracking
    row.append_null(4)

    # Asserts
    assert_equal_i64(r_int64(-9999), row.get_int_signed(0), "Signed I32 readback mismatch")
    assert_equal_f(3.14159, row.get_float(1), "Float IEEE754 roundtrip mismatch")
    
    u128_val = row.get_u128(2)
    expected_u128 = (r_uint128(hi_word) << 64) | r_uint128(lo_word)
    assert_equal_u128(expected_u128, u128_val, "U128 hi/lo split construction mismatch")
    
    assert_equal_s("GnitzDB", row.get_str(3), "String accessor mismatch")
    assert_true(row.is_null(4), "Null bitfield flag mismatch")
    assert_false(row.is_null(0), "Null bitfield bleeding mismatch")

    os.write(1, "[OK] PayloadRow passed.\n")


def test_strings_german_layout():
    os.write(1, "[Core] Testing German String Optimization...\n")

    # 1. Prefix Calculation
    p_short = strings.compute_prefix("AB")
    p_long = strings.compute_prefix("ABCDEF")
    
    # "AB": A=65, B=66. Packed little endian: 65 | (66 << 8) = 65 | 16896 = 16961
    assert_equal_u64(r_uint64(16961), rffi.cast(rffi.ULONGLONG, p_short), "Short prefix packing mismatch")
    
    # 2. Packing and Relocation 
    allocator = DummyBlobAllocator(fixed_offset=4096)
    target = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
    
    try:
        # Scenario A: Short String (Inline)
        short_s = "ShortStr"
        strings.pack_and_write_blob(target, short_s, allocator)
        
        u32_ptr = rffi.cast(rffi.UINTP, target)
        assert_equal_i(8, intmask(u32_ptr[0]), "German struct length mismatch (short)")
        assert_equal_s("tStr", rffi.charpsize2str(rffi.ptradd(target, 8), 4), "German struct inline payload mismatch")
        
        # Scenario B: Long String (Heap allocated)
        long_s = "ThisIsALongStringForHeap"
        strings.pack_and_write_blob(target, long_s, allocator)
        
        assert_equal_i(24, intmask(u32_ptr[0]), "German struct length mismatch (long)")
        assert_equal_s(long_s, allocator.last_allocated, "Allocator didn't receive full long string")
        
        u64_payload = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0]
        assert_equal_u64(r_uint64(4096), rffi.cast(rffi.ULONGLONG, u64_payload), "German struct heap offset mismatch")

    finally:
        lltype.free(target, flavor='raw')

    # 3. O(1) Equality & Content Comparisons
    # Use rffi.cast(rffi.LONGLONG, ...) instead of r_uint32 to ensure RPython 
    # unifies the prefix types as signed 64-bit integers uniformly across the program.
    res1 = strings.compare_structures(
        4, rffi.cast(rffi.LONGLONG, 100), lltype.nullptr(rffi.CCHARP.TO), lltype.nullptr(rffi.CCHARP.TO), "AAAA",
        4, rffi.cast(rffi.LONGLONG, 200), lltype.nullptr(rffi.CCHARP.TO), lltype.nullptr(rffi.CCHARP.TO), "BBBB"
    )
    assert_equal_i(-1, res1, "O(1) prefix rejection failed")

    # Tie-breaker logic (Length matching, prefixes match, diff char later)
    res2 = strings.compare_structures(
        5, rffi.cast(rffi.LONGLONG, 100), lltype.nullptr(rffi.CCHARP.TO), lltype.nullptr(rffi.CCHARP.TO), "TestA",
        5, rffi.cast(rffi.LONGLONG, 100), lltype.nullptr(rffi.CCHARP.TO), lltype.nullptr(rffi.CCHARP.TO), "TestB"
    )
    assert_equal_i(-1, res2, "Suffix tie-breaker loop failed")

    os.write(1, "    [OK] German Strings passed.\n")


def test_serialize_and_hash():
    os.write(1, "[Core] Testing Raw C Serialization & Stable XXHash...\n")

    cols = newlist_hint(3)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, name="i64"))
    cols.append(types.ColumnDefinition(types.TYPE_STRING, name="str"))
    schema = types.TableSchema(cols, pk_index=0)

    # Base Row
    row = values.make_payload_row(schema)
    row.append_int(rffi.cast(rffi.LONGLONG, 12345))
    row.append_string("Deterministic")

    accessor = comparator.PayloadRowAccessor(schema)
    accessor.set_row(row)

    # 1. Test Stable Hashing
    hash_buf = lltype.nullptr(rffi.CCHARP.TO)
    try:
        # Call 1
        h1, hash_buf, cap1 = serialize.compute_hash(schema, accessor, hash_buf, 0)
        # Call 2 (tests reuse of same buffer without dirty padding bytes)
        h2, hash_buf, cap2 = serialize.compute_hash(schema, accessor, hash_buf, cap1)
        
        assert_equal_u64(h1, h2, "Serialization hash is unstable across sequential calls")

        # Prove alteration changes hash
        # We must build a new PayloadRow because mutation of completed rows is forbidden.
        row_mut = values.make_payload_row(schema)
        row_mut.append_int(rffi.cast(rffi.LONGLONG, 12345))
        row_mut.append_string("Mutated_Now_Invalidated")
        accessor.set_row(row_mut)

        h3, hash_buf, cap3 = serialize.compute_hash(schema, accessor, hash_buf, cap2)
        assert_true(h1 != h3, "Serialization hash failed to detect mutation")
    finally:
        if hash_buf:
            lltype.free(hash_buf, flavor='raw')

    # Re-establish stable row
    row2 = values.make_payload_row(schema)
    row2.append_int(rffi.cast(rffi.LONGLONG, 999))
    row2.append_string("Roundtrip")
    accessor.set_row(row2)

    # 2. Test Binary Roundtrip (AoS)
    dest_size = schema.memtable_stride
    dest_ptr = lltype.malloc(rffi.CCHARP.TO, dest_size, flavor='raw')
    allocator = DummyBlobAllocator(fixed_offset=0)
    try:
        serialize.serialize_row(schema, accessor, dest_ptr, allocator)
        
        # Deserialize from pointer
        recovered_row = serialize.deserialize_row(schema, dest_ptr, lltype.nullptr(rffi.CCHARP.TO), r_uint64(0))
        
        assert_equal_i64(r_int64(999), recovered_row.get_int_signed(0), "Recovered int mismatch")
        assert_equal_s("Roundtrip", recovered_row.get_str(1), "Recovered inline string mismatch")
    finally:
        lltype.free(dest_ptr, flavor='raw')

    os.write(1, "    [OK] Serialization & Hash passed.\n")


def test_comparator():
    os.write(1, "[Core] Testing Cross-Format Comparator...\n")

    cols = newlist_hint(3)
    cols.append(types.ColumnDefinition(types.TYPE_U64, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_I64, is_nullable=True, name="i64"))
    cols.append(types.ColumnDefinition(types.TYPE_F64, name="f64"))
    schema = types.TableSchema(cols, pk_index=0)

    # Row A: Null, -5.0
    row_a = values.make_payload_row(schema)
    row_a.append_null(0)
    row_a.append_float(-5.0)
    
    # Row B: 10, 5.0
    row_b = values.make_payload_row(schema)
    row_b.append_int(rffi.cast(rffi.LONGLONG, 10))
    row_b.append_float(5.0)

    # Row C: 10, -5.0
    row_c = values.make_payload_row(schema)
    row_c.append_int(rffi.cast(rffi.LONGLONG, 10))
    row_c.append_float(-5.0)

    acc_a = comparator.PayloadRowAccessor(schema)
    acc_a.set_row(row_a)
    acc_b = comparator.PayloadRowAccessor(schema)
    acc_b.set_row(row_b)
    acc_c = comparator.PayloadRowAccessor(schema)
    acc_c.set_row(row_c)

    # Nulls come first
    assert_equal_i(-1, comparator.compare_rows(schema, acc_a, acc_b), "Null comparison failure (Null < Val)")
    assert_equal_i(1, comparator.compare_rows(schema, acc_b, acc_a), "Null comparison failure (Val > Null)")
    
    # Float comparisons
    assert_equal_i(1, comparator.compare_rows(schema, acc_b, acc_c), "Float comparison failure (5.0 > -5.0)")
    assert_equal_i(0, comparator.compare_rows(schema, acc_c, acc_c), "Equality failure")

    os.write(1, "    [OK] Comparator passed.\n")


def test_batch_operations():
    os.write(1, "[Core] Testing ZSetBatch, Sorting & The Ghost Property...\n")

    cols = newlist_hint(2)
    cols.append(types.ColumnDefinition(types.TYPE_U128, name="pk"))
    cols.append(types.ColumnDefinition(types.TYPE_STRING, name="payload"))
    schema = types.TableSchema(cols, pk_index=0)

    b = batch.ArenaZSetBatch(schema, initial_capacity=10)

    # Verify Alignment 
    assert_equal_i(0, b.record_stride % 16, "Arena stride is not 16-byte aligned")

    pk1 = r_uint128(100)
    pk2 = r_uint128(200)

    # Insert unordered mix
    row_a = values.make_payload_row(schema)
    row_a.append_string("Payload_A")
    row_b = values.make_payload_row(schema)
    row_b.append_string("Payload_B")
    
    b.append(pk2, r_int64(5), row_b)       # ID 0
    b.append(pk1, r_int64(10), row_a)      # ID 1
    b.append(pk2, r_int64(-5), row_b)      # ID 2 (Annihilates ID 0)
    b.append(pk1, r_int64(5), row_a)       # ID 3 (Adds to ID 1)

    assert_equal_i(4, b.length(), "Pre-sort length mismatch")
    assert_false(b.is_sorted(), "Batch erroneously flagged as sorted")

    # Sort
    b.sort()
    assert_true(b.is_sorted(), "Sort failed to flag batch as sorted")

    # Post sort, PK1 (100) should be before PK2 (200)
    assert_equal_u128(pk1, b.get_pk(0), "Sort order mismatch idx 0")
    assert_equal_u128(pk1, b.get_pk(1), "Sort order mismatch idx 1")
    assert_equal_u128(pk2, b.get_pk(2), "Sort order mismatch idx 2")
    assert_equal_u128(pk2, b.get_pk(3), "Sort order mismatch idx 3")

    # Consolidate (Algebraic Merge + Ghost Pruning)
    b.consolidate()
    
    # Expected End State:
    # PK 100, "Payload_A": W = 10 + 5 = 15
    # PK 200, "Payload_B": W = 5 - 5 = 0 -> GHOSTED (Removed)
    
    assert_equal_i(1, b.length(), "Ghost Property failed: zero-weight record was preserved")
    assert_equal_u128(pk1, b.get_pk(0), "Consolidated PK mismatch")
    assert_equal_i64(r_int64(15), b.get_weight(0), "Algebraic summation mismatch")
    
    # Read the row back to ensure blob strings survived the relocation passes
    rec_row = b.get_row(0)
    assert_equal_s("Payload_A", rec_row.get_str(0), "Consolidated payload string mismatch")

    b.free()
    os.write(1, "    [OK] ZSetBatch & Ghost Property passed.\n")


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------

def entry_point(argv):
    os.write(1, "--- GnitzDB Comprehensive Core Package Test ---\n")

    try:
        test_types_and_schema()
        test_values_and_payload_row()
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
