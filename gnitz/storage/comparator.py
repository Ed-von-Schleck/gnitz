# gnitz/storage/comparator.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, strings as string_logic, values as db_values
from gnitz.storage.memtable_node import node_get_payload_ptr

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)

class RowAccessor(object):
    """
    Abstract interface for accessing column data from any storage format.
    Refactored to support direct access from TaggedValue.
    """
    def set_row(self, source, index_or_offset):
        """Resets the accessor to point to a specific row."""
        raise NotImplementedError

    def get_int(self, col_idx):
        """Returns the integer value (casted to u64 for comparison)."""
        raise NotImplementedError

    def get_float(self, col_idx):
        """Returns the float value."""
        raise NotImplementedError
    
    def get_u128(self, col_idx):
        """Returns the 128-bit integer value."""
        raise NotImplementedError

    def get_str_struct(self, col_idx):
        """
        Returns a 5-tuple compatible with strings.compare_structures:
        (length, prefix, struct_ptr, heap_ptr, python_string)
        """
        raise NotImplementedError

class PackedNodeAccessor(RowAccessor):
    """
    Accesses data from a row-oriented (AoS) MemTable node in raw memory.
    """
    def __init__(self, schema, blob_base_ptr):
        self.schema = schema
        self.blob_base_ptr = blob_base_ptr
        self.key_size = schema.get_pk_column().field_type.size
        self.payload_ptr = NULL_PTR

    def set_row(self, base_ptr, node_off):
        self.payload_ptr = node_get_payload_ptr(base_ptr, node_off, self.key_size)

    def _get_ptr(self, col_idx):
        off = self.schema.get_column_offset(col_idx)
        return rffi.ptradd(self.payload_ptr, off)

    def get_int(self, col_idx):
        ptr = self._get_ptr(col_idx)
        return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.LONGLONGP, ptr)[0])

    def get_float(self, col_idx):
        ptr = self._get_ptr(col_idx)
        return float(rffi.cast(rffi.DOUBLEP, ptr)[0])

    def get_u128(self, col_idx):
        ptr = self._get_ptr(col_idx)
        lo = rffi.cast(rffi.ULONGLONGP, ptr)[0]
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
        return (r_uint128(hi) << 64) | r_uint128(lo)

    def get_str_struct(self, col_idx):
        ptr = self._get_ptr(col_idx)
        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr[0])
        prefix = rffi.cast(lltype.Signed, u32_ptr[1])
        return (length, prefix, ptr, self.blob_base_ptr, None)

class SoAAccessor(RowAccessor):
    """
    Accesses data from a column-oriented (SoA) Table Shard View in raw memory.
    """
    def __init__(self, schema):
        self.schema = schema
        self.view = None
        self.row_idx = 0

    def set_row(self, view, row_idx):
        self.view = view
        self.row_idx = row_idx

    def _get_ptr(self, col_idx):
        return self.view.get_col_ptr(self.row_idx, col_idx)

    def get_int(self, col_idx):
        ptr = self._get_ptr(col_idx)
        return rffi.cast(rffi.ULONGLONG, rffi.cast(rffi.LONGLONGP, ptr)[0])

    def get_float(self, col_idx):
        ptr = self._get_ptr(col_idx)
        return float(rffi.cast(rffi.DOUBLEP, ptr)[0])

    def get_u128(self, col_idx):
        ptr = self._get_ptr(col_idx)
        lo = rffi.cast(rffi.ULONGLONGP, ptr)[0]
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
        return (r_uint128(hi) << 64) | r_uint128(lo)

    def get_str_struct(self, col_idx):
        ptr = self._get_ptr(col_idx)
        u32_ptr = rffi.cast(rffi.UINTP, ptr)
        length = rffi.cast(lltype.Signed, u32_ptr[0])
        prefix = rffi.cast(lltype.Signed, u32_ptr[1])
        return (length, prefix, ptr, self.view.blob_buf.ptr, None)

class ValueAccessor(RowAccessor):
    """
    Accesses data from a List[TaggedValue].
    Now uses direct field access instead of polymorphic calls.
    """
    def __init__(self, schema):
        self.schema = schema
        self.values = None
        self.pk_index = schema.pk_index

    def set_row(self, values_list, ignored=None):
        self.values = values_list

    def _get_val(self, col_idx):
        # Map schema column index to values list index (skipping PK)
        idx = col_idx if col_idx < self.pk_index else col_idx - 1
        return self.values[idx]

    def get_int(self, col_idx):
        val = self._get_val(col_idx)
        assert val.tag == db_values.TAG_INT
        return rffi.cast(rffi.ULONGLONG, val.i64)

    def get_float(self, col_idx):
        val = self._get_val(col_idx)
        assert val.tag == db_values.TAG_FLOAT
        return val.f64

    def get_u128(self, col_idx):
        # U128 non-PK columns are stored in TaggedValue.i64 
        val = self._get_val(col_idx)
        assert val.tag == db_values.TAG_INT
        return r_uint128(val.i64)

    def get_str_struct(self, col_idx):
        val = self._get_val(col_idx)
        assert val.tag == db_values.TAG_STRING
        s = val.str_val
        length = len(s)
        prefix = rffi.cast(lltype.Signed, string_logic.compute_prefix(s))
        return (length, prefix, NULL_PTR, NULL_PTR, s)


@jit.unroll_safe
def compare_rows(schema, left, right):
    """
    Unified lexicographical comparison of two rows using RowAccessors.
    The JIT specializes this loop based on the schema and accessor types.
    """
    num_cols = len(schema.columns)
    for i in range(num_cols):
        if i == schema.pk_index:
            continue
        
        col_type = schema.columns[i].field_type
        
        # 1. Handle German Strings
        if col_type == types.TYPE_STRING:
            l_len, l_pref, l_ptr, l_heap, l_str = left.get_str_struct(i)
            r_len, r_pref, r_ptr, r_heap, r_str = right.get_str_struct(i)
            
            res = string_logic.compare_structures(
                l_len, l_pref, l_ptr, l_heap, l_str,
                r_len, r_pref, r_ptr, r_heap, r_str
            )
            if res != 0:
                return res

        # 2. Handle 128-bit Integers (UUIDs/Keys)
        elif col_type == types.TYPE_U128:
            l_val = left.get_u128(i)
            r_val = right.get_u128(i)
            if l_val < r_val: return -1
            if l_val > r_val: return 1

        # 3. Handle Floating Point
        elif col_type == types.TYPE_F64:
            l_val = left.get_float(i)
            r_val = right.get_float(i)
            if l_val < r_val: return -1
            if l_val > r_val: return 1

        # 4. Handle Standard Integers (i8..u64)
        else:
            l_val = left.get_int(i)
            r_val = right.get_int(i)
            if l_val < r_val: return -1
            if l_val > r_val: return 1
            
    return 0
