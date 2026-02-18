# gnitz/vm/batch.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, values, strings, row_logic
from gnitz.core.row_logic import make_payload_row

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)

@jit.unroll_safe
def _compare_payload_rows(schema, left_row, right_row):
    """
    Compares two rows stored as List[TaggedValue] using pure value comparisons.
    Bypasses compare_rows/compare_structures so BatchAccessor objects never
    enter the type-union for those functions' parameters.
    """
    pk_idx = schema.pk_index
    payload_idx = 0
    for i in range(len(schema.columns)):
        if i == pk_idx:
            continue
        col_type = schema.columns[i].field_type
        l_val = left_row[payload_idx]
        r_val = right_row[payload_idx]
        payload_idx += 1

        if col_type == types.TYPE_STRING:
            if l_val.str_val < r_val.str_val:
                return -1
            if l_val.str_val > r_val.str_val:
                return 1
        elif col_type == types.TYPE_F64:
            if l_val.f64 < r_val.f64:
                return -1
            if l_val.f64 > r_val.f64:
                return 1
        else:
            if l_val.i64 < r_val.i64:
                return -1
            if l_val.i64 > r_val.i64:
                return 1
    return 0


class BatchAccessor(row_logic.BaseRowAccessor):
    """
    Concrete accessor for ZSetBatch rows.
    Used by ops.py (op_filter / op_map) to expose rows to ScalarFunctions.
    Never passed to compare_rows / compare_structures.
    """

    _immutable_fields_ = ["schema", "dummy_ptr"]

    def __init__(self, schema):
        self.schema = schema
        # Type hint: teach the annotator that payloads is List[List[TaggedValue]]
        # where the inner list is resizable.  newlist_hint ensures the inner
        # list's listdef is NOT mr.
        _inner = make_payload_row(1)
        self.payloads = [_inner]
        self.payloads.pop()
        self.row_idx = 0
        # Zeroed 16-byte buffer returned as (ptr, heap) in get_str_struct.
        # Keeps the annotator off a null-dereference path in compare_structures
        # without using a fixed-literal that would mark the struct mr.
        self.dummy_ptr = lltype.malloc(rffi.CCHARP.TO, 16, flavor="raw")
        for i in range(16):
            self.dummy_ptr[i] = "\x00"

    def set_row(self, payloads, index):
        self.payloads = payloads
        self.row_idx = index

    def _get_val(self, col_idx):
        pk_idx = self.schema.pk_index
        idx = col_idx if col_idx < pk_idx else col_idx - 1
        return self.payloads[self.row_idx][idx]

    def get_int(self, col_idx):
        return rffi.cast(rffi.ULONGLONG, self._get_val(col_idx).i64)

    def get_float(self, col_idx):
        return self._get_val(col_idx).f64

    def get_u128(self, col_idx):
        return r_uint128(rffi.cast(rffi.ULONGLONG, self._get_val(col_idx).i64))

    def get_str_struct(self, col_idx):
        val = self._get_val(col_idx)
        assert val.tag == values.TAG_STRING
        s = val.str_val
        length = len(s)
        prefix = rffi.cast(lltype.Signed, strings.compute_prefix(s))
        return (length, prefix, self.dummy_ptr, self.dummy_ptr, s)

    def get_col_ptr(self, col_idx):
        return NULL_PTR


class ZSetBatch(object):
    """
    A transient collection of Z-Set deltas (key, weight, payload) triples.
    The fundamental Batch primitive for DBSP operators.
    """

    _immutable_fields_ = ["schema", "left_acc"]

    def __init__(self, schema):
        self.schema = schema
        self.keys = []
        self.weights = []
        self.payloads = []
        self.left_acc = BatchAccessor(schema)

    def append(self, key, weight, payload):
        self.keys.append(r_uint128(key))
        self.weights.append(r_int64(weight))
        self.payloads.append(payload)

    def row_count(self):
        return len(self.keys)

    def clear(self):
        del self.keys[:]
        del self.weights[:]
        del self.payloads[:]

    def _compare_indices(self, i1, i2):
        """Returns True if record i1 is strictly less than record i2."""
        k1 = self.keys[i1]
        k2 = self.keys[i2]
        if k1 < k2:
            return True
        if k1 > k2:
            return False
        return (
            _compare_payload_rows(self.schema, self.payloads[i1], self.payloads[i2])
            < 0
        )

    def sort(self):
        """
        Sorts deltas by (key, payload) ready for consolidation.
        """
        count = self.row_count()
        if count <= 1:
            return

        for i in range(1, count):
            j = i
            while j > 0 and self._compare_indices(j, j - 1):
                tmp_key = self.keys[j - 1]
                self.keys[j - 1] = self.keys[j]
                self.keys[j] = tmp_key

                tmp_weight = self.weights[j - 1]
                self.weights[j - 1] = self.weights[j]
                self.weights[j] = tmp_weight

                tmp_payload = self.payloads[j - 1]
                self.payloads[j - 1] = self.payloads[j]
                self.payloads[j] = tmp_payload

                j -= 1

    @jit.unroll_safe
    def consolidate(self):
        """
        Algebraic consolidation: sum weights of identical records, drop zeros.
        Batch must be sorted first.
        """
        count = self.row_count()
        if count == 0:
            return

        write_idx = 0
        current_idx = 0

        while current_idx < count:
            run_start = current_idx
            total_weight = self.weights[run_start]
            next_idx = current_idx + 1

            while next_idx < count:
                if self.keys[next_idx] != self.keys[run_start]:
                    break
                if (
                    _compare_payload_rows(
                        self.schema,
                        self.payloads[run_start],
                        self.payloads[next_idx],
                    )
                    != 0
                ):
                    break
                total_weight += self.weights[next_idx]
                next_idx += 1

            if total_weight != 0:
                self.keys[write_idx] = self.keys[run_start]
                self.weights[write_idx] = total_weight
                self.payloads[write_idx] = self.payloads[run_start]
                write_idx += 1

            current_idx = next_idx

        del self.keys[write_idx:]
        del self.weights[write_idx:]
        del self.payloads[write_idx:]

    def get_key(self, index):
        return self.keys[index]

    def get_weight(self, index):
        return self.weights[index]

    def get_payload(self, index):
        return self.payloads[index]
