# gnitz/vm/batch.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, values, strings, row_logic
from gnitz.core.row_logic import make_payload_row, PayloadRowComparator

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


class BatchAccessor(row_logic.BaseRowAccessor):
    """
    Concrete accessor for ZSetBatch rows.
    """

    _immutable_fields_ = ["schema"]

    def __init__(self, schema):
        self.schema = schema
        self.batch = None
        self.row_idx = 0

    def set_row(self, batch, index):
        self.batch = batch
        self.row_idx = index

    def _get_val(self, col_idx):
        assert self.batch is not None
        pk_idx = self.schema.pk_index
        idx = col_idx if col_idx < pk_idx else col_idx - 1
        return self.batch.payloads[self.row_idx][idx]

    def get_int(self, col_idx):
        return rffi.cast(rffi.ULONGLONG, self._get_val(col_idx).i64)

    def get_float(self, col_idx):
        return self._get_val(col_idx).f64

    def get_u128(self, col_idx):
        val = self._get_val(col_idx)
        assert val.tag == values.TAG_U128
        lo = r_uint128(r_uint64(val.i64))
        hi = r_uint128(val.u128_hi)
        return (hi << 64) | lo

    def get_str_struct(self, col_idx):
        val = self._get_val(col_idx)
        assert val.tag == values.TAG_STRING
        s = val.str_val
        length = len(s)
        prefix = rffi.cast(lltype.Signed, strings.compute_prefix(s))
        return (length, prefix, NULL_PTR, NULL_PTR, s)

    def get_col_ptr(self, col_idx):
        return NULL_PTR


class ZSetBatch(object):
    """
    A transient collection of Z-Set deltas (key, weight, payload) triples.
    Split-key implementation: 128-bit keys are stored as pairs of u64 lists
    to ensure compatibility with RPython resizable list tracing.
    """

    _immutable_fields_ = ["schema", "left_acc", "right_acc", "_row_cmp"]

    def __init__(self, schema):
        self.schema = schema
        self.keys_lo = newlist_hint(0)
        self.keys_hi = newlist_hint(0)
        self.weights = newlist_hint(0)
        self.payloads = newlist_hint(0)
        self.left_acc = BatchAccessor(schema)
        self.right_acc = BatchAccessor(schema)
        self._row_cmp = PayloadRowComparator(schema)

    def append(self, key, weight, payload):
        k = r_uint128(key)
        self.keys_lo.append(r_uint64(k))
        self.keys_hi.append(r_uint64(k >> 64))
        self.weights.append(r_int64(weight))
        self.payloads.append(payload)

    def row_count(self):
        return len(self.weights)

    def clear(self):
        del self.keys_lo[:]
        del self.keys_hi[:]
        del self.weights[:]
        del self.payloads[:]

    def _compare_indices(self, i1, i2):
        """Returns True if record i1 is strictly less than record i2."""
        hi1 = self.keys_hi[i1]
        hi2 = self.keys_hi[i2]
        if hi1 < hi2:
            return True
        if hi1 > hi2:
            return False

        lo1 = self.keys_lo[i1]
        lo2 = self.keys_lo[i2]
        if lo1 < lo2:
            return True
        if lo1 > lo2:
            return False

        return self._row_cmp.compare(self.payloads[i1], self.payloads[i2]) < 0

    def sort(self):
        """Sorts deltas by (key, payload) ready for consolidation."""
        count = self.row_count()
        if count <= 1:
            return

        for i in range(1, count):
            j = i
            while j > 0 and self._compare_indices(j, j - 1):
                # Swap Key Lo
                tmp_lo = self.keys_lo[j - 1]
                self.keys_lo[j - 1] = self.keys_lo[j]
                self.keys_lo[j] = tmp_lo

                # Swap Key Hi
                tmp_hi = self.keys_hi[j - 1]
                self.keys_hi[j - 1] = self.keys_hi[j]
                self.keys_hi[j] = tmp_hi

                # Swap Weight
                tmp_weight = self.weights[j - 1]
                self.weights[j - 1] = self.weights[j]
                self.weights[j] = tmp_weight

                # Swap Payload
                tmp_payload = self.payloads[j - 1]
                self.payloads[j - 1] = self.payloads[j]
                self.payloads[j] = tmp_payload

                j -= 1

    @jit.unroll_safe
    def consolidate(self):
        """Algebraic consolidation: sum weights of identical records, drop zeros."""
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
                if (
                    self.keys_hi[next_idx] != self.keys_hi[run_start]
                    or self.keys_lo[next_idx] != self.keys_lo[run_start]
                ):
                    break
                if self._row_cmp.compare(self.payloads[run_start], self.payloads[next_idx]) != 0:
                    break
                total_weight += self.weights[next_idx]
                next_idx += 1

            if total_weight != 0:
                self.keys_lo[write_idx] = self.keys_lo[run_start]
                self.keys_hi[write_idx] = self.keys_hi[run_start]
                self.weights[write_idx] = total_weight
                self.payloads[write_idx] = self.payloads[run_start]
                write_idx += 1

            current_idx = next_idx

        del self.keys_lo[write_idx:]
        del self.keys_hi[write_idx:]
        del self.weights[write_idx:]
        del self.payloads[write_idx:]

    def get_key(self, index):
        return (r_uint128(self.keys_hi[index]) << 64) | r_uint128(self.keys_lo[index])

    def get_weight(self, index):
        return self.weights[index]

    def get_payload(self, index):
        return self.payloads[index]
