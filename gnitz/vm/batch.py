# gnitz/vm/batch.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, values, strings, row_logic

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


class BatchAccessor(row_logic.BaseRowAccessor):
    """
    Concrete accessor for ZSetBatch.
    Extracts data from List[TaggedValue] stored in the batch.
    """

    _immutable_fields_ = ["schema", "dummy_ptr"]

    def __init__(self, schema):
        self.schema = schema
        # Hint the nested List[List[TaggedValue]] structure to the annotator
        self.payloads = [[values.TaggedValue.make_null()]]
        self.payloads.pop()
        self.row_idx = 0
        
        # Allocate a safe dummy pointer.
        # This is required because we hint that get_str_struct can return None for the string,
        # forcing the annotator to analyze the pointer-based comparison path in compare_structures.
        # If we passed NULL, the annotator would detect a crash and mark the result as Impossible.
        self.dummy_ptr = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
        # Zero initialize to be safe
        for i in range(16):
            self.dummy_ptr[i] = '\x00'

    def set_row(self, payloads, index):
        """
        Points the accessor to a specific row within the payloads list.
        """
        self.payloads = payloads
        self.row_idx = index

    def _get_val(self, col_idx):
        pk_idx = self.schema.pk_index
        # Map schema column index to payload list index (skipping PK)
        idx = col_idx if col_idx < pk_idx else col_idx - 1
        return self.payloads[self.row_idx][idx]

    def get_int(self, col_idx):
        val = self._get_val(col_idx)
        return rffi.cast(rffi.ULONGLONG, val.i64)

    def get_float(self, col_idx):
        val = self._get_val(col_idx)
        return val.f64

    def get_u128(self, col_idx):
        val = self._get_val(col_idx)
        return r_uint128(rffi.cast(rffi.ULONGLONG, val.i64))

    def get_str_struct(self, col_idx):
        val = self._get_val(col_idx)
        assert val.tag == values.TAG_STRING
        s = val.str_val
        length = len(s)
        prefix = rffi.cast(lltype.Signed, strings.compute_prefix(s))
        
        # Explicitly optional string to match storage accessor signatures.
        res = s
        if False:
            res = None
            
        # Return dummy_ptr instead of NULL_PTR.
        # This ensures that if the annotator explores the 'res is None' branch
        # in the consumer code, it sees valid memory access, not a segfault.
        return (length, prefix, self.dummy_ptr, self.dummy_ptr, res)

    def get_col_ptr(self, col_idx):
        """BatchAccessor operates on TaggedValues, not raw pointers."""
        return NULL_PTR


class SortItem(object):
    """
    Helper object for sorting ZSetBatch.
    Delegates comparison back to the batch to simplify annotation.
    """
    _immutable_fields_ = ['index', 'batch']

    def __init__(self, index, batch):
        self.index = index
        self.batch = batch

    def __lt__(self, other):
        # Explicit type check ensures the annotator can resolve 'other.batch'
        if not isinstance(other, SortItem):
            return False
        return self.batch._compare_indices(self.index, other.index)


class ZSetBatch(object):
    """
    A transient collection of Z-Set updates.
    The fundamental 'Batch' primitive for DBSP operators.
    """

    _immutable_fields_ = ["schema", "left_acc", "right_acc"]

    def __init__(self, schema):
        self.schema = schema
        self.keys = []  # List[r_uint128]
        self.weights = []  # List[r_int64]
        self.payloads = []  # List[List[TaggedValue]]

        # Reusable accessors for internal comparison/sorting
        self.left_acc = BatchAccessor(schema)
        self.right_acc = BatchAccessor(schema)

    def append(self, key, weight, payload):
        """Adds a delta to the batch."""
        self.keys.append(r_uint128(key))
        self.weights.append(r_int64(weight))
        self.payloads.append(payload)

    def row_count(self):
        return len(self.keys)

    def clear(self):
        """Zero-allocation reset for batch reuse."""
        del self.keys[:]
        del self.weights[:]
        del self.payloads[:]

    def _compare_indices(self, i1, i2):
        """
        Internal comparison kernel used by SortItem.
        """
        # 1. Primary Key Comparison
        k1 = self.keys[i1]
        k2 = self.keys[i2]
        if k1 < k2:
            return True
        if k1 > k2:
            return False

        # 2. Row Payload Comparison (Lexicographical)
        self.left_acc.set_row(self.payloads, i1)
        self.right_acc.set_row(self.payloads, i2)

        res = row_logic.compare_records(
            self.schema, self.left_acc, self.right_acc
        )
        return res < 0

    def sort(self):
        """Sorts deltas by Key then Payload to prepare for consolidation."""
        count = self.row_count()
        if count <= 1:
            return

        # Use append loop to build items list to ensure consistent list strategy
        items = []
        for i in range(count):
            items.append(SortItem(i, self))

        items.sort()

        # Reconstruct sorted arrays using append loops.
        # This prevents the list from being marked as non-resizable (fixed-size).
        new_keys = []
        new_weights = []
        new_payloads = []

        for i in range(count):
            idx = items[i].index
            new_keys.append(self.keys[idx])
            new_weights.append(self.weights[idx])
            new_payloads.append(self.payloads[idx])

        self.keys = new_keys
        self.weights = new_weights
        self.payloads = new_payloads

    @jit.unroll_safe
    def consolidate(self):
        """
        Algebraic Consolidation (The Ghost Property).
        Sums weights of identical records and removes those with net weight 0.
        Expects batch to be sorted.
        """
        count = self.row_count()
        if count == 0:
            return

        write_idx = 0
        current_idx = 0

        while current_idx < count:
            run_start = current_idx
            total_weight = self.weights[run_start]

            # Find all identical (Key, Payload) pairs
            next_idx = current_idx + 1
            while next_idx < count:
                if self.keys[next_idx] != self.keys[run_start]:
                    break

                self.left_acc.set_row(self.payloads, run_start)
                self.right_acc.set_row(self.payloads, next_idx)
                if (
                    row_logic.compare_records(
                        self.schema, self.left_acc, self.right_acc
                    )
                    != 0
                ):
                    break

                total_weight += self.weights[next_idx]
                next_idx += 1

            # Annihilation Logic: Only keep if net weight != 0
            if total_weight != 0:
                self.keys[write_idx] = self.keys[run_start]
                self.weights[write_idx] = total_weight
                self.payloads[write_idx] = self.payloads[run_start]
                write_idx += 1

            current_idx = next_idx

        # Truncate lists to consolidated size
        del self.keys[write_idx:]
        del self.weights[write_idx:]
        del self.payloads[write_idx:]

    def get_key(self, index):
        return self.keys[index]

    def get_weight(self, index):
        return self.weights[index]

    def get_payload(self, index):
        return self.payloads[index]
