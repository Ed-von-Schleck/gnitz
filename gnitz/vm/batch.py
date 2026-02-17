# gnitz/vm/batch.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, values, strings, row_logic

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


class BatchAccessor(row_logic.BaseRowAccessor):
    """
    Concrete accessor for ZSetBatch.
    """
    def __init__(self, schema):
        self.schema = schema
        # In RPython, initialize to None but use asserts to prove type/presence
        self.payloads = None 
        self.row_idx = -1

    def set_row(self, payloads, index):
        self.payloads = payloads
        self.row_idx = index

    def _get_val(self, col_idx):
        # 1. Prove to Annotator that payloads is not None
        payloads = self.payloads
        assert payloads is not None
        
        # 2. Fix pk_index reference and prove index is non-negative
        pk_idx = self.schema.pk_index
        idx = col_idx if col_idx < pk_idx else col_idx - 1
        
        # RPython hint: Prove idx is within valid range for non-PK payload
        assert idx >= 0
        
        return payloads[self.row_idx][idx]

    def get_int(self, col_idx):
        val = self._get_val(col_idx)
        return rffi.cast(rffi.ULONGLONG, val.i64)

    def get_float(self, col_idx):
        val = self._get_val(col_idx)
        return val.f64
    
    def get_u128(self, col_idx):
        val = self._get_val(col_idx)
        # Note: Non-PK u128s are stored in i64 in TaggedValue for simplicity
        return r_uint128(val.i64)

    def get_str_struct(self, col_idx):
        val = self._get_val(col_idx)
        s = val.str_val
        length = len(s)
        prefix = rffi.cast(lltype.Signed, strings.compute_prefix(s))
        # Return tuple: (length, prefix, struct_ptr, heap_ptr, python_string)
        return (length, prefix, NULL_PTR, NULL_PTR, s)


class SortItem(object):
    """
    Helper object for sorting ZSetBatch. 
    Wrapping indices in objects allows us to use RPython's list.sort().
    """
    def __init__(self, batch, index):
        self.batch = batch
        self.index = index

    def __lt__(self, other):
        # Primary Key Comparison
        k1 = self.batch.keys[self.index]
        k2 = self.batch.keys[other.index]
        if k1 < k2: return True
        if k1 > k2: return False
        
        # Row Payload Comparison (Lexicographical)
        # Uses the shared row_logic kernel
        self.batch.left_acc.set_row(self.batch.payloads, self.index)
        self.batch.right_acc.set_row(self.batch.payloads, other.index)
        
        res = row_logic.compare_records(
            self.batch.schema, 
            self.batch.left_acc, 
            self.batch.right_acc
        )
        return res < 0


class ZSetBatch(object):
    """
    A transient collection of Z-Set updates.
    The fundamental 'Batch' primitive for DBSP operators.
    """
    _immutable_fields_ = ['schema', 'left_acc', 'right_acc']

    def __init__(self, schema):
        self.schema = schema
        self.keys = []      # List[r_uint128]
        self.weights = []   # List[r_int64]
        self.payloads = []   # List[List[TaggedValue]]
        
        # Reusable accessors for comparison/sorting
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

    def sort(self):
        """Sorts deltas by Key then Payload to prepare for consolidation."""
        count = self.row_count()
        if count <= 1:
            return
            
        # Create an index map to sort
        items = [SortItem(self, i) for i in range(count)]
        items.sort()
        
        # Permute parallel lists based on sorted items
        new_keys = [r_uint128(0)] * count
        new_weights = [r_int64(0)] * count
        new_payloads = [None] * count
        
        for i in range(count):
            old_idx = items[i].index
            new_keys[i] = self.keys[old_idx]
            new_weights[i] = self.weights[old_idx]
            new_payloads[i] = self.payloads[old_idx]
            
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
                # Compare Keys
                if self.keys[next_idx] != self.keys[run_start]:
                    break
                
                # Compare Payloads
                self.left_acc.set_row(self.payloads, run_start)
                self.right_acc.set_row(self.payloads, next_idx)
                if row_logic.compare_records(self.schema, self.left_acc, self.right_acc) != 0:
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
