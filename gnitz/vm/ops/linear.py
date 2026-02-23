# gnitz/vm/ops/linear.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask, r_uint
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import strings, types
from gnitz.core.comparator import RowAccessor
from gnitz.core.values import _analyze_schema

"""
Linear Operators for the DBSP Virtual Machine.

These operators satisfy the identity L(A + B) = L(A) + L(B). 
They are stateless transformations acting on Z-Set Delta Registers.

Zero-Copy Refactor: These operators now use Raw Accessors to pipe data 
between batches without instantiating Python PayloadRow objects.
"""


class MapOutputAccessor(RowAccessor):
    """
    A reusable, mutable RowAccessor used for zero-allocation MAP operations.
    
    It mimics the PayloadRow API (append_*) to satisfy ScalarFunctions,
    but overwrites internal pre-allocated lists instead of appending.
    """
    _immutable_fields_ = ["schema", "pk_index", "_has_nullable"]

    def __init__(self, schema):
        self.schema = schema
        self.pk_index = schema.pk_index
        
        n, has_u128, has_string, has_nullable = _analyze_schema(schema)
        
        # Pre-allocate fixed-size buffers for payload columns
        self._lo = [r_int64(0)] * n
        self._hi = [r_uint64(0)] * n if has_u128 else None
        self._strs = [""] * n if has_string else None
        
        self._has_nullable = has_nullable
        self._null_word = r_uint64(0)
        self._curr = 0

    def clear(self):
        """Reset the cursor and null bitset for a new record."""
        self._curr = 0
        self._null_word = r_uint64(0)

    def _payload_idx(self, col_idx):
        if col_idx < self.pk_index:
            return col_idx
        return col_idx - 1

    # --- ScalarFunction 'Writer' API ---

    def append_int(self, val):
        self._lo[self._curr] = val
        self._curr += 1

    def append_float(self, val_f64):
        self._lo[self._curr] = float2longlong(val_f64)
        self._curr += 1

    def append_string(self, val_str):
        self._lo[self._curr] = r_int64(0)
        if self._strs:
            self._strs[self._curr] = val_str
        self._curr += 1

    def append_u128(self, lo_u64, hi_u64):
        self._lo[self._curr] = r_int64(intmask(lo_u64))
        if self._hi:
            self._hi[self._curr] = hi_u64
        self._curr += 1

    def append_null(self, payload_col_idx):
        # Note: payload_col_idx is provided by evaluate_map logic
        self._lo[self._curr] = r_int64(0)
        self._null_word |= (r_uint64(1) << payload_col_idx)
        self._curr += 1

    # --- RowAccessor 'Reader' API ---

    def is_null(self, col_idx):
        if not self._has_nullable:
            return False
        return bool(self._null_word & (r_uint64(1) << self._payload_idx(col_idx)))

    def get_int(self, col_idx):
        return r_uint64(self._lo[self._payload_idx(col_idx)])

    def get_float(self, col_idx):
        return longlong2float(self._lo[self._payload_idx(col_idx)])

    def get_u128(self, col_idx):
        p_idx = self._payload_idx(col_idx)
        lo = r_uint64(self._lo[p_idx])
        hi = self._hi[p_idx] if self._hi else r_uint64(0)
        from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
        return (r_uint128(hi) << 64) | r_uint128(lo)

    def get_str_struct(self, col_idx):
        s = self._strs[self._payload_idx(col_idx)] if self._strs else ""
        prefix = rffi.cast(lltype.Signed, strings.compute_prefix(s))
        return (
            len(s),
            prefix,
            lltype.nullptr(rffi.CCHARP.TO),
            lltype.nullptr(rffi.CCHARP.TO),
            s,
        )


@jit.unroll_safe
def op_filter(reg_in, reg_out, func):
    """
    Retains only records where the predicate returns True.
    Weights and Payloads are preserved via zero-copy buffer transfer.
    """
    in_batch = reg_in.batch
    out_batch = reg_out.batch

    n = in_batch.length()
    for i in range(n):
        pk = in_batch.get_pk(i)
        weight = in_batch.get_weight(i)
        
        # Zero-copy: Obtain a transient raw accessor to the batch memory
        accessor = in_batch.get_accessor(i)
        
        if func.evaluate_predicate(accessor):
            # Zero-copy: Copy bytes directly from in_batch to out_batch
            out_batch.append_from_accessor(pk, weight, accessor)


@jit.unroll_safe
def op_map(reg_in, reg_out, func):
    """
    Applies a transformation to every row. 
    Zero-allocation: reuses a single MapOutputAccessor for the entire batch.
    """
    in_batch = reg_in.batch
    out_batch = reg_out.batch

    out_schema = reg_out.vm_schema.table_schema
    out_acc = MapOutputAccessor(out_schema)

    n = in_batch.length()
    for i in range(n):
        pk = in_batch.get_pk(i)
        weight = in_batch.get_weight(i)
        
        in_acc = in_batch.get_accessor(i)
        
        out_acc.clear()
        func.evaluate_map(in_acc, out_acc)
        
        # Zero-copy: the serialization kernel reads from the virtual out_acc
        out_batch.append_from_accessor(pk, weight, out_acc)


def op_negate(reg_in, reg_out):
    """
    DBSP negation operator: flips the sign of every weight.
    """
    in_batch = reg_in.batch
    out_batch = reg_out.batch

    n = in_batch.length()
    for i in range(n):
        pk = in_batch.get_pk(i)
        weight = in_batch.get_weight(i)
        accessor = in_batch.get_accessor(i)

        neg_weight = r_int64(-intmask(weight))
        out_batch.append_from_accessor(pk, neg_weight, accessor)


def op_union(reg_in_a, reg_in_b, reg_out):
    """
    Algebraic addition of two Z-Set streams.
    """
    out_batch = reg_out.batch

    batch_a = reg_in_a.batch
    n_a = batch_a.length()
    for i in range(n_a):
        out_batch.append_from_accessor(
            batch_a.get_pk(i), batch_a.get_weight(i), batch_a.get_accessor(i)
        )

    if reg_in_b is not None:
        batch_b = reg_in_b.batch
        n_b = batch_b.length()
        for i in range(n_b):
            out_batch.append_from_accessor(
                batch_b.get_pk(i), batch_b.get_weight(i), batch_b.get_accessor(i)
            )


def op_delay(reg_in, reg_out):
    """
    The z^-1 operator. Moves input delta to output for the next tick.
    """
    in_batch = reg_in.batch
    out_batch = reg_out.batch

    n = in_batch.length()
    for i in range(n):
        out_batch.append_from_accessor(
            in_batch.get_pk(i), in_batch.get_weight(i), in_batch.get_accessor(i)
        )


def op_integrate(reg_in, target_table):
    """
    Terminal sink operator. Flushes batch to storage.
    """
    in_batch = reg_in.batch
    if in_batch.length() > 0:
        target_table.ingest_batch(in_batch)
