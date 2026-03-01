# gnitz/dbsp/ops/linear.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import strings, types
from gnitz.core.batch import ArenaZSetBatch
from gnitz.core.comparator import RowAccessor
from gnitz.core.types import TableSchema
from gnitz.core.values import _analyze_schema

"""
Linear Operators for the DBSP algebra.

These operators satisfy the identity L(A + B) = L(A) + L(B).
They are stateless transformations acting on Z-Set batches.

Each function accepts explicit, concrete arguments — batches, tables, and
schemas — rather than register objects.  The VM interpreter is responsible
for extracting those values from its register file before calling here.

Zero-Copy: operators pipe data between batches without instantiating
PayloadRow objects.
"""


class MapOutputAccessor(RowAccessor):
    """
    A reusable, mutable RowAccessor used for zero-allocation MAP operations.

    It mimics the PayloadRow append_* API to satisfy ScalarFunctions, but
    writes into pre-allocated fixed-size lists instead of appending to
    grow-able ones.  After a call to clear(), the same object can be reused
    for the next row.
    """

    _immutable_fields_ = ["schema", "pk_index", "_has_nullable"]

    def __init__(self, schema):
        self.schema = schema
        self.pk_index = schema.pk_index

        n, has_u128, has_string, has_nullable = _analyze_schema(schema)

        self._lo = [r_int64(0)] * n
        self._hi = [r_uint64(0)] * n if has_u128 else None
        self._strs = [""] * n if has_string else None

        self._has_nullable = has_nullable
        self._null_word = r_uint64(0)
        self._curr = 0

    def clear(self):
        """Reset the write cursor and null bitset for a new record."""
        self._curr = 0
        self._null_word = r_uint64(0)

    def _payload_idx(self, col_idx):
        if col_idx < self.pk_index:
            return col_idx
        return col_idx - 1

    # ------------------------------------------------------------------
    # ScalarFunction 'Writer' API
    # ------------------------------------------------------------------

    def append_int(self, val):
        self._lo[self._curr] = val
        self._curr += 1

    def append_float(self, val_f64):
        self._lo[self._curr] = float2longlong(val_f64)
        self._curr += 1

    def append_string(self, val_str):
        self._lo[self._curr] = r_int64(0)
        if self._strs is not None:
            self._strs[self._curr] = val_str
        self._curr += 1

    def append_u128(self, lo_u64, hi_u64):
        self._lo[self._curr] = r_int64(intmask(lo_u64))
        if self._hi is not None:
            self._hi[self._curr] = hi_u64
        self._curr += 1

    def append_null(self, payload_col_idx):
        self._lo[self._curr] = r_int64(0)
        self._null_word |= r_uint64(1) << payload_col_idx
        self._curr += 1

    # ------------------------------------------------------------------
    # RowAccessor 'Reader' API
    # ------------------------------------------------------------------

    def is_null(self, col_idx):
        if not self._has_nullable:
            return False
        return bool(self._null_word & (r_uint64(1) << self._payload_idx(col_idx)))

    def get_int(self, col_idx):
        return r_uint64(self._lo[self._payload_idx(col_idx)])

    def get_int_signed(self, col_idx):
        return rffi.cast(rffi.LONGLONG, r_uint64(self._lo[self._payload_idx(col_idx)]))

    def get_float(self, col_idx):
        return longlong2float(self._lo[self._payload_idx(col_idx)])

    def get_u128(self, col_idx):
        p_idx = self._payload_idx(col_idx)
        lo = r_uint64(self._lo[p_idx])
        hi = self._hi[p_idx] if self._hi is not None else r_uint64(0)
        from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

        return (r_uint128(hi) << 64) | r_uint128(lo)

    def get_str_struct(self, col_idx):
        s = self._strs[self._payload_idx(col_idx)] if self._strs is not None else ""
        prefix = rffi.cast(lltype.Signed, strings.compute_prefix(s))
        return (
            len(s),
            prefix,
            lltype.nullptr(rffi.CCHARP.TO),
            lltype.nullptr(rffi.CCHARP.TO),
            s,
        )

    def get_col_ptr(self, col_idx):
        return lltype.nullptr(rffi.CCHARP.TO)


# ---------------------------------------------------------------------------
# Linear operator implementations
# ---------------------------------------------------------------------------


@jit.unroll_safe
def op_filter(in_batch, out_batch, func):
    """
    Retains only records for which the predicate returns True.
    Weights and payloads are forwarded via zero-copy accessor transfer.

    in_batch:  ArenaZSetBatch
    out_batch: ArenaZSetBatch  (caller is responsible for clearing beforehand)
    func:      ScalarFunction  (evaluate_predicate)
    """
    n = in_batch.length()
    for i in range(n):
        accessor = in_batch.get_accessor(i)
        if func is not None:
            if func.evaluate_predicate(accessor):
                out_batch.append_from_accessor(
                    in_batch.get_pk(i), in_batch.get_weight(i), accessor
                )


@jit.unroll_safe
def op_map(in_batch, out_batch, func, out_schema):
    """
    Applies a transformation to every row.
    Zero-allocation: a single MapOutputAccessor is reused across the whole batch.

    in_batch:   ArenaZSetBatch
    out_batch:  ArenaZSetBatch  (caller is responsible for clearing beforehand)
    func:       ScalarFunction  (evaluate_map)
    out_schema: TableSchema for the output batch
    """
    out_acc = MapOutputAccessor(out_schema)

    n = in_batch.length()
    for i in range(n):
        in_acc = in_batch.get_accessor(i)
        out_acc.clear()
        if func is not None:
            func.evaluate_map(in_acc, out_acc)
            out_batch.append_from_accessor(
                in_batch.get_pk(i), in_batch.get_weight(i), out_acc
            )


def op_negate(in_batch, out_batch):
    """
    DBSP negation: flips the sign of every weight.

    in_batch:  ArenaZSetBatch
    out_batch: ArenaZSetBatch  (caller is responsible for clearing beforehand)
    """
    n = in_batch.length()
    for i in range(n):
        neg_weight = r_int64(-intmask(in_batch.get_weight(i)))
        out_batch.append_from_accessor(
            in_batch.get_pk(i), neg_weight, in_batch.get_accessor(i)
        )


def op_union(batch_a, batch_b, out_batch):
    """
    Algebraic addition of two Z-Set streams.
    batch_b may be None, in which case this is an identity copy of batch_a.

    batch_a:   ArenaZSetBatch
    batch_b:   ArenaZSetBatch or None
    out_batch: ArenaZSetBatch  (caller is responsible for clearing beforehand)
    """
    n_a = batch_a.length()
    for i in range(n_a):
        out_batch.append_from_accessor(
            batch_a.get_pk(i), batch_a.get_weight(i), batch_a.get_accessor(i)
        )

    if batch_b is not None:
        n_b = batch_b.length()
        for i in range(n_b):
            out_batch.append_from_accessor(
                batch_b.get_pk(i), batch_b.get_weight(i), batch_b.get_accessor(i)
            )


def op_delay(in_batch, out_batch):
    """
    The z^{-1} operator: forwards the current tick's batch to the next tick's
    input register.  The VM interpreter copies the contents rather than
    transferring ownership so that in_batch remains valid within the tick.

    in_batch:  ArenaZSetBatch
    out_batch: ArenaZSetBatch  (caller is responsible for clearing beforehand)
    """
    n = in_batch.length()
    for i in range(n):
        out_batch.append_from_accessor(
            in_batch.get_pk(i), in_batch.get_weight(i), in_batch.get_accessor(i)
        )


def op_integrate(in_batch, target_table):
    """
    Terminal sink: flushes a batch into persistent storage.

    in_batch:     ArenaZSetBatch
    target_table: AbstractTable
    """
    if in_batch.length() > 0:
        target_table.ingest_batch(in_batch)
