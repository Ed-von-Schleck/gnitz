# gnitz/dbsp/ops/linear.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128
from rpython.rlib.longlong2float import float2longlong

from gnitz.core.batch import RowBuilder
from gnitz.core import types as core_types, xxh, strings as core_strings
from rpython.rtyper.lltypesystem import rffi, lltype

"""
Linear Operators for the DBSP algebra.

These operators satisfy the identity L(A + B) = L(A) + L(B).
They are stateless transformations acting on Z-Set batches.
Each function now accepts a BatchWriter for the output register.
"""


def _mix64(v):
    """Murmur3 64-bit finalizer. Must match reduce.py._mix64 exactly."""
    v = r_uint64(v)
    v ^= v >> 33
    v = r_uint64(v * r_uint64(0xFF51AFD7ED558CCD))
    v ^= v >> 33
    v = r_uint64(v * r_uint64(0xC4CEB9FE1A85EC53))
    v ^= v >> 33
    return v


def _promote_col_to_u128(accessor, col_idx, schema):
    """Promote a single column value to a U128 key for reindexing."""
    t = schema.columns[col_idx].field_type.code
    if t == core_types.TYPE_U128.code:
        return accessor.get_u128(col_idx)
    elif t == core_types.TYPE_U64.code or t == core_types.TYPE_I64.code:
        return r_uint128(r_uint64(accessor.get_int(col_idx)))
    elif t == core_types.TYPE_STRING.code:
        length, _, struct_ptr, heap_ptr, py_string = accessor.get_str_struct(col_idx)
        if length == 0:
            h = r_uint64(0)
        elif py_string is not None:
            h = xxh.compute_checksum_bytes(py_string)
        else:
            if length <= core_strings.SHORT_STRING_THRESHOLD:
                target_ptr = rffi.ptradd(struct_ptr, 4)
            else:
                u64_p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(struct_ptr, 8))
                heap_off = rffi.cast(lltype.Signed, u64_p[0])
                target_ptr = rffi.ptradd(heap_ptr, heap_off)
            h = xxh.compute_checksum(target_ptr, length)
        h_hi = _mix64(h)
        return (r_uint128(h_hi) << 64) | r_uint128(h)
    elif t == core_types.TYPE_F64.code or t == core_types.TYPE_F32.code:
        return r_uint128(r_uint64(float2longlong(accessor.get_float(col_idx))))
    else:
        return r_uint128(r_uint64(accessor.get_int(col_idx)))


# ---------------------------------------------------------------------------
# Linear operator implementations
# ---------------------------------------------------------------------------


@jit.unroll_safe
def op_filter(in_batch, out_writer, func):
    """
    Retains only records for which the predicate returns True.
    Uses contiguous-range bulk copy to bypass per-row accessor dispatch.

    in_batch:   ArenaZSetBatch
    out_writer: BatchWriter  (strictly write-only destination)
    func:       ScalarFunction  (evaluate_predicate)
    """
    n = in_batch.length()
    range_start = -1
    for i in range(n):
        accessor = in_batch.get_accessor(i)
        if func is not None and func.evaluate_predicate(accessor):
            if range_start < 0:
                range_start = i
        else:
            if range_start >= 0:
                out_writer.append_batch(in_batch, range_start, i)
                range_start = -1
    if range_start >= 0:
        out_writer.append_batch(in_batch, range_start, n)
    out_writer.mark_sorted(in_batch._sorted)


@jit.unroll_safe
def op_map(in_batch, out_writer, func, out_schema, reindex_col=-1):
    """
    Applies a transformation to every row.
    Uses RowBuilder to validate row construction and commit to the batch.

    in_batch:   ArenaZSetBatch
    out_writer: BatchWriter  (strictly write-only destination)
    func:       ScalarFunction  (evaluate_map)
    out_schema: TableSchema for the output batch
    reindex_col: when >= 0, use this input column's value as the new PK
    """
    builder = RowBuilder(out_schema, out_writer._batch)

    n = in_batch.length()
    for i in range(n):
        in_acc = in_batch.get_accessor(i)
        if func is not None:
            func.evaluate_map(in_acc, builder)
            if reindex_col >= 0:
                new_pk = _promote_col_to_u128(in_acc, reindex_col, in_batch._schema)
                builder.commit_row(new_pk, in_batch.get_weight(i))
            else:
                builder.commit_row(in_batch.get_pk(i), in_batch.get_weight(i))
    if reindex_col >= 0:
        out_writer.mark_sorted(False)
    else:
        out_writer.mark_sorted(in_batch._sorted)


def op_negate(in_batch, out_writer):
    """
    DBSP negation: flips the sign of every weight.
    Uses bulk column copy with negated weight write.
    """
    out_writer.append_batch_negated(in_batch)
    out_writer.mark_sorted(in_batch._sorted)


def op_union(batch_a, batch_b, out_writer):
    """
    Algebraic addition of two Z-Set streams.
    batch_b may be None, in which case this is an identity copy of batch_a.
    """
    out_writer.append_batch(batch_a)
    if batch_b is not None:
        out_writer.append_batch(batch_b)


def op_delay(in_batch, out_writer):
    """
    The z^{-1} operator: forwards the current tick's batch to the next tick's
    input register.
    """
    out_writer.append_batch(in_batch)
    out_writer.mark_sorted(in_batch._sorted)


def op_integrate(in_batch, target_table):
    """
    Terminal sink: flushes a batch into persistent storage.
    Note: integration goes into a ZSetStore, not a transient Delta register.
    """
    if in_batch.length() > 0 and target_table is not None:
        target_table.ingest_batch(in_batch)
