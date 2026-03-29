# gnitz/dbsp/ops/linear.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import intmask
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core.batch import ArenaZSetBatch

"""
Linear Operators for the DBSP algebra.

These operators satisfy the identity L(A + B) = L(A) + L(B).
They are stateless transformations acting on Z-Set batches.
Each function now accepts a BatchWriter for the output register.
"""


# ---------------------------------------------------------------------------
# Linear operator implementations
# ---------------------------------------------------------------------------


def op_filter(in_batch, out_writer, func):
    """
    Retains only records for which the predicate returns True.
    Delegates to Rust via single FFI call.

    in_batch:   ArenaZSetBatch
    out_writer: BatchWriter  (strictly write-only destination)
    func:       ScalarFunction  (evaluate_predicate)
    """
    from gnitz.storage import engine_ffi

    schema = in_batch._schema
    schema_buf = engine_ffi.pack_schema(schema)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        func_handle = func.get_func_handle() if func is not None else lltype.nullptr(rffi.VOIDP.TO)
        rc = engine_ffi._op_filter(
            in_batch._handle,
            func_handle,
            rffi.cast(rffi.VOIDP, schema_buf),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_filter failed: %d" % intmask(rc))
        result_handle = out_result[0]
        is_sorted = intmask(engine_ffi._batch_is_sorted(result_handle)) != 0
        is_consolidated = intmask(engine_ffi._batch_is_consolidated(result_handle)) != 0
        result_batch = ArenaZSetBatch._wrap_handle(schema, result_handle, is_sorted, is_consolidated)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        if is_consolidated:
            out_writer.mark_consolidated(True)
        else:
            out_writer.mark_sorted(is_sorted)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(schema_buf, flavor="raw")


def op_map(in_batch, out_writer, func, out_schema, reindex_col=-1):
    """
    Applies a transformation to every row.
    Delegates to Rust via single FFI call.

    in_batch:   ArenaZSetBatch
    out_writer: BatchWriter  (strictly write-only destination)
    func:       ScalarFunction  (evaluate_map)
    out_schema: TableSchema for the output batch
    reindex_col: when >= 0, use this input column's value as the new PK
    """
    from gnitz.storage import engine_ffi

    in_schema = in_batch._schema
    in_schema_buf = engine_ffi.pack_schema(in_schema)
    out_schema_buf = engine_ffi.pack_schema(out_schema)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        func_handle = func.get_func_handle() if func is not None else lltype.nullptr(rffi.VOIDP.TO)
        rc = engine_ffi._op_map(
            in_batch._handle,
            func_handle,
            rffi.cast(rffi.VOIDP, in_schema_buf),
            rffi.cast(rffi.VOIDP, out_schema_buf),
            rffi.cast(rffi.INT, reindex_col),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_map failed: %d" % intmask(rc))
        result_handle = out_result[0]
        is_sorted = intmask(engine_ffi._batch_is_sorted(result_handle)) != 0
        result_batch = ArenaZSetBatch._wrap_handle(out_schema, result_handle, is_sorted, False)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        if reindex_col >= 0:
            out_writer.mark_sorted(False)
        else:
            out_writer.mark_sorted(is_sorted)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(in_schema_buf, flavor="raw")
        lltype.free(out_schema_buf, flavor="raw")


def op_negate(in_batch, out_writer):
    """
    DBSP negation: flips the sign of every weight.
    Delegates to Rust via single FFI call.
    """
    from gnitz.storage import engine_ffi

    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_negate(
            in_batch._handle,
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_negate failed: %d" % intmask(rc))
        result_handle = out_result[0]
        schema = in_batch._schema
        is_sorted = intmask(engine_ffi._batch_is_sorted(result_handle)) != 0
        is_consolidated = intmask(engine_ffi._batch_is_consolidated(result_handle)) != 0
        result_batch = ArenaZSetBatch._wrap_handle(schema, result_handle, is_sorted, is_consolidated)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        if is_consolidated:
            out_writer.mark_consolidated(True)
        else:
            out_writer.mark_sorted(is_sorted)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")


def op_union(batch_a, batch_b, out_writer):
    """
    Algebraic addition of two Z-Set streams.
    batch_b may be None or empty, in which case this is an identity copy of batch_a.
    Delegates to Rust via single FFI call.
    """
    from gnitz.storage import engine_ffi

    schema = batch_a._schema
    schema_buf = engine_ffi.pack_schema(schema)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        batch_b_handle = batch_b._handle if (batch_b is not None and batch_b.length() > 0) else lltype.nullptr(rffi.VOIDP.TO)
        rc = engine_ffi._op_union(
            batch_a._handle,
            batch_b_handle,
            rffi.cast(rffi.VOIDP, schema_buf),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_union failed: %d" % intmask(rc))
        result_handle = out_result[0]
        is_sorted = intmask(engine_ffi._batch_is_sorted(result_handle)) != 0
        is_consolidated = intmask(engine_ffi._batch_is_consolidated(result_handle)) != 0
        result_batch = ArenaZSetBatch._wrap_handle(schema, result_handle, is_sorted, is_consolidated)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        if is_consolidated:
            out_writer.mark_consolidated(True)
        else:
            out_writer.mark_sorted(is_sorted)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(schema_buf, flavor="raw")


def op_delay(in_batch, out_writer):
    """
    The z^{-1} operator: forwards the current tick's batch to the next tick's
    input register.
    """
    out_writer.append_batch(in_batch)
    if in_batch._consolidated:
        out_writer.mark_consolidated(True)
    else:
        out_writer.mark_sorted(in_batch._sorted)


def op_integrate(in_batch, target_table):
    """
    Terminal sink: flushes a batch into persistent storage.
    Note: integration goes into a ZSetStore, not a transient Delta register.
    """
    if in_batch.length() > 0 and target_table is not None:
        target_table.ingest_batch(in_batch)
