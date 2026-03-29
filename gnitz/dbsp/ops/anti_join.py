# gnitz/dbsp/ops/anti_join.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import intmask

from gnitz.core.batch import ArenaZSetBatch

"""
Anti-Join and Semi-Join Operators for the DBSP algebra.

Anti-join: antijoin(A, B) = A - semijoin(A, distinct(B))
    Emits rows from A whose join key has NO match in B.

Semi-join: semijoin(A, B)
    Emits rows from A whose join key DOES have a match in B.

Both operators output using the LEFT schema only (no column merge,
no CompositeAccessor needed).

The incremental expansion of antijoin uses these operators:
    Δ(antijoin(A, B)) = anti_dt(ΔA, I(D))
                       - semi_dt(ΔD, I(A))   [output from I(A)]
                       - semi_dd(ΔA, ΔD)
where D = distinct(B).
"""


def op_anti_join_delta_trace(delta_batch, trace_cursor, out_writer, left_schema):
    """
    Delta-Trace Anti-Join: emit ΔA rows with NO positive-weight match in I(B).
    Single FFI call into Rust.
    """
    from gnitz.storage import engine_ffi

    schema = delta_batch._schema
    schema_buf = engine_ffi.pack_schema(schema)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_anti_join_dt(
            delta_batch._handle,
            trace_cursor._handle,
            rffi.cast(rffi.VOIDP, schema_buf),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_anti_join_dt failed: %d" % intmask(rc))

        result_handle = out_result[0]
        result_batch = ArenaZSetBatch._wrap_handle(schema, result_handle, True, True)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        out_writer.mark_consolidated(True)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(schema_buf, flavor="raw")


def op_anti_join_delta_delta(batch_a, batch_b, out_writer, left_schema):
    """
    Delta-Delta Anti-Join: emit ΔA rows whose keys have NO positive-weight
    match in ΔB. Single FFI call into Rust.
    """
    from gnitz.storage import engine_ffi

    schema = batch_a._schema
    schema_buf = engine_ffi.pack_schema(schema)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_anti_join_dd(
            batch_a._handle,
            batch_b._handle,
            rffi.cast(rffi.VOIDP, schema_buf),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_anti_join_dd failed: %d" % intmask(rc))

        result_handle = out_result[0]
        result_batch = ArenaZSetBatch._wrap_handle(schema, result_handle, True, True)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        out_writer.mark_consolidated(True)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(schema_buf, flavor="raw")


def op_semi_join_delta_trace(delta_batch, trace_cursor, out_writer, left_schema):
    """
    Delta-Trace Semi-Join: emit ΔA rows whose key HAS a positive-weight
    match in I(B). Single FFI call into Rust.
    """
    from gnitz.storage import engine_ffi

    schema = delta_batch._schema
    schema_buf = engine_ffi.pack_schema(schema)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_semi_join_dt(
            delta_batch._handle,
            trace_cursor._handle,
            rffi.cast(rffi.VOIDP, schema_buf),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_semi_join_dt failed: %d" % intmask(rc))

        result_handle = out_result[0]
        is_sorted = intmask(engine_ffi._batch_is_sorted(result_handle)) != 0
        is_consolidated = intmask(engine_ffi._batch_is_consolidated(result_handle)) != 0
        result_batch = ArenaZSetBatch._wrap_handle(schema, result_handle, is_sorted, is_consolidated)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        if is_consolidated:
            out_writer.mark_consolidated(True)
        else:
            out_writer.mark_sorted(True)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(schema_buf, flavor="raw")


def op_semi_join_delta_delta(batch_a, batch_b, out_writer, left_schema):
    """
    Delta-Delta Semi-Join: emit ΔA rows whose keys DO have a positive-weight
    match in ΔB. Single FFI call into Rust.
    """
    from gnitz.storage import engine_ffi

    schema = batch_a._schema
    schema_buf = engine_ffi.pack_schema(schema)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_semi_join_dd(
            batch_a._handle,
            batch_b._handle,
            rffi.cast(rffi.VOIDP, schema_buf),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_semi_join_dd failed: %d" % intmask(rc))

        result_handle = out_result[0]
        result_batch = ArenaZSetBatch._wrap_handle(schema, result_handle, True, True)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        out_writer.mark_consolidated(True)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(schema_buf, flavor="raw")
