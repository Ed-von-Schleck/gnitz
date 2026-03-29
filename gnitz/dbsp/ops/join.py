# gnitz/dbsp/ops/join.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import intmask

from gnitz.core.batch import ArenaZSetBatch

"""
Bilinear Join Operators for the DBSP algebra.

Implements the incremental bilinear expansion:
    Δ(A ⋈ B) = ΔA ⋈ I(B) + I(A) ⋈ ΔB

The VM compiles joins into one or two instructions depending on whether
the operand is a persistent trace (JoinDeltaTrace) or another in-flight
delta (JoinDeltaDelta).
"""

# ---------------------------------------------------------------------------
# Join operator implementations
# ---------------------------------------------------------------------------


def op_join_delta_trace(delta_batch, trace_cursor, out_writer, d_schema, t_schema):
    """
    Delta-Trace Join: ΔA ⋈ I(B). Single FFI call into Rust.
    """
    from gnitz.storage import engine_ffi

    out_schema = out_writer.get_schema()
    left_buf = engine_ffi.pack_schema(d_schema)
    right_buf = engine_ffi.pack_schema(t_schema)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_join_dt(
            delta_batch._handle,
            trace_cursor._handle,
            rffi.cast(rffi.VOIDP, left_buf),
            rffi.cast(rffi.VOIDP, right_buf),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_join_dt failed: %d" % intmask(rc))

        result_handle = out_result[0]
        result_batch = ArenaZSetBatch._wrap_handle(out_schema, result_handle, True, False)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        out_writer.mark_sorted(True)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(left_buf, flavor="raw")
        lltype.free(right_buf, flavor="raw")


def op_join_delta_trace_outer(delta_batch, trace_cursor, out_writer, d_schema, t_schema):
    """
    Delta-Trace Left Outer Join: ΔA LEFT⋈ I(B). Single FFI call into Rust.
    """
    from gnitz.storage import engine_ffi

    out_schema = out_writer.get_schema()
    left_buf = engine_ffi.pack_schema(d_schema)
    right_buf = engine_ffi.pack_schema(t_schema)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_join_dt_outer(
            delta_batch._handle,
            trace_cursor._handle,
            rffi.cast(rffi.VOIDP, left_buf),
            rffi.cast(rffi.VOIDP, right_buf),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_join_dt_outer failed: %d" % intmask(rc))

        result_handle = out_result[0]
        result_batch = ArenaZSetBatch._wrap_handle(out_schema, result_handle, True, False)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        out_writer.mark_sorted(True)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(left_buf, flavor="raw")
        lltype.free(right_buf, flavor="raw")


def op_join_delta_delta(batch_a, batch_b, out_writer, schema_a, schema_b):
    """
    Delta-Delta Join: ΔA ⋈ ΔB. Single FFI call into Rust.
    """
    from gnitz.storage import engine_ffi

    out_schema = out_writer.get_schema()
    left_buf = engine_ffi.pack_schema(schema_a)
    right_buf = engine_ffi.pack_schema(schema_b)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_join_dd(
            batch_a._handle,
            batch_b._handle,
            rffi.cast(rffi.VOIDP, left_buf),
            rffi.cast(rffi.VOIDP, right_buf),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_join_dd failed: %d" % intmask(rc))

        result_handle = out_result[0]
        result_batch = ArenaZSetBatch._wrap_handle(out_schema, result_handle, True, False)
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        out_writer.mark_sorted(True)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(left_buf, flavor="raw")
        lltype.free(right_buf, flavor="raw")
