# gnitz/dbsp/ops/reduce.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import intmask

from gnitz.core.batch import ArenaZSetBatch

"""
Non-linear Reduce Operator for the DBSP algebra.

Implements incremental GROUP BY + aggregation.
Formula: δ_out = Agg(history + δ_in) - Agg(history).

All algorithmic logic is in Rust (ops.rs). This module is an FFI wrapper.
"""


def _pack_agg_descs(agg_funcs):
    """Pack list of AggregateFunction into a raw buffer of AggDescriptor structs."""
    from gnitz.storage import engine_ffi

    n = len(agg_funcs)
    buf_size = n * engine_ffi.AGG_DESC_SIZE
    buf = lltype.malloc(rffi.CCHARP.TO, buf_size, flavor="raw")
    i = 0
    while i < buf_size:
        buf[i] = "\x00"
        i += 1
    for i in range(n):
        af = agg_funcs[i]
        base = i * engine_ffi.AGG_DESC_SIZE
        rffi.cast(rffi.UINTP, rffi.ptradd(buf, base))[0] = rffi.cast(
            rffi.UINT, af.col_idx
        )
        buf[base + 4] = chr(af.agg_op)
        buf[base + 5] = chr(af.col_type_code)
    return buf, n


def _pack_u32_array(int_list):
    """Pack a list of ints into a raw uint32_t array."""
    n = len(int_list)
    arr = lltype.malloc(rffi.UINTP.TO, n, flavor="raw")
    for i in range(n):
        arr[i] = rffi.cast(rffi.UINT, int_list[i])
    return arr, n


def op_reduce(
    delta_in,
    input_schema,
    trace_in_cursor,
    trace_out_cursor,
    out_writer,
    group_by_cols,
    agg_funcs,
    output_schema,
    trace_in_group_idx=None,
    agg_value_idx=None,
    finalize_prog=None,
    fin_out_writer=None,
):
    """
    Incremental DBSP REDUCE: δ_out = Agg(history + δ_in) - Agg(history).
    Delegates entirely to Rust via single FFI call.
    """
    from gnitz.storage import engine_ffi

    if agg_funcs is None:
        return

    in_schema_buf = engine_ffi.pack_schema(input_schema)
    out_schema_buf = engine_ffi.pack_schema(output_schema)
    agg_buf, num_aggs = _pack_agg_descs(agg_funcs)
    gcols_arr, num_gcols = _pack_u32_array(group_by_cols)

    # Cursor handles
    null_handle = lltype.nullptr(rffi.VOIDP.TO)
    trace_in_handle = trace_in_cursor._handle if trace_in_cursor is not None else null_handle
    trace_out_handle = trace_out_cursor._handle

    # AVI
    avi_handle = null_handle
    avi_for_max = 0
    avi_agg_type = 0
    avi_gcols = lltype.nullptr(rffi.UINTP.TO)
    avi_num_gcols = 0
    avi_schema_buf = lltype.nullptr(rffi.CCHARP.TO)
    if agg_value_idx is not None:
        avi_handle = agg_value_idx.table.create_cursor()._handle
        avi_for_max = 1 if agg_value_idx.for_max else 0
        avi_agg_type = agg_value_idx.agg_col_type.code
        avi_gcols, avi_num_gcols = _pack_u32_array(agg_value_idx.group_by_cols)
        avi_schema_buf = engine_ffi.pack_schema(agg_value_idx.input_schema)

    # GI
    gi_handle = null_handle
    gi_col_idx = 0
    gi_col_type = 0
    if trace_in_group_idx is not None:
        gi_handle = trace_in_group_idx.table.create_cursor()._handle
        gi_col_idx = trace_in_group_idx.col_idx
        gi_col_type = trace_in_group_idx.col_type.code

    # Finalize
    fin_prog_handle = null_handle
    fin_schema_buf = lltype.nullptr(rffi.CCHARP.TO)
    if finalize_prog is not None and fin_out_writer is not None:
        fin_prog_handle = finalize_prog._rust_handle
        fin_schema_buf = engine_ffi.pack_schema(fin_out_writer._batch._schema)

    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    out_finalized = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_reduce(
            delta_in._handle,
            trace_in_handle,
            trace_out_handle,
            rffi.cast(rffi.VOIDP, in_schema_buf),
            rffi.cast(rffi.VOIDP, out_schema_buf),
            gcols_arr,
            rffi.cast(rffi.UINT, num_gcols),
            rffi.cast(rffi.VOIDP, agg_buf),
            rffi.cast(rffi.UINT, num_aggs),
            avi_handle,
            rffi.cast(rffi.INT, avi_for_max),
            rffi.cast(rffi.UCHAR, avi_agg_type),
            avi_gcols,
            rffi.cast(rffi.UINT, avi_num_gcols),
            rffi.cast(rffi.VOIDP, avi_schema_buf) if avi_schema_buf else null_handle,
            gi_handle,
            rffi.cast(rffi.UINT, gi_col_idx),
            rffi.cast(rffi.UCHAR, gi_col_type),
            fin_prog_handle,
            rffi.cast(rffi.VOIDP, fin_schema_buf) if fin_schema_buf else null_handle,
            out_result,
            out_finalized,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_reduce failed: %d" % intmask(rc))

        # Wrap raw output
        result_handle = out_result[0]
        is_sorted = intmask(engine_ffi._batch_is_sorted(result_handle)) != 0
        is_cons = intmask(engine_ffi._batch_is_consolidated(result_handle)) != 0
        result_batch = ArenaZSetBatch._wrap_handle(
            output_schema, result_handle, is_sorted, is_cons
        )
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        if is_cons:
            out_writer.mark_consolidated(True)
        else:
            out_writer.mark_sorted(is_sorted)
        result_batch.free()

        # Wrap finalized output
        fin_handle = out_finalized[0]
        if fin_handle and fin_out_writer is not None:
            fin_schema = fin_out_writer._batch._schema
            fin_batch = ArenaZSetBatch._wrap_handle(fin_schema, fin_handle, is_sorted, False)
            if fin_batch.length() > 0:
                fin_out_writer.append_batch(fin_batch)
            fin_out_writer.mark_sorted(is_sorted)
            fin_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(out_finalized, flavor="raw")
        lltype.free(in_schema_buf, flavor="raw")
        lltype.free(out_schema_buf, flavor="raw")
        lltype.free(agg_buf, flavor="raw")
        lltype.free(gcols_arr, flavor="raw")
        if avi_gcols:
            lltype.free(avi_gcols, flavor="raw")
        if avi_schema_buf:
            lltype.free(avi_schema_buf, flavor="raw")
        if fin_schema_buf:
            lltype.free(fin_schema_buf, flavor="raw")


def op_gather_reduce(
    partial_batch,
    partial_schema,
    trace_out_cursor,
    trace_out_table,
    out_writer,
    agg_funcs,
):
    """
    Gather-reduce: merge partial aggregate deltas from workers.
    Delegates entirely to Rust via single FFI call.
    """
    from gnitz.storage import engine_ffi

    if agg_funcs is None:
        return

    schema_buf = engine_ffi.pack_schema(partial_schema)
    agg_buf, num_aggs = _pack_agg_descs(agg_funcs)

    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_gather_reduce(
            partial_batch._handle,
            trace_out_cursor._handle,
            rffi.cast(rffi.VOIDP, schema_buf),
            rffi.cast(rffi.VOIDP, agg_buf),
            rffi.cast(rffi.UINT, num_aggs),
            out_result,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_gather_reduce failed: %d" % intmask(rc))

        result_handle = out_result[0]
        is_sorted = intmask(engine_ffi._batch_is_sorted(result_handle)) != 0
        result_batch = ArenaZSetBatch._wrap_handle(
            partial_schema, result_handle, is_sorted, False
        )
        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
        out_writer.mark_sorted(True)
        result_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(schema_buf, flavor="raw")
        lltype.free(agg_buf, flavor="raw")
