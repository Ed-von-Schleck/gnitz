# gnitz/dbsp/ops/linear.py

from rpython.rlib.rarithmetic import intmask
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core.batch import ArenaZSetBatch


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
