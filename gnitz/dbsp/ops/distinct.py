# gnitz/dbsp/ops/distinct.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import intmask

from gnitz.core.batch import ArenaZSetBatch

"""
Distinct Operator for the DBSP algebra.

Converts multiset semantics to set semantics: a record's output weight is 1
if its net accumulated weight is positive, -1 if negative, and 0 otherwise.
In practice GnitzDB only accumulates non-negative weights from the ingestion
layer, so the -1 case does not arise for base tables — but it can arise in
intermediate circuit nodes, and the implementation handles it correctly.

The algebraic identity being implemented is:

    δ_out = set_step(I(δ_in)) - set_step(z^{-1}(I(δ_in)))
          = set_step(history + δ_in) - set_step(history)

where set_step(w) = sign(w) clamped to {-1, 0, 1}.

State (the running integral I(δ_in)) is maintained by the caller in
history_table, which is an AbstractTable acting as the operator's trace.
"""


def op_distinct(delta_batch, hist_cursor, hist_table, out_writer):
    """
    DBSP Distinct: converts multiset deltas into set-membership deltas.

    delta_batch: ArenaZSetBatch  — consolidated input delta for this tick
    hist_cursor: AbstractCursor  — seekable cursor over the persistent history
                                   (I(δ_in) snapped at the start of this tick)
    hist_table:  ZSetStore       — persistent trace holding the running sum
                                   I(δ_in) accumulated over all past ticks
    out_writer:  BatchWriter     — strictly write-only destination
    """
    from gnitz.storage import engine_ffi

    schema = delta_batch._schema
    schema_buf = engine_ffi.pack_schema(schema)
    out_result = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    out_consolidated = lltype.malloc(rffi.VOIDPP.TO, 1, flavor="raw")
    try:
        rc = engine_ffi._op_distinct(
            delta_batch._handle,
            hist_cursor._handle,
            rffi.cast(rffi.VOIDP, schema_buf),
            out_result,
            out_consolidated,
        )
        if intmask(rc) < 0:
            raise Exception("gnitz_op_distinct failed: %d" % intmask(rc))

        result_handle = out_result[0]
        consolidated_handle = out_consolidated[0]

        result_batch = ArenaZSetBatch._wrap_handle(schema, result_handle, True, True)
        consolidated_batch = ArenaZSetBatch._wrap_handle(schema, consolidated_handle, True, True)

        if result_batch.length() > 0:
            out_writer.append_batch(result_batch)
            out_writer.mark_consolidated(True)

        hist_table.ingest_batch(consolidated_batch)

        result_batch.free()
        consolidated_batch.free()
    finally:
        lltype.free(out_result, flavor="raw")
        lltype.free(out_consolidated, flavor="raw")
        lltype.free(schema_buf, flavor="raw")
