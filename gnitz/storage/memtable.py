# gnitz/storage/memtable.py
#
# Shared helpers for packing ArenaZSetBatch data into FFI region arrays.

from rpython.rtyper.lltypesystem import rffi, lltype

ACCUMULATOR_THRESHOLD = 64


def _pack_batch_regions(batch, schema):
    """Pack an ArenaZSetBatch into FFI region arrays.

    Returns (ptrs, sizes, count, rpb) where ptrs and sizes are malloc'd
    arrays that MUST be freed by the caller.
    """
    num_cols = len(schema.columns)
    pk_index = schema.pk_index
    num_payload_cols = num_cols - 1
    regions_per_batch = 4 + num_payload_cols + 1
    count = batch.length()

    ptrs = lltype.malloc(rffi.VOIDPP.TO, regions_per_batch, flavor="raw")
    sizes = lltype.malloc(rffi.UINTP.TO, regions_per_batch, flavor="raw")

    idx = 0
    ptrs[idx] = rffi.cast(rffi.VOIDP, batch.pk_lo_buf.base_ptr)
    sizes[idx] = rffi.cast(rffi.UINT, count * 8)
    idx += 1
    ptrs[idx] = rffi.cast(rffi.VOIDP, batch.pk_hi_buf.base_ptr)
    sizes[idx] = rffi.cast(rffi.UINT, count * 8)
    idx += 1
    ptrs[idx] = rffi.cast(rffi.VOIDP, batch.weight_buf.base_ptr)
    sizes[idx] = rffi.cast(rffi.UINT, count * 8)
    idx += 1
    ptrs[idx] = rffi.cast(rffi.VOIDP, batch.null_buf.base_ptr)
    sizes[idx] = rffi.cast(rffi.UINT, count * 8)
    idx += 1
    for ci in range(num_cols):
        if ci == pk_index:
            continue
        col_sz = count * batch.col_strides[ci]
        ptrs[idx] = rffi.cast(rffi.VOIDP, batch.col_bufs[ci].base_ptr)
        sizes[idx] = rffi.cast(rffi.UINT, col_sz)
        idx += 1
    ptrs[idx] = rffi.cast(rffi.VOIDP, batch.blob_arena.base_ptr)
    sizes[idx] = rffi.cast(rffi.UINT, batch.blob_arena.offset)

    return ptrs, sizes, count, regions_per_batch
