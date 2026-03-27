# gnitz/storage/memtable.py
#
# Shared helpers for packing ArenaZSetBatch data into FFI region arrays.

from rpython.rlib.rarithmetic import intmask
from rpython.rtyper.lltypesystem import rffi, lltype

ACCUMULATOR_THRESHOLD = 64


def _pack_batch_regions(batch, schema):
    """Pack an ArenaZSetBatch into FFI region arrays.

    Returns (ptrs, sizes, count, rpb) where ptrs and sizes are malloc'd
    arrays that MUST be freed by the caller.
    """
    from gnitz.storage import engine_ffi

    num_regions = intmask(engine_ffi._batch_num_regions(batch._handle))
    count = batch.length()

    ptrs = lltype.malloc(rffi.VOIDPP.TO, num_regions, flavor="raw")
    sizes = lltype.malloc(rffi.UINTP.TO, num_regions, flavor="raw")

    for i in range(num_regions):
        ptrs[i] = rffi.cast(
            rffi.VOIDP, engine_ffi._batch_region_ptr(batch._handle, rffi.cast(rffi.UINT, i))
        )
        sizes[i] = rffi.cast(
            rffi.UINT, engine_ffi._batch_region_size(batch._handle, rffi.cast(rffi.UINT, i))
        )

    return ptrs, sizes, count, num_regions
