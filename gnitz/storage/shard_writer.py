# gnitz/storage/shard_writer.py
#
# Shard file writing via Rust FFI. All paths are fully columnar (SoA).

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import errors
from gnitz.storage import engine_ffi
from gnitz.storage.mmap_posix import AT_FDCWD


def _write_shard_via_ffi(dirfd, name, table_id, count, region_list,
                         durable=True):
    """Write a shard file via Rust FFI (gnitz_write_shard)."""
    num_regions = len(region_list)
    ptrs_arr = lltype.malloc(rffi.VOIDPP.TO, num_regions, flavor="raw")
    sizes_arr = lltype.malloc(rffi.UINTP.TO, num_regions, flavor="raw")
    try:
        i = 0
        while i < num_regions:
            buf_ptr, sz = region_list[i]
            ptrs_arr[i] = rffi.cast(rffi.VOIDP, buf_ptr)
            sizes_arr[i] = rffi.cast(rffi.UINT, sz)
            i += 1
        with rffi.scoped_str2charp(name) as c_name:
            rc = engine_ffi._write_shard(
                rffi.cast(rffi.INT, dirfd),
                c_name,
                rffi.cast(rffi.UINT, table_id),
                rffi.cast(rffi.UINT, count),
                ptrs_arr,
                sizes_arr,
                rffi.cast(rffi.UINT, num_regions),
                rffi.cast(rffi.INT, 1 if durable else 0),
            )
        rc_int = intmask(rc)
        if rc_int < 0:
            raise errors.StorageError(
                "write_shard failed (error %d)" % rc_int
            )
    finally:
        lltype.free(ptrs_arr, flavor="raw")
        lltype.free(sizes_arr, flavor="raw")


def write_shard_at(dirfd, basename, table_id, count, region_list, pk_lo_ptr,
                   pk_hi_ptr, durable=True):
    """Write a shard file using openat/renameat via Rust FFI."""
    _write_shard_via_ffi(dirfd, basename, table_id, count, region_list,
                         durable=durable)


def write_batch_to_shard(batch, dirfd, basename, table_id, durable=True):
    """Write a consolidated ArenaZSetBatch directly to a shard file.
    Returns True if a file was written, False if the batch was empty."""
    count = batch.length()
    if count == 0:
        return False

    schema = batch._schema
    num_cols = len(schema.columns)
    num_regions = 3 + 1 + (num_cols - 1) + 1
    regions = newlist_hint(num_regions)
    regions.append((batch.pk_lo_buf.base_ptr, batch.pk_lo_buf.offset))
    regions.append((batch.pk_hi_buf.base_ptr, batch.pk_hi_buf.offset))
    regions.append((batch.weight_buf.base_ptr, batch.weight_buf.offset))
    regions.append((batch.null_buf.base_ptr, batch.null_buf.offset))
    for ci in range(num_cols):
        if ci != schema.pk_index:
            regions.append((batch.col_bufs[ci].base_ptr, batch.col_bufs[ci].offset))
    regions.append((batch.blob_arena.base_ptr, batch.blob_arena.offset))

    write_shard_at(
        dirfd, basename, table_id, count, regions,
        batch.pk_lo_buf.base_ptr, batch.pk_hi_buf.base_ptr,
        durable=durable,
    )
    return True


def write_batch_to_file(batch, path, table_id, durable=True):
    """Write a batch to an absolute file path. Convenience for tests."""
    count = batch.length()
    if count == 0:
        return False

    schema = batch._schema
    num_cols = len(schema.columns)
    num_regions = 3 + 1 + (num_cols - 1) + 1
    regions = newlist_hint(num_regions)
    regions.append((batch.pk_lo_buf.base_ptr, batch.pk_lo_buf.offset))
    regions.append((batch.pk_hi_buf.base_ptr, batch.pk_hi_buf.offset))
    regions.append((batch.weight_buf.base_ptr, batch.weight_buf.offset))
    regions.append((batch.null_buf.base_ptr, batch.null_buf.offset))
    for ci in range(num_cols):
        if ci != schema.pk_index:
            regions.append((batch.col_bufs[ci].base_ptr, batch.col_bufs[ci].offset))
    regions.append((batch.blob_arena.base_ptr, batch.blob_arena.offset))

    _write_shard_via_ffi(AT_FDCWD, path, table_id, count, regions,
                         durable=durable)
    return True
