# gnitz/server/master.py
#
# Thin FFI wrapper around the Rust MasterDispatcher.
# The Rust implementation handles all IPC, exchange relay, index routing,
# and unique validation internally.

from rpython.rlib.rarithmetic import intmask
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.storage import engine_ffi


def _raise_master_error(handle):
    """Raise StorageError with the last master FFI error message."""
    out_len = lltype.malloc(rffi.UINTP.TO, 1, flavor="raw")
    try:
        err_ptr = engine_ffi._master_last_error(handle, out_len)
        elen = intmask(out_len[0])
        if elen > 0 and err_ptr:
            msg = rffi.charpsize2str(err_ptr, elen)
        else:
            msg = "unknown master error"
    finally:
        lltype.free(out_len, flavor="raw")
    raise errors.StorageError(msg)


class MasterDispatcher(object):
    """Thin FFI wrapper around the Rust MasterDispatcher."""

    def __init__(self, num_workers, worker_pids, assignment,
                 dag_handle, catalog_handle, sal, w2m_regions, m2w_efds, w2m_efds):
        # Marshal worker_pids array
        pids_arr = lltype.malloc(rffi.INTP.TO, num_workers, flavor="raw")
        for w in range(num_workers):
            pids_arr[w] = rffi.cast(rffi.INT, worker_pids[w])

        # Marshal w2m region pointers and sizes
        w2m_ptrs_arr = lltype.malloc(rffi.CCHARPP.TO, num_workers, flavor="raw")
        w2m_sizes_arr = lltype.malloc(rffi.ULONGLONGP.TO, num_workers, flavor="raw")
        for w in range(num_workers):
            w2m_ptrs_arr[w] = w2m_regions[w].ptr
            w2m_sizes_arr[w] = rffi.cast(rffi.ULONGLONG, w2m_regions[w].size)

        # Marshal eventfd arrays
        m2w_arr = lltype.malloc(rffi.INTP.TO, num_workers, flavor="raw")
        w2m_arr = lltype.malloc(rffi.INTP.TO, num_workers, flavor="raw")
        for w in range(num_workers):
            m2w_arr[w] = rffi.cast(rffi.INT, m2w_efds[w])
            w2m_arr[w] = rffi.cast(rffi.INT, w2m_efds[w])

        self._handle = engine_ffi._master_create(
            catalog_handle,
            rffi.cast(rffi.UINT, num_workers),
            pids_arr,
            sal.ptr,
            rffi.cast(rffi.INT, sal.fd),
            rffi.cast(rffi.ULONGLONG, sal.mmap_size),
            w2m_ptrs_arr, w2m_sizes_arr,
            m2w_arr, w2m_arr,
        )

        lltype.free(pids_arr, flavor="raw")
        lltype.free(w2m_ptrs_arr, flavor="raw")
        lltype.free(w2m_sizes_arr, flavor="raw")
        lltype.free(m2w_arr, flavor="raw")
        lltype.free(w2m_arr, flavor="raw")

        if not self._handle:
            raise errors.StorageError("gnitz_master_create returned NULL")

        self._catalog_handle = catalog_handle
        self.num_workers = num_workers

    def _collect_acks(self):
        rc = engine_ffi._master_collect_acks(self._handle)
        if intmask(rc) < 0:
            _raise_master_error(self._handle)

    def fan_out_backfill(self, view_id, source_id, source_schema):
        rc = engine_ffi._master_fan_out_backfill(
            self._handle,
            rffi.cast(rffi.LONGLONG, view_id),
            rffi.cast(rffi.LONGLONG, source_id))
        if intmask(rc) < 0:
            _raise_master_error(self._handle)
