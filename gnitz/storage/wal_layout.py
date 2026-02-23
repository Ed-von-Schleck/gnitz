# gnitz/storage/wal_layout.py

from rpython.rlib.rarithmetic import r_uint64, r_uint32, intmask, r_int64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import jit

WAL_OFF_LSN = 0
WAL_OFF_TID = 8
WAL_OFF_COUNT = 12
WAL_OFF_SIZE = 16
WAL_OFF_VERSION = 20
WAL_OFF_CHECKSUM = 24
WAL_BLOCK_HEADER_SIZE = 32

_REC_PK_OFFSET = 0
_REC_WEIGHT_OFFSET = 16
_REC_NULL_OFFSET = 24
_REC_PAYLOAD_BASE = 32

WAL_FORMAT_VERSION_LEGACY = 0
WAL_FORMAT_VERSION_CURRENT = 1


class WALBlockHeaderView(object):
    _immutable_fields_ = ["ptr"]

    def __init__(self, ptr):
        self.ptr = ptr

    def get_lsn(self):
        return rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, WAL_OFF_LSN))[0]

    def set_lsn(self, lsn):
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, WAL_OFF_LSN))[0] = lsn

    def get_table_id(self):
        p = rffi.cast(rffi.UINTP, rffi.ptradd(self.ptr, WAL_OFF_TID))
        return intmask(p[0])

    def set_table_id(self, tid):
        p = rffi.cast(rffi.UINTP, rffi.ptradd(self.ptr, WAL_OFF_TID))
        p[0] = rffi.cast(rffi.UINT, tid)

    def get_entry_count(self):
        p = rffi.cast(rffi.UINTP, rffi.ptradd(self.ptr, WAL_OFF_COUNT))
        return intmask(p[0])

    def set_entry_count(self, count):
        p = rffi.cast(rffi.UINTP, rffi.ptradd(self.ptr, WAL_OFF_COUNT))
        p[0] = rffi.cast(rffi.UINT, count)

    def get_total_size(self):
        p = rffi.cast(rffi.UINTP, rffi.ptradd(self.ptr, WAL_OFF_SIZE))
        return intmask(p[0])

    def set_total_size(self, size):
        p = rffi.cast(rffi.UINTP, rffi.ptradd(self.ptr, WAL_OFF_SIZE))
        p[0] = rffi.cast(rffi.UINT, size)

    def get_format_version(self):
        p = rffi.cast(rffi.UINTP, rffi.ptradd(self.ptr, WAL_OFF_VERSION))
        return intmask(p[0])

    def set_format_version(self, v):
        p = rffi.cast(rffi.UINTP, rffi.ptradd(self.ptr, WAL_OFF_VERSION))
        p[0] = rffi.cast(rffi.UINT, v)

    def get_checksum(self):
        return rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, WAL_OFF_CHECKSUM))[0]

    def set_checksum(self, cs):
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, WAL_OFF_CHECKSUM))[0] = cs


# Utility raw writers used by wal_format
def write_u128(buf, offset, val):
    p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, offset))
    p[0] = rffi.cast(rffi.ULONGLONG, r_uint64(val))
    p[1] = rffi.cast(rffi.ULONGLONG, r_uint64(val >> 64))


def read_u128(buf, offset):
    p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, offset))
    return (r_uint128(p[1]) << 64) | r_uint128(p[0])


def write_u64(buf, offset, val):
    p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, offset))
    p[0] = rffi.cast(rffi.ULONGLONG, val)


def read_u64(buf, offset):
    p = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, offset))
    return r_uint64(p[0])


def write_i64(buf, offset, val):
    p = rffi.cast(rffi.LONGLONGP, rffi.ptradd(buf, offset))
    p[0] = rffi.cast(rffi.LONGLONG, val)
