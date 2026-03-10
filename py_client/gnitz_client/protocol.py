"""Wire format constants and header pack/unpack for IPC v2."""

import struct

# "GNITZ2PC" in little-endian hex
MAGIC_V2 = 0x474E49545A325043

HEADER_SIZE = 96
ALIGNMENT = 64

# Byte offsets within the 96-byte header
OFF_MAGIC = 0
OFF_STATUS = 8
OFF_ERR_LEN = 12
OFF_TARGET_ID = 16
OFF_CLIENT_ID = 24
OFF_SCHEMA_COUNT = 32
OFF_SCHEMA_BLOB_SZ = 40
OFF_DATA_COUNT = 48
OFF_DATA_BLOB_SZ = 56
OFF_DATA_PK_INDEX = 64
OFF_FLAGS = 72
# Bytes 80-95: reserved

STATUS_OK = 0
STATUS_ERROR = 1

FLAG_ALLOCATE_ID = 1

IPC_STRING_STRIDE = 8
IPC_NULL_STRING_OFFSET = 0xFFFFFFFF

META_FLAG_NULLABLE = 1
META_FLAG_IS_PK = 2

# struct format: magic(Q) status(I) err_len(I) target_id(Q) client_id(Q)
#   schema_count(Q) schema_blob_sz(Q) data_count(Q) data_blob_sz(Q)
#   data_pk_index(Q) flags(Q)
# Total packed = 8+4+4+8+8+8+8+8+8+8+8 = 80, then 16 reserved = 96
_HEADER_FMT = "<QIIQQQQQQQQ"
_HEADER_PACK_SIZE = struct.calcsize(_HEADER_FMT)  # 80
_RESERVED_SIZE = HEADER_SIZE - _HEADER_PACK_SIZE   # 16


def align_up(val: int, align: int) -> int:
    return (val + align - 1) & ~(align - 1)


def pack_header(
    magic: int = MAGIC_V2,
    status: int = STATUS_OK,
    err_len: int = 0,
    target_id: int = 0,
    client_id: int = 0,
    schema_count: int = 0,
    schema_blob_sz: int = 0,
    data_count: int = 0,
    data_blob_sz: int = 0,
    data_pk_index: int = 0,
    flags: int = 0,
) -> bytes:
    packed = struct.pack(
        _HEADER_FMT,
        magic, status, err_len, target_id, client_id,
        schema_count, schema_blob_sz, data_count, data_blob_sz,
        data_pk_index, flags,
    )
    return packed + b'\x00' * _RESERVED_SIZE


def unpack_header(buf: bytes) -> dict:
    vals = struct.unpack_from(_HEADER_FMT, buf, 0)
    return {
        "magic": vals[0],
        "status": vals[1],
        "err_len": vals[2],
        "target_id": vals[3],
        "client_id": vals[4],
        "schema_count": vals[5],
        "schema_blob_sz": vals[6],
        "data_count": vals[7],
        "data_blob_sz": vals[8],
        "data_pk_index": vals[9],
        "flags": vals[10],
    }
