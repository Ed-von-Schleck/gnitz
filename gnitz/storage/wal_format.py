# gnitz/storage/wal_format.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, r_uint32
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.storage import errors, mmap_posix
from gnitz.core import checksum, types, strings as string_logic, values as db_values
from gnitz.core.row_logic import make_payload_row

WAL_BLOCK_HEADER_SIZE = 32


def align_8(val):
    return (val + 7) & ~7


class WALBlobAllocator(string_logic.BlobAllocator):
    """
    Allocator strategy for WAL block serialization.
    Writes long string payloads into the trailing overflow area of the WAL block.
    """

    def __init__(self, buf, cursor_ref):
        self.buf = buf
        # cursor_ref is a List[int] containing the current byte offset in the buffer
        self.cursor_ref = cursor_ref

    def allocate(self, string_data):
        length = len(string_data)
        start_pos = self.cursor_ref[0]

        # Copy string data into the overflow area of the buffer
        for i in range(length):
            self.buf[start_pos + i] = string_data[i]

        # Advance the shared cursor
        self.cursor_ref[0] += length

        # Return the absolute offset (Header + Body Offset)
        return r_uint64(WAL_BLOCK_HEADER_SIZE + start_pos)


class WALRecord(object):
    """
    In-memory representation of a WAL log entry.
    """

    # NOTE: "component_data" is intentionally NOT listed as "component_data[*]".
    # The [*] suffix would mark the List[TaggedValue] as read-only (r flag) in
    # RPython's listdef system. Because all List[TaggedValue] instances share a
    # single global listdef, one r-flagged list permanently poisons that listdef,
    # causing ListChangeUnallowed at every .append() site in the entire program.
    _immutable_fields_ = ["pk_lo", "pk_hi", "weight"]

    def __init__(self, pk_lo, pk_hi, weight, component_data):
        self.pk_lo = r_uint64(pk_lo)
        self.pk_hi = r_uint64(pk_hi)
        self.weight = weight
        # component_data is List[db_values.TaggedValue]
        self.component_data = component_data

    @staticmethod
    def from_key(key, weight, component_data):
        k = r_uint128(key)
        return WALRecord(r_uint64(k), r_uint64(k >> 64), weight, component_data)

    def get_key(self):
        return (r_uint128(self.pk_hi) << 64) | r_uint128(self.pk_lo)


class DecodedBlock(object):
    _immutable_fields_ = ["lsn", "tid", "records[*]"]

    def __init__(self, lsn, tid, records):
        self.lsn = lsn
        self.tid = tid
        self.records = records


def write_wal_block(fd, lsn, table_id, records, schema):
    """
    Serializes a batch of Z-Set deltas into the WAL binary format.
    Uses centralized German String serialization with WALBlobAllocator.
    """
    is_u128 = schema.get_pk_column().field_type == types.TYPE_U128
    key_size = 16 if is_u128 else 8
    stride = schema.memtable_stride
    num_cols = len(schema.columns)

    # 1. Calculate required body size (AoS + Long String Overflows)
    total_body_size = 0
    rec_idx = 0
    num_records = len(records)
    while rec_idx < num_records:
        rec = records[rec_idx]
        total_body_size += key_size + 8 + stride

        arg_idx = 0
        i = 0
        while i < num_cols:
            if i != schema.pk_index:
                val_obj = rec.component_data[arg_idx]
                arg_idx += 1
                if schema.columns[i].field_type == types.TYPE_STRING:
                    assert val_obj.tag == db_values.TAG_STRING
                    s_len = len(val_obj.str_val)
                    if s_len > string_logic.SHORT_STRING_THRESHOLD:
                        total_body_size += s_len
            i += 1
        total_body_size = align_8(total_body_size)
        rec_idx += 1

    # 2. Allocate and pack the body
    buf = lltype.malloc(rffi.CCHARP.TO, total_body_size, flavor="raw")
    try:
        for k in range(total_body_size):
            buf[k] = "\x00"

        curr_off = 0
        # Reference used by the WALBlobAllocator to advance the overflow cursor
        cursor_ref = [0]

        rec_idx = 0
        while rec_idx < num_records:
            rec = records[rec_idx]

            # Use local cursor for fixed AoS part
            record_base = curr_off

            # Pack Primary Key
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, record_base))[0] = r_uint64(
                rec.pk_lo
            )
            if is_u128:
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(buf, record_base + 8))[
                    0
                ] = r_uint64(rec.pk_hi)
            record_base += key_size

            # Pack Weight
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(buf, record_base))[0] = rffi.cast(
                rffi.LONGLONG, rec.weight
            )
            record_base += 8

            # Payload start
            p_start = record_base
            # The next available overflow slot starts after all AoS records in this batch,
            # but for simplicity, we keep them intermingled per-record as in previous spec.
            # Advance overflow cursor to start after current AoS record fixed stride.
            cursor_ref[0] = record_base + stride

            # Pack Columnar Payload
            arg_idx = 0
            i = 0
            allocator = WALBlobAllocator(buf, cursor_ref)
            while i < num_cols:
                if i != schema.pk_index:
                    val_obj = rec.component_data[arg_idx]
                    arg_idx += 1
                    target = rffi.ptradd(buf, p_start + schema.get_column_offset(i))
                    col_type = schema.columns[i].field_type

                    if col_type == types.TYPE_STRING:
                        assert val_obj.tag == db_values.TAG_STRING
                        string_logic.pack_and_write_blob(
                            target, val_obj.str_val, allocator
                        )

                    elif col_type == types.TYPE_F64:
                        assert val_obj.tag == db_values.TAG_FLOAT
                        rffi.cast(rffi.DOUBLEP, target)[0] = rffi.cast(
                            rffi.DOUBLE, val_obj.f64
                        )

                    elif col_type == types.TYPE_U128:
                        assert val_obj.tag == db_values.TAG_INT
                        uv = r_uint128(val_obj.i64)
                        rffi.cast(rffi.ULONGLONGP, target)[0] = r_uint64(uv)
                        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = r_uint64(
                            uv >> 64
                        )

                    else:
                        assert val_obj.tag == db_values.TAG_INT
                        rffi.cast(rffi.ULONGLONGP, target)[0] = r_uint64(val_obj.i64)
                i += 1

            # Align next record to 8 bytes relative to the block start
            curr_off = align_8(cursor_ref[0])
            rec_idx += 1

        # 3. Write Header with Checksum
        h_buf = lltype.malloc(rffi.CCHARP.TO, WAL_BLOCK_HEADER_SIZE, flavor="raw")
        try:
            for k in range(WAL_BLOCK_HEADER_SIZE):
                h_buf[k] = "\x00"
            rffi.cast(rffi.ULONGLONGP, h_buf)[0] = r_uint64(lsn)
            rffi.cast(rffi.UINTP, rffi.ptradd(h_buf, 8))[0] = rffi.cast(
                rffi.UINT, table_id
            )
            rffi.cast(rffi.UINTP, rffi.ptradd(h_buf, 12))[0] = rffi.cast(
                rffi.UINT, num_records
            )
            rffi.cast(rffi.UINTP, rffi.ptradd(h_buf, 16))[0] = rffi.cast(
                rffi.UINT, WAL_BLOCK_HEADER_SIZE + total_body_size
            )
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(h_buf, 24))[
                0
            ] = checksum.compute_checksum(buf, total_body_size)

            if (
                mmap_posix.write_c(fd, h_buf, rffi.cast(rffi.SIZE_T, WAL_BLOCK_HEADER_SIZE))
                < WAL_BLOCK_HEADER_SIZE
            ):
                raise errors.StorageError("WAL header write failed")
        finally:
            lltype.free(h_buf, flavor="raw")

        if (
            mmap_posix.write_c(fd, buf, rffi.cast(rffi.SIZE_T, total_body_size))
            < total_body_size
        ):
            raise errors.StorageError("WAL body write failed")
    finally:
        lltype.free(buf, flavor="raw")


def decode_wal_block(block_ptr, block_len, schema):
    """
    Deserializes a WAL block into DecodedBlock containing TaggedValues.
    """
    lsn = rffi.cast(rffi.ULONGLONGP, block_ptr)[0]
    tid = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, rffi.ptradd(block_ptr, 8))[0])
    cnt = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, rffi.ptradd(block_ptr, 12))[0])
    stored_cs = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(block_ptr, 24))[0]

    body_ptr = rffi.ptradd(block_ptr, WAL_BLOCK_HEADER_SIZE)
    body_len = block_len - WAL_BLOCK_HEADER_SIZE
    if checksum.compute_checksum(body_ptr, body_len) != stored_cs:
        raise errors.CorruptShardError("WAL Checksum Mismatch")

    is_u128 = schema.get_pk_column().field_type == types.TYPE_U128
    key_size = 16 if is_u128 else 8
    stride = schema.memtable_stride
    num_cols = len(schema.columns)
    num_payload_fields = num_cols - 1

    records = [None] * cnt
    curr_ptr = body_ptr

    rec_idx = 0
    while rec_idx < cnt:
        # Keep track of the furthest byte used by this record (fixed + blobs)
        current_rec_max_ptr = rffi.ptradd(curr_ptr, key_size + 8 + stride)

        pk_lo = rffi.cast(rffi.ULONGLONGP, curr_ptr)[0]
        pk_hi = r_uint64(0)
        if is_u128:
            pk_hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(curr_ptr, 8))[0]
        curr_ptr = rffi.ptradd(curr_ptr, key_size)

        weight = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, curr_ptr)[0])
        curr_ptr = rffi.ptradd(curr_ptr, 8)

        p_ptr = curr_ptr

        # Use make_payload_row instead of [None] * n.
        # [None] * n produces an mr (must-not-resize) list. Because all
        # List[TaggedValue] instances share a single global listdef, one mr
        # construction permanently prevents .append() on any such list.
        field_values = make_payload_row(num_payload_fields)
        val_idx = 0
        i = 0
        while i < num_cols:
            if i != schema.pk_index:
                col_def = schema.columns[i]
                fptr = rffi.ptradd(p_ptr, schema.get_column_offset(i))
                val = None

                if col_def.field_type == types.TYPE_STRING:
                    s_val = string_logic.unpack_string(fptr, block_ptr)
                    val = db_values.TaggedValue.make_string(s_val)

                    u32_ptr = rffi.cast(rffi.UINTP, fptr)
                    length = rffi.cast(lltype.Signed, u32_ptr[0])
                    if length > string_logic.SHORT_STRING_THRESHOLD:
                        u64_payload_ptr = rffi.cast(
                            rffi.ULONGLONGP, rffi.ptradd(fptr, 8)
                        )
                        offset = rffi.cast(lltype.Signed, u64_payload_ptr[0])
                        blob_end_ptr = rffi.ptradd(block_ptr, offset + length)

                        if rffi.cast(lltype.Signed, blob_end_ptr) > rffi.cast(
                            lltype.Signed, current_rec_max_ptr
                        ):
                            current_rec_max_ptr = blob_end_ptr

                elif col_def.field_type == types.TYPE_F64:
                    f_val = float(rffi.cast(rffi.DOUBLEP, fptr)[0])
                    val = db_values.TaggedValue.make_float(f_val)

                elif col_def.field_type == types.TYPE_U128:
                    l = rffi.cast(rffi.ULONGLONGP, fptr)[0]
                    val = db_values.TaggedValue.make_int(l)

                else:
                    i_val = rffi.cast(rffi.ULONGLONGP, fptr)[0]
                    val = db_values.TaggedValue.make_int(i_val)

                field_values.append(val)
                val_idx += 1
            i += 1

        consumed_bytes = rffi.cast(lltype.Signed, current_rec_max_ptr) - rffi.cast(
            lltype.Signed, body_ptr
        )
        aligned_bytes = align_8(consumed_bytes)
        curr_ptr = rffi.ptradd(body_ptr, aligned_bytes)

        records[rec_idx] = WALRecord(pk_lo, pk_hi, weight, field_values)
        rec_idx += 1

    return DecodedBlock(lsn, tid, records)
