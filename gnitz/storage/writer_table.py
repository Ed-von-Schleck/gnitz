import os
from rpython.rlib import rposix
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.jit import unrolling_iterable
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long
from gnitz.storage import layout, mmap_posix
from gnitz.core import checksum
from gnitz.core import types, strings as string_logic

FIELD_INDICES = unrolling_iterable(range(64))

def _align_64(val): return (val + 63) & ~63
def _copy_memory(dest_ptr, src_ptr, size):
    for i in range(size): dest_ptr[i] = src_ptr[i]
def _compare_memory(p1, p2, size):
    for i in range(size):
        if p1[i] != p2[i]: return False
    return True

class RawBuffer(object):
    def __init__(self, item_size, initial_capacity=4096):
        self.item_size = item_size
        self.count = 0
        self.capacity = initial_capacity
        self.ptr = lltype.malloc(rffi.CCHARP.TO, self.capacity * self.item_size, flavor='raw')

    def ensure_capacity(self, needed_slots):
        new_count = self.count + needed_slots
        if new_count > self.capacity:
            new_cap = max(self.capacity * 2, new_count)
            new_ptr = lltype.malloc(rffi.CCHARP.TO, new_cap * self.item_size, flavor='raw')
            if self.ptr:
                _copy_memory(new_ptr, self.ptr, self.count * self.item_size)
                lltype.free(self.ptr, flavor='raw')
            self.ptr = new_ptr
            self.capacity = new_cap

    def append_u128(self, value):
        self.ensure_capacity(1)
        target = rffi.cast(rffi.CCHARP, rffi.ptradd(self.ptr, self.count * 16))
        mask = r_uint128(0xFFFFFFFFFFFFFFFF)
        rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, value & mask)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(rffi.ULONGLONG, (value >> 64) & mask)
        self.count += 1

    def append_u64(self, value):
        self.ensure_capacity(1)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(self.ptr, self.count * 8))[0] = rffi.cast(rffi.ULONGLONG, value)
        self.count += 1

    def append_i64(self, value):
        self.ensure_capacity(1)
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(self.ptr, self.count * 8))[0] = rffi.cast(rffi.LONGLONG, value)
        self.count += 1

    def append_bytes(self, source_ptr, length_in_items):
        self.ensure_capacity(length_in_items)
        if source_ptr:
            _copy_memory(rffi.ptradd(self.ptr, self.count * self.item_size), source_ptr, length_in_items * self.item_size)
        else:
            dest = rffi.ptradd(self.ptr, self.count * self.item_size)
            for i in range(length_in_items * self.item_size): dest[i] = '\x00'
        self.count += length_in_items

    def free(self):
        if self.ptr:
            lltype.free(self.ptr, flavor='raw')
            self.ptr = lltype.nullptr(rffi.CCHARP.TO)

class TableShardWriter(object):
    def __init__(self, schema, table_id=0):
        self.schema = schema
        self.table_id = table_id
        self.count = 0
        pk_col = schema.get_pk_column()
        self.pk_buf = RawBuffer(pk_col.field_type.size)
        self.w_buf = RawBuffer(8)
        self.col_bufs = []
        for i in range(len(schema.columns)):
            if i == schema.pk_index: 
                self.col_bufs.append(None)
            else: 
                self.col_bufs.append(RawBuffer(schema.columns[i].field_type.size))
        self.b_buf = RawBuffer(1)
        self.blob_cache = {}

    def _get_or_append_blob(self, src_ptr, length):
        if length <= 0 or not src_ptr: return 0
        h = checksum.compute_checksum(src_ptr, length)
        cache_key = (h, length)
        if cache_key in self.blob_cache:
            existing_offset = self.blob_cache[cache_key]
            existing_ptr = rffi.ptradd(self.b_buf.ptr, existing_offset)
            if _compare_memory(src_ptr, existing_ptr, length): return existing_offset
        new_offset = self.b_buf.count
        self.b_buf.append_bytes(src_ptr, length)
        self.blob_cache[cache_key] = new_offset
        return new_offset

    def add_row(self, key, weight, packed_row_ptr, source_heap_ptr):
        if weight == 0: return
        self.count += 1
        if self.schema.get_pk_column().field_type.size == 16: 
            self.pk_buf.append_u128(key)
        else: 
            self.pk_buf.append_u64(rffi.cast(rffi.ULONGLONG, key))
        self.w_buf.append_i64(weight)
        
        for i in FIELD_INDICES:
            if i >= len(self.schema.columns): break
            if i == self.schema.pk_index: continue
            
            col_def = self.schema.columns[i]
            col_off = self.schema.get_column_offset(i)
            buf = self.col_bufs[i]
            
            val_ptr = lltype.nullptr(rffi.CCHARP.TO)
            if packed_row_ptr:
                val_ptr = rffi.ptradd(packed_row_ptr, col_off)

            if col_def.field_type == types.TYPE_STRING:
                if val_ptr:
                    length = rffi.cast(lltype.Signed, rffi.cast(rffi.UINTP, val_ptr)[0])
                    if length > string_logic.SHORT_STRING_THRESHOLD:
                        u64_view = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(val_ptr, 8))
                        old_off = rffi.cast(lltype.Signed, u64_view[0])
                        # Fix: Check for NULL source_heap_ptr when dealing with long strings
                        if not source_heap_ptr:
                            raise errors.StorageError("Long string relocation requires source heap")
                        new_off = self._get_or_append_blob(rffi.ptradd(source_heap_ptr, old_off), length)
                        tmp_str = lltype.malloc(rffi.CCHARP.TO, 16, flavor='raw')
                        try:
                            _copy_memory(tmp_str, val_ptr, 16)
                            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(tmp_str, 8))[0] = rffi.cast(rffi.ULONGLONG, new_off)
                            buf.append_bytes(tmp_str, 1)
                        finally: lltype.free(tmp_str, flavor='raw')
                    else: buf.append_bytes(val_ptr, 1)
                else: buf.append_bytes(None, 1)
            else:
                buf.append_bytes(val_ptr, 1)

    def add_row_values(self, pk, *args):
        stride = self.schema.memtable_stride
        tmp = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
        blob_tmp = lltype.nullptr(rffi.CCHARP.TO)
        try:
            for i in range(stride): tmp[i] = '\x00'
            arg_idx = 0
            for i in range(len(self.schema.columns)):
                if i == self.schema.pk_index: continue
                if arg_idx >= len(args): break
                
                val = args[arg_idx]
                arg_idx += 1
                off = self.schema.get_column_offset(i)
                ftype = self.schema.columns[i].field_type
                target = rffi.ptradd(tmp, off)
                
                if ftype == types.TYPE_STRING:
                    s_val = str(val)
                    if len(s_val) > string_logic.SHORT_STRING_THRESHOLD:
                        if blob_tmp: lltype.free(blob_tmp, flavor='raw')
                        blob_tmp = rffi.str2charp(s_val)
                        string_logic.pack_string(target, s_val, 0)
                    else:
                        string_logic.pack_string(target, s_val, 0)
                else:
                    ival = int(val)
                    if ftype.size == 8: rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, ival)
                    elif ftype.size == 4: rffi.cast(rffi.UINTP, target)[0] = rffi.cast(rffi.UINT, ival)
            
            self.add_row(r_uint128(pk), 1, tmp, blob_tmp)
        finally: 
            lltype.free(tmp, flavor='raw')
            if blob_tmp: rffi.free_charp(blob_tmp)

    def _add_row_weighted(self, pk, weight, *args):
        stride = self.schema.memtable_stride
        tmp = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
        blob_tmp = lltype.nullptr(rffi.CCHARP.TO)
        try:
            for i in range(stride): tmp[i] = '\x00'
            arg_idx = 0
            for i in range(len(self.schema.columns)):
                if i == self.schema.pk_index: continue
                if arg_idx >= len(args): break
                val = args[arg_idx]
                arg_idx += 1
                off = self.schema.get_column_offset(i)
                ftype = self.schema.columns[i].field_type
                target = rffi.ptradd(tmp, off)
                if ftype == types.TYPE_STRING:
                    s_val = str(val)
                    if len(s_val) > string_logic.SHORT_STRING_THRESHOLD:
                        if blob_tmp: lltype.free(blob_tmp, flavor='raw')
                        blob_tmp = rffi.str2charp(s_val)
                    string_logic.pack_string(target, s_val, 0)
                else:
                    ival = int(val)
                    if ftype.size == 8: rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, ival)
            self.add_row(r_uint128(pk), weight, tmp, blob_tmp)
        finally: 
            lltype.free(tmp, flavor='raw')
            if blob_tmp: rffi.free_charp(blob_tmp)

    def close(self):
        self.pk_buf.free()
        self.w_buf.free()
        for b in self.col_bufs:
            if b: b.free()
        self.b_buf.free()

    def finalize(self, filename):
        tmp_filename = filename + ".tmp"
        fd = rposix.open(tmp_filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            region_list = []
            region_list.append((self.pk_buf.ptr, 0, self.pk_buf.count * self.pk_buf.item_size))
            region_list.append((self.w_buf.ptr, 0, self.w_buf.count * 8))
            for b in self.col_bufs:
                if b: region_list.append((b.ptr, 0, self.count * b.item_size))
            region_list.append((self.b_buf.ptr, 0, self.b_buf.count))

            num_regions = len(region_list)
            dir_size = num_regions * layout.DIR_ENTRY_SIZE
            dir_offset = layout.HEADER_SIZE
            current_pos = _align_64(dir_offset + dir_size)
            final_regions = []
            for buf_ptr, _, sz in region_list:
                final_regions.append((buf_ptr, current_pos, sz))
                current_pos = _align_64(current_pos + sz)

            header = lltype.malloc(rffi.CCHARP.TO, layout.HEADER_SIZE, flavor='raw')
            try:
                for i in range(layout.HEADER_SIZE): header[i] = '\x00'
                rffi.cast(rffi.ULONGLONGP, header)[0] = layout.MAGIC_NUMBER
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_VERSION))[0] = rffi.cast(rffi.ULONGLONG, layout.VERSION)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_ROW_COUNT))[0] = rffi.cast(rffi.ULONGLONG, self.count)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_DIR_OFFSET))[0] = rffi.cast(rffi.ULONGLONG, dir_offset)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_TABLE_ID))[0] = rffi.cast(rffi.ULONGLONG, self.table_id)
                mmap_posix.write_c(fd, header, rffi.cast(rffi.SIZE_T, layout.HEADER_SIZE))
            finally: lltype.free(header, flavor='raw')

            dir_buf = lltype.malloc(rffi.CCHARP.TO, dir_size, flavor='raw')
            try:
                for i in range(num_regions):
                    buf_ptr, off, sz = final_regions[i]
                    cs = checksum.compute_checksum(buf_ptr, sz)
                    base = rffi.ptradd(dir_buf, i * layout.DIR_ENTRY_SIZE)
                    rffi.cast(rffi.ULONGLONGP, base)[0] = rffi.cast(rffi.ULONGLONG, off)
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 8))[0] = rffi.cast(rffi.ULONGLONG, sz)
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base, 16))[0] = rffi.cast(rffi.ULONGLONG, cs)
                mmap_posix.write_c(fd, dir_buf, rffi.cast(rffi.SIZE_T, dir_size))
            finally: lltype.free(dir_buf, flavor='raw')

            last_written_pos = dir_offset + dir_size
            for buf_ptr, off, sz in final_regions:
                padding = off - last_written_pos
                if padding > 0:
                    pad_buf = lltype.malloc(rffi.CCHARP.TO, padding, flavor='raw')
                    for i in range(padding): pad_buf[i] = '\x00'
                    mmap_posix.write_c(fd, pad_buf, rffi.cast(rffi.SIZE_T, padding))
                    lltype.free(pad_buf, flavor='raw')
                if sz > 0:
                    mmap_posix.write_c(fd, buf_ptr, rffi.cast(rffi.SIZE_T, sz))
                last_written_pos = off + sz
                
            mmap_posix.fsync_c(fd)
        finally:
            rposix.close(fd)
            self.close()
        os.rename(tmp_filename, filename)
        mmap_posix.fsync_dir(filename)

