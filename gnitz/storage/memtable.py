# gnitz/storage/memtable.py

from rpython.rlib.rrandom import Random
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.longlong2float import float2longlong
from gnitz.core import errors, comparator as core_comparator, xxh
from gnitz.storage import buffer, writer_table, comparator
from gnitz.storage.memtable_node import (
    node_get_next_off, node_set_next_off, node_get_weight, node_set_weight,
    node_get_key, node_get_payload_ptr, get_key_offset, node_get_hash, node_set_hash, HASH_SIZE
)
from gnitz.core import types, values, strings as string_logic

MAX_HEIGHT = 16

class MemTableBlobAllocator(string_logic.BlobAllocator):
    def __init__(self, arena):
        self.arena = arena

    def allocate(self, string_data):
        length = len(string_data)
        b_ptr = self.arena.alloc(length, alignment=8)
        for j in range(length):
            b_ptr[j] = string_data[j]
        off = rffi.cast(lltype.Signed, b_ptr) - rffi.cast(lltype.Signed, self.arena.base_ptr)
        return r_uint64(off)


class MemTable(object):
    _immutable_fields_ = ["schema", "arena", "blob_arena", "rng", "key_size", "head_off", "node_accessor", "value_accessor"]

    def __init__(self, schema, arena_size):
        self.schema = schema
        self.arena = buffer.Buffer(arena_size, growable=False)
        self.blob_arena = buffer.Buffer(arena_size, growable=False)
        self.rng = Random(1234)
        self._update_offsets = [0] * MAX_HEIGHT
        self.key_size = schema.get_pk_column().field_type.size

        self.node_accessor = comparator.PackedNodeAccessor(schema, self.blob_arena.base_ptr)
        self.value_accessor = core_comparator.ValueAccessor(schema)

        # Raw memory buffer for fast hashing (avoids object overhead)
        self.hash_buf_cap = 1024
        self.hash_buf = lltype.malloc(rffi.CCHARP.TO, self.hash_buf_cap, flavor='raw')

        self.arena.alloc(8, alignment=8)

        head_key_off = get_key_offset(MAX_HEIGHT)
        h_sz = head_key_off + self.key_size + HASH_SIZE + self.schema.memtable_stride

        ptr = self.arena.alloc(h_sz, alignment=16)
        self.head_off = rffi.cast(lltype.Signed, ptr) - rffi.cast(lltype.Signed, self.arena.base_ptr)

        ptr[8] = chr(MAX_HEIGHT)
        for i in range(MAX_HEIGHT):
            node_set_next_off(self.arena.base_ptr, self.head_off, i, 0)

        key_ptr = rffi.ptradd(ptr, head_key_off)
        all_ones = r_uint64(-1)
        if self.key_size == 16:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, all_ones)
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = rffi.cast(rffi.ULONGLONG, all_ones)
        else:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, all_ones)

    def _compute_payload_hash(self, row):
        schema = self.schema
        num_cols = len(schema.columns)
        
        # 1. Calculate exact required capacity WITH memory alignment protection
        sz = 0
        payload_col = 0
        for i in range(num_cols):
            if i == schema.pk_index:
                continue
            sz += 1 # Null flag byte
            if not row.is_null(payload_col):
                ft = schema.columns[i].field_type
                if ft.code == types.TYPE_STRING.code:
                    sz = (sz + 3) & ~3 # align 4
                    sz += 4 + len(row.get_str(payload_col))
                elif ft.code == types.TYPE_U128.code:
                    sz = (sz + 7) & ~7 # align 8
                    sz += 16
                else:
                    sz = (sz + 7) & ~7 # align 8
                    sz += 8
            payload_col += 1
            
        # 2. Ensure capacity (grow if necessary)
        if sz > self.hash_buf_cap:
            if self.hash_buf != lltype.nullptr(rffi.CCHARP.TO):
                lltype.free(self.hash_buf, flavor='raw')
            new_cap = sz * 2
            self.hash_buf = lltype.malloc(rffi.CCHARP.TO, new_cap, flavor='raw')
            self.hash_buf_cap = new_cap
            
        # 3. Serialize logically
        ptr = self.hash_buf
        offset = 0
        payload_col = 0
        for i in range(num_cols):
            if i == schema.pk_index:
                continue
                
            if row.is_null(payload_col):
                ptr[offset] = '\x00'
                offset += 1
            else:
                ptr[offset] = '\x01'
                offset += 1
                
                ft = schema.columns[i].field_type
                
                if ft.code == types.TYPE_STRING.code:
                    offset = (offset + 3) & ~3 # enforce 4-byte alignment
                    target = rffi.ptradd(ptr, offset)
                    s = row.get_str(payload_col)
                    length = len(s)
                    rffi.cast(rffi.UINTP, target)[0] = rffi.cast(rffi.UINT, length)
                    offset += 4
                    target_str = rffi.ptradd(ptr, offset)
                    for j in range(length):
                        target_str[j] = s[j]
                    offset += length
                    
                elif ft.code == types.TYPE_U128.code:
                    offset = (offset + 7) & ~7 # enforce 8-byte alignment
                    target = rffi.ptradd(ptr, offset)
                    val = row.get_u128(payload_col)
                    rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(val))
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(rffi.ULONGLONG, r_uint64(val >> 64))
                    offset += 16
                    
                elif ft.code == types.TYPE_F64.code or ft.code == types.TYPE_F32.code:
                    offset = (offset + 7) & ~7
                    target = rffi.ptradd(ptr, offset)
                    bits = float2longlong(row.get_float(payload_col))
                    rffi.cast(rffi.LONGLONGP, target)[0] = bits
                    offset += 8
                    
                else:
                    offset = (offset + 7) & ~7
                    target = rffi.ptradd(ptr, offset)
                    rffi.cast(rffi.LONGLONGP, target)[0] = row.get_int_signed(payload_col)
                    offset += 8
                    
            payload_col += 1
            
        # 4. Compute XXH3 over the exact serialized bytes
        return xxh.compute_checksum(self.hash_buf, sz)

    def _find_exact_values(self, key, row):
        """Backward compatible wrapper for external callers."""
        target_hash = self._compute_payload_hash(row)
        return self._find_exact_values_with_hash(key, row, target_hash)

    def _find_exact_values_with_hash(self, key, row, target_hash):
        base = self.arena.base_ptr
        curr_off = self.head_off

        self.value_accessor.set_row(row)

        for i in range(MAX_HEIGHT - 1, -1, -1):
            next_off = node_get_next_off(base, curr_off, i)
            while next_off != 0:
                next_key = node_get_key(base, next_off, self.key_size)

                if next_key < key:
                    curr_off = next_off
                elif next_key == key:
                    stored_hash = node_get_hash(base, next_off, self.key_size)
                    
                    if stored_hash < target_hash:
                        curr_off = next_off
                    elif stored_hash > target_hash:
                        break
                    else:
                        self.node_accessor.set_row(base, next_off)
                        if core_comparator.compare_rows(self.schema, self.node_accessor, self.value_accessor) < 0:
                            curr_off = next_off
                        else:
                            break
                else:
                    break

                next_off = node_get_next_off(base, curr_off, i)
            self._update_offsets[i] = curr_off

        match_off = node_get_next_off(base, curr_off, 0)
        if match_off != 0:
            if node_get_key(base, match_off, self.key_size) == key:
                if node_get_hash(base, match_off, self.key_size) == target_hash:
                    self.node_accessor.set_row(base, match_off)
                    if core_comparator.compare_rows(self.schema, self.node_accessor, self.value_accessor) == 0:
                        return match_off
        return 0

    def _lower_bound_node(self, key):
        base = self.arena.base_ptr
        curr_off = self.head_off

        for i in range(MAX_HEIGHT - 1, -1, -1):
            next_off = node_get_next_off(base, curr_off, i)
            while next_off != 0:
                next_key = node_get_key(base, next_off, self.key_size)
                if next_key < key:
                    curr_off = next_off
                else:
                    break
                next_off = node_get_next_off(base, curr_off, i)

        return node_get_next_off(base, curr_off, 0)

    def upsert(self, key, weight, row):
        base = self.arena.base_ptr
        
        target_hash = self._compute_payload_hash(row)
        match_off = self._find_exact_values_with_hash(key, row, target_hash)

        if match_off != 0:
            new_w = node_get_weight(base, match_off) + weight
            if new_w == 0:
                h = ord(base[match_off + 8])
                for i in range(h):
                    pred_off = self._update_offsets[i]
                    node_set_next_off(base, pred_off, i, node_get_next_off(base, match_off, i))
            else:
                node_set_weight(base, match_off, new_w)
            return

        h = 1
        while h < MAX_HEIGHT and (self.rng.genrand32() & 1):
            h += 1

        key_off = get_key_offset(h)
        node_full_sz = key_off + self.key_size + HASH_SIZE + self.schema.memtable_stride
        self._ensure_capacity(node_full_sz, row)

        new_ptr = self.arena.alloc(node_full_sz, alignment=16)
        new_off = rffi.cast(lltype.Signed, new_ptr) - rffi.cast(lltype.Signed, base)

        node_set_weight(base, new_off, weight)
        new_ptr[8] = chr(h)
        key_ptr = rffi.ptradd(new_ptr, key_off)

        if self.key_size == 16:
            r_key = r_uint128(key)
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(r_key))
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = rffi.cast(rffi.ULONGLONG, r_uint64(r_key >> 64))
        else:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(key))

        node_set_hash(base, new_off, self.key_size, target_hash)

        payload_ptr = node_get_payload_ptr(base, new_off, self.key_size)
        self._pack_into_node(payload_ptr, row)

        for i in range(h):
            pred = self._update_offsets[i]
            node_set_next_off(base, new_off, i, node_get_next_off(base, pred, i))
            node_set_next_off(base, pred, i, new_off)

    def _ensure_capacity(self, node_sz, row):
        blob_sz = 0
        payload_col = 0
        for i in range(len(self.schema.columns)):
            if i == self.schema.pk_index:
                continue

            if self.schema.columns[i].field_type.code == types.TYPE_STRING.code:
                if not row.is_null(payload_col):
                    s = row.get_str(payload_col)
                    if len(s) > string_logic.SHORT_STRING_THRESHOLD:
                        blob_sz += len(s)
            payload_col += 1

        if self.arena.offset + node_sz > self.arena.size or \
           self.blob_arena.offset + blob_sz > self.blob_arena.size:
            raise errors.MemTableFullError()

    def _pack_into_node(self, dest_ptr, row):
        allocator = MemTableBlobAllocator(self.blob_arena)
        payload_col = 0
        for i in range(len(self.schema.columns)):
            if i == self.schema.pk_index:
                continue

            f_type = self.schema.columns[i].field_type
            off = self.schema.get_column_offset(i)
            target = rffi.ptradd(dest_ptr, off)

            if row.is_null(payload_col):
                if f_type.code == types.TYPE_U128.code:
                    rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, 0)
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(rffi.ULONGLONG, 0)
                elif f_type.code == types.TYPE_STRING.code:
                    rffi.cast(rffi.UINTP, target)[0] = rffi.cast(rffi.UINT, 0)
                    rffi.cast(rffi.UINTP, rffi.ptradd(target, 4))[0] = rffi.cast(rffi.UINT, 0)
                    rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(rffi.ULONGLONG, 0)
                elif f_type.size == 8:
                    rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, 0)
                elif f_type.size == 4:
                    rffi.cast(rffi.UINTP, target)[0] = rffi.cast(rffi.UINT, 0)
                elif f_type.size == 2:
                    target[0] = '\x00'
                    target[1] = '\x00'
                elif f_type.size == 1:
                    target[0] = '\x00'
                payload_col += 1
                continue

            if f_type.code == types.TYPE_STRING.code:
                s = row.get_str(payload_col)
                string_logic.pack_and_write_blob(target, s, allocator)

            elif f_type.code == types.TYPE_F64.code:
                f_val = row.get_float(payload_col)
                rffi.cast(rffi.DOUBLEP, target)[0] = rffi.cast(rffi.DOUBLE, f_val)
                
            elif f_type.code == types.TYPE_F32.code:
                f_val = row.get_float(payload_col)
                rffi.cast(rffi.FLOATP, target)[0] = rffi.cast(rffi.FLOAT, f_val)

            elif f_type.code == types.TYPE_U128.code:
                u128_val = row.get_u128(payload_col)
                rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(u128_val))
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(rffi.ULONGLONG, r_uint64(u128_val >> 64))

            elif f_type.code == types.TYPE_U64.code:
                rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, row.get_int(payload_col))

            elif f_type.code == types.TYPE_U32.code:
                u32_ptr = rffi.cast(rffi.UINTP, target)
                u32_ptr[0] = rffi.cast(rffi.UINT, row.get_int(payload_col) & r_uint64(0xFFFFFFFF))

            elif f_type.code == types.TYPE_U16.code:
                u16_val = intmask(row.get_int(payload_col))
                target[0] = chr(u16_val & 0xFF)
                target[1] = chr((u16_val >> 8) & 0xFF)

            elif f_type.code == types.TYPE_U8.code:
                target[0] = chr(intmask(row.get_int(payload_col)) & 0xFF)

            elif f_type.code == types.TYPE_I64.code:
                rffi.cast(rffi.LONGLONGP, target)[0] = rffi.cast(rffi.LONGLONG, row.get_int_signed(payload_col))

            elif f_type.code == types.TYPE_I32.code:
                i32_val = row.get_int_signed(payload_col)
                u32_ptr = rffi.cast(rffi.UINTP, target)
                u32_ptr[0] = rffi.cast(rffi.UINT, i32_val & 0xFFFFFFFF)

            elif f_type.code == types.TYPE_I16.code:
                i16_val = intmask(row.get_int_signed(payload_col))
                target[0] = chr(i16_val & 0xFF)
                target[1] = chr((i16_val >> 8) & 0xFF)

            elif f_type.code == types.TYPE_I8.code:
                target[0] = chr(intmask(row.get_int_signed(payload_col)) & 0xFF)

            else:
                rffi.cast(rffi.LONGLONGP, target)[0] = rffi.cast(rffi.LONGLONG, row.get_int_signed(payload_col))

            payload_col += 1

    def flush(self, filename, table_id=0):
        sw = writer_table.TableShardWriter(self.schema, table_id)
        base, curr_off = self.arena.base_ptr, node_get_next_off(self.arena.base_ptr, self.head_off, 0)
        while curr_off != 0:
            w = node_get_weight(base, curr_off)
            if w != 0:
                sw.add_row(node_get_key(base, curr_off, self.key_size), w,
                           node_get_payload_ptr(base, curr_off, self.key_size), self.blob_arena.base_ptr)
            curr_off = node_get_next_off(base, curr_off, 0)
        sw.finalize(filename)

    def free(self):
        self.arena.free()
        self.blob_arena.free()
        if self.hash_buf != lltype.nullptr(rffi.CCHARP.TO):
            lltype.free(self.hash_buf, flavor='raw')
            self.hash_buf = lltype.nullptr(rffi.CCHARP.TO)
