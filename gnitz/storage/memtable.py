from rpython.rlib.rrandom import Random
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib import jit

try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long
from rpython.rlib.jit import unrolling_iterable, unroll_safe
from gnitz.storage import arena, writer_ecs, errors, wal_format
from gnitz.core import types, strings as string_logic, values as db_values

MAX_HEIGHT = 16
MAX_FIELDS = 64
FIELD_INDICES = unrolling_iterable(range(MAX_FIELDS))

def _align(val, alignment): return (val + alignment - 1) & ~(alignment - 1)

def node_get_next_off(base_ptr, node_off, level):
    next_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(base_ptr, node_off + 12))
    return rffi.cast(lltype.Signed, next_ptr[level])

def node_set_next_off(base_ptr, node_off, level, target_off):
    next_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(base_ptr, node_off + 12))
    next_ptr[level] = rffi.cast(rffi.UINT, target_off)

def node_get_weight(base_ptr, node_off):
    return rffi.cast(rffi.LONGLONGP, rffi.ptradd(base_ptr, node_off))[0]

def node_set_weight(base_ptr, node_off, weight):
    rffi.cast(rffi.LONGLONGP, rffi.ptradd(base_ptr, node_off))[0] = rffi.cast(rffi.LONGLONG, weight)

def node_get_key64(base_ptr, node_off):
    height = ord(base_ptr[node_off + 8])
    off = node_off + 12 + (height * 4)
    return rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base_ptr, off))[0]

def node_get_key128(base_ptr, node_off):
    height = ord(base_ptr[node_off + 8])
    off = node_off + 12 + (height * 4)
    ptr = rffi.ptradd(base_ptr, off)
    low = rffi.cast(rffi.ULONGLONGP, ptr)[0]
    high = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
    return (r_uint128(high) << 64) | r_uint128(low)

def node_get_payload_ptr(base_ptr, node_off, key_size):
    height = ord(base_ptr[node_off + 8])
    off = _align(12 + (height * 4) + key_size, 8)
    return rffi.ptradd(base_ptr, node_off + off)

@unroll_safe
def compare_payloads(schema, ptr1, heap1, ptr2, heap2):
    for i in FIELD_INDICES:
        if i >= len(schema.columns): break
        if i == schema.pk_index: continue
        
        col_def = schema.columns[i]
        foff = schema.get_column_offset(i)
        ftype = col_def.field_type
        
        if ftype == types.TYPE_I64 or ftype == types.TYPE_U64:
            v1 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr1, foff))[0]
            v2 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr2, foff))[0]
            if v1 < v2: return -1
            if v1 > v2: return 1
        elif ftype == types.TYPE_STRING:
            p1 = rffi.ptradd(ptr1, foff)
            p2 = rffi.ptradd(ptr2, foff)
            if not string_logic.string_equals_dual(p1, heap1, p2, heap2):
                res = string_logic.string_compare(p1, heap1, p2, heap2)
                if res != 0: return res
    return 0

class MemTable(object):
    _immutable_fields_ = ['arena', 'blob_arena', 'schema', 'head_off', 'key_size', 'is_u128', 'threshold_bytes']
    
    def __init__(self, schema, arena_size):
        self.schema = schema
        self.arena = arena.Arena(arena_size)
        self.blob_arena = arena.Arena(arena_size)
        self.is_u128 = schema.get_pk_column().field_type == types.TYPE_U128
        self.key_size = 16 if self.is_u128 else 8
        self.threshold_bytes = int(arena_size * 0.9)
        self.rng = Random()
        self._update_offsets = [0] * MAX_HEIGHT
        
        self.pointers_offset = 12
        self.key_offset = _align(self.pointers_offset + (MAX_HEIGHT * 4), 8)
        self.payload_offset = _align(self.key_offset + self.key_size, 8)
        self.node_base_size = self.payload_offset + self.schema.memtable_stride
        
        ptr = self.arena.alloc(self.node_base_size)
        self.head_off = rffi.cast(lltype.Signed, ptr) - rffi.cast(lltype.Signed, self.arena.base_ptr)
        
        ptr[8] = chr(MAX_HEIGHT)
        node_set_weight(self.arena.base_ptr, self.head_off, 0)
        for i in range(MAX_HEIGHT): node_set_next_off(self.arena.base_ptr, self.head_off, i, 0)

    def _get_node_key(self, node_off):
        if self.is_u128: return node_get_key128(self.arena.base_ptr, node_off)
        return r_uint128(node_get_key64(self.arena.base_ptr, node_off))

    @jit.unroll_safe
    def _find_exact(self, key, encoded_payload_ptr, heap_ptr, update_offsets=None):
        base_ptr = self.arena.base_ptr
        blob_base = self.blob_arena.base_ptr
        curr_off = self.head_off
        
        for i in range(MAX_HEIGHT - 1, -1, -1):
            while True:
                next_off = node_get_next_off(base_ptr, curr_off, i)
                if next_off == 0: break
                
                nk = self._get_node_key(next_off)
                if nk < key:
                    curr_off = next_off
                elif nk == key:
                    next_payload = node_get_payload_ptr(base_ptr, next_off, self.key_size)
                    if compare_payloads(self.schema, next_payload, blob_base, encoded_payload_ptr, heap_ptr) < 0:
                        curr_off = next_off
                    else: break
                else: break
            if update_offsets is not None: update_offsets[i] = curr_off
        return curr_off

    def upsert(self, key, weight, field_values):
        stride = self.schema.memtable_stride
        tmp_buf = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
        try:
            for i in range(stride): tmp_buf[i] = '\x00'
            
            if isinstance(field_values, str):
                for i in range(min(stride, len(field_values))):
                    tmp_buf[i] = field_values[i]
            else:
                self._pack_to_buf(tmp_buf, field_values)
            
            base = self.arena.base_ptr
            blob_base = self.blob_arena.base_ptr
            pred_off = self._find_exact(key, tmp_buf, blob_base, self._update_offsets)
            next_off = node_get_next_off(base, pred_off, 0)
            
            match_found = False
            if next_off != 0 and self._get_node_key(next_off) == key:
                if compare_payloads(self.schema, node_get_payload_ptr(base, next_off, self.key_size), blob_base, tmp_buf, blob_base) == 0:
                    match_found = True
            
            if match_found:
                node_set_weight(base, next_off, node_get_weight(base, next_off) + weight)
                return

            h = 1
            while h < MAX_HEIGHT and (self.rng.genrand32() & 1): h += 1
            node_sz = _align(12 + (h * 4) + self.key_size, 8) + stride
            new_ptr = self.arena.alloc(node_sz)
            new_off = rffi.cast(lltype.Signed, new_ptr) - rffi.cast(lltype.Signed, base)
            
            node_set_weight(base, new_off, weight)
            new_ptr[8] = chr(h)
            
            key_ptr = rffi.ptradd(new_ptr, 12 + (h * 4))
            mask = r_uint128(0xFFFFFFFFFFFFFFFF)
            if self.is_u128:
                rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, key & mask)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = rffi.cast(rffi.ULONGLONG, (key >> 64) & mask)
            else:
                rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, key & mask)
            
            payload_dest = node_get_payload_ptr(base, new_off, self.key_size)
            for i in range(stride): payload_dest[i] = tmp_buf[i]
            
            for i in range(h):
                p_off = self._update_offsets[i]
                node_set_next_off(base, new_off, i, node_get_next_off(base, p_off, i))
                node_set_next_off(base, p_off, i, new_off)
        finally:
            lltype.free(tmp_buf, flavor='raw')

    def _pack_to_buf(self, dest_ptr, values):
        arg_idx = 0
        for i in range(len(self.schema.columns)):
            if i == self.schema.pk_index: continue
            if arg_idx >= len(values): break
            
            val_obj = values[arg_idx]
            arg_idx += 1
            
            col_def = self.schema.columns[i]
            f_off = self.schema.get_column_offset(i)
            dest = rffi.ptradd(dest_ptr, f_off)
            
            if col_def.field_type == types.TYPE_STRING:
                if isinstance(val_obj, db_values.StringValue):
                    s_val = val_obj.v
                    l_val = len(s_val)
                    h_off = 0
                    if l_val > string_logic.SHORT_STRING_THRESHOLD:
                        b_ptr = self.blob_arena.alloc(l_val)
                        h_off = rffi.cast(lltype.Signed, b_ptr) - rffi.cast(lltype.Signed, self.blob_arena.base_ptr)
                        for j in range(l_val): b_ptr[j] = s_val[j]
                    string_logic.pack_string(dest, s_val, h_off)
            else:
                val = val_obj.v if isinstance(val_obj, db_values.IntValue) else 0
                if col_def.field_type.size == 8:
                    rffi.cast(rffi.ULONGLONGP, dest)[0] = rffi.cast(rffi.ULONGLONG, val)
                elif col_def.field_type.size == 4:
                    rffi.cast(rffi.UINTP, dest)[0] = rffi.cast(rffi.UINT, val)
                elif col_def.field_type.size == 1:
                    dest[0] = chr(val & 0xFF)

    def flush(self, filename, table_id):
        sw = writer_ecs.TableShardWriter(self.schema, table_id)
        base = self.arena.base_ptr
        blob_base = self.blob_arena.base_ptr
        try:
            curr_off = node_get_next_off(base, self.head_off, 0)
            while curr_off != 0:
                w = node_get_weight(base, curr_off)
                if w != 0:
                    key = self._get_node_key(curr_off)
                    payload = node_get_payload_ptr(base, curr_off, self.key_size)
                    sw.add_row(key, w, payload, blob_base)
                curr_off = node_get_next_off(base, curr_off, 0)
            sw.finalize(filename)
        finally:
            sw.close()

    def free(self):
        self.arena.free()
        self.blob_arena.free()

class MemTableManager(object):
    def __init__(self, schema, arena_size, wal_writer=None, table_id=1, **kwargs):
        self.schema = schema
        self.capacity = arena_size
        self.active_table = MemTable(schema, arena_size)
        self.wal_writer = wal_writer
        self.table_id = kwargs.get('component_id', table_id)
        self.current_lsn = r_uint64(1)
        self.starting_lsn = r_uint64(1)

    def put(self, key, weight, field_values):
        lsn = self.current_lsn
        self.current_lsn += r_uint64(1)
        
        if self.wal_writer is not None:
            self.wal_writer.append_block(lsn, self.table_id, [(key, weight, field_values)])
            
        if self.active_table.arena.offset > self.active_table.threshold_bytes:
            raise errors.MemTableFullError()
        self.active_table.upsert(key, weight, field_values)

    def close(self):
        self.active_table.free()

    def flush_and_rotate(self, filename):
        self.active_table.flush(filename, self.table_id)
        self.active_table.free()
        self.active_table = MemTable(self.schema, self.capacity)
        self.starting_lsn = self.current_lsn
