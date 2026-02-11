"""
MemTable: In-memory write-ahead buffer for GnitzDB.

Implements a row-oriented SkipList with algebraic weight summation for
high-velocity ingestion. Supports atomic flush to columnar shards.
"""

from rpython.rlib.rrandom import Random
from rpython.rlib.rarithmetic import r_uint64
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.jit import unrolling_iterable
from gnitz.storage import arena, writer_ecs, errors
from gnitz.core import types, values, strings as string_logic

MAX_HEIGHT = 16
MAX_FIELDS = 64
FIELD_INDICES = unrolling_iterable(range(MAX_FIELDS))


# ============================================================================
# SkipList Node Access Helpers
# ============================================================================

def node_get_next_off(base_ptr, node_off, level):
    """Get the offset of the next node at a given level."""
    ptr = rffi.ptradd(base_ptr, node_off + 12)
    next_ptr = rffi.cast(rffi.UINTP, ptr)
    return rffi.cast(lltype.Signed, next_ptr[level])


def node_set_next_off(base_ptr, node_off, level, target_off):
    """Set the offset of the next node at a given level."""
    ptr = rffi.ptradd(base_ptr, node_off + 12)
    next_ptr = rffi.cast(rffi.UINTP, ptr)
    next_ptr[level] = rffi.cast(rffi.UINT, target_off)


def node_get_weight(base_ptr, node_off):
    """Get the algebraic weight of a node (signed 64-bit)."""
    ptr = rffi.ptradd(base_ptr, node_off)
    return rffi.cast(rffi.LONGLONGP, ptr)[0]


def node_set_weight(base_ptr, node_off, weight):
    """Set the algebraic weight of a node."""
    ptr = rffi.ptradd(base_ptr, node_off)
    rffi.cast(rffi.LONGLONGP, ptr)[0] = rffi.cast(rffi.LONGLONG, weight)


def node_get_key(base_ptr, node_off, key_size):
    """
    Get the primary key of a node.
    
    Args:
        base_ptr: Arena base pointer
        node_off: Offset to node in arena
        key_size: Size of key (8 for u64, 16 for u128)
        
    Returns:
        r_uint128 key (even for u64, zero-extended)
    """
    height = ord(base_ptr[node_off + 8])
    ptr = rffi.ptradd(base_ptr, node_off + 12 + (height * 4))
    
    if key_size == 16:
        # u128 key: read both halves
        lo = rffi.cast(rffi.ULONGLONGP, ptr)[0]
        hi = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, 8))[0]
        return (r_uint128(hi) << 64) | r_uint128(lo)
    else:
        # u64 key: zero-extend to u128
        key64 = rffi.cast(rffi.ULONGLONGP, ptr)[0]
        return r_uint128(key64)


def node_get_payload_ptr(base_ptr, node_off, key_size):
    """Get pointer to the payload (component bundle) of a node."""
    height = ord(base_ptr[node_off + 8])
    return rffi.ptradd(base_ptr, node_off + 12 + (height * 4) + key_size)


def compare_payloads(schema, ptr1, heap1, ptr2, heap2):
    """
    Compare two payload buffers for exact equality (0 = Equal).
    """
    for i in FIELD_INDICES:
        if i >= len(schema.columns):
            break
        if i == schema.pk_index:
            continue  # Skip PK column
        
        f_type = schema.columns[i].field_type
        f_off = schema.get_column_offset(i)
        
        p1 = rffi.ptradd(ptr1, f_off)
        p2 = rffi.ptradd(ptr2, f_off)
        
        if f_type == types.TYPE_STRING:
            res = string_logic.string_compare(p1, heap1, p2, heap2)
            if res != 0:
                return res
        else:
            for j in range(f_type.size):
                if p1[j] != p2[j]:
                    return 1
    return 0

def unpack_payload_to_values(memtable_inst, node_off):
    """
    Decodes a raw payload pointer back into DBValue objects.
    Used for ZSet inspection and distribution.
    """
    from gnitz.core import values, strings as string_logic
    base = memtable_inst.arena.base_ptr
    blob_base = memtable_inst.blob_arena.base_ptr
    ptr = node_get_payload_ptr(base, node_off, memtable_inst.key_size)
    schema = memtable_inst.schema
    res = []
    
    # Iterate through schema columns, skipping PK
    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue
        col_def = schema.columns[i]
        off = schema.get_column_offset(i)
        f_ptr = rffi.ptradd(ptr, off)
        
        if col_def.field_type == types.TYPE_STRING:
            u32_ptr = rffi.cast(rffi.UINTP, f_ptr)
            length = rffi.cast(lltype.Signed, u32_ptr[0])
            if length == 0:
                res.append(values.StringValue(""))
            elif length <= string_logic.SHORT_STRING_THRESHOLD:
                # Inline strings start at offset 4 (Prefix included in data)
                s_bytes = rffi.charpsize2str(rffi.ptradd(f_ptr, 4), length)
                res.append(values.StringValue(s_bytes))
            else:
                # Long strings follow the heap offset at Bytes 8-15
                u64_payload = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(f_ptr, 8))
                blob_off = rffi.cast(lltype.Signed, u64_payload[0])
                blob_ptr = rffi.ptradd(blob_base, blob_off)
                s_bytes = rffi.charpsize2str(blob_ptr, length)
                res.append(values.StringValue(s_bytes))
        elif col_def.field_type == types.TYPE_I64:
            val = rffi.cast(rffi.LONGLONGP, f_ptr)[0]
            res.append(values.IntValue(val))
        # Add other type handling as needed
    return res


# ============================================================================
# MemTable: Row-Oriented Write Buffer
# ============================================================================

class MemTable(object):
    _immutable_fields_ = [
        'arena', 'blob_arena', 'schema', 'head_off', 'threshold_bytes', 'key_size'
    ]

    def __init__(self, schema, arena_size):
        self.schema = schema
        self.arena = arena.Arena(arena_size)
        self.blob_arena = arena.Arena(arena_size)
        self.arena_size = arena_size
        self.threshold_bytes = (arena_size * 90) // 100
        self.rng = Random(1234)
        self._update_offsets = [0] * MAX_HEIGHT
        
        pk_col = schema.get_pk_column()
        self.key_size = pk_col.field_type.size
        
        self.arena.alloc(8) # NULL sentinel
        
        self.node_base_size = 12 + self.key_size + self.schema.stride
        
        h_sz = self.node_base_size + (MAX_HEIGHT * 4)
        ptr = self.arena.alloc(h_sz)
        self.head_off = rffi.cast(lltype.Signed, ptr) - rffi.cast(lltype.Signed, self.arena.base_ptr)
        
        ptr[8] = chr(MAX_HEIGHT)
        node_set_weight(self.arena.base_ptr, self.head_off, 0)
        for i in range(MAX_HEIGHT):
            node_set_next_off(self.arena.base_ptr, self.head_off, i, 0)
        
        key_ptr = rffi.ptradd(ptr, 12 + (MAX_HEIGHT * 4))
        max_u64 = rffi.cast(rffi.ULONGLONG, -1)
        if self.key_size == 16:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = max_u64
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = max_u64
        else:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = max_u64

    def _pack_values(self, dest_ptr, values_list):
        payload_idx = 0
        for i in FIELD_INDICES:
            if i >= len(self.schema.columns): break
            if i == self.schema.pk_index: continue
            
            if payload_idx >= len(values_list): break
                
            val_obj = values_list[payload_idx]
            f_type = self.schema.columns[i].field_type
            f_off = self.schema.get_column_offset(i)
            dest = rffi.ptradd(dest_ptr, f_off)
            
            if f_type == types.TYPE_STRING:
                if isinstance(val_obj, values.StringValue):
                    s_val = val_obj.v
                elif isinstance(val_obj, str):
                    s_val = val_obj
                else:
                    s_val = str(val_obj)
                    
                l_val = len(s_val)
                heap_off = 0
                if l_val > string_logic.SHORT_STRING_THRESHOLD:
                    blob_ptr = self.blob_arena.alloc(l_val)
                    heap_off = rffi.cast(lltype.Signed, blob_ptr) - \
                               rffi.cast(lltype.Signed, self.blob_arena.base_ptr)
                    for j in range(l_val):
                        blob_ptr[j] = s_val[j]
                string_logic.pack_string(dest, s_val, heap_off)
            elif isinstance(val_obj, values.IntValue):
                rffi.cast(rffi.LONGLONGP, dest)[0] = rffi.cast(rffi.LONGLONG, val_obj.v)
            elif isinstance(val_obj, (int, long)):
                 rffi.cast(rffi.LONGLONGP, dest)[0] = rffi.cast(rffi.LONGLONG, val_obj)
            
            payload_idx += 1

    def _pack_to_buf(self, dest_ptr, values):
        self._pack_values(dest_ptr, values)

    def _get_node_key(self, node_off):
        return node_get_key(self.arena.base_ptr, node_off, self.key_size)

    def _find_exact(self, key, packed_payload_ptr, payload_heap_ptr):
        base = self.arena.base_ptr
        curr_off = self.head_off
        for i in range(MAX_HEIGHT - 1, -1, -1):
            next_off = node_get_next_off(base, curr_off, i)
            while next_off != 0:
                next_key = node_get_key(base, next_off, self.key_size)
                if next_key < key:
                    curr_off = next_off
                    next_off = node_get_next_off(base, curr_off, i)
                else:
                    break
        return curr_off

    def _find_first_key(self, key):
        """Find the first node with node.key >= key."""
        base = self.arena.base_ptr
        curr_off = self.head_off
        for i in range(MAX_HEIGHT - 1, -1, -1):
            next_off = node_get_next_off(base, curr_off, i)
            while next_off != 0:
                next_key = node_get_key(base, next_off, self.key_size)
                if next_key < key:
                    curr_off = next_off
                    next_off = node_get_next_off(base, curr_off, i)
                else:
                    break
        return node_get_next_off(base, curr_off, 0)

    def _compare_payloads_ordering(self, ptr1, ptr2):
        for i in FIELD_INDICES:
            if i >= len(self.schema.columns): break
            if i == self.schema.pk_index: continue
            
            f_off = self.schema.get_column_offset(i)
            p1 = rffi.ptradd(ptr1, f_off)
            p2 = rffi.ptradd(ptr2, f_off)
            f_type = self.schema.columns[i].field_type
            
            if f_type == types.TYPE_STRING:
                res = string_logic.string_compare(p1, self.blob_arena.base_ptr, 
                                                 p2, self.blob_arena.base_ptr)
                if res != 0: return res
            else:
                # Byte-wise comparison for now
                for b in range(f_type.size):
                    if p1[b] < p2[b]: return -1
                    if p1[b] > p2[b]: return 1
        return 0

    def upsert(self, key, weight, field_values):
        scratch_ptr = lltype.malloc(rffi.CCHARP.TO, self.schema.stride, flavor='raw')
        try:
            for i in range(self.schema.stride): scratch_ptr[i] = '\x00'
            self._pack_values(scratch_ptr, field_values)
            
            base = self.arena.base_ptr
            curr_off = self.head_off
            
            for i in range(MAX_HEIGHT - 1, -1, -1):
                next_off = node_get_next_off(base, curr_off, i)
                while next_off != 0:
                    next_key = node_get_key(base, next_off, self.key_size)
                    if next_key < key:
                        curr_off = next_off
                        next_off = node_get_next_off(base, curr_off, i)
                    elif next_key == key:
                        next_payload = node_get_payload_ptr(base, next_off, self.key_size)
                        cmp_res = self._compare_payloads_ordering(scratch_ptr, next_payload)
                        if cmp_res > 0:
                            curr_off = next_off
                            next_off = node_get_next_off(base, curr_off, i)
                        else:
                            break
                    else:
                        break
                self._update_offsets[i] = curr_off

            next_off = node_get_next_off(base, curr_off, 0)
            is_match = False
            if next_off != 0:
                existing_key = node_get_key(base, next_off, self.key_size)
                if existing_key == key:
                    existing_payload = node_get_payload_ptr(base, next_off, self.key_size)
                    if self._compare_payloads_ordering(scratch_ptr, existing_payload) == 0:
                        is_match = True

            if is_match:
                old_w = node_get_weight(base, next_off)
                new_w = old_w + weight
                if new_w == 0:
                    h = ord(base[next_off + 8])
                    for i in range(h):
                        pred_off = self._update_offsets[i]
                        node_set_next_off(base, pred_off, i,
                                        node_get_next_off(base, next_off, i))
                else:
                    node_set_weight(base, next_off, new_w)
                return

            h = 1
            while h < MAX_HEIGHT and (self.rng.genrand32() & 1):
                h += 1
            
            node_sz = self.node_base_size + (h * 4)
            if self.arena.offset + node_sz > self.arena.size:
                raise errors.MemTableFullError()
            
            new_ptr = self.arena.alloc(node_sz)
            new_off = rffi.cast(lltype.Signed, new_ptr) - rffi.cast(lltype.Signed, base)
            
            node_set_weight(base, new_off, weight)
            new_ptr[8] = chr(h)
            
            key_ptr = rffi.ptradd(new_ptr, 12 + (h * 4))
            if self.key_size == 16:
                lo = rffi.cast(rffi.ULONGLONG, key & r_uint128(0xFFFFFFFFFFFFFFFF))
                hi = rffi.cast(rffi.ULONGLONG, key >> 64)
                rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = lo
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = hi
            else:
                rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, key)
            
            payload_ptr = rffi.ptradd(key_ptr, self.key_size)
            for i in range(self.schema.stride):
                payload_ptr[i] = scratch_ptr[i]

            for i in range(h):
                pred_off = self._update_offsets[i]
                node_set_next_off(base, new_off, i, node_get_next_off(base, pred_off, i))
                node_set_next_off(base, pred_off, i, new_off)
        finally:
            lltype.free(scratch_ptr, flavor='raw')

    def flush(self, filename):
        sw = writer_ecs.TableShardWriter(self.schema)
        base = self.arena.base_ptr
        blob_base = self.blob_arena.base_ptr
        
        curr_off = node_get_next_off(base, self.head_off, 0)
        while curr_off != 0:
            w = node_get_weight(base, curr_off)
            if w != 0:
                key = node_get_key(base, curr_off, self.key_size)
                payload_ptr = node_get_payload_ptr(base, curr_off, self.key_size)
                sw.add_row(key, w, payload_ptr, blob_base)
            curr_off = node_get_next_off(base, curr_off, 0)
        
        sw.finalize(filename)

    def free(self):
        self.arena.free()
        self.blob_arena.free()


# ============================================================================
# MemTableManager
# ============================================================================

class MemTableManager(object):
    _immutable_fields_ = ['schema', 'capacity', 'component_id']
    
    def __init__(self, schema, capacity, wal_writer=None, component_id=None):
        self.schema = schema
        self.component_id = component_id
        self.capacity = capacity
        self.wal_writer = wal_writer
        self.active_table = MemTable(self.schema, self.capacity)
        self.current_lsn = r_uint64(1)
        self.starting_lsn = r_uint64(1)

    def put(self, key, weight, field_values):
        try:
            self.active_table.upsert(r_uint128(key), weight, field_values)
        except errors.MemTableFullError:
            raise
        
        lsn = self.current_lsn
        self.current_lsn += r_uint64(1)
        
        if self.wal_writer:
            self.wal_writer.append_block(lsn, self.component_id, [(key, weight, field_values)])

    def flush_and_rotate(self, filename):
        self.active_table.flush(filename)
        self.active_table.free()
        self.active_table = MemTable(self.schema, self.capacity)

    def close(self):
        if self.active_table:
            self.active_table.free()
