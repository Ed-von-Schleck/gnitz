"""
gnitz/storage/memtable.py
"""
from rpython.rlib.rrandom import Random
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.jit import unrolling_iterable, unroll_safe
from gnitz.storage import arena, writer_ecs, errors
from gnitz.core import types, strings as string_logic

MAX_HEIGHT = 16
MAX_FIELDS = 64
FIELD_INDICES = unrolling_iterable(range(MAX_FIELDS))

# --- Low-level Node Accessors ---

def node_get_next_off(base_ptr, node_off, level):
    ptr = rffi.ptradd(base_ptr, node_off + 12)
    next_ptr = rffi.cast(rffi.UINTP, ptr)
    return rffi.cast(lltype.Signed, next_ptr[level])

def node_set_next_off(base_ptr, node_off, level, target_off):
    ptr = rffi.ptradd(base_ptr, node_off + 12)
    next_ptr = rffi.cast(rffi.UINTP, ptr)
    next_ptr[level] = rffi.cast(rffi.UINT, target_off)

def node_get_weight(base_ptr, node_off):
    ptr = rffi.ptradd(base_ptr, node_off)
    return rffi.cast(rffi.LONGLONGP, ptr)[0]

def node_set_weight(base_ptr, node_off, weight):
    ptr = rffi.ptradd(base_ptr, node_off)
    rffi.cast(rffi.LONGLONGP, ptr)[0] = rffi.cast(rffi.LONGLONG, weight)

def node_get_entity_id(base_ptr, node_off):
    height = ord(base_ptr[node_off + 8])
    ptr = rffi.ptradd(base_ptr, node_off + 12 + (height * 4))
    return rffi.cast(rffi.LONGLONGP, ptr)[0]

def node_get_payload_ptr(base_ptr, node_off):
    height = ord(base_ptr[node_off + 8])
    return rffi.ptradd(base_ptr, node_off + 12 + (height * 4) + 8)

# --- Unified SkipList Navigation ---

@unroll_safe
def skip_list_find(base_ptr, head_off, entity_id, update_offsets=None):
    """
    Standardizes SkipList traversal. Returns the offset of the node 
    immediately preceding the target entity_id at level 0.
    
    If update_offsets is provided, it populates the array with the 
    predecessor at every level (required for insertion).
    """
    curr_off = head_off
    for i in range(MAX_HEIGHT - 1, -1, -1):
        next_off = node_get_next_off(base_ptr, curr_off, i)
        while next_off != 0:
            if node_get_entity_id(base_ptr, next_off) < entity_id:
                curr_off = next_off
                next_off = node_get_next_off(base_ptr, curr_off, i)
            else:
                break
        if update_offsets is not None:
            update_offsets[i] = curr_off
    return curr_off

# --- MemTable Implementation ---

class MemTable(object):
    _immutable_fields_ = ['arena', 'blob_arena', 'layout', 'head_off', 'threshold_bytes']

    def __init__(self, layout, arena_size):
        self.layout = layout
        self.arena = arena.Arena(arena_size)
        self.blob_arena = arena.Arena(arena_size)
        self.arena_size = arena_size
        self.threshold_bytes = (arena_size * 90) / 100
        self.rng = Random(1234)
        self._update_offsets = [0] * MAX_HEIGHT
        
        self.arena.alloc(8) 
        self.node_base_size = 12 + 8 + self.layout.stride
        
        h_sz = self.node_base_size + (MAX_HEIGHT * 4)
        ptr = self.arena.alloc(h_sz)
        self.head_off = rffi.cast(lltype.Signed, ptr) - rffi.cast(lltype.Signed, self.arena.base_ptr)
        
        ptr[8] = chr(MAX_HEIGHT)
        node_set_weight(self.arena.base_ptr, self.head_off, 0)
        for i in range(MAX_HEIGHT):
            node_set_next_off(self.arena.base_ptr, self.head_off, i, 0)
        
        eid_ptr = rffi.ptradd(ptr, 12 + (MAX_HEIGHT * 4))
        rffi.cast(rffi.LONGLONGP, eid_ptr)[0] = rffi.cast(rffi.LONGLONG, -1)

    def _pack_values(self, dest_ptr, values):
        for i in FIELD_INDICES:
            if i >= len(values) or i >= len(self.layout.field_types):
                break
            
            val = values[i]
            f_type = self.layout.field_types[i]
            f_off = self.layout.field_offsets[i]
            dest = rffi.ptradd(dest_ptr, f_off)
            
            if f_type == types.TYPE_STRING:
                s_val = str(val)
                l_val = len(s_val)
                heap_off = 0
                if l_val > string_logic.SHORT_STRING_THRESHOLD:
                    blob_ptr = self.blob_arena.alloc(l_val)
                    heap_off = rffi.cast(lltype.Signed, blob_ptr) - rffi.cast(lltype.Signed, self.blob_arena.base_ptr)
                    for j in range(l_val): blob_ptr[j] = s_val[j]
                string_logic.pack_string(dest, s_val, heap_off)
            elif isinstance(val, int):
                rffi.cast(rffi.LONGLONGP, dest)[0] = rffi.cast(rffi.LONGLONG, val)
            elif isinstance(val, float):
                rffi.cast(rffi.DOUBLEP, dest)[0] = rffi.cast(rffi.DOUBLE, val)

    def upsert(self, entity_id, weight, field_values):
        base = self.arena.base_ptr
        
        # Unified search
        pred_off = skip_list_find(base, self.head_off, entity_id, self._update_offsets)
        next_off = node_get_next_off(base, pred_off, 0)
        
        # 1. Update existing node
        if next_off != 0 and node_get_entity_id(base, next_off) == entity_id:
            node_set_weight(base, next_off, node_get_weight(base, next_off) + weight)
            self._pack_values(node_get_payload_ptr(base, next_off), field_values)
            return

        # 2. Insert new node
        h = 1
        while h < MAX_HEIGHT and (self.rng.genrand32() & 1): h += 1
        
        node_sz = self.node_base_size + (h * 4)
        new_ptr = self.arena.alloc(node_sz)
        new_off = rffi.cast(lltype.Signed, new_ptr) - rffi.cast(lltype.Signed, base)
        
        node_set_weight(base, new_off, weight)
        new_ptr[8] = chr(h)
        
        eid_ptr = rffi.ptradd(new_ptr, 12 + (h * 4))
        rffi.cast(rffi.LONGLONGP, eid_ptr)[0] = rffi.cast(rffi.LONGLONG, entity_id)
        
        self._pack_values(rffi.ptradd(eid_ptr, 8), field_values)

        for i in range(h):
            p_off = self._update_offsets[i]
            node_set_next_off(base, new_off, i, node_get_next_off(base, p_off, i))
            node_set_next_off(base, p_off, i, new_off)

    def flush(self, filename):
        sw = writer_ecs.ECSShardWriter(self.layout)
        base = self.arena.base_ptr
        blob_base = self.blob_arena.base_ptr
        
        min_eid = -1
        max_eid = -1
        
        curr_off = node_get_next_off(base, self.head_off, 0)
        while curr_off != 0:
            w = node_get_weight(base, curr_off)
            if w != 0:
                eid = node_get_entity_id(base, curr_off)
                payload_ptr = node_get_payload_ptr(base, curr_off)
                sw.add_packed_row(eid, w, payload_ptr, blob_base)
                
                if min_eid == -1 or eid < min_eid: min_eid = eid
                if eid > max_eid: max_eid = eid
            curr_off = node_get_next_off(base, curr_off, 0)
        sw.finalize(filename)
        return min_eid, max_eid

    def free(self):
        self.arena.free()
        self.blob_arena.free()

class MemTableManager(object):
    def __init__(self, layout, capacity):
        self.layout = layout
        self.capacity = capacity
        self.active_table = MemTable(self.layout, self.capacity)

    def put(self, entity_id, weight, *field_values):
        self.active_table.upsert(entity_id, weight, field_values)

    def flush_and_rotate(self, filename):
        min_eid, max_eid = self.active_table.flush(filename)
        self.active_table.free()
        self.active_table = MemTable(self.layout, self.capacity)
        return min_eid, max_eid

    def close(self):
        if self.active_table:
            self.active_table.free()
