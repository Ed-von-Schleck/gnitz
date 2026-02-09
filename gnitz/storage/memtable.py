from rpython.rlib.rrandom import Random
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.jit import unrolling_iterable, unroll_safe
from gnitz.storage import arena, writer_ecs, errors, wal_format
from gnitz.core import types, strings as string_logic, values as db_values

MAX_HEIGHT = 16
MAX_FIELDS = 64
FIELD_INDICES = unrolling_iterable(range(MAX_FIELDS))

# Optimized accessors: Perform cast once and index
def node_get_next_off(base_ptr, node_off, level):
    # node_off + 12 is the start of the next-pointer array (U32 offsets)
    next_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(base_ptr, node_off + 12))
    return rffi.cast(lltype.Signed, next_ptr[level])

def node_set_next_off(base_ptr, node_off, level, target_off):
    next_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(base_ptr, node_off + 12))
    next_ptr[level] = rffi.cast(rffi.UINT, target_off)

def node_get_weight(base_ptr, node_off):
    return rffi.cast(rffi.LONGLONGP, rffi.ptradd(base_ptr, node_off))[0]

def node_set_weight(base_ptr, node_off, weight):
    rffi.cast(rffi.LONGLONGP, rffi.ptradd(base_ptr, node_off))[0] = rffi.cast(rffi.LONGLONG, weight)

def node_get_entity_id(base_ptr, node_off):
    height = ord(base_ptr[node_off + 8])
    # ID is after the weight(8), height(1), padding(3), and next-pointer array (height*4)
    off = node_off + 12 + (height * 4)
    return rffi.cast(rffi.ULONGLONGP, rffi.ptradd(base_ptr, off))[0]

def node_get_payload_ptr(base_ptr, node_off):
    height = ord(base_ptr[node_off + 8])
    return rffi.ptradd(base_ptr, node_off + 12 + (height * 4) + 8)

@unroll_safe
def compare_payloads(layout, ptr1, heap1, ptr2, heap2):
    """Optimized comparison to minimize pointer view creation."""
    for i in FIELD_INDICES:
        if i >= len(layout.field_types): break
        ftype = layout.field_types[i]
        foff = layout.field_offsets[i]
        
        # Access memory directly via cast+offset
        if ftype == types.TYPE_I64:
            v1 = rffi.cast(rffi.LONGLONGP, rffi.ptradd(ptr1, foff))[0]
            v2 = rffi.cast(rffi.LONGLONGP, rffi.ptradd(ptr2, foff))[0]
            if v1 < v2: return -1
            if v1 > v2: return 1
        elif ftype == types.TYPE_STRING:
            p1 = rffi.ptradd(ptr1, foff)
            p2 = rffi.ptradd(ptr2, foff)
            if not string_logic.string_equals_dual(p1, heap1, p2, heap2):
                res = string_logic.string_compare(p1, heap1, p2, heap2)
                if res != 0: return res
        elif ftype == types.TYPE_U64:
            v1 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr1, foff))[0]
            v2 = rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr2, foff))[0]
            if v1 < v2: return -1
            if v1 > v2: return 1
    return 0

@unroll_safe
def skip_list_find_exact(base_ptr, head_off, entity_id, layout, encoded_payload_ptr, heap_ptr, update_offsets=None):
    curr_off = head_off
    for i in range(MAX_HEIGHT - 1, -1, -1):
        # We index the next array directly inside the while loop
        while True:
            next_off = node_get_next_off(base_ptr, curr_off, i)
            if next_off == 0:
                break
            
            next_eid = node_get_entity_id(base_ptr, next_off)
            if next_eid < entity_id:
                curr_off = next_off
            elif next_eid == entity_id:
                next_payload = node_get_payload_ptr(base_ptr, next_off)
                cmp = compare_payloads(layout, next_payload, heap_ptr, encoded_payload_ptr, heap_ptr)
                if cmp < 0:
                    curr_off = next_off
                else:
                    break
            else:
                break
        if update_offsets is not None:
            update_offsets[i] = curr_off
    return curr_off

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
        # Head entity_id is initialized to 0 (unsigned), skip search will handle correctly
        rffi.cast(rffi.ULONGLONGP, eid_ptr)[0] = rffi.cast(rffi.ULONGLONG, 0)

    def has_active_data(self):
        base = self.arena.base_ptr
        curr_off = node_get_next_off(base, self.head_off, 0)
        while curr_off != 0:
            if node_get_weight(base, curr_off) != 0: return True
            curr_off = node_get_next_off(base, curr_off, 0)
        return False

    def _pack_values_to_buf(self, dest_ptr, values):
        for i in FIELD_INDICES:
            if i >= len(values) or i >= len(self.layout.field_types): break
            val_obj = values[i]
            f_type = self.layout.field_types[i]
            f_off = self.layout.field_offsets[i]
            dest = rffi.ptradd(dest_ptr, f_off)
            if f_type == types.TYPE_STRING:
                if isinstance(val_obj, db_values.StringValue):
                    s_val = val_obj.v
                    l_val = len(s_val)
                    heap_off = 0
                    if l_val > string_logic.SHORT_STRING_THRESHOLD:
                        blob_ptr = self.blob_arena.alloc(l_val)
                        heap_off = rffi.cast(lltype.Signed, blob_ptr) - rffi.cast(lltype.Signed, self.blob_arena.base_ptr)
                        for j in range(l_val): blob_ptr[j] = s_val[j]
                    string_logic.pack_string(dest, s_val, heap_off)
            elif f_type == types.TYPE_I64:
                if isinstance(val_obj, db_values.IntValue):
                    rffi.cast(rffi.LONGLONGP, dest)[0] = rffi.cast(rffi.LONGLONG, val_obj.v)
            elif f_type == types.TYPE_U64:
                if isinstance(val_obj, db_values.IntValue):
                    rffi.cast(rffi.ULONGLONGP, dest)[0] = rffi.cast(rffi.ULONGLONG, val_obj.v)

    def upsert(self, entity_id, weight, field_values):
        tmp_buf = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')
        try:
            for i in range(self.layout.stride): tmp_buf[i] = '\x00'
            self._pack_values_to_buf(tmp_buf, field_values)
            base = self.arena.base_ptr
            blob_base = self.blob_arena.base_ptr
            pred_off = skip_list_find_exact(base, self.head_off, entity_id, self.layout, tmp_buf, blob_base, self._update_offsets)
            next_off = node_get_next_off(base, pred_off, 0)
            match_found = False
            if next_off != 0:
                if node_get_entity_id(base, next_off) == entity_id:
                    if compare_payloads(self.layout, node_get_payload_ptr(base, next_off), blob_base, tmp_buf, blob_base) == 0:
                        match_found = True
            if match_found:
                node_set_weight(base, next_off, node_get_weight(base, next_off) + weight)
                return
            h = 1
            while h < MAX_HEIGHT and (self.rng.genrand32() & 1): h += 1
            node_sz = self.node_base_size + (h * 4)
            new_ptr = self.arena.alloc(node_sz)
            new_off = rffi.cast(lltype.Signed, new_ptr) - rffi.cast(lltype.Signed, base)
            node_set_weight(base, new_off, weight)
            new_ptr[8] = chr(h)
            eid_ptr = rffi.ptradd(new_ptr, 12 + (h * 4))
            rffi.cast(rffi.ULONGLONGP, eid_ptr)[0] = rffi.cast(rffi.ULONGLONG, entity_id)
            payload_dest = rffi.ptradd(eid_ptr, 8)
            for i in range(self.layout.stride): payload_dest[i] = tmp_buf[i]
            for i in range(h):
                p_off = self._update_offsets[i]
                node_set_next_off(base, new_off, i, node_get_next_off(base, p_off, i))
                node_set_next_off(base, p_off, i, new_off)
        finally: lltype.free(tmp_buf, flavor='raw')

    def upsert_raw(self, entity_id, weight, raw_data):
        """Standardizes search to only use Pointers, resolving UnionError."""
        base = self.arena.base_ptr
        blob_base = self.blob_arena.base_ptr
        # Fix: Create raw buffer so skip_list_find_exact sees consistent Ptr type
        tmp_buf = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')
        try:
            for i in range(self.layout.stride): tmp_buf[i] = raw_data[i]
            pred_off = skip_list_find_exact(base, self.head_off, entity_id, self.layout, tmp_buf, blob_base, self._update_offsets)
            
            next_off = node_get_next_off(base, pred_off, 0)
            match_found = False
            if next_off != 0:
                if node_get_entity_id(base, next_off) == entity_id:
                    if compare_payloads(self.layout, node_get_payload_ptr(base, next_off), blob_base, tmp_buf, blob_base) == 0:
                        match_found = True
            if match_found:
                node_set_weight(base, next_off, node_get_weight(base, next_off) + weight)
                return
            h = 1
            while h < MAX_HEIGHT and (self.rng.genrand32() & 1): h += 1
            node_sz = self.node_base_size + (h * 4)
            new_ptr = self.arena.alloc(node_sz)
            new_off = rffi.cast(lltype.Signed, new_ptr) - rffi.cast(lltype.Signed, base)
            node_set_weight(base, new_off, weight)
            new_ptr[8] = chr(h)
            eid_ptr = rffi.ptradd(new_ptr, 12 + (h * 4))
            rffi.cast(rffi.ULONGLONGP, eid_ptr)[0] = rffi.cast(rffi.ULONGLONG, entity_id)
            payload_dest = rffi.ptradd(eid_ptr, 8)
            for i in range(self.layout.stride): payload_dest[i] = tmp_buf[i]
            for i in range(h):
                p_off = self._update_offsets[i]
                node_set_next_off(base, new_off, i, node_get_next_off(base, p_off, i))
                node_set_next_off(base, p_off, i, new_off)
        finally: lltype.free(tmp_buf, flavor='raw')

    def flush(self, filename):
        sw = writer_ecs.ECSShardWriter(self.layout)
        base = self.arena.base_ptr
        blob_base = self.blob_arena.base_ptr
        
        # RPython fix: Initialize with unsigned 0 to match Entity ID type
        min_eid = r_uint64(0) 
        max_eid = r_uint64(0)
        first = True
        
        curr_off = node_get_next_off(base, self.head_off, 0)
        while curr_off != 0:
            w = node_get_weight(base, curr_off)
            if w != 0:
                eid = node_get_entity_id(base, curr_off)
                sw.add_packed_row(eid, w, node_get_payload_ptr(base, curr_off), blob_base)
                
                if first or eid < min_eid: 
                    min_eid = eid
                if first or eid > max_eid: 
                    max_eid = eid
                first = False
            curr_off = node_get_next_off(base, curr_off, 0)
        sw.finalize(filename)
        return min_eid, max_eid


    def free(self):
        self.arena.free()
        self.blob_arena.free()

class MemTableManager(object):
    def __init__(self, layout, capacity, wal_writer=None, component_id=1):
        self.layout = layout
        self.capacity = capacity
        self.active_table = MemTable(self.layout, self.capacity)
        self.wal_writer = wal_writer
        self.component_id = component_id
        self.current_lsn = 1
        self.starting_lsn = 1

    def put(self, entity_id, weight, field_values):
        lsn = self.current_lsn
        self.current_lsn += 1
        if self.wal_writer is not None:
            component_data = self._pack_component_for_wal(field_values)
            self.wal_writer.append_block(lsn, self.component_id, [(entity_id, weight, component_data)])
        if self.active_table.arena.offset > self.active_table.threshold_bytes:
            raise errors.MemTableFullError()
        self.active_table.upsert(entity_id, weight, field_values)

    def put_from_recovery(self, entity_id, weight, raw_component_data):
        if self.active_table.arena.offset > self.active_table.threshold_bytes:
            raise errors.MemTableFullError()
        self.active_table.upsert_raw(entity_id, weight, raw_component_data)

    def flush_and_rotate(self, filename):
        min_eid, max_eid = self.active_table.flush(filename)
        self.active_table.free()
        self.active_table = MemTable(self.layout, self.capacity)
        self.starting_lsn = self.current_lsn
        return min_eid, max_eid

    def _pack_component_for_wal(self, field_values):
        stride = self.layout.stride
        buf = lltype.malloc(rffi.CCHARP.TO, stride, flavor='raw')
        try:
            for i in range(stride): buf[i] = '\x00'
            for i in FIELD_INDICES:
                if i >= len(field_values) or i >= len(self.layout.field_types): break
                val_obj = field_values[i]
                f_type = self.layout.field_types[i]
                dest = rffi.ptradd(buf, self.layout.field_offsets[i])
                if f_type == types.TYPE_STRING:
                    if isinstance(val_obj, db_values.StringValue):
                        string_logic.pack_string(dest, val_obj.v, 0)
                elif f_type == types.TYPE_I64:
                    if isinstance(val_obj, db_values.IntValue):
                         rffi.cast(rffi.LONGLONGP, dest)[0] = rffi.cast(rffi.LONGLONG, val_obj.v)
            return rffi.charpsize2str(buf, stride)
        finally: lltype.free(buf, flavor='raw')

    def close(self):
        if self.active_table: self.active_table.free()
