from rpython.rlib.rrandom import Random
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import arena, writer_table, errors
from gnitz.storage.memtable_node import (
    node_get_next_off, node_set_next_off, node_get_weight, node_set_weight,
    node_get_key, node_get_payload_ptr
)
from gnitz.storage.comparator import compare_values_to_packed
from gnitz.core import types, strings as string_logic

MAX_HEIGHT = 16

class MemTable(object):
    """
    Mutable, row-oriented SkipList for Z-Set ingestion.
    Performs immediate in-place algebraic coalescing of weights.
    """
    _immutable_fields_ = ['arena', 'blob_arena', 'schema', 'head_off', 'key_size']

    def __init__(self, schema, arena_size):
        self.schema = schema
        self.arena = arena.Arena(arena_size)
        self.blob_arena = arena.Arena(arena_size)
        self.rng = Random(1234)
        self._update_offsets = [0] * MAX_HEIGHT
        self.key_size = schema.get_pk_column().field_type.size
        
        # Reserved NULL sentinel at offset 0
        self.arena.alloc(8) 
        
        # Calculate base node size (Weight + Height + Padding + Key + Stride)
        self.node_base_size = 12 + self.key_size + self.schema.stride
        # Full head node size includes the next-pointer array at max height
        h_sz = self.node_base_size + (MAX_HEIGHT * 4)
        ptr = self.arena.alloc(h_sz)
        self.head_off = rffi.cast(lltype.Signed, ptr) - rffi.cast(lltype.Signed, self.arena.base_ptr)
        
        ptr[8] = chr(MAX_HEIGHT)
        for i in range(MAX_HEIGHT): 
            node_set_next_off(self.arena.base_ptr, self.head_off, i, 0)
        
        # Initialize head node key to max value
        key_ptr = rffi.ptradd(ptr, 12 + (MAX_HEIGHT * 4))
        if self.key_size == 16:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, -1)
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = rffi.cast(rffi.ULONGLONG, -1)
        else:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, -1)

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

    def _find_exact_values(self, key, field_values):
        """Finds node with exact Primary Key and Semantic Payload equality."""
        base = self.arena.base_ptr
        blob_base = self.blob_arena.base_ptr
        curr_off = self.head_off
        for i in range(MAX_HEIGHT - 1, -1, -1):
            next_off = node_get_next_off(base, curr_off, i)
            while next_off != 0:
                next_key = node_get_key(base, next_off, self.key_size)
                if next_key < key:
                    curr_off = next_off
                elif next_key == key:
                    next_payload = node_get_payload_ptr(base, next_off, self.key_size)
                    if compare_values_to_packed(self.schema, field_values, next_payload, blob_base) > 0:
                        curr_off = next_off
                    else: break
                else: break
                next_off = node_get_next_off(base, curr_off, i)
            self._update_offsets[i] = curr_off
        
        match_off = node_get_next_off(base, curr_off, 0)
        if match_off != 0:
            if node_get_key(base, match_off, self.key_size) == key:
                payload = node_get_payload_ptr(base, match_off, self.key_size)
                if compare_values_to_packed(self.schema, field_values, payload, blob_base) == 0:
                    return match_off
        return 0

    def upsert(self, key, weight, field_values):
        """Idempotent Z-Set update with algebraic summation and annihilation."""
        base = self.arena.base_ptr
        match_off = self._find_exact_values(key, field_values)
        
        if match_off != 0:
            new_w = node_get_weight(base, match_off) + weight
            if new_w == 0:
                # PHYSICAL ANNIHILATION: Unlink node from SkipList
                h = ord(base[match_off + 8])
                for i in range(h):
                    pred_off = self._update_offsets[i]
                    node_set_next_off(base, pred_off, i, node_get_next_off(base, match_off, i))
            else:
                node_set_weight(base, match_off, new_w)
            return

        self._ensure_capacity(field_values)
        h = 1
        while h < MAX_HEIGHT and (self.rng.genrand32() & 1): 
            h += 1
        
        new_ptr = self.arena.alloc(self.node_base_size + (h * 4))
        new_off = rffi.cast(lltype.Signed, new_ptr) - rffi.cast(lltype.Signed, base)
        
        node_set_weight(base, new_off, weight)
        new_ptr[8] = chr(h)
        key_ptr = rffi.ptradd(new_ptr, 12 + (h * 4))
        
        if self.key_size == 16:
            r_key = r_uint128(key)
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(r_key))
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = rffi.cast(rffi.ULONGLONG, r_uint64(r_key >> 64))
        else:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(key))
            
        payload_ptr = rffi.ptradd(key_ptr, self.key_size)
        self._pack_into_node(payload_ptr, field_values)

        for i in range(h):
            pred = self._update_offsets[i]
            node_set_next_off(base, new_off, i, node_get_next_off(base, pred, i))
            node_set_next_off(base, pred, i, new_off)

    def _ensure_capacity(self, values):
        """Validates that the update fits within Arena bounds."""
        blob_sz = 0
        v_idx = 0
        for i in range(len(self.schema.columns)):
            if i == self.schema.pk_index: 
                continue
            if self.schema.columns[i].field_type == types.TYPE_STRING:
                s = values[v_idx].get_string() 
                if len(s) > string_logic.SHORT_STRING_THRESHOLD: 
                    blob_sz += len(s)
            v_idx += 1
            
        if self.arena.offset + self.node_base_size + (MAX_HEIGHT*4) > self.arena.size or \
           self.blob_arena.offset + blob_sz > self.blob_arena.size:
            raise errors.MemTableFullError()

    def _pack_into_node(self, dest_ptr, values):
        """Serializes DBValues into the row-oriented node payload."""
        v_idx = 0
        for i in range(len(self.schema.columns)):
            if i == self.schema.pk_index: 
                continue
            val_obj, f_type = values[v_idx], self.schema.columns[i].field_type
            v_idx += 1
            off = self.schema.get_column_offset(i)
            target = rffi.ptradd(dest_ptr, off)
            
            if f_type == types.TYPE_STRING:
                s_val = val_obj.get_string()
                h_off = 0
                if len(s_val) > string_logic.SHORT_STRING_THRESHOLD:
                    b_ptr = self.blob_arena.alloc(len(s_val))
                    h_off = rffi.cast(lltype.Signed, b_ptr) - rffi.cast(lltype.Signed, self.blob_arena.base_ptr)
                    for j in range(len(s_val)): 
                        b_ptr[j] = s_val[j]
                string_logic.pack_string(target, s_val, h_off)
            elif f_type == types.TYPE_F64:
                rffi.cast(rffi.DOUBLEP, target)[0] = rffi.cast(rffi.DOUBLE, val_obj.get_float())
            elif f_type == types.TYPE_U128:
                u128_val = val_obj.get_u128()
                rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(u128_val))
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(rffi.ULONGLONG, r_uint64(u128_val >> 64))
            else:
                rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, val_obj.get_int())

    def flush(self, filename):
        """Transmutes row-oriented MemTable to columnar Table Shard."""
        sw = writer_table.TableShardWriter(self.schema)
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
