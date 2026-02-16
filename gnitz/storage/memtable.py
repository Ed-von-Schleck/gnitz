# gnitz/storage/memtable.py

from rpython.rlib.rrandom import Random
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import buffer, writer_table, errors, comparator
from gnitz.storage.memtable_node import (
    node_get_next_off, node_set_next_off, node_get_weight, node_set_weight,
    node_get_key, node_get_payload_ptr, get_key_offset
)
from gnitz.core import types, values, strings as string_logic

MAX_HEIGHT = 16

class MemTableBlobAllocator(string_logic.BlobAllocator):
    """
    Allocator strategy for MemTable string tails.
    Persists long strings into the MemTable's Blob Arena.
    """
    def __init__(self, arena):
        self.arena = arena

    def allocate(self, string_data):
        length = len(string_data)
        # Strings in the blob arena use 8-byte alignment for 64-bit access
        b_ptr = self.arena.alloc(length, alignment=8)
        
        # Copy string data to the arena
        for j in range(length):
            b_ptr[j] = string_data[j]
            
        # Return the offset relative to the start of the blob arena as r_uint64
        off = rffi.cast(lltype.Signed, b_ptr) - rffi.cast(lltype.Signed, self.arena.base_ptr)
        return r_uint64(off)

class MemTable(object):
    _immutable_fields_ = [
        'arena', 'blob_arena', 'schema', 'head_off', 'key_size',
        'node_accessor', 'value_accessor'
    ]

    def __init__(self, schema, arena_size):
        self.schema = schema
        self.arena = buffer.Buffer(arena_size, growable=False)
        self.blob_arena = buffer.Buffer(arena_size, growable=False)
        self.rng = Random(1234)
        self._update_offsets = [0] * MAX_HEIGHT
        self.key_size = schema.get_pk_column().field_type.size
        
        # Initialize reusable accessors for comparison
        self.node_accessor = comparator.PackedNodeAccessor(schema, self.blob_arena.base_ptr)
        self.value_accessor = comparator.ValueAccessor(schema)

        # Reserved NULL sentinel at offset 0
        self.arena.alloc(8, alignment=8) 
        
        # Head node allocation
        head_key_off = get_key_offset(MAX_HEIGHT)
        h_sz = head_key_off + self.key_size + self.schema.memtable_stride
        
        ptr = self.arena.alloc(h_sz, alignment=16)
        self.head_off = rffi.cast(lltype.Signed, ptr) - rffi.cast(lltype.Signed, self.arena.base_ptr)
        
        ptr[8] = chr(MAX_HEIGHT)
        for i in range(MAX_HEIGHT): 
            node_set_next_off(self.arena.base_ptr, self.head_off, i, 0)
        
        # Use r_uint64(-1) to generate 0xFF...FF without prebuilt long trap
        key_ptr = rffi.ptradd(ptr, head_key_off)
        all_ones = r_uint64(-1)
        if self.key_size == 16:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, all_ones)
            rffi.cast(rffi.ULONGLONGP, rffi.ptradd(key_ptr, 8))[0] = rffi.cast(rffi.ULONGLONG, all_ones)
        else:
            rffi.cast(rffi.ULONGLONGP, key_ptr)[0] = rffi.cast(rffi.ULONGLONG, all_ones)

    def _find_first_key(self, key):
        """Locates the first node in the SkipList matching the Primary Key."""
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
        
        match_off = node_get_next_off(base, curr_off, 0)
        if match_off != 0:
            if node_get_key(base, match_off, self.key_size) == key:
                return match_off
        return 0

    def _find_exact_values(self, key, field_values):
        """
        Locates the specific node matching both the Primary Key AND the payload.
        Args:
            field_values: List[values.TaggedValue]
        """
        base = self.arena.base_ptr
        curr_off = self.head_off

        # Set the value accessor once per search operation
        self.value_accessor.set_row(field_values)

        for i in range(MAX_HEIGHT - 1, -1, -1):
            next_off = node_get_next_off(base, curr_off, i)
            while next_off != 0:
                next_key = node_get_key(base, next_off, self.key_size)
                
                # Primary Key check
                if next_key < key:
                    curr_off = next_off
                # PK matches, now compare payload lexicographically
                elif next_key == key:
                    self.node_accessor.set_row(base, next_off)
                    # Advance if the current node's payload is smaller than the target
                    if comparator.compare_rows(self.schema, self.node_accessor, self.value_accessor) < 0:
                        curr_off = next_off
                    else:
                        break # Found position or overshot
                # Overshot PK
                else:
                    break
                
                next_off = node_get_next_off(base, curr_off, i)
            self._update_offsets[i] = curr_off
        
        match_off = node_get_next_off(base, curr_off, 0)
        if match_off != 0:
            if node_get_key(base, match_off, self.key_size) == key:
                self.node_accessor.set_row(base, match_off)
                # Final check for exact payload match
                if comparator.compare_rows(self.schema, self.node_accessor, self.value_accessor) == 0:
                    return match_off
        return 0

    def upsert(self, key, weight, field_values):
        """
        Upserts a row into the MemTable.
        Args:
            field_values: List[values.TaggedValue]
        """
        base = self.arena.base_ptr
        match_off = self._find_exact_values(key, field_values)
        
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
        node_full_sz = key_off + self.key_size + self.schema.memtable_stride
        self._ensure_capacity(node_full_sz, field_values)
        
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
            
        payload_ptr = rffi.ptradd(key_ptr, self.key_size)
        self._pack_into_node(payload_ptr, field_values)

        for i in range(h):
            pred = self._update_offsets[i]
            node_set_next_off(base, new_off, i, node_get_next_off(base, pred, i))
            node_set_next_off(base, pred, i, new_off)

    def _ensure_capacity(self, node_sz, field_values):
        """
        Checks if arenas have enough space for the new node and its blobs.
        Args:
            field_values: List[values.TaggedValue]
        """
        blob_sz = 0
        v_idx = 0
        for i in range(len(self.schema.columns)):
            if i == self.schema.pk_index: continue
            
            # Use TaggedValue fields directly
            val = field_values[v_idx]
            if self.schema.columns[i].field_type == types.TYPE_STRING:
                assert val.tag == values.TAG_STRING
                s = val.str_val
                if len(s) > string_logic.SHORT_STRING_THRESHOLD: 
                    blob_sz += len(s)
            v_idx += 1
            
        if self.arena.offset + node_sz > self.arena.size or \
           self.blob_arena.offset + blob_sz > self.blob_arena.size:
            raise errors.MemTableFullError()

    def _pack_into_node(self, dest_ptr, field_values):
        """
        Serializes TaggedValues into the packed row format in the Arena.
        Args:
            field_values: List[values.TaggedValue]
        """
        allocator = MemTableBlobAllocator(self.blob_arena)
        v_idx = 0
        for i in range(len(self.schema.columns)):
            if i == self.schema.pk_index: continue
            
            val_obj = field_values[v_idx]
            f_type = self.schema.columns[i].field_type
            v_idx += 1
            
            off = self.schema.get_column_offset(i)
            target = rffi.ptradd(dest_ptr, off)
            
            if f_type == types.TYPE_STRING:
                assert val_obj.tag == values.TAG_STRING
                string_logic.pack_and_write_blob(target, val_obj.str_val, allocator)
                
            elif f_type == types.TYPE_F64:
                assert val_obj.tag == values.TAG_FLOAT
                rffi.cast(rffi.DOUBLEP, target)[0] = rffi.cast(rffi.DOUBLE, val_obj.f64)
                
            elif f_type == types.TYPE_U128:
                # U128 non-PK columns are stored as TaggedValue.i64 (lo) 
                assert val_obj.tag == values.TAG_INT
                u128_val = r_uint128(val_obj.i64)
                rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, r_uint64(u128_val))
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(target, 8))[0] = rffi.cast(rffi.ULONGLONG, r_uint64(u128_val >> 64))
                
            else:
                # Standard integers (i8...u64)
                assert val_obj.tag == values.TAG_INT
                rffi.cast(rffi.ULONGLONGP, target)[0] = rffi.cast(rffi.ULONGLONG, val_obj.i64)

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
