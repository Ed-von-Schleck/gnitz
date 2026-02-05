from rpython.rlib.rrandom import Random
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.jit import unroll_safe
from gnitz.storage import arena, errors, writer

MAX_HEIGHT = 16

# ============================================================================
# Static Helper Functions (Raw Memory Access)
# ============================================================================

def get_varint_size(val):
    if val == 0: return 1
    res = 0
    temp = val
    while temp > 0:
        res += 1
        temp >>= 7
    return res

def write_varint(ptr, val):
    idx = 0
    temp = val
    if temp == 0:
        ptr[0] = chr(0)
        return 1
    while temp >= 0x80:
        ptr[idx] = chr((temp & 0x7f) | 0x80)
        temp >>= 7
        idx += 1
    ptr[idx] = chr(temp & 0x7f)
    return idx + 1

def calculate_node_size(height, key_len, val_len):
    # Layout: weight(8) + height(1) + pad(3) + next_pointers(h*4) + key + val
    size = 12 + (height * 4)
    size += get_varint_size(key_len) + key_len
    size += get_varint_size(val_len) + val_len
    return size

@unroll_safe
def node_get_next_off(base_ptr, node_off, level):
    """Reads a 32-bit pointer offset from the SkipList tower."""
    ptr = rffi.ptradd(base_ptr, node_off + 12)
    next_ptr = rffi.cast(rffi.UINTP, ptr)
    return rffi.cast(lltype.Signed, next_ptr[level])

def node_set_next_off(base_ptr, node_off, level, target_off):
    """Writes a 32-bit pointer offset into the SkipList tower."""
    ptr = rffi.ptradd(base_ptr, node_off + 12)
    next_ptr = rffi.cast(rffi.UINTP, ptr)
    next_ptr[level] = rffi.cast(rffi.UINT, target_off)

def node_get_weight(base_ptr, node_off):
    ptr = rffi.ptradd(base_ptr, node_off)
    return rffi.cast(rffi.LONGLONGP, ptr)[0]

def node_set_weight(base_ptr, node_off, weight):
    ptr = rffi.ptradd(base_ptr, node_off)
    rffi.cast(rffi.LONGLONGP, ptr)[0] = rffi.cast(rffi.LONGLONG, weight)

def node_compare_key(base_ptr, node_off, search_key):
    """
    Zero-allocation key comparison. Compares the raw bytes inside
    the Arena directly against the search_key string.
    """
    height = ord(base_ptr[node_off + 8])
    p = rffi.ptradd(base_ptr, node_off + 12 + (height * 4))
    
    # Decode key length from varint
    res = 0
    shift = 0
    idx = 0
    while True:
        b = ord(p[idx])
        res |= (b & 0x7f) << shift
        idx += 1
        if not (b & 0x80): break
        shift += 7
    
    node_key_len = res
    search_key_len = len(search_key)
    min_len = node_key_len if node_key_len < search_key_len else search_key_len
    
    # Byte-by-byte comparison
    for i in range(min_len):
        c1 = p[idx + i]
        c2 = search_key[i]
        if c1 < c2: return -1
        if c1 > c2: return 1
    
    if node_key_len < search_key_len: return -1
    if node_key_len > search_key_len: return 1
    return 0

def node_get_payload_info(base_ptr, node_off):
    """
    Extracts raw pointers and lengths for transmutation.
    Crucial for avoiding intermediate RPython strings during flush.
    """
    height = ord(base_ptr[node_off + 8])
    p = rffi.ptradd(base_ptr, node_off + 12 + (height * 4))
    
    # Decode key length
    res = 0
    shift = 0
    idx = 0
    while True:
        b = ord(p[idx])
        res |= (b & 0x7f) << shift
        idx += 1
        if not (b & 0x80): break
        shift += 7
    key_len = res
    key_ptr = rffi.ptradd(p, idx)
    
    idx += key_len
    # Decode val length
    res = 0
    shift = 0
    while True:
        b = ord(p[idx])
        res |= (b & 0x7f) << shift
        idx += 1
        if not (b & 0x80): break
        shift += 7
    val_len = res
    val_ptr = rffi.ptradd(p, idx)
    
    return key_ptr, key_len, val_ptr, val_len

def node_get_key_str(base_ptr, node_off):
    """Materializes key into RPython string. Used for testing/non-hot paths."""
    ptr, length, _, _ = node_get_payload_info(base_ptr, node_off)
    return rffi.charpsize2str(ptr, length)

def node_get_val_str(base_ptr, node_off):
    """Materializes value into RPython string. Used for testing/non-hot paths."""
    _, _, ptr, length = node_get_payload_info(base_ptr, node_off)
    return rffi.charpsize2str(ptr, length)

# ============================================================================
# MemTable Implementation
# ============================================================================

class MemTable(object):
    _immutable_fields_ = ['arena', 'arena_size', 'head_off', 'threshold_bytes']

    def __init__(self, arena_size, threshold_percent=90):
        self.arena = arena.Arena(arena_size)
        self.arena_size = arena_size
        self.threshold_bytes = (arena_size * threshold_percent) / 100
        self.rng = Random(1234)
        self._update_offsets = [0] * MAX_HEIGHT
        
        # Offset 0 safeguard: Ensure valid pointers are never 0
        self.arena.alloc(8) 
        
        # Initialize Head Node
        h_sz = calculate_node_size(MAX_HEIGHT, 0, 0)
        ptr = self.arena.alloc(h_sz)
        self.head_off = rffi.cast(lltype.Signed, ptr) - rffi.cast(lltype.Signed, self.arena.base_ptr)
        
        ptr[8] = chr(MAX_HEIGHT)
        node_set_weight(self.arena.base_ptr, self.head_off, 0)
        for i in range(MAX_HEIGHT):
            node_set_next_off(self.arena.base_ptr, self.head_off, i, 0)
        
        # Write 0-length descriptors for key/val in head
        p = rffi.ptradd(ptr, 12 + (MAX_HEIGHT * 4))
        p[0] = chr(0) 
        p[1] = chr(0) 

    def is_full(self):
        return self.arena.offset > self.threshold_bytes

    def upsert(self, key, value, weight):
        """Standard SkipList Upsert with Algebraic Coalescing."""
        base = self.arena.base_ptr
        curr_off = self.head_off
        
        # Find predecessors
        for i in range(MAX_HEIGHT - 1, -1, -1):
            next_off = node_get_next_off(base, curr_off, i)
            while next_off != 0:
                if node_compare_key(base, next_off, key) < 0:
                    curr_off = next_off
                    next_off = node_get_next_off(base, curr_off, i)
                else:
                    break
            self._update_offsets[i] = curr_off

        # Algebraic Coalescing on hit
        next_off = node_get_next_off(base, curr_off, 0)
        if next_off != 0 and node_compare_key(base, next_off, key) == 0:
            node_set_weight(base, next_off, node_get_weight(base, next_off) + weight)
            return

        # New Node Insertion
        h = 1
        while h < MAX_HEIGHT and (self.rng.genrand32() & 1): h += 1
        
        node_sz = calculate_node_size(h, len(key), len(value))
        new_ptr = self.arena.alloc(node_sz)
        new_off = rffi.cast(lltype.Signed, new_ptr) - rffi.cast(lltype.Signed, base)
        
        node_set_weight(base, new_off, weight)
        new_ptr[8] = chr(h)
        
        # Write Key/Value payloads
        p = rffi.ptradd(new_ptr, 12 + (h * 4))
        idx = write_varint(p, len(key))
        for i in range(len(key)): p[idx + i] = key[i]
        idx += len(key)
        idx += write_varint(rffi.ptradd(p, idx), len(value))
        for i in range(len(value)): p[idx + i] = value[i]

        # Update pointers
        for i in range(h):
            pred_off = self._update_offsets[i]
            node_set_next_off(base, new_off, i, node_get_next_off(base, pred_off, i))
            node_set_next_off(base, pred_off, i, new_off)

    def flush(self, filename):
        """
        Transmutation Pipeline: Unzips row-oriented SkipList directly 
        into columnar Shard format via raw pointer passing.
        """
        sw = writer.ShardWriter()
        base = self.arena.base_ptr
        # Iterate Level 0 (sorted order)
        curr_off = node_get_next_off(base, self.head_off, 0)
        while curr_off != 0:
            w = node_get_weight(base, curr_off)
            if w != 0:
                k_ptr, k_len, v_ptr, v_len = node_get_payload_info(base, curr_off)
                # Pass raw pointers to bypass GC
                sw.add_entry_raw(k_ptr, k_len, v_ptr, v_len, w)
            curr_off = node_get_next_off(base, curr_off, 0)
        sw.finalize(filename)

    def free(self):
        self.arena.free()

class MemTableManager(object):
    def __init__(self, memtable_capacity):
        self.capacity = memtable_capacity
        self.active_table = MemTable(self.capacity)

    def put(self, key, value, weight):
        self.active_table.upsert(key, value, weight)

    def flush_and_rotate(self, filename):
        self.active_table.flush(filename)
        self.active_table.free()
        self.active_table = MemTable(self.capacity)

    def close(self):
        if self.active_table:
            self.active_table.free()
