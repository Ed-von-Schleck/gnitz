from rpython.rlib.rrandom import Random
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import arena, errors, writer

MAX_HEIGHT = 16

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

class MemTableNode(object):
    _immutable_fields_ = ['ptr', 'arena_base']
    def __init__(self, ptr, arena_base):
        self.ptr = ptr
        self.arena_base = arena_base

    def get_weight(self):
        return rffi.cast(rffi.LONGLONGP, self.ptr)[0]
    
    def set_weight(self, val):
        rffi.cast(rffi.LONGLONGP, self.ptr)[0] = rffi.cast(rffi.LONGLONG, val)
    
    def get_height(self):
        return ord(self.ptr[8])
    
    def set_height(self, h):
        self.ptr[8] = chr(h)
    
    def get_next_offset(self, level):
        next_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(self.ptr, 12))
        return rffi.cast(lltype.Signed, next_ptr[level])
    
    def set_next_offset(self, level, offset):
        next_ptr = rffi.cast(rffi.UINTP, rffi.ptradd(self.ptr, 12))
        next_ptr[level] = rffi.cast(rffi.UINT, offset)

    def _get_payload_ptr(self):
        return rffi.ptradd(self.ptr, 12 + (self.get_height() * 4))

    def _decode_string(self, p, start_idx):
        res = 0
        shift = 0
        idx = start_idx
        while True:
            b = ord(p[idx])
            res |= (b & 0x7f) << shift
            idx += 1
            if not (b & 0x80): break
            shift += 7
        
        length = res
        chars = [chr(0)] * length
        for i in range(length):
            chars[i] = p[idx + i]
        return "".join(chars), idx + length

    def get_key(self):
        p = self._get_payload_ptr()
        s, _ = self._decode_string(p, 0)
        return s

    def get_value(self):
        p = self._get_payload_ptr()
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
        val_start_idx = idx + key_len
        s, _ = self._decode_string(p, val_start_idx)
        return s

    def set_payload(self, key, value):
        p = self._get_payload_ptr()
        idx = write_varint(p, len(key))
        for i in range(len(key)):
            p[idx + i] = key[i]
        idx += len(key)
        idx += write_varint(rffi.ptradd(p, idx), len(value))
        for i in range(len(value)):
            p[idx + i] = value[i]

def calculate_node_size(height, key_len, val_len):
    size = 12 + (height * 4)
    size += get_varint_size(key_len) + key_len
    size += get_varint_size(val_len) + val_len
    return size

class MemTable(object):
    def __init__(self, arena_size):
        self.arena = arena.Arena(arena_size)
        self.arena_size = arena_size
        self.rng = Random(1234)
        self.arena.alloc(8)
        
        head_size = calculate_node_size(MAX_HEIGHT, 0, 0)
        self.head_ptr = self.arena.alloc(head_size)
        self.head = MemTableNode(self.head_ptr, self.arena.base_ptr)
        self.head.set_height(MAX_HEIGHT)
        for i in range(MAX_HEIGHT):
            self.head.set_next_offset(i, 0)
        self.head.set_payload("", "")

    def get_usage(self):
        return self.arena.offset

    def get_capacity(self):
        return self.arena_size

    def _generate_height(self):
        h = 1
        while h < MAX_HEIGHT and (self.rng.genrand32() & 1):
            h += 1
        return h

    def _get_node_at(self, offset):
        if offset == 0: return None
        return MemTableNode(rffi.ptradd(self.arena.base_ptr, offset), self.arena.base_ptr)

    def _get_offset(self, ptr):
        return rffi.cast(lltype.Signed, ptr) - rffi.cast(lltype.Signed, self.arena.base_ptr)

    def upsert(self, key, value, weight):
        update_offsets = [0] * MAX_HEIGHT
        curr = self.head
        
        for i in range(MAX_HEIGHT - 1, -1, -1):
            next_off = curr.get_next_offset(i)
            while next_off != 0:
                next_node = self._get_node_at(next_off)
                if next_node.get_key() < key:
                    curr = next_node
                    next_off = curr.get_next_offset(i)
                else:
                    break
            update_offsets[i] = self._get_offset(curr.ptr)

        next_off = curr.get_next_offset(0)
        if next_off != 0:
            existing = self._get_node_at(next_off)
            if existing.get_key() == key:
                existing.set_weight(existing.get_weight() + weight)
                return

        height = self._generate_height()
        node_size = calculate_node_size(height, len(key), len(value))
        new_ptr = self.arena.alloc(node_size)
        new_node = MemTableNode(new_ptr, self.arena.base_ptr)
        new_node.set_height(height)
        new_node.set_weight(weight)
        new_node.set_payload(key, value)
        
        new_off = self._get_offset(new_ptr)
        for i in range(height):
            pred = self._get_node_at(update_offsets[i])
            new_node.set_next_offset(i, pred.get_next_offset(i))
            pred.set_next_offset(i, new_off)

    def flush(self, filename):
        sw = writer.ShardWriter()
        curr_off = self.head.get_next_offset(0)
        while curr_off != 0:
            node = self._get_node_at(curr_off)
            if node.get_weight() != 0:
                sw.add_entry(node.get_key(), node.get_value(), node.get_weight())
            curr_off = node.get_next_offset(0)
        sw.finalize(filename)

    def free(self):
        self.arena.free()

class MemTableManager(object):
    def __init__(self, memtable_capacity):
        self.capacity = memtable_capacity
        self.active_table = MemTable(self.capacity)

    def put(self, key, value, weight):
        self.active_table.upsert(key, value, weight)

    def is_full(self, threshold=0.9):
        usage = float(self.active_table.get_usage())
        limit = float(self.active_table.get_capacity()) * threshold
        return usage > limit

    def flush_and_rotate(self, filename):
        self.active_table.flush(filename)
        self.active_table.free()
        self.active_table = MemTable(self.capacity)

    def close(self):
        if self.active_table:
            self.active_table.free()
