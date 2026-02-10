from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long
from rpython.rlib.jit import unrolling_iterable
from gnitz.core import types

class StreamCursor(object):
    _immutable_fields_ = ['view', 'schema', 'is_u128']

    def __init__(self, shard_view):
        self.view = shard_view
        self.schema = shard_view.schema
        self.is_u128 = self.schema.get_pk_column().field_type == types.TYPE_U128
        self.position = 0
        self.exhausted = False
        self._skip_ghosts()
    
    def _skip_ghosts(self):
        while self.position < self.view.count:
            if self.view.get_weight(self.position) != 0:
                return
            self.position += 1
        self.exhausted = True

    def peek_key(self):
        if self.exhausted:
            return r_uint128(-1)
        if self.is_u128:
            return self.view.get_pk_u128(self.position)
        return r_uint128(self.view.get_pk_u64(self.position))
    
    def advance(self):
        if self.exhausted: return
        self.position += 1
        self._skip_ghosts()
    
    def is_exhausted(self):
        return self.exhausted

HEAP_NODE = lltype.Struct("HeapNode", 
    ("key_low", rffi.ULONGLONG),
    ("key_high", rffi.ULONGLONG),
    ("cursor_idx", rffi.INT)
)

class TournamentTree(object):
    def __init__(self, cursors):
        self.cursors = cursors
        self.num_cursors = len(cursors)
        self.heap_size = 0
        self.heap = lltype.malloc(rffi.CArray(HEAP_NODE), self.num_cursors, flavor='raw')
        
        for i in range(self.num_cursors):
            if not cursors[i].is_exhausted():
                self._heap_push(cursors[i].peek_key(), i)

    def _heap_push(self, key, c_idx):
        idx = self.heap_size
        self.heap_size += 1
        self.heap[idx].key_low = rffi.cast(rffi.ULONGLONG, key & 0xFFFFFFFFFFFFFFFF)
        self.heap[idx].key_high = rffi.cast(rffi.ULONGLONG, key >> 64)
        self.heap[idx].cursor_idx = rffi.cast(rffi.INT, c_idx)
        self._sift_up(idx)

    def _get_key(self, idx):
        low = r_uint128(self.heap[idx].key_low)
        high = r_uint128(self.heap[idx].key_high)
        return (high << 64) | low

    def _sift_up(self, idx):
        while idx > 0:
            parent = (idx - 1) // 2
            if self._get_key(idx) < self._get_key(parent):
                self._swap(idx, parent)
                idx = parent
            else: break

    def _sift_down(self, idx):
        while True:
            smallest = idx
            left = 2 * idx + 1
            right = 2 * idx + 2
            if left < self.heap_size and self._get_key(left) < self._get_key(smallest):
                smallest = left
            if right < self.heap_size and self._get_key(right) < self._get_key(smallest):
                smallest = right
            if smallest != idx:
                self._swap(idx, smallest)
                idx = smallest
            else: break

    def _swap(self, i, j):
        l, h, c = self.heap[i].key_low, self.heap[i].key_high, self.heap[i].cursor_idx
        self.heap[i].key_low = self.heap[j].key_low
        self.heap[i].key_high = self.heap[j].key_high
        self.heap[i].cursor_idx = self.heap[j].cursor_idx
        self.heap[j].key_low = l
        self.heap[j].key_high = h
        self.heap[j].cursor_idx = c

    def get_min_key(self):
        if self.heap_size == 0: return r_uint128(-1)
        return self._get_key(0)

    # Backward compatibility for tests
    def get_min_entity_id(self):
        return self.get_min_key()

    def get_all_cursors_at_min(self):
        """ Returns all cursors that point to the current minimum key. """
        results = []
        if self.heap_size == 0: return results
        target = self.get_min_key()
        # In a min-heap, we have to check multiple nodes if keys are equal
        for i in range(self.num_cursors):
            if not self.cursors[i].is_exhausted() and self.cursors[i].peek_key() == target:
                results.append(self.cursors[i])
        return results
    
    def advance_min_cursors(self, target_key=None):
        if target_key is None: target_key = self.get_min_key()
        while self.heap_size > 0 and self._get_key(0) == target_key:
            c_idx = rffi.cast(lltype.Signed, self.heap[0].cursor_idx)
            self.cursors[c_idx].advance()
            
            if self.cursors[c_idx].is_exhausted():
                last = self.heap_size - 1
                self.heap[0].key_low = self.heap[last].key_low
                self.heap[0].key_high = self.heap[last].key_high
                self.heap[0].cursor_idx = self.heap[last].cursor_idx
                self.heap_size -= 1
                if self.heap_size > 0: self._sift_down(0)
            else:
                nk = self.cursors[c_idx].peek_key()
                self.heap[0].key_low = rffi.cast(rffi.ULONGLONG, nk & 0xFFFFFFFFFFFFFFFF)
                self.heap[0].key_high = rffi.cast(rffi.ULONGLONG, nk >> 64)
                self._sift_down(0)

    def is_exhausted(self): return self.heap_size == 0
    def close(self):
        if self.heap:
            lltype.free(self.heap, flavor='raw')
