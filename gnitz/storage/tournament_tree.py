from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.storage.cursor import ShardCursor # New Import

# StreamCursor class removed from here

# Use a struct for the heap nodes to ensure 8-byte alignment for u64 parts
# and avoid GC-heap allocation of r_uint128 objects.
HEAP_NODE = lltype.Struct("HeapNode", 
    ("key_low", rffi.ULONGLONG),
    ("key_high", rffi.ULONGLONG),
    ("cursor_idx", rffi.INT)
)

class TournamentTree(object):
    """N-way merge heap specialized for 64-bit and 128-bit Primary Keys."""
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
        self.heap[idx].key_low = rffi.cast(rffi.ULONGLONG, r_uint64(key))
        self.heap[idx].key_high = rffi.cast(rffi.ULONGLONG, r_uint64(key >> 64))
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

    def get_all_cursors_at_min(self):
        """Collects all cursors sharing the current minimum Primary Key."""
        results = []
        if self.heap_size == 0: 
            return results
        
        target = self.get_min_key()
        self._collect_equal_keys(0, target, results)
        return results

    def _collect_equal_keys(self, idx, target, results):
        if idx >= self.heap_size:
            return
        if self._get_key(idx) == target:
            c_idx = rffi.cast(lltype.Signed, self.heap[idx].cursor_idx)
            results.append(self.cursors[c_idx])
            self._collect_equal_keys(2 * idx + 1, target, results)
            self._collect_equal_keys(2 * idx + 2, target, results)
    
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
                self.heap[0].key_low = rffi.cast(rffi.ULONGLONG, r_uint64(nk))
                self.heap[0].key_high = rffi.cast(rffi.ULONGLONG, r_uint64(nk >> 64))
                self._sift_down(0)

    def is_exhausted(self): return self.heap_size == 0
    def close(self):
        if self.heap:
            lltype.free(self.heap, flavor='raw')
