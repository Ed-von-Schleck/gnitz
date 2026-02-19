# gnitz/storage/tournament_tree.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint

HEAP_NODE = lltype.Struct(
    "HeapNode",
    ("key_low", rffi.ULONGLONG),
    ("key_high", rffi.ULONGLONG),
    ("cursor_idx", rffi.INT),
)


class TournamentTree(object):
    """
    N-way merge heap specialized for 64-bit and 128-bit Primary Keys.
    Optimized for in-place reuse to eliminate malloc-churn.
    """

    def __init__(self, cursors):
        self.cursors = cursors
        self.num_cursors = len(cursors)
        self.heap_size = 0
        self.heap = lltype.malloc(rffi.CArray(HEAP_NODE), self.num_cursors, flavor="raw")

        # Maps Original Cursor Index -> Current Heap Index
        self.pos_map = newlist_hint(self.num_cursors)
        for _ in range(self.num_cursors):
            self.pos_map.append(-1)

        self.rebuild()

    def rebuild(self):
        self.heap_size = 0
        for i in range(self.num_cursors):
            cursor = self.cursors[i]
            if not cursor.is_exhausted():
                idx = self.heap_size
                self.heap_size += 1
                key = cursor.peek_key()
                self.heap[idx].key_low = rffi.cast(rffi.ULONGLONG, r_uint64(key))
                self.heap[idx].key_high = rffi.cast(rffi.ULONGLONG, r_uint64(key >> 64))
                self.heap[idx].cursor_idx = rffi.cast(rffi.INT, i)
                self.pos_map[i] = idx
            else:
                self.pos_map[i] = -1

        if self.heap_size > 1:
            for i in range((self.heap_size // 2) - 1, -1, -1):
                self._sift_down(i)

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
            else:
                break

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
            else:
                break

    def _swap(self, i, j):
        l, h, c = self.heap[i].key_low, self.heap[i].key_high, self.heap[i].cursor_idx
        self.heap[i].key_low = self.heap[j].key_low
        self.heap[i].key_high = self.heap[j].key_high
        self.heap[i].cursor_idx = self.heap[j].cursor_idx
        self.heap[j].key_low = l
        self.heap[j].key_high = h
        self.heap[j].cursor_idx = c

        idx_i = rffi.cast(lltype.Signed, self.heap[i].cursor_idx)
        idx_j = rffi.cast(lltype.Signed, self.heap[j].cursor_idx)
        self.pos_map[idx_i] = i
        self.pos_map[idx_j] = j

    def get_min_key(self):
        if self.heap_size == 0:
            return r_uint128(-1)
        return self._get_key(0)

    def get_all_indices_at_min(self):
        """Returns the original cursor indices currently at the minimum key."""
        results = []
        if self.heap_size == 0:
            return results
        target = self.get_min_key()
        self._collect_equal_indices(0, target, results)
        return results

    def _collect_equal_indices(self, idx, target, results):
        if idx >= self.heap_size:
            return
        if self._get_key(idx) == target:
            c_idx = rffi.cast(lltype.Signed, self.heap[idx].cursor_idx)
            results.append(c_idx)
            self._collect_equal_indices(2 * idx + 1, target, results)
            self._collect_equal_indices(2 * idx + 2, target, results)
            
    def _collect_equal_keys(self, idx, target, results):
        if idx >= self.heap_size:
            return
        if self._get_key(idx) == target:
            c_idx = rffi.cast(lltype.Signed, self.heap[idx].cursor_idx)
            results.append(self.cursors[c_idx])
            self._collect_equal_keys(2 * idx + 1, target, results)
            self._collect_equal_keys(2 * idx + 2, target, results)

    def advance_min_cursors(self, target_key=None):
        """Restored method: Advances all cursors currently at min_key."""
        if target_key is None:
            target_key = self.get_min_key()
        while self.heap_size > 0 and self._get_key(0) == target_key:
            c_idx = rffi.cast(lltype.Signed, self.heap[0].cursor_idx)
            self.advance_cursor_by_index(c_idx)
            
    def get_all_cursors_at_min(self):
        results = []
        if self.heap_size == 0:
            return results
        target = self.get_min_key()
        self._collect_equal_keys(0, target, results)
        return results

    def advance_cursor_by_index(self, cursor_idx):
        # Defend against -1 from bugs or identity failures
        if cursor_idx < 0 or cursor_idx >= self.num_cursors:
            return
            
        heap_idx = self.pos_map[cursor_idx]
        if heap_idx == -1:
            return
            
        cursor = self.cursors[cursor_idx]
        cursor.advance()
        
        if cursor.is_exhausted():
            self.pos_map[cursor_idx] = -1
            last = self.heap_size - 1
            if heap_idx != last:
                last_c_idx = rffi.cast(lltype.Signed, self.heap[last].cursor_idx)
                self.heap[heap_idx].key_low = self.heap[last].key_low
                self.heap[heap_idx].key_high = self.heap[last].key_high
                self.heap[heap_idx].cursor_idx = self.heap[last].cursor_idx
                self.pos_map[last_c_idx] = heap_idx
                self.heap_size -= 1
                self._sift_down(heap_idx)
                self._sift_up(heap_idx)
            else:
                self.heap_size -= 1
        else:
            nk = cursor.peek_key()
            self.heap[heap_idx].key_low = rffi.cast(rffi.ULONGLONG, r_uint64(nk))
            self.heap[heap_idx].key_high = rffi.cast(rffi.ULONGLONG, r_uint64(nk >> 64))
            self._sift_down(heap_idx)
            self._sift_up(heap_idx)

    def is_exhausted(self):
        return self.heap_size == 0

    def close(self):
        if self.heap:
            lltype.free(self.heap, flavor="raw")
            self.heap = lltype.nullptr(rffi.CArray(HEAP_NODE))
