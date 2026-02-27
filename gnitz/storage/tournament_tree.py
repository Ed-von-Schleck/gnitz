# gnitz/storage/tournament_tree.py

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib import jit
from gnitz.core import comparator as core_comparator

# Struct uses two 64-bit words for the key to avoid r_uint128 struct-alignment
# bugs and SIGSEGV during property assignment in the translated C code.
HEAP_NODE = lltype.Struct(
    "HeapNode",
    ("key_low", rffi.ULONGLONG),
    ("key_high", rffi.ULONGLONG),
    ("cursor_idx", rffi.INT),
)


class TournamentTree(object):
    """
    N-way merge heap specialized for (Primary Key, Payload) sorting.

    Optimized for in-place reuse to eliminate malloc-churn. Making this heap
    payload-aware enables UnifiedCursor to prune branches of the heap during
    equivalence collection, reducing complexity from O(Sources^2) to O(Sources log Sources).
    """

    _immutable_fields_ = ["schema", "cursors", "num_cursors"]

    def __init__(self, cursors, schema):
        self.schema = schema
        self.cursors = cursors
        self.num_cursors = len(cursors)
        self.heap_size = 0
        self.heap = lltype.malloc(rffi.CArray(HEAP_NODE), self.num_cursors, flavor="raw")

        # Maps Original Cursor Index -> Current Heap Index
        # Initialized via newlist_hint to prevent mr-poisoning.
        self.pos_map = newlist_hint(self.num_cursors)
        for _ in range(self.num_cursors):
            self.pos_map.append(-1)

        self.rebuild()

    def rebuild(self):
        """Full rebuild of the heap from current cursor positions."""
        self.heap_size = 0
        for i in range(self.num_cursors):
            cursor = self.cursors[i]
            if cursor.is_valid():
                idx = self.heap_size
                self.heap_size += 1
                key = cursor.peek_key()

                # CRITICAL: Split u128 into u64 components to satisfy alignment.
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
        """Reconstructs u128 PK from lo/hi storage for comparison."""
        low = r_uint128(r_uint64(self.heap[idx].key_low))
        high = r_uint128(r_uint64(self.heap[idx].key_high))
        return (high << 64) | low

    @jit.unroll_safe
    def _compare_nodes(self, i, j):
        """
        Primary comparison logic: (PrimaryKey, Payload).
        Returns < 0 if node i is smaller, > 0 if larger, 0 if identical.
        """
        # 1. Compare Primary Keys
        hi_i = r_uint64(self.heap[i].key_high)
        hi_j = r_uint64(self.heap[j].key_high)
        if hi_i < hi_j:
            return -1
        if hi_i > hi_j:
            return 1

        lo_i = r_uint64(self.heap[i].key_low)
        lo_j = r_uint64(self.heap[j].key_low)
        if lo_i < lo_j:
            return -1
        if lo_i > lo_j:
            return 1

        # 2. Compare Payloads (Keys are identical)
        # Extract original cursors using their indices.
        c_idx_i = rffi.cast(lltype.Signed, self.heap[i].cursor_idx)
        c_idx_j = rffi.cast(lltype.Signed, self.heap[j].cursor_idx)

        acc_i = self.cursors[c_idx_i].get_accessor()
        acc_j = self.cursors[c_idx_j].get_accessor()

        return core_comparator.compare_rows(self.schema, acc_i, acc_j)

    def _sift_up(self, idx):
        while idx > 0:
            parent = (idx - 1) // 2
            if self._compare_nodes(idx, parent) < 0:
                self._swap(idx, parent)
                idx = parent
            else:
                break

    def _sift_down(self, idx):
        while True:
            smallest = idx
            left = 2 * idx + 1
            right = 2 * idx + 2
            if left < self.heap_size and self._compare_nodes(left, smallest) < 0:
                smallest = left
            if right < self.heap_size and self._compare_nodes(right, smallest) < 0:
                smallest = right
            if smallest != idx:
                self._swap(idx, smallest)
                idx = smallest
            else:
                break

    def _swap(self, i, j):
        # Swap logical contents
        l, h, c = self.heap[i].key_low, self.heap[i].key_high, self.heap[i].cursor_idx
        self.heap[i].key_low = self.heap[j].key_low
        self.heap[i].key_high = self.heap[j].key_high
        self.heap[i].cursor_idx = self.heap[j].cursor_idx
        self.heap[j].key_low = l
        self.heap[j].key_high = h
        self.heap[j].cursor_idx = c

        # Update position map
        idx_i = rffi.cast(lltype.Signed, self.heap[i].cursor_idx)
        idx_j = rffi.cast(lltype.Signed, self.heap[j].cursor_idx)
        self.pos_map[idx_i] = i
        self.pos_map[idx_j] = j

    def get_min_key(self):
        """Returns the Primary Key of the global minimum node."""
        if self.heap_size == 0:
            return r_uint128(-1)
        return self._get_key(0)

    def get_all_indices_at_min(self):
        """
        Returns all original cursor indices currently matching the global
        minimum (PrimaryKey, Payload).

        LEVERAGES HEAP PRUNING: Because the heap is sorted by (PK, Payload),
        once we encounter a node > root, we prune the entire branch.
        """
        results = newlist_hint(self.num_cursors)
        if self.heap_size == 0:
            return results

        # Node 0 is always the global minimum.
        self._collect_equal_indices(0, results)
        return results

    def _collect_equal_indices(self, idx, results):
        if idx >= self.heap_size:
            return

        # Compare current node to the root (the global minimum).
        cmp_res = self._compare_nodes(idx, 0)

        if cmp_res == 0:
            # Exact match (PK and Payload).
            c_idx = rffi.cast(lltype.Signed, self.heap[idx].cursor_idx)
            results.append(c_idx)
            # Recurse to children.
            self._collect_equal_indices(2 * idx + 1, results)
            self._collect_equal_indices(2 * idx + 2, results)
        # If cmp_res > 0, the node is strictly larger than the minimum,
        # so all its descendants must also be larger. PRUNE.

    def advance_cursor_by_index(self, cursor_idx):
        """
        Advances a specific cursor and updates its position in the heap.
        If the cursor is exhausted, it is removed from the heap.
        """
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
                # Restore heap property from the replacement node
                self._sift_down(heap_idx)
                self._sift_up(heap_idx)
            else:
                self.heap_size -= 1
        else:
            # Update key and re-sift
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
