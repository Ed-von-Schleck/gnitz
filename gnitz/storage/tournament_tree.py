from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from gnitz.storage import errors

# CHANGE: entity_id must be ULONGLONG to match the rest of the engine
HEAP_NODE_PTR = lltype.Ptr(lltype.Struct("HeapNode", 
    ("entity_id", rffi.ULONGLONG),
    ("cursor_idx", rffi.INT)
))

class StreamCursor(object):
    def __init__(self, shard_view):
        self.view = shard_view
        self.position = 0
        self.exhausted = False
        self._skip_ghosts()
    
    def _skip_ghosts(self):
        while self.position < self.view.count:
            if self.view.get_weight(self.position) != 0:
                return
            self.position += 1
        self.exhausted = True

    def peek_entity_id(self):
        if self.exhausted:
            # Fix: r_uint64(-1) avoids prebuilt long error
            return r_uint64(-1)
        return self.view.get_entity_id(self.position)
    
    def advance(self):
        if self.exhausted: return
        self.position += 1
        self._skip_ghosts()
    
    def is_exhausted(self):
        return self.exhausted
    
    def get_current_index(self):
        return self.position

class TournamentTree(object):
    def __init__(self, cursors):
        self.cursors = cursors
        self.num_cursors = len(cursors)
        self.heap_size = 0
        self.heap = lltype.malloc(rffi.CArray(HEAP_NODE_PTR.TO), self.num_cursors, flavor='raw')
        
        for i in range(self.num_cursors):
            if not cursors[i].is_exhausted():
                self._heap_push(cursors[i].peek_entity_id(), i)

    def _heap_push(self, eid, c_idx):
        idx = self.heap_size
        self.heap_size += 1
        self.heap[idx].entity_id = eid
        self.heap[idx].cursor_idx = rffi.cast(rffi.INT, c_idx)
        self._sift_up(idx)

    def _sift_up(self, idx):
        while idx > 0:
            parent = (idx - 1) // 2
            if self.heap[idx].entity_id < self.heap[parent].entity_id:
                eid, c_idx = self.heap[idx].entity_id, self.heap[idx].cursor_idx
                self.heap[idx].entity_id = self.heap[parent].entity_id
                self.heap[idx].cursor_idx = self.heap[parent].cursor_idx
                self.heap[parent].entity_id = eid
                self.heap[parent].cursor_idx = c_idx
                idx = parent
            else: break

    def _sift_down(self, idx):
        while True:
            smallest = idx
            left = 2 * idx + 1
            right = 2 * idx + 2
            if left < self.heap_size and self.heap[left].entity_id < self.heap[smallest].entity_id:
                smallest = left
            if right < self.heap_size and self.heap[right].entity_id < self.heap[smallest].entity_id:
                smallest = right
            if smallest != idx:
                eid, c_idx = self.heap[idx].entity_id, self.heap[idx].cursor_idx
                self.heap[idx].entity_id = self.heap[smallest].entity_id
                self.heap[idx].cursor_idx = self.heap[smallest].cursor_idx
                self.heap[smallest].entity_id = eid
                self.heap[smallest].cursor_idx = c_idx
                idx = smallest
            else: break

    def get_min_entity_id(self):
        if self.heap_size == 0:
            # Fix: r_uint64(-1) avoids prebuilt long error
            return r_uint64(-1)
        return self.heap[0].entity_id
    
    def get_all_cursors_at_min(self):
        if self.heap_size == 0: return []
        min_eid = self.heap[0].entity_id
        res = []
        for i in range(self.heap_size):
            if self.heap[i].entity_id == min_eid:
                res.append(rffi.cast(lltype.Signed, self.heap[i].cursor_idx))
        return res
    
    def advance_min_cursors(self):
        if self.heap_size == 0: return
        min_eid = self.heap[0].entity_id
        to_advance = []
        i = 0
        while i < self.heap_size:
            if self.heap[i].entity_id == min_eid:
                to_advance.append(rffi.cast(lltype.Signed, self.heap[i].cursor_idx))
                self.heap_size -= 1
                if i < self.heap_size:
                    self.heap[i].entity_id = self.heap[self.heap_size].entity_id
                    self.heap[i].cursor_idx = self.heap[self.heap_size].cursor_idx
                    self._sift_down(i)
                    continue
            i += 1
        for c_idx in to_advance:
            self.cursors[c_idx].advance()
            if not self.cursors[c_idx].is_exhausted():
                self._heap_push(self.cursors[c_idx].peek_entity_id(), c_idx)

    def is_exhausted(self):
        return self.heap_size == 0

    def close(self):
        if self.heap:
            lltype.free(self.heap, flavor='raw')
            self.heap = lltype.nullptr(rffi.CArray(HEAP_NODE_PTR.TO))
