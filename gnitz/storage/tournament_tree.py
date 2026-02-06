from gnitz.storage import errors

class StreamCursor(object):
    """
    Wraps an ECSShardView and tracks current position.
    Provides iterator interface over entities in a shard.
    """
    def __init__(self, shard_view):
        self.view = shard_view
        self.position = 0
        self.exhausted = False
        
        if self.view.count == 0:
            self.exhausted = True
    
    def peek_entity_id(self):
        """
        Returns the next entity ID without advancing the cursor.
        
        Returns:
            Entity ID (i64) or -1 if exhausted
        """
        if self.exhausted:
            return -1
        
        return self.view.get_entity_id(self.position)
    
    def advance(self):
        """
        Advances the cursor to the next entity.
        Marks cursor as exhausted if no more entities.
        """
        if self.exhausted:
            return
        
        self.position += 1
        
        if self.position >= self.view.count:
            self.exhausted = True
    
    def is_exhausted(self):
        """Returns True if cursor has no more entities."""
        return self.exhausted
    
    def get_current_index(self):
        """Returns the current position index."""
        return self.position


class TournamentTree(object):
    """
    Maintains a tournament tree (min-heap) for N-way merge of sorted streams.
    Each stream is represented by a StreamCursor.
    """
    def __init__(self, cursors):
        """
        Initializes tournament tree with the given cursors.
        
        Args:
            cursors: List of StreamCursor objects
        """
        self.cursors = cursors
        self.num_cursors = len(cursors)
        
        # Heap of (entity_id, cursor_index)
        # We maintain this manually as a binary heap
        self.heap = []
        
        # Build initial heap from all non-exhausted cursors
        for i in range(self.num_cursors):
            if not cursors[i].is_exhausted():
                entity_id = cursors[i].peek_entity_id()
                self._heap_insert(entity_id, i)
    
    def _heap_insert(self, entity_id, cursor_idx):
        """Inserts (entity_id, cursor_idx) into the min-heap."""
        self.heap.append((entity_id, cursor_idx))
        self._sift_up(len(self.heap) - 1)
    
    def _sift_up(self, idx):
        """Sifts element at idx up to maintain heap property."""
        while idx > 0:
            parent = (idx - 1) // 2
            if self.heap[idx][0] < self.heap[parent][0]:
                # Swap with parent
                temp = self.heap[idx]
                self.heap[idx] = self.heap[parent]
                self.heap[parent] = temp
                idx = parent
            else:
                break
    
    def _sift_down(self, idx):
        """Sifts element at idx down to maintain heap property."""
        while True:
            smallest = idx
            left = 2 * idx + 1
            right = 2 * idx + 2
            
            if left < len(self.heap) and self.heap[left][0] < self.heap[smallest][0]:
                smallest = left
            
            if right < len(self.heap) and self.heap[right][0] < self.heap[smallest][0]:
                smallest = right
            
            if smallest != idx:
                # Swap
                temp = self.heap[idx]
                self.heap[idx] = self.heap[smallest]
                self.heap[smallest] = temp
                idx = smallest
            else:
                break
    
    def _extract_min(self):
        """
        Extracts the minimum (entity_id, cursor_idx) from heap.
        
        Returns:
            Tuple of (entity_id, cursor_idx) or None if heap is empty
        """
        if len(self.heap) == 0:
            return None
        
        if len(self.heap) == 1:
            return self.heap.pop()
        
        # Replace root with last element and sift down
        min_elem = self.heap[0]
        self.heap[0] = self.heap.pop()
        self._sift_down(0)
        
        return min_elem
    
    def get_min_entity_id(self):
        """
        Returns the smallest entity ID across all streams.
        
        Returns:
            Entity ID (i64) or -1 if all streams exhausted
        """
        if len(self.heap) == 0:
            return -1
        
        return self.heap[0][0]
    
    def get_all_cursors_at_min(self):
        """
        Returns all cursor indices that are currently at the minimum entity ID.
        This handles the case where the same entity appears in multiple shards.
        
        Returns:
            List of cursor indices
        """
        if len(self.heap) == 0:
            return []
        
        min_entity_id = self.heap[0][0]
        result = []
        
        # Collect all cursors at min entity ID
        # We need to check the entire heap since multiple cursors might have same entity
        for entity_id, cursor_idx in self.heap:
            if entity_id == min_entity_id:
                result.append(cursor_idx)
        
        return result
    
    def advance_min_cursors(self):
        """
        Advances all cursors that are at the minimum entity ID and rebuilds heap.
        This is called after processing the current minimum entity.
        """
        if len(self.heap) == 0:
            return
        
        min_entity_id = self.heap[0][0]
        
        # Find all cursors at min and advance them
        cursors_to_advance = []
        for entity_id, cursor_idx in self.heap:
            if entity_id == min_entity_id:
                cursors_to_advance.append(cursor_idx)
        
        # Remove these cursors from heap
        new_heap = []
        for entity_id, cursor_idx in self.heap:
            if entity_id != min_entity_id:
                new_heap.append((entity_id, cursor_idx))
        
        self.heap = new_heap
        
        # Rebuild heap property (simple approach: re-heapify)
        for i in range(len(self.heap) // 2 - 1, -1, -1):
            self._sift_down(i)
        
        # Advance cursors and re-insert if not exhausted
        for cursor_idx in cursors_to_advance:
            self.cursors[cursor_idx].advance()
            
            if not self.cursors[cursor_idx].is_exhausted():
                new_entity_id = self.cursors[cursor_idx].peek_entity_id()
                self._heap_insert(new_entity_id, cursor_idx)
    
    def is_exhausted(self):
        """Returns True if all streams are exhausted."""
        return len(self.heap) == 0
