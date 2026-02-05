from rpython.rlib import jit
from gnitz.storage.shard import ShardView

class ShardHandle(object):
    _immutable_fields_ = ['view', 'min_key', 'max_key', 'count']

    def __init__(self, filename):
        self.view = ShardView(filename)
        self.count = self.view.count
        if self.count > 0:
            self.min_key = self.view.materialize_key(0)
            self.max_key = self.view.materialize_key(self.count - 1)
        else:
            self.min_key = ""
            self.max_key = ""

    def get_weight(self, idx):
        return self.view.get_weight(idx)

    def materialize_key(self, idx):
        return self.view.materialize_key(idx)

    def materialize_value(self, idx):
        return self.view.materialize_value(idx)

    def close(self):
        self.view.close()

class Spine(object):
    _immutable_fields_ = ['handles[*]', 'min_keys[*]', 'max_keys[*]', 'shard_count']

    def __init__(self, handles):
        self.handles = handles
        self.shard_count = len(handles)
        self.min_keys = [h.min_key for h in handles]
        self.max_keys = [h.max_key for h in handles]

    @jit.elidable
    def lookup_candidate_index(self, key):
        """Finds the index of the shard that could contain 'key'."""
        low = 0
        high = self.shard_count - 1
        ans = -1
        while low <= high:
            mid = (low + high) // 2
            if self.min_keys[mid] <= key:
                ans = mid
                low = mid + 1
            else:
                high = mid - 1
        
        if ans != -1:
            if key <= self.max_keys[ans]:
                return ans
        return -1

    def get_weight_for_key(self, key):
        """Returns weight from the Spine for a specific key."""
        idx = self.lookup_candidate_index(key)
        if idx == -1:
            return 0
        
        shard = self.handles[idx]
        key_idx = shard.view.find_key_index(key)
        if key_idx == -1:
            return 0
            
        return shard.get_weight(key_idx)

    def close_all(self):
        for h in self.handles:
            h.close()
