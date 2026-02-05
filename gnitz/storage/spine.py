from rpython.rlib import jit
from gnitz.storage.shard import ShardView

class ShardHandle(object):
    """
    Wraps a ShardView with its key-range metadata.
    This allows the Spine to search key ranges without touching the heaps.
    """
    _immutable_fields_ = ['view', 'min_key', 'max_key', 'count']

    def __init__(self, filename):
        self.view = ShardView(filename)
        self.count = self.view.count
        
        # Extract boundaries for the Spine's search logic
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
