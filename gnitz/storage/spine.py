"""
gnitz/storage/spine.py
"""
from rpython.rlib import jit

class ShardHandle(object):
    """
    Lightweight wrapper for a memory-mapped ECS shard.
    Includes LSN for Last-Write-Wins resolution.
    """
    _immutable_fields_ = ['view', 'min_eid', 'max_eid', 'lsn', 'filename']

    def __init__(self, filename, layout, lsn):
        from gnitz.storage import shard_ecs
        self.filename = filename
        self.lsn = lsn
        self.view = shard_ecs.ECSShardView(filename, layout)
        
        if self.view.count > 0:
            self.min_eid = self.view.get_entity_id(0)
            self.max_eid = self.view.get_entity_id(self.view.count - 1)
        else:
            self.min_eid = 0
            self.max_eid = 0

    def get_weight(self, idx):
        return self.view.get_weight(idx)
    
    def find_entity_index(self, entity_id):
        return self.view.find_entity_index(entity_id)

    def read_field_i64(self, idx, field_idx):
        return self.view.read_field_i64(idx, field_idx)

    def close(self):
        self.view.close()

class Spine(object):
    _immutable_fields_ = ['handles[*]', 'count', 'ref_counter']

    def __init__(self, handles, ref_counter=None):
        self.handles = handles
        self.count = len(handles)
        self.ref_counter = ref_counter
        self.closed = False
        
        if self.ref_counter:
            for h in self.handles:
                self.ref_counter.acquire(h.filename)

    @staticmethod
    def from_manifest(manifest_filename, component_id, layout, ref_counter=None):
        from gnitz.storage import manifest
        reader = manifest.ManifestReader(manifest_filename)
        try:
            handles = []
            for entry in reader.iterate_entries():
                if entry.component_id == component_id:
                    # Capture max_lsn from manifest entry
                    handle = ShardHandle(entry.shard_filename, layout, entry.max_lsn)
                    handles.append(handle)
            return Spine(handles, ref_counter)
        finally:
            reader.close()

    @jit.elidable
    def _find_upper_bound(self, entity_id):
        low = 0
        high = self.count - 1
        ans = -1
        while low <= high:
            mid = (low + high) // 2
            if self.handles[mid].min_eid <= entity_id:
                ans = mid
                low = mid + 1
            else:
                high = mid - 1
        return ans

    def find_all_shards_and_indices(self, entity_id):
        results = []
        upper = self._find_upper_bound(entity_id)
        if upper == -1:
            return results

        for i in range(upper, -1, -1):
            h = self.handles[i]
            if entity_id <= h.max_eid:
                row_idx = h.find_entity_index(entity_id)
                if row_idx != -1:
                    results.append((h, row_idx))
        return results

    def close_all(self):
        if self.closed: return
        for h in self.handles:
            h.close()
            if self.ref_counter: self.ref_counter.release(h.filename)
        if self.ref_counter: self.ref_counter.try_cleanup()
        self.closed = True
