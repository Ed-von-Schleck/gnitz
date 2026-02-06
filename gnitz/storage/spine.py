"""
gnitz/storage/spine.py
"""
from rpython.rlib import jit
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import shard_ecs, manifest

class ShardHandle(object):
    _immutable_fields_ = ['view', 'min_eid', 'max_eid', 'count', 'filename']

    def __init__(self, filename, layout):
        self.filename = filename
        self.view = shard_ecs.ECSShardView(filename, layout)
        self.count = self.view.count
        if self.count > 0:
            self.min_eid = self.view.get_entity_id(0)
            self.max_eid = self.view.get_entity_id(self.count - 1)
        else:
            self.min_eid = 0
            self.max_eid = 0

    def get_entity_id(self, idx):
        return self.view.get_entity_id(idx)
        
    def get_weight(self, idx):
        return self.view.get_weight(idx)
    
    def find_entity_index(self, entity_id):
        return self.view.find_entity_index(entity_id)

    def read_field_i64(self, idx, field_idx):
        return self.view.read_field_i64(idx, field_idx)

    def string_field_equals(self, idx, field_idx, val):
        return self.view.string_field_equals(idx, field_idx, val)

    def close(self):
        self.view.close()

class Spine(object):
    _immutable_fields_ = ['handles[*]', 'min_eids[*]', 'max_eids[*]', 'shard_count', 'ref_counter']

    def __init__(self, handles, ref_counter=None):
        self.handles = handles
        self.shard_count = len(handles)
        self.min_eids = [h.min_eid for h in handles]
        self.max_eids = [h.max_eid for h in handles]
        self.ref_counter = ref_counter
        self.closed = False
        
        if self.ref_counter:
            for h in self.handles:
                self.ref_counter.acquire(h.filename)

    @staticmethod
    def from_manifest(manifest_filename, component_id, layout, ref_counter=None):
        reader = manifest.ManifestReader(manifest_filename)
        try:
            handles = []
            for entry in reader.iterate_entries():
                if entry.component_id == component_id:
                    handle = ShardHandle(entry.shard_filename, layout)
                    handles.append(handle)
            return Spine(handles, ref_counter)
        finally:
            reader.close()

    @jit.elidable
    def lookup_candidate_index(self, entity_id):
        low = 0
        high = self.shard_count - 1
        ans = -1
        while low <= high:
            mid = (low + high) // 2
            if self.min_eids[mid] <= entity_id:
                ans = mid
                low = mid + 1
            else:
                high = mid - 1
        
        if ans != -1:
            if entity_id <= self.max_eids[ans]:
                return ans
        return -1

    def find_shard_and_index(self, entity_id):
        idx = self.lookup_candidate_index(entity_id)
        if idx == -1:
            return None, -1
        
        shard = self.handles[idx]
        row_idx = shard.find_entity_index(entity_id)
        if row_idx == -1:
            return None, -1
            
        return shard, row_idx

    def find_all_shards_and_indices(self, entity_id):
        results = []
        for i in range(self.shard_count):
            if self.min_eids[i] <= entity_id <= self.max_eids[i]:
                handle = self.handles[i]
                row_idx = handle.find_entity_index(entity_id)
                if row_idx != -1:
                    results.append((handle, row_idx))
        return results

    def close_all(self):
        if self.closed:
            return
        
        for h in self.handles:
            h.close()
            if self.ref_counter:
                self.ref_counter.release(h.filename)
        
        if self.ref_counter:
            self.ref_counter.try_cleanup()
            
        self.closed = True
