from rpython.rlib import jit
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long

class ShardHandle(object):
    _immutable_fields_ = ['filename', 'lsn', 'view', 'min_key', 'max_key']
    def __init__(self, filename, schema, lsn):
        from gnitz.storage import shard_table
        from gnitz.core import types 
        self.filename = filename
        self.lsn = lsn
        # Checksums are expensive for every handle open in query loops; 
        # normally validated once at publish time.
        self.view = shard_table.TableShardView(filename, schema, validate_checksums=False)
        
        if self.view.count > 0:
            is_u128 = schema.get_pk_column().field_type == types.TYPE_U128
            self.min_key = self.view.get_pk_u128(0) if is_u128 else r_uint128(self.view.get_pk_u64(0))
            self.max_key = self.view.get_pk_u128(self.view.count - 1) if is_u128 else r_uint128(self.view.get_pk_u64(self.view.count - 1))
        else:
            self.min_key = r_uint128(0)
            self.max_key = r_uint128(0)

    def get_weight(self, idx): return self.view.get_weight(idx)
    def find_row_index(self, key): return self.view.find_row_index(key)
    def close(self): self.view.close()

class Spine(object):
    _immutable_fields_ = ['ref_counter']

    def __init__(self, handles, ref_counter=None):
        self.handles = handles
        self.ref_counter = ref_counter
        self.closed = False
        self._sort_handles()
        if self.ref_counter:
            for h in self.handles: self.ref_counter.acquire(h.filename)

    def _sort_handles(self):
        """ Maintains binary search invariant sorted by min_key. """
        for i in range(1, len(self.handles)):
            h = self.handles[i]
            j = i - 1
            while j >= 0 and self.handles[j].min_key > h.min_key:
                self.handles[j+1] = self.handles[j]
                j -= 1
            self.handles[j+1] = h

    @staticmethod
    def from_manifest(manifest_filename, table_id=1, schema=None, ref_counter=None, **kwargs):
        tid = kwargs.get('component_id', kwargs.get('table_id', table_id))
        sch = kwargs.get('layout', schema)
        from gnitz.storage import manifest
        reader = manifest.ManifestReader(manifest_filename)
        try:
            handles = []
            for entry in reader.iterate_entries():
                if entry.table_id == tid:
                    handle = ShardHandle(entry.shard_filename, sch, entry.max_lsn)
                    handles.append(handle)
            return Spine(handles, ref_counter)
        finally:
            reader.close()

    def add_handle(self, handle):
        self.handles.append(handle)
        if self.ref_counter:
            self.ref_counter.acquire(handle.filename)
        self._sort_handles()

    def replace_handles(self, old_filenames, new_handle):
        """ Used after compaction to atomically swap shards in the spine. """
        new_list = []
        for h in self.handles:
            found = False
            for old in old_filenames:
                if h.filename == old:
                    found = True; break
            if found:
                h.close()
                if self.ref_counter: self.ref_counter.release(h.filename)
            else:
                new_list.append(h)
        
        new_list.append(new_handle)
        if self.ref_counter: self.ref_counter.acquire(new_handle.filename)
        self.handles = new_list
        self._sort_handles()

    def _find_upper_bound(self, key):
        low = 0
        high = len(self.handles) - 1
        ans = -1
        while low <= high:
            mid = (low + high) // 2
            if self.handles[mid].min_key <= key:
                ans = mid
                low = mid + 1
            else:
                high = mid - 1
        return ans

    def find_all_shards_and_indices(self, key):
        results = []
        upper = self._find_upper_bound(key)
        if upper == -1: return results
        # Scan backwards from upper bound to find all potentially overlapping shards
        for i in range(upper, -1, -1):
            h = self.handles[i]
            if key <= h.max_key:
                row_idx = h.find_row_index(key)
                if row_idx != -1: results.append((h, row_idx))
        return results

    def close_all(self):
        if self.closed: return
        for h in self.handles:
            h.close()
            if self.ref_counter: self.ref_counter.release(h.filename)
        if self.ref_counter: self.ref_counter.try_cleanup()
        self.closed = True
