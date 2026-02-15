from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from gnitz.storage import manifest

class ShardHandle(object):
    """
    Reader handle for a shard with cached key bounds for fast Spine lookups.
    """
    _immutable_fields_ = [
        'filename', 'lsn', 'view', 'pk_min_lo', 'pk_min_hi', 
        'pk_max_lo', 'pk_max_hi'
    ]

    def __init__(self, filename, schema, lsn, validate_checksums=False):
        from gnitz.storage import shard_table
        from gnitz.core import types
        self.filename = filename
        self.lsn = lsn
        self.view = shard_table.TableShardView(filename, schema, validate_checksums=validate_checksums)
        
        if self.view.count > 0:
            is_u128 = schema.get_pk_column().field_type == types.TYPE_U128
            k_min = self.view.get_pk_u128(0) if is_u128 else r_uint128(self.view.get_pk_u64(0))
            k_max = self.view.get_pk_u128(self.view.count - 1) if is_u128 else r_uint128(self.view.get_pk_u64(self.view.count - 1))
            
            self.pk_min_lo = r_uint64(k_min)
            self.pk_min_hi = r_uint64(k_min >> 64)
            self.pk_max_lo = r_uint64(k_max)
            self.pk_max_hi = r_uint64(k_max >> 64)
        else:
            self.pk_min_lo = self.pk_min_hi = self.pk_max_lo = self.pk_max_hi = r_uint64(0)

    def get_min_key(self):
        return (r_uint128(self.pk_min_hi) << 64) | r_uint128(self.pk_min_lo)

    def get_max_key(self):
        return (r_uint128(self.pk_max_hi) << 64) | r_uint128(self.pk_max_lo)

    def find_row_index(self, key):
        return self.view.find_row_index(key)

    def get_weight(self, row_idx):
        return self.view.get_weight(row_idx)

    def close(self):
        self.view.close()

class Spine(object):
    """In-memory index tracking active shards for a specific table."""
    def __init__(self, handles, ref_counter=None):
        self.handles = handles
        self.ref_counter = ref_counter
        self._sort_handles()
        if self.ref_counter:
            for h in self.handles:
                self.ref_counter.acquire(h.filename)

    def _sort_handles(self):
        for i in range(1, len(self.handles)):
            h = self.handles[i]
            target_min_k = h.get_min_key()
            j = i - 1
            while j >= 0 and self.handles[j].get_min_key() > target_min_k:
                self.handles[j+1] = self.handles[j]
                j -= 1
            self.handles[j+1] = h

    def find_all_shards_and_indices(self, key):
        results = []
        for h in self.handles:
            if h.get_min_key() <= key <= h.get_max_key():
                row_idx = h.find_row_index(key)
                if row_idx != -1:
                    results.append((h, row_idx))
        return results

    def add_handle(self, handle):
        self.handles.append(handle)
        self._sort_handles()
        if self.ref_counter:
            self.ref_counter.acquire(handle.filename)

    def replace_handles(self, old_filenames, new_handle):
        new_list = []
        for h in self.handles:
            is_superseded = False
            for old_fn in old_filenames:
                if h.filename == old_fn:
                    is_superseded = True
                    break
            
            if is_superseded:
                h.close()
                if self.ref_counter:
                    self.ref_counter.release(h.filename)
            else:
                new_list.append(h)
        
        new_list.append(new_handle)
        if self.ref_counter:
            self.ref_counter.acquire(new_handle.filename)
            
        self.handles = new_list
        self._sort_handles()

    def close_all(self):
        for h in self.handles:
            h.close()
            if self.ref_counter:
                self.ref_counter.release(h.filename)
        self.handles = []

def spine_from_manifest(manifest_path, table_id, schema, ref_counter=None, validate_checksums=False):
    """
    Standalone function for Spine initialization.
    Avoids @classmethod binding issues in RPython.
    """
    reader = manifest.ManifestReader(manifest_path)
    handles = []
    try:
        for entry in reader.iterate_entries():
            if entry.table_id == table_id:
                handle = ShardHandle(
                    entry.shard_filename, 
                    schema, 
                    entry.max_lsn, 
                    validate_checksums
                )
                handles.append(handle)
    finally:
        reader.close()
    return Spine(handles, ref_counter)
