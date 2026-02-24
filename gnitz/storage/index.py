# gnitz/storage/index.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import errors
from gnitz.storage import shard_table
from gnitz.storage.metadata import ManifestEntry
from gnitz.storage import manifest


class ShardHandle(object):
    """ACTIVE HANDLE: Owns the mmap and provides fast PK-range filtering."""
    _immutable_fields_ = [
        'filename', 'min_lsn', 'lsn', 'view', 'pk_min_lo', 'pk_min_hi', 
        'pk_max_lo', 'pk_max_hi'
    ]

    def __init__(self, filename, schema, min_lsn, max_lsn, validate_checksums=False):
        self.filename = filename
        self.min_lsn = min_lsn
        self.lsn = max_lsn 
        self.view = shard_table.TableShardView(filename, schema, validate_checksums=validate_checksums)
        
        if self.view.count > 0:
            from gnitz.core import types
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

    def close(self):
        self.view.close()


class ShardIndex(object):
    """UNIFIED MANAGER: Coordinates handles, refcounting, and structural health."""
    def __init__(self, table_id, schema, ref_counter, compaction_threshold=4):
        self.table_id = table_id
        self.schema = schema
        self.ref_counter = ref_counter
        self.compaction_threshold = compaction_threshold
        self.handles = newlist_hint(8)
        self.needs_compaction = False

    def _sort(self):
        """RPython-safe insertion sort for handles by MinPK."""
        for i in range(1, len(self.handles)):
            h = self.handles[i]
            target_min_k = h.get_min_key()
            j = i - 1
            while j >= 0 and self.handles[j].get_min_key() > target_min_k:
                self.handles[j+1] = self.handles[j]
                j -= 1
            self.handles[j+1] = h

    def add_handle(self, handle):
        """Adds handle to index and acquires file lock."""
        self.ref_counter.acquire(handle.filename)
        self.handles.append(handle)
        self._sort()
        self._check_compaction_health()

    def replace_handles(self, old_filenames, new_handle):
        """Atomically replaces a set of shards (e.g. after compaction)."""
        num_existing = len(self.handles)
        new_list = newlist_hint(num_existing + 1)
        
        for i in range(num_existing):
            h = self.handles[i]
            superseded = False
            for old_fn in old_filenames:
                if h.filename == old_fn:
                    superseded = True
                    break
            
            if superseded:
                h.close()
                self.ref_counter.release(h.filename)
            else:
                new_list.append(h)
        
        self.ref_counter.acquire(new_handle.filename)
        new_list.append(new_handle)
        self.handles = new_list
        self._sort()
        self._check_compaction_health()

    def find_all_shards_and_indices(self, key):
        """Prunes shards via cached bounds before performing binary search."""
        num_h = len(self.handles)
        results = newlist_hint(num_h)
        for i in range(num_h):
            h = self.handles[i]
            if h.get_min_key() <= key <= h.get_max_key():
                row_idx = h.view.find_row_index(key)
                if row_idx != -1:
                    results.append((h, row_idx))
        return results

    def _check_compaction_health(self):
        """Monitors read amplification/overlap depth."""
        # Simple threshold check for this table
        if len(self.handles) > self.compaction_threshold:
            self.needs_compaction = True
        else:
            self.needs_compaction = False

    def get_metadata_list(self):
        """Produces a list of ManifestEntry objects for Manifest/Compactor use."""
        num_h = len(self.handles)
        meta_list = newlist_hint(num_h)
        for i in range(num_h):
            h = self.handles[i]
            # Note: ManifestEntry takes (table_id, shard_filename, ...) 
            # whereas the old ShardMetadata took (filename, table_id, ...)
            meta_list.append(ManifestEntry(
                self.table_id,
                h.filename, 
                h.get_min_key(), 
                h.get_max_key(), 
                h.min_lsn, 
                h.lsn
            ))
        return meta_list

    def close_all(self):
        """Releases all handles and locks."""
        for h in self.handles:
            h.close()
            self.ref_counter.release(h.filename)
        self.handles = newlist_hint(0)


def index_from_manifest(manifest_path, table_id, schema, ref_counter, validate_checksums=False):
    """Factory to initialize an Index from a Manifest file."""
    idx = ShardIndex(table_id, schema, ref_counter)
    reader = manifest.ManifestReader(manifest_path)
    try:
        for entry in reader.iterate_entries():
            if entry.table_id == table_id:
                handle = ShardHandle(
                    entry.shard_filename, 
                    schema, 
                    entry.min_lsn,
                    entry.max_lsn, 
                    validate_checksums
                )
                idx.add_handle(handle)
    finally:
        reader.close()
    return idx
