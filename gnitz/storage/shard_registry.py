from rpython.rlib.rarithmetic import r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

class ShardMetadata(object):
    """Metadata for a shard stored in the Registry."""
    _immutable_fields_ = [
        'filename', 'table_id', 'min_key_lo', 'min_key_hi', 
        'max_key_lo', 'max_key_hi', 'min_lsn', 'max_lsn'
    ]
    
    def __init__(self, filename, table_id, min_key, max_key, min_lsn, max_lsn):
        self.filename = filename
        self.table_id = table_id
        mk = r_uint128(min_key)
        xk = r_uint128(max_key)
        self.min_key_lo = r_uint64(mk)
        self.min_key_hi = r_uint64(mk >> 64)
        self.max_key_lo = r_uint64(xk)
        self.max_key_hi = r_uint64(xk >> 64)
        self.min_lsn = min_lsn
        self.max_lsn = max_lsn

    def get_min_key(self):
        return (r_uint128(self.min_key_hi) << 64) | r_uint128(self.min_key_lo)

    def get_max_key(self):
        return (r_uint128(self.max_key_hi) << 64) | r_uint128(self.max_key_lo)

class ShardRegistry(object):
    def __init__(self):
        self.shards = []
        self.compaction_threshold = 4
        self.tables_needing_compaction = []
    
    def register_shard(self, metadata):
        self.shards.append(metadata)
        self._sort_shards()
    
    def unregister_shard(self, filename):
        """
        Removes a shard from the registry.
        FIXED: Uses an explicit loop and new list to satisfy RPython slicing constraints.
        """
        found_idx = -1
        for i in range(len(self.shards)):
            if self.shards[i].filename == filename:
                found_idx = i
                break
        
        if found_idx != -1:
            new_shards = []
            for i in range(len(self.shards)):
                if i != found_idx:
                    new_shards.append(self.shards[i])
            self.shards = new_shards
            return True
        return False
    
    def _sort_shards(self):
        """Stable insertion sort for RPython."""
        for i in range(1, len(self.shards)):
            key_meta = self.shards[i]
            target_min_k = key_meta.get_min_key()
            j = i - 1
            while j >= 0:
                curr = self.shards[j]
                if curr.table_id > key_meta.table_id:
                    self.shards[j + 1] = self.shards[j]
                    j -= 1
                elif curr.table_id == key_meta.table_id and curr.get_min_key() > target_min_k:
                    self.shards[j + 1] = self.shards[j]
                    j -= 1
                else: break
            self.shards[j + 1] = key_meta

    def find_overlapping_shards(self, table_id, min_key, max_key):
        res = []
        for i in range(len(self.shards)):
            s = self.shards[i]
            if s.table_id != table_id: 
                continue
            if s.get_min_key() <= max_key and min_key <= s.get_max_key():
                res.append(s)
        return res
    
    def get_read_amplification(self, table_id, key):
        count = 0
        for i in range(len(self.shards)):
            s = self.shards[i]
            if s.table_id == table_id and s.get_min_key() <= key <= s.get_max_key():
                count += 1
        return count
    
    def mark_for_compaction(self, table_id):
        count = 0
        for i in range(len(self.shards)):
            if self.shards[i].table_id == table_id: 
                count += 1
        
        if count > self.compaction_threshold:
            found = False
            for j in range(len(self.tables_needing_compaction)):
                if self.tables_needing_compaction[j] == table_id:
                    found = True
                    break
            if not found:
                self.tables_needing_compaction.append(table_id)
            return True
        return False

    def clear_compaction_flag(self, table_id):
        new_list = []
        for i in range(len(self.tables_needing_compaction)):
            if self.tables_needing_compaction[i] != table_id:
                new_list.append(self.tables_needing_compaction[i])
        self.tables_needing_compaction = new_list

    def get_shards_for_table(self, table_id):
        """
        FIXED: Explicit loop for type-stable return list.
        """
        res = []
        for i in range(len(self.shards)):
            s = self.shards[i]
            if s.table_id == table_id:
                res.append(s)
        return res
