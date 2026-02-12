from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

class ShardMetadata(object):
    def __init__(self, filename, table_id, min_key, max_key, min_lsn, max_lsn):
        self.filename = filename
        self.table_id = table_id
        self.min_key = r_uint128(min_key)
        self.max_key = r_uint128(max_key)
        self.min_lsn = min_lsn
        self.max_lsn = max_lsn

class ShardRegistry(object):
    def __init__(self):
        self.shards = []
        self.compaction_threshold = 4
        self.tables_needing_compaction = []
    
    def register_shard(self, metadata):
        self.shards.append(metadata)
        self._sort_shards()
    
    def unregister_shard(self, filename):
        for i in range(len(self.shards)):
            if self.shards[i].filename == filename:
                self.shards = self.shards[:i] + self.shards[i+1:]
                return True
        return False
    
    def _sort_shards(self):
        for i in range(1, len(self.shards)):
            key_meta = self.shards[i]
            j = i - 1
            while j >= 0:
                curr = self.shards[j]
                if curr.table_id > key_meta.table_id:
                    self.shards[j + 1] = self.shards[j]
                    j -= 1
                elif curr.table_id == key_meta.table_id and curr.min_key > key_meta.min_key:
                    self.shards[j + 1] = self.shards[j]
                    j -= 1
                else: break
            self.shards[j + 1] = key_meta

    def find_overlapping_shards(self, table_id, min_key, max_key):
        res = []
        for s in self.shards:
            if s.table_id != table_id: continue
            if s.min_key <= max_key and min_key <= s.max_key:
                res.append(s)
        return res
    
    def get_read_amplification(self, table_id, key):
        count = 0
        for s in self.shards:
            if s.table_id == table_id and s.min_key <= key <= s.max_key:
                count += 1
        return count
    
    def mark_for_compaction(self, table_id):
        # Simplistic heuristic for now: count shards per table
        count = 0
        for s in self.shards:
            if s.table_id == table_id: count += 1
        
        if count > self.compaction_threshold:
            if table_id not in self.tables_needing_compaction:
                self.tables_needing_compaction.append(table_id)
            return True
        return False

    def clear_compaction_flag(self, table_id):
        self.tables_needing_compaction = [t for t in self.tables_needing_compaction if t != table_id]

    def get_shards_for_table(self, table_id):
        return [s for s in self.shards if s.table_id == table_id]
