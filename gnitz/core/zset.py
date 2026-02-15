# gnitz/core/zset.py

import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128

from gnitz.storage import memtable_node, spine, engine, manifest, shard_registry, refcount, wal, compactor, memtable_manager
from gnitz.core import values as db_values, types

class PersistentTable(object):
    """
    Persistent Z-Set Table.
    Integrates MemTable, WAL, and Columnar Shards.
    """
    def __init__(self, directory, name, schema, table_id=1, cache_size=1048576, read_only=False, validate_checksums=False):
        self.directory = directory
        self.name = name
        self.schema = schema
        self.table_id = table_id
        self.read_only = read_only
        self.validate_checksums = validate_checksums
        self.is_closed = False
        
        if not os.path.exists(directory): 
            if read_only: raise OSError("Directory does not exist")
            os.mkdir(directory)
            
        self.manifest_path = os.path.join(directory, "%s.manifest" % name)
        self.wal_path = os.path.join(directory, "%s.wal" % name)
        
        self.ref_counter = refcount.RefCounter()
        self.registry = shard_registry.ShardRegistry()
        self.manifest_manager = manifest.ManifestManager(self.manifest_path)
        
        if read_only:
            self.wal_writer = None
        else:
            self.wal_writer = wal.WALWriter(self.wal_path, schema)
        
        self.mem_manager = memtable_manager.MemTableManager(
            schema, cache_size, wal_writer=self.wal_writer, table_id=self.table_id
        )
        
        if self.manifest_manager.exists():
            self.spine = spine.spine_from_manifest(
                self.manifest_path, 
                self.table_id, 
                self.schema,
                self.ref_counter,
                self.validate_checksums
            )
        else:
            self.spine = spine.Spine([], self.ref_counter)
            
        self.engine = engine.Engine(
            self.mem_manager, 
            self.spine, 
            manifest_manager=self.manifest_manager, 
            registry=self.registry, 
            table_id=self.table_id, 
            recover_wal_filename=self.wal_path,
            validate_checksums=self.validate_checksums
        )
        self.compaction_policy = compactor.CompactionPolicy(self.registry)

    def insert(self, key, db_values_list):
        self.mem_manager.put(r_uint128(key), 1, db_values_list)

    def remove(self, key, db_values_list):
        self.mem_manager.put(r_uint128(key), -1, db_values_list)

    def get_weight(self, key, db_values_list):
        return self.engine.get_effective_weight_raw(r_uint128(key), db_values_list)

    def flush(self):
        """
        Commits current MemTable to a new Shard.
        FIXED: Updated to receive only 'needs_compaction' from the engine.
        """
        lsn_val = intmask(self.mem_manager.starting_lsn)
        filename = os.path.join(self.directory, "%s_shard_%d.db" % (
            self.name, lsn_val)
        )
        needs_compaction = self.engine.flush_and_rotate(filename)
        self.checkpoint()
        return filename

    def checkpoint(self):
        if not self.read_only and self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            lsn = reader.global_max_lsn
            reader.close()
            self.wal_writer.truncate_before_lsn(lsn + r_uint64(1))

    def _trigger_compaction(self):
        compactor.execute_compaction(
            self.table_id, 
            self.compaction_policy, 
            self.manifest_manager, 
            self.ref_counter, 
            self.schema, 
            self.directory, 
            self.spine,
            self.validate_checksums
        )

    def close(self):
        if self.is_closed: return
        self.engine.close()
        if self.wal_writer: self.wal_writer.close()
        self.is_closed = True

class ZSet(object):
    def __init__(self, schema):
        self.schema = schema
        self.mem_manager = memtable_manager.MemTableManager(schema, 64 * 1024 * 1024) 

    def upsert(self, key, weight, payload):
        self.mem_manager.put(r_uint128(key), weight, payload)

    def get_weight(self, key, payload=None):
        table = self.mem_manager.active_table
        base = table.arena.base_ptr
        total = 0
        curr = table._find_first_key(r_uint128(key))
        while curr != 0:
            k = memtable_node.node_get_key(base, curr, table.key_size)
            if k != r_uint128(key): break
            node_w = memtable_node.node_get_weight(base, curr)
            if payload is not None:
                from gnitz.storage.comparator import compare_values_to_packed
                p_ptr = memtable_node.node_get_payload_ptr(base, curr, table.key_size)
                if compare_values_to_packed(self.schema, payload, p_ptr, table.blob_arena.base_ptr) == 0:
                    return int(node_w)
            else:
                total += node_w
            curr = memtable_node.node_get_next_off(base, curr, 0)
        return int(total)

    def get_payload(self, key):
        table = self.mem_manager.active_table
        base = table.arena.base_ptr
        curr = table._find_first_key(r_uint128(key))
        while curr != 0:
            k = memtable_node.node_get_key(base, curr, table.key_size)
            if k != r_uint128(key): break
            if memtable_node.node_get_weight(base, curr) != 0: 
                return memtable_node.unpack_payload_to_values(table, curr)
            curr = memtable_node.node_get_next_off(base, curr, 0)
        return None

    def iter_nonzero(self):
        table = self.mem_manager.active_table
        base = table.arena.base_ptr
        curr = memtable_node.node_get_next_off(base, table.head_off, 0)
        while curr != 0:
            w = memtable_node.node_get_weight(base, curr)
            if w != 0:
                k = memtable_node.node_get_key(base, curr, table.key_size)
                k_val = intmask(k) if table.key_size == 8 else k
                p = memtable_node.unpack_payload_to_values(table, curr)
                yield (k_val, int(w), p)
            curr = memtable_node.node_get_next_off(base, curr, 0)

    def iter_positive(self):
        for k, w, p in self.iter_nonzero():
            if w > 0: yield (k, w, p)
