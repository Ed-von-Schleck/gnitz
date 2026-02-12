import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long

from gnitz.storage import memtable_node, spine, engine, manifest, shard_registry, refcount, wal, compactor, memtable_manager
from gnitz.core import values as db_values, types

class PersistentTable(object):
    """
    Persistent Z-Set Table.
    Integrates MemTable, WAL, and Columnar Shards.
    
    Querying now uses "dry-run" comparison, bypassing the need for 
    intermediate payload serialization buffers.
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
            self.spine = spine.Spine.from_manifest(
                self.manifest_path, 
                table_id=self.table_id, 
                schema=self.schema,
                ref_counter=self.ref_counter,
                validate_checksums=self.validate_checksums
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
        """
        Calculates net weight for a specific key and payload across the 
        entire storage hierarchy. Uses dry-run comparison.
        """
        return self.engine.get_effective_weight_raw(r_uint128(key), db_values_list)

    def flush(self):
        filename = os.path.join(self.directory, "%s_shard_%d.db" % (
            self.name, int(self.mem_manager.starting_lsn))
        )
        min_key, max_key, needs_compaction = self.engine.flush_and_rotate(filename)
        self.checkpoint()
        return filename

    def checkpoint(self):
        if not self.read_only and self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            lsn = reader.global_max_lsn
            reader.close()
            # Prune WAL up to the global max LSN recorded in the shards
            self.wal_writer.truncate_before_lsn(lsn + r_uint64(1))

    def _trigger_compaction(self):
        compactor.execute_compaction(
            self.table_id, 
            self.compaction_policy, 
            self.manifest_manager, 
            self.ref_counter, 
            self.schema, 
            output_dir=self.directory, 
            spine_obj=self.spine,
            validate_checksums=self.validate_checksums
        )

    def close(self):
        if self.is_closed: return
        self.engine.close()
        if self.wal_writer: self.wal_writer.close()
        self.is_closed = True

PersistentZSet = PersistentTable

class ZSet(object):
    """
    In-memory Z-Set multiset logic. 
    Updated to use public memtable_node helpers.
    """
    def __init__(self, schema):
        self.schema = schema
        # Large capacity for in-memory only usage
        self.mem_manager = memtable_manager.MemTableManager(schema, 64 * 1024 * 1024) 

    def upsert(self, key, weight, payload):
        self.mem_manager.put(r_uint128(key), weight, payload)

    def get_weight(self, key, payload=None):
        """
        If payload is provided: returns weight of the specific record.
        If payload is None: returns the sum of weights for the key.
        """
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
        """Returns the payload of the first record with a non-zero weight."""
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
        """Iterates over all key-weight-payload triples where weight != 0."""
        table = self.mem_manager.active_table
        base = table.arena.base_ptr
        curr = memtable_node.node_get_next_off(base, table.head_off, 0)
        while curr != 0:
            w = memtable_node.node_get_weight(base, curr)
            if w != 0:
                k = memtable_node.node_get_key(base, curr, table.key_size)
                # Downcast to int for u64 keys to simplify standard tests
                k_val = int(k) if table.key_size == 8 else k
                p = memtable_node.unpack_payload_to_values(table, curr)
                yield (k_val, int(w), p)
            curr = memtable_node.node_get_next_off(base, curr, 0)

    def iter_positive(self):
        for k, w, p in self.iter_nonzero():
            if w > 0: yield (k, w, p)
