import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
try:
    from rpython.rlib.rarithmetic import r_uint128
except ImportError:
    r_uint128 = long

from gnitz.storage import memtable, spine, engine, manifest, shard_registry, refcount, wal, compactor
from gnitz.core import values as db_values, types

class PersistentTable(object):
    """
    Persistent Z-Set Table.
    Integrates MemTable, WAL, and Columnar Shards.
    """
    def __init__(self, directory, name, schema, table_id=1, cache_size=1048576, read_only=False, **kwargs):
        self.directory = directory
        self.name = name
        self.schema = schema
        self.table_id = kwargs.get('component_id', table_id)
        self.read_only = read_only
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
        
        self.mem_manager = memtable.MemTableManager(schema, cache_size, wal_writer=self.wal_writer, component_id=self.table_id)
        
        if self.manifest_manager.exists():
            self.spine = spine.Spine.from_manifest(self.manifest_path, self.table_id, schema, ref_counter=self.ref_counter)
        else:
            self.spine = spine.Spine([], self.ref_counter)
            
        self.engine = engine.Engine(self.mem_manager, self.spine, self.manifest_manager, self.registry, table_id=self.table_id, recover_wal_filename=self.wal_path)
        self.compaction_policy = compactor.CompactionPolicy(self.registry)
        self._query_scratch = lltype.malloc(rffi.CCHARP.TO, self.schema.memtable_stride, flavor='raw')

    def insert(self, key, db_values_list):
        self.engine.mem_manager.put(r_uint128(key), 1, db_values_list)

    def remove(self, key, db_values_list):
        self.engine.mem_manager.put(r_uint128(key), -1, db_values_list)

    def get_weight(self, key, db_values_list):
        for i in range(self.schema.memtable_stride): self._query_scratch[i] = '\x00'
        self.mem_manager.active_table._pack_to_buf(self._query_scratch, db_values_list)
        blob_base = self.mem_manager.active_table.blob_arena.base_ptr
        return self.engine.get_effective_weight(r_uint128(key), self._query_scratch, blob_base)

    def flush(self):
        """
        Flushes MemTable to disk, updates manifest, and prunes the WAL.
        """
        filename = os.path.join(self.directory, "%s_shard_%d.db" % (self.name, int(self.engine.mem_manager.starting_lsn)))
        min_key, max_key, needs_compaction = self.engine.flush_and_rotate(filename)
        
        # After a successful flush, data is durable in a Shard.
        # We can now prune the WAL up to the last persisted LSN.
        self.checkpoint()
        
        return filename

    def checkpoint(self):
        """
        Prunes the WAL by removing all entries that have been successfully
        persisted into immutable Shards, as recorded in the Manifest.
        """
        if not self.read_only and self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            # The manifest tracks the global max LSN that is now in shards
            lsn = reader.global_max_lsn
            reader.close()
            # Truncate everything before the next expected LSN
            self.wal_writer.truncate_before_lsn(lsn + r_uint64(1))

    def _trigger_compaction(self):
        compactor.execute_compaction(self.table_id, self.compaction_policy, self.manifest_manager, self.ref_counter, self.schema, output_dir=self.directory, spine_obj=self.spine)

    def close(self):
        if self.is_closed: return
        if self._query_scratch:
            lltype.free(self._query_scratch, flavor='raw')
            self._query_scratch = lltype.nullptr(rffi.CCHARP.TO)
        
        self.engine.close()
        if self.wal_writer:
            self.wal_writer.close()
        self.is_closed = True

PersistentZSet = PersistentTable

class ZSet(object):
    """
    In-memory Z-Set (Multiset).
    Used for DBSP transient state and unit testing.
    """
    def __init__(self, schema):
        self.schema = schema
        self.mem_manager = memtable.MemTableManager(schema, 10 * 1024 * 1024) 

    def upsert(self, key, weight, payload):
        self.mem_manager.put(r_uint128(key), weight, payload)

    def get_weight(self, key):
        table = self.mem_manager.active_table
        base = table.arena.base_ptr
        total = 0
        curr = table._find_first_key(r_uint128(key))
        while curr != 0:
            k = table._get_node_key(curr)
            if k != r_uint128(key):
                break
            total += memtable.node_get_weight(base, curr)
            curr = memtable.node_get_next_off(base, curr, 0)
        return int(total)

    def get_payload(self, key):
        table = self.mem_manager.active_table
        base = table.arena.base_ptr
        curr = table._find_first_key(r_uint128(key))
        last_valid_node = 0
        while curr != 0:
            k = table._get_node_key(curr)
            if k != r_uint128(key):
                break
            if memtable.node_get_weight(base, curr) != 0:
                last_valid_node = curr
            curr = memtable.node_get_next_off(base, curr, 0)
            
        if last_valid_node != 0:
            return memtable.unpack_payload_to_values(table, last_valid_node)
        return None

    def iter_nonzero(self):
        table = self.mem_manager.active_table
        base = table.arena.base_ptr
        curr = memtable.node_get_next_off(base, table.head_off, 0)
        while curr != 0:
            w = memtable.node_get_weight(base, curr)
            if w != 0:
                k = table._get_node_key(curr)
                k_val = int(k) if self.schema.get_pk_column().field_type.size == 8 else k
                p = memtable.unpack_payload_to_values(table, curr)
                yield (k_val, int(w), p)
            curr = memtable.node_get_next_off(base, curr, 0)

    def iter_positive(self):
        for k, w, p in self.iter_nonzero():
            if w > 0:
                yield (k, w, p)
