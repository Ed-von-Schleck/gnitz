# gnitz/storage/ephemeral_table.py

import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import types, errors, row_logic, comparator as core_comparator
from gnitz.storage import (
    index,
    memtable,
    refcount,
    shard_table,
    comparator as storage_comparator,
    cursor,
)
from gnitz.storage.memtable_node import node_get_weight, node_get_next_off
from gnitz.backend.table import AbstractTable


class EphemeralTable(AbstractTable):
    """
    An Ephemeral Z-Set Table used for DBSP Trace state (DISTINCT, REDUCE).

    This implementation bypasses the Write-Ahead Log and Persistent Manifest.
    Data is held in memory (MemTable) and spills to temporary columnar shards
    only when memory pressure triggers a flush. All disk artifacts are
    deleted upon closing the table.
    """

    _immutable_fields_ = [
        "schema",
        "table_id",
        "directory",
        "name",
        "ref_counter",
        "row_cmp",
    ]

    def __init__(
        self,
        directory,
        name,
        schema,
        table_id=1,
        memtable_arena_size=1 * 1024 * 1024,
    ):
        self.directory = directory
        self.name = name
        self.schema = schema
        self.table_id = table_id
        self.is_closed = False

        # Local lifecycle management
        self.ref_counter = refcount.RefCounter()
        self.row_cmp = row_logic.PayloadRowComparator(schema)
        self.temp_files = []

        # Incremental state tracking
        self.current_lsn = r_uint64(1)
        self.index = index.ShardIndex(table_id, schema, self.ref_counter)
        self.memtable = memtable.MemTable(schema, memtable_arena_size)

        if not os.path.exists(directory):
            os.makedirs(directory)

    # -------------------------------------------------------------------------
    # AbstractTable Implementation
    # -------------------------------------------------------------------------

    def get_schema(self):
        return self.schema

    def create_cursor(self):
        """
        Creates a UnifiedCursor merging the active MemTable and spilled shards.
        """
        num_shards = len(self.index.handles)
        cs = newlist_hint(1 + num_shards)

        cs.append(cursor.MemTableCursor(self.memtable))
        for h in self.index.handles:
            cs.append(cursor.ShardCursor(h.view))

        return cursor.UnifiedCursor(self.schema, cs)

    def create_scratch_table(self, name, schema):
        """
        Creates another EphemeralTable for nested operator state.
        """
        scratch_dir = os.path.join(self.directory, name)

        tid = 0
        for char in name:
            tid = (tid * 31 + ord(char)) & 0x7FFFFFFF
        if tid == 0:
            tid = 1

        return EphemeralTable(
            scratch_dir,
            name,
            schema,
            table_id=tid,
            memtable_arena_size=self.memtable.arena.size,
        )

    def ingest(self, key, weight, payload):
        """
        Direct ingestion into the MemTable, bypassing the WAL.
        Each individual record receives a unique LSN.
        """
        if self.is_closed:
            raise errors.StorageError("EphemeralTable is closed")

        # Advance LSN per record for standard ingestion
        self.current_lsn += r_uint64(1)
        self.memtable.upsert(r_uint128(key), weight, payload)

    def ingest_batch(self, pks, weights, rows):
        """
        Optimized batch ingestion for DBSP operators.
        Increments the LSN once for the entire batch (logical tick).
        """
        if self.is_closed:
            raise errors.StorageError("EphemeralTable is closed")

        # Entire batch shares a single logical time increment
        self.current_lsn += r_uint64(1)

        for i in range(len(pks)):
            # Only ingest non-zero weights (Ghost Property enforcement)
            if weights[i] != 0:
                self.memtable.upsert(pks[i], weights[i], rows[i])

    def get_weight(self, key, payload):
        """
        Calculates net weight across memory and spilled shards.
        Used primarily for trace-based lookups in non-linear operators.
        """
        r_key = r_uint128(key)
        total_w = r_int64(0)

        # 1. Check SkipList MemTable
        node_off = self.memtable._find_exact_values(r_key, payload)
        if node_off != 0:
            total_w += node_get_weight(self.memtable.arena.base_ptr, node_off)

        # 2. Check Spilled Columnar Shards
        shard_matches = self.index.find_all_shards_and_indices(r_key)
        if shard_matches:
            soa = storage_comparator.SoAAccessor(self.schema)
            val_acc = core_comparator.ValueAccessor(self.schema)
            val_acc.set_row(payload)

            for handle, row_idx in shard_matches:
                view = handle.view
                idx = row_idx
                while idx < view.count:
                    if self.schema.get_pk_column().field_type.size == 16:
                        if view.get_pk_u128(idx) != r_key:
                            break
                    else:
                        if r_uint128(view.get_pk_u64(idx)) != r_key:
                            break

                    soa.set_row(view, idx)
                    if (
                        core_comparator.compare_rows(self.schema, soa, val_acc)
                        == 0
                    ):
                        total_w += view.get_weight(idx)
                    idx += 1
        return total_w

    # -------------------------------------------------------------------------
    # Maintenance and Lifecycle
    # -------------------------------------------------------------------------

    def flush(self):
        """
        Spills the current MemTable to a temporary columnar shard.
        This is triggered by the VM or MemTable pressure.
        """
        if self.is_closed:
            raise errors.StorageError("EphemeralTable is closed")

        # Generate a unique temporary shard name
        shard_name = "temp_trace_%d_%d.db" % (self.table_id, intmask(self.current_lsn))
        shard_path = os.path.join(self.directory, shard_name)

        # AoS -> SoA Transmutation
        self.memtable.flush(shard_path, self.table_id)

        if os.path.exists(shard_path):
            lsn_max = self.current_lsn
            h = index.ShardHandle(
                shard_path,
                self.schema,
                r_uint64(0),
                lsn_max,
                validate_checksums=False,  # Disable for speed in ephemeral state
            )

            if h.view.count > 0:
                self.index.add_handle(h)
                self.temp_files.append(shard_path)
            else:
                h.close()
                try:
                    os.unlink(shard_path)
                except OSError:
                    pass

        # Reset MemTable
        arena_size = self.memtable.arena.size
        self.memtable.free()
        self.memtable = memtable.MemTable(self.schema, arena_size)

        return shard_path

    def close(self):
        """
        Releases all memory and physically deletes spilled shard files.
        """
        if self.is_closed:
            return

        # 1. Close the index (releases mmap and file locks)
        self.index.close_all()

        # 2. Free arenas
        if self.memtable:
            self.memtable.free()

        # 3. Unlink all temporary shards
        for path in self.temp_files:
            try:
                os.unlink(path)
            except OSError:
                pass

        self.is_closed = True
