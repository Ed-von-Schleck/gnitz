# gnitz/storage/table.py

import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core.batch import make_singleton_batch
from gnitz.core import types, errors, row_logic, strings as string_logic
from gnitz.core import comparator as core_comparator
from gnitz.storage import (
    wal,
    index,
    manifest,
    memtable,
    refcount,
    wal_format,
    shard_table,
    comparator as storage_comparator,
    cursor,
)
from gnitz.storage.memtable_node import node_get_weight, node_get_next_off, node_get_key
from gnitz.storage.ephemeral_table import EphemeralTable
from gnitz.backend.table import AbstractTable


class MemTableEntry(object):
    """Test-compatibility wrapper for SkipList nodes."""

    def __init__(self, pk, weight, row):
        self.pk = pk
        self.weight = weight
        self.row = row


class PersistentTable(AbstractTable):
    """
    Coordinator for a single Z-Set table.

    Manages the lifecycle of the SkipList MemTable, the Write-Ahead Log,
    the columnar ShardIndex, and the atomic Manifest Authority.

    Implements gnitz.backend.table.AbstractTable to satisfy the VM/Query interface.
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
        validate_checksums=False,
    ):
        self.directory = directory
        self.name = name
        self.schema = schema
        self.table_id = table_id
        self.validate_checksums = validate_checksums
        self.is_closed = False

        self.ref_counter = refcount.RefCounter()
        self.row_cmp = row_logic.PayloadRowComparator(schema)

        if not os.path.exists(directory):
            os.makedirs(directory)
        manifest_path = os.path.join(directory, "MANIFEST")
        self.manifest_manager = manifest.ManifestManager(manifest_path)

        self.index = index.ShardIndex(table_id, schema, self.ref_counter)
        if self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            for entry in reader.iterate_entries():
                if entry.table_id == self.table_id:
                    h = index.ShardHandle(
                        entry.shard_filename,
                        schema,
                        entry.min_lsn,
                        entry.max_lsn,
                        validate_checksums=validate_checksums,
                    )
                    self.index.add_handle(h)
            self.current_lsn = reader.global_max_lsn + r_uint64(1)
            reader.close()
        else:
            self.current_lsn = r_uint64(1)

        self.memtable = memtable.MemTable(schema, memtable_arena_size)

        wal_path = os.path.join(directory, name + ".wal")
        self.wal_writer = wal.WALWriter(wal_path, schema)

        self.recover_from_wal(wal_path)

    # -------------------------------------------------------------------------
    # AbstractTable Implementation
    # -------------------------------------------------------------------------

    def get_schema(self):
        return self.schema

    def create_cursor(self):
        num_shards = len(self.index.handles)
        # Fix: Use newlist_hint to avoid mr-poisoning
        cs = newlist_hint(1 + num_shards)

        cs.append(cursor.MemTableCursor(self.memtable))
        for h in self.index.handles:
            cs.append(cursor.ShardCursor(h.view))

        return cursor.UnifiedCursor(self.schema, cs)

    def create_scratch_table(self, name, schema):
        """
        Architectural Pivot: Always return an EphemeralTable for trace data.
        This eliminates per-record fsyncs for DISTINCT/REDUCE operations.
        """
        scratch_dir = os.path.join(self.directory, "scratch_" + name)

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
        batch = make_singleton_batch(self.schema, r_uint128(key), weight, payload)
        self.ingest_batch(batch)

    def ingest_batch(self, batch):
        """
        Durable Z-Set batch update.
        Assigns one LSN to the entire batch (DBSP tick semantics).
        Converts N WAL writes/fsyncs into 1 block append.
        """
        if self.is_closed:
            raise errors.StorageError("Table is closed")

        n = batch.length()
        if n == 0:
            return

        lsn = self.current_lsn
        self.current_lsn += r_uint64(1)

        # Fix: Use newlist_hint to avoid mr-poisoning
        wal_records = newlist_hint(n)
        for i in range(n):
            w = batch.get_weight(i)
            # Ghost Property: skip records that sum to zero
            if w == r_int64(0):
                continue

            pk = batch.get_pk(i)
            row = batch.get_row(i)

            # Update in-memory state
            self.memtable.upsert(pk, w, row)

            # Prepare for durability
            wal_records.append(wal_format.WALRecord(pk, w, row))

        # Commit batch to WAL with a single block write and fsync
        if len(wal_records) > 0:
            self.wal_writer.append_batch(lsn, self.table_id, wal_records)

    def get_weight(self, key, payload):
        r_key = r_uint128(key)
        total_w = r_int64(0)

        # 1. Check SkipList MemTable
        node_off = self.memtable._find_exact_values(r_key, payload)
        if node_off != 0:
            total_w += node_get_weight(self.memtable.arena.base_ptr, node_off)

        # 2. Check Columnar Shards via Index
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
    # Internal Logic
    # -------------------------------------------------------------------------

    def recover_from_wal(self, wal_path):
        """
        Replays WAL blocks into the MemTable to recover volatile state.
        Uses the zero-copy upsert_raw path.
        """
        if not os.path.exists(wal_path):
            return

        # Start recovery from the point where the last manifest left off
        boundary = self.current_lsn
        reader = wal.WALReader(wal_path, self.schema)
        try:
            while True:
                block = reader.read_next_block()
                if block is None:
                    break
                
                try:
                    if block.tid != self.table_id or block.lsn < boundary:
                        continue

                    # Zero-Copy Recovery Path: Pass RawWALRecord to upsert_raw
                    for rec in block.records:
                        self.memtable.upsert_raw(rec.pk, rec.weight, rec)

                    if block.lsn >= self.current_lsn:
                        self.current_lsn = block.lsn + r_uint64(1)
                finally:
                    # Essential: Release the raw C memory held by the block
                    block.free()
        finally:
            reader.close()

    def insert(self, pk, row):
        self.ingest(pk, r_int64(1), row)

    def delete(self, pk, row):
        self.ingest(pk, r_int64(-1), row)

    def flush(self):
        """Transmutes MemTable to a columnar shard and clears memory."""
        if self.is_closed:
            raise errors.StorageError("Table is closed")

        shard_name = "shard_%d_%d.db" % (self.table_id, intmask(self.current_lsn))
        shard_path = os.path.join(self.directory, shard_name)

        self.memtable.flush(shard_path, self.table_id)

        if not os.path.exists(shard_path):
            return ""

        lsn_max = self.current_lsn - r_uint64(1)
        h = index.ShardHandle(
            shard_path,
            self.schema,
            r_uint64(0),
            lsn_max,
            validate_checksums=self.validate_checksums,
        )

        if h.view.count > 0:
            self.index.add_handle(h)
            self.manifest_manager.publish_new_version(
                self.index.get_metadata_list(), lsn_max
            )
            self.wal_writer.truncate_before_lsn(self.current_lsn)
        else:
            h.close()
            try:
                os.unlink(shard_path)
            except OSError:
                pass
            self.manifest_manager.publish_new_version(
                self.index.get_metadata_list(), lsn_max
            )

        arena_size = self.memtable.arena.size
        self.memtable.free()
        self.memtable = memtable.MemTable(self.schema, arena_size)

        return shard_path

    def close(self):
        if self.is_closed:
            return
        if self.wal_writer:
            self.wal_writer.close()
        if self.memtable:
            self.memtable.free()
        if self.index:
            self.index.close_all()
        self.is_closed = True
