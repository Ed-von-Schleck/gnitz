# gnitz/storage/table.py

import os
import errno
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, errors, serialize
from gnitz.core import comparator as core_comparator
from gnitz.storage import (
    wal,
    index,
    manifest,
    memtable,
    refcount,
    comparator as storage_comparator,
    cursor,
)
from gnitz.storage.memtable_node import node_get_weight
from gnitz.storage.ephemeral_table import EphemeralTable
from gnitz.backend.table import AbstractTable


class PersistentTable(AbstractTable):
    """
    Coordinator for a single persistent Z-Set table.

    Operates purely on raw memory buffers and Accessors. All PayloadRow
    handling is confined to the external API layer.
    """

    _immutable_fields_ = [
        "schema",
        "table_id",
        "directory",
        "name",
        "ref_counter",
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

        try:
            rposix.mkdir(directory, 0o755)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

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
    # AbstractTable Implementation (Pure Accessor/Batch Path)
    # -------------------------------------------------------------------------

    def get_schema(self):
        return self.schema

    def create_cursor(self):
        num_shards = len(self.index.handles)
        cs = newlist_hint(1 + num_shards)

        cs.append(cursor.MemTableCursor(self.memtable))
        for h in self.index.handles:
            cs.append(cursor.ShardCursor(h.view))

        return cursor.UnifiedCursor(self.schema, cs)

    def create_scratch_table(self, name, schema):
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

    def ingest_batch(self, batch):
        """
        Durable Z-Set batch update. Bypasses all Python object overhead
        by reading directly from the batch's raw memory arenas.
        """
        if self.is_closed:
            raise errors.StorageError("Table is closed")

        if batch.length() == 0:
            return

        lsn = self.current_lsn
        self.current_lsn += r_uint64(1)

        # 1. Durability: Write to WAL (Zero-copy)
        self.wal_writer.append_batch(lsn, self.table_id, batch)

        # 2. Visibility: Update MemTable (Zero-copy)
        self.memtable.upsert_batch(batch)

    def get_weight(self, key, accessor):
        """
        Returns the net algebraic weight for a specific record.
        Zero-allocation: uses the provided accessor for SkipList search
        and columnar shard comparisons.
        """
        r_key = r_uint128(key)
        total_w = r_int64(0)

        # 1. Check SkipList MemTable
        # Calculate hash for SkipList O(1) payload check directly from accessor.
        # We must capture the scratch buffer (h_buf) to free it, preventing leaks.
        h_val, h_buf, _ = serialize.compute_hash(
            self.schema, accessor, lltype.nullptr(rffi.CCHARP.TO), 0
        )
        try:
            node_off = self.memtable._find_exact_values(r_key, h_val, accessor)
            if node_off != 0:
                total_w += node_get_weight(self.memtable.arena.base_ptr, node_off)
        finally:
            if h_buf:
                lltype.free(h_buf, flavor="raw")

        # 2. Check Columnar Shards via Index
        shard_matches = self.index.find_all_shards_and_indices(r_key)
        if shard_matches:
            soa = storage_comparator.SoAAccessor(self.schema)

            for handle, row_idx in shard_matches:
                view = handle.view
                idx = row_idx
                while idx < view.count:
                    # Check PK Match
                    if self.schema.get_pk_column().field_type.size == 16:
                        if view.get_pk_u128(idx) != r_key:
                            break
                    else:
                        if r_uint128(view.get_pk_u64(idx)) != r_key:
                            break

                    # Check Payload Semantic Equality
                    soa.set_row(view, idx)
                    if core_comparator.compare_rows(self.schema, soa, accessor) == 0:
                        total_w += view.get_weight(idx)
                    idx += 1

        return total_w

    # -------------------------------------------------------------------------
    # Internal Logic
    # -------------------------------------------------------------------------

    def recover_from_wal(self, wal_path):
        """
        Replays WAL blocks into the MemTable.
        Zero-copy: uses RawWALAccessor to bypass PayloadRow instantiation.
        """
        if not os.path.exists(wal_path):
            return

        boundary = self.current_lsn
        reader = wal.WALReader(wal_path, self.schema)

        # Pre-allocate recovery accessor
        rec_acc = storage_comparator.RawWALAccessor(self.schema)

        try:
            while True:
                block = reader.read_next_block()
                if block is None:
                    break

                try:
                    if block.tid != self.table_id or block.lsn < boundary:
                        continue

                    for rec in block.records:
                        rec_acc.set_record(rec)
                        self.memtable._upsert_internal(rec.pk, rec.weight, rec_acc)

                    if block.lsn >= self.current_lsn:
                        self.current_lsn = block.lsn + r_uint64(1)
                finally:
                    block.free()
        finally:
            reader.close()

    def flush(self):
        """Transitions MemTable state to a permanent columnar shard."""
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
