# gnitz/storage/table.py

import os
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    intmask,
)
from rpython.rtyper.lltypesystem import rffi

from gnitz.core import errors
from gnitz.storage import (
    wal,
    engine_ffi,
    memtable,
    mmap_posix,
)
from gnitz.storage.ephemeral_table import EphemeralTable


class PersistentTable(EphemeralTable):
    """
    Coordinator for a single durable Z-Set table.
    Extends EphemeralTable by adding a Write-Ahead Log (WAL) and
    a Manifest for persistent columnar shards.
    """

    _immutable_fields_ = [
        "schema",
        "table_id",
        "directory",
        "name",
        "wal_writer",
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
        # EphemeralTable.__init__ handles directory creation,
        # ShardIndex handle, and memtable initialization.
        EphemeralTable.__init__(
            self, directory, name, schema, table_id,
            memtable_arena_size, validate_checksums
        )

        # Monotonic counter incremented on every flush().
        # Used by cursors to detect when the MemTable has been rotated.
        self._cursor_generation = 0

        # Avoid os.path.join (Appendix A §10 slicing proof failure)
        self._manifest_path = directory + "/MANIFEST"

        # 1. Recover Shard Index from Manifest
        if os.path.exists(self._manifest_path):
            path_c = rffi.str2charp(self._manifest_path)
            try:
                rc = intmask(engine_ffi._shard_index_load_manifest(
                    self._index_handle, path_c))
            finally:
                rffi.free_charp(path_c)
            if rc < 0:
                raise errors.StorageError(
                    "Manifest load failed (error %d)" % rc)
            max_lsn = r_uint64(engine_ffi._shard_index_max_lsn(
                self._index_handle))
            self.current_lsn = max_lsn + r_uint64(1)
        else:
            self.current_lsn = r_uint64(1)

        # 2. Initialize Durability Layer
        wal_path = directory + "/" + name + ".wal"
        self.wal_writer = wal.WALWriter(wal_path, self.schema)

        # 3. Replay WAL into MemTable to recover recent un-flushed writes
        self.recover_from_wal(wal_path)

    def _erase_stale_shards(self):
        """No-op: persistent shards are tracked by MANIFEST and must survive restarts."""

    # -- Cursor Interface Override --------------------------------------------

    def create_cursor(self):
        return self._build_cursor()

    def compact_if_needed(self):
        if not intmask(engine_ffi._shard_index_needs_compaction(self._index_handle)):
            return
        rc = intmask(engine_ffi._shard_index_compact(self._index_handle))
        if rc < 0:
            raise errors.StorageError("ShardIndex compact failed (error %d)" % rc)
        self._publish_manifest()
        mmap_posix.fsync_c(self._dirfd)
        engine_ffi._shard_index_try_cleanup(self._index_handle)

    def _publish_manifest(self):
        """Publish current ShardIndex state as a manifest file."""
        path_c = rffi.str2charp(self._manifest_path)
        try:
            rc = intmask(engine_ffi._shard_index_publish_manifest(
                self._index_handle, path_c))
        finally:
            rffi.free_charp(path_c)
        if rc < 0:
            raise errors.StorageError(
                "Manifest publish failed (error %d)" % rc)

    # -- Mutations ------------------------------------------------------------

    def ingest_batch(self, batch):
        """
        Durable Z-Set batch update.
        Atomicity: Batch is committed to WAL before it becomes visible in the
        MemTable.
        """
        if self.is_closed:
            raise errors.StorageError("Table '%s' is closed" % self.name)

        if batch.length() == 0:
            return

        lsn = self.current_lsn
        self.current_lsn += r_uint64(1)

        # Step 1: Write to Write-Ahead Log (Durability)
        if self._has_wal:
            self.wal_writer.append_batch(lsn, self.table_id, batch)

        # Step 2: Write to MemTable (Visibility)
        # Guard against MemTableFullError: the WAL write above has already
        # committed, so the data is durable. Flush the MemTable to a shard
        # and retry so that the batch also becomes visible in memory.
        try:
            self.memtable.upsert_batch(batch)
            if self.memtable.should_flush():
                self.flush()
        except errors.MemTableFullError:
            self.flush()
            self.memtable.upsert_batch(batch)

    def ingest_batch_memonly(self, batch):
        """Ingest into memtable only, bypassing the WAL.

        Used by workers for DDL sync: the master owns the system-table WAL,
        so workers must never touch it. This method uses the EphemeralTable
        flush path (no WAL, no manifest) if the memtable overflows.
        """
        if self.is_closed:
            raise errors.StorageError("Table '%s' is closed" % self.name)
        if batch.length() == 0:
            return
        try:
            self.memtable.upsert_batch(batch)
            if self.memtable.should_flush():
                EphemeralTable.flush(self)
        except errors.MemTableFullError:
            EphemeralTable.flush(self)
            self.memtable.upsert_batch(batch)

    def recover_from_wal(self, wal_path):
        """
        Synchronizes the MemTable state with the persistent WAL during startup.
        """
        if not os.path.exists(wal_path):
            return

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

                    self.memtable.upsert_batch(block.batch)

                    if block.lsn >= self.current_lsn:
                        self.current_lsn = block.lsn + r_uint64(1)
                finally:
                    block.free()
        finally:
            reader.close()

    # -- Maintenance ----------------------------------------------------------

    def flush(self):
        """
        Transitions MemTable state to a permanent shard and updates the manifest.
        """
        if self.is_closed:
            raise errors.StorageError("Table '%s' is closed" % self.name)

        mt = self.memtable
        if mt.is_empty():
            return ""

        shard_name = "shard_%d_%d.db" % (self.table_id, intmask(self.current_lsn))

        # 1. Physical: Build and sync the columnar file
        wrote = self.memtable.flush(self._dirfd, shard_name, self.table_id)

        if not wrote:
            return ""

        shard_path = self.directory + "/" + shard_name
        lsn_max = self.current_lsn - r_uint64(1)

        # Check row count
        shard_count = self._check_shard_row_count(shard_path)
        has_data = shard_count > 0

        # 2. Logic: Update the Manifest (The Authority)
        if has_data:
            path_c = rffi.str2charp(shard_path)
            try:
                rc = intmask(engine_ffi._shard_index_add_shard(
                    self._index_handle,
                    path_c,
                    rffi.cast(rffi.ULONGLONG, r_uint64(0)),
                    rffi.cast(rffi.ULONGLONG, lsn_max),
                ))
            finally:
                rffi.free_charp(path_c)
            if rc < 0:
                raise errors.StorageError(
                    "ShardIndex add_shard failed (error %d)" % rc)
            self._publish_manifest()
        else:
            # Ghost Shard cleanup
            try:
                mmap_posix.unlinkat_c(self._dirfd, shard_name)
            except mmap_posix.MMapError:
                pass
            self._publish_manifest()

        # 3. Dir fsync — makes shard rename + manifest rename durable.
        mmap_posix.fsync_c(self._dirfd)

        # 4. WAL truncation — safe because manifest is now durable.
        if has_data:
            self.wal_writer.truncate_before_lsn(self.current_lsn)

        # 5. Rotation: reset MemTable (keeps accumulator + bloom buffers)
        self.memtable.reset()

        # Bump generation to notify UnifiedCursors that sources have changed
        self._cursor_generation += 1

        return shard_path

    def close(self):
        """
        Idempotent closure. The WAL writer must be closed before the parent
        frees the memory arenas to ensure consistent teardown if a crash occurs.
        """
        if self.is_closed:
            return

        if self.wal_writer:
            self.wal_writer.close()

        # Delegates to EphemeralTable to handle memtable.free(),
        # ShardIndex close, and setting is_closed = True.
        EphemeralTable.close(self)
