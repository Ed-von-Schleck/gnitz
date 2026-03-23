# gnitz/storage/table.py

import os
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    intmask,
)

from gnitz.core import errors
from gnitz.storage import (
    wal,
    index,
    manifest,
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
        "ref_counter",
        "manifest_manager",
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
        # EphemeralTable.__init__ handles directory creation, ref_counter,
        # index, and memtable initialization.
        EphemeralTable.__init__(
            self, directory, name, schema, table_id, 
            memtable_arena_size, validate_checksums
        )

        # Monotonic counter incremented on every flush().
        # Used by cursors to detect when the MemTable has been rotated.
        self._cursor_generation = 0

        # Avoid os.path.join (Appendix A §10 slicing proof failure)
        manifest_path = directory + "/MANIFEST"
        self.manifest_manager = manifest.ManifestManager(manifest_path)

        # 1. Recover Shard Index from Manifest
        if self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            try:
                self.index.populate_from_reader(self.table_id, reader)
                self.current_lsn = reader.global_max_lsn + r_uint64(1)
            finally:
                reader.close()
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
        if not self.index.needs_compaction:
            return
        self.index.run_compact()
        self.manifest_manager.publish_new_version(
            self.index.get_metadata_list(), self.index.max_lsn()
        )

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
        h = index.ShardHandle(
            shard_path,
            self.schema,
            r_uint64(0),
            lsn_max,
            validate_checksums=self.validate_checksums,
        )

        # 2. Logic: Update the Manifest (The Authority)
        if h.view.count > 0:
            self.index.add_handle(h)
            self.manifest_manager.publish_new_version(
                self.index.get_metadata_list(), lsn_max
            )
            # 3. Cleanup: WAL is no longer needed for recovered data
            self.wal_writer.truncate_before_lsn(self.current_lsn)
        else:
            # Ghost Shard cleanup
            h.close()
            try:
                mmap_posix.unlinkat_c(self._dirfd, shard_name)
            except mmap_posix.MMapError:
                pass
            self.manifest_manager.publish_new_version(
                self.index.get_metadata_list(), lsn_max
            )

        # One dir fsync via cached dirfd — no open/close needed.
        mmap_posix.fsync_c(self._dirfd)

        # 4. Rotation: reset MemTable (keeps accumulator + bloom buffers)
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
        # index.close_all(), and setting is_closed = True.
        EphemeralTable.close(self)
