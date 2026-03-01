# gnitz/storage/table.py

import os
import errno
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import errors
from gnitz.storage import (
    wal,
    index,
    manifest,
    memtable,
    refcount,
    comparator as storage_comparator,
)
from gnitz.storage.ephemeral_table import _StorageBase


def _ensure_dir(path):
    """Ensures a directory exists, ignoring EEXIST."""
    try:
        rposix.mkdir(path, 0o755)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


class PersistentTable(_StorageBase):
    """
    Coordinator for a single durable Z-Set table.
    Manages the lifecycle of the WAL, MemTable, and Columnar Shards.
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

        # Monotonic counter incremented on every flush().
        # Used by cursors to detect when the MemTable has been rotated.
        self._cursor_generation = 0

        self.ref_counter = refcount.RefCounter()

        _ensure_dir(directory)

        manifest_path = os.path.join(directory, "MANIFEST")
        self.manifest_manager = manifest.ManifestManager(manifest_path)

        self.index = index.ShardIndex(table_id, schema, self.ref_counter)
        
        # 1. Recover Shard Index from Manifest
        if self.manifest_manager.exists():
            reader = self.manifest_manager.load_current()
            try:
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
            finally:
                reader.close()
        else:
            self.current_lsn = r_uint64(1)

        # 2. Initialize Volatile State
        self.memtable = memtable.MemTable(schema, memtable_arena_size)

        # 3. Initialize Durability Layer
        wal_path = os.path.join(directory, name + ".wal")
        self.wal_writer = wal.WALWriter(wal_path, self.schema)

        # 4. Replay WAL into MemTable to recover recent un-flushed writes
        self.recover_from_wal(wal_path)

    # ── Mutations ────────────────────────────────────────────────────────────

    def ingest_batch(self, batch):
        """
        Durable Z-Set batch update.
        Atomicity: Batch is committed to WAL before it becomes visible in the MemTable.
        """
        if self.is_closed:
            raise errors.StorageError("Table '%s' is closed" % self.name)

        if batch.length() == 0:
            return

        lsn = self.current_lsn
        self.current_lsn += r_uint64(1)

        # Step 1: Write to Write-Ahead Log (Durability)
        self.wal_writer.append_batch(lsn, self.table_id, batch)

        # Step 2: Write to SkipList (Visibility)
        self.memtable.upsert_batch(batch)

    def recover_from_wal(self, wal_path):
        """
        Synchronizes the MemTable state with the persistent WAL during startup.
        """
        if not os.path.exists(wal_path):
            return

        boundary = self.current_lsn
        reader = wal.WALReader(wal_path, self.schema)

        # Reuse a single accessor for the recovery loop
        rec_acc = storage_comparator.RawWALAccessor(self.schema)

        try:
            while True:
                block = reader.read_next_block()
                if block is None:
                    break

                try:
                    # Only replay batches that haven't been flushed to shards yet
                    if block.tid != self.table_id or block.lsn < boundary:
                        continue

                    for rec in block.records:
                        rec_acc.set_record(rec)
                        self.memtable.upsert_single(rec.get_key(), rec.weight, rec_acc)

                    # Ensure the current LSN is higher than any recovered LSN
                    if block.lsn >= self.current_lsn:
                        self.current_lsn = block.lsn + r_uint64(1)
                finally:
                    block.free()
        finally:
            reader.close()

    # ── Maintenance ──────────────────────────────────────────────────────────

    def flush(self):
        """
        The Point of No Return: Transitions MemTable state to a permanent shard.
        """
        if self.is_closed:
            raise errors.StorageError("Table '%s' is closed" % self.name)

        mt = self.memtable
        if mt is None or mt.is_empty():
            return ""

        shard_name = "shard_%d_%d.db" % (self.table_id, intmask(self.current_lsn))
        shard_path = os.path.join(self.directory, shard_name)

        # 1. Physical: Build and sync the columnar file
        self.memtable.flush(shard_path, self.table_id)

        if not os.path.exists(shard_path):
            return ""

        lsn_max = self.current_lsn - r_uint64(1)
        h = index.ShardHandle(
            shard_path,
            self.schema,
            r_uint64(0),  # We keep min_lsn=0 for base shards
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
                os.unlink(shard_path)
            except OSError:
                pass
            self.manifest_manager.publish_new_version(
                self.index.get_metadata_list(), lsn_max
            )

        # 4. Rotation: Swap MemTable and invalidate cursors
        arena_size = self.memtable.arena.size
        self.memtable.free()
        self.memtable = memtable.MemTable(self.schema, arena_size)

        # Bump generation to notify UnifiedCursors that sources have changed
        self._cursor_generation += 1

        return shard_path

    def close(self):
        """
        Idempotent closure of all file handles and memory arenas.
        """
        if self.is_closed:
            return
        
        self.is_closed = True
        
        if self.wal_writer:
            self.wal_writer.close()
        
        if self.memtable:
            self.memtable.free()
            self.memtable = None
            
        if self.index:
            self.index.close_all()
            self.index = None
