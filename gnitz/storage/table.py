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


class PersistentTable(_StorageBase):
    """
    Coordinator for a single persistent Z-Set table.
    Manages durability via WAL and versioning via Manifest.
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

        # Monotonic counter incremented on every flush(). Any UnifiedCursor
        # that was constructed before a flush() must be discarded and rebuilt,
        # because flush() frees the old MemTable arena and replaces the object.
        self._cursor_generation = 0

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
    # Mutations
    # -------------------------------------------------------------------------

    def ingest_batch(self, batch):
        """
        Durable Z-Set batch update.
        Writes to WAL (durability) then to MemTable (visibility).
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

    def recover_from_wal(self, wal_path):
        """
        Replays WAL blocks into the MemTable.
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
                        # Step 8: Call renamed upsert_single
                        self.memtable.upsert_single(rec.pk, rec.weight, rec_acc)

                    if block.lsn >= self.current_lsn:
                        self.current_lsn = block.lsn + r_uint64(1)
                finally:
                    block.free()
        finally:
            reader.close()

    # -------------------------------------------------------------------------
    # Maintenance
    # -------------------------------------------------------------------------

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

        # Bump generation to invalidate cached cursors (they contain dead MemTable refs)
        self._cursor_generation += 1

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
