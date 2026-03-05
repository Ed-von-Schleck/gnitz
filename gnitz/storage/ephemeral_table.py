# gnitz/storage/ephemeral_table.py

import errno
import os
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import types, errors
from gnitz.core import comparator as core_comparator
from gnitz.core.store import ZSetStore
from gnitz.core.keys import promote_to_index_key
from gnitz.storage import (
    index,
    memtable,
    refcount,
    comparator as storage_comparator,
    cursor,
)


def _name_to_tid(name):
    """
    Standardizes name-to-TID hashing for internal state tables.
    """
    tid = 0
    for char in name:
        tid = (tid * 31 + ord(char)) & 0x7FFFFFFF
    if tid == 0:
        tid = 1
    return tid


class EphemeralTable(ZSetStore):
    """
    Scratch Z-Set storage for internal VM state or Secondary Indices.
    Bypasses the Write-Ahead Log to maximize throughput.
    Inherits directly from ZSetStore; PersistentTable inherits from this.
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
        table_id=0,
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

        self.index = index.ShardIndex(table_id, schema, self.ref_counter)
        self.memtable = memtable.MemTable(schema, memtable_arena_size)

    # -- ZSetStore Interface Implementation -----------------------------------

    def get_schema(self):
        return self.schema

    def create_child(self, name, schema):
        """Returns another EphemeralTable instance for recursive state."""
        # Avoid os.path.join (Appendix A §10 slicing proof failure)
        scratch_dir = self.directory + "/scratch_" + name
        tid = _name_to_tid(name)

        return EphemeralTable(
            scratch_dir,
            name,
            schema,
            table_id=tid,
            memtable_arena_size=self.memtable.max_bytes,
        )

    def create_cursor(self):
        num_shards = len(self.index.handles)
        cs = newlist_hint(1 + num_shards)

        cs.append(cursor.MemTableCursor(self.memtable))
        for h in self.index.handles:
            cs.append(cursor.ShardCursor(h.view))

        return cursor.UnifiedCursor(self.schema, cs)

    def has_pk(self, key):
        """
        Fast-path existence check for a Primary Key.
        Returns True if the net algebraic weight across MemTable and Shards is > 0.
        """
        r_key = r_uint128(key)

        # 1. MemTable (Bloom-filtered)
        if self.memtable.may_contain_pk(r_key):
            total_w = self.memtable.get_weight_for_pk(r_key)
        else:
            total_w = r_int64(0)

        # 2. Columnar Shards via Index
        shard_matches = self.index.find_all_shards_and_indices(r_key)
        if shard_matches:
            is_u128 = self.schema.get_pk_column().field_type.size == 16
            for handle, row_idx in shard_matches:
                view = handle.view
                idx = row_idx
                while idx < view.count:
                    if is_u128:
                        if view.get_pk_u128(idx) != r_key:
                            break
                    else:
                        if r_uint128(view.get_pk_u64(idx)) != r_key:
                            break

                    total_w += view.get_weight(idx)
                    idx += 1

        return total_w > 0

    def get_weight(self, key, accessor):
        """
        Returns the net algebraic weight for a specific record.
        """
        r_key = r_uint128(key)
        total_w = r_int64(0)

        # 1. Check MemTable (Bloom-filtered)
        if self.memtable.may_contain_pk(r_key):
            total_w += self.memtable.find_weight_for_row(r_key, accessor)

        # 2. Check Columnar Shards via Index
        shard_matches = self.index.find_all_shards_and_indices(r_key)
        if shard_matches:
            soa = storage_comparator.SoAAccessor(self.schema)

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
                    if core_comparator.compare_rows(self.schema, soa, accessor) == 0:
                        total_w += view.get_weight(idx)
                    idx += 1

        return total_w

    def ingest_batch(self, batch):
        if self.is_closed:
            raise errors.StorageError("Table is closed")

        if batch.length() == 0:
            return

        try:
            self.memtable.upsert_batch(batch)
            if self.memtable.should_flush():
                self.flush()
        except errors.MemTableFullError:
            self.flush()
            self.memtable.upsert_batch(batch)

    def flush(self):
        """Transitions MemTable state to a temporary columnar shard."""
        if self.is_closed:
            raise errors.StorageError("Table is closed")

        # Avoid os.path.join (Appendix A §10)
        shard_name = "eph_shard_%d_%d.db" % (
            self.table_id,
            self.memtable.batch.length(),
        )
        shard_path = self.directory + "/" + shard_name

        self.memtable.flush(shard_path, self.table_id)

        if not os.path.exists(shard_path):
            return ""

        h = index.ShardHandle(
            shard_path,
            self.schema,
            r_uint64(0),
            r_uint64(0),
            validate_checksums=self.validate_checksums,
        )

        if h.view.count > 0:
            self.index.add_handle(h)
        else:
            h.close()
            try:
                os.unlink(shard_path)
            except OSError:
                pass

        max_bytes = self.memtable.max_bytes
        self.memtable.free()
        self.memtable = memtable.MemTable(self.schema, max_bytes)

        return shard_path

    def close(self):
        if self.is_closed:
            return
        if self.memtable:
            self.memtable.free()
        if self.index:
            self.index.close_all()
        self.is_closed = True

    # -- Internal / Index Specific APIs ---------------------------------------

    def ingest_projection(
        self, source_batch, source_col_idx, source_col_type, payload_accessor, is_unique
    ):
        """
        Hot-path projection kernel for secondary indices.
        Extracts index keys from source_batch and injects them into the local store.
        """
        n = source_batch.length()
        if n == 0:
            return

        acc = source_batch.get_accessor(0)

        for i in range(n):
            source_batch.bind_accessor(i, acc)

            if acc.is_null(source_col_idx):
                continue

            weight = source_batch.get_weight(i)
            if weight == r_int64(0):
                continue

            index_key = promote_to_index_key(acc, source_col_idx, source_col_type)

            if is_unique and weight > r_int64(0):
                if self.has_pk(index_key):
                    raise errors.LayoutError(
                        "Unique index violation on column index %d" % source_col_idx
                    )

            source_pk = source_batch.get_pk(i)
            # Alignment-safe u128 assignment (Appendix A §4)
            payload_accessor.pk_lo = r_uint64(source_pk)
            payload_accessor.pk_hi = r_uint64(source_pk >> 64)

            try:
                self.memtable.upsert_single(index_key, weight, payload_accessor)
            except errors.MemTableFullError:
                self.flush()
                self.memtable.upsert_single(index_key, weight, payload_accessor)

    def ingest_one(self, key, weight, accessor):
        """Cold-path injection for index backfills."""
        try:
            self.memtable.upsert_single(key, weight, accessor)
        except errors.MemTableFullError:
            self.flush()
            self.memtable.upsert_single(key, weight, accessor)
