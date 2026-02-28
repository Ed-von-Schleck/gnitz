# gnitz/storage/ephemeral_table.py

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

from gnitz.core import types, errors, serialize
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


class _StorageBase(ZSetStore):
    """
    Shared base class for Persistent and Ephemeral tables.
    Deduplicates logic related to schema management and read-path operations.
    """

    _immutable_fields_ = [
        "schema",
        "table_id",
        "directory",
        "name",
        "ref_counter",
    ]

    def get_schema(self):
        return self.schema

    def create_child(self, name, schema):
        """Returns another EphemeralTable instance for recursive state."""
        scratch_dir = os.path.join(self.directory, "scratch_" + name)
        tid = _name_to_tid(name)

        return EphemeralTable(
            scratch_dir,
            name,
            schema,
            table_id=tid,
            memtable_arena_size=self.memtable.arena.size,
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

        # 1. SkipList MemTable
        total_w = self.memtable.get_weight_for_pk(r_key)

        # 2. Columnar Shards via Index
        shard_matches = self.index.find_all_shards_and_indices(r_key)
        if shard_matches:
            is_u128 = self.schema.get_pk_column().field_type.size == 16
            for handle, row_idx in shard_matches:
                view = handle.view
                idx = row_idx
                while idx < view.count:
                    # Check PK Match
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
        Zero-allocation: uses the provided accessor for SkipList search
        and columnar shard comparisons.
        """
        r_key = r_uint128(key)
        total_w = r_int64(0)

        # 1. Check SkipList MemTable
        h_val, h_buf, h_cap = serialize.compute_hash(
            self.schema, accessor, self.memtable.hash_buf, self.memtable.hash_buf_cap
        )
        self.memtable.hash_buf = h_buf
        self.memtable.hash_buf_cap = h_cap

        node_off = self.memtable._find_exact_values(r_key, h_val, accessor)
        if node_off != 0:
            from gnitz.storage.memtable_node import node_get_weight

            total_w += node_get_weight(self.memtable.arena.base_ptr, node_off)

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


class EphemeralTable(_StorageBase):
    """
    Scratch Z-Set storage for internal VM state or Secondary Indices.
    Bypasses the Write-Ahead Log to maximize throughput.
    """

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
        except OSError:
            pass

        self.index = index.ShardIndex(table_id, schema, self.ref_counter)
        self.memtable = memtable.MemTable(schema, memtable_arena_size)

    def ingest_batch(self, batch):
        if self.is_closed:
            raise errors.StorageError("Table is closed")

        if batch.length() == 0:
            return

        self.memtable.upsert_batch(batch)

    def ingest_projection(
        self, source_batch, source_col_idx, source_col_type, payload_accessor, is_unique
    ):
        """
        Hot-path projection kernel for secondary indices.
        Extracts index keys from source_batch and injects them into the local store.
        """
        n = source_batch.length()
        for i in range(n):
            acc = source_batch.get_accessor(i)
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
            # Mutation of payload_accessor (IndexPayloadAccessor) to avoid allocation
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

    def flush(self):
        """Transitions MemTable state to a temporary columnar shard."""
        if self.is_closed:
            raise errors.StorageError("Table is closed")

        shard_name = "eph_shard_%d_%d.db" % (
            self.table_id,
            intmask(rffi.cast(rffi.SIZE_T, self.memtable.arena.offset)),
        )
        shard_path = os.path.join(self.directory, shard_name)

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

        arena_size = self.memtable.arena.size
        self.memtable.free()
        self.memtable = memtable.MemTable(self.schema, arena_size)

        return shard_path

    def close(self):
        if self.is_closed:
            return
        if self.memtable:
            self.memtable.free()
        if self.index:
            self.index.close_all()
        self.is_closed = True
