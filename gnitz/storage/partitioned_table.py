# gnitz/storage/partitioned_table.py

import errno
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
    r_ulonglonglong as r_uint128,
    intmask,
)
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import xxh
from gnitz.core.store import ZSetStore
from gnitz.core.batch import ArenaZSetBatch
from gnitz.core.store import AbstractCursor
from gnitz.storage.cursor import UnifiedCursor
from gnitz.storage.table import PersistentTable
from gnitz.storage.ephemeral_table import EphemeralTable
from gnitz.catalog.system_tables import FIRST_USER_TABLE_ID

NUM_PARTITIONS = 256


class _EmptyCursor(AbstractCursor):
    """A cursor that is immediately exhausted. Used when all partitions are closed."""

    def seek(self, key):
        pass

    def advance(self):
        pass

    def is_valid(self):
        return False

    def is_exhausted(self):
        return True

    def key(self):
        return r_uint128(0)

    def weight(self):
        return r_int64(0)

    def get_accessor(self):
        return None

    def close(self):
        pass


def get_num_partitions(table_id):
    """Returns 1 for system tables, NUM_PARTITIONS for user tables."""
    if table_id < FIRST_USER_TABLE_ID:
        return 1
    return NUM_PARTITIONS


def _partition_for_key(pk_lo, pk_hi):
    """Compute partition index from a PK's lo/hi components."""
    h = xxh.hash_u128_inline(pk_lo, pk_hi)
    return intmask(r_uint64(h) & r_uint64(0xFF))


class PartitionedTable(ZSetStore):
    """
    A ZSetStore wrapping N partition-level stores (PersistentTable or
    EphemeralTable). Routes rows by hash(PK) % N.

    partitions is a dense list containing only live stores.
    part_offset is the global partition index of partitions[0].
    num_partitions is the total global count (256 for user tables, 1 for system).
    """

    _immutable_fields_ = [
        "schema", "num_partitions",
        "directory", "name",
    ]

    def __init__(self, directory, name, schema, num_partitions, partitions, part_offset):
        self.directory = directory
        self.name = name
        self.schema = schema
        self.num_partitions = num_partitions
        self.partitions = partitions
        self.part_offset = part_offset

    def get_schema(self):
        return self.schema

    def ingest_batch_memonly(self, batch):
        """Delegates to the partition's ingest_batch_memonly (bypasses WAL).
        Only meaningful for num_partitions == 1 (system tables).
        """
        if batch.length() == 0 or len(self.partitions) == 0:
            return
        self.partitions[0].ingest_batch_memonly(batch)

    def ingest_batch(self, batch):
        n = batch.length()
        if n == 0:
            return

        num_live = len(self.partitions)
        if num_live == 0:
            return

        if self.num_partitions == 1:
            self.partitions[0].ingest_batch(batch)
            return

        # Build per-partition sub-batches (indexed by global partition)
        sub_batches = [None] * self.num_partitions
        schema = self.schema

        for i in range(n):
            pk_lo = r_uint64(batch._read_pk_lo(i))
            pk_hi = r_uint64(batch._read_pk_hi(i))
            p = _partition_for_key(pk_lo, pk_hi)
            sb = sub_batches[p]
            if sb is None:
                sb = ArenaZSetBatch(schema)
                sub_batches[p] = sb
            sb._direct_append_row(batch, i, batch.get_weight(i))

        # Propagate sortedness: subsequences of a sorted sequence are sorted
        if batch._sorted:
            p2 = 0
            while p2 < self.num_partitions:
                sb = sub_batches[p2]
                if sb is not None:
                    sb._sorted = True
                p2 += 1

        # Ingest owned sub-batches, free all sub-batches
        offset = self.part_offset
        for p in range(self.num_partitions):
            sb = sub_batches[p]
            if sb is not None:
                local = p - offset
                if 0 <= local < num_live:
                    self.partitions[local].ingest_batch(sb)
                sb.free()

    def create_cursor(self):
        num_live = len(self.partitions)
        if num_live == 0:
            return _EmptyCursor()

        if self.num_partitions == 1:
            return self.partitions[0].create_cursor()

        cursors = newlist_hint(num_live)
        for local in range(num_live):
            cursors.append(self.partitions[local].create_cursor())
        return UnifiedCursor(self.schema, cursors)

    def has_pk(self, key):
        if len(self.partitions) == 0:
            return False
        if self.num_partitions == 1:
            return self.partitions[0].has_pk(key)
        r_key = r_uint128(key)
        pk_lo = r_uint64(r_key)
        pk_hi = r_uint64(r_key >> 64)
        p = _partition_for_key(pk_lo, pk_hi)
        local = p - self.part_offset
        if local < 0 or local >= len(self.partitions):
            return False
        return self.partitions[local].has_pk(key)

    def get_weight(self, key, accessor):
        if len(self.partitions) == 0:
            return r_int64(0)
        if self.num_partitions == 1:
            return self.partitions[0].get_weight(key, accessor)
        r_key = r_uint128(key)
        pk_lo = r_uint64(r_key)
        pk_hi = r_uint64(r_key >> 64)
        p = _partition_for_key(pk_lo, pk_hi)
        local = p - self.part_offset
        if local < 0 or local >= len(self.partitions):
            return r_int64(0)
        return self.partitions[local].get_weight(key, accessor)

    def create_child(self, name, schema):
        return self.partitions[0].create_child(name, schema)

    def flush(self):
        last = ""
        for local in range(len(self.partitions)):
            result = self.partitions[local].flush()
            if result:
                last = result
        return last

    def close_partitions_outside(self, start, end):
        """Close partitions not in [start, end). Rebuilds the dense list."""
        old_offset = self.part_offset
        old_parts = self.partitions

        # Close partitions outside [start, end)
        for local in range(len(old_parts)):
            p = local + old_offset
            if p < start or p >= end:
                old_parts[local].close()

        # Rebuild dense list for the new range
        new_len = end - start
        if new_len <= 0:
            self.partitions = newlist_hint(0)
            self.part_offset = 0
            return
        base = start - old_offset
        new_parts = newlist_hint(new_len)
        for i in range(new_len):
            new_parts.append(old_parts[base + i])
        self.partitions = new_parts
        self.part_offset = start

    def close_all_partitions(self):
        """Close all partitions. Master use after fork."""
        for local in range(len(self.partitions)):
            self.partitions[local].close()
        self.partitions = newlist_hint(0)
        self.part_offset = 0

    def close(self):
        for local in range(len(self.partitions)):
            self.partitions[local].close()


def _partition_arena_size(num_partitions):
    """Scales per-partition memtable arena inversely with partition count."""
    if num_partitions <= 1:
        return 1 * 1024 * 1024  # 1MB for single partition
    # 64KB per partition (256 * 64KB = 16MB total)
    return 64 * 1024


def _ensure_dir(path):
    try:
        rposix.mkdir(path, 0o755)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def make_partitioned_persistent(directory, name, schema, table_id, num_partitions,
                                part_start=0, part_end=-1):
    """Creates a PartitionedTable backed by PersistentTable partitions.
    Only partitions in [part_start, part_end) are created.
    """
    if part_end == -1:
        part_end = num_partitions
    _ensure_dir(directory)
    arena_size = _partition_arena_size(num_partitions)
    partitions = newlist_hint(part_end - part_start)
    for p in range(part_start, part_end):
        if num_partitions == 1:
            part_dir = directory + "/part_0"
        else:
            part_dir = directory + "/part_" + str(p)
        partitions.append(PersistentTable(
            part_dir, name, schema, table_id=table_id,
            memtable_arena_size=arena_size,
        ))
    return PartitionedTable(directory, name, schema, num_partitions, partitions, part_start)


def make_partitioned_ephemeral(directory, name, schema, table_id, num_partitions,
                               part_start=0, part_end=-1):
    """Creates a PartitionedTable backed by EphemeralTable partitions.
    Only partitions in [part_start, part_end) are created.
    """
    if part_end == -1:
        part_end = num_partitions
    _ensure_dir(directory)
    arena_size = _partition_arena_size(num_partitions)
    partitions = newlist_hint(part_end - part_start)
    for p in range(part_start, part_end):
        if num_partitions == 1:
            part_dir = directory + "/part_0"
        else:
            part_dir = directory + "/part_" + str(p)
        partitions.append(EphemeralTable(
            part_dir, name, schema, table_id=table_id,
            memtable_arena_size=arena_size,
        ))
    return PartitionedTable(directory, name, schema, num_partitions, partitions, part_start)
