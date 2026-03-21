# gnitz/storage/partitioned_table.py

import errno
from rpython.rlib import rposix
from rpython.rlib.rarithmetic import (
    r_int64,
    r_uint64,
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

    def seek(self, key_lo, key_hi):
        pass

    def advance(self):
        pass

    def is_valid(self):
        return False

    def is_exhausted(self):
        return True

    def key_lo(self):
        return r_uint64(0)

    def key_hi(self):
        return r_uint64(0)

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

    def set_has_wal(self, flag):
        """Propagate _has_wal to all partition sub-stores."""
        for i in range(len(self.partitions)):
            self.partitions[i]._has_wal = flag

    def get_max_flushed_lsn(self):
        """Max current_lsn across all partitions (for SAL recovery)."""
        max_lsn = r_uint64(0)
        for i in range(len(self.partitions)):
            if self.partitions[i].current_lsn > max_lsn:
                max_lsn = self.partitions[i].current_lsn
        return max_lsn

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

        schema = self.schema

        # Pass 1: collect per-partition index lists
        part_indices = newlist_hint(self.num_partitions)
        for _p in range(self.num_partitions):
            part_indices.append(newlist_hint(0))

        for i in range(n):
            pk_lo = r_uint64(batch._read_pk_lo(i))
            pk_hi = r_uint64(batch._read_pk_hi(i))
            p = _partition_for_key(pk_lo, pk_hi)
            part_indices[p].append(i)

        # Pass 2: bulk-copy each partition's rows using _copy_rows_indexed_src_weights
        sorted_flag = batch._sorted
        offset = self.part_offset
        for p in range(self.num_partitions):
            pidx = part_indices[p]
            if len(pidx) == 0:
                continue
            local = p - offset
            if local < 0 or local >= num_live:
                continue
            sb = ArenaZSetBatch(schema, initial_capacity=len(pidx))
            sb._copy_rows_indexed_src_weights(batch, pidx)
            if sorted_flag:
                sb._sorted = True
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

    def retract_pk(self, key_lo, key_hi, out_batch):
        if len(self.partitions) == 0:
            return False
        if self.num_partitions == 1:
            return self.partitions[0].retract_pk(key_lo, key_hi, out_batch)
        p = _partition_for_key(key_lo, key_hi)
        local = p - self.part_offset
        if local < 0 or local >= len(self.partitions):
            return False
        return self.partitions[local].retract_pk(key_lo, key_hi, out_batch)

    def has_pk(self, key_lo, key_hi):
        if len(self.partitions) == 0:
            return False
        if self.num_partitions == 1:
            return self.partitions[0].has_pk(key_lo, key_hi)
        p = _partition_for_key(key_lo, key_hi)
        local = p - self.part_offset
        if local < 0 or local >= len(self.partitions):
            return False
        return self.partitions[local].has_pk(key_lo, key_hi)

    def get_weight(self, key_lo, key_hi, accessor):
        if len(self.partitions) == 0:
            return r_int64(0)
        if self.num_partitions == 1:
            return self.partitions[0].get_weight(key_lo, key_hi, accessor)
        p = _partition_for_key(key_lo, key_hi)
        local = p - self.part_offset
        if local < 0 or local >= len(self.partitions):
            return r_int64(0)
        return self.partitions[local].get_weight(key_lo, key_hi, accessor)

    def create_child(self, name, schema):
        return self.partitions[0].create_child(name, schema)

    def flush(self):
        last = ""
        for local in range(len(self.partitions)):
            result = self.partitions[local].flush()
            if result:
                last = result
        return last

    def compact_if_needed(self):
        for local in range(len(self.partitions)):
            self.partitions[local].compact_if_needed()

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
