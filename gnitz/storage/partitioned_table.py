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
from gnitz.storage.cursor import UnifiedCursor
from gnitz.storage.table import PersistentTable
from gnitz.storage.ephemeral_table import EphemeralTable
from gnitz.catalog.system_tables import FIRST_USER_TABLE_ID

NUM_PARTITIONS = 256


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
    """

    _immutable_fields_ = [
        'schema', 'num_partitions', 'partitions[*]',
        'directory', 'name',
    ]

    def __init__(self, directory, name, schema, num_partitions, partitions):
        self.directory = directory
        self.name = name
        self.schema = schema
        self.num_partitions = num_partitions
        self.partitions = partitions

    def get_schema(self):
        return self.schema

    def ingest_batch(self, batch):
        n = batch.length()
        if n == 0:
            return

        num_p = self.num_partitions
        if num_p == 1:
            self.partitions[0].ingest_batch(batch)
            return

        # Build per-partition sub-batches
        sub_batches = [None] * num_p
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

        # Ingest non-empty sub-batches and free them
        for p in range(num_p):
            sb = sub_batches[p]
            if sb is not None:
                self.partitions[p].ingest_batch(sb)
                sb.free()

    def create_cursor(self):
        num_p = self.num_partitions
        if num_p == 1:
            return self.partitions[0].create_cursor()

        cursors = newlist_hint(num_p)
        for p in range(num_p):
            cursors.append(self.partitions[p].create_cursor())
        return UnifiedCursor(self.schema, cursors)

    def has_pk(self, key):
        if self.num_partitions == 1:
            return self.partitions[0].has_pk(key)
        r_key = r_uint128(key)
        pk_lo = r_uint64(r_key)
        pk_hi = r_uint64(r_key >> 64)
        p = _partition_for_key(pk_lo, pk_hi)
        return self.partitions[p].has_pk(key)

    def get_weight(self, key, accessor):
        if self.num_partitions == 1:
            return self.partitions[0].get_weight(key, accessor)
        r_key = r_uint128(key)
        pk_lo = r_uint64(r_key)
        pk_hi = r_uint64(r_key >> 64)
        p = _partition_for_key(pk_lo, pk_hi)
        return self.partitions[p].get_weight(key, accessor)

    def create_child(self, name, schema):
        return self.partitions[0].create_child(name, schema)

    def flush(self):
        last = ""
        for p in range(self.num_partitions):
            result = self.partitions[p].flush()
            if result:
                last = result
        return last

    def close(self):
        for p in range(self.num_partitions):
            self.partitions[p].close()


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


def make_partitioned_persistent(directory, name, schema, table_id, num_partitions):
    """Creates a PartitionedTable backed by PersistentTable partitions."""
    _ensure_dir(directory)
    arena_size = _partition_arena_size(num_partitions)
    partitions = [None] * num_partitions
    for p in range(num_partitions):
        if num_partitions == 1:
            part_dir = directory + "/part_0"
        else:
            part_dir = directory + "/part_" + str(p)
        partitions[p] = PersistentTable(
            part_dir, name, schema, table_id=table_id,
            memtable_arena_size=arena_size,
        )
    return PartitionedTable(directory, name, schema, num_partitions, partitions)


def make_partitioned_ephemeral(directory, name, schema, table_id, num_partitions):
    """Creates a PartitionedTable backed by EphemeralTable partitions."""
    _ensure_dir(directory)
    arena_size = _partition_arena_size(num_partitions)
    partitions = [None] * num_partitions
    for p in range(num_partitions):
        if num_partitions == 1:
            part_dir = directory + "/part_0"
        else:
            part_dir = directory + "/part_" + str(p)
        partitions[p] = EphemeralTable(
            part_dir, name, schema, table_id=table_id,
            memtable_arena_size=arena_size,
        )
    return PartitionedTable(directory, name, schema, num_partitions, partitions)
