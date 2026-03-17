# gnitz/dbsp/ops/exchange.py
#
# Column-based hash for repartitioning batches across workers.

from rpython.rlib.rarithmetic import r_uint64, intmask
from gnitz.core import xxh, types
from gnitz.core.batch import ArenaZSetBatch
from gnitz.storage.partitioned_table import _partition_for_key


def _mix64(v):
    """Murmur3 64-bit finalizer — must match reduce.py._mix64 exactly."""
    v = r_uint64(v)
    v ^= v >> 33
    v = r_uint64(v * r_uint64(0xFF51AFD7ED558CCD))
    v ^= v >> 33
    v = r_uint64(v * r_uint64(0xC4CEB9FE1A85EC53))
    v ^= v >> 33
    return v


def _read_col_or_pk(batch, row_idx, col_idx):
    """Read column value as u64, handling the PK column correctly.

    col_bufs[pk_index] is an empty buffer (stride=0) — PK data lives in
    pk_lo_buf.  This helper redirects PK reads transparently.
    """
    if col_idx == batch._schema.pk_index:
        return r_uint64(batch._read_pk_lo(row_idx))
    return r_uint64(batch._read_col_int(row_idx, col_idx))


def hash_row_by_columns(batch, row_idx, col_indices):
    """Compute destination partition for a row using group-key semantics.

    Mirrors _extract_group_key in reduce.py so that the exchange routing
    is consistent with the reduce output PK partition routing:
      - Single U64/I64 column: use value directly as PK lo; partition = _partition_for_key(val, 0)
      - Everything else:       Murmur3 combination → _partition_for_key(lo, hi)

    This guarantees that the worker receiving the exchange data also owns the
    partition where the reduce result will be stored.
    """
    schema = batch._schema
    if len(col_indices) == 1:
        c_idx = col_indices[0]
        col_type = schema.columns[c_idx].field_type.code
        if col_type == types.TYPE_U64.code or col_type == types.TYPE_I64.code:
            val = _read_col_or_pk(batch, row_idx, c_idx)
            return _partition_for_key(val, r_uint64(0))

    # General path: Murmur3 combination (matches _extract_group_key for non-U64 single-col
    # and all multi-col cases)
    h = r_uint64(0x9E3779B97F4A7C15)
    for i in range(len(col_indices)):
        col_hash = _mix64(_read_col_or_pk(batch, row_idx, col_indices[i]))
        h = _mix64(h ^ col_hash ^ r_uint64(i))
    h_hi = _mix64(h ^ r_uint64(len(col_indices)))
    return _partition_for_key(h, h_hi)


def repartition_batch(batch, shard_col_indices, num_workers, assignment):
    """Split a batch into N sub-batches by hashing shard columns.
    Returns list[ArenaZSetBatch or None] of length num_workers."""
    sub_batches = [None] * num_workers
    schema = batch._schema
    for i in range(batch.length()):
        p = hash_row_by_columns(batch, i, shard_col_indices)
        w = assignment.worker_for_partition(p)
        if sub_batches[w] is None:
            sub_batches[w] = ArenaZSetBatch(schema)
        sub_batches[w]._direct_append_row(batch, i, batch.get_weight(i))
    return sub_batches
