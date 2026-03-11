# gnitz/dbsp/ops/exchange.py
#
# Column-based hash for repartitioning batches across workers.

from rpython.rlib.rarithmetic import r_uint64, intmask
from gnitz.core import xxh
from gnitz.core.batch import ArenaZSetBatch


def hash_row_by_columns(batch, row_idx, col_indices):
    """Hash specified columns of a row to determine destination partition.
    Uses xxh.hash_u128_inline for quality. Fixed-width columns only."""
    lo = r_uint64(0)
    hi = r_uint64(0)
    for i in range(len(col_indices)):
        val = r_uint64(batch._read_col_int(row_idx, col_indices[i]))
        if i % 2 == 0:
            lo = lo ^ (val * r_uint64(0x9E3779B97F4A7C15))
        else:
            hi = hi ^ (val * r_uint64(0x9E3779B97F4A7C15))
    h = xxh.hash_u128_inline(lo, hi)
    return intmask(r_uint64(h) & r_uint64(0xFF))


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
