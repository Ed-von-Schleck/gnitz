# gnitz/dbsp/ops/linear.py

from rpython.rlib import jit

from gnitz.core.batch import RowBuilder

"""
Linear Operators for the DBSP algebra.

These operators satisfy the identity L(A + B) = L(A) + L(B).
They are stateless transformations acting on Z-Set batches.
Each function now accepts a BatchWriter for the output register.
"""


# ---------------------------------------------------------------------------
# Linear operator implementations
# ---------------------------------------------------------------------------


@jit.unroll_safe
def op_filter(in_batch, out_writer, func):
    """
    Retains only records for which the predicate returns True.
    Weights and payloads are forwarded via zero-copy accessor transfer.

    in_batch:   ArenaZSetBatch
    out_writer: BatchWriter  (strictly write-only destination)
    func:       ScalarFunction  (evaluate_predicate)
    """
    n = in_batch.length()
    for i in range(n):
        accessor = in_batch.get_accessor(i)
        if func is not None:
            if func.evaluate_predicate(accessor):
                out_writer.append_from_accessor(
                    in_batch.get_pk(i), in_batch.get_weight(i), accessor
                )


@jit.unroll_safe
def op_map(in_batch, out_batch, func, out_schema):
    """
    Applies a transformation to every row.
    Uses RowBuilder to validate row construction and commit to the batch.

    in_batch:   ArenaZSetBatch
    out_batch:  ArenaZSetBatch (destination)
    func:       ScalarFunction  (evaluate_map)
    out_schema: TableSchema for the output batch
    """
    builder = RowBuilder(out_schema, out_batch)

    n = in_batch.length()
    for i in range(n):
        in_acc = in_batch.get_accessor(i)
        if func is not None:
            func.evaluate_map(in_acc, builder)
            builder.commit_row(in_batch.get_pk(i), in_batch.get_weight(i))


def op_negate(in_batch, out_writer):
    """
    DBSP negation: flips the sign of every weight.
    Uses bulk column copy with negated weight write.
    """
    out_writer.append_batch_negated(in_batch)


def op_union(batch_a, batch_b, out_writer):
    """
    Algebraic addition of two Z-Set streams.
    batch_b may be None, in which case this is an identity copy of batch_a.
    """
    out_writer.append_batch(batch_a)
    if batch_b is not None:
        out_writer.append_batch(batch_b)


def op_delay(in_batch, out_writer):
    """
    The z^{-1} operator: forwards the current tick's batch to the next tick's
    input register.
    """
    out_writer.append_batch(in_batch)


def op_integrate(in_batch, target_table):
    """
    Terminal sink: flushes a batch into persistent storage.
    Note: integration goes into a ZSetStore, not a transient Delta register.
    """
    if in_batch.length() > 0 and target_table is not None:
        target_table.ingest_batch(in_batch)
