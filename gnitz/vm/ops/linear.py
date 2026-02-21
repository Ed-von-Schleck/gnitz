# gnitz/vm/ops/linear.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, intmask
from gnitz.core.comparator import PayloadRowAccessor
from gnitz.core.values import make_payload_row

"""
Linear Operators for the DBSP Virtual Machine.

These operators satisfy the identity L(A + B) = L(A) + L(B). 
They are stateless transformations acting on Z-Set Delta Registers.
"""


@jit.unroll_safe
def op_filter(reg_in, reg_out, func):
    """
    Retains only records where the predicate returns True.
    Weights and Payloads are preserved by reference.
    """
    in_batch = reg_in.batch
    out_batch = reg_out.batch
    # Note: clear() is called by the interpreter via reg_file.clear_all_deltas()

    in_schema = reg_in.vm_schema.table_schema
    accessor = PayloadRowAccessor(in_schema)

    n = in_batch.length()
    for i in range(n):
        pk = in_batch.get_pk(i)
        row = in_batch.get_row(i)
        weight = in_batch.get_weight(i)

        accessor.set_row(row)
        if func.evaluate_predicate(accessor):
            # PayloadRow is immutable once in a batch; safe to copy reference
            out_batch.append(pk, weight, row)


@jit.unroll_safe
def op_map(reg_in, reg_out, func):
    """
    Applies a transformation to every row. 
    Creates new PayloadRows for the output batch based on the output schema.
    """
    in_batch = reg_in.batch
    out_batch = reg_out.batch

    in_schema = reg_in.vm_schema.table_schema
    out_schema = reg_out.vm_schema.table_schema

    accessor = PayloadRowAccessor(in_schema)

    n = in_batch.length()
    for i in range(n):
        pk = in_batch.get_pk(i)
        weight = in_batch.get_weight(i)
        row = in_batch.get_row(i)

        accessor.set_row(row)

        # Allocate fresh PayloadRow for the transformation result
        out_row = make_payload_row(out_schema)
        # scalar_fn populates out_row via typed append_* calls
        func.evaluate_map(accessor, out_row)

        out_batch.append(pk, weight, out_row)


def op_negate(reg_in, reg_out):
    """
    DBSP negation operator: flips the sign of every weight.
    Used for retractions and differentiation.
    """
    in_batch = reg_in.batch
    out_batch = reg_out.batch

    n = in_batch.length()
    for i in range(n):
        pk = in_batch.get_pk(i)
        weight = in_batch.get_weight(i)
        row = in_batch.get_row(i)

        # Arithmetic negation of weight
        neg_weight = r_int64(-intmask(weight))
        out_batch.append(pk, neg_weight, row)


def op_union(reg_in_a, reg_in_b, reg_out):
    """
    Algebraic addition of two Z-Set streams.
    The output batch is the concatenation of both inputs.
    """
    out_batch = reg_out.batch

    batch_a = reg_in_a.batch
    n_a = batch_a.length()
    for i in range(n_a):
        out_batch.append(batch_a.get_pk(i), batch_a.get_weight(i), batch_a.get_row(i))

    if reg_in_b is not None:
        batch_b = reg_in_b.batch
        n_b = batch_b.length()
        for i in range(n_b):
            out_batch.append(
                batch_b.get_pk(i), batch_b.get_weight(i), batch_b.get_row(i)
            )


def op_delay(reg_in, reg_out):
    """
    The z^-1 operator. Moves the input delta to the output register 
    for processing in the subsequent logical tick.
    """
    in_batch = reg_in.batch
    out_batch = reg_out.batch

    n = in_batch.length()
    for i in range(n):
        out_batch.append(in_batch.get_pk(i), in_batch.get_weight(i), in_batch.get_row(i))


def op_integrate(reg_in, target_table):
    """
    Terminal sink operator. Flushes the in-memory Z-Set batch into 
    persistent storage (Table/FLSM) using optimized batch ingestion.
    """
    in_batch = reg_in.batch
    if in_batch.length() > 0:
        # The storage layer implementation of ingest_batch handles WAL 
        # durability (fsync) and MemTable updates for the entire batch 
        # as a single atomic operation.
        target_table.ingest_batch(in_batch)
