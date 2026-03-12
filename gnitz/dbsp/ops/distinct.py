# gnitz/dbsp/ops/distinct.py

from rpython.rlib.rarithmetic import r_int64, intmask
from gnitz.core.batch import ConsolidatedScope, BatchWriter

"""
Distinct Operator for the DBSP algebra.

Converts multiset semantics to set semantics: a record's output weight is 1
if its net accumulated weight is positive, -1 if negative, and 0 otherwise.
In practice GnitzDB only accumulates non-negative weights from the ingestion
layer, so the -1 case does not arise for base tables — but it can arise in
intermediate circuit nodes, and the implementation handles it correctly.

The algebraic identity being implemented is:

    δ_out = set_step(I(δ_in)) - set_step(z^{-1}(I(δ_in)))
          = set_step(history + δ_in) - set_step(history)

where set_step(w) = sign(w) clamped to {-1, 0, 1}.

State (the running integral I(δ_in)) is maintained by the caller in
history_table, which is an AbstractTable acting as the operator's trace.
"""


def op_distinct(delta_batch, history_table, out_writer):
    """
    DBSP Distinct: converts multiset deltas into set-membership deltas.

    delta_batch:   ArenaZSetBatch  — consolidated input delta for this tick
    history_table: ZSetStore       — persistent trace holding the running sum
                                     I(δ_in) accumulated over all past ticks
    out_writer:    BatchWriter     — strictly write-only destination

    The logic is wrapped in ConsolidatedScope, which ensures that delta_batch 
    is sorted and consolidated before processing. If a new temporary batch 
    is created during consolidation, it is automatically freed when exiting 
    the 'with' block.
    """
    with ConsolidatedScope(delta_batch) as b:
        n = b.length()
        if n == 0:
            return

        for i in range(n):
            key = b.get_pk(i)
            w_delta = b.get_weight(i)
            accessor = b.get_accessor(i)

            w_old = history_table.get_weight(key, accessor)

            # DBSP distinct converts a multiset to a set.
            # An element is in the set (weight 1) if its accumulated weight is > 0, else 0.
            # In intermediate nodes, weights can be negative, so we use sign logic.
            s_old = 0
            if w_old > r_int64(0):
                s_old = 1
            elif w_old < r_int64(0):
                s_old = -1

            # Algebraic summation with RPython machine-word truncation
            w_new = r_int64(intmask(w_old + w_delta))
            
            s_new = 0
            if w_new > r_int64(0):
                s_new = 1
            elif w_new < r_int64(0):
                s_new = -1

            out_w = s_new - s_old
            if out_w != 0:
                out_writer.append_from_accessor(key, r_int64(out_w), accessor)

        out_writer.mark_sorted(True)

        # Update the history with the consolidated delta before the scope expires.
        # This reflects history_table = I(δ_in)
        history_table.ingest_batch(b)
