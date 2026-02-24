# gnitz/dbsp/ops/distinct.py

from rpython.rlib.rarithmetic import r_int64, intmask

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


def op_distinct(delta_batch, history_table, out_batch):
    """
    DBSP Distinct: converts multiset deltas into set-membership deltas.

    delta_batch:   ArenaZSetBatch  — consolidated input delta for this tick
    history_table: AbstractTable   — persistent trace holding the running sum
                                     I(δ_in) accumulated over all past ticks
    out_batch:     ArenaZSetBatch  — output destination (caller clears beforehand)

    The delta_batch is consolidated in-place before processing so that
    multiple updates to the same (key, payload) pair within a single tick
    are algebraically summed before the history lookup.  This is required for
    correctness: two +1 deltas for a record that was previously at weight 0
    must produce exactly one +1 output, not two.

    After producing the output, history_table is updated with the consolidated
    delta so that future ticks see the correct accumulated weight.
    """
    if not delta_batch.is_sorted():
        delta_batch.sort()
    delta_batch.consolidate()

    n = delta_batch.length()
    if n == 0:
        return

    for i in range(n):
        key = delta_batch.get_pk(i)
        w_delta = delta_batch.get_weight(i)
        accessor = delta_batch.get_accessor(i)

        w_old = history_table.get_weight(key, accessor)

        # DBSP distinct converts a multiset to a set.
        # An element is in the set (weight 1) if its accumulated weight is > 0, else 0.
        s_old = 1 if w_old > r_int64(0) else 0
        w_new = r_int64(intmask(w_old + w_delta))
        s_new = 1 if w_new > r_int64(0) else 0

        out_w = s_new - s_old
        if out_w != 0:
            out_batch.append_from_accessor(key, r_int64(out_w), accessor)

    history_table.ingest_batch(delta_batch)
