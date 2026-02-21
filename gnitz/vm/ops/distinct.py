# gnitz/vm/ops/distinct.py

from rpython.rlib.rarithmetic import r_int64
from gnitz.vm import runtime


def op_distinct(reg_in, reg_history, reg_out):
    """
    DBSP Distinct Operator: Converts multiset semantics to set semantics.

    The operator ensures that for any record, the net weight in the output
    is either 0 or 1.

    Algebraic identity:
    delta_out = set_step(history + delta_in) - set_step(history)

    Logic:
    1. Consolidate delta_in to ensure each (PK, Payload) is unique per tick.
    2. For each record in delta_in, lookup its current weight (W_old) in history.
    3. S_old = 1 if W_old > 0 else 0
    4. W_new = W_old + delta_weight
    5. S_new = 1 if W_new > 0 else 0
    6. Delta_weight_out = S_new - S_old
    7. If Delta_weight_out != 0, emit the record to output_batch.
    8. Batch-update history with the consolidated delta_in.
    """
    assert isinstance(reg_in, runtime.DeltaRegister)
    assert isinstance(reg_history, runtime.TraceRegister)
    assert isinstance(reg_out, runtime.DeltaRegister)

    delta_batch = reg_in.batch
    history = reg_history
    output_batch = reg_out.batch

    # 1. Algebraic Consolidation
    # We MUST consolidate the input to ensure that if multiple updates for the
    # same key/payload exist in one tick, they are summed before we check
    # against the historical 'set' status.
    if not delta_batch.is_sorted():
        delta_batch.sort()
    delta_batch.consolidate()

    # 2. Preparation
    output_batch.clear()
    n = delta_batch.length()
    if n == 0:
        return

    # 3. Delta Computation Loop
    for i in range(n):
        key = delta_batch.get_pk(i)
        w_delta = delta_batch.get_weight(i)
        payload = delta_batch.get_row(i)

        # Retrieve current accumulated weight from the trace/table
        w_old = history.get_weight(key, payload)

        # Calculate set status before update (presence: weight > 0)
        s_old = 1 if w_old > r_int64(0) else 0

        # Calculate status after applying the consolidated delta
        w_new = w_old + w_delta
        s_new = 1 if w_new > r_int64(0) else 0

        # Determine set status change (-1, 0, or 1)
        out_w_val = s_new - s_old

        if out_w_val != 0:
            # Emit set-semantic delta to the output stream
            output_batch.append(key, r_int64(out_w_val), payload)

    # 4. State Maintenance
    # We update the history table using the consolidated input batch.
    # This is the high-throughput path: EphemeralTable.ingest_batch
    # will handle the in-memory ingestion as a single atomic operation.
    history.update_batch(delta_batch)
