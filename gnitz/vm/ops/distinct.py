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
    1. Consolidate delta_in. This ensures that if the same record appears
       multiple times in the input batch, we process the net change as a
       single atomic update to the set status.
    2. For each unique record in the consolidated batch:
       a. Lookup current accumulated weight (W_old) in history.
       b. S_old = 1 if W_old > 0 else 0
       c. W_new = W_old + delta_weight
       d. S_new = 1 if W_new > 0 else 0
       e. Delta_weight_out = S_new - S_old
       f. If Delta_weight_out != 0, emit to output.
    3. Update the multiset history using the consolidated input batch in
        a single high-throughput operation.
    """
    assert isinstance(reg_in, runtime.DeltaRegister)
    assert isinstance(reg_history, runtime.TraceRegister)
    assert isinstance(reg_out, runtime.DeltaRegister)

    delta_batch = reg_in.batch
    history = reg_history
    output_batch = reg_out.batch

    # 1. Consolidate input to handle multiple updates per key in a single tick.
    # This is critical for the set-status calculation logic below.
    if not delta_batch.is_sorted():
        delta_batch.sort()
    delta_batch.consolidate()

    output_batch.clear()

    n = delta_batch.length()
    if n == 0:
        return

    for i in range(n):
        key = delta_batch.get_pk(i)
        w_delta = delta_batch.get_weight(i)
        payload = delta_batch.get_row(i)

        # 2. Retrieve the current accumulated weight from the history trace.
        # Note: history.get_weight handles the case where table is None.
        w_old = history.get_weight(key, payload)

        # 3. Calculate set status (signum) before update.
        # DBSP Set semantics defines presence as weight > 0.
        s_old = 1 if w_old > r_int64(0) else 0

        # 4. Calculate status after applying the net delta for this tick.
        w_new = w_old + w_delta
        s_new = 1 if w_new > r_int64(0) else 0

        # 5. Determine the change in set status (-1, 0, or 1).
        out_w_val = s_new - s_old

        if out_w_val != 0:
            # Emit the set-semantic delta to the output register.
            output_batch.append(key, r_int64(out_w_val), payload)

    # 6. High-throughput history update.
    # We use the consolidated delta_batch directly. This avoids a separate
    # scratch buffer and minimizes I/O calls to the EphemeralTable.
    history.update_batch(delta_batch)
