# gnitz/vm/ops/distinct.py

from rpython.rlib.rarithmetic import r_int64
from gnitz.vm import runtime


def op_distinct(reg_in, reg_history, reg_out):
    """
    DBSP Distinct Operator: Converts multiset semantics to set semantics.
    Output weight is 1 if record exists (net weight > 0), else 0.

    Algebraic identity:
    delta_out = set_step(history + delta_in) - set_step(history)

    Zero-Copy Refactor: 
    Uses Batch accessors to bypass PayloadRow instantiation during historical 
    weight lookups and result emission.
    """
    assert isinstance(reg_in, runtime.DeltaRegister)
    assert isinstance(reg_history, runtime.TraceRegister)
    assert isinstance(reg_out, runtime.DeltaRegister)

    delta_batch = reg_in.batch
    history = reg_history
    output_batch = reg_out.batch

    # 1. Algebraic Consolidation
    # Crucial: sum multiple updates for the same key/payload within the tick.
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
        
        # Zero-copy: Obtain a transient raw accessor to the batch memory
        accessor = delta_batch.get_accessor(i)

        # Retrieve current accumulated weight from the trace/table
        # Updated AbstractTable API: get_weight(key, accessor)
        w_old = history.get_weight(key, accessor)

        # Calculate set status before update (presence: weight > 0)
        s_old = 1 if w_old > r_int64(0) else 0

        # Calculate status after applying the consolidated delta
        w_new = w_old + w_delta
        s_new = 1 if w_new > r_int64(0) else 0

        # Determine set status change (-1, 0, or 1)
        out_w_val = s_new - s_old

        if out_w_val != 0:
            # Zero-copy: serialize result directly to output batch
            output_batch.append_from_accessor(key, r_int64(out_w_val), accessor)

    # 4. State Maintenance
    # Update history with the consolidated batch via fast-path ingestion.
    history.update_batch(delta_batch)
