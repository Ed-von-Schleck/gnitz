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
    1. For each record in delta_in, lookup its current weight (W_old) in history.
    2. S_old = 1 if W_old > 0 else 0
    3. W_new = W_old + delta_weight
    4. S_new = 1 if W_new > 0 else 0
    5. Delta_weight_out = S_new - S_old
    6. If Delta_weight_out != 0, emit the record.
    7. Update history with the raw delta_in weight.
    """
    assert isinstance(reg_in, runtime.DeltaRegister)
    assert isinstance(reg_history, runtime.TraceRegister)
    assert isinstance(reg_out, runtime.DeltaRegister)

    delta_batch = reg_in.batch
    history = reg_history
    output_batch = reg_out.batch
    
    # We clear the output delta register for the new epoch
    output_batch.clear()

    n = delta_batch.length()
    for i in range(n):
        key = delta_batch.get_pk(i)
        w_delta = delta_batch.get_weight(i)
        payload = delta_batch.get_row(i)

        # 1. Retrieve the current accumulated weight from the persistent trace
        w_old = history.get_weight(key, payload)

        # 2. Calculate the 'set' status (presence) before the update
        # In DBSP Set semantics, presence is defined as weight > 0.
        s_old = 1 if w_old > r_int64(0) else 0

        # 3. Calculate the weight after applying the update
        w_new = w_old + w_delta
        s_new = 1 if w_new > r_int64(0) else 0

        # 4. Determine the change in set status
        # Possible values: 
        #  1: Record appeared (0 -> 1)
        # -1: Record disappeared (1 -> 0)
        #  0: No change in set status (e.g., 2 -> 3 or -1 -> 0)
        out_w_val = s_new - s_old
        
        if out_w_val != 0:
            # Emit the set-semantic delta
            output_batch.append(key, r_int64(out_w_val), payload)

        # 5. Maintain the underlying multiset history.
        # This update must reflect the raw delta so that the trace remains 
        # an accurate summation of all linear inputs.
        history.update_weight(key, w_delta, payload)
