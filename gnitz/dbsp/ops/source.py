# gnitz/dbsp/ops/source.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64

def op_scan_trace(cursor, out_batch, chunk_limit):
    """
    Scans records from a stateful cursor into a Delta batch.
    
    Returns the number of records scanned.
    The cursor is left at the position of the next record to be scanned.
    """
    scanned_count = 0
    
    while cursor.is_valid():
        # chunk_limit <= 0 means scan everything until exhaustion
        if chunk_limit > 0 and scanned_count >= chunk_limit:
            break
            
        key = cursor.key()
        weight = cursor.weight()
        
        # In DBSP/Differential logic, we only process records with non-zero weight.
        if weight != r_int64(0):
            accessor = cursor.get_accessor()
            out_batch.append_from_accessor(key, weight, accessor)
            scanned_count += 1
            
        cursor.advance()
        
    return scanned_count


def op_seek_trace(cursor, key):
    """
    Positions a stateful cursor at the first record >= key.
    Used for index-nested-loop joins or targeted state lookups.
    """
    cursor.seek(key)


def op_clear_deltas(reg_file):
    """
    Explicitly clears all transient Delta registers in the register file.
    Unlike prepare_for_tick(), this does NOT refresh cursors, making it
    safe for mid-program execution (e.g., between chunked scans).
    """
    reg_file.clear_deltas()
