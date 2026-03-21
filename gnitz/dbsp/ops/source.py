# gnitz/dbsp/ops/source.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64
from gnitz.core.batch import BatchWriter

"""
Source Operators for the DBSP algebra.

These operators bridge the gap between persistent ZSetStore state and 
the transient Delta registers used by the circuit.
"""

def op_scan_trace(cursor, out_writer, chunk_limit):
    """
    Scans records from a stateful cursor into a Delta batch.
    
    cursor:      AbstractCursor  — the source persistent trace
    out_writer:  BatchWriter     — strictly write-only destination
    chunk_limit: int             — max records to scan; <= 0 for unlimited
    
    Returns the number of records scanned into the writer.
    The cursor is left at the position of the next record to be scanned.
    """
    scanned_count = 0
    
    while cursor.is_valid():
        # chunk_limit <= 0 means scan everything until exhaustion
        if chunk_limit > 0 and scanned_count >= chunk_limit:
            break
            
        key_lo, key_hi = cursor.key_lo(), cursor.key_hi()
        weight = cursor.weight()

        # In DBSP/Differential logic, we only process records with non-zero weight.
        # Zero-weight records logically do not exist in the Z-Set.
        if weight != r_int64(0):
            accessor = cursor.get_accessor()
            out_writer.append_from_accessor(key_lo, key_hi, weight, accessor)
            scanned_count += 1
            
        cursor.advance()

    out_writer.mark_consolidated(True)
    return scanned_count


def op_seek_trace(cursor, key_lo, key_hi):
    """
    Positions a stateful cursor at the first record >= key.
    Used for index-nested-loop joins or targeted state lookups.
    """
    cursor.seek(key_lo, key_hi)


def op_clear_deltas(reg_file):
    """
    Explicitly clears all transient Delta registers in the register file.
    Unlike prepare_for_tick(), this does NOT refresh cursors, making it
    safe for mid-program execution (e.g., between chunked scans).
    """
    reg_file.clear_deltas()
