# gnitz/core/row_logic.py

from gnitz.storage import comparator

"""
Row Logic API: The Storage-VM Boundary.

This module provides the abstract interfaces and comparison kernels required 
to implement the DBSP algebra. 

-------------------------------------------------------------------------------
THE DBSP CURSOR PROTOCOL
-------------------------------------------------------------------------------
To participate in VM operations (Joins, Reductions), a Trace Reader must 
implement the following protocol. This allows the VM to remain decoupled 
from specific storage layouts (MemTable vs. Shards).

1. Navigation:
   - seek(key): Positions the cursor at the first record where PK >= key.
   - advance(): Moves to the next record in the sorted sequence.
   - is_valid(): Returns True if the cursor is positioned on a valid record.

2. Metadata:
   - key(): Returns the current 128-bit Primary Key (r_uint128).
   - weight(): Returns the current algebraic weight (r_int64).

3. Data Extraction:
   - get_accessor(): Returns an object implementing BaseRowAccessor to 
     retrieve non-PK column data for the current record.
-------------------------------------------------------------------------------
""" 

# The comparison kernel for relational records.
# It performs a type-aware lexicographical comparison of all non-PK columns.
compare_records = comparator.compare_rows


class BaseRowAccessor(comparator.RowAccessor):
    """
    The Abstract Data Accessor interface.
    
    The VM and its comparison kernels call these methods to extract 
    primitive values for comparison and transformation without needing 
    to know the physical memory layout (MemTable, Shard, or Batch).

    Note: Subclasses define their own storage-specific initialization 
    mechanisms (e.g., set_row). These are not part of the virtual interface 
    to allow for varying parameters and optimized initialization.
    """

    def get_int(self, col_idx):
        """Returns the integer value of the column."""
        raise NotImplementedError

    def get_float(self, col_idx):
        """Returns the float (f64) value of the column."""
        raise NotImplementedError
    
    def get_u128(self, col_idx):
        """Returns the 128-bit integer value (r_uint128)."""
        raise NotImplementedError

    def get_str_struct(self, col_idx):
        """Returns the German String metadata for O(1) comparison."""
        raise NotImplementedError

    def get_col_ptr(self, col_idx):
        """Returns a raw pointer to column data (if available)."""
        raise NotImplementedError
