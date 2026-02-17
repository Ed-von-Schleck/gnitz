# gnitz/core/row_logic.py

from gnitz.storage.comparator import compare_rows, RowAccessor

"""
Row Logic API: The Storage-VM Boundary.

This module provides the abstract interfaces and comparison kernels required 
to implement the DBSP algebra. In DBSP, records with the same Primary Key 
are further grouped or joined based on the equality of their 'Row Payload' 
(all non-PK columns).

To maintain the isolation of the VM from storage internals (SkipLists, Shards, 
Arenas), the VM should use the interfaces defined here.
"""

# Alias for the JIT-optimized comparison function.
# This function performs a lexicographical comparison of all non-PK columns.
#
# Inputs:
#   - schema: The TableSchema defining column types and which index is the PK.
#   - left:  A BaseRowAccessor pointing to the first record.
#   - right: A BaseRowAccessor pointing to the second record.
#
# Returns:
#   - -1 if left < right
#   -  0 if left == right
#   -  1 if left > right
compare_records = compare_rows


class BaseRowAccessor(RowAccessor):
    """
    The Abstract Data Accessor.
    
    To compare a custom VM structure (like a ZSetBatch) against persistent 
    storage, the VM must implement a subclass of BaseRowAccessor. 
    
    The comparison kernel calls these methods to extract primitive values 
    for comparison without needing to know the underlying memory layout 
    (Row-oriented, Column-oriented, or TaggedValue-list).
    """

    def set_row(self, source, index_or_offset):
        """
        Points the accessor to a specific record within its source.
        
        Args:
            source: The container (e.g., a ShardView or a ZSetBatch).
            index_or_offset: The location within that container.
        """
        raise NotImplementedError

    def get_int(self, col_idx):
        """
        Returns the integer value of the column.
        For comparison purposes, all integer types (u8-u64, i8-i64) 
        are treated as 64-bit unsigned integers to ensure 
        consistent bitwise comparison.
        """
        raise NotImplementedError

    def get_float(self, col_idx):
        """Returns the float (f64) value of the column."""
        raise NotImplementedError
    
    def get_u128(self, col_idx):
        """
        Returns the 128-bit integer value (r_uint128). 
        Used for UUIDs or high-precision keys stored in payloads.
        """
        raise NotImplementedError

    def get_str_struct(self, col_idx):
        """
        Returns the 'German String' metadata required for O(1) comparison.
        
        The implementation must return a 5-tuple:
        (length, prefix, struct_ptr, heap_ptr, python_string)
        
        Fields:
            1. length (int): Total length of the string.
            2. prefix (int): The 4-byte prefix cast to a signed integer.
            3. struct_ptr (rffi.CCHARP): Pointer to the 16-byte German 
               struct in memory. If the data is only in a Python string 
               (e.g., during VM ingestion), this can be NULL.
            4. heap_ptr (rffi.CCHARP): Pointer to the start of the 
               Blob Heap (Region B) where long tails are stored. 
               Can be NULL if data is only in a Python string.
            5. python_string (str or None): The raw Python string. 
               If the data is only in raw memory (Storage Shard), 
               this should be None to avoid allocation.
        
        Logic Note: The comparison kernel uses the prefix and length 
        first. It only follows the pointers or looks at the python_string 
        if the prefixes match and the string is longer than 4 bytes.
        """
        raise NotImplementedError
