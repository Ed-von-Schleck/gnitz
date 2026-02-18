# gnitz/core/row_logic.py

from rpython.rlib.objectmodel import newlist_hint
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


def make_payload_row(n_payload_cols):
    """
    Canonical constructor for a payload row (List[TaggedValue]).

    MUST use newlist_hint() rather than a list literal [x, y, ...], [],
    [None]*n, or list + list. RPython marks all those forms as 'must not
    resize' (mr) at construction time. Because the annotator unifies ALL
    List[TaggedValue] instances in the program into a single shared element
    listdef, even one mr construction anywhere permanently poisons that
    listdef — causing ListChangeUnallowed at every subsequent .append() site.

    newlist_hint(n) allocates a resizable list pre-sized to n slots so the
    N appends that follow require no realloc, recovering the performance of
    a fixed-size allocation without the mr constraint.

    Every site in the codebase that constructs a fresh List[TaggedValue]
    MUST call this function instead of writing a literal.
    """
    return newlist_hint(n_payload_cols)


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
