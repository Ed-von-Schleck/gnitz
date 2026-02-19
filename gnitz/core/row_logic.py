# gnitz/core/row_logic.py

from rpython.rlib.objectmodel import newlist_hint
from gnitz.core import comparator


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


class PayloadRowComparator(object):
    """
    Pre-allocated comparator for two List[TaggedValue] payload rows.

    This is the canonical comparison entry point for the VM layer
    (gnitz/vm). Code in gnitz/vm must never import gnitz/storage/comparator
    directly; instead it imports and uses this class, which acts as an API
    proxy that delegates to the storage comparator.

    Holds a reusable pair of ValueAccessor instances so that ZSetBatch
    sort() and consolidate() can compare rows without per-call allocation.
    Handles all column types correctly, including TAG_U128 columns (which
    carry both i64 and u128_hi fields) and unsigned integer types whose
    ordering must be unsigned-correct.

    Usage:
        cmp = PayloadRowComparator(schema)
        result = cmp.compare(left_list, right_list)  # returns -1, 0, or 1
    """

    _immutable_fields_ = ["_schema"]

    def __init__(self, schema):
        self._schema = schema
        self._left = comparator.ValueAccessor(schema)
        self._right = comparator.ValueAccessor(schema)

    def compare(self, left_values, right_values):
        """
        Compares two List[TaggedValue] rows using the canonical storage
        comparator (comparator.compare_rows). Handles all column types
        including TAG_U128 correctly.

        Args:
            left_values:  List[TaggedValue] — non-PK payload for the left row.
            right_values: List[TaggedValue] — non-PK payload for the right row.

        Returns:
            -1 if left < right, 0 if equal, 1 if left > right.
        """
        self._left.set_row(left_values)
        self._right.set_row(right_values)
        return comparator.compare_rows(self._schema, self._left, self._right)
