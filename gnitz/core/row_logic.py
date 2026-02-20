# gnitz/core/row_logic.py

from gnitz.core import comparator
from gnitz.core.values import make_payload_row, PayloadRow


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
   - get_accessor(): Returns an object implementing RowAccessor to
     retrieve non-PK column data for the current record.
-------------------------------------------------------------------------------

make_payload_row
----------------
``make_payload_row(schema)`` is imported from ``gnitz.core.values`` and
re-exported here so that existing call sites that import it from
``gnitz.core.row_logic`` continue to work after the migration.

CRITICAL — The newlist_hint Requirement and mr-Poisoning
---------------------------------------------------------
``make_payload_row`` creates ``PayloadRow`` instances whose internal lists
MUST be initialised with ``newlist_hint``. Never use ``[]``, ``[x, y]``,
``[None] * n``, or list concatenation.

RPython's annotator tracks list resizability as part of the list's type
descriptor (listdef). If any construction site creates a list that the
annotator considers fixed-size, that list's entire listdef is permanently
marked must-not-resize (mr). Because the annotator unifies all list instances
of the same element type globally across the entire program, this mr flag
poisons every list of that element type everywhere. Every subsequent
``.append()`` on any list of that type fails with ``ListChangeUnallowed`` at
annotation time.

Concrete failure modes for ``PayloadRow``:

- ``self._lo = []`` anywhere → all ``List[r_int64]`` poisoned globally.
- ``self._strs = []`` anywhere → all ``List[str]`` poisoned globally.
- ``self._hi = []`` anywhere → all ``List[r_uint64]`` poisoned globally.

The fix is uniform and mandatory: ``PayloadRow.__init__`` uses
``newlist_hint(n)`` for every list. The ``None`` path (``_hi = None``) does
not involve list construction and is safe. ``make_payload_row(schema)`` is
the single enforcement point. No other code may construct a ``PayloadRow``
instance directly.
"""

# Re-export for callers that import from this module.
# The canonical definition and docstring live in gnitz.core.values.
make_payload_row = make_payload_row


class PayloadRowComparator(object):
    """
    Pre-allocated comparator for two ``PayloadRow`` instances.

    This is the canonical comparison entry point for the VM layer
    (``gnitz/vm``). Code in ``gnitz/vm`` must never import
    ``gnitz/storage/comparator`` directly; instead import and use this class,
    which acts as an API proxy that delegates to the storage comparator.

    Holds a reusable pair of ``PayloadRowAccessor`` instances so that
    ``ZSetBatch.sort()`` and ``ZSetBatch.consolidate()`` can compare rows
    without per-call allocation. Handles all column types correctly,
    including TYPE_U128 columns.

    Usage::

        cmp = PayloadRowComparator(schema)
        result = cmp.compare(left_row, right_row)  # returns -1, 0, or 1
    """

    _immutable_fields_ = ["_schema"]

    def __init__(self, schema):
        self._schema = schema
        self._left = comparator.PayloadRowAccessor(schema)
        self._right = comparator.PayloadRowAccessor(schema)

    def compare(self, left_row, right_row):
        """
        Compares two ``PayloadRow`` instances using the canonical storage
        comparator (``comparator.compare_rows``). Handles all column types
        including TYPE_U128 correctly.

        Args:
            left_row:  ``PayloadRow`` for the left row.
            right_row: ``PayloadRow`` for the right row.

        Returns:
            -1 if left < right, 0 if equal, 1 if left > right.
        """
        self._left.set_row(left_row)
        self._right.set_row(right_row)
        return comparator.compare_rows(self._schema, self._left, self._right)
