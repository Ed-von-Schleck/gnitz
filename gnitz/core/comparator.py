# gnitz/core/comparator.py

from rpython.rlib import jit
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, strings as string_logic

NULL_PTR = lltype.nullptr(rffi.CCHARP.TO)


class RowAccessor(object):
    """
    Abstract interface for accessing column data from any storage format.

    Note: ``set_row`` is NOT defined here because its signature varies
    significantly between storage backends (e.g. offsets vs indexes vs
    ``PayloadRow`` objects). Subclasses define their own ``set_row`` methods
    for private initialization. This prevents RPython's annotator from
    forcing a unified signature that conflicts across different
    implementations.
    """

    def get_int(self, col_idx):
        """Returns the integer value cast to ``r_uint64`` for comparison."""
        raise NotImplementedError

    def get_float(self, col_idx):
        """Returns the float value."""
        raise NotImplementedError

    def get_u128(self, col_idx):
        """Returns the 128-bit integer value as ``r_uint128``."""
        raise NotImplementedError

    def get_str_struct(self, col_idx):
        """
        Returns a 5-tuple compatible with ``strings.compare_structures``:
        ``(length, prefix, struct_ptr, heap_ptr, python_string)``
        """
        raise NotImplementedError

    def get_col_ptr(self, col_idx):
        """
        Returns a raw pointer to the column data for the current row.
        Used by the compactor for physical row materialisation.
        """
        raise NotImplementedError


class PayloadRowAccessor(RowAccessor):
    """
    Adapts a ``PayloadRow`` to the ``RowAccessor`` interface consumed by
    ``compare_rows`` and ``PayloadRowComparator``.

    The schema is held in ``_immutable_fields_`` so the JIT promotes it to a
    compile-time constant. All column-type dispatch in ``compare_rows`` is
    therefore resolved at trace-compile time — no runtime branching on column
    types.

    ``col_idx`` passed to all ``get_*`` methods is a **schema** column index
    (0-based, includes the PK position). The accessor maps it to the
    corresponding payload array index before delegating to ``PayloadRow``.
    """

    _immutable_fields_ = ["_schema", "_pk_index"]

    def __init__(self, schema):
        self._schema = schema
        self._pk_index = schema.pk_index
        self._row = None  # PayloadRow

    def set_row(self, payload_row):
        self._row = payload_row

    def _payload_idx(self, col_idx):
        """Map schema column index → payload array index (PK excluded)."""
        return col_idx if col_idx < self._pk_index else col_idx - 1

    def get_int(self, col_idx):
        return self._row.get_int(self._payload_idx(col_idx))

    def get_float(self, col_idx):
        return self._row.get_float(self._payload_idx(col_idx))

    def get_u128(self, col_idx):
        return self._row.get_u128(self._payload_idx(col_idx))

    def get_str_struct(self, col_idx):
        """
        Returns the 5-tuple expected by ``compare_structures``.

        ``NULL_PTR`` for the struct and heap pointers is correct and safe:
        ``compare_structures`` inspects whether the fifth element
        (``python_string``) is ``None`` before deciding to follow raw memory
        pointers. Because ``PayloadRow`` strings are always Python ``str``
        objects, the raw-pointer path is never taken.
        """
        s = self._row.get_str(self._payload_idx(col_idx))
        length = len(s)
        prefix = rffi.cast(lltype.Signed, string_logic.compute_prefix(s))
        return (length, prefix, NULL_PTR, NULL_PTR, s)

    def get_col_ptr(self, col_idx):
        """
        Returns ``NULL_PTR``. This is intentional: the compactor, which is
        the only caller of ``get_col_ptr`` for raw memory copy, exclusively
        operates on ``ShardCursor`` and ``MemTableCursor`` instances, never
        on ``PayloadRow``. ``PayloadRow`` lives only in the VM layer
        (``ZSetBatch``). The VM serialises to raw C-buffers via
        ``engine.ingest`` before any storage cursor sees the data.
        """
        return NULL_PTR


# Backward-compatibility alias for storage-layer code that imports
# ``ValueAccessor`` by name (``gnitz/storage/comparator.py``,
# ``gnitz/storage/engine.py``, ``gnitz/storage/memtable.py``).
# Both names refer to the same class; the annotator sees a single type.
ValueAccessor = PayloadRowAccessor


@jit.unroll_safe
def compare_rows(schema, left, right):
    """
    Unified lexicographical comparison of two rows using ``RowAccessor``
    objects. The JIT specialises this loop based on the schema and accessor
    types.
    """
    num_cols = len(schema.columns)
    for i in range(num_cols):
        if i == schema.pk_index:
            continue

        col_type = schema.columns[i].field_type

        if col_type == types.TYPE_STRING:
            l_len, l_pref, l_ptr, l_heap, l_str = left.get_str_struct(i)
            r_len, r_pref, r_ptr, r_heap, r_str = right.get_str_struct(i)

            res = string_logic.compare_structures(
                l_len,
                l_pref,
                l_ptr,
                l_heap,
                l_str,
                r_len,
                r_pref,
                r_ptr,
                r_heap,
                r_str,
            )
            if res != 0:
                return res

        elif col_type == types.TYPE_U128:
            l_val = left.get_u128(i)
            r_val = right.get_u128(i)
            if l_val < r_val:
                return -1
            if l_val > r_val:
                return 1

        elif col_type == types.TYPE_F64:
            l_val = left.get_float(i)
            r_val = right.get_float(i)
            if l_val < r_val:
                return -1
            if l_val > r_val:
                return 1

        else:
            l_val = left.get_int(i)
            r_val = right.get_int(i)
            if l_val < r_val:
                return -1
            if l_val > r_val:
                return 1

    return 0
