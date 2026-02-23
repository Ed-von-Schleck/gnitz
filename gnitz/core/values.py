# gnitz/core/values.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_int64, r_uint64, intmask
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.longlong2float import float2longlong, longlong2float
from rpython.rtyper.lltypesystem import rffi


def _analyze_schema(schema):
    """
    Derives PayloadRow allocation flags from a TableSchema.

    Scans every non-PK column exactly once. Called at row-construction time;
    callers in hot ingestion paths should cache the result if the schema is
    stable.

    Returns (n_payload_cols, has_u128, has_string, has_nullable).
    """
    # Imported here at the function level to keep the module-level import
    # graph cycle-free. gnitz.core.types does not import gnitz.core.values,
    # so the import is safe, but placing it at module level would impose a
    # load-order constraint that is fragile at translation time.
    from gnitz.core.types import TYPE_U128, TYPE_STRING

    has_u128 = False
    has_string = False
    has_nullable = False
    n = 0
    for i in range(len(schema.columns)):
        if i == schema.pk_index:
            continue
        n += 1
        col = schema.columns[i]
        ft = col.field_type
        if ft.code == TYPE_U128.code:
            has_u128 = True
        if ft.code == TYPE_STRING.code:
            has_string = True
        if col.is_nullable:
            has_nullable = True
    return n, has_u128, has_string, has_nullable


class PayloadRow(object):
    """
    A struct-of-arrays encoding of a single non-PK payload row.

    Replaces ``List[TaggedValue]`` throughout the entire codebase. Column
    types are implicit in the ``TableSchema`` and are **not** stored per-row.
    Nullability is tracked in a scalar bit-field rather than per-column tags,
    eliminating the GC header tax paid by ``TaggedValue`` objects.

    Fields
    ------
    _lo : List[r_int64]
        Always present. Stores all column data as 64-bit words:

        - Integer types (i8..u64): the value bitcast to ``r_int64``.
          Unsigned callers bitcast back with ``rffi.cast(rffi.ULONGLONG, v)``.
        - Float types (f32, f64): the IEEE 754 bit pattern stored via
          ``float2longlong`` — a lossless reinterpret cast, **not** a
          value cast.
        - TYPE_U128: the low 64 bits, bitcast ``r_uint64 → r_int64`` via
          ``intmask``.
        - TYPE_STRING: ``r_int64(0)`` (data lives in ``_strs``).
        - NULL: ``r_int64(0)``.

    _hi : List[r_uint64] or None
        Present only when the schema contains at least one non-PK TYPE_U128
        column. Stores the high 64 bits of u128 values; ``r_uint64(0)`` for
        all other column types at the corresponding index. ``None`` when the
        schema has no u128 non-PK columns — avoids a heap allocation per row
        in the common numeric-only case.

    _strs : List[str] or None
        Present only when the schema contains at least one TYPE_STRING column.
        Stores the string value for string columns; ``""`` (the RPython
        prebuilt singleton — no heap allocation) for all other column types
        at the corresponding index. ``None`` when the schema has no string
        columns.

    _null_word : r_uint64
        Always present as a direct scalar field (8 bytes, no heap object).
        Bit *N* is set if payload column *N* is null. For schemas with no
        nullable columns, ``_has_nullable`` is promoted to ``False`` by the
        JIT and ``_null_word`` is never written or read at runtime.

    _has_u128, _has_string, _has_nullable : bool
        Derived from the schema at construction. Listed in
        ``_immutable_fields_`` so the JIT promotes them to compile-time
        constants per trace. All branches on these flags are resolved at
        trace-compile time; dead branches are never emitted in machine code.

    Construction
    ------------
    Use ``make_payload_row(schema)`` — never construct ``PayloadRow``
    directly. ``make_payload_row`` is the sole enforcement point for the
    ``newlist_hint`` requirement (see ``gnitz/core/row_logic.py``).

    Mutation
    --------
    Call the ``append_*`` methods in column-index order (skipping the PK
    column) during construction. The row is considered immutable once all
    non-PK columns have been appended.
    """

    _immutable_fields_ = ["_has_u128", "_has_string", "_has_nullable"]

    def __init__(self, has_u128, has_string, has_nullable, n):
        self._has_u128 = has_u128
        self._has_string = has_string
        self._has_nullable = has_nullable
        # All list initialisation MUST use newlist_hint. See the mr-poisoning
        # warning in gnitz/core/row_logic.py for a full explanation.
        self._lo = newlist_hint(n)
        self._hi = newlist_hint(n) if has_u128 else None
        self._strs = newlist_hint(n) if has_string else None
        self._null_word = r_uint64(0)

    # ------------------------------------------------------------------
    # Append methods
    # Call in column-index order (PK excluded) during row construction.
    # ------------------------------------------------------------------

    def append_int(self, val_i64):
        """
        For all integer column types: i8, i16, i32, i64, u8, u16, u32, u64.

        ``val_i64`` must be ``r_int64``. For unsigned source values, callers
        bitcast with ``rffi.cast(rffi.LONGLONG, uint_val)`` before calling.
        """
        self._lo.append(val_i64)
        if self._has_u128:
            assert self._hi is not None
            self._hi.append(r_uint64(0))
        if self._has_string:
            assert self._strs is not None
            self._strs.append("")

    def append_float(self, val_f64):
        """
        For TYPE_F64 (and TYPE_F32, widened to f64 at ingestion).

        **CRITICAL:** ``float2longlong`` performs a bit-level reinterpret
        cast, equivalent to ``memcpy(&bits, &val, 8)`` in C. This is a
        lossless round-trip for all IEEE 754 values including infinities,
        NaNs, and negative zero.

        Do **not** use ``rffi.cast(rffi.LONGLONG, val_f64)``. In RPython
        that produces a C value cast — ``(long long)3.14 == 3`` — silently
        destroying the fractional part.
        """
        self._lo.append(float2longlong(val_f64))
        if self._has_u128:
            assert self._hi is not None
            self._hi.append(r_uint64(0))
        if self._has_string:
            assert self._strs is not None
            self._strs.append("")

    def append_string(self, val_str):
        """For TYPE_STRING columns. ``_has_string`` must be True."""
        self._lo.append(r_int64(0))
        if self._has_u128:
            assert self._hi is not None
            self._hi.append(r_uint64(0))
        assert self._strs is not None
        self._strs.append(val_str)

    def append_u128(self, lo_u64, hi_u64):
        """
        For non-PK TYPE_U128 columns (e.g. UUID foreign keys).

        ``lo_u64``: ``r_uint64`` — low 64 bits.
        ``hi_u64``: ``r_uint64`` — high 64 bits.

        ``_has_u128`` must be True; this is enforced by the schema at the
        call site.
        """
        self._lo.append(r_int64(intmask(lo_u64)))
        assert self._hi is not None
        self._hi.append(hi_u64)
        if self._has_string:
            assert self._strs is not None
            self._strs.append("")

    def append_null(self, payload_col_idx):
        """
        Appends a null value and sets the corresponding bit in
        ``_null_word``. ``_has_nullable`` must be True.

        ``payload_col_idx`` is the 0-based payload column index (PK
        excluded) and must equal ``len(self._lo)`` at the time of the
        call — i.e. each column is appended in order.
        """
        self._lo.append(r_int64(0))
        if self._has_u128:
            assert self._hi is not None
            self._hi.append(r_uint64(0))
        if self._has_string:
            assert self._strs is not None
            self._strs.append("")
        # r_uint64 semantics: at payload_col_idx == 63, the shift produces
        # 9223372036854775808 with correct unsigned wraparound — no sign
        # extension, no overflow.
        self._null_word = self._null_word | (r_uint64(1) << payload_col_idx)

    # ------------------------------------------------------------------
    # Read accessors
    # col_idx is always the payload column index (0-based, PK excluded).
    # ------------------------------------------------------------------

    def is_null(self, payload_col_idx):
        """Returns True if this payload column is null."""
        if not self._has_nullable:
            return False
        return bool(self._null_word & (r_uint64(1) << payload_col_idx))

    def get_int(self, payload_col_idx):
        """Returns the value as ``r_uint64`` (unsigned semantics)."""
        return r_uint64(self._lo[payload_col_idx])

    def get_int_signed(self, payload_col_idx):
        return rffi.cast(rffi.LONGLONG, r_uint64(self._lo[payload_col_idx]))

    def get_float(self, payload_col_idx):
        """
        Bit-level reinterpret cast from the stored ``r_int64`` back to
        ``float``. Symmetric inverse of ``float2longlong`` used in
        ``append_float``.
        """
        return longlong2float(self._lo[payload_col_idx])

    def get_u128(self, payload_col_idx):
        """Returns the full 128-bit value as ``r_uint128``."""
        assert self._hi is not None
        lo = r_uint128(r_uint64(self._lo[payload_col_idx]))
        hi = r_uint128(self._hi[payload_col_idx])
        return (hi << 64) | lo

    def get_str(self, payload_col_idx):
        """Returns the string value for a TYPE_STRING column."""
        assert self._strs is not None
        return self._strs[payload_col_idx]


def make_payload_row(schema):
    """
    Canonical constructor for a ``PayloadRow``.

    MUST be called via this function rather than constructing ``PayloadRow``
    directly. This function is the sole enforcement point for the
    ``newlist_hint`` requirement described in ``gnitz/core/row_logic.py``:
    every list inside ``PayloadRow`` uses ``newlist_hint``, never a list
    literal. Using a literal (``[]``, ``[x, y]``, ``[None] * n``) anywhere
    marks that listdef as must-not-resize (mr), permanently poisoning every
    ``.append()`` call on any list of that element type across the entire
    program.
    """
    n, has_u128, has_string, has_nullable = _analyze_schema(schema)
    return PayloadRow(has_u128, has_string, has_nullable, n)
