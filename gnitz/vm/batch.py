# gnitz/vm/batch.py
#
# ZSetBatch: the runtime Z-set batch type used throughout the DBSP VM layer.
#
# Migration note (PayloadRow)
# ----------------------------
# Internal row storage changes from ``List[List[TaggedValue]]`` to
# ``List[PayloadRow]``.  The three parallel arrays (_pks, _weights, _rows)
# now store:
#   _pks    : List[r_uint128]   primary keys
#   _weights: List[r_int64]     algebraic weights
#   _rows   : List[PayloadRow]  payload data (non-PK columns)
#
# CRITICAL — newlist_hint across batch arrays and PayloadRow arrays
# ----------------------------------------------------------------
# ``_weights`` is ``List[r_int64]``, which shares the same RPython listdef as
# ``PayloadRow._lo`` (also ``List[r_int64]``).  Any construction of a
# ``List[r_int64]`` using a list literal (``[]``, ``[x]``, ``[None]*n``)
# anywhere in the program would mark that listdef as must-not-resize (mr),
# permanently preventing ``PayloadRow.append_int`` and every other
# ``_lo.append`` from working.
#
# The rule therefore extends beyond ``PayloadRow`` itself: every list in
# ``ZSetBatch`` that shares an element type with a ``PayloadRow`` internal
# array MUST also use ``newlist_hint``.  This module enforces the rule for
# ``_weights`` (List[r_int64]) and ``_rows`` (List[PayloadRow]).
#
# ``_pks`` (List[r_uint128]) is a distinct element type with its own listdef
# and is not at risk, but uses ``newlist_hint`` uniformly for consistency.

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128

from gnitz.core.values import PayloadRow, make_payload_row, _analyze_schema
from gnitz.core.row_logic import PayloadRowComparator


# ---------------------------------------------------------------------------
# Null-row singleton cache (plan §13)
# ---------------------------------------------------------------------------
# VM operators that produce identity elements (all-null rows) for empty
# groups must avoid repeated allocation.  This cache maps schema id to the
# pre-built all-null PayloadRow for that schema.
#
# Safety: PayloadRow has no public mutation API after construction.  Schemas
# are module-level singletons in practice, making id(schema) a stable key.
# In RPython, id() returns an integer (intmask of the object address); for
# non-moving GC roots this is stable for the process lifetime.

_NULL_ROW_CACHE = {}   # Dict[int, PayloadRow]


def get_null_row(schema):
    """
    Return a shared all-null ``PayloadRow`` for ``schema``.

    The row is constructed once per schema and cached.  All values are null
    (``_null_word`` has every payload-column bit set).  ``_has_nullable`` is
    forced to ``True`` so the null bitmap is always present, even for schemas
    that declare no nullable columns — this is intentional: the null row is
    a DBSP identity element, not a real database row.

    The returned row must never be mutated; treat it as an immutable singleton.
    """
    sid = id(schema)
    if sid not in _NULL_ROW_CACHE:
        n, has_u128, has_string, _ = _analyze_schema(schema)
        # Force has_nullable=True so _null_word is active.
        row = PayloadRow(has_u128, has_string, True, n)
        for i in range(n):
            row.append_null(i)
        _NULL_ROW_CACHE[sid] = row
    return _NULL_ROW_CACHE[sid]


# ---------------------------------------------------------------------------
# Merge-sort helpers (O(n log n) sort of the index array by PK)
# ---------------------------------------------------------------------------
# Sorting three parallel arrays (pks, weights, rows) is implemented as:
#   1. Build an index array [0, 1, 2, ..., n-1].
#   2. Merge-sort the index array by comparing pks[indices[i]].
#   3. Permute the three arrays according to the sorted index array.
#
# This avoids touching the heavyweight PayloadRow objects during the sort
# pass — only integer indices are moved.  The permutation step copies object
# references (not the row data itself).

def _build_index_array(n):
    """Return a fresh ``List[int]`` containing 0..n-1."""
    indices = newlist_hint(n)
    for i in range(n):
        indices.append(i)
    return indices


def _mergesort_indices(indices, pks, lo, hi, scratch):
    """
    Recursively merge-sort ``indices[lo:hi]`` so that
    ``pks[indices[lo]] <= pks[indices[lo+1]] <= ...``.

    ``scratch`` is a pre-allocated ``List[int]`` of length ``n`` used as a
    temporary buffer during the merge step.  It is passed in to avoid
    repeated allocation.
    """
    if hi - lo <= 1:
        return
    mid = (lo + hi) >> 1
    _mergesort_indices(indices, pks, lo,  mid, scratch)
    _mergesort_indices(indices, pks, mid, hi,  scratch)
    _merge_indices(indices, pks, lo, mid, hi, scratch)


def _merge_indices(indices, pks, lo, mid, hi, scratch):
    """
    In-place merge of two already-sorted halves ``indices[lo:mid]`` and
    ``indices[mid:hi]``.  Uses ``scratch`` as a temporary buffer.
    """
    # Copy left half into scratch.
    for k in range(lo, mid):
        scratch[k] = indices[k]

    i = lo    # pointer into scratch (left half)
    j = mid   # pointer into right half (still in indices)
    k = lo    # write pointer into indices

    while i < mid and j < hi:
        if pks[scratch[i]] <= pks[indices[j]]:
            indices[k] = scratch[i]
            i += 1
        else:
            indices[k] = indices[j]
            j += 1
        k += 1

    # Drain remaining left elements (right elements are already in place).
    while i < mid:
        indices[k] = scratch[i]
        i += 1
        k += 1


def _permute_arrays(indices, pks, weights, rows, n):
    """
    Reorder three parallel arrays in-place according to ``indices``.

    Returns three new lists in sorted order.  Uses ``newlist_hint`` to avoid
    mr-poisoning (see module-level note).
    """
    new_pks     = newlist_hint(n)
    new_weights = newlist_hint(n)
    new_rows    = newlist_hint(n)
    for i in range(n):
        idx = indices[i]
        new_pks.append(pks[idx])
        new_weights.append(weights[idx])
        new_rows.append(rows[idx])
    return new_pks, new_weights, new_rows


# ---------------------------------------------------------------------------
# ZSetBatch
# ---------------------------------------------------------------------------

class ZSetBatch(object):
    """
    A DBSP Z-set batch: a multiset of ``(pk, weight, payload)`` triples where
    each entry assigns an algebraic weight to a database record.

    Layout
    ------
    Three parallel lists of equal length, indexed by position:

    ``_pks[i]``     : ``r_uint128`` — primary key of the i-th entry.
    ``_weights[i]`` : ``r_int64``   — algebraic weight (positive = insert,
                                      negative = delete, any non-zero integer
                                      is valid in a batch before consolidation).
    ``_rows[i]``    : ``PayloadRow`` — non-PK column values.

    Lifecycle
    ---------
    1. Construct with ``ZSetBatch(schema)``.
    2. Call ``append(pk, weight, row)`` to add records.
    3. Call ``sort()`` to order by PK (required before ``consolidate``).
    4. Call ``consolidate()`` to merge same-PK entries and drop zero weights.
    5. Pass to downstream operators or flush to storage.

    Schema
    ------
    ``_schema`` is held in ``_immutable_fields_`` so the JIT can specialise
    on it.  All downstream operations that receive a ``ZSetBatch`` can assume
    the schema is a JIT-promoted constant.
    """

    _immutable_fields_ = ['_schema']

    def __init__(self, schema):
        self._schema  = schema
        # All three arrays MUST use newlist_hint.  See module-level note.
        self._pks     = newlist_hint(0)   # List[r_uint128]
        self._weights = newlist_hint(0)   # List[r_int64]
        self._rows    = newlist_hint(0)   # List[PayloadRow]
        self._sorted  = False

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def append(self, pk, weight, row):
        """
        Append a single record to the batch.

        ``pk``     : ``r_uint128`` primary key.
        ``weight`` : ``r_int64`` algebraic weight.
        ``row``    : ``PayloadRow`` constructed via ``make_payload_row(schema)``
                     with all non-PK columns appended in schema order.

        After calling ``append``, the batch is no longer considered sorted.
        """
        self._pks.append(pk)
        self._weights.append(weight)
        self._rows.append(row)
        self._sorted = False

    # ------------------------------------------------------------------
    # Sorting
    # ------------------------------------------------------------------

    def sort(self):
        """
        Sort the batch by primary key (ascending).

        Uses an O(n log n) merge sort on an index array to avoid moving
        ``PayloadRow`` objects during the sort pass.  The three parallel arrays
        are then permuted in a single O(n) pass.

        After this call ``_sorted`` is ``True`` and ``consolidate`` can be
        called safely.
        """
        n = len(self._pks)
        if n <= 1:
            self._sorted = True
            return

        indices = _build_index_array(n)
        # Scratch buffer for the merge step — allocated once.
        scratch = _build_index_array(n)

        _mergesort_indices(indices, self._pks, 0, n, scratch)

        new_pks, new_weights, new_rows = _permute_arrays(
            indices, self._pks, self._weights, self._rows, n
        )
        self._pks     = new_pks
        self._weights = new_weights
        self._rows    = new_rows
        self._sorted  = True

    # ------------------------------------------------------------------
    # Consolidation
    # ------------------------------------------------------------------

    def consolidate(self):
        """
        Merge same-PK entries by summing their weights and drop entries with
        net weight zero.

        Requires ``sort()`` to have been called first (the batch must be sorted
        by PK).

        For each group of consecutive entries sharing the same PK, their
        weights are summed.  If the net weight is non-zero, one entry is
        emitted with the first payload row seen for that PK and the summed
        weight.

        In GnitzDB's uniqueness model, two entries with the same PK carry the
        same payload (one represents a logical delete, the other a logical
        insert of the same record, or they are duplicate inserts that cancel).
        Keeping the first payload row is always correct.
        """
        n = len(self._pks)
        if n == 0:
            return

        new_pks     = newlist_hint(n)
        new_weights = newlist_hint(n)
        new_rows    = newlist_hint(n)

        cur_pk     = self._pks[0]
        cur_weight = self._weights[0]
        cur_row    = self._rows[0]

        for i in range(1, n):
            pk = self._pks[i]
            if pk == cur_pk:
                # Same PK — accumulate the weight.
                cur_weight = cur_weight + self._weights[i]
            else:
                # PK boundary — emit previous group if net weight is non-zero.
                if cur_weight != r_int64(0):
                    new_pks.append(cur_pk)
                    new_weights.append(cur_weight)
                    new_rows.append(cur_row)
                cur_pk     = pk
                cur_weight = self._weights[i]
                cur_row    = self._rows[i]

        # Flush the last group.
        if cur_weight != r_int64(0):
            new_pks.append(cur_pk)
            new_weights.append(cur_weight)
            new_rows.append(cur_row)

        self._pks     = new_pks
        self._weights = new_weights
        self._rows    = new_rows

    # ------------------------------------------------------------------
    # Payload row comparison
    # ------------------------------------------------------------------

    def _row_cmp(self, row_a, row_b):
        """
        Compare two ``PayloadRow`` objects by their data content using the
        schema-aware comparator from ``gnitz.core.row_logic``.

        Before: this method called ``TaggedValue``-based comparison logic.
        After: delegates to ``PayloadRowComparator`` which uses
        ``PayloadRowAccessor`` and ``comparator.compare_rows``.

        Returns -1, 0, or 1.  Used when ordering by data columns
        (e.g. ORDER BY non-PK columns, sort-merge join payload comparison).
        """
        cmp = PayloadRowComparator(self._schema)
        return cmp.compare(row_a, row_b)

    # ------------------------------------------------------------------
    # Read accessors
    # ------------------------------------------------------------------

    def length(self):
        """Return the number of entries in the batch."""
        return len(self._pks)

    def get_pk(self, i):
        """Return the primary key of the i-th entry."""
        return self._pks[i]

    def get_weight(self, i):
        """Return the algebraic weight of the i-th entry."""
        return self._weights[i]

    def get_row(self, i):
        """Return the ``PayloadRow`` for the i-th entry."""
        return self._rows[i]

    def is_empty(self):
        """Return True if the batch contains no entries."""
        return len(self._pks) == 0

    def is_sorted(self):
        """Return True if ``sort()`` has been called since the last append."""
        return self._sorted

    # ------------------------------------------------------------------
    # Batch combination
    # ------------------------------------------------------------------

    def extend(self, other):
        """
        Append all entries from ``other`` into this batch.

        The batch is considered unsorted after this operation.  Typically used
        when merging the outputs of two delta operators before consolidation.

        ``other`` must have the same schema as ``self``.
        """
        n = other.length()
        for i in range(n):
            self.append(other.get_pk(i), other.get_weight(i), other.get_row(i))


# ---------------------------------------------------------------------------
# ZSetBatch construction helpers
# ---------------------------------------------------------------------------

def make_empty_batch(schema):
    """Return a new empty ``ZSetBatch`` for ``schema``."""
    return ZSetBatch(schema)


def make_singleton_batch(schema, pk, weight, row):
    """Return a ``ZSetBatch`` containing exactly one entry."""
    batch = ZSetBatch(schema)
    batch.append(pk, weight, row)
    batch._sorted = True   # One-element batch is trivially sorted.
    return batch
