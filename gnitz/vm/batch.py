# gnitz/vm/batch.py

from rpython.rlib.objectmodel import newlist_hint
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128

from gnitz.core.values import PayloadRow, make_payload_row, _analyze_schema
from gnitz.core.row_logic import PayloadRowComparator


# ---------------------------------------------------------------------------
# Null-row singleton cache
# ---------------------------------------------------------------------------
_NULL_ROW_CACHE = {}   # Dict[int, PayloadRow]


def get_null_row(schema):
    """
    Return a shared all-null ``PayloadRow`` for ``schema``.
    Used as the identity element for DBSP operators.
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
# Merge-sort helpers (O(n log n) sort of the index array)
# ---------------------------------------------------------------------------

def _build_index_array(n):
    indices = newlist_hint(n)
    for i in range(n):
        indices.append(i)
    return indices


def _compare_entries(idx_a, idx_b, pks, rows, comparator):
    """
    Lexicographical comparison of (PK, Row Payload).
    Satisfies Multiset Semantics: identical PKs with different payloads
    are distinct records.
    """
    pk_a = pks[idx_a]
    pk_b = pks[idx_b]
    if pk_a < pk_b:
        return -1
    if pk_a > pk_b:
        return 1
    # PKs match, compare row payloads
    return comparator.compare(rows[idx_a], rows[idx_b])


def _mergesort_indices(indices, pks, rows, lo, hi, scratch, comparator):
    if hi - lo <= 1:
        return
    mid = (lo + hi) >> 1
    _mergesort_indices(indices, pks, rows, lo, mid, scratch, comparator)
    _mergesort_indices(indices, pks, rows, mid, hi, scratch, comparator)
    _merge_indices(indices, pks, rows, lo, mid, hi, scratch, comparator)


def _merge_indices(indices, pks, rows, lo, mid, hi, scratch, comparator):
    for k in range(lo, mid):
        scratch[k] = indices[k]

    i = lo
    j = mid
    k = lo

    while i < mid and j < hi:
        if _compare_entries(scratch[i], indices[j], pks, rows, comparator) <= 0:
            indices[k] = scratch[i]
            i += 1
        else:
            indices[k] = indices[j]
            j += 1
        k += 1

    while i < mid:
        indices[k] = scratch[i]
        i += 1
        k += 1


def _permute_arrays(indices, pks, weights, rows, n):
    new_pks = newlist_hint(n)
    new_weights = newlist_hint(n)
    new_rows = newlist_hint(n)
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
    A DBSP Z-set batch: a multiset of (pk, weight, payload) triples.
    
    The engine maintains the "Ghost Property": records with net weight zero 
    are annihilated and removed from the stream during consolidation.
    """

    _immutable_fields_ = ["_schema", "_comparator"]

    def __init__(self, schema):
        self._schema = schema
        # All three arrays MUST use newlist_hint to avoid mr-poisoning.
        self._pks = newlist_hint(0)
        self._weights = newlist_hint(0)
        self._rows = newlist_hint(0)
        self._sorted = False
        # Pre-allocated to avoid GC churn during sorting/consolidation
        self._comparator = PayloadRowComparator(schema)

    # ------------------------------------------------------------------
    # Maintenance
    # ------------------------------------------------------------------

    def clear(self):
        """
        Resets the batch to empty state for reuse.
        
        CRITICAL: Uses in-place slice deletion to prevent mr-poisoning of 
        the List[r_int64] and List[r_uint128] listdefs.
        """
        del self._pks[:]
        del self._weights[:]
        del self._rows[:]
        self._sorted = False

    def append(self, pk, weight, row):
        self._pks.append(pk)
        self._weights.append(weight)
        self._rows.append(row)
        self._sorted = False

    # ------------------------------------------------------------------
    # Algebraic Operations
    # ------------------------------------------------------------------

    def sort(self):
        """
        Sorts the batch by (Primary Key, Row Payload).
        Required before consolidation or sort-merge joins.
        """
        n = len(self._pks)
        if n <= 1:
            self._sorted = True
            return

        indices = _build_index_array(n)
        scratch = _build_index_array(n)

        _mergesort_indices(
            indices, self._pks, self._rows, 0, n, scratch, self._comparator
        )

        new_pks, new_weights, new_rows = _permute_arrays(
            indices, self._pks, self._weights, self._rows, n
        )
        self._pks = new_pks
        self._weights = new_weights
        self._rows = new_rows
        self._sorted = True

    def consolidate(self):
        """
        Groups entries by (PK, Row Payload) and sums weights.
        Annihilates "Ghost" records where the net weight is zero.
        """
        n = len(self._pks)
        if n == 0:
            return

        if not self._sorted:
            self.sort()

        new_pks = newlist_hint(n)
        new_weights = newlist_hint(n)
        new_rows = newlist_hint(n)

        cur_pk = self._pks[0]
        cur_weight = self._weights[0]
        cur_row = self._rows[0]

        for i in range(1, n):
            pk = self._pks[i]
            row = self._rows[i]
            
            # Semantic equality check for both Key and Row Payload
            if pk == cur_pk and self._comparator.compare(row, cur_row) == 0:
                cur_weight = cur_weight + self._weights[i]
            else:
                # Flush group if net weight is non-zero (Ghost Property)
                if cur_weight != r_int64(0):
                    new_pks.append(cur_pk)
                    new_weights.append(cur_weight)
                    new_rows.append(cur_row)
                cur_pk = pk
                cur_weight = self._weights[i]
                cur_row = row

        if cur_weight != r_int64(0):
            new_pks.append(cur_pk)
            new_weights.append(cur_weight)
            new_rows.append(cur_row)

        self._pks = new_pks
        self._weights = new_weights
        self._rows = new_rows

    # ------------------------------------------------------------------
    # Read accessors
    # ------------------------------------------------------------------

    def length(self):
        return len(self._pks)

    def get_pk(self, i):
        return self._pks[i]

    def get_weight(self, i):
        return self._weights[i]

    def get_row(self, i):
        return self._rows[i]

    def is_empty(self):
        return len(self._pks) == 0

    def is_sorted(self):
        return self._sorted

    def extend(self, other):
        n = other.length()
        for i in range(n):
            self.append(other.get_pk(i), other.get_weight(i), other.get_row(i))


# ---------------------------------------------------------------------------
# Construction helpers
# ---------------------------------------------------------------------------

def make_empty_batch(schema):
    return ZSetBatch(schema)


def make_singleton_batch(schema, pk, weight, row):
    batch = ZSetBatch(schema)
    batch.append(pk, weight, row)
    batch._sorted = True
    return batch
