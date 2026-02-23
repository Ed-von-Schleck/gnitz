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


def _compare_entries(idx_a, idx_b, pks_lo, pks_hi, rows, comparator):
    """
    Lexicographical comparison of (PK, Row Payload).
    Reconstructs u128 keys from 64-bit words to avoid alignment segfaults.
    """
    pk_a = (r_uint128(pks_hi[idx_a]) << 64) | r_uint128(pks_lo[idx_a])
    pk_b = (r_uint128(pks_hi[idx_b]) << 64) | r_uint128(pks_lo[idx_b])
    
    if pk_a < pk_b:
        return -1
    if pk_a > pk_b:
        return 1
    # PKs match, compare row payloads
    return comparator.compare(rows[idx_a], rows[idx_b])


def _mergesort_indices(indices, pks_lo, pks_hi, rows, lo, hi, scratch, comparator):
    if hi - lo <= 1:
        return
    mid = (lo + hi) >> 1
    _mergesort_indices(indices, pks_lo, pks_hi, rows, lo, mid, scratch, comparator)
    _mergesort_indices(indices, pks_lo, pks_hi, rows, mid, hi, scratch, comparator)
    _merge_indices(indices, pks_lo, pks_hi, rows, lo, mid, hi, scratch, comparator)


def _merge_indices(indices, pks_lo, pks_hi, rows, lo, mid, hi, scratch, comparator):
    for k in range(lo, mid):
        scratch[k] = indices[k]

    i = lo
    j = mid
    k = lo

    while i < mid and j < hi:
        if _compare_entries(scratch[i], indices[j], pks_lo, pks_hi, rows, comparator) <= 0:
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


def _permute_arrays(indices, pks_lo, pks_hi, weights, rows, n):
    new_pks_lo = newlist_hint(n)
    new_pks_hi = newlist_hint(n)
    new_weights = newlist_hint(n)
    new_rows = newlist_hint(n)
    for i in range(n):
        idx = indices[i]
        new_pks_lo.append(pks_lo[idx])
        new_pks_hi.append(pks_hi[idx])
        new_weights.append(weights[idx])
        new_rows.append(rows[idx])
    return new_pks_lo, new_pks_hi, new_weights, new_rows


# ---------------------------------------------------------------------------
# ZSetBatch
# ---------------------------------------------------------------------------

class ZSetBatch(object):
    """
    A DBSP Z-set batch: a multiset of (pk, weight, payload) triples.
    
    The engine maintains the "Ghost Property": records with net weight zero 
    are annihilated and removed from the stream during consolidation.

    PHYSICAL LAYOUT: Primary Keys are split into parallel List[r_uint64] 
    (_pks_lo, _pks_hi) to bypass a known RPython GC alignment bug when 
    storing r_uint128 in resizable lists.
    """

    _immutable_fields_ = ["_schema", "_comparator"]

    def __init__(self, schema):
        self._schema = schema
        # Parallel PK arrays used to avoid alignment segfaults
        self._pks_lo = newlist_hint(0)
        self._pks_hi = newlist_hint(0)
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
        """
        del self._pks_lo[:]
        del self._pks_hi[:]
        del self._weights[:]
        del self._rows[:]
        self._sorted = False

    def append(self, pk, weight, row):
        # PK Split: cast to r_uint64 and shift
        self._pks_lo.append(r_uint64(pk))
        self._pks_hi.append(r_uint64(pk >> 64))
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
        n = len(self._pks_lo)
        if n <= 1:
            self._sorted = True
            return

        indices = _build_index_array(n)
        scratch = _build_index_array(n)

        _mergesort_indices(
            indices, self._pks_lo, self._pks_hi, self._rows, 0, n, scratch, self._comparator
        )

        new_lo, new_hi, new_weights, new_rows = _permute_arrays(
            indices, self._pks_lo, self._pks_hi, self._weights, self._rows, n
        )
        self._pks_lo = new_lo
        self._pks_hi = new_hi
        self._weights = new_weights
        self._rows = new_rows
        self._sorted = True

    def consolidate(self):
        """
        Groups entries by (PK, Row Payload) and sums weights.
        Annihilates "Ghost" records where the net weight is zero.
        """
        n = len(self._pks_lo)
        if n == 0:
            return

        if not self._sorted:
            self.sort()

        new_lo = newlist_hint(n)
        new_hi = newlist_hint(n)
        new_weights = newlist_hint(n)
        new_rows = newlist_hint(n)

        cur_pk_lo = self._pks_lo[0]
        cur_pk_hi = self._pks_hi[0]
        cur_weight = self._weights[0]
        cur_row = self._rows[0]

        for i in range(1, n):
            lo = self._pks_lo[i]
            hi = self._pks_hi[i]
            row = self._rows[i]
            
            # Semantic equality check for both Key and Row Payload
            if lo == cur_pk_lo and hi == cur_pk_hi and self._comparator.compare(row, cur_row) == 0:
                cur_weight = cur_weight + self._weights[i]
            else:
                # Flush group if net weight is non-zero (Ghost Property)
                if cur_weight != r_int64(0):
                    new_lo.append(cur_pk_lo)
                    new_hi.append(cur_pk_hi)
                    new_weights.append(cur_weight)
                    new_rows.append(cur_row)
                cur_pk_lo = lo
                cur_pk_hi = hi
                cur_weight = self._weights[i]
                cur_row = row

        if cur_weight != r_int64(0):
            new_lo.append(cur_pk_lo)
            new_hi.append(cur_pk_hi)
            new_weights.append(cur_weight)
            new_rows.append(cur_row)

        self._pks_lo = new_lo
        self._pks_hi = new_hi
        self._weights = new_weights
        self._rows = new_rows

    # ------------------------------------------------------------------
    # Read accessors
    # ------------------------------------------------------------------

    def length(self):
        return len(self._pks_lo)

    def get_pk(self, i):
        # Reconstruct on-demand
        return (r_uint128(self._pks_hi[i]) << 64) | r_uint128(self._pks_lo[i])

    def get_weight(self, i):
        return self._weights[i]

    def get_row(self, i):
        return self._rows[i]

    def is_empty(self):
        return len(self._pks_lo) == 0

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
