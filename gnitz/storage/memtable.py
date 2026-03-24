# gnitz/storage/memtable.py

from rpython.rlib.rarithmetic import r_int64, r_uint64
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.core import comparator as core_comparator
from gnitz.core.batch import ArenaZSetBatch, ColumnarBatchAccessor, pk_lt, pk_eq
from gnitz.storage import writer_table
from gnitz.storage.bloom import BloomFilter

ACCUMULATOR_THRESHOLD = 64


def _batch_bytes(batch):
    """Sum all buffer offsets for a single ArenaZSetBatch."""
    total = (batch.pk_lo_buf.offset + batch.pk_hi_buf.offset +
             batch.weight_buf.offset + batch.null_buf.offset +
             batch.blob_arena.offset)
    col_bufs = batch.col_bufs
    for i in range(len(col_bufs)):
        total += col_bufs[i].offset
    return total


class MemTable(object):
    """
    Mutable, in-memory Z-Set storage.
    Maintains a list of sorted runs plus a small unsorted accumulator.
    """

    _immutable_fields_ = ["schema", "max_bytes"]

    def __init__(self, schema, arena_size):
        self.schema = schema
        initial_capacity = arena_size // (schema.memtable_stride + 32)
        if initial_capacity < 16:
            initial_capacity = 16
        self.runs = newlist_hint(8)               # list of sorted ArenaZSetBatch
        self._accumulator = ArenaZSetBatch(schema, initial_capacity=initial_capacity)
        self._total_row_count = 0
        self.max_bytes = arena_size
        self.bloom = BloomFilter(initial_capacity)
        self._runs_bytes = 0
        self._cached_consolidated = None

    def _invalidate_runs_cache(self):
        if self._cached_consolidated is not None:
            self._cached_consolidated.free()
            self._cached_consolidated = None

    def get_consolidated_snapshot(self):
        """Return an owned, consolidated snapshot of runs + accumulator.

        Caches the N-way merge of sorted runs.  If accumulator is non-empty,
        does a cheap 2-way merge of [cached_runs, sorted_acc].
        Caller owns the returned batch and must free it.
        """
        if self._cached_consolidated is None and len(self.runs) > 0:
            self._cached_consolidated = _merge_runs_to_consolidated(
                self.runs, self.schema
            )

        has_acc = self._accumulator.length() > 0

        if self._cached_consolidated is None:
            if has_acc:
                sorted_acc = self._accumulator.to_sorted()
                result = sorted_acc.to_consolidated()
                if result is sorted_acc:
                    result = sorted_acc.clone()
                if sorted_acc is not self._accumulator:
                    sorted_acc.free()
                return result
            return ArenaZSetBatch(self.schema)

        if not has_acc:
            return self._cached_consolidated.clone()

        sorted_acc = self._accumulator.to_sorted()
        two = [self._cached_consolidated, sorted_acc]
        result = _merge_runs_to_consolidated(two, self.schema)
        if sorted_acc is not self._accumulator:
            sorted_acc.free()
        return result

    def upsert_batch(self, batch):
        self._check_capacity()
        if batch.length() == 0:
            return
        sorted_batch = batch.to_sorted()
        # to_sorted() returns self when already sorted; we need an owned copy
        # because the caller may free the original batch after this call.
        if sorted_batch is batch:
            sorted_batch = batch.clone()
        self.runs.append(sorted_batch)
        self._runs_bytes += _batch_bytes(sorted_batch)
        self._invalidate_runs_cache()
        self._total_row_count += sorted_batch.length()
        num_records = batch.length()
        for i in range(num_records):
            self.bloom.add(batch.get_pk_lo(i), batch.get_pk_hi(i))

    def upsert_single(self, key_lo, key_hi, weight, accessor):
        self._check_capacity()
        self._accumulator.append_from_accessor(key_lo, key_hi, weight, accessor)
        self._total_row_count += 1
        self.bloom.add(key_lo, key_hi)
        if self._accumulator.length() >= ACCUMULATOR_THRESHOLD:
            self._flush_accumulator()

    def _flush_accumulator(self):
        if self._accumulator.length() == 0:
            return
        sorted_acc = self._accumulator.to_sorted()
        if sorted_acc is self._accumulator:
            # to_sorted returned self (<=1 row or already sorted)
            # clone it so we can reset the accumulator
            sorted_acc = self._accumulator.clone()
            self._accumulator.free()
        else:
            # to_sorted created a new batch; free the old accumulator
            self._accumulator.free()
        self._accumulator = ArenaZSetBatch(self.schema)
        self.runs.append(sorted_acc)
        self._runs_bytes += _batch_bytes(sorted_acc)
        self._invalidate_runs_cache()

    def _total_bytes(self):
        return self._runs_bytes + _batch_bytes(self._accumulator)

    def _check_capacity(self):
        if self._total_bytes() > self.max_bytes:
            raise errors.MemTableFullError()

    def should_flush(self):
        return self._total_bytes() > self.max_bytes * 3 // 4

    def lookup_pk(self, key_lo, key_hi):
        """Find a PK across all runs and the accumulator.

        Returns (net_weight, batch_or_None, row_idx) where batch+row_idx
        point to the first row found with this PK.
        """
        total_w = r_int64(0)
        found_batch = None
        found_idx = -1
        for ri in range(len(self.runs)):
            run = self.runs[ri]
            lo = _lower_bound(run, key_lo, key_hi)
            while lo < run.length() and pk_eq(run.get_pk_lo(lo), run.get_pk_hi(lo), key_lo, key_hi):
                total_w += run.get_weight(lo)
                if found_batch is None:
                    found_batch = run
                    found_idx = lo
                lo += 1
        n = self._accumulator.length()
        for i in range(n):
            if pk_eq(self._accumulator.get_pk_lo(i), self._accumulator.get_pk_hi(i), key_lo, key_hi):
                total_w += self._accumulator.get_weight(i)
                if found_batch is None:
                    found_batch = self._accumulator
                    found_idx = i
        return total_w, found_batch, found_idx

    def find_weight_for_row(self, key_lo, key_hi, accessor):
        total_w = r_int64(0)
        batch_acc = ColumnarBatchAccessor(self.schema)
        # Binary search each sorted run
        for ri in range(len(self.runs)):
            run = self.runs[ri]
            lo = _lower_bound(run, key_lo, key_hi)
            while lo < run.length() and pk_eq(run.get_pk_lo(lo), run.get_pk_hi(lo), key_lo, key_hi):
                batch_acc.bind(run, lo)
                if core_comparator.compare_rows(self.schema, batch_acc, accessor) == 0:
                    total_w += run.get_weight(lo)
                lo += 1
        # Linear scan accumulator
        n = self._accumulator.length()
        for i in range(n):
            if pk_eq(self._accumulator.get_pk_lo(i), self._accumulator.get_pk_hi(i), key_lo, key_hi):
                batch_acc.bind(self._accumulator, i)
                if core_comparator.compare_rows(self.schema, batch_acc, accessor) == 0:
                    total_w += self._accumulator.get_weight(i)
        return total_w

    def may_contain_pk(self, key_lo, key_hi):
        return self.bloom.may_contain(key_lo, key_hi)

    def flush(self, dirfd, basename, table_id=0, durable=True):
        self._flush_accumulator()
        consolidated = self.get_consolidated_snapshot()
        wrote = writer_table.write_batch_to_shard(
            consolidated, dirfd, basename, table_id, durable=durable
        )
        consolidated.free()
        return wrote

    def is_empty(self):
        return self._total_row_count == 0

    def reset(self):
        """Reset for reuse after flush. Frees run data, keeps accumulator and bloom buffers."""
        for r in self.runs:
            r.free()
        self.runs = newlist_hint(8)
        self._runs_bytes = 0
        self._invalidate_runs_cache()
        self._total_row_count = 0
        self.bloom.reset()

    def free(self):
        for r in self.runs:
            r.free()
        self.runs = newlist_hint(8)
        self._runs_bytes = 0
        self._invalidate_runs_cache()
        self._accumulator.free()
        self._total_row_count = 0
        self.bloom.free()


def _merge_runs_to_consolidated(runs, schema):
    """Merge N sorted runs into one owned, consolidated ArenaZSetBatch."""
    if len(runs) == 0:
        return ArenaZSetBatch(schema)

    if len(runs) == 1:
        consolidated = runs[0].to_consolidated()
        if consolidated is runs[0]:
            consolidated = runs[0].clone()
        consolidated.mark_consolidated(True)
        return consolidated

    # Fused k-way merge + inline consolidation: one pass, one allocation.
    from gnitz.storage.cursor import SortedBatchCursor
    from gnitz.storage import tournament_tree

    cursors = newlist_hint(len(runs))
    for r in runs:
        cursors.append(SortedBatchCursor(r))
    tree = tournament_tree.TournamentTree(cursors, schema)

    consolidated = ArenaZSetBatch(schema)

    has_pending = False
    pending_pk_lo = r_uint64(0)
    pending_pk_hi = r_uint64(0)
    pending_weight_acc = r_int64(0)
    pending_acc = ColumnarBatchAccessor(schema)
    cur_acc = ColumnarBatchAccessor(schema)

    while not tree.is_exhausted():
        ci = rffi.cast(lltype.Signed, tree.heap[0].cursor_idx)
        cur = cursors[ci]
        cur_pk_lo = cur.key_lo()
        cur_pk_hi = cur.key_hi()
        cur_weight = cur.weight()

        if not has_pending:
            pending_pk_lo = cur_pk_lo
            pending_pk_hi = cur_pk_hi
            pending_weight_acc = cur_weight
            cur.bind_to(pending_acc)
            has_pending = True
        elif not pk_eq(cur_pk_lo, cur_pk_hi, pending_pk_lo, pending_pk_hi):
            if pending_weight_acc != r_int64(0):
                consolidated.append_from_accessor(pending_pk_lo, pending_pk_hi, pending_weight_acc, pending_acc)
            pending_pk_lo = cur_pk_lo
            pending_pk_hi = cur_pk_hi
            pending_weight_acc = cur_weight
            cur.bind_to(pending_acc)
        else:
            cur.bind_to(cur_acc)
            if core_comparator.compare_rows(schema, pending_acc, cur_acc) == 0:
                pending_weight_acc += cur_weight
            else:
                if pending_weight_acc != r_int64(0):
                    consolidated.append_from_accessor(pending_pk_lo, pending_pk_hi, pending_weight_acc, pending_acc)
                pending_pk_lo = cur_pk_lo
                pending_pk_hi = cur_pk_hi
                pending_weight_acc = cur_weight
                cur.bind_to(pending_acc)

        tree.advance_cursor_by_index(ci)

    if has_pending and pending_weight_acc != r_int64(0):
        consolidated.append_from_accessor(pending_pk_lo, pending_pk_hi, pending_weight_acc, pending_acc)

    tree.close()
    consolidated._sorted = True
    return consolidated


def _lower_bound(run, key_lo, key_hi):
    """Binary search for the first index where pk >= key."""
    lo = 0
    hi = run.length()
    while lo < hi:
        mid = (lo + hi) >> 1
        if pk_lt(run.get_pk_lo(mid), run.get_pk_hi(mid), key_lo, key_hi):
            lo = mid + 1
        else:
            hi = mid
    return lo


def _binary_search_weight(run, key_lo, key_hi):
    """Binary search a sorted run for all rows with the given PK, sum weights."""
    total_w = r_int64(0)
    lo = _lower_bound(run, key_lo, key_hi)
    while lo < run.length() and pk_eq(run.get_pk_lo(lo), run.get_pk_hi(lo), key_lo, key_hi):
        total_w += run.get_weight(lo)
        lo += 1
    return total_w
