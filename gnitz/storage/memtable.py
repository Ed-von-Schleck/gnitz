# gnitz/storage/memtable.py

from rpython.rlib.rarithmetic import r_int64, r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rlib.objectmodel import newlist_hint
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.core import comparator as core_comparator
from gnitz.core.batch import ArenaZSetBatch, ColumnarBatchAccessor
from gnitz.storage import writer_table
from gnitz.storage.bloom import BloomFilter

ACCUMULATOR_THRESHOLD = 64


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
        self._total_row_count += sorted_batch.length()
        num_records = batch.length()
        for i in range(num_records):
            self.bloom.add(batch.get_pk(i))

    def upsert_single(self, key, weight, accessor):
        self._check_capacity()
        self._accumulator.append_from_accessor(key, weight, accessor)
        self._total_row_count += 1
        self.bloom.add(key)
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

    def _total_bytes(self):
        total = (
            self._accumulator.pk_lo_buf.offset
            + self._accumulator.pk_hi_buf.offset
            + self._accumulator.weight_buf.offset
            + self._accumulator.null_buf.offset
            + self._accumulator.blob_arena.offset
        )
        col_bufs = self._accumulator.col_bufs
        for i in range(len(col_bufs)):
            total += col_bufs[i].offset
        for r in self.runs:
            total += (
                r.pk_lo_buf.offset
                + r.pk_hi_buf.offset
                + r.weight_buf.offset
                + r.null_buf.offset
                + r.blob_arena.offset
            )
            rc = r.col_bufs
            for j in range(len(rc)):
                total += rc[j].offset
        return total

    def _check_capacity(self):
        if self._total_bytes() > self.max_bytes:
            raise errors.MemTableFullError()

    def should_flush(self):
        return self._total_bytes() > self.max_bytes * 3 // 4

    def get_weight_for_pk(self, key):
        total_w = r_int64(0)
        # Binary search each sorted run
        for ri in range(len(self.runs)):
            run = self.runs[ri]
            total_w += _binary_search_weight(run, key)
        # Linear scan accumulator
        n = self._accumulator.length()
        for i in range(n):
            if self._accumulator.get_pk(i) == key:
                total_w += self._accumulator.get_weight(i)
        return total_w

    def find_weight_for_row(self, key, accessor):
        total_w = r_int64(0)
        batch_acc = ColumnarBatchAccessor(self.schema)
        # Binary search each sorted run
        for ri in range(len(self.runs)):
            run = self.runs[ri]
            lo = 0
            hi = run.length()
            while lo < hi:
                mid = (lo + hi) >> 1
                if run.get_pk(mid) < key:
                    lo = mid + 1
                else:
                    hi = mid
            while lo < run.length() and run.get_pk(lo) == key:
                batch_acc.bind(run, lo)
                if core_comparator.compare_rows(self.schema, batch_acc, accessor) == 0:
                    total_w += run.get_weight(lo)
                lo += 1
        # Linear scan accumulator
        n = self._accumulator.length()
        for i in range(n):
            if self._accumulator.get_pk(i) == key:
                batch_acc.bind(self._accumulator, i)
                if core_comparator.compare_rows(self.schema, batch_acc, accessor) == 0:
                    total_w += self._accumulator.get_weight(i)
        return total_w

    def may_contain_pk(self, key):
        return self.bloom.may_contain(key)

    def flush(self, filename, table_id=0):
        # Flush accumulator into runs first
        self._flush_accumulator()

        if len(self.runs) == 0:
            # Nothing to flush — write an empty consolidated batch
            empty = ArenaZSetBatch(self.schema)
            writer_table.write_batch_to_shard(empty, filename, table_id)
            empty.free()
            return

        if len(self.runs) == 1:
            consolidated = self.runs[0].to_consolidated()
            writer_table.write_batch_to_shard(consolidated, filename, table_id)
            if consolidated is not self.runs[0]:
                consolidated.free()
        else:
            # k-way merge all runs into a single consolidated batch
            from gnitz.storage.cursor import SortedBatchCursor
            from gnitz.storage import tournament_tree

            cursors = newlist_hint(len(self.runs))
            for r in self.runs:
                cursors.append(SortedBatchCursor(r))
            tree = tournament_tree.TournamentTree(cursors, self.schema)

            merged = ArenaZSetBatch(self.schema)

            while not tree.is_exhausted():
                # Heap root is always the global minimum
                ci = rffi.cast(lltype.Signed, tree.heap[0].cursor_idx)
                cur = cursors[ci]
                merged.append_from_accessor(cur.key(), cur.weight(), cur.get_accessor())
                tree.advance_cursor_by_index(ci)

            tree.close()

            # The merged batch is sorted but not consolidated — consolidate it
            consolidated = merged.to_consolidated()
            writer_table.write_batch_to_shard(consolidated, filename, table_id)
            if consolidated is not merged:
                consolidated.free()
            merged.free()

    def is_empty(self):
        return self._total_row_count == 0

    def free(self):
        for r in self.runs:
            r.free()
        self.runs = newlist_hint(8)
        self._accumulator.free()
        self._total_row_count = 0
        self.bloom.free()


def _binary_search_weight(run, key):
    """Binary search a sorted run for all rows with the given PK, sum weights."""
    total_w = r_int64(0)
    lo = 0
    hi = run.length()
    while lo < hi:
        mid = (lo + hi) >> 1
        if run.get_pk(mid) < key:
            lo = mid + 1
        else:
            hi = mid
    # lo is now the first position where pk >= key
    while lo < run.length() and run.get_pk(lo) == key:
        total_w += run.get_weight(lo)
        lo += 1
    return total_w
