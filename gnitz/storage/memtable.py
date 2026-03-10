# gnitz/storage/memtable.py

from rpython.rlib.rarithmetic import r_int64, r_uint64
from rpython.rlib.rarithmetic import r_ulonglonglong as r_uint128
from rpython.rtyper.lltypesystem import rffi, lltype

from gnitz.core import errors
from gnitz.core import comparator as core_comparator
from gnitz.core.batch import ArenaZSetBatch, ColumnarBatchAccessor
from gnitz.storage import writer_table
from gnitz.storage.bloom import BloomFilter


class MemTable(object):
    """
    Mutable, in-memory Z-Set storage.
    Flat columnar batch with deferred sorting and coalescing on flush/cursor.
    """

    _immutable_fields_ = ["schema", "max_bytes"]

    def __init__(self, schema, arena_size):
        self.schema = schema
        initial_capacity = arena_size // (schema.memtable_stride + 32)
        if initial_capacity < 16:
            initial_capacity = 16
        self.batch = ArenaZSetBatch(schema, initial_capacity=initial_capacity)
        self.max_bytes = arena_size
        self.bloom = BloomFilter(initial_capacity)

    def upsert_batch(self, batch):
        self._check_capacity()
        self.batch.append_batch(batch)
        num_records = batch.length()
        for i in range(num_records):
            self.bloom.add(batch.get_pk(i))

    def upsert_single(self, key, weight, accessor):
        self._check_capacity()
        self.batch.append_from_accessor(key, weight, accessor)
        self.bloom.add(key)

    def _total_bytes(self):
        total = (
            self.batch.pk_lo_buf.offset
            + self.batch.pk_hi_buf.offset
            + self.batch.weight_buf.offset
            + self.batch.null_buf.offset
            + self.batch.blob_arena.offset
        )
        col_bufs = self.batch.col_bufs
        for i in range(len(col_bufs)):
            total += col_bufs[i].offset
        return total

    def _check_capacity(self):
        if self._total_bytes() > self.max_bytes:
            raise errors.MemTableFullError()

    def should_flush(self):
        return self._total_bytes() > self.max_bytes * 3 // 4

    def get_weight_for_pk(self, key):
        total_w = r_int64(0)
        n = self.batch.length()
        for i in range(n):
            if self.batch.get_pk(i) == key:
                total_w += self.batch.get_weight(i)
        return total_w

    def find_weight_for_row(self, key, accessor):
        total_w = r_int64(0)
        batch_acc = ColumnarBatchAccessor(self.schema)
        n = self.batch.length()
        for i in range(n):
            if self.batch.get_pk(i) == key:
                batch_acc.bind(self.batch, i)
                if (
                    core_comparator.compare_rows(
                        self.schema, batch_acc, accessor
                    )
                    == 0
                ):
                    total_w += self.batch.get_weight(i)
        return total_w

    def may_contain_pk(self, key):
        return self.bloom.may_contain(key)

    def flush(self, filename, table_id=0):
        consolidated = self.batch.to_consolidated()
        writer_table.write_batch_to_shard(consolidated, filename, table_id)
        if consolidated is not self.batch:
            consolidated.free()

    def is_empty(self):
        return self.batch.is_empty()

    def free(self):
        self.batch.free()
        self.bloom.free()
