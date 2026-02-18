# tests/storage/test_u128_payload.py

"""
Round-trip correctness tests for TYPE_U128 non-PK columns.

Covers the full data path:
  TaggedValue.make_u128 → MemTable._pack_into_node → MemTable.flush →
  writer_table.add_row / TableShardWriter → SoAAccessor.get_u128 →
  Engine.get_effective_weight_raw → PersistentTable.get_weight

And WAL recovery:
  write_wal_block TYPE_U128 branch → decode_wal_block TYPE_U128 branch →
  MemTable.upsert → PersistentTable.get_weight

The key invariant in all tests: a u128 value with non-zero HIGH WORD must
survive every layer of serialization and comparison with the high word intact.
Any pre-fix truncation would cause the high word to read back as zero,
making two distinct UUIDs appear equal when they differ only in the high word.
"""

import os
import unittest
from rpython.rlib.rarithmetic import r_uint64, r_ulonglonglong as r_uint128

from gnitz.core import types, values
from gnitz.core.row_logic import make_payload_row
from gnitz.core import zset


def _make_schema():
    return types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U64),   # PK (64-bit)
            types.ColumnDefinition(types.TYPE_U128),  # non-PK UUID foreign key
            types.ColumnDefinition(types.TYPE_STRING),# extra column
        ],
        pk_index=0,
    )


def _uuid_row(lo, hi, label):
    row = make_payload_row(2)
    row.append(values.TaggedValue.make_u128(r_uint64(lo), r_uint64(hi)))
    row.append(values.TaggedValue.make_string(label))
    return row


def _cleanup(path):
    if not os.path.exists(path):
        return
    for item in os.listdir(path):
        full = os.path.join(path, item)
        if os.path.isfile(full):
            os.unlink(full)
    os.rmdir(path)


class TestU128NonPKRoundTrip(unittest.TestCase):

    def setUp(self):
        self.db_path = "/tmp/test_u128_payload_%d" % id(self)
        os.makedirs(self.db_path)
        self.schema = _make_schema()

    def tearDown(self):
        _cleanup(self.db_path)

    def test_memtable_basic(self):
        """In-memory path: insert with non-zero high word, get_weight must return 1."""
        db = zset.PersistentTable(self.db_path, "t", self.schema)
        try:
            pk = 1
            uuid_lo = 0xCAFEBABEDEADBEEF
            uuid_hi = 0x0123456789ABCDEF  # non-zero — would read back as 0 pre-fix
            row = _uuid_row(uuid_lo, uuid_hi, "hello")

            db.insert(pk, row)
            self.assertEqual(db.get_weight(pk, row), 1)
        finally:
            db.close()

    def test_high_word_distinguishes_rows(self):
        """
        Two rows with the same PK and same lo-word but different hi-words must
        be treated as distinct records (separate multiset entries).
        Pre-fix both would truncate to the same lo-word and be merged.
        """
        db = zset.PersistentTable(self.db_path, "t", self.schema)
        try:
            pk = 2
            row_a = _uuid_row(0xDEAD, 0xBEEF, "a")
            row_b = _uuid_row(0xDEAD, 0xBEEF + 1, "a")  # only hi differs

            db.insert(pk, row_a)
            db.insert(pk, row_b)

            self.assertEqual(db.get_weight(pk, row_a), 1)
            self.assertEqual(db.get_weight(pk, row_b), 1)
        finally:
            db.close()

    def test_flush_and_shard_read(self):
        """Flush to columnar shard, then verify weight is correct post-flush."""
        db = zset.PersistentTable(self.db_path, "t", self.schema)
        try:
            pk = 3
            uuid_lo = 0xCAFEBABEDEADBEEF
            uuid_hi = 0x0123456789ABCDEF
            row = _uuid_row(uuid_lo, uuid_hi, "world")

            db.insert(pk, row)
            db.flush()

            self.assertEqual(db.get_weight(pk, row), 1)

            # A different uuid at the same PK must still have weight 0
            other = _uuid_row(0xFFFFFFFFFFFFFFFF, 0x1, "world")
            self.assertEqual(db.get_weight(pk, other), 0)
        finally:
            db.close()

    def test_wal_recovery(self):
        """
        Crash-simulation: insert without flushing, close, reopen.
        WAL decode_wal_block must reconstruct the full 128-bit value.
        """
        db_path = self.db_path
        schema = self.schema
        pk = 4
        uuid_lo = 0xCAFEBABEDEADBEEF
        uuid_hi = 0x0123456789ABCDEF
        row = _uuid_row(uuid_lo, uuid_hi, "recover")

        db = zset.PersistentTable(db_path, "t", schema)
        db.insert(pk, row)
        db.close()  # Close without flush — WAL is the only record

        db2 = zset.PersistentTable(db_path, "t", schema)
        try:
            self.assertEqual(db2.get_weight(pk, row), 1)
            # A row with the same lo but zero hi must NOT match
            truncated = _uuid_row(uuid_lo, 0, "recover")
            self.assertEqual(db2.get_weight(pk, truncated), 0)
        finally:
            db2.close()

    def test_annihilation_with_u128_payload(self):
        """Insert then remove: net weight must reach 0 and stay 0 after flush."""
        db = zset.PersistentTable(self.db_path, "t", self.schema)
        try:
            pk = 5
            row = _uuid_row(0xABCDEF0123456789, 0xFEDCBA9876543210, "ghost")

            db.insert(pk, row)
            db.remove(pk, row)
            db.flush()

            self.assertEqual(db.get_weight(pk, row), 0)
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
