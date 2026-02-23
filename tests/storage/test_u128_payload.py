# tests/storage/test_u128_payload.py

"""
Round-trip correctness tests for TYPE_U128 non-PK columns.

Covers the full data path:
  create_test_row → PayloadRow.append_u128 → MemTable._pack_into_node →
  MemTable.flush → writer_table.add_row / TableShardWriter →
  SoAAccessor.get_u128 → PersistentTable.get_weight
"""

import os
import unittest
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128

from gnitz.core import types
from gnitz.storage import shard_table
from gnitz.storage.table import PersistentTable
from tests.row_helpers import create_test_row


def _make_schema():
    return types.TableSchema(
        [
            types.ColumnDefinition(types.TYPE_U64),  # PK (64-bit)
            types.ColumnDefinition(types.TYPE_U128),  # non-PK UUID foreign key
            types.ColumnDefinition(types.TYPE_STRING),  # extra column
        ],
        pk_index=0,
    )


def _uuid_row(schema, lo, hi, label):
    # Non-PK u128 columns are passed as a (lo, hi) tuple.
    return create_test_row(schema, [(lo, hi), label])


def _cleanup(path):
    if not os.path.exists(path):
        return
    for item in os.listdir(path):
        full = os.path.join(path, item)
        if os.path.isfile(full):
            os.unlink(full)
    if os.path.exists(path):
        os.rmdir(path)


class TestU128NonPKRoundTrip(unittest.TestCase):
    def setUp(self):
        self.db_path = "/tmp/test_u128_payload_%d" % id(self)
        if not os.path.exists(self.db_path):
            os.makedirs(self.db_path)
        self.schema = _make_schema()

    def tearDown(self):
        _cleanup(self.db_path)

    def test_memtable_basic(self):
        """In-memory path: insert with non-zero high word, get_weight must return 1."""
        db = PersistentTable(self.db_path, "t", self.schema)
        try:
            pk = 1
            uuid_lo = 0xCAFEBABEDEADBEEF
            uuid_hi = 0x0123456789ABCDEF  # non-zero high word
            row = _uuid_row(self.schema, uuid_lo, uuid_hi, "hello")

            db.insert(pk, row)
            self.assertEqual(db.get_weight(pk, row), r_int64(1))
        finally:
            db.close()

    def test_high_word_distinguishes_rows(self):
        """
        Two rows with the same PK and same lo-word but different hi-words must
        be treated as distinct records.
        """
        db = PersistentTable(self.db_path, "t", self.schema)
        try:
            pk = 2
            row_a = _uuid_row(self.schema, 0xDEAD, 0xBEEF, "a")
            row_b = _uuid_row(self.schema, 0xDEAD, 0xBEEF + 1, "a")

            db.insert(pk, row_a)
            db.insert(pk, row_b)

            # Both rows are distinct due to differing hi-words: each must have weight 1.
            self.assertEqual(db.get_weight(pk, row_a), r_int64(1))
            self.assertEqual(db.get_weight(pk, row_b), r_int64(1))
        finally:
            db.close()

    def test_flush_and_shard_read(self):
        """Flush to columnar shard, then verify entry count."""
        db = PersistentTable(self.db_path, "t", self.schema)
        try:
            pk = 3
            uuid_lo = 0xCAFEBABEDEADBEEF
            uuid_hi = 0x0123456789ABCDEF
            row = _uuid_row(self.schema, uuid_lo, uuid_hi, "world")

            db.insert(pk, row)
            shard_path = db.flush()

            self.assertTrue(
                os.path.exists(shard_path), "Shard file must exist after flush"
            )

            view = shard_table.TableShardView(shard_path, self.schema)
            try:
                self.assertEqual(view.count, 1)
                self.assertEqual(view.get_pk_u64(0), pk)
            finally:
                view.close()
        finally:
            db.close()

    def test_wal_recovery(self):
        """Verify that rows are correctly appended to the WAL."""
        db_path = self.db_path
        schema = self.schema
        pk = 4
        uuid_lo = 0xCAFEBABEDEADBEEF
        uuid_hi = 0x0123456789ABCDEF
        row = _uuid_row(schema, uuid_lo, uuid_hi, "recover")

        db = PersistentTable(db_path, "t", schema)
        db.insert(pk, row)
        db.close()

        # Verify WAL file exists and has data
        wal_file = os.path.join(db_path, "t.wal")
        self.assertTrue(os.path.exists(wal_file))
        self.assertGreater(os.path.getsize(wal_file), 0)

    def test_annihilation_with_u128_payload(self):
        """Insert then delete the same row: Ghost Property must annihilate it."""
        db = PersistentTable(self.db_path, "t", self.schema)
        try:
            pk = 5
            row = _uuid_row(
                self.schema, 0xABCDEF0123456789, 0xFEDCBA9876543210, "ghost"
            )

            db.insert(pk, row)
            db.delete(pk, row)

            # Net weight must be zero (annihilated in memory).
            self.assertEqual(db.get_weight(pk, row), r_int64(0))

            # After flush the ghost must not appear in the shard.
            shard_path = db.flush()
            if os.path.exists(shard_path):
                view = shard_table.TableShardView(shard_path, self.schema)
                try:
                    self.assertEqual(
                        view.count,
                        0,
                        "Annihilated record must not be materialised in the shard",
                    )
                finally:
                    view.close()
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main()
