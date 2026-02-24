# tests/catalog/test_catalog_restart.py

import unittest
import shutil
import tempfile
import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128

from gnitz.catalog.engine import open_engine
from gnitz.core.types import ColumnDefinition, TYPE_U64, TYPE_STRING
from gnitz.core.batch import ZSetBatch
from gnitz.core.values import make_payload_row


class TestCatalogRestart(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        if os.path.exists(self.tmpdir):
            shutil.rmtree(self.tmpdir)

    def _get_test_columns(self):
        return [
            ColumnDefinition(TYPE_U64, name="id"),
            ColumnDefinition(TYPE_STRING, name="name"),
        ]

    def test_table_metadata_survives_restart(self):
        """Verifies that table definitions are correctly rebuilt from _system._columns."""
        # 1. Setup: Create a table with specific columns
        engine = open_engine(self.tmpdir)
        cols = self._get_test_columns()
        engine.create_table("orders", cols, 0)
        engine.close()

        # 2. Restart: Re-open and verify
        engine2 = open_engine(self.tmpdir)
        try:
            self.assertTrue(engine2.registry.has("public", "orders"))
            family = engine2.get_table("orders")
            
            # Verify schema reconstruction
            rebuilt_schema = family.schema
            self.assertEqual(len(rebuilt_schema.columns), 2)
            self.assertEqual(rebuilt_schema.columns[0].name, "id")
            self.assertEqual(rebuilt_schema.columns[1].name, "name")
            self.assertEqual(rebuilt_schema.pk_index, 0)
        finally:
            engine2.close()

    def test_data_persistence_across_restart(self):
        """Verifies that actual user data remains accessible after a catalog rebuild."""
        engine = open_engine(self.tmpdir)
        cols = self._get_test_columns()
        table = engine.create_table("products", cols, 0)
        
        # Ingest a record
        batch = ZSetBatch(table.schema)
        row = make_payload_row(table.schema)
        row.append_string("Gnitz-O-Matic")
        batch.append(r_uint128(r_uint64(42)), r_int64(1), row)
        table.ingest_batch(batch)
        batch.free()
        
        # Flush to shard to test shard-opening path during restart
        table.flush()
        engine.close()

        # Restart
        engine2 = open_engine(self.tmpdir)
        try:
            table2 = engine2.get_table("products")
            cursor = table2.create_cursor()
            
            self.assertTrue(cursor.is_valid())
            self.assertEqual(intmask(r_uint64(cursor.key())), 42)
            
            acc = cursor.get_accessor()
            # col 1 is name (payload col 0 since pk_index=0)
            length, _, _, _, py_string = acc.get_str_struct(1)
            self.assertEqual(py_string, "Gnitz-O-Matic")
            
            cursor.close()
        finally:
            engine2.close()

    def test_dropped_entities_do_not_reappear(self):
        """Verifies that Z-Set retractions in system tables are honored on restart."""
        engine = open_engine(self.tmpdir)
        engine.create_schema("trash")
        engine.create_table("trash.items", self._get_test_columns(), 0)
        
        # Drop them
        engine.drop_table("trash.items")
        engine.drop_schema("trash")
        engine.close()

        # Restart
        engine2 = open_engine(self.tmpdir)
        try:
            self.assertFalse(engine2.registry.has_schema("trash"))
            self.assertFalse(engine2.registry.has("trash", "items"))
        finally:
            engine2.close()

    def test_allocator_state_recovery(self):
        """Verifies that table and schema ID counters continue correctly after restart."""
        engine = open_engine(self.tmpdir)
        t1 = engine.create_table("t1", self._get_test_columns(), 0)
        id1 = t1.table_id
        engine.close()

        engine2 = open_engine(self.tmpdir)
        try:
            t2 = engine2.create_table("t2", self._get_test_columns(), 0)
            # Should be next in sequence, no collision with id1
            self.assertEqual(t2.table_id, id1 + 1)
        finally:
            engine2.close()

    def test_complex_hierarchy_restart(self):
        """Verifies a multi-schema, multi-table layout survives restart."""
        engine = open_engine(self.tmpdir)
        engine.create_schema("hr")
        engine.create_schema("fin")
        engine.create_table("hr.employees", self._get_test_columns(), 0)
        engine.create_table("fin.ledgers", self._get_test_columns(), 0)
        engine.close()

        engine2 = open_engine(self.tmpdir)
        try:
            self.assertTrue(engine2.registry.has("hr", "employees"))
            self.assertTrue(engine2.registry.has("fin", "ledgers"))
            self.assertTrue(engine2.registry.has_schema("hr"))
            self.assertTrue(engine2.registry.has_schema("fin"))
        finally:
            engine2.close()
