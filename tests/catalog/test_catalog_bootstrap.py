# tests/catalog/test_catalog_bootstrap.py

import unittest
import shutil
import tempfile
import os
from rpython.rlib.rarithmetic import r_uint64

from gnitz.catalog.engine import open_engine
from gnitz.catalog.system_tables import (
    SYSTEM_SCHEMA_ID,
    PUBLIC_SCHEMA_ID,
    SYS_TABLE_SCHEMAS,
    SYS_TABLE_TABLES,
    SYS_TABLE_COLUMNS,
    SYS_TABLE_SEQUENCES,
    FIRST_USER_TABLE_ID,
    FIRST_USER_SCHEMA_ID,
)


class TestCatalogBootstrap(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    def _count_records(self, table):
        """Helper to count records using a cursor."""
        count = 0
        cursor = table.create_cursor()
        while cursor.is_valid():
            count += 1
            cursor.advance()
        cursor.close()
        return count

    def test_first_startup_creates_metadata(self):
        """Verifies that the first startup populates all system tables."""
        engine = open_engine(self.tmpdir)
        try:
            # 1. Check Schemas (_system and public)
            self.assertEqual(self._count_records(engine.sys.schemas), 2)
            self.assertTrue(engine.registry.has_schema("_system"))
            self.assertTrue(engine.registry.has_schema("public"))

            # 2. Check Tables (7 system tables: schemas, tables, views, columns, indices, view_deps, sequences)
            self.assertEqual(self._count_records(engine.sys.tables), 7)
            
            # 3. Check Columns
            # _schemas(2) + _tables(6) + _views(6) + _columns(9) + _indices(7) + _view_deps(4) + _sequences(2) = 36
            self.assertEqual(self._count_records(engine.sys.columns), 36)

            # 4. Check Sequences
            self.assertEqual(self._count_records(engine.sys.sequences), 2)
            
            # 5. Check Registry State
            self.assertEqual(engine.registry._next_table_id, FIRST_USER_TABLE_ID)
            self.assertEqual(engine.registry._next_schema_id, FIRST_USER_SCHEMA_ID)
        finally:
            engine.close()

    def test_restart_rebuilds_registry(self):
        """Verifies that state is recovered from disk without re-bootstrapping."""
        # First pass: create something
        engine = open_engine(self.tmpdir)
        engine.create_schema("marketing")
        engine.close()

        # Second pass: reopen
        engine2 = open_engine(self.tmpdir)
        try:
            # Registry should have the user schema
            self.assertTrue(engine2.registry.has_schema("marketing"))
            self.assertEqual(engine2.registry.get_schema_id("marketing"), FIRST_USER_SCHEMA_ID)
            
            # Sequence counters should be preserved
            self.assertEqual(engine2.registry._next_schema_id, FIRST_USER_SCHEMA_ID + 1)
            
            # System tables should still only have the bootstrap records (plus our 1 new schema)
            self.assertEqual(self._count_records(engine2.sys.schemas), 3)
        finally:
            engine2.close()

    def test_idempotent_bootstrap(self):
        """Verifies that opening an engine multiple times doesn't duplicate system records."""
        engine = open_engine(self.tmpdir)
        engine.close()
        
        engine2 = open_engine(self.tmpdir)
        try:
            self.assertEqual(self._count_records(engine2.sys.schemas), 2)
            self.assertEqual(self._count_records(engine2.sys.tables), 7)
        finally:
            engine2.close()

    def test_system_table_access_via_registry(self):
        """Verifies that system tables are registered in the EntityRegistry correctly."""
        engine = open_engine(self.tmpdir)
        try:
            # Should be able to look up _system._columns
            family = engine.registry.get("_system", "_columns")
            self.assertEqual(family.table_id, SYS_TABLE_COLUMNS)
            self.assertEqual(family.schema_name, "_system")
            
            # The primary table should be the same instance as engine.sys.columns
            self.assertEqual(family.primary, engine.sys.columns)
        finally:
            engine.close()
