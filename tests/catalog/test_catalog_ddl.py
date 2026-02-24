# tests/catalog/test_catalog_ddl.py

import unittest
import shutil
import tempfile
import os
from rpython.rlib.rarithmetic import r_int64

from gnitz.catalog.engine import open_engine
from gnitz.core.types import ColumnDefinition, TYPE_U64, TYPE_STRING
from gnitz.core.errors import LayoutError


class TestCatalogDDL(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.engine = open_engine(self.tmpdir)

    def tearDown(self):
        self.engine.close()
        shutil.rmtree(self.tmpdir)

    def _count_records(self, table):
        count = 0
        cursor = table.create_cursor()
        while cursor.is_valid():
            count += 1
            cursor.advance()
        cursor.close()
        return count

    def _get_test_columns(self):
        cols = [
            ColumnDefinition(TYPE_U64, name="id"),
            ColumnDefinition(TYPE_STRING, name="name"),
        ]
        return cols

    # ── Schema DDL ───────────────────────────────────────────────────────────

    def test_create_schema_success(self):
        """Verifies that a valid schema is registered and persisted."""
        initial_count = self._count_records(self.engine.sys.schemas)
        self.engine.create_schema("sales")
        
        self.assertTrue(self.registry.has_schema("sales"))
        self.assertEqual(
            self._count_records(self.engine.sys.schemas), initial_count + 1
        )

    def test_create_schema_duplicate(self):
        """Verifies that duplicate schemas are rejected."""
        self.engine.create_schema("sales")
        with self.assertRaises(LayoutError) as cm:
            self.engine.create_schema("sales")
        self.assertIn("already exists", str(cm.exception))

    def test_create_schema_invalid_name(self):
        """Verifies that invalid identifiers (e.g. system reserved) are rejected."""
        with self.assertRaises(LayoutError):
            self.engine.create_schema("_private")

    def test_drop_schema_success(self):
        """Verifies that a schema can be dropped, emitting a Z-Set retraction."""
        self.engine.create_schema("sales")
        initial_count = self._count_records(self.engine.sys.schemas)
        
        self.engine.drop_schema("sales")
        
        self.assertFalse(self.registry.has_schema("sales"))
        # In Phase A, Z-Set retractions annihilate data upon compaction or scan.
        # Record count should be back to initial (2).
        self.assertEqual(self._count_records(self.engine.sys.schemas), initial_count - 1)

    def test_drop_schema_nonempty_fails(self):
        """Verifies that a schema containing tables cannot be dropped."""
        self.engine.create_schema("sales")
        self.engine.create_table("sales.orders", self._get_test_columns(), 0)
        
        with self.assertRaises(LayoutError) as cm:
            self.engine.drop_schema("sales")
        self.assertIn("non-empty", str(cm.exception))

    # ── Table DDL ────────────────────────────────────────────────────────────

    def test_create_table_public_success(self):
        """Verifies creation of a table in the default 'public' schema."""
        cols = self._get_test_columns()
        family = self.engine.create_table("orders", cols, 0)
        
        self.assertEqual(family.schema_name, "public")
        self.assertEqual(family.table_name, "orders")
        self.assertTrue(self.registry.has("public", "orders"))
        
        # Verify physical directory was created
        self.assertTrue(os.path.isdir(family.directory))

    def test_create_table_explicit_schema(self):
        """Verifies creation of a table in a custom schema."""
        self.engine.create_schema("sales")
        cols = self._get_test_columns()
        family = self.engine.create_table("sales.orders", cols, 0)
        
        self.assertEqual(family.schema_name, "sales")
        self.assertTrue(self.registry.has("sales", "orders"))

    def test_create_table_metadata_verification(self):
        """Checks that _tables and _columns contain the new records."""
        initial_table_count = self._count_records(self.engine.sys.tables)
        initial_col_count = self._count_records(self.engine.sys.columns)
        
        cols = self._get_test_columns()
        self.engine.create_table("orders", cols, 0)
        
        # _tables grew by 1
        self.assertEqual(
            self._count_records(self.engine.sys.tables), initial_table_count + 1
        )
        # _columns grew by 2 (id, name)
        self.assertEqual(
            self._count_records(self.engine.sys.columns), initial_col_count + 2
        )

    def test_create_table_duplicate_fails(self):
        """Verifies that duplicate tables in the same schema are rejected."""
        cols = self._get_test_columns()
        self.engine.create_table("orders", cols, 0)
        with self.assertRaises(LayoutError):
            self.engine.create_table("orders", cols, 0)

    def test_create_table_missing_schema_fails(self):
        """Verifies that creating a table in a non-existent schema fails."""
        with self.assertRaises(LayoutError) as cm:
            self.engine.create_table("nosuchschema.orders", self._get_test_columns(), 0)
        self.assertIn("Schema does not exist", str(cm.exception))

    def test_drop_table_success(self):
        """Verifies that dropping a table cleans up the registry and system tables."""
        cols = self._get_test_columns()
        self.engine.create_table("orders", cols, 0)
        
        table_initial = self._count_records(self.engine.sys.tables)
        col_initial = self._count_records(self.engine.sys.columns)
        
        self.engine.drop_table("orders")
        
        self.assertFalse(self.registry.has("public", "orders"))
        # Check system table retractions
        self.assertEqual(self._count_records(self.engine.sys.tables), table_initial - 1)
        self.assertEqual(self._count_records(self.engine.sys.columns), col_initial - 2)

    def test_drop_system_table_rejected(self):
        """Verifies that users cannot drop system tables."""
        # _system schema starts with _, so parse_qualified_name + validate_user_identifier rejects it
        with self.assertRaises(LayoutError):
            self.engine.drop_table("_system._columns")

    @property
    def registry(self):
        return self.engine.registry
