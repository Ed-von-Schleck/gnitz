# catalog_comprehensive_test.py

import sys
import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask

from gnitz.core import types, values, batch
from gnitz.core.errors import LayoutError
from gnitz.catalog import identifiers
from gnitz.catalog.engine import open_engine, _read_string
from gnitz.catalog.system_tables import FIRST_USER_TABLE_ID, FIRST_USER_SCHEMA_ID

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def cleanup_dir(path):
    if not os.path.exists(path):
        return
    for item in os.listdir(path):
        p = os.path.join(path, item)
        if os.path.isdir(p):
            cleanup_dir(p)
        else:
            os.unlink(p)
    os.rmdir(path)


def _count_records(table):
    count = 0
    cursor = table.create_cursor()
    while cursor.is_valid():
        count += 1
        cursor.advance()
    cursor.close()
    return count


# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------


def test_identifiers():
    os.write(1, "[Catalog] Testing Identifiers...\n")

    valid_names = ["orders", "Orders123", "my_table", "a", "A1_b2", "1a", "99_problems"]
    for name in valid_names:
        try:
            identifiers.validate_user_identifier(name)
        except LayoutError:
            raise Exception("Valid identifier rejected: " + name)

    invalid_names = [
        "_private",
        "_",
        "_system",
        "__init__",
        "",
        "has space",
        "has-dash",
        "has.dot",
        "has@",
        "table$",
    ]
    for name in invalid_names:
        raised = False
        try:
            identifiers.validate_user_identifier(name)
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Invalid identifier accepted: " + name)

    sc, ent = identifiers.parse_qualified_name("orders", "public")
    if sc != "public" or ent != "orders":
        raise Exception("Failed to parse simple name")

    sc, ent = identifiers.parse_qualified_name("sales.orders", "public")
    if sc != "sales" or ent != "orders":
        raise Exception("Failed to parse qualified name")

    os.write(1, "    [OK] Identifiers verified.\n")


def test_bootstrap(base_dir):
    os.write(1, "[Catalog] Testing Bootstrap...\n")
    db_path = os.path.join(base_dir, "bootstrap")
    if not os.path.exists(db_path):
        os.mkdir(db_path)

    engine = open_engine(db_path)
    try:
        # 1. Check Schemas (_system and public)
        c = _count_records(engine.sys.schemas)
        if c != 2:
            raise Exception("Expected 2 schemas, got %d" % c)
        if not engine.registry.has_schema("_system"):
            raise Exception("Missing _system schema")
        if not engine.registry.has_schema("public"):
            raise Exception("Missing public schema")

        # 2. Check Tables (7 system tables expected)
        c = _count_records(engine.sys.tables)
        if c != 7:
            raise Exception("Expected 7 system tables, got %d" % c)

        # 3. Check Columns (36 bootstrap columns expected)
        c = _count_records(engine.sys.columns)
        if c != 36:
            raise Exception("Expected 36 system columns, got %d" % c)

        # 4. Check Sequences (2 expected)
        c = _count_records(engine.sys.sequences)
        if c != 2:
            raise Exception("Expected 2 sequences, got %d" % c)

        # 5. Check Registry State
        if engine.registry._next_table_id != FIRST_USER_TABLE_ID:
            raise Exception("Wrong next table ID initialized")
        if engine.registry._next_schema_id != FIRST_USER_SCHEMA_ID:
            raise Exception("Wrong next schema ID initialized")
    finally:
        engine.close()

    # Idempotent re-open check
    engine2 = open_engine(db_path)
    try:
        if _count_records(engine2.sys.schemas) != 2:
            raise Exception("Bootstrap duplicated records on second open")
    finally:
        engine2.close()

    os.write(1, "    [OK] Bootstrap verified.\n")


def test_ddl(base_dir):
    os.write(1, "[Catalog] Testing DDL...\n")
    db_path = os.path.join(base_dir, "ddl")
    if not os.path.exists(db_path):
        os.mkdir(db_path)

    engine = open_engine(db_path)
    try:
        init_schemas = _count_records(engine.sys.schemas)

        # Schema Creation
        engine.create_schema("sales")
        if not engine.registry.has_schema("sales"):
            raise Exception("'sales' schema not found")
        if _count_records(engine.sys.schemas) != init_schemas + 1:
            raise Exception("schema count didn't increase")

        # Duplicate Schema Rejection
        raised = False
        try:
            engine.create_schema("sales")
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Duplicate schema allowed")

        cols = [
            types.ColumnDefinition(types.TYPE_U64, name="id"),
            types.ColumnDefinition(types.TYPE_STRING, name="name"),
        ]
        init_tables = _count_records(engine.sys.tables)
        init_cols = _count_records(engine.sys.columns)

        # Table Creation
        family = engine.create_table("sales.orders", cols, 0)
        if family.schema_name != "sales" or family.table_name != "orders":
            raise Exception("Family name metadata incorrect")
        if not engine.registry.has("sales", "orders"):
            raise Exception("Table not found in registry")
        if _count_records(engine.sys.tables) != init_tables + 1:
            raise Exception("Table count didn't increase")
        if _count_records(engine.sys.columns) != init_cols + 2:
            raise Exception("Columns count didn't increase correctly")

        # Drop Table (Check Retractions)
        engine.drop_table("sales.orders")
        if engine.registry.has("sales", "orders"):
            raise Exception("Table still in registry after drop")
        if _count_records(engine.sys.tables) != init_tables:
            raise Exception("Table count didn't return to baseline")
        if _count_records(engine.sys.columns) != init_cols:
            raise Exception("Columns count didn't return to baseline")

        # System Table Drop Rejection
        # This relies on the fix in engine.py: drop_table now validates identifiers
        raised = False
        try:
            engine.drop_table("_system._columns")
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to drop system table (Identifier validation failed)")

        # Drop Schema
        engine.create_schema("temp")
        engine.drop_schema("temp")
        if engine.registry.has_schema("temp"):
            raise Exception("Schema still in registry after drop")
        if _count_records(engine.sys.schemas) != init_schemas + 1:
            raise Exception("Schema count didn't return to baseline")
    finally:
        engine.close()

    os.write(1, "    [OK] DDL verified.\n")


def test_restart(base_dir):
    os.write(1, "[Catalog] Testing Restart & Persistence...\n")
    db_path = os.path.join(base_dir, "restart")
    if not os.path.exists(db_path):
        os.mkdir(db_path)

    engine1 = open_engine(db_path)
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="id"),
        types.ColumnDefinition(types.TYPE_STRING, name="name"),
    ]

    # Track table IDs to verify Allocator ID recovery
    engine1.create_schema("marketing")
    table1 = engine1.create_table("marketing.products", cols, 0)
    id1 = table1.table_id

    # Test dropped entities do not reappear on restart
    engine1.create_schema("trash")
    engine1.create_table("trash.items", cols, 0)
    engine1.drop_table("trash.items")
    engine1.drop_schema("trash")

    # Ingest data into the persistent table
    batch1 = batch.ZSetBatch(table1.schema)
    row = values.make_payload_row(table1.schema)
    row.append_string("Gnitz-O-Matic")
    batch1.append(r_uint128(r_uint64(42)), r_int64(1), row)
    table1.ingest_batch(batch1)
    batch1.free()

    # Flush to ensure table metadata writes are hardened
    table1.flush()
    engine1.close()

    # Restart Engine
    engine2 = open_engine(db_path)
    try:
        # Check standard recovery
        if not engine2.registry.has_schema("marketing"):
            raise Exception("schema 'marketing' lost on restart")
        if not engine2.registry.has("marketing", "products"):
            raise Exception("table 'marketing.products' lost on restart")

        # Check retraction persistence
        if engine2.registry.has_schema("trash"):
            raise Exception("'trash' schema reappeared!")
        if engine2.registry.has("trash", "items"):
            raise Exception("'trash.items' table reappeared!")

        # Verify schema layout was accurately rebuilt
        table2 = engine2.get_table("marketing.products")
        rebuilt_schema = table2.schema
        if len(rebuilt_schema.columns) != 2:
            raise Exception("Rebuilt schema columns count is wrong")
        if rebuilt_schema.columns[1].name != "name":
            raise Exception("Rebuilt schema column 1 name is wrong")
        if rebuilt_schema.pk_index != 0:
            raise Exception("Rebuilt schema pk_index is wrong")

        # Verify user data mapping works correctly
        cursor = table2.create_cursor()
        if not cursor.is_valid():
            raise Exception("no data in restarted table")

        k = cursor.key()
        if intmask(r_uint64(k)) != 42:
            raise Exception("expected key 42")

        acc = cursor.get_accessor()
        # col 1 is 'name' in our layout
        py_string = _read_string(acc, 1)
        if py_string != "Gnitz-O-Matic":
            raise Exception("expected 'Gnitz-O-Matic', got " + py_string)

        cursor.close()

        # Verify sequence/allocator recovery
        t3 = engine2.create_table("marketing.other", cols, 0)
        if t3.table_id <= id1:
            raise Exception(
                "Allocator sequence recovery failed. New ID: %d <= Old ID: %d"
                % (t3.table_id, id1)
            )
    finally:
        engine2.close()

    os.write(1, "    [OK] Restart & Persistence verified.\n")


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------


def entry_point(argv):
    os.write(1, "--- GnitzDB Comprehensive Catalog Test ---\n")
    base_dir = "catalog_test_data"
    if os.path.exists(base_dir):
        cleanup_dir(base_dir)
    os.mkdir(base_dir)

    try:
        test_identifiers()
        test_bootstrap(base_dir)
        test_ddl(base_dir)
        test_restart(base_dir)
        os.write(1, "\nALL CATALOG TEST PATHS PASSED\n")
    finally:
        # cleanup_dir(base_dir)
        pass

    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
