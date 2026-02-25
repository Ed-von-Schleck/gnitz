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
    
    # Valid User Identifiers
    valid_names =["orders", "Orders123", "my_table", "a", "A1_b2", "1a", "99_problems"]
    for name in valid_names:
        try:
            identifiers.validate_user_identifier(name)
        except LayoutError:
            os.write(2, "ERR: Valid identifier rejected: " + name + "\n")
            return False
            
    # Invalid User Identifiers
    invalid_names =[
        "_private", "_", "_system", "__init__", "", 
        "has space", "has-dash", "has.dot", "has@", "table$"
    ]
    for name in invalid_names:
        raised = False
        try:
            identifiers.validate_user_identifier(name)
        except LayoutError:
            raised = True
        if not raised:
            os.write(2, "ERR: Invalid identifier accepted: " + name + "\n")
            return False
            
    # Parse Qualified Names
    sc, ent = identifiers.parse_qualified_name("orders", "public")
    if sc != "public" or ent != "orders":
        os.write(2, "ERR: Failed to parse simple name\n")
        return False
        
    sc, ent = identifiers.parse_qualified_name("sales.orders", "public")
    if sc != "sales" or ent != "orders":
        os.write(2, "ERR: Failed to parse qualified name\n")
        return False
        
    os.write(1, "    [OK] Identifiers verified.\n")
    return True

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
            os.write(2, "ERR: Expected 2 schemas, got %d\n" % c)
            return False
        if not engine.registry.has_schema("_system"):
            os.write(2, "ERR: Missing _system schema\n")
            return False
        if not engine.registry.has_schema("public"):
            os.write(2, "ERR: Missing public schema\n")
            return False
            
        # 2. Check Tables (7 system tables expected)
        c = _count_records(engine.sys.tables)
        if c != 7:
            os.write(2, "ERR: Expected 7 system tables, got %d\n" % c)
            return False
            
        # 3. Check Columns (36 bootstrap columns expected)
        c = _count_records(engine.sys.columns)
        if c != 36:
            os.write(2, "ERR: Expected 36 system columns, got %d\n" % c)
            return False
            
        # 4. Check Sequences (2 expected)
        c = _count_records(engine.sys.sequences)
        if c != 2:
            os.write(2, "ERR: Expected 2 sequences, got %d\n" % c)
            return False
            
        # 5. Check Registry State
        if engine.registry._next_table_id != FIRST_USER_TABLE_ID:
            os.write(2, "ERR: Wrong next table ID initialized\n")
            return False
        if engine.registry._next_schema_id != FIRST_USER_SCHEMA_ID:
            os.write(2, "ERR: Wrong next schema ID initialized\n")
            return False
    finally:
        engine.close()
        
    # Idempotent re-open check
    engine2 = open_engine(db_path)
    try:
        if _count_records(engine2.sys.schemas) != 2:
            os.write(2, "ERR: Bootstrap duplicated records on second open\n")
            return False
    finally:
        engine2.close()
        
    os.write(1, "    [OK] Bootstrap verified.\n")
    return True

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
            os.write(2, "ERR: 'sales' schema not found\n")
            return False
        if _count_records(engine.sys.schemas) != init_schemas + 1:
            os.write(2, "ERR: schema count didn't increase\n")
            return False
            
        # Duplicate Schema Rejection
        raised = False
        try:
            engine.create_schema("sales")
        except LayoutError:
            raised = True
        if not raised:
            os.write(2, "ERR: Duplicate schema allowed\n")
            return False
            
        cols =[
            types.ColumnDefinition(types.TYPE_U64, name="id"),
            types.ColumnDefinition(types.TYPE_STRING, name="name")
        ]
        init_tables = _count_records(engine.sys.tables)
        init_cols = _count_records(engine.sys.columns)
        
        # Table Creation
        family = engine.create_table("sales.orders", cols, 0)
        if family.schema_name != "sales" or family.table_name != "orders":
            os.write(2, "ERR: Family name metadata incorrect\n")
            return False
        if not engine.registry.has("sales", "orders"):
            os.write(2, "ERR: Table not found in registry\n")
            return False
        if _count_records(engine.sys.tables) != init_tables + 1:
            os.write(2, "ERR: Table count didn't increase\n")
            return False
        if _count_records(engine.sys.columns) != init_cols + 2:
            os.write(2, "ERR: Columns count didn't increase correctly\n")
            return False
            
        # Drop Table (Check Retractions)
        engine.drop_table("sales.orders")
        if engine.registry.has("sales", "orders"):
            os.write(2, "ERR: Table still in registry after drop\n")
            return False
        if _count_records(engine.sys.tables) != init_tables:
            os.write(2, "ERR: Table count didn't return to baseline\n")
            return False
        if _count_records(engine.sys.columns) != init_cols:
            os.write(2, "ERR: Columns count didn't return to baseline\n")
            return False
            
        # System Table Drop Rejection
        raised = False
        try:
            engine.drop_table("_system._columns")
        except LayoutError:
            raised = True
        if not raised:
            os.write(2, "ERR: Allowed to drop system table\n")
            return False
            
        # Drop Schema
        engine.drop_schema("sales")
        if engine.registry.has_schema("sales"):
            os.write(2, "ERR: Schema still in registry after drop\n")
            return False
        if _count_records(engine.sys.schemas) != init_schemas:
            os.write(2, "ERR: Schema count didn't return to baseline\n")
            return False
    finally:
        engine.close()
        
    os.write(1, "    [OK] DDL verified.\n")
    return True

def test_restart(base_dir):
    os.write(1, "[Catalog] Testing Restart & Persistence...\n")
    db_path = os.path.join(base_dir, "restart")
    if not os.path.exists(db_path):
        os.mkdir(db_path)
        
    engine1 = open_engine(db_path)
    cols =[
        types.ColumnDefinition(types.TYPE_U64, name="id"),
        types.ColumnDefinition(types.TYPE_STRING, name="name")
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
            os.write(2, "ERR: schema 'marketing' lost on restart\n")
            return False
        if not engine2.registry.has("marketing", "products"):
            os.write(2, "ERR: table 'marketing.products' lost on restart\n")
            return False
            
        # Check retraction persistence 
        if engine2.registry.has_schema("trash"):
            os.write(2, "ERR: 'trash' schema reappeared!\n")
            return False
        if engine2.registry.has("trash", "items"):
            os.write(2, "ERR: 'trash.items' table reappeared!\n")
            return False
            
        # Verify schema layout was accurately rebuilt
        table2 = engine2.get_table("marketing.products")
        rebuilt_schema = table2.schema
        if len(rebuilt_schema.columns) != 2:
            os.write(2, "ERR: Rebuilt schema columns count is wrong\n")
            return False
        if rebuilt_schema.columns[1].name != "name":
            os.write(2, "ERR: Rebuilt schema column 1 name is wrong\n")
            return False
        if rebuilt_schema.pk_index != 0:
            os.write(2, "ERR: Rebuilt schema pk_index is wrong\n")
            return False
            
        # Verify user data mapping works correctly
        cursor = table2.create_cursor()
        if not cursor.is_valid():
            os.write(2, "ERR: no data in restarted table\n")
            return False
        
        k = cursor.key()
        if intmask(r_uint64(k)) != 42:
            os.write(2, "ERR: expected key 42\n")
            return False
            
        acc = cursor.get_accessor()
        # col 1 is 'name' in our layout
        py_string = _read_string(acc, 1)
        if py_string != "Gnitz-O-Matic":
            os.write(2, "ERR: expected 'Gnitz-O-Matic', got " + py_string + "\n")
            return False
            
        cursor.close()
        
        # Verify sequence/allocator recovery
        t3 = engine2.create_table("marketing.other", cols, 0)
        if t3.table_id <= id1:
            os.write(2, "ERR: Allocator sequence recovery failed. New ID: %d <= Old ID: %d\n" % (t3.table_id, id1))
            return False
    finally:
        engine2.close()
        
    os.write(1, "    [OK] Restart & Persistence verified.\n")
    return True

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
        if not test_identifiers():
            return 1
        if not test_bootstrap(base_dir):
            return 1
        if not test_ddl(base_dir):
            return 1
        if not test_restart(base_dir):
            return 1

        os.write(1, "\nALL CATALOG TEST PATHS PASSED\n")
        return 0
    except Exception as e:
        os.write(2, "FATAL ERROR: %s\n" % str(e))
        return 1
    finally:
        cleanup_dir(base_dir)

def target(driver, args):
    return entry_point, None

if __name__ == "__main__":
    entry_point(sys.argv)
