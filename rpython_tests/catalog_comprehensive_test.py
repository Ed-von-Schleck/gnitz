# catalog_comprehensive_test.py

import sys
import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rlib import rposix, rposix_stat

from gnitz.core import types, batch
from gnitz.core.batch import RowBuilder
from gnitz.core.types import _analyze_schema
from gnitz.core.errors import LayoutError
from gnitz.core.strings import resolve_string
from gnitz.catalog import identifiers
from gnitz.catalog.engine import open_engine
from gnitz.catalog.system_tables import FIRST_USER_TABLE_ID, FIRST_USER_SCHEMA_ID

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------


def os_path_exists(path):
    try:
        rposix_stat.stat(path)
        return True
    except OSError:
        return False


def cleanup_dir(path):
    if not os_path_exists(path):
        return
    for item in os.listdir(path):
        p = os.path.join(path, item)
        try:
            st = rposix_stat.stat(p)
            import stat

            if stat.S_ISDIR(st.st_mode):
                cleanup_dir(p)
            else:
                os.unlink(p)
        except OSError:
            pass
    try:
        rposix.rmdir(path)
    except OSError:
        pass


def count_records(table):
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

    valid_names = [
        "orders",
        "Orders123",
        "my_table",
        "a",
        "A1_b2",
        "1a",
        "99_problems",
    ]
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
    db_path = base_dir + "/bootstrap"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        # 1. Check Schemas (_system and public)
        c = count_records(engine.sys.schemas)
        if c != 2:
            raise Exception("Expected 2 schemas, got %d" % c)
        if not engine.registry.has_schema("_system"):
            raise Exception("Missing _system schema")
        if not engine.registry.has_schema("public"):
            raise Exception("Missing public schema")

        # 2. Check Tables (12 system tables expected)
        c = count_records(engine.sys.tables)
        if c != 12:
            raise Exception("Expected 12 system tables, got %d" % c)

        # 3. Check Columns (46 bootstrap columns expected)
        c = count_records(engine.sys.columns)
        if c != 46:
            raise Exception("Expected 46 system columns, got %d" % c)

        # 4. Check Sequences (4 expected)
        c = count_records(engine.sys.sequences)
        if c != 4:
            raise Exception("Expected 4 sequences, got %d" % c)

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
        if count_records(engine2.sys.schemas) != 2:
            raise Exception("Bootstrap duplicated records on second open")
    finally:
        engine2.close()

    os.write(1, "    [OK] Bootstrap verified.\n")


def test_ddl(base_dir):
    os.write(1, "[Catalog] Testing DDL...\n")
    db_path = base_dir + "/ddl"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        init_schemas = count_records(engine.sys.schemas)

        # Schema Creation
        engine.create_schema("sales")
        if not engine.registry.has_schema("sales"):
            raise Exception("'sales' schema not found")
        if count_records(engine.sys.schemas) != init_schemas + 1:
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
        init_tables = count_records(engine.sys.tables)
        init_cols = count_records(engine.sys.columns)

        # Table Creation
        family = engine.create_table("sales.orders", cols, 0)
        if family.schema_name != "sales" or family.table_name != "orders":
            raise Exception("Family name metadata incorrect")
        if not engine.registry.has("sales", "orders"):
            raise Exception("Table not found in registry")
        if count_records(engine.sys.tables) != init_tables + 1:
            raise Exception("Table count didn't increase")
        if count_records(engine.sys.columns) != init_cols + 2:
            raise Exception("Columns count didn't increase correctly")

        # Drop Table (Check Retractions)
        engine.drop_table("sales.orders")
        if engine.registry.has("sales", "orders"):
            raise Exception("Table still in registry after drop")
        if count_records(engine.sys.tables) != init_tables:
            raise Exception("Table count didn't return to baseline")
        if count_records(engine.sys.columns) != init_cols:
            raise Exception("Columns count didn't return to baseline")

        # System Table Drop Rejection
        raised = False
        try:
            engine.drop_table("_system._columns")
        except LayoutError:
            raised = True
        if not raised:
            raise Exception(
                "Allowed to drop system table (Identifier validation failed)"
            )

        # Drop Schema
        engine.create_schema("temp")
        engine.drop_schema("temp")
        if engine.registry.has_schema("temp"):
            raise Exception("Schema still in registry after drop")
        if count_records(engine.sys.schemas) != init_schemas + 1:
            raise Exception("Schema count didn't return to baseline")
    finally:
        engine.close()

    os.write(1, "    [OK] DDL verified.\n")


def test_edge_cases(base_dir):
    os.write(1, "[Catalog] Testing Edge Cases...\n")
    db_path = base_dir + "/edge_cases"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        cols = [
            types.ColumnDefinition(types.TYPE_U64, name="id"),
        ]

        # 1. Dropping a non-existent schema
        raised = False
        try:
            engine.drop_schema("nonexistent_schema")
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to drop non-existent schema")

        # 2. Creating a table in a non-existent schema
        raised = False
        try:
            engine.create_table("nonexistent_schema.tbl", cols, 0)
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to create table in non-existent schema")

        # 3. Dropping a non-existent table
        raised = False
        try:
            engine.drop_table("public.nonexistent_table")
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to drop non-existent table")

        # 4. Creating a table that already exists
        engine.create_table("public.tbl1", cols, 0)
        raised = False
        try:
            engine.create_table("public.tbl1", cols, 0)
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to create duplicate table")

        # 5. Dropping a non-empty schema
        engine.create_schema("my_schema")
        engine.create_table("my_schema.tbl2", cols, 0)
        raised = False
        try:
            engine.drop_schema("my_schema")
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to drop non-empty schema")

        # 6. Drop the table, then drop the schema should succeed
        engine.drop_table("my_schema.tbl2")
        engine.drop_schema("my_schema")
        if engine.registry.has_schema("my_schema"):
            raise Exception("Schema should have been dropped")

        # 7. Unqualified table name defaults to public
        tbl3 = engine.create_table("tbl3", cols, 0)
        if tbl3.schema_name != "public":
            raise Exception("Unqualified table did not default to public")
        engine.drop_table("public.tbl3")

        # 8. Unqualified drop table
        engine.create_table("public.tbl4", cols, 0)
        engine.drop_table("tbl4")
        if engine.registry.has("public", "tbl4"):
            raise Exception("Unqualified drop table failed to drop from public")

        # 9. Invalid PK type
        invalid_cols = [
            types.ColumnDefinition(types.TYPE_STRING, name="id"),
        ]
        raised = False
        try:
            engine.create_table("public.invalid_pk", invalid_cols, 0)
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to create table with non-U64/U128 PK")

        # 10. Too many columns (> 64)
        many_cols = []
        for i in range(65):
            many_cols.append(
                types.ColumnDefinition(types.TYPE_U64, name="c" + str(i))
            )
        raised = False
        try:
            engine.create_table("public.too_many_cols", many_cols, 0)
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to create table with > 64 columns")

        # 11. Dropping system schema
        raised = False
        try:
            engine.drop_schema("_system")
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to drop system schema")

        # 12. Validating PK col index out of bounds
        raised = False
        try:
            engine.create_table("public.bad_pk_idx", cols, 5)
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to create table with out-of-bounds pk_col_idx")

        # 13. Recreated schema gets new ID
        engine.create_schema("temp")
        sid1 = engine.registry.get_schema_id("temp")
        engine.drop_schema("temp")
        engine.create_schema("temp")
        sid2 = engine.registry.get_schema_id("temp")
        if sid1 == sid2:
            raise Exception("Schema ID was reused after drop and recreate")
        engine.drop_schema("temp")

        # 14. Recreated table gets new ID
        family1 = engine.create_table("public.tbl1_recreate", cols, 0)
        tid1 = family1.table_id
        engine.drop_table("public.tbl1_recreate")
        family2 = engine.create_table("public.tbl1_recreate", cols, 0)
        if family2.table_id == tid1:
            raise Exception("Table ID was reused after drop and recreate")
        engine.drop_table("public.tbl1_recreate")

        # 15. U128 PK support
        cols_u128 = [
            types.ColumnDefinition(types.TYPE_U128, name="uuid_pk"),
            types.ColumnDefinition(types.TYPE_STRING, name="data"),
        ]
        u128_table = engine.create_table("public.u128_tbl", cols_u128, 0)
        if u128_table.schema.columns[0].field_type.code != types.TYPE_U128.code:
            raise Exception("Failed to create u128 table correctly")
        engine.drop_table("public.u128_tbl")

        # 16. Multiple dots in qualified name
        raised = False
        try:
            engine.create_table("public.schema.tbl", cols, 0)
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to create table with multiple dots")

        # 17. get_table raises LayoutError on nonexistent
        raised = False
        try:
            engine.get_table("public.nonexistent")
        except LayoutError:
            raised = True
        if not raised:
            raise Exception(
                "get_table on non-existent table should raise LayoutError"
            )

        # 18. schema_is_empty logic
        engine.create_schema("empty_test")
        if not engine.registry.schema_is_empty("empty_test"):
            raise Exception("Newly created schema should be empty")
        engine.create_table("empty_test.tbl", cols, 0)
        if engine.registry.schema_is_empty("empty_test"):
            raise Exception("Schema with table should not be empty")
        engine.drop_table("empty_test.tbl")
        if not engine.registry.schema_is_empty("empty_test"):
            raise Exception("Schema should be empty after dropping all tables")
        engine.drop_schema("empty_test")

        # 19. Case sensitivity of tables
        engine.create_table("public.CaseTest", cols, 0)
        engine.create_table("public.casetest", cols, 0)
        if not engine.registry.has(
            "public", "CaseTest"
        ) or not engine.registry.has("public", "casetest"):
            raise Exception("Case sensitivity failed")
        engine.drop_table("public.CaseTest")
        engine.drop_table("public.casetest")

        # 20. Registry ID methods
        t1 = engine.create_table("public.reg_test", cols, 0)
        tid = t1.table_id
        if not engine.registry.has_id(tid):
            raise Exception("registry has_id failed")
        if engine.registry.get_by_id(tid).table_name != "reg_test":
            raise Exception("registry get_by_id failed")

        raised = False
        try:
            engine.registry.get_by_id(999999)
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("get_by_id on invalid ID should raise LayoutError")

        engine.drop_table("public.reg_test")

        # 21. Table with U128 payload and Nullable cols
        cols_payload_u128 = [
            types.ColumnDefinition(types.TYPE_U64, name="id"),
            types.ColumnDefinition(types.TYPE_U128, name="uuid_payload"),
            types.ColumnDefinition(
                types.TYPE_STRING, is_nullable=True, name="nullable_str"
            ),
        ]
        u128_payload_table = engine.create_table(
            "public.u128_payload_tbl", cols_payload_u128, 0
        )
        if (
            u128_payload_table.schema.columns[1].field_type.code
            != types.TYPE_U128.code
        ):
            raise Exception("Failed to create u128 payload table correctly")
        if not u128_payload_table.schema.columns[2].is_nullable:
            raise Exception("Failed to create nullable column correctly")
        engine.drop_table("public.u128_payload_tbl")

        # 22. Schema padding / stride logic
        cols_padding = [
            types.ColumnDefinition(types.TYPE_U64, name="id"),
            types.ColumnDefinition(types.TYPE_U8, name="tiny"),
            types.ColumnDefinition(types.TYPE_U32, name="medium"),
        ]
        pad_tbl = engine.create_table("public.pad_tbl", cols_padding, 0)
        if pad_tbl.schema.memtable_stride != 8:
            raise Exception("Incorrect memtable_stride (expected 8)")
        engine.drop_table("public.pad_tbl")

        # 23. Test schema analysis flags
        n, has_u128, has_string, has_nullable = _analyze_schema(pad_tbl.schema)
        if has_u128 or has_string or has_nullable:
            raise Exception("Incorrect flags for pad_tbl")

        n, has_u128, has_string, has_nullable = _analyze_schema(
            u128_payload_table.schema
        )
        if not has_u128 or not has_string or not has_nullable:
            raise Exception("Incorrect flags for u128_payload_tbl")

        # 24. Invalid schema ID lookup
        name = engine.registry.get_schema_name(999999)
        if name != "":
            raise Exception(
                "get_schema_name for invalid ID should return empty string"
            )

        sid = engine.registry.get_schema_id("nonexistent")
        if sid != -1:
            raise Exception(
                "get_schema_id for invalid name should return -1"
            )

        # 25. Unregister non-existent
        engine.registry.unregister("public", "never_existed")

        # 26. Adding table to _system schema
        raised = False
        try:
            engine.create_table("_system.new_tbl", cols, 0)
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to create user table in _system schema")

    finally:
        engine.close()

    os.write(1, "    [OK] Edge Cases verified.\n")


def test_unique_pk_metadata(base_dir):
    os.write(1, "[Catalog] Testing unique_pk metadata...\n")
    db_path = base_dir + "/unique_pk"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="id"),
        types.ColumnDefinition(types.TYPE_STRING, name="val"),
    ]

    # Sub-test 1: default is True
    engine = open_engine(db_path)
    try:
        engine.create_schema("sales")
        family = engine.create_table("sales.u_default", cols, 0)
        if not family.unique_pk:
            raise Exception("unique_pk should default to True")
        os.write(1, "    [OK] default unique_pk=True\n")

        # Sub-test 2: explicit False
        family2 = engine.create_table("sales.u_off", cols, 0, unique_pk=False)
        if family2.unique_pk:
            raise Exception("unique_pk should be False when explicitly set")
        os.write(1, "    [OK] explicit unique_pk=False\n")

        # Sub-test 3: survives restart
        engine.create_table("sales.u_restart", cols, 0)
    finally:
        engine.close()

    engine2 = open_engine(db_path)
    try:
        family3 = engine2.registry.get("sales", "u_restart")
        if not family3.unique_pk:
            raise Exception("unique_pk=True did not survive restart")

        family4 = engine2.registry.get("sales", "u_off")
        if family4.unique_pk:
            raise Exception("unique_pk=False did not survive restart")
        os.write(1, "    [OK] unique_pk survives restart\n")
    finally:
        engine2.close()

    os.write(1, "    [OK] unique_pk metadata verified.\n")


def _make_upk_row(schema, pk, val, w):
    """Build a single-row ZSetBatch for the (U64 id, I64 val) schema."""
    from gnitz.core.batch import ArenaZSetBatch, RowBuilder
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    rb.begin(r_uint128(r_uint64(pk)), r_int64(w))
    rb.put_int(r_int64(val))
    rb.commit()
    return b


def test_enforce_unique_pk(base_dir):
    os.write(1, "[Catalog] Testing _enforce_unique_pk...\n")
    db_path = base_dir + "/enforce_unique_pk"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    from gnitz.catalog.registry import _enforce_unique_pk
    from gnitz.core.batch import ArenaZSetBatch, RowBuilder

    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="id"),
        types.ColumnDefinition(types.TYPE_I64, name="val"),
    ]

    engine = open_engine(db_path)
    try:
        engine.create_schema("utest")
        family = engine.create_table("utest.t", cols, 0)
        schema = family.schema

        # Sub-test 1: insert new PK (no stored row) → 1 row in output
        b1 = _make_upk_row(schema, 1, 10, 1)
        out1 = _enforce_unique_pk(family, b1)
        if out1.length() != 1:
            raise Exception("ST1: expected 1 row, got %d" % out1.length())
        if out1.get_weight(0) != r_int64(1):
            raise Exception("ST1: expected w=+1")
        out1.free()
        b1.free()
        os.write(1, "    [OK] ST1: insert new PK\n")

        # Sub-test 2: update existing PK → retraction + new row
        b2a = _make_upk_row(schema, 2, 10, 1)
        family.store.ingest_batch(b2a)
        family.store.flush()
        b2a.free()

        b2b = _make_upk_row(schema, 2, 20, 1)
        out2 = _enforce_unique_pk(family, b2b)
        if out2.length() != 2:
            raise Exception("ST2: expected 2 rows, got %d" % out2.length())
        neg_count = 0
        pos_count = 0
        for i in range(out2.length()):
            w = out2.get_weight(i)
            if w < r_int64(0):
                neg_count += 1
            else:
                pos_count += 1
        if neg_count != 1 or pos_count != 1:
            raise Exception("ST2: expected 1 retraction + 1 insertion")
        out2.free()
        b2b.free()
        os.write(1, "    [OK] ST2: update existing PK\n")

        # Sub-test 3: delete-by-PK existing → retraction with stored payload
        b3a = _make_upk_row(schema, 3, 30, 1)
        family.store.ingest_batch(b3a)
        family.store.flush()
        b3a.free()

        b3b = _make_upk_row(schema, 3, 0, -1)
        out3 = _enforce_unique_pk(family, b3b)
        if out3.length() != 1:
            raise Exception("ST3: expected 1 retraction, got %d" % out3.length())
        if out3.get_weight(0) != r_int64(-1):
            raise Exception("ST3: expected w=-1")
        out3.free()
        b3b.free()
        os.write(1, "    [OK] ST3: delete-by-PK existing\n")

        # Sub-test 4: delete-by-PK absent → empty output, no error
        b4 = _make_upk_row(schema, 999, 0, -1)
        out4 = _enforce_unique_pk(family, b4)
        if out4.length() != 0:
            raise Exception("ST4: expected empty output, got %d" % out4.length())
        out4.free()
        b4.free()
        os.write(1, "    [OK] ST4: delete-by-PK absent\n")

        # Sub-test 5: intra-batch duplicate insert → last value wins
        b5 = ArenaZSetBatch(schema)
        rb5 = RowBuilder(schema, b5)
        rb5.begin(r_uint128(r_uint64(5)), r_int64(1))
        rb5.put_int(r_int64(10))
        rb5.commit()
        rb5.begin(r_uint128(r_uint64(5)), r_int64(1))
        rb5.put_int(r_int64(20))
        rb5.commit()
        out5 = _enforce_unique_pk(family, b5)
        # 3 rows: (+1,v=10), (-1,v=10), (+1,v=20) — net: (PK=5,v=20)
        if out5.length() != 3:
            raise Exception("ST5: expected 3 rows, got %d" % out5.length())
        pos5 = 0
        neg5 = 0
        for i in range(out5.length()):
            w = out5.get_weight(i)
            if w > r_int64(0):
                pos5 += 1
            else:
                neg5 += 1
        if pos5 != 2 or neg5 != 1:
            raise Exception("ST5: expected 2 pos + 1 neg, got %d pos %d neg" % (pos5, neg5))
        out5.free()
        b5.free()
        os.write(1, "    [OK] ST5: intra-batch duplicate insert\n")

        # Sub-test 6: intra-batch insert then delete → cancel each other
        b6 = ArenaZSetBatch(schema)
        rb6 = RowBuilder(schema, b6)
        rb6.begin(r_uint128(r_uint64(6)), r_int64(1))
        rb6.put_int(r_int64(10))
        rb6.commit()
        rb6.begin(r_uint128(r_uint64(6)), r_int64(-1))
        rb6.put_int(r_int64(10))
        rb6.commit()
        out6 = _enforce_unique_pk(family, b6)
        # 2 rows: (+1,v=10) original + (-1,v=10) cancellation — net weight = 0
        if out6.length() != 2:
            raise Exception("ST6: expected 2 rows, got %d" % out6.length())
        sum_w6 = r_int64(0)
        for i in range(out6.length()):
            sum_w6 += out6.get_weight(i)
        if sum_w6 != r_int64(0):
            raise Exception("ST6: expected net weight 0")
        out6.free()
        b6.free()
        os.write(1, "    [OK] ST6: intra-batch insert then delete\n")

    finally:
        engine.close()

    os.write(1, "    [OK] _enforce_unique_pk verified.\n")


def test_restart(base_dir):
    os.write(1, "[Catalog] Testing Restart & Persistence...\n")
    db_path = base_dir + "/restart"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

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
    batch1 = batch.ArenaZSetBatch(table1.schema)
    rb = RowBuilder(table1.schema, batch1)
    rb.begin(r_uint128(r_uint64(42)), r_int64(1))
    rb.put_string("Gnitz-O-Matic")
    rb.commit()
    table1.store.ingest_batch(batch1)
    batch1.free()

    # Flush to ensure table metadata writes are hardened
    table1.store.flush()
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
        cursor = table2.store.create_cursor()
        if not cursor.is_valid():
            raise Exception("no data in restarted table")

        k = cursor.key()
        if intmask(r_uint64(k)) != 42:
            raise Exception("expected key 42")

        acc = cursor.get_accessor()
        # col 1 is 'name' in our layout
        l, pref, s_ptr, h_ptr, s_obj = acc.get_str_struct(1)
        py_string = resolve_string(s_ptr, h_ptr, s_obj)
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
    cleanup_dir(base_dir)
    if not os_path_exists(base_dir):
        rposix.mkdir(base_dir, 0o755)

    try:
        test_identifiers()
        test_bootstrap(base_dir)
        test_ddl(base_dir)
        test_unique_pk_metadata(base_dir)
        test_edge_cases(base_dir)
        test_enforce_unique_pk(base_dir)
        test_restart(base_dir)
        os.write(1, "\nALL CATALOG TEST PATHS PASSED\n")
    except Exception as e:
        os.write(2, "TEST FAILED: " + str(e) + "\n")
        return 1

    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
