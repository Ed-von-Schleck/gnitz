# catalog_comprehensive_test.py

import sys
import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import rposix, rposix_stat
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core import types
core_types = types
from gnitz.catalog.system_tables import RowBuilder
from gnitz.storage import owned_batch
from gnitz.core.types import _analyze_schema
from gnitz.core.errors import LayoutError, GnitzError
from gnitz.core.strings import resolve_string
from gnitz.catalog import identifiers
from gnitz.catalog.engine import open_engine
from gnitz.catalog import engine as engine_module
from gnitz.catalog import system_tables as sys
from gnitz.catalog.registry import ingest_to_family, validate_fk_inline
from gnitz.catalog.system_tables import (
    FIRST_USER_TABLE_ID,
    FIRST_USER_SCHEMA_ID,
    OWNER_KIND_TABLE,
    pack_column_id,
    IdxTab,
    ColTab,
)
from gnitz.catalog.index_circuit import (
    IndexCircuit,
    get_index_key_type,
    make_index_schema,
    make_fk_index_name,
    make_secondary_index_name,
)
from gnitz.catalog.metadata import ensure_dir
from gnitz.storage.ephemeral_table import EphemeralTable
from gnitz.server.executor import evaluate_dag
from rpython_tests.helpers.circuit_builder import CircuitBuilder
from rpython_tests.helpers.jit_stub import ensure_jit_reachable
from rpython_tests.helpers.assertions import (
    fail, assert_true, assert_equal_i, assert_equal_u128,
)

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


_count_records = count_records


def log(msg):
    os.write(1, "[TEST] " + msg + "\n")


def log_step(name):
    os.write(1, "\n[CHECKPOINT] " + name + "...\n")


def _make_passthrough_view(engine, view_name, source_family):
    """Build a passthrough (SELECT *) view over source_family."""
    schema = source_family.schema
    out_cols = []
    for col in schema.columns:
        out_cols.append((col.name, col.field_type.code))
    builder = CircuitBuilder(view_id=0, primary_source_id=source_family.table_id)
    src = builder.input_delta()
    builder.sink(src, target_table_id=0)
    graph = builder.build(out_cols)
    return engine.create_view(view_name, graph, "")


# ------------------------------------------------------------------------------
# Tests (original catalog)
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
    from gnitz.storage.owned_batch import ArenaZSetBatch
    from gnitz.catalog.system_tables import RowBuilder
    b = ArenaZSetBatch(schema)
    rb = RowBuilder(schema, b)
    rb.begin(r_uint64(pk), r_uint64(0), r_int64(w))
    rb.put_int(r_int64(val))
    rb.commit()
    return b


def test_enforce_unique_pk(base_dir):
    os.write(1, "[Catalog] Testing _enforce_unique_pk...\n")
    db_path = base_dir + "/enforce_unique_pk"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    from gnitz.catalog.registry import _enforce_unique_pk
    from gnitz.storage.owned_batch import ArenaZSetBatch
    from gnitz.catalog.system_tables import RowBuilder

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
        rb5.begin(r_uint64(5), r_uint64(0), r_int64(1))
        rb5.put_int(r_int64(10))
        rb5.commit()
        rb5.begin(r_uint64(5), r_uint64(0), r_int64(1))
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
        rb6.begin(r_uint64(6), r_uint64(0), r_int64(1))
        rb6.put_int(r_int64(10))
        rb6.commit()
        rb6.begin(r_uint64(6), r_uint64(0), r_int64(-1))
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
    batch1 = owned_batch.ArenaZSetBatch(table1.schema)
    rb = RowBuilder(table1.schema, batch1)
    rb.begin(r_uint64(42), r_uint64(0), r_int64(1))
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
# Tests (from catalog_additional)
# ------------------------------------------------------------------------------


def test_index_functional_and_fanout(base_dir):
    os.write(1, "[Catalog+] Testing Index Functional Fan-out...\n")
    db_path = base_dir + "/index_func"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        cols = [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="id"),
            core_types.ColumnDefinition(core_types.TYPE_I64, name="val"),
        ]

        family = engine.create_table("public.tfanout", cols, 0)

        # Ingest baseline
        b = owned_batch.ArenaZSetBatch(family.schema)
        rb = RowBuilder(family.schema, b)
        for i in range(5):
            rb.begin(r_uint64(i), r_uint64(0), r_int64(1))
            rb.put_int(r_int64(i * 100))
            rb.commit()
        ingest_to_family(family, b)
        b.free()

        # Create Index
        circuit = engine.create_index("public.tfanout", "val")

        if _count_records(circuit.table) != 5:
            raise Exception("Index backfill count mismatch")

        # Live fan-out test (Verify the 3-stage ingestion pipeline)
        b2 = owned_batch.ArenaZSetBatch(family.schema)
        rb2 = RowBuilder(family.schema, b2)
        rb2.begin(r_uint64(99), r_uint64(0), r_int64(1))
        rb2.put_int(r_int64(777))
        rb2.commit()
        ingest_to_family(family, b2)
        b2.free()

        if _count_records(circuit.table) != 6:
            raise Exception("Live index fan-out failed")

        # Verify Standard Index Drop
        engine.drop_index(circuit.name)
        if engine.registry.has_index_by_name(circuit.name):
            raise Exception("Standard index should have been dropped")

    finally:
        engine.close()
    os.write(1, "    [OK] Index fan-out and drop verified.\n")


def test_orphaned_metadata_recovery(base_dir):
    os.write(1, "[Catalog+] Testing Orphaned Metadata Recovery...\n")
    db_path = base_dir + "/orphaned"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        idx_sys = engine.sys.indices
        b = owned_batch.ArenaZSetBatch(idx_sys.schema)
        # Manually inject index metadata pointing to a non-existent table ID 99999
        IdxTab.append(
            b,
            idx_sys.schema,
            888,
            99999,
            OWNER_KIND_TABLE,
            1,
            "orphaned_idx",
            0,
            "",
        )
        idx_sys.ingest_batch(b)
        b.free()
        idx_sys.flush()
    finally:
        engine.close()

    # Re-open: The loader should fail because orphaned metadata is corruption
    raised = False
    try:
        engine2 = open_engine(db_path)
        engine2.close()
    except LayoutError:
        raised = True
    if not raised:
        raise Exception("Orphaned index should cause a LayoutError on reload")
    os.write(1, "    [OK] Orphaned metadata correctly rejected.\n")


def test_schema_mr_poisoning():
    os.write(1, "[Catalog+] Testing Schema mr-poisoning...\n")
    s1 = core_types.TableSchema(
        [core_types.ColumnDefinition(core_types.TYPE_U64, name="pk")], 0
    )
    s2 = core_types.TableSchema(
        [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="pk2"),
            core_types.ColumnDefinition(core_types.TYPE_I64, name="val"),
        ],
        0,
    )

    s3 = core_types.merge_schemas_for_join(s1, s2)
    b = owned_batch.ArenaZSetBatch(s3)
    rb = RowBuilder(s3, b)
    rb.begin(r_uint64(0), r_uint64(0), r_int64(1))
    rb.put_int(r_int64(123))
    rb.commit()

    acc = b.get_accessor(0)
    if intmask(rffi.cast(rffi.LONGLONG, acc.get_int(1))) != 123:
        raise Exception("Row interaction on joined schema failed")
    b.free()
    os.write(1, "    [OK] Joined schema resizability safe.\n")


def test_identifier_boundary_slicing():
    os.write(1, "[Catalog+] Testing Identifier Slicing...\n")
    res = identifiers.parse_qualified_name(".table", "def")
    if res[0] != "" or res[1] != "table":
        raise Exception("Parse .table failed")
    res = identifiers.parse_qualified_name("schema.", "def")
    if res[0] != "schema" or res[1] != "":
        raise Exception("Parse schema. failed")
    os.write(1, "    [OK] Identifier slicing bounds safe.\n")


def test_sequence_gap_recovery(base_dir):
    os.write(1, "[Catalog+] Testing Sequence Gap Recovery...\n")
    db_path = base_dir + "/seq_gap"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        cols = [core_types.ColumnDefinition(core_types.TYPE_U64, name="id")]
        engine.create_table("public.t1", cols, 0)

        # 1. Inject table record for tid 250
        tbl_sys = engine.sys.tables
        b = owned_batch.ArenaZSetBatch(tbl_sys.schema)
        rb = RowBuilder(tbl_sys.schema, b)
        rb.begin(r_uint64(250), r_uint64(0), r_int64(1))
        rb.put_int(r_int64(2))  # sid public
        rb.put_string("gap_table")
        rb.put_string(db_path + "/gap")
        rb.put_int(r_int64(0))
        rb.put_int(r_int64(0))
        rb.commit()
        tbl_sys.ingest_batch(b)
        b.free()
        tbl_sys.flush()

        # 2. Inject column record for tid 250 (Required for reconstruction)
        col_sys = engine.sys.columns
        bc = owned_batch.ArenaZSetBatch(col_sys.schema)
        ColTab.append(
            bc,
            col_sys.schema,
            250,
            OWNER_KIND_TABLE,
            0,
            "id",
            core_types.TYPE_U64.code,
            0,
            0,
            0,
        )
        col_sys.ingest_batch(bc)
        bc.free()
        col_sys.flush()
    finally:
        engine.close()

    # Engine re-open will rebuild registry. It should find table 250 and set next_id to 251.
    engine2 = open_engine(db_path)
    try:
        t_new = engine2.create_table("public.tnext", cols, 0)
        if t_new.table_id != 251:
            raise Exception(
                "Sequence recovery failed. Expected 251, got %d" % t_new.table_id
            )
    finally:
        engine2.close()
    os.write(1, "    [OK] Sequence gap recovery verified.\n")


def test_fk_referential_integrity(base_dir):
    os.write(1, "[Catalog+] Testing FK Referential Integrity...\n")
    db_path = base_dir + "/fk_integrity"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        # 1. Create Parent (U64 PK)
        parent_cols = [core_types.ColumnDefinition(core_types.TYPE_U64, name="pid")]
        parent = engine.create_table("public.parents", parent_cols, 0)

        # 2. Create Child (U64 FK referencing Parent)
        child_cols = [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="cid"),
            core_types.ColumnDefinition(
                core_types.TYPE_U64,
                name="pid_fk",
                fk_table_id=parent.table_id,
                fk_col_idx=0,
            ),
        ]
        child = engine.create_table("public.children", child_cols, 0)

        # 3. Insert valid parent (PK-only schema, no payload columns)
        pb = owned_batch.ArenaZSetBatch(parent.schema)
        rb_p = RowBuilder(parent.schema, pb)
        rb_p.begin(r_uint64(10), r_uint64(0), r_int64(1))
        rb_p.commit()
        ingest_to_family(parent, pb)
        pb.free()

        # 4. Insert valid child
        cb = owned_batch.ArenaZSetBatch(child.schema)
        rb_c = RowBuilder(child.schema, cb)
        rb_c.begin(r_uint64(1), r_uint64(0), r_int64(1))
        rb_c.put_int(r_int64(10))  # Valid pid
        rb_c.commit()
        validate_fk_inline(child, cb)
        ingest_to_family(child, cb)
        cb.free()

        # 5. Insert INVALID child (pid 99 does not exist)
        cb2 = owned_batch.ArenaZSetBatch(child.schema)
        rb_c2 = RowBuilder(child.schema, cb2)
        rb_c2.begin(r_uint64(2), r_uint64(0), r_int64(1))
        rb_c2.put_int(r_int64(99))
        rb_c2.commit()

        raised = False
        try:
            validate_fk_inline(child, cb2)
            ingest_to_family(child, cb2)
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Referential integrity failed to block invalid FK")
        cb2.free()

    finally:
        engine.close()

    # 6. Test U128 FK (UUID style) — separate engine to stay under fd limit
    db_path2 = base_dir + "/fk_integrity_u128"
    if not os_path_exists(db_path2):
        rposix.mkdir(db_path2, 0o755)

    engine2 = open_engine(db_path2)
    try:
        u_parent_cols = [
            core_types.ColumnDefinition(core_types.TYPE_U128, name="uuid")
        ]
        u_parent = engine2.create_table("public.uparents", u_parent_cols, 0)

        u_child_cols = [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="id"),
            core_types.ColumnDefinition(
                core_types.TYPE_U128,
                name="ufk",
                fk_table_id=u_parent.table_id,
                fk_col_idx=0,
            ),
        ]
        u_child = engine2.create_table("public.uchildren", u_child_cols, 0)

        upb = owned_batch.ArenaZSetBatch(u_parent.schema)
        rb_up = RowBuilder(u_parent.schema, upb)
        rb_up.begin(r_uint64(0xBBBB), r_uint64(0xAAAA), r_int64(1))
        rb_up.commit()
        ingest_to_family(u_parent, upb)
        upb.free()

        ucb = owned_batch.ArenaZSetBatch(u_child.schema)
        rb_uc = RowBuilder(u_child.schema, ucb)
        rb_uc.begin(r_uint64(1), r_uint64(0), r_int64(1))
        rb_uc.put_u128(r_uint64(0xBBBB), r_uint64(0xAAAA))
        rb_uc.commit()
        validate_fk_inline(u_child, ucb)
        ingest_to_family(u_child, ucb)
        ucb.free()

    finally:
        engine2.close()
    os.write(1, "    [OK] FK Integrity verified.\n")


def test_fk_nullability_and_retractions(base_dir):
    os.write(1, "[Catalog+] Testing FK Nullability and Retractions...\n")
    db_path = base_dir + "/fk_null"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        parent = engine.create_table(
            "public.p",
            [core_types.ColumnDefinition(core_types.TYPE_U64, name="id")],
            0,
        )
        # Nullable FK
        child = engine.create_table(
            "public.c",
            [
                core_types.ColumnDefinition(core_types.TYPE_U64, name="id"),
                core_types.ColumnDefinition(
                    core_types.TYPE_U64,
                    is_nullable=True,
                    name="pid_fk",
                    fk_table_id=parent.table_id,
                    fk_col_idx=0,
                ),
            ],
            0,
        )

        # 1. Insert NULL FK (Should be allowed even if parent is empty)
        cb = owned_batch.ArenaZSetBatch(child.schema)
        rb = RowBuilder(child.schema, cb)
        rb.begin(r_uint64(1), r_uint64(0), r_int64(1))
        rb.put_null()
        rb.commit()
        validate_fk_inline(child, cb)
        ingest_to_family(child, cb)
        cb.free()

        # 2. Test Retraction (Weight -1)
        # Ingesting a retraction for a non-existent parent should NOT trigger FK check
        cb2 = owned_batch.ArenaZSetBatch(child.schema)
        rb2 = RowBuilder(child.schema, cb2)
        rb2.begin(r_uint64(2), r_uint64(0), r_int64(-1))
        rb2.put_int(r_int64(999))  # Non-existent
        rb2.commit()
        validate_fk_inline(child, cb2)
        ingest_to_family(child, cb2)  # Should succeed
        cb2.free()

    finally:
        engine.close()
    os.write(1, "    [OK] FK Nullability and Retractions verified.\n")


def test_fk_protections(base_dir):
    os.write(1, "[Catalog+] Testing FK Drop/Index Protections...\n")
    db_path = base_dir + "/fk_prot"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        parent = engine.create_table(
            "public.parent",
            [core_types.ColumnDefinition(core_types.TYPE_U64, name="pid")],
            0,
        )
        child = engine.create_table(
            "public.child",
            [
                core_types.ColumnDefinition(core_types.TYPE_U64, name="cid"),
                core_types.ColumnDefinition(
                    core_types.TYPE_U64,
                    name="pid_fk",
                    fk_table_id=parent.table_id,
                    fk_col_idx=0,
                ),
            ],
            0,
        )

        # 1. Attempt to drop Parent (Should fail)
        raised = False
        try:
            engine.drop_table("public.parent")
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to drop a referenced table")

        # 2. Attempt to drop the auto-generated FK index (Should fail)
        idx_name = make_fk_index_name("public", "child", "pid_fk")
        raised = False
        try:
            engine.drop_index(idx_name)
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to drop auto-generated FK index")

        # 3. Drop child, then parent (Should succeed)
        engine.drop_table("public.child")
        engine.drop_table("public.parent")

    finally:
        engine.close()
    os.write(1, "    [OK] FK Drop protections verified.\n")


def test_fk_invalid_targets(base_dir):
    os.write(1, "[Catalog+] Testing Invalid FK Targets...\n")
    db_path = base_dir + "/fk_invalid"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        p = engine.create_table(
            "public.p",
            [
                core_types.ColumnDefinition(core_types.TYPE_U64, name="pk"),
                core_types.ColumnDefinition(core_types.TYPE_I64, name="other"),
            ],
            0,
        )

        # Attempt to target column index 1 (not the PK)
        raised = False
        try:
            engine.create_table(
                "public.c_bad",
                [
                    core_types.ColumnDefinition(core_types.TYPE_U64, name="pk"),
                    core_types.ColumnDefinition(
                        core_types.TYPE_I64,
                        name="fk",
                        fk_table_id=p.table_id,
                        fk_col_idx=1,
                    ),
                ],
                0,
            )
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Allowed to create FK targeting non-PK column")

    finally:
        engine.close()
    os.write(1, "    [OK] Invalid FK targets blocked.\n")


def test_fk_self_reference(base_dir):
    os.write(1, "[Catalog+] Testing FK Self-Reference...\n")
    db_path = base_dir + "/fk_self"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        # Predict the TID of the next table for self-reference
        tid = engine.registry._next_table_id
        cols = [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="emp_id"),
            core_types.ColumnDefinition(
                core_types.TYPE_U64,
                is_nullable=True,
                name="mgr_id",
                fk_table_id=tid,
                fk_col_idx=0,
            ),
        ]
        emp = engine.create_table("public.employees", cols, 0)

        # 1. Ingest Manager first (Commit required for FK check)
        b1 = owned_batch.ArenaZSetBatch(emp.schema)
        rb1 = RowBuilder(emp.schema, b1)
        rb1.begin(r_uint64(1), r_uint64(0), r_int64(1))
        rb1.put_null()
        rb1.commit()
        ingest_to_family(emp, b1)
        b1.free()

        # 2. Ingest Subordinate referencing the Manager
        b2 = owned_batch.ArenaZSetBatch(emp.schema)
        rb2 = RowBuilder(emp.schema, b2)
        rb2.begin(r_uint64(2), r_uint64(0), r_int64(1))
        rb2.put_int(r_int64(1))  # Refers to emp_id 1
        rb2.commit()
        ingest_to_family(emp, b2)
        b2.free()

        if _count_records(emp.store) != 2:
            raise Exception("Self-referential ingestion failed")

    finally:
        engine.close()
    os.write(1, "    [OK] FK Self-reference verified.\n")


def test_view_backfill_simple(base_dir):
    os.write(1, "[Catalog+] Testing View Backfill on Creation...\n")
    db_path = base_dir + "/view_backfill_simple"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        cols = [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="id"),
            core_types.ColumnDefinition(core_types.TYPE_I64, name="val"),
        ]
        family = engine.create_table("public.t", cols, 0)

        b = owned_batch.ArenaZSetBatch(family.schema)
        rb = RowBuilder(family.schema, b)
        for i in range(5):
            rb.begin(r_uint64(i), r_uint64(0), r_int64(1))
            rb.put_int(r_int64(i * 10))
            rb.commit()
        ingest_to_family(family, b)
        b.free()

        view_family = _make_passthrough_view(engine, "public.v", family)

        if _count_records(view_family.store) != 5:
            raise Exception(
                "View backfill on creation failed: expected 5, got %d"
                % _count_records(view_family.store)
            )

        b2 = owned_batch.ArenaZSetBatch(family.schema)
        rb2 = RowBuilder(family.schema, b2)
        rb2.begin(r_uint64(99), r_uint64(0), r_int64(1))
        rb2.put_int(r_int64(999))
        rb2.commit()
        effective2 = ingest_to_family(family, b2)
        evaluate_dag(engine, family.table_id, effective2)
        if effective2 is not b2:
            effective2.free()
        b2.free()

        if _count_records(view_family.store) != 6:
            raise Exception(
                "Live view update failed: expected 6, got %d"
                % _count_records(view_family.store)
            )
    finally:
        engine.close()
    os.write(1, "    [OK] View backfill on creation verified.\n")


def test_view_backfill_on_restart(base_dir):
    os.write(1, "[Catalog+] Testing View Backfill on Restart...\n")
    db_path = base_dir + "/view_backfill_restart"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        cols = [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="id"),
            core_types.ColumnDefinition(core_types.TYPE_I64, name="val"),
        ]
        family = engine.create_table("public.t", cols, 0)
        _make_passthrough_view(engine, "public.v", family)

        b = owned_batch.ArenaZSetBatch(family.schema)
        rb = RowBuilder(family.schema, b)
        for i in range(5):
            rb.begin(r_uint64(i), r_uint64(0), r_int64(1))
            rb.put_int(r_int64(i * 10))
            rb.commit()
        ingest_to_family(family, b)
        b.free()
        family.store.flush()
    finally:
        engine.close()

    engine2 = open_engine(db_path)
    try:
        view_family2 = engine2.registry.get("public", "v")
        if _count_records(view_family2.store) != 5:
            raise Exception(
                "View backfill on restart failed: expected 5, got %d"
                % _count_records(view_family2.store)
            )
    finally:
        engine2.close()
    os.write(1, "    [OK] View backfill on restart verified.\n")


def test_view_on_view_backfill_on_restart(base_dir):
    os.write(1, "[Catalog+] Testing View-on-View Backfill on Restart...\n")
    db_path = base_dir + "/view_on_view_restart"
    if not os_path_exists(db_path):
        rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        cols = [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="id"),
            core_types.ColumnDefinition(core_types.TYPE_I64, name="val"),
        ]
        t = engine.create_table("public.t", cols, 0)

        b = owned_batch.ArenaZSetBatch(t.schema)
        rb = RowBuilder(t.schema, b)
        for val in [5, 20, 50, 80, 100]:
            rb.begin(r_uint64(val), r_uint64(0), r_int64(1))
            rb.put_int(r_int64(val))
            rb.commit()
        ingest_to_family(t, b)
        b.free()
        t.store.flush()

        v1_family = _make_passthrough_view(engine, "public.v1", t)
        v2_family = _make_passthrough_view(engine, "public.v2", v1_family)

        c1 = _count_records(v1_family.store)
        if c1 != 5:
            raise Exception("V1 backfill on creation: expected 5, got %d" % c1)
        c2 = _count_records(v2_family.store)
        if c2 != 5:
            raise Exception("V2 backfill on creation: expected 5, got %d" % c2)

        if v1_family.depth != 1:
            raise Exception("V1 depth: expected 1, got %d" % v1_family.depth)
        if v2_family.depth != 2:
            raise Exception("V2 depth: expected 2, got %d" % v2_family.depth)
    finally:
        engine.close()

    engine2 = open_engine(db_path)
    try:
        v1_f2 = engine2.registry.get("public", "v1")
        v2_f2 = engine2.registry.get("public", "v2")
        c1 = _count_records(v1_f2.store)
        if c1 != 5:
            raise Exception("V1 backfill on restart: expected 5, got %d" % c1)
        c2 = _count_records(v2_f2.store)
        if c2 != 5:
            raise Exception("V2 backfill on restart: expected 5, got %d" % c2)
    finally:
        engine2.close()
    os.write(1, "    [OK] View-on-view backfill on restart verified.\n")


# ------------------------------------------------------------------------------
# Tests (from zstore_comprehensive)
# ------------------------------------------------------------------------------


def test_programmable_zset_lifecycle():
    log("Starting Comprehensive Programmable Z-Set Lifecycle Test...")

    base_dir = "zstore_test_data"
    cleanup_dir(base_dir)
    ensure_dir(base_dir)

    # 1. Bootstrapping
    log_step("Phase 1: Bootstrapping Engine")
    db = engine_module.open_engine(base_dir)
    assert_true(db.registry.has_schema("public"), "Public schema missing")

    # 2. Schema and 128-bit Table Creation
    log_step("Phase 2: Creating 128-bit Relational Schema")
    db.create_schema("app")

    user_cols = newlist_hint(2)
    user_cols.append(types.ColumnDefinition(types.TYPE_U128, name="uid"))
    user_cols.append(types.ColumnDefinition(types.TYPE_STRING, name="username"))
    users_family = db.create_table("app.users", user_cols, 0)

    order_cols = newlist_hint(3)
    order_cols.append(types.ColumnDefinition(types.TYPE_U64, name="oid"))
    order_cols.append(
        types.ColumnDefinition(
            types.TYPE_U128,
            name="uid",
            fk_table_id=users_family.table_id,
            fk_col_idx=0,
        )
    )
    order_cols.append(types.ColumnDefinition(types.TYPE_I64, name="amount"))
    orders_family = db.create_table("app.orders", order_cols, 0)

    # 3. Referential Integrity Enforcement
    log_step("Phase 3: Testing Foreign Key Enforcement")
    # Synthetic 128-bit key using shifts to avoid prebuilt long literals
    u128_val = (r_uint128(0xDEADBEEF) << 64) | r_uint128(0xCAFEBABE)

    bad_batch = owned_batch.ArenaZSetBatch(orders_family.schema)
    rb_bad = RowBuilder(orders_family.schema, bad_batch)
    rb_bad.begin(r_uint64(1), r_uint64(0), r_int64(1))
    rb_bad.put_u128(r_uint64(0xCAFEBABE), r_uint64(0xDEADBEEF))
    rb_bad.put_int(r_int64(500))
    rb_bad.commit()

    fk_raised = False
    try:
        validate_fk_inline(orders_family, bad_batch)
        ingest_to_family(orders_family, bad_batch)
    except LayoutError:
        fk_raised = True
        log("Caught expected FK violation (User not found)")
    assert_true(fk_raised, "FK violation should have raised LayoutError")
    bad_batch.free()

    # 4. Valid Data Ingestion
    log_step("Phase 4: Ingesting valid relational data")
    u_batch = owned_batch.ArenaZSetBatch(users_family.schema)
    rb_u = RowBuilder(users_family.schema, u_batch)
    rb_u.begin(r_uint64(u128_val), r_uint64(u128_val >> 64), r_int64(1))
    rb_u.put_string("alice")
    rb_u.commit()
    ingest_to_family(users_family, u_batch)
    u_batch.free()

    assert_true(
        users_family.store.has_pk(r_uint64(u128_val), r_uint64(u128_val >> 64)), "User ingestion visibility failed"
    )

    o_batch = owned_batch.ArenaZSetBatch(orders_family.schema)
    rb_o = RowBuilder(orders_family.schema, o_batch)
    rb_o.begin(r_uint64(101), r_uint64(0), r_int64(1))
    rb_o.put_u128(r_uint64(0xCAFEBABE), r_uint64(0xDEADBEEF))
    rb_o.put_int(r_int64(1000))
    rb_o.commit()
    ingest_to_family(orders_family, o_batch)
    o_batch.free()

    # 5. Reactive View Construction (New Circuit API)
    log_step("Phase 5: Creating Reactive View (Scan Users)")

    # CircuitBuilder now requires the primary_source_id (users table)
    builder = CircuitBuilder(
        view_id=0, primary_source_id=users_family.table_id
    )
    # input_delta() represents the reactive delta stream for that source
    users_src = builder.input_delta()
    builder.sink(users_src, target_table_id=0)

    out_cols = newlist_hint(2)
    out_cols.append(("uid", types.TYPE_U128.code))
    out_cols.append(("username", types.TYPE_STRING.code))

    graph = builder.build(out_cols)

    db.create_view("app.active_users", graph, "SELECT * FROM users")
    view_family = db.get_table("app.active_users")

    # 5.1 Relational Dependency Enforcement
    log_step("Phase 5.1: Verifying Dependency Enforcement")
    drop_denied = False
    try:
        db.drop_table("app.users")
    except LayoutError:
        drop_denied = True
        log("Correctly blocked dropping table referenced by view")
    assert_true(drop_denied, "Dropping table used by view should be blocked")

    # 6. Persistence Audit
    log_step("Phase 6: Persistence Audit (Close and Restart)")
    db.close()

    db2 = engine_module.open_engine(base_dir)
    assert_true(
        db2.registry.has("app", "users"), "Registry lost users table"
    )
    assert_true(
        db2.registry.has("app", "active_users"), "Registry lost view"
    )

    # 7. Recovery Audit
    log_step("Phase 7: Auditing recovered VM Program")
    plan = db2.program_cache.get_program(view_family.table_id)
    assert_true(plan is not None, "Failed to recover view plan")

    # 8. View Execution (New View API)
    log_step("Phase 8: Execution of Recovered View handle")
    # Feed the actual alice record as a delta to the reactive view
    in_batch = owned_batch.ArenaZSetBatch(users_family.schema)
    rb_in = RowBuilder(users_family.schema, in_batch)
    rb_in.begin(r_uint64(u128_val), r_uint64(u128_val >> 64), r_int64(1))
    rb_in.put_string("alice")
    rb_in.commit()

    out_batch = plan.execute_epoch(in_batch)
    in_batch.free()

    assert_true(out_batch is not None, "View should have produced Alice")
    assert_equal_i(1, out_batch.length(), "View produced wrong row count")
    assert_equal_u128(u128_val, out_batch.get_pk(0), "View data corrupted")
    out_batch.free()

    # 9. Edge Case: Graph Compilation Failures
    log_step("Phase 9: Testing Graph Compilation Failures")

    # A. Disconnected sink failure
    bad_builder = CircuitBuilder(0, users_family.table_id)
    bad_builder.input_delta()
    # No sink
    bad_graph = bad_builder.build(out_cols)

    compilation_failed = False
    try:
        db2.create_view("app.bad_view", bad_graph, "")
    except LayoutError:
        compilation_failed = True
        log("Correctly rejected view missing sink")
    assert_true(compilation_failed, "Should fail compilation: missing sink")

    # B. Cyclic Graph
    cycle_builder = CircuitBuilder(0, users_family.table_id)
    s1 = cycle_builder.input_delta()
    d1 = cycle_builder.delay(s1)
    # Manual connection to create a cycle (illegal for Kahn's)
    cycle_builder._connect(d1, s1, 1)  # PORT_IN
    cycle_graph = cycle_builder.build(out_cols)

    compilation_failed = False
    try:
        db2.create_view("app.cycle_view", cycle_graph, "")
    except LayoutError:
        compilation_failed = True
        log("Correctly rejected cyclic graph")
    assert_true(
        compilation_failed, "Should fail compilation: cycle detected"
    )

    # 10. Teardown
    log_step("Phase 10: Full Teardown and Cleanup")
    db2.drop_view("app.active_users")
    db2.drop_table("app.orders")
    db2.drop_table("app.users")
    db2.drop_schema("app")

    assert_true(not db2.registry.has_schema("app"), "Schema cleanup failed")
    db2.close()
    log("Full Programmable Z-Set lifecycle audit PASSED.")


# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------


def entry_point(argv):
    ensure_jit_reachable()
    os.write(1, "--- GnitzDB Comprehensive Catalog Test ---\n")
    base_dir = "catalog_test_data"
    cleanup_dir(base_dir)
    if not os_path_exists(base_dir):
        rposix.mkdir(base_dir, 0o755)

    try:
        # Original catalog tests
        test_identifiers()
        test_bootstrap(base_dir)
        test_ddl(base_dir)
        test_unique_pk_metadata(base_dir)
        test_edge_cases(base_dir)
        test_enforce_unique_pk(base_dir)
        test_restart(base_dir)

        # Catalog additional tests
        test_index_functional_and_fanout(base_dir)
        test_orphaned_metadata_recovery(base_dir)
        test_schema_mr_poisoning()
        test_identifier_boundary_slicing()
        test_sequence_gap_recovery(base_dir)
        test_fk_referential_integrity(base_dir)
        test_fk_nullability_and_retractions(base_dir)
        test_fk_protections(base_dir)
        test_fk_invalid_targets(base_dir)
        test_fk_self_reference(base_dir)
        test_view_backfill_simple(base_dir)
        test_view_backfill_on_restart(base_dir)
        test_view_on_view_backfill_on_restart(base_dir)

        # ZStore lifecycle test (self-contained)
        test_programmable_zset_lifecycle()

        os.write(1, "\nALL CATALOG TEST PATHS PASSED\n")
    except GnitzError as e:
        os.write(2, "TEST FAILED (GnitzError): " + e.msg + "\n")
        return 1
    except OSError as e:
        os.write(2, "TEST FAILED (OSError): errno=" + str(e.errno) + "\n")
        return 1
    except Exception as e:
        os.write(2, "TEST FAILED: " + str(e) + "\n")
        return 1
    finally:
        cleanup_dir(base_dir)
        cleanup_dir("zstore_test_data")

    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
