# catalog_additional_test.py

import sys
import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import rposix, rposix_stat

from gnitz.core import types as core_types
from gnitz.core import values, batch
from gnitz.core.errors import LayoutError, GnitzError
from gnitz.catalog import identifiers
from gnitz.catalog.engine import open_engine
from gnitz.catalog.system_tables import (
    FIRST_USER_TABLE_ID, 
    OWNER_KIND_TABLE,
    pack_column_id
)
from gnitz.catalog.index_circuit import (
    IndexCircuit, 
    get_index_key_type,
    make_index_schema,
    make_fk_index_name,
    make_secondary_index_name
)
from gnitz.catalog.system_records import (
    _append_index_record,
    _append_column_record
)
from gnitz.storage.ephemeral_table import EphemeralTable

# --- Diagnostic Helpers ---

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

def _count_records(table):
    count = 0
    cursor = table.create_cursor()
    while cursor.is_valid():
        count += 1
        cursor.advance()
    cursor.close()
    return count

# --- Test Cases ---

def test_index_functional_and_fanout(base_dir):
    os.write(1, "[Catalog+] Testing Index Functional Fan-out...\n")
    db_path = base_dir + "/index_func"
    if not os_path_exists(db_path): rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        cols = [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="id"),
            core_types.ColumnDefinition(core_types.TYPE_I64, name="val"),
        ]
        
        family = engine.create_table("public.tfanout", cols, 0)

        # Ingest baseline
        b = batch.ZSetBatch(family.schema)
        for i in range(5):
            row = values.make_payload_row(family.schema)
            row.append_int(r_int64(i * 100))
            b.append(r_uint128(r_uint64(i)), r_int64(1), row)
        family.ingest_batch(b)
        b.free()

        # Create Index
        circuit = engine.create_index("public.tfanout", "val")

        if _count_records(circuit.table) != 5:
            raise Exception("Index backfill count mismatch")

        # Live fan-out test (Verify the 3-stage ingestion pipeline)
        b2 = batch.ZSetBatch(family.schema)
        row = values.make_payload_row(family.schema)
        row.append_int(r_int64(777))
        b2.append(r_uint128(r_uint64(99)), r_int64(1), row)
        family.ingest_batch(b2)
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
    if not os_path_exists(db_path): rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        idx_sys = engine.sys.indices
        b = batch.ZSetBatch(idx_sys.schema)
        # Manually inject index metadata pointing to a non-existent table ID 99999
        # to test the loader's resilience to orphaned records.
        _append_index_record(b, idx_sys.schema, 888, 99999, OWNER_KIND_TABLE, 1, 
                             "orphaned_idx", 0, "")
        idx_sys.ingest_batch(b)
        b.free()
        idx_sys.flush()
    finally:
        engine.close()

    # Re-open: The loader should skip the index record because table 99999 doesn't exist
    engine2 = open_engine(db_path)
    try:
        if engine2.registry.has_index_by_name("orphaned_idx"):
            raise Exception("Orphaned index should not have been registered")
    finally:
        engine2.close()
    os.write(1, "    [OK] Orphaned metadata handled.\n")


def test_schema_mr_poisoning():
    os.write(1, "[Catalog+] Testing Schema mr-poisoning...\n")
    s1 = core_types.TableSchema([core_types.ColumnDefinition(core_types.TYPE_U64, name="pk")], 0)
    s2 = core_types.TableSchema([core_types.ColumnDefinition(core_types.TYPE_U64, name="pk2"), 
                                 core_types.ColumnDefinition(core_types.TYPE_I64, name="val")], 0)
    
    s3 = core_types.merge_schemas_for_join(s1, s2)
    row = values.make_payload_row(s3)
    row.append_int(r_int64(123))
    if intmask(row.get_int(0)) != 123:
        raise Exception("Row interaction on joined schema failed")
    os.write(1, "    [OK] Joined schema resizability safe.\n")


def test_identifier_boundary_slicing():
    os.write(1, "[Catalog+] Testing Identifier Slicing...\n")
    res = identifiers.parse_qualified_name(".table", "def")
    if res[0] != "" or res[1] != "table": raise Exception("Parse .table failed")
    res = identifiers.parse_qualified_name("schema.", "def")
    if res[0] != "schema" or res[1] != "": raise Exception("Parse schema. failed")
    os.write(1, "    [OK] Identifier slicing bounds safe.\n")


def test_sequence_gap_recovery(base_dir):
    os.write(1, "[Catalog+] Testing Sequence Gap Recovery...\n")
    db_path = base_dir + "/seq_gap"
    if not os_path_exists(db_path): rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        cols = [core_types.ColumnDefinition(core_types.TYPE_U64, name="id")]
        engine.create_table("public.t1", cols, 0)
        
        # 1. Inject table record for tid 250
        tbl_sys = engine.sys.tables
        b = batch.ZSetBatch(tbl_sys.schema)
        row = values.make_payload_row(tbl_sys.schema)
        row.append_int(r_int64(2)) # sid public
        row.append_string("gap_table")
        row.append_string(db_path + "/gap")
        row.append_int(r_int64(0)) 
        row.append_int(r_int64(0)) 
        b.append(r_uint128(r_uint64(250)), r_int64(1), row)
        tbl_sys.ingest_batch(b)
        b.free()
        tbl_sys.flush()

        # 2. Inject column record for tid 250 (Required for reconstruction)
        col_sys = engine.sys.columns
        bc = batch.ZSetBatch(col_sys.schema)
        _append_column_record(
            bc, col_sys.schema, 250, OWNER_KIND_TABLE, 0, "id", 
            core_types.TYPE_U64.code, 0, 0, 0
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
            raise Exception("Sequence recovery failed. Expected 251, got %d" % t_new.table_id)
    finally:
        engine2.close()
    os.write(1, "    [OK] Sequence gap recovery verified.\n")


def test_fk_referential_integrity(base_dir):
    os.write(1, "[Catalog+] Testing FK Referential Integrity...\n")
    db_path = base_dir + "/fk_integrity"
    if not os_path_exists(db_path): rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        # 1. Create Parent (U64 PK)
        parent_cols = [core_types.ColumnDefinition(core_types.TYPE_U64, name="pid")]
        parent = engine.create_table("public.parents", parent_cols, 0)
        
        # 2. Create Child (U64 FK referencing Parent)
        child_cols = [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="cid"),
            core_types.ColumnDefinition(core_types.TYPE_U64, name="pid_fk", 
                                       fk_table_id=parent.table_id, fk_col_idx=0)
        ]
        child = engine.create_table("public.children", child_cols, 0)

        # 3. Insert valid parent
        pb = batch.ZSetBatch(parent.schema)
        pb.append(r_uint128(r_uint64(10)), r_int64(1), values.make_payload_row(parent.schema))
        parent.ingest_batch(pb)
        pb.free()

        # 4. Insert valid child
        cb = batch.ZSetBatch(child.schema)
        row = values.make_payload_row(child.schema)
        row.append_int(r_int64(10)) # Valid pid
        cb.append(r_uint128(r_uint64(1)), r_int64(1), row)
        child.ingest_batch(cb)
        cb.free()

        # 5. Insert INVALID child (pid 99 does not exist)
        cb2 = batch.ZSetBatch(child.schema)
        row2 = values.make_payload_row(child.schema)
        row2.append_int(r_int64(99))
        cb2.append(r_uint128(r_uint64(2)), r_int64(1), row2)
        
        raised = False
        try:
            child.ingest_batch(cb2)
        except LayoutError:
            raised = True
        if not raised:
            raise Exception("Referential integrity failed to block invalid FK")
        cb2.free()

        # 6. Test U128 FK (UUID style)
        u_parent_cols = [core_types.ColumnDefinition(core_types.TYPE_U128, name="uuid")]
        u_parent = engine.create_table("public.uparents", u_parent_cols, 0)
        
        u_child_cols = [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="id"),
            core_types.ColumnDefinition(core_types.TYPE_U128, name="ufk",
                                       fk_table_id=u_parent.table_id, fk_col_idx=0)
        ]
        u_child = engine.create_table("public.uchildren", u_child_cols, 0)

        uuid_val = (r_uint128(0xAAAA) << 64) | r_uint128(0xBBBB)
        upb = batch.ZSetBatch(u_parent.schema)
        upb.append(uuid_val, r_int64(1), values.make_payload_row(u_parent.schema))
        u_parent.ingest_batch(upb)
        upb.free()

        ucb = batch.ZSetBatch(u_child.schema)
        row3 = values.make_payload_row(u_child.schema)
        row3.append_u128(r_uint64(0xBBBB), r_uint64(0xAAAA))
        ucb.append(r_uint128(r_uint64(1)), r_int64(1), row3)
        u_child.ingest_batch(ucb)
        ucb.free()
        
    finally:
        engine.close()
    os.write(1, "    [OK] FK Integrity verified.\n")


def test_fk_nullability_and_retractions(base_dir):
    os.write(1, "[Catalog+] Testing FK Nullability and Retractions...\n")
    db_path = base_dir + "/fk_null"
    if not os_path_exists(db_path): rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        parent = engine.create_table("public.p", [core_types.ColumnDefinition(core_types.TYPE_U64, name="id")], 0)
        # Nullable FK
        child = engine.create_table("public.c", [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="id"),
            core_types.ColumnDefinition(core_types.TYPE_U64, is_nullable=True, name="pid_fk",
                                       fk_table_id=parent.table_id, fk_col_idx=0)
        ], 0)

        # 1. Insert NULL FK (Should be allowed even if parent is empty)
        cb = batch.ZSetBatch(child.schema)
        row = values.make_payload_row(child.schema)
        row.append_null(0) # pid_fk is the first payload column
        cb.append(r_uint128(r_uint64(1)), r_int64(1), row)
        child.ingest_batch(cb)
        cb.free()

        # 2. Test Retraction (Weight -1)
        # Ingesting a retraction for a non-existent parent should NOT trigger FK check
        cb2 = batch.ZSetBatch(child.schema)
        row2 = values.make_payload_row(child.schema)
        row2.append_int(r_int64(999)) # Non-existent
        cb2.append(r_uint128(r_uint64(2)), r_int64(-1), row2)
        child.ingest_batch(cb2) # Should succeed
        cb2.free()

    finally:
        engine.close()
    os.write(1, "    [OK] FK Nullability and Retractions verified.\n")


def test_fk_protections(base_dir):
    os.write(1, "[Catalog+] Testing FK Drop/Index Protections...\n")
    db_path = base_dir + "/fk_prot"
    if not os_path_exists(db_path): rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        parent = engine.create_table("public.parent", [core_types.ColumnDefinition(core_types.TYPE_U64, name="pid")], 0)
        child = engine.create_table("public.child", [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="cid"),
            core_types.ColumnDefinition(core_types.TYPE_U64, name="pid_fk", fk_table_id=parent.table_id, fk_col_idx=0)
        ], 0)

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
    if not os_path_exists(db_path): rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        p = engine.create_table("public.p", [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="pk"),
            core_types.ColumnDefinition(core_types.TYPE_I64, name="other")
        ], 0)

        # Attempt to target column index 1 (not the PK)
        raised = False
        try:
            engine.create_table("public.c_bad", [
                core_types.ColumnDefinition(core_types.TYPE_U64, name="pk"),
                core_types.ColumnDefinition(core_types.TYPE_I64, name="fk", fk_table_id=p.table_id, fk_col_idx=1)
            ], 0)
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
    if not os_path_exists(db_path): rposix.mkdir(db_path, 0o755)

    engine = open_engine(db_path)
    try:
        # Predict the TID of the next table for self-reference
        tid = engine.registry._next_table_id
        cols = [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="emp_id"),
            core_types.ColumnDefinition(core_types.TYPE_U64, is_nullable=True, 
                                       name="mgr_id", fk_table_id=tid, fk_col_idx=0)
        ]
        emp = engine.create_table("public.employees", cols, 0)

        # 1. Ingest Manager first (Commit required for FK check)
        b1 = batch.ZSetBatch(emp.schema)
        r_mgr = values.make_payload_row(emp.schema)
        r_mgr.append_null(0)
        b1.append(r_uint128(r_uint64(1)), r_int64(1), r_mgr)
        emp.ingest_batch(b1)
        b1.free()

        # 2. Ingest Subordinate referencing the Manager
        b2 = batch.ZSetBatch(emp.schema)
        r_sub = values.make_payload_row(emp.schema)
        r_sub.append_int(r_int64(1)) # Refers to emp_id 1
        b2.append(r_uint128(r_uint64(2)), r_int64(1), r_sub)
        emp.ingest_batch(b2)
        b2.free()

        if _count_records(emp.primary) != 2:
            raise Exception("Self-referential ingestion failed")

    finally:
        engine.close()
    os.write(1, "    [OK] FK Self-reference verified.\n")


# --- Entry Point ---

def entry_point(argv):
    os.write(1, "--- GnitzDB Additional Catalog Tests ---\n")
    base_dir = "catalog_additional_test_data"
    cleanup_dir(base_dir)
    if not os_path_exists(base_dir):
        rposix.mkdir(base_dir, 0o755)

    try:
        test_index_functional_and_fanout(base_dir)
        test_orphaned_metadata_recovery(base_dir)
        test_schema_mr_poisoning()
        test_identifier_boundary_slicing()
        test_sequence_gap_recovery(base_dir)
        
        # Foreign Key tests
        test_fk_referential_integrity(base_dir)
        test_fk_nullability_and_retractions(base_dir)
        test_fk_protections(base_dir)
        test_fk_invalid_targets(base_dir)
        test_fk_self_reference(base_dir)
        
        os.write(1, "\nALL ADDITIONAL CATALOG TEST PATHS PASSED\n")
    except Exception as e:
        os.write(2, "\nADDITIONAL TEST FAILED\n")
        os.write(2, "Error: " + str(e) + "\n")
        return 1

    return 0

def target(driver, args):
    return entry_point, None

if __name__ == "__main__":
    entry_point(sys.argv)
