# catalog_additional_test.py

import sys
import os
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import rposix, rposix_stat

from gnitz.core import types as core_types
from gnitz.core import batch
from gnitz.core.batch import RowBuilder
from gnitz.core.errors import LayoutError, GnitzError
from gnitz.catalog import identifiers
from gnitz.catalog.engine import open_engine
from gnitz.catalog.registry import ingest_to_family, validate_fk_inline
from gnitz.catalog.system_tables import (
    FIRST_USER_TABLE_ID,
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
from gnitz.storage.ephemeral_table import EphemeralTable
from gnitz.server.executor import evaluate_dag
from rpython_tests.helpers.circuit_builder import CircuitBuilder

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
            if (st.st_mode & 0o170000) == 0o040000:
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
        b = batch.ArenaZSetBatch(family.schema)
        rb = RowBuilder(family.schema, b)
        for i in range(5):
            rb.begin(r_uint128(r_uint64(i)), r_int64(1))
            rb.put_int(r_int64(i * 100))
            rb.commit()
        ingest_to_family(family, b)
        b.free()

        # Create Index
        circuit = engine.create_index("public.tfanout", "val")

        if _count_records(circuit.table) != 5:
            raise Exception("Index backfill count mismatch")

        # Live fan-out test (Verify the 3-stage ingestion pipeline)
        b2 = batch.ArenaZSetBatch(family.schema)
        rb2 = RowBuilder(family.schema, b2)
        rb2.begin(r_uint128(r_uint64(99)), r_int64(1))
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
        b = batch.ArenaZSetBatch(idx_sys.schema)
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
    b = batch.ArenaZSetBatch(s3)
    rb = RowBuilder(s3, b)
    rb.begin(r_uint128(r_uint64(0)), r_int64(1))
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
        b = batch.ArenaZSetBatch(tbl_sys.schema)
        rb = RowBuilder(tbl_sys.schema, b)
        rb.begin(r_uint128(r_uint64(250)), r_int64(1))
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
        bc = batch.ArenaZSetBatch(col_sys.schema)
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
        pb = batch.ArenaZSetBatch(parent.schema)
        rb_p = RowBuilder(parent.schema, pb)
        rb_p.begin(r_uint128(r_uint64(10)), r_int64(1))
        rb_p.commit()
        ingest_to_family(parent, pb)
        pb.free()

        # 4. Insert valid child
        cb = batch.ArenaZSetBatch(child.schema)
        rb_c = RowBuilder(child.schema, cb)
        rb_c.begin(r_uint128(r_uint64(1)), r_int64(1))
        rb_c.put_int(r_int64(10))  # Valid pid
        rb_c.commit()
        validate_fk_inline(child, cb)
        ingest_to_family(child, cb)
        cb.free()

        # 5. Insert INVALID child (pid 99 does not exist)
        cb2 = batch.ArenaZSetBatch(child.schema)
        rb_c2 = RowBuilder(child.schema, cb2)
        rb_c2.begin(r_uint128(r_uint64(2)), r_int64(1))
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

        # 6. Test U128 FK (UUID style)
        u_parent_cols = [
            core_types.ColumnDefinition(core_types.TYPE_U128, name="uuid")
        ]
        u_parent = engine.create_table("public.uparents", u_parent_cols, 0)

        u_child_cols = [
            core_types.ColumnDefinition(core_types.TYPE_U64, name="id"),
            core_types.ColumnDefinition(
                core_types.TYPE_U128,
                name="ufk",
                fk_table_id=u_parent.table_id,
                fk_col_idx=0,
            ),
        ]
        u_child = engine.create_table("public.uchildren", u_child_cols, 0)

        uuid_val = (r_uint128(0xAAAA) << 64) | r_uint128(0xBBBB)
        upb = batch.ArenaZSetBatch(u_parent.schema)
        rb_up = RowBuilder(u_parent.schema, upb)
        rb_up.begin(uuid_val, r_int64(1))
        rb_up.commit()
        ingest_to_family(u_parent, upb)
        upb.free()

        ucb = batch.ArenaZSetBatch(u_child.schema)
        rb_uc = RowBuilder(u_child.schema, ucb)
        rb_uc.begin(r_uint128(r_uint64(1)), r_int64(1))
        rb_uc.put_u128(r_uint64(0xBBBB), r_uint64(0xAAAA))
        rb_uc.commit()
        validate_fk_inline(u_child, ucb)
        ingest_to_family(u_child, ucb)
        ucb.free()

    finally:
        engine.close()
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
        cb = batch.ArenaZSetBatch(child.schema)
        rb = RowBuilder(child.schema, cb)
        rb.begin(r_uint128(r_uint64(1)), r_int64(1))
        rb.put_null()
        rb.commit()
        validate_fk_inline(child, cb)
        ingest_to_family(child, cb)
        cb.free()

        # 2. Test Retraction (Weight -1)
        # Ingesting a retraction for a non-existent parent should NOT trigger FK check
        cb2 = batch.ArenaZSetBatch(child.schema)
        rb2 = RowBuilder(child.schema, cb2)
        rb2.begin(r_uint128(r_uint64(2)), r_int64(-1))
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
        b1 = batch.ArenaZSetBatch(emp.schema)
        rb1 = RowBuilder(emp.schema, b1)
        rb1.begin(r_uint128(r_uint64(1)), r_int64(1))
        rb1.put_null()
        rb1.commit()
        ingest_to_family(emp, b1)
        b1.free()

        # 2. Ingest Subordinate referencing the Manager
        b2 = batch.ArenaZSetBatch(emp.schema)
        rb2 = RowBuilder(emp.schema, b2)
        rb2.begin(r_uint128(r_uint64(2)), r_int64(1))
        rb2.put_int(r_int64(1))  # Refers to emp_id 1
        rb2.commit()
        ingest_to_family(emp, b2)
        b2.free()

        if _count_records(emp.store) != 2:
            raise Exception("Self-referential ingestion failed")

    finally:
        engine.close()
    os.write(1, "    [OK] FK Self-reference verified.\n")


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

        b = batch.ArenaZSetBatch(family.schema)
        rb = RowBuilder(family.schema, b)
        for i in range(5):
            rb.begin(r_uint128(r_uint64(i)), r_int64(1))
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

        b2 = batch.ArenaZSetBatch(family.schema)
        rb2 = RowBuilder(family.schema, b2)
        rb2.begin(r_uint128(r_uint64(99)), r_int64(1))
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

        b = batch.ArenaZSetBatch(family.schema)
        rb = RowBuilder(family.schema, b)
        for i in range(5):
            rb.begin(r_uint128(r_uint64(i)), r_int64(1))
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

        b = batch.ArenaZSetBatch(t.schema)
        rb = RowBuilder(t.schema, b)
        for val in [5, 20, 50, 80, 100]:
            rb.begin(r_uint128(r_uint64(val)), r_int64(1))
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

        # View backfill tests
        test_view_backfill_simple(base_dir)
        test_view_backfill_on_restart(base_dir)
        test_view_on_view_backfill_on_restart(base_dir)

        os.write(1, "\nALL ADDITIONAL CATALOG TEST PATHS PASSED\n")
    except GnitzError as e:
        os.write(2, "\nADDITIONAL TEST FAILED\n")
        os.write(2, "Error: " + e.msg + "\n")
        return 1
    except Exception as e:
        os.write(2, "\nADDITIONAL TEST FAILED\n")
        os.write(2, "Error: " + str(e) + "\n")
        return 1

    return 0


def target(driver, args):
    return entry_point, None


if __name__ == "__main__":
    entry_point(sys.argv)
