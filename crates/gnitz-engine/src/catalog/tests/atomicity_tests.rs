use super::*;
use std::fs;

// ---------------------------------------------------------------------------
// Helpers (supplement the shared helpers in mod.rs)
// ---------------------------------------------------------------------------

fn build_schema_tab_row(sid: i64, name: &str) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Schema.schema());
    bb.begin_row(sid as u128, 1);
    bb.put_string(name);
    bb.end_row();
    bb.finish()
}

/// A rejected relation CREATE must leave the catalog exactly as it was: no
/// name/id cache entry, no DAG registration, and no orphaned row in the
/// family's memtable. `init_rows` is the family's row count taken before the
/// attempt.
fn assert_no_relation_residue(engine: &mut CatalogEngine, family: SysFamily, id: i64, qname: &str, init_rows: usize) {
    let noun = family.row_noun();
    assert!(
        !engine.caches.entity_by_qname.contains_key(qname),
        "entity_by_qname holds the rejected {noun} {qname}"
    );
    assert!(
        !engine.caches.entity_by_id.contains_key(&id),
        "entity_by_id holds the rejected {noun} {id}"
    );
    assert!(
        !engine.dag.tables.contains_key(&id),
        "dag.tables holds the rejected {noun} {id}"
    );
    assert_eq!(
        count_records(engine.sys_store_mut(family)),
        init_rows,
        "the {noun} family memtable holds an orphaned row"
    );
}

/// Write one column record at an arbitrary `col_idx` — the gap and
/// out-of-order shapes `build_col_batch`, which numbers columns by position,
/// cannot produce.
fn write_col_at_index(engine: &mut CatalogEngine, owner_id: i64, col_idx: i64, cd: &ColumnDef) -> Result<(), String> {
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    push_col_tab_row(&mut bb, owner_id, OWNER_KIND_TABLE, col_idx, cd, 1);
    engine.ingest_to_family(COL_TAB_ID, &bb.finish())
}

// ---------------------------------------------------------------------------
// Part 1 — precheck-before-mutate tests
//
// `precheck_family` runs before any mutation, so a rejected positive-weight
// (CREATE) row for TABLE_TAB, VIEW_TAB, or IDX_TAB leaves no orphaned memtable
// row and no dirty cache entry behind.
// ---------------------------------------------------------------------------

// ── Part 1: TABLE_TAB, no column records ─────────────────────────────────────

#[test]
fn test_table_tab_no_cols_leaves_clean_state() {
    let dir = temp_dir("atomicity_no_cols");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let init_rows = count_records(engine.sys_store_mut(SysFamily::Table));

    let tid = engine.allocate_table_id().unwrap();
    // No column records written — TABLE_TAB ingestion must fail.
    let batch = build_table_tab_row(tid, pack_pk_cols(&[0]), "badtable");
    let result = engine.ingest_to_family(TABLE_TAB_ID, &batch);
    assert!(result.is_err(), "expected error for TABLE_TAB with no column records");

    assert_no_relation_residue(&mut engine, SysFamily::Table, tid, "public.badtable", init_rows);

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: TABLE_TAB, non-pk-eligible PK column ─────────────────────────────

#[test]
fn test_table_tab_invalid_pk_col_type_leaves_clean_state() {
    // validate_pk_cols runs inside hook_relation_register *after*
    // `apply_entity_caches` runs before the register hook, so a hook rejection
    // must leave no cache entry behind.
    let dir = temp_dir("atomicity_bad_pk_type");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let init_rows = count_records(engine.sys_store_mut(SysFamily::Table));

    let tid = engine.allocate_table_id().unwrap();
    // STRING column is not pk-eligible.
    let cols = vec![col_def("label", type_code::STRING)];
    engine.write_column_records(tid, OWNER_KIND_TABLE, &cols).unwrap();

    let batch = build_table_tab_row(tid, pack_pk_cols(&[0]), "badpktable");
    let result = engine.ingest_to_family(TABLE_TAB_ID, &batch);
    assert!(
        result.is_err(),
        "expected error for TABLE_TAB with non-pk-eligible PK column"
    );

    assert_no_relation_residue(&mut engine, SysFamily::Table, tid, "public.badpktable", init_rows);

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: TABLE_TAB, duplicate qualified name ───────────────────────────────

#[test]
fn test_table_tab_dup_name_leaves_clean_state() {
    // A raw ingest_to_family with a duplicate qualified name should be
    // rejected. `apply_entity_caches` would otherwise overwrite the cache entry
    // with the new tid.
    let dir = temp_dir("atomicity_dup_name");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let orig_tid = engine.create_table("public.dupname", &cols, &[0]).unwrap();
    let init_rows = count_records(engine.sys_store_mut(SysFamily::Table));

    let new_tid = engine.allocate_table_id().unwrap();
    engine.write_column_records(new_tid, OWNER_KIND_TABLE, &cols).unwrap();

    let batch = build_table_tab_row(new_tid, pack_pk_cols(&[0]), "dupname");
    let result = engine.ingest_to_family(TABLE_TAB_ID, &batch);
    assert!(
        result.is_err(),
        "expected error: qualified name 'public.dupname' already exists"
    );

    assert_eq!(
        engine.caches.entity_by_qname.get("public.dupname").copied(),
        Some(orig_tid),
        "entity_by_qname must still point to the original table after rejected duplicate"
    );
    assert!(
        !engine.dag.tables.contains_key(&new_tid),
        "new_tid must not appear in dag.tables"
    );
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Table)),
        init_rows,
        "sys_tables must have no extra orphaned row"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: TABLE_TAB, non-contiguous column indices ─────────────────────────

#[test]
fn test_table_tab_col_contiguity_gap_rejected() {
    // Pre-fix: the gap is not detected; the table is silently registered
    // with two columns that are actually at positions 0 and 2 (not 0 and 1),
    // causing schema mismatches downstream.  The test asserts the DDL fails
    // and leaves no trace in the catalog.
    let dir = temp_dir("atomicity_col_gap");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let init_rows = count_records(engine.sys_store_mut(SysFamily::Table));

    let tid = engine.allocate_table_id().unwrap();
    // Insert columns at indices 0 and 2 — index 1 is absent (gap).
    write_col_at_index(&mut engine, tid, 0, &col_def("id", type_code::U64)).unwrap();
    write_col_at_index(&mut engine, tid, 2, &col_def("gapped", type_code::U64)).unwrap();

    let batch = build_table_tab_row(tid, pack_pk_cols(&[0]), "gaptable");
    let result = engine.ingest_to_family(TABLE_TAB_ID, &batch);
    assert!(
        result.is_err(),
        "expected error for TABLE_TAB with non-contiguous column indices"
    );

    assert_no_relation_residue(&mut engine, SysFamily::Table, tid, "public.gaptable", init_rows);

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: VIEW_TAB, no column records ──────────────────────────────────────

#[test]
fn test_view_tab_no_cols_leaves_clean_state() {
    // hook_relation_register fires its col_defs.is_empty() rejection after
    // apply_entity_caches has already written entity_by_qname.
    let dir = temp_dir("atomicity_view_no_cols");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let init_rows = count_records(engine.sys_store_mut(SysFamily::View));

    let vid = engine.allocate_table_id().unwrap();
    // No column records for vid.
    let batch = build_view_tab_row(vid, "badview", "");
    let result = engine.ingest_to_family(VIEW_TAB_ID, &batch);
    assert!(result.is_err(), "expected error for VIEW_TAB with no column records");

    assert_no_relation_residue(&mut engine, SysFamily::View, vid, "public.badview", init_rows);

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: VIEW_TAB, over-wide schema rejected cleanly ──────────────────────

#[test]
fn test_view_tab_too_many_cols_rejected() {
    // An over-wide view must be rejected with a clean catalog error that leaves no
    // orphaned cache/memtable state — mirroring the TABLE_TAB path, where the
    // precheck rejects before `apply_entity_caches` mutates the caches.
    // hook_relation_register carries the same guard as the build_schema_from_col_defs
    // assert backstop. This is the engine-side counterpart to the client guard in
    // create_view_chain.
    let dir = temp_dir("atomicity_view_too_many_cols");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let init_rows = count_records(engine.sys_store_mut(SysFamily::View));

    let vid = engine.allocate_table_id().unwrap();
    // MAX_COLUMNS + 1 contiguous column records (col 0 is a valid U64 PK).
    for i in 0..(crate::schema::MAX_COLUMNS as i64 + 1) {
        write_col_at_index(&mut engine, vid, i, &col_def(&format!("c{i}"), type_code::U64)).unwrap();
    }
    let batch = build_view_tab_row(vid, "wideview", "");
    let err = engine
        .ingest_to_family(VIEW_TAB_ID, &batch)
        .expect_err("expected error for over-wide view");
    assert!(
        err.contains("columns") && err.contains("max"),
        "expected the column-count guard message, got: {err}"
    );

    assert_no_relation_residue(&mut engine, SysFamily::View, vid, "public.wideview", init_rows);

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: IDX_TAB, non-existent owner ──────────────────────────────────────

#[test]
fn test_idx_tab_bad_owner_leaves_clean_state() {
    // `apply_index_caches` runs before hook_index_register, so the
    // cache entry is inserted before the hook returns Err for missing owner.
    let dir = temp_dir("atomicity_idx_bad_owner");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let init_rows = count_records(engine.sys_store_mut(SysFamily::Index));

    let nonexistent_owner = engine.allocate_table_id().unwrap();
    let idx_id = engine.allocate_index_id().unwrap();
    let batch = idx_tab_batch(idx_id, nonexistent_owner, 0, "bad_owner_idx", false, 1);
    let result = engine.ingest_to_family(IDX_TAB_ID, &batch);
    assert!(result.is_err(), "expected error for IDX_TAB with non-existent owner");

    assert!(
        !engine.caches.index_by_name.contains_key("bad_owner_idx"),
        "index_by_name must not contain the rejected index"
    );
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Index)),
        init_rows,
        "sys_indices memtable must have no orphaned row"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: IDX_TAB, view owner ──────────────────────────────────────────────

#[test]
fn test_idx_tab_view_owner_rejected() {
    // Only base tables can own a secondary index: index projection runs only
    // on the base-table DML paths, so an index registered on a view would
    // backfill once and then silently serve stale results. The SQL binder
    // rejects CREATE INDEX on a view by name resolution; this is the
    // engine-side guard for a raw IDX_TAB push naming a view owner.
    let dir = temp_dir("atomicity_idx_view_owner");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    engine
        .create_table("public.base", &[col_def("id", type_code::U64)], &[0])
        .unwrap();

    // Register a view via the raw system-table path (no circuit needed — the
    // precheck must fire before any backfill).
    let vid = engine.allocate_table_id().unwrap();
    engine
        .write_column_records(vid, OWNER_KIND_VIEW, &[col_def("id", type_code::U64)])
        .unwrap();
    let batch = build_view_tab_row(vid, "vowner", "");
    engine.ingest_to_family(VIEW_TAB_ID, &batch).unwrap();
    assert!(engine.dag.tables.contains_key(&vid), "view registered");

    let init_rows = count_records(engine.sys_store_mut(SysFamily::Index));
    let idx_id = engine.allocate_index_id().unwrap();
    let batch = idx_tab_batch(idx_id, vid, 0, "idx_on_view", false, 1);
    let err = engine
        .ingest_to_family(IDX_TAB_ID, &batch)
        .expect_err("IDX_TAB row naming a view owner must be rejected");
    assert!(
        err.contains("is a view") && err.contains("only a base table can be indexed"),
        "expected the owner-kind guard message, got: {err}"
    );

    assert!(
        !engine.caches.index_by_name.contains_key("idx_on_view"),
        "index_by_name must not contain the rejected index"
    );
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Index)),
        init_rows,
        "sys_indices must have no orphaned row"
    );
    assert!(
        engine.dag.tables.get(&vid).unwrap().index_circuits.is_empty(),
        "the view must not gain an index circuit"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: IDX_TAB, duplicate index name ────────────────────────────────────

#[test]
fn test_idx_tab_dup_name_leaves_clean_state() {
    // `apply_index_caches` would otherwise overwrite the cache entry with new_idx_id
    // when two IDX_TAB rows carry the same name string.
    let dir = temp_dir("atomicity_idx_dup");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![
        col_def("id", type_code::U64),
        col_def("val", type_code::I64),
        col_def("ts", type_code::I64),
    ];
    let tid = engine.create_table("public.idxtest", &cols, &[0]).unwrap();
    let orig_idx_id = engine.create_index("public.idxtest", &["val"], false).unwrap();
    let init_rows = count_records(engine.sys_store_mut(SysFamily::Index));

    let orig_name = "public__idxtest__idx_val";
    let new_idx_id = engine.allocate_index_id().unwrap();
    // Same name, different col (ts at index 2) — bypasses create_index dup check.
    let batch = idx_tab_batch(new_idx_id, tid, 2, orig_name, false, 1);
    let result = engine.ingest_to_family(IDX_TAB_ID, &batch);
    assert!(
        result.is_err(),
        "expected error: index name '{orig_name}' already exists"
    );

    assert_eq!(
        engine.caches.index_by_name.get(orig_name).copied(),
        Some(orig_idx_id),
        "index_by_name must still point to the original index"
    );
    assert!(
        !engine.caches.indices_by_owner.values().any(|v| v.contains(&new_idx_id)),
        "the rejected index id must not appear under any owner"
    );
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Index)),
        init_rows,
        "sys_indices must have no extra orphaned row"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Part 2d — CREATE INDEX backfill failure must not leak the index directory
// ---------------------------------------------------------------------------

#[test]
fn test_create_unique_index_backfill_fail_no_dir_leak() {
    // `Table::new` creates the index directory before backfill_index checks for
    // duplicates, so a failed backfill would orphan it. The hook stages the
    // directory first, and `with_staged_dir` reclaims a stage whose closure
    // failed — no caller has to remember to drain.
    let dir = temp_dir("atomicity_idx_dir_leak");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.leaktest", &cols, &[0]).unwrap();
    let schema = engine.get_schema_desc(tid).unwrap();

    // Ingest two rows that share the same 'val' — unique index backfill must fail.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(42u64);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(42u64);
    bb.end_row();
    engine.ingest_to_family(tid, &bb.finish()).unwrap();
    engine.flush_family(tid).unwrap();

    // Capture the expected index directory before create_index allocates the id.
    let expected_idx_id = engine.next_index_id;
    let owner_dir = format!("{dir}/public/leaktest_{tid}");
    let idx_dir = format!("{owner_dir}/idx_{expected_idx_id}");

    let result = engine.create_index("public.leaktest", &["val"], true);
    assert!(result.is_err(), "unique index with duplicate values must fail");

    // Reclaimed by the failing stage itself, with no drain from the caller.
    assert!(
        engine.pending_dir_deletions.is_empty(),
        "a failed stage must leave nothing queued"
    );
    assert!(
        !std::path::Path::new(&idx_dir).exists(),
        "index directory must not leak after failed backfill: {idx_dir}"
    );
    assert!(
        engine
            .dag
            .tables
            .get(&tid)
            .map(|e| e.index_circuits.is_empty())
            .unwrap_or(true),
        "no index circuit must be registered after failed CREATE INDEX"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Part 6 — next_index_id worker synchronization
// ---------------------------------------------------------------------------

#[test]
fn test_next_index_id_advances_on_index_register() {
    // Pre-fix: hook_index_register(+1) never advances next_index_id.
    // A subsequent allocate_index_id() call on the worker would return an ID
    // that the master already assigned to an explicit user index, causing
    // directory collisions.
    let dir = temp_dir("atomicity_idx_seq");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    let tid = engine.create_table("public.seqsync", &cols, &[0]).unwrap();

    // Register an index with an idx_id far ahead of the current counter,
    // simulating a worker receiving a broadcast for a master-allocated ID.
    let large_idx_id = engine.next_index_id + 500;
    let batch = idx_tab_batch(large_idx_id, tid, 1, "public__seqsync__idx_val_sync", false, 1);
    engine.ingest_to_family(IDX_TAB_ID, &batch).unwrap();

    // next_index_id must now be > large_idx_id so that a local
    // allocate_index_id() never returns large_idx_id again.
    assert!(
        engine.next_index_id > large_idx_id,
        "next_index_id ({}) must exceed the registered idx_id ({}) after \
         hook_index_register",
        engine.next_index_id,
        large_idx_id
    );

    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn test_next_schema_id_advances_on_schema_register() {
    // Pre-fix: the Schema-family appliers never advance next_schema_id, so a
    // SCHEMA_TAB row replayed from the SAL after a crash-before-checkpoint
    // restores the schema's caches but leaves next_schema_id stale — the next
    // CREATE SCHEMA re-allocates the same schema_id. Mirrors
    // test_next_index_id_advances_on_index_register.
    let dir = temp_dir("atomicity_schema_seq");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // A SCHEMA_TAB row whose id is far ahead of the recovered counter, standing
    // in for a durable CREATE SCHEMA whose sys_sequences advance never reached a
    // flushed shard.
    let large_sid = engine.next_schema_id + 500;
    let batch = build_schema_tab_row(large_sid, "recovered_schema");
    engine.ingest_to_family(SCHEMA_TAB_ID, &batch).unwrap();

    assert!(
        engine.next_schema_id > large_sid,
        "next_schema_id ({}) must exceed the registered sid ({}) so \
         allocate_schema_id never re-issues it",
        engine.next_schema_id,
        large_sid,
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 2: DROP SCHEMA must not probe the table-keyed dep map ────────────────

#[test]
fn test_drop_schema_id_colliding_with_dependent_table_id_ok() {
    // Schema ids (allocated from FIRST_USER_SCHEMA_ID = 3) and table ids
    // (from FIRST_USER_TABLE_ID = 16) share one i64 space. As both counters
    // climb, a freshly-allocated schema id eventually equals an EARLIER table
    // id. The negative-weight (DROP) precheck must not probe the table-keyed
    // view-dependency map with a SCHEMA_TAB drop id — a schema row is never a
    // dependency source. Probing it spuriously matches the unrelated table's
    // dependents and wrongly rejects the DROP SCHEMA.
    //
    // Pre-fix: drop_schema("victim") fails with "View dependency: owner.t".
    let dir = temp_dir("atomicity_schema_id_collision");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // Table T (in schema `owner`) with a dependent view V → dep_map[T] = [V].
    engine.create_schema("owner").unwrap();
    let tid = engine
        .create_table("owner.t", &[col_def("id", type_code::U64)], &[0])
        .unwrap();
    let vid = engine.allocate_table_id().unwrap();
    write_identity_circuit(&mut engine, vid, tid, None);
    assert_eq!(
        engine.dag.get_dep_map().get(&tid),
        Some(&vec![vid]),
        "precondition: dependency edge T -> V must be present"
    );

    // Allocate filler schemas until the next schema id collides with `tid`,
    // then create the victim schema so that schema_id("victim") == Some(tid).
    while engine.next_schema_id < tid {
        let name = format!("filler_{}", engine.next_schema_id);
        engine.create_schema(&name).unwrap();
    }
    assert_eq!(
        engine.next_schema_id, tid,
        "test setup: next schema id must land exactly on tid"
    );
    engine.create_schema("victim").unwrap();
    assert_eq!(
        engine.schema_id("victim").expect("the schema exists"),
        tid,
        "test setup: victim schema id must collide with tid"
    );

    // The victim schema is empty and unrelated to T/V. Dropping it must
    // succeed despite its id colliding with a table that has a dependent view.
    engine.drop_schema("victim").unwrap();
    assert!(
        !engine.caches.schema_by_name.contains_key("victim"),
        "victim schema must be gone after a successful DROP SCHEMA"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// DDL_TXN bundle rollback — the ghost regression and the hook-failure path
//
// These drive the generalized handler shape directly: precheck_family +
// apply_and_enqueue_family per family, with the between-precheck-and-apply
// marker, then compensate_stage_a — exactly as `handle_ddl_txn` does. The
// existing tests above stop at `ingest_to_family` and never reach
// compensate_stage_a, so the ghost -1 was untested.
// ---------------------------------------------------------------------------

/// The durable relation-id ceiling is enforced at `precheck_family` — the point
/// an id ENTERS the `dag.tables` namespace, BEFORE any mutation.
///
/// Guarding `allocate_table_id` alone would not cover it: the register hooks take
/// the id straight off the ingested row and `raise_id_counter` it, and that id is
/// caller-chosen (a client may preset `circuit.view_id`). The ceiling is a
/// conservative tripwire held safely short of the u32 physical contract every id
/// narrows to (see `RELATION_ID_CEILING`). Both families whose PK is a
/// `dag.tables` id must reject it. (Index ids are a disjoint namespace.)
#[test]
fn precheck_rejects_relation_id_at_or_above_ceiling() {
    let dir = temp_dir("relation_id_ceiling_reject");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    for (label, tid) in [
        ("at the ceiling", sys_tables::RELATION_ID_CEILING),
        ("above the ceiling", sys_tables::RELATION_ID_CEILING + 4096),
    ] {
        let table_batch = build_table_tab_row(tid, pack_pk_cols(&[0]), "banded");
        let err = engine
            .precheck_family(SysFamily::Table, &table_batch)
            .expect_err(&format!("TABLE_TAB id at/above the ceiling must be rejected ({label})"));
        assert!(
            err.contains("relation-id ceiling"),
            "error must name the cause, got: {err}"
        );

        let view_batch = build_view_tab_row(tid, "bandedview", "SELECT 1");
        let err = engine
            .precheck_family(SysFamily::View, &view_batch)
            .expect_err(&format!("VIEW_TAB id at/above the ceiling must be rejected ({label})"));
        assert!(
            err.contains("relation-id ceiling"),
            "error must name the cause, got: {err}"
        );
    }

    // A ceiling, not a blanket ban: an ordinary durable id below it still passes
    // this guard (it fails later for unrelated reasons, if at all).
    let ok_batch = build_table_tab_row(sys_tables::RELATION_ID_CEILING - 1, pack_pk_cols(&[0]), "just_below");
    let err = engine
        .precheck_family(SysFamily::Table, &ok_batch)
        .err()
        .unwrap_or_default();
    assert!(
        !err.contains("relation-id ceiling"),
        "an id below the ceiling must not trip the ceiling guard, got: {err}"
    );
}

/// A CREATE bundle `[COL_TAB, TABLE_TAB]` whose TABLE_TAB fails **precheck**
/// (duplicate name) must leave neither an orphan COL_TAB nor a ghost `-1`
/// TABLE_TAB. The marker stays `None` (precheck failed before apply), so
/// compensation reconstructs nothing and only negates the drained COL_TAB.
#[test]
fn ddl_txn_precheck_failure_no_orphan_or_ghost() {
    let dir = temp_dir("ddl_txn_precheck_ghost");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    // Occupy the qualified name "public.dupname".
    engine.create_table("public.dupname", &cols, &[0]).unwrap();
    let cols_before = count_records(engine.sys_store_mut(SysFamily::Column));
    let tables_before = count_records(engine.sys_store_mut(SysFamily::Table));
    // Discard any queue entries the setup left behind, exactly as `handle_ddl_txn`
    // does before ingesting a new bundle — so compensation drains only this
    // bundle's families.
    let _ = engine.drain_pending_broadcasts();

    let new_tid = engine.allocate_table_id().unwrap();
    // Ascending topo: COL_TAB(1) applied + enqueued first.
    let col_batch = engine.build_col_batch(new_tid, OWNER_KIND_TABLE, &cols, 1);
    engine.precheck_family(SysFamily::Column, &col_batch).unwrap();
    engine.apply_and_enqueue_family(SysFamily::Column, col_batch).unwrap();

    // TABLE_TAB(6): precheck fails (duplicate name), so the handler's loop never
    // applies it and its marker stays None. Compensation reconstructs nothing.
    let table_batch = build_table_tab_row(new_tid, pack_pk_cols(&[0]), "dupname");
    assert!(
        engine.precheck_family(SysFamily::Table, &table_batch).is_err(),
        "duplicate-name TABLE_TAB must fail precheck"
    );
    engine.compensate_stage_a(None).unwrap();

    // The durable property: no orphan COL_TAB, no ghost -1 TABLE_TAB.
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Column)),
        cols_before,
        "orphan COL_TAB rows must be negated to zero"
    );
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Table)),
        tables_before,
        "no ghost -1 TABLE_TAB row"
    );
    assert!(
        !engine.caches.entity_by_id.contains_key(&new_tid),
        "no phantom entity survives for the reusable new_tid"
    );

    let _ = fs::remove_dir_all(&dir);
}

/// A CREATE bundle `[COL_TAB, TABLE_TAB]` whose TABLE_TAB passes precheck but
/// fails **inside** apply_and_enqueue (a register-hook error) must reconstruct
/// and negate the applied-not-enqueued TABLE_TAB row (net-zero sys_tables) and
/// negate the drained COL_TAB exactly once (no double-retraction ghost). A
/// REPLICATED table with a non-default distribution prefix is the trigger:
/// precheck does not check the pair, but `hook_relation_register` rejects it after
/// the row is applied.
#[test]
fn ddl_txn_hook_failure_negates_applied_not_enqueued() {
    let dir = temp_dir("ddl_txn_hook_rollback");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let cols_before = count_records(engine.sys_store_mut(SysFamily::Column));
    let tables_before = count_records(engine.sys_store_mut(SysFamily::Table));

    let new_tid = engine.allocate_table_id().unwrap();
    let col_batch = engine.build_col_batch(new_tid, OWNER_KIND_TABLE, &cols, 1);
    engine.precheck_family(SysFamily::Column, &col_batch).unwrap();
    engine.apply_and_enqueue_family(SysFamily::Column, col_batch).unwrap();

    // REPLICATED + dist_prefix = 1: passes precheck, rejected by hook_relation_register.
    let flags = gnitz_wire::TableProps {
        replicated: true,
        dist_prefix_len: 1,
        ..Default::default()
    }
    .pack();
    let table_batch = build_table_tab_row_flags(new_tid, pack_pk_cols(&[0]), "hooktbl", flags);
    engine
        .precheck_family(SysFamily::Table, &table_batch)
        .expect("replicated + dist_prefix passes precheck (it is a hook-layer check)");
    // Marker set BEFORE apply; apply fails in the hook, so it stays Some.
    let mut marker: Option<(SysFamily, Batch)> = Some((SysFamily::Table, table_batch.clone()));
    let applied = engine.apply_and_enqueue_family(SysFamily::Table, table_batch);
    assert!(
        applied.is_err(),
        "hook_relation_register must reject a REPLICATED table with a distribution prefix"
    );
    engine.compensate_stage_a(marker.take()).unwrap();

    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Table)),
        tables_before,
        "applied-not-enqueued TABLE_TAB row must net to zero"
    );
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Column)),
        cols_before,
        "drained COL_TAB rows must net to zero (negated exactly once)"
    );
    assert!(
        !engine.dag.tables.contains_key(&new_tid),
        "no registered table survives the rollback"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── Two `+1` relation rows may not claim one qualified name ──────────────────
// The name collision test reads `entity_by_qname`, which the batch has not been
// applied to yet — so a bundle carrying two CREATEs of the same `schema.name`
// under different ids passes it twice. Both would register, the second would
// overwrite the cache entry, and the first's store would be stranded under a
// name nothing resolves.

#[test]
fn two_creates_of_one_name_in_one_batch_rejected() {
    let dir = temp_dir("atomicity_dup_name_in_batch");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let init_rows = count_records(engine.sys_store_mut(SysFamily::Table));

    let cols = vec![col_def("id", type_code::U64)];
    let (a, b) = (engine.allocate_table_id().unwrap(), engine.allocate_table_id().unwrap());
    engine.write_column_records(a, OWNER_KIND_TABLE, &cols).unwrap();
    engine.write_column_records(b, OWNER_KIND_TABLE, &cols).unwrap();

    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    for tid in [a, b] {
        push_table_tab_row(&mut bb, tid, PUBLIC_SCHEMA_ID, "twins", pack_pk_cols(&[0]), 0, 1);
    }
    let err = engine
        .ingest_to_family(TABLE_TAB_ID, &bb.finish())
        .expect_err("two rows claiming public.twins must be rejected");
    assert!(err.contains("already exists"), "{err}");

    assert!(!engine.dag.tables.contains_key(&a));
    assert!(!engine.dag.tables.contains_key(&b));
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Table)),
        init_rows,
        "the rejected batch must leave no TABLE_TAB row"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── `_sequences` never accumulates an unmatched retraction ───────────────────
// A sequence advance retracts the row that is actually live, not a value the
// caller guessed. `_sequences` runs no `enforce_unique_pk`, so a `-1` against a
// row that was never inserted would never cancel — a permanent net −1 ghost, in
// violation of §1 base-table positivity.

#[test]
fn sequence_advances_leave_no_negative_ghost() {
    let dir = temp_dir("atomicity_seq_no_ghost");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    // First use of every catalog sequence: object ids, the checkpoint generation
    // (seq 4) and the topology word (seq 5) are all seeded on demand.
    engine.create_schema("s").unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::I64)];
    engine.create_table("s.t", &cols, &[0]).unwrap();
    engine.create_index("s.t", &["val"], false).unwrap();
    engine.record_topology(1).unwrap();
    engine.bump_checkpoint_generation().unwrap();
    // …and a second round, where each retraction now has a live row to cancel.
    engine.create_table("s.t2", &cols, &[0]).unwrap();
    engine.record_topology(4).unwrap();
    engine.bump_checkpoint_generation().unwrap();

    assert_eq!(
        count_negative_records(engine.sys_store_mut(SysFamily::Sequence)),
        0,
        "_sequences must hold no net-negative row"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── A rejected CREATE INDEX writes no compensating retraction ────────────────
// `create_index` prechecks and applies as two steps: a precheck rejection wrote
// nothing, so submitting the `-1` undo would leave a permanent net −1 ghost in
// sys_indices (which runs no `enforce_unique_pk`).

#[test]
fn precheck_rejected_create_index_writes_no_ghost() {
    let cols = vec![col_def("id", type_code::U64), col_def("name", type_code::STRING)];
    let (mut engine, _tid, dir) = table_fixture("atomicity_idx_precheck_ghost", &cols);
    let init_rows = count_records(engine.sys_store_mut(SysFamily::Index));

    // A STRING column has no index key type — rejected inside `precheck_family`,
    // before anything is applied.
    engine
        .create_index("public.t", &["name"], false)
        .expect_err("an index on a STRING column must be rejected");

    assert_eq!(
        count_negative_records(engine.sys_store_mut(SysFamily::Index)),
        0,
        "a precheck rejection must write no compensating -1"
    );
    assert_eq!(
        count_records(engine.sys_store_mut(SysFamily::Index)),
        init_rows,
        "and no +1 either"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
