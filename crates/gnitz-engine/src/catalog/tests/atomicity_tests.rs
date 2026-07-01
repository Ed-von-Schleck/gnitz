use super::*;
use std::fs;

// ---------------------------------------------------------------------------
// Helpers (supplement the shared helpers in mod.rs)
// ---------------------------------------------------------------------------

fn build_idx_tab_row(idx_id: i64, owner_id: i64, source_col_idx: u64, name: &str, is_unique: bool) -> Batch {
    let mut bb = BatchBuilder::new(idx_tab_schema());
    bb.begin_row(idx_id as u128, 1);
    bb.put_u64(owner_id as u64);
    bb.put_u64(OWNER_KIND_TABLE as u64);
    bb.put_u64(source_col_idx);
    bb.put_string(name);
    bb.put_u64(if is_unique { 1 } else { 0 });
    bb.put_string("");
    bb.end_row();
    bb.finish()
}

fn write_col_at_index(engine: &mut CatalogEngine, owner_id: i64, col_idx: i64, cd: &ColumnDef) -> Result<(), String> {
    let schema = col_tab_schema();
    let mut bb = BatchBuilder::new(schema);
    let pk = pack_column_id(owner_id, col_idx);
    bb.begin_row(pk as u128, 1);
    bb.put_u64(owner_id as u64);
    bb.put_u64(OWNER_KIND_TABLE as u64);
    bb.put_u64(col_idx as u64);
    bb.put_string(&cd.name);
    bb.put_u64(cd.type_code as u64);
    bb.put_u64(if cd.is_nullable { 1 } else { 0 });
    bb.put_u64(cd.fk_table_id as u64);
    bb.put_u64(cd.fk_col_idx as u64);
    bb.put_u64(0); // is_serial (engine ColumnDef has no SERIAL marker)
    bb.end_row();
    engine.ingest_to_family(COL_TAB_ID, &bb.finish())
}

// ---------------------------------------------------------------------------
// Part 1 — precheck-before-mutate tests
//
// Each test is RED until precheck_sys_ingest is extended to cover positive-
// weight (CREATE) rows for TABLE_TAB, VIEW_TAB, and IDX_TAB.  Before that
// extension the hook fires *after* ingest_batch_into and the cache appliers,
// so a hook failure leaves orphaned rows in the memtable and dirty cache
// entries.  After the extension, precheck rejects before any mutation.
// ---------------------------------------------------------------------------

// ── Part 1: TABLE_TAB, no column records ─────────────────────────────────────

#[test]
fn test_table_tab_no_cols_leaves_clean_state() {
    let dir = temp_dir("atomicity_no_cols");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let init_rows = count_records(&mut engine.sys_tables);

    let tid = engine.allocate_table_id();
    // No column records written — TABLE_TAB ingestion must fail.
    let batch = build_table_tab_row(&dir, tid, pack_pk_cols(&[0]), "badtable");
    let result = engine.ingest_to_family(TABLE_TAB_ID, &batch);
    assert!(result.is_err(), "expected error for TABLE_TAB with no column records");

    // All catalog state must be exactly as before the failed DDL.
    assert!(
        !engine.caches.entity_by_qname.contains_key("public.badtable"),
        "entity_by_qname must not contain bad table after rejected DDL (dirty before fix)"
    );
    assert!(
        !engine.caches.entity_by_id.contains_key(&tid),
        "entity_by_id must not contain bad table after rejected DDL"
    );
    assert!(
        !engine.dag.tables.contains_key(&tid),
        "dag.tables must not contain bad table after rejected DDL"
    );
    assert_eq!(
        count_records(&mut engine.sys_tables),
        init_rows,
        "sys_tables memtable must have no orphaned row (dirty before fix)"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: TABLE_TAB, non-pk-eligible PK column ─────────────────────────────

#[test]
fn test_table_tab_invalid_pk_col_type_leaves_clean_state() {
    // validate_pk_cols runs inside hook_table_register *after*
    // apply_entity_by_qname has already dirtied the cache (pre-fix).
    let dir = temp_dir("atomicity_bad_pk_type");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let init_rows = count_records(&mut engine.sys_tables);

    let tid = engine.allocate_table_id();
    // STRING column is not pk-eligible.
    let cols = vec![str_col_def("label")];
    engine.write_column_records(tid, OWNER_KIND_TABLE, &cols).unwrap();

    let batch = build_table_tab_row(&dir, tid, pack_pk_cols(&[0]), "badpktable");
    let result = engine.ingest_to_family(TABLE_TAB_ID, &batch);
    assert!(
        result.is_err(),
        "expected error for TABLE_TAB with non-pk-eligible PK column"
    );

    assert!(
        !engine.caches.entity_by_qname.contains_key("public.badpktable"),
        "entity_by_qname must not contain bad table after rejected DDL (dirty before fix)"
    );
    assert!(
        !engine.caches.entity_by_id.contains_key(&tid),
        "entity_by_id must not contain bad table after rejected DDL"
    );
    assert!(
        !engine.dag.tables.contains_key(&tid),
        "dag.tables must not contain bad table after rejected DDL"
    );
    assert_eq!(
        count_records(&mut engine.sys_tables),
        init_rows,
        "sys_tables memtable must have no orphaned row (dirty before fix)"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: TABLE_TAB, duplicate qualified name ───────────────────────────────

#[test]
fn test_table_tab_dup_name_leaves_clean_state() {
    // A raw ingest_to_family with a duplicate qualified name should be
    // rejected.  Pre-fix: apply_entity_by_qname overwrites the cache entry
    // with the new tid.
    let dir = temp_dir("atomicity_dup_name");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), u64_col_def("val")];
    let orig_tid = engine.create_table("public.dupname", &cols, &[0], false).unwrap();
    let init_rows = count_records(&mut engine.sys_tables);

    let new_tid = engine.allocate_table_id();
    engine.write_column_records(new_tid, OWNER_KIND_TABLE, &cols).unwrap();

    let batch = build_table_tab_row(&dir, new_tid, pack_pk_cols(&[0]), "dupname");
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
        count_records(&mut engine.sys_tables),
        init_rows,
        "sys_tables must have no extra orphaned row (dirty before fix)"
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
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let init_rows = count_records(&mut engine.sys_tables);

    let tid = engine.allocate_table_id();
    // Insert columns at indices 0 and 2 — index 1 is absent (gap).
    write_col_at_index(&mut engine, tid, 0, &u64_col_def("id")).unwrap();
    write_col_at_index(&mut engine, tid, 2, &u64_col_def("gapped")).unwrap();

    let batch = build_table_tab_row(&dir, tid, pack_pk_cols(&[0]), "gaptable");
    let result = engine.ingest_to_family(TABLE_TAB_ID, &batch);
    assert!(
        result.is_err(),
        "expected error for TABLE_TAB with non-contiguous column indices (no error before fix)"
    );

    assert!(
        !engine.caches.entity_by_qname.contains_key("public.gaptable"),
        "entity_by_qname must not contain bad table after rejected DDL"
    );
    assert!(
        !engine.dag.tables.contains_key(&tid),
        "dag.tables must not contain bad table after rejected DDL"
    );
    assert_eq!(
        count_records(&mut engine.sys_tables),
        init_rows,
        "sys_tables memtable must have no orphaned row"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: VIEW_TAB, no column records ──────────────────────────────────────

#[test]
fn test_view_tab_no_cols_leaves_clean_state() {
    // hook_view_register fires col_defs.is_empty() after apply_entity_by_qname
    // (pre-fix), leaving entity_by_qname dirty.
    let dir = temp_dir("atomicity_view_no_cols");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let init_rows = count_records(&mut engine.sys_views);

    let vid = engine.allocate_table_id();
    // No column records for vid.
    let batch = build_view_tab_row(vid, "badview", "");
    let result = engine.ingest_to_family(VIEW_TAB_ID, &batch);
    assert!(result.is_err(), "expected error for VIEW_TAB with no column records");

    assert!(
        !engine.caches.entity_by_qname.contains_key("public.badview"),
        "entity_by_qname must not contain bad view after rejected DDL (dirty before fix)"
    );
    assert!(
        !engine.caches.entity_by_id.contains_key(&vid),
        "entity_by_id must not contain bad view after rejected DDL"
    );
    assert!(
        !engine.dag.tables.contains_key(&vid),
        "dag.tables must not contain bad view after rejected DDL"
    );
    assert_eq!(
        count_records(&mut engine.sys_views),
        init_rows,
        "sys_views memtable must have no orphaned row (dirty before fix)"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: VIEW_TAB, over-wide schema rejected cleanly ──────────────────────

#[test]
fn test_view_tab_too_many_cols_rejected() {
    // An over-wide view must be rejected with a clean catalog error that leaves no
    // orphaned cache/memtable state — mirroring the TABLE_TAB path, where the
    // precheck rejects before apply_entity_by_qname mutates the caches.
    // hook_view_register carries the same guard as the build_schema_from_col_defs
    // assert backstop. This is the engine-side counterpart to the client guard in
    // write_circuit_rows.
    let dir = temp_dir("atomicity_view_too_many_cols");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let init_rows = count_records(&mut engine.sys_views);

    let vid = engine.allocate_table_id();
    // MAX_COLUMNS + 1 contiguous column records (col 0 is a valid U64 PK).
    for i in 0..(crate::schema::MAX_COLUMNS as i64 + 1) {
        write_col_at_index(&mut engine, vid, i, &u64_col_def(&format!("c{i}"))).unwrap();
    }
    let batch = build_view_tab_row(vid, "wideview", "");
    let err = engine
        .ingest_to_family(VIEW_TAB_ID, &batch)
        .expect_err("expected error for over-wide view");
    assert!(
        err.contains("columns") && err.contains("max"),
        "expected the column-count guard message, got: {err}"
    );

    assert!(
        !engine.caches.entity_by_qname.contains_key("public.wideview"),
        "entity_by_qname must not contain the rejected view"
    );
    assert!(
        !engine.dag.tables.contains_key(&vid),
        "dag.tables must not contain the rejected view"
    );
    assert_eq!(
        count_records(&mut engine.sys_views),
        init_rows,
        "sys_views memtable must have no orphaned row"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ── Part 1: IDX_TAB, non-existent owner ──────────────────────────────────────

#[test]
fn test_idx_tab_bad_owner_leaves_clean_state() {
    // Pre-fix: apply_index_by_name runs before hook_index_register, so the
    // cache entry is inserted before the hook returns Err for missing owner.
    let dir = temp_dir("atomicity_idx_bad_owner");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let init_rows = count_records(&mut engine.sys_indices);

    let nonexistent_owner = engine.allocate_table_id();
    let idx_id = engine.allocate_index_id();
    let batch = build_idx_tab_row(idx_id, nonexistent_owner, 0, "bad_owner_idx", false);
    let result = engine.ingest_to_family(IDX_TAB_ID, &batch);
    assert!(result.is_err(), "expected error for IDX_TAB with non-existent owner");

    assert!(
        !engine.caches.index_by_name.contains_key("bad_owner_idx"),
        "index_by_name must not contain bad index after rejected DDL (dirty before fix)"
    );
    assert!(
        !engine.caches.index_by_id.contains_key(&idx_id),
        "index_by_id must not contain bad index after rejected DDL"
    );
    assert_eq!(
        count_records(&mut engine.sys_indices),
        init_rows,
        "sys_indices memtable must have no orphaned row (dirty before fix)"
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
    let mut engine = CatalogEngine::open(&dir).unwrap();

    engine
        .create_table("public.base", &[u64_col_def("id")], &[0], true)
        .unwrap();

    // Register a view via the raw system-table path (no circuit needed — the
    // precheck must fire before any backfill).
    let vid = engine.allocate_table_id();
    engine
        .write_column_records(vid, OWNER_KIND_VIEW, &[u64_col_def("id")])
        .unwrap();
    let batch = build_view_tab_row(vid, "vowner", "");
    engine.ingest_to_family(VIEW_TAB_ID, &batch).unwrap();
    assert!(engine.dag.tables.contains_key(&vid), "view registered");

    let init_rows = count_records(&mut engine.sys_indices);
    let idx_id = engine.allocate_index_id();
    let batch = build_idx_tab_row(idx_id, vid, 0, "idx_on_view", false);
    let err = engine
        .ingest_to_family(IDX_TAB_ID, &batch)
        .expect_err("IDX_TAB row naming a view owner must be rejected");
    assert!(
        err.contains("not a base table"),
        "expected the owner-kind guard message, got: {err}"
    );

    assert!(
        !engine.caches.index_by_name.contains_key("idx_on_view"),
        "index_by_name must not contain the rejected index"
    );
    assert!(
        !engine.caches.index_by_id.contains_key(&idx_id),
        "index_by_id must not contain the rejected index"
    );
    assert_eq!(
        count_records(&mut engine.sys_indices),
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
    // Pre-fix: apply_index_by_name overwrites the cache entry with new_idx_id
    // when two IDX_TAB rows carry the same name string.
    let dir = temp_dir("atomicity_idx_dup");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val"), i64_col_def("ts")];
    let tid = engine.create_table("public.idxtest", &cols, &[0], false).unwrap();
    let orig_idx_id = engine.create_index("public.idxtest", &["val"], false).unwrap();
    let init_rows = count_records(&mut engine.sys_indices);

    let orig_name = "public__idxtest__idx_val";
    let new_idx_id = engine.allocate_index_id();
    // Same name, different col (ts at index 2) — bypasses create_index dup check.
    let batch = build_idx_tab_row(new_idx_id, tid, 2, orig_name, false);
    let result = engine.ingest_to_family(IDX_TAB_ID, &batch);
    assert!(
        result.is_err(),
        "expected error: index name '{orig_name}' already exists"
    );

    assert_eq!(
        engine.caches.index_by_name.get(orig_name).copied(),
        Some(orig_idx_id),
        "index_by_name must still point to original index (overwritten before fix)"
    );
    assert!(
        !engine.caches.index_by_id.contains_key(&new_idx_id),
        "new_idx_id must not appear in index_by_id"
    );
    assert_eq!(
        count_records(&mut engine.sys_indices),
        init_rows,
        "sys_indices must have no extra orphaned row (dirty before fix)"
    );

    let _ = fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------------
// Part 2d — CREATE INDEX backfill failure must not leak the index directory
// ---------------------------------------------------------------------------

#[test]
fn test_create_unique_index_backfill_fail_no_dir_leak() {
    // Pre-fix: Table::new creates the index directory before backfill_index
    // checks for duplicates.  When backfill_index fails, the directory is
    // never added to pending_dir_deletions and survives.
    //
    // After Part 2d the directory is pre-staged in pending_dir_deletions
    // before Table::new; the truncation only fires on success.  The test
    // explicitly drains pending_dir_deletions after the error to simulate
    // what compensate_stage_a does in the production executor.
    let dir = temp_dir("atomicity_idx_dir_leak");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.leaktest", &cols, &[0], false).unwrap();
    let schema = engine.get_schema(tid).unwrap();

    // Ingest two rows that share the same 'val' — unique index backfill must fail.
    let mut bb = BatchBuilder::new(schema);
    bb.begin_row(1u128, 1);
    bb.put_u64(42u64);
    bb.end_row();
    bb.begin_row(2u128, 1);
    bb.put_u64(42u64);
    bb.end_row();
    engine.dag.ingest_to_family(tid, bb.finish());
    let _ = engine.dag.flush(tid);

    // Capture the expected index directory before create_index allocates the id.
    let expected_idx_id = engine.next_index_id;
    let owner_dir = format!("{dir}/public/leaktest_{tid}");
    let idx_dir = format!("{owner_dir}/idx_{expected_idx_id}");

    let result = engine.create_index("public.leaktest", &["val"], true);
    assert!(result.is_err(), "unique index with duplicate values must fail");

    // Simulate compensate_stage_a: drain pending_dir_deletions.
    engine.drain_pending_dir_deletions();

    // The index directory must be cleaned up (not orphaned on disk).
    assert!(
        !std::path::Path::new(&idx_dir).exists(),
        "index directory must not leak after failed backfill: {idx_dir} (not cleaned up before fix)"
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
    let mut engine = CatalogEngine::open(&dir).unwrap();

    let cols = vec![u64_col_def("id"), i64_col_def("val")];
    let tid = engine.create_table("public.seqsync", &cols, &[0], false).unwrap();

    // Register an index with an idx_id far ahead of the current counter,
    // simulating a worker receiving a broadcast for a master-allocated ID.
    let large_idx_id = engine.next_index_id + 500;
    let batch = build_idx_tab_row(large_idx_id, tid, 1, "public__seqsync__idx_val_sync", false);
    engine.ingest_to_family(IDX_TAB_ID, &batch).unwrap();

    // next_index_id must now be > large_idx_id so that a local
    // allocate_index_id() never returns large_idx_id again.
    assert!(
        engine.next_index_id > large_idx_id,
        "next_index_id ({}) must exceed the registered idx_id ({}) after \
         hook_index_register (not advanced before fix)",
        engine.next_index_id,
        large_idx_id
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
    // dep_map source. Probing it spuriously matches the unrelated table's
    // dependents and wrongly rejects the DROP SCHEMA.
    //
    // Pre-fix: drop_schema("victim") fails with "View dependency: owner.t".
    let dir = temp_dir("atomicity_schema_id_collision");
    let mut engine = CatalogEngine::open(&dir).unwrap();

    // Table T (in schema `owner`) with a dependent view V → dep_map[T] = [V].
    engine.create_schema("owner").unwrap();
    let tid = engine
        .create_table("owner.t", &[u64_col_def("id")], &[0], true)
        .unwrap();
    let vid = engine.allocate_table_id();
    engine.write_view_deps(vid, &[tid]).unwrap();
    assert_eq!(
        engine.dag.get_dep_map().get(&tid),
        Some(&vec![vid]),
        "precondition: dependency edge T -> V must be present"
    );

    // Allocate filler schemas until the next schema id collides with `tid`,
    // then create the victim schema so that get_schema_id("victim") == tid.
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
        engine.get_schema_id("victim"),
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
