//! The per-PK CAS + net retraction contract on the IDX_TAB and SCHEMA_TAB
//! families, whose drop consumers act on the batch payload (`apply_index_caches`
//! unmaps `index_by_name` by the payload name, `hook_schema_dir` builds its
//! `remove_dir_all` path from it). Two shapes reach them without the contract: a
//! stale `-1` whose row is already gone (`net = -1`) and a `-1` naming a
//! different live row (`net = 0`).
//!
//! The relation families' equivalents are in `alter_tests`; the legitimate DROP
//! paths these guards must not break are in `index_tests` / `fk_tests` /
//! `ddl_tests` / `dir_deletion_tests`.

use super::*;
use gnitz_wire::{IDXTAB_COL_IS_UNIQUE, IDXTAB_COL_NAME, IDXTAB_COL_OWNER_ID, IDXTAB_COL_SOURCE_COLS};
use std::path::Path;

/// A one-row SCHEMA_TAB batch (the family's only payload column is the name).
fn schema_tab_batch(sid: i64, weight: i64, name: &str) -> Batch {
    let mut bb = BatchBuilder::new(SysFamily::Schema.schema());
    bb.begin_row(sid as u128, weight);
    bb.put_string(name);
    bb.end_row();
    bb.finish()
}

/// The live IDX_TAB payload for one index. Named fields rather than a tuple
/// because these tests hand the row around and mutate one field of it, which a
/// positional `.2` makes silently easy to get wrong.
#[derive(Clone)]
struct IdxRow {
    owner_id: i64,
    source_cols: u64,
    name: String,
    is_unique: bool,
}

fn live_index_row(engine: &CatalogEngine, idx_id: i64) -> IdxRow {
    let schema = SysFamily::Index.schema();
    let mut c = engine.sys_store(SysFamily::Index).open_cursor();
    assert!(
        c.advance_to_exact_live(sys_opk(&schema, idx_id as u128).pk_bytes()),
        "live IDX_TAB row for index {idx_id} missing"
    );
    IdxRow {
        owner_id: cursor_read_u64(&c, IDXTAB_COL_OWNER_ID) as i64,
        source_cols: cursor_read_u64(&c, IDXTAB_COL_SOURCE_COLS),
        name: cursor_read_string(&c, IDXTAB_COL_NAME),
        is_unique: cursor_read_u64(&c, IDXTAB_COL_IS_UNIQUE) != 0,
    }
}

/// A one-row IDX_TAB batch at `weight` reproducing `row` — what a client's
/// read-then-push drop helper builds.
fn idx_row_batch(idx_id: i64, weight: i64, row: &IdxRow) -> Batch {
    idx_tab_batch(idx_id, row.owner_id, row.source_cols, &row.name, row.is_unique, weight)
}

/// Every stored weight under `idx_id` in IDX_TAB: empty once a `(+1, -1)` pair
/// has cancelled (the cursor skips a net-zero PK), `[1]` for a live index,
/// `[-1]` for a durable ghost.
fn idx_weights_for(engine: &CatalogEngine, idx_id: i64) -> Vec<i64> {
    let mut c = engine.sys_store(SysFamily::Index).open_cursor();
    let mut v = Vec::new();
    while c.valid {
        if c.current_key_narrow() as i64 == idx_id {
            v.push(c.current_weight);
        }
        c.advance();
    }
    v
}

// ── SCHEMA_TAB ──────────────────────────────────────────────────────────────

#[test]
fn stale_schema_retraction_spares_the_live_schemas_directory() {
    // A resolved `s` to its id; B dropped and recreated `s`; A's `-1` for the
    // dead id then lands carrying the name `s` — now the live schema's. Without
    // the net check the deletion queue takes `<dir>/s`.
    let dir = temp_dir("sysretract_stale_schema");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64)];

    engine.create_schema("s").unwrap();
    let old_sid = engine.get_schema_id("s");
    engine.drop_schema("s").unwrap();
    engine.defer_pending_dir_deletions(); // the DROP-success path
    engine.create_schema("s").unwrap();
    let new_sid = engine.get_schema_id("s");
    assert_ne!(old_sid, new_sid, "the recreate must allocate a fresh id");
    let tid = engine.create_table("s.t", &cols, &[0]).unwrap();
    let tbl_dir = format!("{dir}/s/t_{tid}");
    assert!(Path::new(&tbl_dir).exists());

    let err = engine
        .ingest_to_family(SCHEMA_TAB_ID, &schema_tab_batch(old_sid, -1, "s"))
        .unwrap_err();
    assert!(
        err.contains("catalog changed concurrently"),
        "a `-1` for a schema id that is already gone must be rejected: {err}"
    );

    assert!(
        engine.pending_dir_deletions.is_empty(),
        "a rejected drop must queue no directory deletion"
    );
    engine.drain_pending_dir_deletions();
    engine.drain_checkpoint_gated_deletions();
    assert!(
        Path::new(&tbl_dir).exists(),
        "the live schema's table directory must survive both drains"
    );
    assert_eq!(engine.get_by_name("s", "t"), Some(tid));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn schema_retraction_under_another_schemas_name_rejected() {
    // `net = 0`, so only the CAS catches it — the member-count guard counts
    // members of the retracted (empty) id, not of the named schema.
    let dir = temp_dir("sysretract_schema_mismatch");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64)];

    engine.create_schema("a").unwrap();
    engine.create_schema("b").unwrap();
    let sid_a = engine.get_schema_id("a");
    let tid = engine.create_table("b.t", &cols, &[0]).unwrap();
    let b_dir = format!("{dir}/b");

    let err = engine
        .ingest_to_family(SCHEMA_TAB_ID, &schema_tab_batch(sid_a, -1, "b"))
        .unwrap_err();
    assert!(
        err.contains("catalog changed concurrently"),
        "a `-1` on schema a's id carrying schema b's name must be rejected: {err}"
    );

    assert!(engine.has_schema("a") && engine.has_schema("b"));
    assert_eq!(engine.get_by_name("b", "t"), Some(tid));
    assert!(
        engine.pending_dir_deletions.is_empty(),
        "b's directory must never be queued for deletion"
    );
    engine.drain_pending_dir_deletions();
    assert!(Path::new(&b_dir).exists());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── IDX_TAB ─────────────────────────────────────────────────────────────────

/// The accepted path, to the tests below's rejected one: `drop_index` builds its
/// `-1` by copying the live row, so the pair must consolidate away and leave no
/// stored row at all.
#[test]
fn create_then_drop_unique_index_cancels_to_empty() {
    let dir = temp_dir("idx_create_drop_cancels");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("pk", type_code::U64), col_def("val", type_code::I64)];
    engine.create_table("public.cancels", &cols, &[0]).unwrap();
    let idx_id = engine.create_index("public.cancels", &["val"], true).unwrap();
    assert_eq!(
        idx_weights_for(&engine, idx_id),
        vec![1],
        "the create leaves one live row"
    );

    engine
        .drop_index(&make_secondary_index_name("public", "cancels", "val"))
        .unwrap();
    assert!(
        idx_weights_for(&engine, idx_id).is_empty(),
        "the drop's `-1` must cancel the create's `+1`, leaving no stored row"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn stale_index_retraction_leaves_no_ghost_row() {
    let (mut engine, _tid, dir) = table_fixture(
        "sysretract_stale_index",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    let idx = engine.create_index("public.t", &["val"], false).unwrap();
    let row = live_index_row(&engine, idx);
    engine.drop_index(&row.name).unwrap();

    let err = engine
        .ingest_to_family(IDX_TAB_ID, &idx_row_batch(idx, -1, &row))
        .unwrap_err();
    assert!(
        err.contains("catalog changed concurrently"),
        "a `-1` for an index that is already net-dead must be rejected: {err}"
    );
    assert!(
        idx_weights_for(&engine, idx).is_empty(),
        "the rejected `-1` must leave no negative-weight row in sys_indices"
    );
    assert!(!engine.caches.index_by_name.contains_key(&row.name));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn stale_index_retraction_after_recreate_keeps_the_live_index_nameable() {
    // The stale `-1` unmaps `index_by_name` by its payload name, which now
    // belongs to the recreated index — stranding a live index no client can name
    // and freeing that name for a third row.
    let (mut engine, _tid, dir) = table_fixture(
        "sysretract_index_recreate",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    let idx1 = engine.create_index("public.t", &["val"], false).unwrap();
    let row1 = live_index_row(&engine, idx1);
    engine.drop_index(&row1.name).unwrap();
    let idx2 = engine.create_index("public.t", &["val"], false).unwrap();
    assert_ne!(idx1, idx2, "the recreate must allocate a fresh index id");

    let err = engine
        .ingest_to_family(IDX_TAB_ID, &idx_row_batch(idx1, -1, &row1))
        .unwrap_err();
    assert!(
        err.contains("catalog changed concurrently"),
        "a `-1` for the dropped index's id must be rejected: {err}"
    );

    assert_eq!(
        engine.caches.index_by_name.get(&row1.name),
        Some(&idx2),
        "the live index must keep its name mapping"
    );
    assert!(idx_weights_for(&engine, idx1).is_empty());
    assert!(
        engine.create_index("public.t", &["val"], false).is_err(),
        "a second index under that name must still be refused"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn index_retraction_under_another_indexs_name_rejected() {
    let (mut engine, _tid, dir) = table_fixture(
        "sysretract_index_mismatch",
        &[
            col_def("id", type_code::U64),
            col_def("a", type_code::U64),
            col_def("b", type_code::U64),
        ],
    );
    let i1 = engine.create_index("public.t", &["a"], false).unwrap();
    let i2 = engine.create_index("public.t", &["b"], false).unwrap();
    let row1 = live_index_row(&engine, i1);
    let row2 = live_index_row(&engine, i2);

    // i1's PK and payload, but i2's name — `net = 0`, so only the CAS sees it.
    let mut mismatched = row1.clone();
    mismatched.name = row2.name.clone();
    let err = engine
        .ingest_to_family(IDX_TAB_ID, &idx_row_batch(i1, -1, &mismatched))
        .unwrap_err();
    assert!(
        err.contains("catalog changed concurrently"),
        "a `-1` on i1's id carrying i2's name must be rejected: {err}"
    );

    assert_eq!(engine.caches.index_by_name.get(&row1.name), Some(&i1));
    assert_eq!(engine.caches.index_by_name.get(&row2.name), Some(&i2));
    assert_eq!(idx_weights_for(&engine, i1), vec![1]);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── Both families: the duplicate live head ──────────────────────────────────

#[test]
fn duplicate_live_head_rejected_for_index_and_schema() {
    // The sys stores run no `enforce_unique_pk`, so nothing but the net stops a
    // re-ingested `+1` from leaving two live heads under one PK.
    let (mut engine, _tid, dir) = table_fixture(
        "sysretract_dup_head",
        &[col_def("id", type_code::U64), col_def("val", type_code::U64)],
    );
    let idx = engine.create_index("public.t", &["val"], false).unwrap();
    let row = live_index_row(&engine, idx);
    let err = engine
        .ingest_to_family(IDX_TAB_ID, &idx_row_batch(idx, 1, &row))
        .unwrap_err();
    assert!(
        err.contains("net weight 2"),
        "a duplicate live IDX_TAB head must be rejected: {err}"
    );
    assert_eq!(idx_weights_for(&engine, idx), vec![1]);

    engine.create_schema("s").unwrap();
    let sid = engine.get_schema_id("s");
    let err = engine
        .ingest_to_family(SCHEMA_TAB_ID, &schema_tab_batch(sid, 1, "s"))
        .unwrap_err();
    assert!(
        err.contains("net weight 2"),
        "a duplicate live SCHEMA_TAB head must be rejected: {err}"
    );
    assert!(engine.has_schema("s"));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
