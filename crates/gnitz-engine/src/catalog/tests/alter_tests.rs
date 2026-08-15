//! ALTER (rename) catalog mechanics driven directly at the `CatalogEngine`
//! layer: the reconciling register hooks (a rename pair fires no cascade / no
//! dir deletion), the §3.3 post-image retraction contract (CAS, per-PK net,
//! system-range rewrite guard) exercised with names longer than 12 bytes so the
//! German-string blob heap is on the CAS path, and the id-only directory resume
//! (§4) across a reopen. The end-to-end SQL surface is in
//! `crates/gnitz-sql/tests/planner_alter.rs`.

use super::*;
use gnitz_wire::{TABTAB_COL_FLAGS, TABTAB_COL_NAME, TABTAB_COL_PK_COL_IDX, TABTAB_COL_SCHEMA_ID};
use std::path::Path;

/// The live TABLE_TAB row's payload for `tid`. Named fields rather than a tuple
/// because these tests rebuild the row with one field changed.
struct TableRow {
    schema_id: u64,
    name: String,
    pk_col_idx: u64,
    flags: u64,
}

fn live_table_row(engine: &CatalogEngine, tid: i64) -> TableRow {
    let mut c = engine.sys_store(SysFamily::Table).open_cursor();
    c.seek_bytes(&(tid as u64).to_be_bytes());
    assert!(
        c.valid && c.current_key_narrow() as u64 == tid as u64,
        "live TABLE_TAB row for tid {tid} missing"
    );
    TableRow {
        schema_id: cursor_read_u64(&c, TABTAB_COL_SCHEMA_ID),
        name: cursor_read_string(&c, TABTAB_COL_NAME),
        pk_col_idx: cursor_read_u64(&c, TABTAB_COL_PK_COL_IDX),
        flags: cursor_read_u64(&c, TABTAB_COL_FLAGS),
    }
}

/// One TABLE_TAB row at `weight` reproducing `row`, with `name` substituted.
fn push_table_row(bb: &mut BatchBuilder, tid: i64, row: &TableRow, name: &str, weight: i64) {
    push_table_tab_row(bb, tid, row.schema_id as i64, name, row.pk_col_idx, row.flags, weight);
}

/// A TABLE_TAB rename pair: `-1` reproduces the live payload byte-for-byte (so
/// the CAS accepts it), `+1` differs only in `name`.
fn table_rename_pair(engine: &CatalogEngine, tid: i64, new_name: &str) -> Batch {
    let row = live_table_row(engine, tid);
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    for (weight, name) in [(-1i64, row.name.as_str()), (1i64, new_name)] {
        push_table_row(&mut bb, tid, &row, name, weight);
    }
    bb.finish()
}

/// Count live positive-weight rows for `tid` in the TABLE_TAB store — 1 for a
/// clean rename, 2+ for a persistent ghost.
fn live_rows_for(engine: &CatalogEngine, tid: i64) -> usize {
    let mut c = engine.sys_store(SysFamily::Table).open_cursor();
    let mut n = 0;
    while c.valid {
        if c.current_key_narrow() as u64 == tid as u64 && c.current_weight > 0 {
            n += 1;
        }
        c.advance();
    }
    n
}

// ── Reconciling hooks: a rename fires no teardown ───────────────────────────

#[test]
fn rename_fires_no_cascade_and_leaves_dir_untouched() {
    let dir = temp_dir("alter_no_cascade");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("v", type_code::U64)];
    let tid = engine.create_table("public.orig", &cols, &[0]).unwrap();
    let table_path = format!("{dir}/public/t_{tid}");
    assert!(Path::new(&table_path).exists());

    let pair = table_rename_pair(&engine, tid, "renamed");
    engine.ingest_to_family(TABLE_TAB_ID, &pair).unwrap();

    // The registration survives; no directory is queued or removed.
    assert!(
        engine.dag.tables.contains_key(&tid),
        "rename must not unregister the table"
    );
    assert!(Path::new(&table_path).exists(), "rename must not delete the table dir");
    assert!(
        engine.pending_dir_deletions.is_empty(),
        "rename must queue no dir deletion (reconciling hooks no-op the registration)"
    );
    // Caches reflect the new name; no persistent ghost row.
    assert!(engine.caches.entity_by_qname.contains_key("public.renamed"));
    assert!(!engine.caches.entity_by_qname.contains_key("public.orig"));
    assert_eq!(
        live_rows_for(&engine, tid),
        1,
        "a rename must leave exactly one live row"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── §4: id-only dirs resume after a rename + reopen ─────────────────────────

#[test]
fn rename_then_reopen_resolves_flushed_data() {
    let dir = temp_dir("alter_reopen");
    let tid;
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let cols = vec![col_def("id", type_code::U64), col_def("v", type_code::U64)];
        tid = engine.create_table("public.orig", &cols, &[0]).unwrap();
        // Flush a row so only the on-disk (id-only) path can serve it after reopen.
        let schema = engine.get_schema_desc(tid).unwrap();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(7u128, 1);
        bb.put_u64(70);
        bb.end_row();
        engine.ingest_to_family(tid, &bb.finish()).unwrap();
        engine.flush_family(tid).unwrap();

        let pair = table_rename_pair(&engine, tid, "renamed");
        engine.ingest_to_family(TABLE_TAB_ID, &pair).unwrap();
        engine.close();
    }

    // Reopen: boot replay re-registers at the id-only path `t_{tid}` (unchanged by
    // the rename), so the flushed row still resolves under the new name.
    let mut engine = CatalogEngine::open(&dir).unwrap();
    assert_eq!(
        engine.caches.entity_by_qname.get("public.renamed").copied(),
        Some(tid),
        "renamed relation must resolve after reopen"
    );
    assert!(
        engine.seek_family(tid, 7u128, &[]).unwrap().0.is_some(),
        "flushed row must resolve after rename + reopen (id-only dir was untouched)"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── §3.3(A): the retraction contract, exercised with > 12-byte names ────────

#[test]
fn valid_long_name_rename_accepted() {
    // Regression against a naive region `memcmp`: a name > 12 bytes lives in the
    // blob heap, so the `-1` and live rows carry independent heap offsets. The CAS
    // compares German-string content, so a valid rename to a long name is accepted.
    let dir = temp_dir("alter_long_ok");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64)];
    let tid = engine.create_table("public.original_long_name", &cols, &[0]).unwrap();
    let pair = table_rename_pair(&engine, tid, "renamed_to_a_long_name");
    engine.ingest_to_family(TABLE_TAB_ID, &pair).unwrap();
    assert!(engine
        .caches
        .entity_by_qname
        .contains_key("public.renamed_to_a_long_name"));
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn stale_snapshot_rename_rejected_long_name() {
    let dir = temp_dir("alter_stale");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64)];
    let tid = engine.create_table("public.original_long_name", &cols, &[0]).unwrap();
    // The `-1` carries a stale (wrong) old name > 12 bytes that does not match the
    // live row — the CAS must reject it.
    let row = live_table_row(&engine, tid);
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    for (weight, name) in [(-1i64, "stale_wrong_long_name"), (1i64, "new_desired_long_name")] {
        push_table_row(&mut bb, tid, &row, name, weight);
    }
    let err = engine.ingest_to_family(TABLE_TAB_ID, &bb.finish()).unwrap_err();
    assert!(
        err.contains("catalog changed concurrently"),
        "a stale-snapshot rename must be rejected: {err}"
    );
    // The catalog is unchanged (still the original name, one live row).
    assert!(engine.caches.entity_by_qname.contains_key("public.original_long_name"));
    assert_eq!(live_rows_for(&engine, tid), 1);
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn duplicate_live_head_rejected() {
    let dir = temp_dir("alter_dup_head");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![col_def("id", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    // A bare `+1` re-ingest of the live row → net weight 2 (a duplicate live head).
    let row = live_table_row(&engine, tid);
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    push_table_row(&mut bb, tid, &row, &row.name, 1);
    let err = engine.ingest_to_family(TABLE_TAB_ID, &bb.finish()).unwrap_err();
    assert!(
        err.contains("net weight 2") || err.contains("expected 0 or 1"),
        "a duplicate live head must be rejected: {err}"
    );
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

#[test]
fn system_range_rewrite_rejected() {
    let dir = temp_dir("alter_sysrange");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    // A rewrite pair on a system-range tid, submitted directly to precheck_family
    // (unreachable via the SQL layer). The system-range guard fires before the CAS.
    let sys_tid: i64 = 5; // < FIRST_USER_TABLE_ID
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    for (weight, name) in [(-1i64, "a"), (1i64, "b")] {
        push_table_tab_row(&mut bb, sys_tid, PUBLIC_SCHEMA_ID, name, pack_pk_cols(&[0]), 0, weight);
    }
    let err = engine.precheck_family(SysFamily::Table, &bb.finish()).unwrap_err();
    assert!(
        err.contains("system relation"),
        "a rewrite on a system-range id must be rejected: {err}"
    );
    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── §3.3(A) for COL_TAB: the column retraction contract ─────────────────────

#[test]
fn stale_column_rename_rejected_and_drop_cascade_passes() {
    let dir = temp_dir("alter_col");
    let mut engine = CatalogEngine::open(&dir).unwrap();
    let cols = vec![
        col_def("id", type_code::U64),
        col_def("original_column_name", type_code::U64),
    ];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let col_idx: i64 = 1;

    // A COL_TAB rewrite pair whose `-1` carries a stale (wrong) old column name
    // > 12 bytes — the Column precheck arm must reject it via the CAS.
    let mut bb = BatchBuilder::new(SysFamily::Column.schema());
    for (weight, name) in [(-1i64, "stale_wrong_column_x"), (1i64, "new_column_name_here")] {
        push_col_tab_row(
            &mut bb,
            tid,
            OWNER_KIND_TABLE,
            col_idx,
            &col_def(name, type_code::U64),
            weight,
        );
    }
    let err = engine.precheck_family(SysFamily::Column, &bb.finish()).unwrap_err();
    assert!(
        err.contains("catalog changed concurrently"),
        "a stale column rename must be rejected by the Column precheck arm: {err}"
    );

    // A DROP TABLE cascade (unpaired COL `-1`s with the full live payload) still
    // passes the Column arm — the drop succeeds and removes the columns.
    let cols_before = count_records(engine.sys_store_mut(SysFamily::Column));
    engine.drop_table("public.t").unwrap();
    let cols_after = count_records(engine.sys_store_mut(SysFamily::Column));
    assert_eq!(
        cols_after,
        cols_before - 2,
        "DROP TABLE must cascade-retract both columns"
    );

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
