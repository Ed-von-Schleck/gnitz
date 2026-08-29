//! ALTER (rename) catalog mechanics driven directly at the `CatalogEngine`
//! layer: the reconciling register hooks (a rename pair fires no cascade / no
//! dir deletion), the post-image retraction contract (CAS, per-PK net,
//! system-range rewrite guard) exercised with names longer than 12 bytes so the
//! German-string blob heap is on the CAS path, and the id-only directory resume
//! across a reopen. The end-to-end SQL surface is in
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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

// ── Id-only dirs resume after a rename + reopen ─────────────────────────────

#[test]
fn rename_then_reopen_resolves_flushed_data() {
    let dir = temp_dir("alter_reopen");
    let tid;
    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        let cols = vec![col_def("id", type_code::U64), col_def("v", type_code::U64)];
        tid = engine.create_table("public.orig", &cols, &[0]).unwrap();
        // Flush a row so only the on-disk (id-only) path can serve it after reopen.
        let schema = engine.get_schema_desc(tid).unwrap();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(7u128, 1);
        bb.put_u64(70);
        bb.end_row();
        engine.ingest_to_family(tid, &bb.finish()).unwrap();
        engine.dag_mut().flush(tid).unwrap();

        let pair = table_rename_pair(&engine, tid, "renamed");
        engine.ingest_to_family(TABLE_TAB_ID, &pair).unwrap();
        engine.close();
    }

    // Reopen: boot replay re-registers at the id-only path `t_{tid}` (unchanged by
    // the rename), so the flushed row still resolves under the new name.
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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

// ── The retraction contract, exercised with > 12-byte names ────────────────

#[test]
fn valid_long_name_rename_accepted() {
    // Regression against a naive region `memcmp`: a name > 12 bytes lives in the
    // blob heap, so the `-1` and live rows carry independent heap offsets. The CAS
    // compares German-string content, so a valid rename to a long name is accepted.
    let dir = temp_dir("alter_long_ok");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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

/// The guard keys on the id space, not the batch shape: every sign is rejected,
/// on both relation families, at an id the SQL layer cannot even name. Each
/// batch reproduces the bootstrap row byte-for-byte where it carries a `-1`, so
/// the CAS would pass and only the id-space guard can stop it.
#[test]
fn system_range_mutations_rejected() {
    let dir = temp_dir("alter_sysrange");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let table_drop = {
        let mut bb = BatchBuilder::new(SysFamily::Table.schema());
        push_table_tab_row(&mut bb, IDX_TAB_ID, SYSTEM_SCHEMA_ID, "_indices", 0, 0, -1);
        bb.finish()
    };
    let table_rename = {
        let mut bb = BatchBuilder::new(SysFamily::Table.schema());
        push_table_tab_row(&mut bb, IDX_TAB_ID, SYSTEM_SCHEMA_ID, "_indices", 0, 0, -1);
        push_table_tab_row(&mut bb, IDX_TAB_ID, SYSTEM_SCHEMA_ID, "renamed", 0, 0, 1);
        bb.finish()
    };
    let view_rename = {
        let mut bb = BatchBuilder::new(SysFamily::View.schema());
        push_view_tab_row(&mut bb, -1, IDX_TAB_ID, "a", "", 0);
        push_view_tab_row(&mut bb, 1, IDX_TAB_ID, "b", "", 0);
        bb.finish()
    };
    // A bare `+1` on VIEW_TAB at a system TABLE_TAB id: VIEW_TAB holds no live
    // row there, so the net test passes and the caches would alias
    // `_system._indices` away.
    let view_create = build_view_tab_row(IDX_TAB_ID, "v", "SELECT 1");

    let members_before = engine.schema_member_count(PUBLIC_SCHEMA_ID);
    for (family, batch, verb, noun) in [
        (SysFamily::Table, &table_drop, "DROP", "table"),
        (SysFamily::Table, &table_rename, "ALTER", "table"),
        (SysFamily::View, &view_rename, "ALTER", "view"),
        (SysFamily::View, &view_create, "CREATE", "view"),
    ] {
        let err = engine.ingest_to_family(family.id(), batch).unwrap_err();
        assert!(err.contains(&format!("cannot {verb} a system {noun}")), "{err}");
    }

    // Nothing was torn down or aliased on the way to the reject.
    for family in SysFamily::ALL {
        assert!(
            engine.dag.tables.contains_key(&family.id()),
            "{} unregistered",
            family.name()
        );
    }
    assert!(engine.pending_dir_deletions.is_empty());
    assert_eq!(
        engine.caches.entity_by_id.get(&IDX_TAB_ID),
        Some(&("_system".to_string(), "_indices".to_string())),
        "the system relation must still resolve under its own name"
    );
    assert_eq!(engine.caches.entity_by_qname.get("public.v"), None);
    assert_eq!(engine.schema_member_count(PUBLIC_SCHEMA_ID), members_before);

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// The same guard on the schema id space: `public` and `_system` are bootstrap
/// rows, so no DROP or rename of one passes.
#[test]
fn system_range_schema_mutations_rejected() {
    let dir = temp_dir("alter_sysrange_schema");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    for sid in [SYSTEM_SCHEMA_ID, PUBLIC_SCHEMA_ID] {
        let mut bb = BatchBuilder::new(SysFamily::Schema.schema());
        bb.begin_row(sid as u128, -1);
        bb.put_string("gone");
        bb.end_row();
        let err = engine.ingest_to_family(SCHEMA_TAB_ID, &bb.finish()).unwrap_err();
        assert!(err.contains("cannot DROP a system schema"), "{err}");
        assert!(engine.caches.schema_by_id.contains_key(&sid), "schema {sid} dropped");
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── COL_TAB: the column retraction contract ─────────────────────────────────

#[test]
fn stale_column_rename_rejected_and_drop_cascade_passes() {
    let dir = temp_dir("alter_col");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
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

// ── The column-ALTER owner guard covers every rewrite pair ──────────────────

fn rename_to(new_name: &str) -> impl FnOnce(&mut ColumnDef) + '_ {
    move |c: &mut ColumnDef| c.name = new_name.to_string()
}

/// RENAME COLUMN on a view and on a system table — neither is a user base
/// table, and the SQL planner is not the trust boundary that stops them.
#[test]
fn column_rename_on_non_base_owner_rejected() {
    let dir = temp_dir("alter_col_owner");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let vid = register_identity_view(&mut engine, tid, "v", &cols);

    let err = engine
        .precheck_family(
            SysFamily::Column,
            &col_alter_pair(vid, OWNER_KIND_VIEW, 0, &cols[0], rename_to("id2")),
        )
        .unwrap_err();
    assert!(err.contains("not a user base table"), "view owner: {err}");

    // A system table's own COL_TAB self-description row, read back from what
    // bootstrap wrote so the `-1` cannot drift from it.
    let sys_col = engine.scan_column_defs(IDX_TAB_ID, true).unwrap().swap_remove(0);
    let err = engine
        .precheck_family(
            SysFamily::Column,
            &col_alter_pair(IDX_TAB_ID, OWNER_KIND_TABLE, 0, &sys_col, rename_to("renamed")),
        )
        .unwrap_err();
    assert!(err.contains("not a user base table"), "system owner: {err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// The two guards that stay inside `is_drop`: a rename is legal on a PK column
/// and on a table with dependent views (views bind columns by ordinal).
#[test]
fn column_rename_on_pk_column_and_with_dependent_views_accepted() {
    let dir = temp_dir("alter_col_rename_ok");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();

    let cols = vec![col_def("id", type_code::U64), col_def("val", type_code::U64)];
    let tid = engine.create_table("public.t", &cols, &[0]).unwrap();
    let vid = register_identity_view(&mut engine, tid, "v", &cols);
    assert_eq!(
        engine.dag.get_dep_map().get(&tid),
        Some(&vec![vid]),
        "precondition: the table has a dependent view"
    );

    for (col_idx, new_name) in [(0usize, "id2"), (1, "val2")] {
        engine
            .precheck_family(
                SysFamily::Column,
                &col_alter_pair(
                    tid,
                    OWNER_KIND_TABLE,
                    col_idx as i64,
                    &cols[col_idx],
                    rename_to(new_name),
                ),
            )
            .expect("renaming a column with dependent views is legal");
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── The hook canonicalizer repairs a `+1`-first rewrite pair ────────────────
// Every forward producer emits a rename pair `-1`-first; `compensate_stage_a`'s
// in-place negation is what reverses one. The payload-keyed cache folds read the
// *outgoing* name off `entity_by_id`, so an insert-first pair applied verbatim
// would unmap the new name and leave the old one resolving.

#[test]
fn insert_first_rename_pair_lands_the_new_name() {
    let dir = temp_dir("alter_insert_first_pair");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let cols = vec![col_def("id", type_code::U64), col_def("v", type_code::U64)];
    let tid = engine.create_table("public.orig", &cols, &[0]).unwrap();

    let row = live_table_row(&engine, tid);
    let mut bb = BatchBuilder::new(SysFamily::Table.schema());
    for (weight, name) in [(1i64, "renamed"), (-1i64, row.name.as_str())] {
        push_table_row(&mut bb, tid, &row, name, weight);
    }
    engine.ingest_to_family(TABLE_TAB_ID, &bb.finish()).unwrap();

    assert!(
        engine.caches.entity_by_qname.contains_key("public.renamed"),
        "the new name must resolve"
    );
    assert!(
        !engine.caches.entity_by_qname.contains_key("public.orig"),
        "the outgoing name must be unmapped"
    );
    assert_eq!(
        engine.caches.entity_by_id.get(&tid).map(|(_, n)| n.as_str()),
        Some("renamed"),
        "entity_by_id must carry the new name"
    );
    assert!(
        engine.dag.tables.contains_key(&tid),
        "a rename must not unregister the table, whatever the row order"
    );
    assert!(
        engine.pending_dir_deletions.is_empty(),
        "a rename queues no dir deletion"
    );
    assert_eq!(live_rows_for(&engine, tid), 1, "a rename leaves exactly one live row");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
