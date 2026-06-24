mod atomicity_tests;
mod compound_pk_smoke;
mod ddl_tests;
mod dir_deletion_tests;
mod engine_tests;
mod fk_tests;
mod index_tests;
mod reopen_rebuild_tests;
mod uuid_tests;
mod wide_pk_validation;

use super::sys_tables::*;
use super::*;

use std::fs;

fn temp_dir(name: &str) -> String {
    crate::foundation::posix_io::raise_fd_limit_for_tests();
    // Namespace the fixed path by user: /tmp is shared and sticky, so a dir
    // left by a DIFFERENT user (mode 0755, owner-only-writable) would occupy
    // the bare `gnitz_catalog_test_{name}` path forever — the start-of-test
    // `remove_dir_all` cannot delete it (sticky bit) and `CatalogEngine::open`
    // then fails EACCES creating subdirs under it. Per-user namespacing keeps
    // each user on a path they own (so the remove-at-start self-cleans the
    // previous same-user run — no turd accumulation), while never colliding
    // across users. Falls back to the pid if $USER is unset.
    let owner = std::env::var("USER").unwrap_or_else(|_| std::process::id().to_string());
    let path = std::env::temp_dir()
        .join(format!("gnitz_catalog_test_{owner}_{name}"))
        .to_str()
        .unwrap()
        .to_owned();
    let _ = fs::remove_dir_all(&path);
    path
}

fn u64_col_def(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        type_code: type_code::U64,
        is_nullable: false,
        fk_table_id: 0,
        fk_col_idx: 0,
    }
}
fn i64_col_def(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        type_code: type_code::I64,
        is_nullable: false,
        fk_table_id: 0,
        fk_col_idx: 0,
    }
}
fn u8_col_def(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        type_code: type_code::U8,
        is_nullable: false,
        fk_table_id: 0,
        fk_col_idx: 0,
    }
}
fn u16_col_def(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        type_code: type_code::U16,
        is_nullable: false,
        fk_table_id: 0,
        fk_col_idx: 0,
    }
}
fn i32_col_def(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        type_code: type_code::I32,
        is_nullable: false,
        fk_table_id: 0,
        fk_col_idx: 0,
    }
}
fn str_col_def(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        type_code: type_code::STRING,
        is_nullable: false,
        fk_table_id: 0,
        fk_col_idx: 0,
    }
}
fn u128_col_def(name: &str) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        type_code: type_code::U128,
        is_nullable: false,
        fk_table_id: 0,
        fk_col_idx: 0,
    }
}

fn count_records(table: &mut Table) -> usize {
    let mut count = 0;
    let mut c = table.open_cursor();
    while c.cursor.valid {
        if c.cursor.current_weight > 0 {
            count += 1;
        }
        c.cursor.advance();
    }
    count
}

/// Build a raw TABLE_TAB row for tests that drive the catalog applier or
/// hook layer directly (bypassing `create_table`). `dir` only feeds the
/// stored directory string — the path is never actually created on disk.
fn build_table_tab_row(dir: &str, tid: i64, raw_pk_cols: u64, table_name: &str) -> Batch {
    let mut bb = BatchBuilder::new(table_tab_schema());
    bb.begin_row(tid as u128, 1);
    bb.put_u64(PUBLIC_SCHEMA_ID as u64);
    bb.put_string(table_name);
    bb.put_string(&format!("{dir}/public/{table_name}"));
    bb.put_u64(raw_pk_cols);
    bb.put_u64(0); // created_lsn
    bb.put_u64(0); // flags
    bb.end_row();
    bb.finish()
}

/// Build the minimal identity circuit `ScanDelta(base) → Integrate` for
/// `vid` and write its rows through the applied-delta path. The payload
/// column layout follows `gnitz_wire::CIRCUIT_NODES_COLS` /
/// `CIRCUIT_EDGES_COLS`; the compound PK `(view_id, sub)` is packed by
/// `pack_view_pk`.
fn write_identity_circuit(engine: &mut CatalogEngine, vid: i64, base_tid: i64) {
    let nodes_schema = sys_tab_schema(CIRCUIT_NODES_TAB_ID);
    let mut bb = BatchBuilder::new(nodes_schema);
    // node 0: ScanDelta(base_tid)
    bb.begin_row(pack_view_pk(vid, 0), 1);
    bb.put_u64(0);
    bb.put_u64(gnitz_wire::OPCODE_SCAN_DELTA);
    bb.put_u64(base_tid as u64); // source_table
    bb.put_null(); // expr_program
    bb.end_row();
    // node 1: Integrate (terminal sink — moves the delta into the view store)
    bb.begin_row(pack_view_pk(vid, 1), 1);
    bb.put_u64(1);
    bb.put_u64(gnitz_wire::OPCODE_INTEGRATE);
    bb.put_null(); // source_table
    bb.put_null(); // expr_program
    bb.end_row();
    engine.ingest_to_family(CIRCUIT_NODES_TAB_ID, &bb.finish()).unwrap();

    let edges_schema = sys_tab_schema(CIRCUIT_EDGES_TAB_ID);
    let mut bb = BatchBuilder::new(edges_schema);
    bb.begin_row(pack_view_pk(vid, 0), 1);
    bb.put_u64(1); // dst_node
    bb.put_u64(gnitz_wire::PORT_IN); // dst_port
    bb.put_u64(0); // src_node
    bb.end_row();
    engine.ingest_to_family(CIRCUIT_EDGES_TAB_ID, &bb.finish()).unwrap();
}

/// Build a raw VIEW_TAB row for tests that register a view via the raw
/// system-table path. `sql` is stored verbatim; cache_directory is left empty
/// (the register hook computes the real view directory itself and neither
/// column is read back by the appliers). The bare `0` pk_col_idx decodes back
/// to a single-column PK `[0]`.
fn build_view_tab_row(vid: i64, view_name: &str, sql: &str) -> Batch {
    let mut bb = BatchBuilder::new(view_tab_schema());
    bb.begin_row(vid as u128, 1);
    bb.put_u64(PUBLIC_SCHEMA_ID as u64);
    bb.put_string(view_name);
    bb.put_string(sql);
    bb.put_string(""); // cache_directory
    bb.put_u64(0); // created_lsn
    bb.put_u64(0); // pk_col_idx
    bb.end_row();
    bb.finish()
}
