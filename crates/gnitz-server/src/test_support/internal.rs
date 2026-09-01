//! The catalog-level test helpers — the store-level ones are `gnitz-store`'s
//! `shared` file, compiled here through a `#[path]`.
//!
//! This file is compiled once, and only inside this crate, so it names
//! crate-internals as `crate::` and widens no API — nothing links this crate.

use crate::catalog::{CatalogEngine, ColumnDef, SysFamily, PUBLIC_SCHEMA_ID};
use gnitz_store::storage::{BatchBuilder, ReadCursor};
use gnitz_wire::sys_rows::{
    write_circuit_edge_row, write_circuit_node_row, write_col_tab_row, write_table_tab_row, write_view_tab_row,
    CircuitEdgeRow, CircuitNodeRow, ColTabRow, TableTabRow, ViewTabRow,
};
use gnitz_wire::type_code;

// ── Catalog ColumnDef fixtures ────────────────────────────────────────────
//
// `ColumnDef: Default` is the plain column, so each builder names only what it
// varies and a new field costs no construction site anything.

/// A plain non-nullable, non-FK, non-hidden column of the given type.
pub fn col_def(name: &str, type_code: u8) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        type_code,
        ..Default::default()
    }
}

/// Column defs carrying just the names, for a test that needs a *named* schema
/// block. Type and nullability come off the descriptor, so only `name` matters.
pub fn named_col_defs<S: AsRef<str>>(names: &[S]) -> Vec<ColumnDef> {
    names.iter().map(|n| col_def(n.as_ref(), 0)).collect()
}

/// A plain non-nullable UUID column.
pub fn uuid_def(name: &str) -> ColumnDef {
    col_def(name, type_code::UUID)
}

/// A nullable column of the given type.
pub fn nullable_def(name: &str, type_code: u8) -> ColumnDef {
    ColumnDef {
        is_nullable: true,
        ..col_def(name, type_code)
    }
}

/// A column of `type_code` carrying an FK onto `(parent_tid, parent_col)`.
pub fn fk_def(name: &str, type_code: u8, parent_tid: i64, parent_col: u32) -> ColumnDef {
    ColumnDef {
        fk_table_id: parent_tid,
        fk_col_idx: parent_col,
        ..col_def(name, type_code)
    }
}

/// Net weight summed over every (PK, payload) a store's cursor yields — what a
/// Z-set store actually holds. A row *count* would hide a double-drive, which
/// leaves the row set identical and only doubles the weights.
pub fn sum_weights(mut c: ReadCursor) -> i64 {
    let mut sum = 0;
    while c.valid {
        sum += c.current_weight;
        c.advance();
    }
    sum
}

// ---------------------------------------------------------------------------
// Circuit + view fixtures
// ---------------------------------------------------------------------------

/// One circuit node for [`write_circuit_chain`]: opcode, source table, and the
/// optional expr/param blob.
type CircuitNode<'a> = (u64, Option<i64>, Option<&'a [u8]>);

/// Write `vid`'s circuit through the applied-delta path: one node per entry of
/// `nodes`, chained `i → i+1` on `PORT_IN`.
pub fn write_circuit_chain(engine: &mut CatalogEngine, vid: i64, nodes: &[CircuitNode<'_>]) {
    let mut bb = BatchBuilder::new(SysFamily::CircuitNodes.schema());
    for (i, &(opcode, source, blob)) in nodes.iter().enumerate() {
        write_circuit_node_row(
            &mut bb,
            &CircuitNodeRow {
                view_id: vid as u64,
                node_id: i as u64,
                opcode,
                source_table: source.map(|t| t as u64),
                expr_program: blob,
            },
            1,
        )
        .unwrap();
    }
    engine
        .ingest_to_family(SysFamily::CircuitNodes.id(), &bb.finish())
        .unwrap();

    let mut bb = BatchBuilder::new(SysFamily::CircuitEdges.schema());
    for src in 0..nodes.len().saturating_sub(1) as u64 {
        write_circuit_edge_row(
            &mut bb,
            &CircuitEdgeRow {
                view_id: vid as u64,
                dst_node: src + 1,
                dst_port: gnitz_wire::PORT_IN,
                src_node: src,
            },
            1,
        )
        .unwrap();
    }
    engine
        .ingest_to_family(SysFamily::CircuitEdges.id(), &bb.finish())
        .unwrap();
}

/// The minimal identity circuit `ScanDelta(source) → Integrate`. `scan_blob` is
/// the scan node's optional expr/param blob — a bounded-scan fixture ships its
/// `RangeDescriptor` there.
pub fn write_identity_circuit(engine: &mut CatalogEngine, vid: i64, source_tid: i64, scan_blob: Option<&[u8]>) {
    write_circuit_chain(
        engine,
        vid,
        &[
            (gnitz_wire::OPCODE_SCAN_DELTA, Some(source_tid), scan_blob),
            (gnitz_wire::OPCODE_INTEGRATE, None, None),
        ],
    );
}

/// Append one raw VIEW_TAB row at `weight`, with both `WITH (…)` budgets in
/// bytes (`0` = off). The bare `0` pk_col_idx decodes back to a single-column
/// PK `[0]`.
pub fn push_view_tab_row(
    bb: &mut BatchBuilder,
    weight: i64,
    vid: i64,
    view_name: &str,
    sql: &str,
    capacity_bytes: u64,
    delta_bytes: u64,
) {
    write_view_tab_row(
        bb,
        &ViewTabRow {
            view_id: vid as u64,
            schema_id: PUBLIC_SCHEMA_ID as u64,
            name: view_name,
            sql_definition: sql,
            pk_col_idx: 0,
            capacity_bytes,
            delta_bytes,
        },
        weight,
    );
}

/// Register an identity view over `source_tid` through the raw system-table
/// path, returning its vid or the registration's own error. Budgets in bytes,
/// `0` = off.
pub fn try_register_identity_view(
    engine: &mut CatalogEngine,
    source_tid: i64,
    name: &str,
    cols: &[ColumnDef],
    capacity_bytes: u64,
    delta_bytes: u64,
) -> Result<i64, String> {
    let vid = engine.allocate_table_id().unwrap();
    write_identity_circuit(engine, vid, source_tid, None);
    engine
        .write_column_records(vid, gnitz_wire::OWNER_KIND_VIEW as i64, cols)
        .unwrap();
    let mut bb = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut bb, 1, vid, name, "", capacity_bytes, delta_bytes);
    engine.ingest_to_family(SysFamily::View.id(), &bb.finish())?;
    Ok(vid)
}

/// [`try_register_identity_view`] for an unbounded view that must succeed.
pub fn register_identity_view(engine: &mut CatalogEngine, source_tid: i64, name: &str, cols: &[ColumnDef]) -> i64 {
    try_register_identity_view(engine, source_tid, name, cols, 0, 0).unwrap()
}

// ---------------------------------------------------------------------------
// register_table — register a relation the way a worker does
// ---------------------------------------------------------------------------

impl CatalogEngine {
    /// In-process registration of a base table at a caller-chosen `tid`, for
    /// tests that need a real relation store without a server. `pk` names the PK
    /// column indices into `cols`, in key order.
    ///
    /// It writes the same COL_TAB and TABLE_TAB rows a DDL bundle carries and
    /// hands each to [`CatalogEngine::ddl_sync`], which is the ingest tail the
    /// wire path reaches too — so the register hooks fire and the relation store
    /// is built. The columns go first: the TABLE_TAB register hook reads them
    /// back out of `sys_columns` to derive the schema.
    ///
    /// A caller-chosen id lifts the engine's allocator past itself (the register
    /// hook raises the id counter to `tid + 1`), so a later engine-side
    /// allocation cannot collide with one chosen here.
    pub fn register_table(
        &mut self,
        tid: i64,
        schema_id: i64,
        name: &str,
        cols: &[ColumnDef],
        pk: &[u32],
    ) -> Result<(), String> {
        let mut bb = BatchBuilder::new(SysFamily::Column.schema());
        for (i, cd) in cols.iter().enumerate() {
            write_col_tab_row(
                &mut bb,
                &ColTabRow {
                    owner_id: tid as u64,
                    owner_kind: gnitz_wire::OWNER_KIND_TABLE,
                    col_idx: i as u64,
                    name: &cd.name,
                    type_code: cd.type_code as u64,
                    is_nullable: cd.is_nullable,
                    fk_table_id: cd.fk_table_id as u64,
                    fk_col_idx: cd.fk_col_idx as u64,
                    is_serial: cd.is_serial,
                    is_hidden: cd.is_hidden,
                },
                1,
            )?;
        }
        self.ddl_sync(SysFamily::Column.id(), bb.finish())?;

        let mut bb = BatchBuilder::new(SysFamily::Table.schema());
        write_table_tab_row(
            &mut bb,
            &TableTabRow {
                table_id: tid as u64,
                schema_id: schema_id as u64,
                name,
                pk_col_idx: gnitz_wire::pack_pk_cols(pk),
                flags: gnitz_wire::TableProps::default().pack(),
            },
            1,
        );
        self.ddl_sync(SysFamily::Table.id(), bb.finish())
    }
}
