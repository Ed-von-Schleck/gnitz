//! The catalog-level test helpers — the store-level ones are `gnitz-store`'s
//! `shared` file, compiled here through a `#[path]`.
//!
//! This file is compiled once, and only inside this crate, so it names
//! crate-internals as `crate::` and widens no API — nothing links this crate.

use crate::catalog::{CatalogEngine, ColumnDef, SysFamily, PUBLIC_SCHEMA_ID};
use gnitz_store::storage::{Batch, BatchBuilder, ReadCursor};
use gnitz_wire::sys_rows::{
    write_circuit_rows, write_col_tab_row, write_idx_tab_row, write_table_tab_row, ColTabRow, IdxTabRow, SysRowSink,
    TableTabRow,
};
use gnitz_wire::type_code;
use gnitz_wire::Circuit;

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

/// Write `vid`'s circuit through the applied-delta path, through the row writer
/// the client commits with.
pub fn write_circuit(engine: &mut CatalogEngine, vid: i64, circuit: Circuit) {
    let mut bb = BatchBuilder::new(*SysFamily::CircuitNodes.schema());
    write_circuit_rows(&mut bb, vid as u64, &circuit);
    engine.submit(SysFamily::CircuitNodes, bb.finish()).unwrap();
}

/// The minimal identity circuit `ScanDelta(source, bound) → Integrate`.
pub fn write_identity_circuit(engine: &mut CatalogEngine, vid: i64, source_tid: i64, bound: gnitz_wire::ReadBound) {
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(source_tid as u64, bound);
    circuit.sink(scan);
    write_circuit(engine, vid, circuit);
}

// ── Positional row builders ──────────────────────────────────────────────
//
// Fixtures over `gnitz_wire::sys_rows`' codecs, defaulting what a test never
// varies. Production writes the wire struct inline.

/// Append one COL_TAB row for column `col_idx` of `owner_id`. Takes the whole
/// `ColumnDef` rather than its fields, mirroring the catalog's read side, which
/// reassembles exactly this struct.
pub fn push_col_tab_row(
    bb: &mut BatchBuilder,
    owner_id: i64,
    owner_kind: i64,
    col_idx: i64,
    cd: &ColumnDef,
    weight: i64,
) {
    write_col_tab_row(
        bb,
        &ColTabRow {
            owner_id: owner_id as u64,
            col_idx: col_idx as u64,
            owner_kind: owner_kind as u64,
            name: &cd.name,
            type_code: cd.type_code as u64,
            is_nullable: cd.is_nullable,
            fk_table_id: cd.fk_table_id as u64,
            fk_col_idx: cd.fk_col_idx as u64,
            is_serial: cd.is_serial,
            is_hidden: cd.is_hidden,
            scale: cd.scale,
        },
        weight,
    );
}

/// Append one TABLE_TAB row at `weight`.
pub fn push_table_tab_row(
    bb: &mut BatchBuilder,
    tid: i64,
    schema_id: i64,
    name: &str,
    pk_col_idx: u64,
    flags: u64,
    weight: i64,
) {
    write_table_tab_row(
        bb,
        &TableTabRow {
            table_id: tid as u64,
            schema_id: schema_id as u64,
            name,
            pk_col_idx,
            flags,
        },
        weight,
    );
}

/// The one-row IDX_TAB batch at `weight`. A `-1` must reproduce its `+1`'s
/// payload exactly — the retraction CAS rejects a mismatch.
pub fn idx_tab_batch(
    index_id: i64,
    owner_id: i64,
    packed_cols: u64,
    name: &str,
    props: gnitz_wire::IndexProps,
    weight: i64,
) -> Batch {
    let mut bb = BatchBuilder::new(*SysFamily::Index.schema());
    write_idx_tab_row(
        &mut bb,
        &IdxTabRow {
            index_id: index_id as u64,
            owner_id: owner_id as u64,
            source_col_idx: packed_cols,
            name,
            flags: props.pack(),
        },
        weight,
    );
    bb.finish()
}

/// Append one raw VIEW_TAB row with PK list `[0]`, word by word, so a test can forge
/// budget words no `ViewProps` holds (`0` = off; `owner_view_id` `0` = a user view).
pub fn push_view_tab_row(
    bb: &mut BatchBuilder,
    weight: i64,
    vid: i64,
    view_name: &str,
    capacity_bytes: u64,
    delta_bytes: u64,
    owner_view_id: i64,
) {
    let sink: &mut dyn SysRowSink = bb;
    sink.begin_row(&[vid as u128], weight);
    sink.put_u64(PUBLIC_SCHEMA_ID as u64);
    sink.put_string(view_name);
    sink.put_u64(gnitz_wire::pack_pk_cols(&[0]));
    sink.put_u64(capacity_bytes);
    sink.put_u64(delta_bytes);
    sink.put_u64(owner_view_id as u64);
    sink.end_row();
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
    let vid = engine.allocate_ids(1).unwrap();
    write_identity_circuit(engine, vid, source_tid, gnitz_wire::ReadBound::None);
    engine
        .write_column_records(vid, gnitz_wire::OWNER_KIND_VIEW as i64, cols)
        .unwrap();
    let mut bb = BatchBuilder::new(*SysFamily::View.schema());
    push_view_tab_row(&mut bb, 1, vid, name, capacity_bytes, delta_bytes, 0);
    engine.submit(SysFamily::View, bb.finish())?;
    Ok(vid)
}

/// [`try_register_identity_view`] for an unbounded view that must succeed.
pub fn register_identity_view(engine: &mut CatalogEngine, source_tid: i64, name: &str, cols: &[ColumnDef]) -> i64 {
    try_register_identity_view(engine, source_tid, name, cols, 0, 0).unwrap()
}

/// The rows an `IndexRange`/`Required` read of `range` over `cols` returns, or
/// `None` when none match — the production read, through `open_bound`.
pub fn seek_by_index_range(
    engine: &mut CatalogEngine,
    tid: i64,
    cols: &[u32],
    range: gnitz_wire::RangeDescriptor,
) -> Result<(Option<std::rc::Rc<Batch>>, gnitz_store::schema::SchemaDescriptor), gnitz_wire::WireFault> {
    let schema = engine
        .registry()
        .relation_or_err(tid)
        .map_err(|e| gnitz_wire::WireFault::from(e.to_string()))?
        .schema();
    let spec = gnitz_wire::ReadSpec::all_rows(gnitz_wire::ReadBound::IndexRange {
        bound: gnitz_wire::IndexBound {
            idx_cols: gnitz_wire::PkColList::from_slice(cols),
            desc: range,
        },
        walk: gnitz_wire::IndexWalk::Required,
    });
    let rows = engine.scan_spec(tid, spec, &schema)?;
    Ok(((!rows.is_empty()).then_some(rows), schema))
}

/// [`seek_by_index_range`] at the point `natives` names: equality on every
/// supplied value, fewer than `cols` for a prefix seek.
pub fn seek_by_index(
    engine: &mut CatalogEngine,
    tid: i64,
    cols: &[u32],
    natives: &[u128],
) -> Result<(Option<std::rc::Rc<Batch>>, gnitz_store::schema::SchemaDescriptor), gnitz_wire::WireFault> {
    let (&last, eq) = natives.split_last().expect("at least one key value");
    seek_by_index_range(engine, tid, cols, gnitz_wire::RangeDescriptor::point(eq, last))
}
