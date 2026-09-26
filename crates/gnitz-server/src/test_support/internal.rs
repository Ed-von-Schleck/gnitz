//! The catalog-level test helpers — the store-level ones are `gnitz-store`'s
//! `shared` file, compiled here through a `#[path]`.
//!
//! This file is compiled once, and only inside this crate, so it names
//! crate-internals as `crate::` and widens no API — nothing links this crate.

use crate::catalog::{CatalogEngine, ColumnDef, SysFamily, PUBLIC_SCHEMA_ID};
use gnitz_store::storage::{Batch, BatchBuilder, ReadCursor};
use gnitz_wire::sys_rows::{
    write_circuit_rows, write_idx_tab_row, write_table_tab_row, IdxTabRow, SysRowSink, TableTabRow,
};
use gnitz_wire::Circuit;
use gnitz_wire::{ColType, TypeCode};

// ── Catalog ColumnDef fixtures ────────────────────────────────────────────
//
// `ColumnDef::new` is the plain column, so each builder names only what it
// varies and a new field costs no construction site anything.

/// A plain non-nullable, non-FK, non-hidden column of the given type.
pub fn col_def(name: &str, type_code: TypeCode) -> ColumnDef {
    ColumnDef::new(name, ColType::of(type_code))
}

/// Column defs carrying just the names, for a test that needs a *named* schema
/// block. Type and nullability come off the descriptor, so only `name` matters.
pub fn named_col_defs<S: AsRef<str>>(names: &[S]) -> Vec<ColumnDef> {
    names.iter().map(|n| col_def(n.as_ref(), TypeCode::U64)).collect()
}

/// A plain non-nullable UUID column.
pub fn uuid_def(name: &str) -> ColumnDef {
    col_def(name, TypeCode::UUID)
}

/// A nullable column of the given type.
pub fn nullable_def(name: &str, type_code: TypeCode) -> ColumnDef {
    ColumnDef {
        is_nullable: true,
        ..col_def(name, type_code)
    }
}

/// A column of `type_code` carrying an FK onto `(parent_tid, parent_col)`.
pub fn fk_def(name: &str, type_code: TypeCode, parent_tid: i64, parent_col: u32) -> ColumnDef {
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
pub fn identity_circuit(source_tid: i64, bound: gnitz_wire::ReadBound) -> Circuit {
    let mut circuit = Circuit::default();
    let scan = circuit.input_delta(source_tid as u64, bound);
    circuit.sink(scan);
    circuit
}

/// `ScanDelta(source)` then `n - 1` `Negate`s: an `n`-node circuit.
pub fn negate_chain(source: i64, n: usize) -> Circuit {
    let mut circuit = Circuit::default();
    let mut tip = circuit.input_delta(source as u64, gnitz_wire::ReadBound::None);
    for _ in 1..n {
        tip = circuit.negate(tip);
    }
    circuit
}

/// An equi-join of `a` and `b` on their column 1, each side stating its scatter
/// key where `keyed` says so.
pub fn equi_join_circuit(a: i64, b: i64, keyed: [bool; 2]) -> Circuit {
    let mut circuit = Circuit::default();
    let key = [(1, None)];
    let [ka, kb] = [(a, keyed[0]), (b, keyed[1])].map(|(source, keyed)| {
        let scan = circuit.input_delta(source as u64, gnitz_wire::ReadBound::None);
        let role = gnitz_wire::ReindexRole::ScatterKey {
            source: source as u64,
            source_key: key.to_vec(),
        };
        match keyed {
            true => circuit.map_reindex(scan, &key, &[0], role),
            false => scan,
        }
    });
    let tb = circuit.integrate_trace(kb);
    let joined = circuit.join(ka, tb, gnitz_wire::JoinKind::Equi, false);
    circuit.sink(joined);
    circuit
}

/// A drive host for circuits that exchange nothing.
pub struct LocalDrive<'a>(pub &'a mut CatalogEngine);

impl crate::query::DriveHost for LocalDrive<'_> {
    fn parts(
        &mut self,
    ) -> (
        &mut crate::query::DagEngine,
        &mut gnitz_store::relation::RelationRegistry,
    ) {
        (&mut self.0.dag, &mut self.0.registry)
    }

    fn exchange(&mut self, view_id: i64, _batch: Batch, _key: i64) -> Batch {
        panic!("view {view_id} relayed: this host serves exchange-free circuits only");
    }
}

/// [`identity_circuit`] written as `vid`'s circuit.
pub fn write_identity_circuit(engine: &mut CatalogEngine, vid: i64, source_tid: i64, bound: gnitz_wire::ReadBound) {
    write_circuit(engine, vid, identity_circuit(source_tid, bound));
}

// ── Positional row builders ──────────────────────────────────────────────
//
// Fixtures over `gnitz_wire::sys_rows`' codecs, defaulting what a test never
// varies. Production writes the wire struct inline.

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

/// `defs` as `owner_id`'s COL_TAB batch at `weight`, numbered by position.
pub fn col_tab_batch(owner_id: i64, defs: &[ColumnDef], weight: i64) -> Batch {
    let mut bb = BatchBuilder::new(*SysFamily::Column.schema());
    crate::catalog::write_col_tab_rows(&mut bb, owner_id, defs, weight);
    bb.finish()
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
    sink.put_u64(gnitz_wire::ViewFlags::default().pack());
    sink.end_row();
}

/// Register a view running `circuit` through the raw system-table path,
/// returning its vid or the registration's own error. Budgets in bytes,
/// `0` = off.
pub fn try_register_view(
    engine: &mut CatalogEngine,
    circuit: Circuit,
    name: &str,
    cols: &[ColumnDef],
    capacity_bytes: u64,
    delta_bytes: u64,
) -> Result<i64, String> {
    let vid = engine.allocate_ids(1).unwrap();
    write_circuit(engine, vid, circuit);
    engine.write_column_records(vid, cols).unwrap();
    let mut bb = BatchBuilder::new(*SysFamily::View.schema());
    push_view_tab_row(&mut bb, 1, vid, name, capacity_bytes, delta_bytes, 0);
    engine.submit(SysFamily::View, bb.finish())?;
    Ok(vid)
}

/// [`try_register_view`] for an identity view over `source_tid`.
pub fn try_register_identity_view(
    engine: &mut CatalogEngine,
    source_tid: i64,
    name: &str,
    cols: &[ColumnDef],
    capacity_bytes: u64,
    delta_bytes: u64,
) -> Result<i64, String> {
    let circuit = identity_circuit(source_tid, gnitz_wire::ReadBound::None);
    try_register_view(engine, circuit, name, cols, capacity_bytes, delta_bytes)
}

/// [`try_register_identity_view`] for an unbounded view that must succeed.
pub fn register_identity_view(engine: &mut CatalogEngine, source_tid: i64, name: &str, cols: &[ColumnDef]) -> i64 {
    try_register_identity_view(engine, source_tid, name, cols, 0, 0).unwrap()
}

/// The rows a range read over `cols` returns, or `None` when none match.
pub fn seek_by_index_range(
    engine: &mut CatalogEngine,
    tid: i64,
    cols: &[u32],
    eq: &[u128],
    start: gnitz_wire::Cut,
    end: gnitz_wire::Cut,
) -> Result<(Option<std::rc::Rc<Batch>>, gnitz_store::schema::SchemaDescriptor), gnitz_wire::WireFault> {
    let schema = engine
        .registry
        .relation_or_err(tid)
        .map_err(|e| gnitz_wire::WireFault::from(e.to_string()))?
        .schema();
    let range = gnitz_wire::KeyRange::new(gnitz_wire::PkColList::from_slice(cols), eq, start, end);
    let spec = gnitz_wire::ReadSpec::all_rows(gnitz_wire::ReadBound::Range(range));
    let rows = engine.scan_spec(tid, spec, &schema)?;
    Ok(((!rows.is_empty()).then_some(rows), schema))
}

/// [`seek_by_index_range`] at the point `natives` names, fewer than `cols` for a
/// prefix seek.
pub fn seek_by_index(
    engine: &mut CatalogEngine,
    tid: i64,
    cols: &[u32],
    natives: &[u128],
) -> Result<(Option<std::rc::Rc<Batch>>, gnitz_store::schema::SchemaDescriptor), gnitz_wire::WireFault> {
    let schema = engine
        .registry
        .relation_or_err(tid)
        .map_err(|e| gnitz_wire::WireFault::from(e.to_string()))?
        .schema();
    let images: Vec<u128> = cols
        .iter()
        .zip(natives)
        .map(|(&c, &v)| gnitz_wire::key_image(schema.columns[c as usize].type_code, v))
        .collect();
    let (&last, eq) = images.split_last().expect("at least one key value");
    seek_by_index_range(
        engine,
        tid,
        cols,
        eq,
        gnitz_wire::Cut::before(last),
        gnitz_wire::Cut::after(last),
    )
}
