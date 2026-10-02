//! The system-catalog **row codecs**: one writer per family, taking a struct of
//! named fields and emitting against the same `*_PAY_*` payload positions the
//! readers on both sides resolve by name. A family's column list can therefore
//! be reordered without transposing a writer against its readers.
//!
//! One writer per family a client may write — the six [`crate::SYS_FAMILIES`]
//! entries `SysFamily::client_writable` admits; `SEQ_TAB` is engine-built and
//! has none. One and not two, because a `-1` row must reproduce its `+1`'s
//! payload byte-for-byte: the engine's retraction CAS rejects a mismatch, and
//! only byte-equal `(PK, payload)` rows cancel in the Z-set. A drop and its
//! create cannot diverge if they are the same code.

/// Where a row codec writes. Both sides already build batches this way — begin a
/// row with its key and weight, push one value per payload column in schema
/// order, close it — so this is the shape they have, not a new one.
///
/// `end_row` is where a builder that defers per-row bookkeeping (the engine's
/// null word, its row count) does it; a builder that writes eagerly implements
/// it as a no-op.
pub trait SysRowSink {
    /// Begin a row keyed by `pk`: the family's PK columns in PK-list order, each
    /// as its native value widened to `u128`. Both sinks OPK-encode them into
    /// the same PK region layout, so the client's block and the engine's are
    /// byte-identical.
    fn begin_row(&mut self, pk: &[u128], weight: i64);
    fn put_u64(&mut self, v: u64);
    fn put_string(&mut self, s: &str);
    /// A variable-length column that is not UTF-8 (a view's encoded circuit).
    fn put_bytes(&mut self, b: &[u8]);
    fn end_row(&mut self);
}

// ---------------------------------------------------------------------------
// SCHEMA_TAB
// ---------------------------------------------------------------------------

/// One `SCHEMA_TAB` row: the schema `schema_id`, named `name`.
pub struct SchemaTabRow<'a> {
    pub schema_id: u64,
    pub name: &'a str,
}

pub fn write_schema_tab_row(sink: &mut impl SysRowSink, r: &SchemaTabRow, weight: i64) {
    sink.begin_row(&[r.schema_id as u128], weight);
    sink.put_string(r.name);
    sink.end_row();
}

// ---------------------------------------------------------------------------
// COL_TAB
// ---------------------------------------------------------------------------

/// The column a FOREIGN KEY column references.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FkRef {
    pub table_id: u64,
    pub col: u32,
}

/// One `COL_TAB` row: column `col_idx` of the table or view `owner_id`.
pub struct ColTabRow<'a> {
    pub owner_id: u64,
    pub col_idx: u64,
    pub col: &'a crate::ColumnDef,
    /// The **resolved** parent column. A client's deferred self-reference is
    /// substituted before a row reaches here.
    pub fk: Option<FkRef>,
}

/// Write one `COL_TAB` row, keyed by the compound `(owner_id, col_idx)` — the
/// identity of a column record, so neither half is repeated in the payload.
pub fn write_col_tab_row(sink: &mut impl SysRowSink, r: &ColTabRow, weight: i64) {
    // COL_TAB stores "no FK" as table id 0, which no relation has.
    let (fk_table_id, fk_col_idx) = r.fk.map_or((0, 0), |fk| (fk.table_id, fk.col as u64));
    sink.begin_row(&[r.owner_id as u128, r.col_idx as u128], weight);
    sink.put_string(&r.col.name);
    sink.put_u64(r.col.ty.tc.as_wire() as u64);
    sink.put_u64(r.col.is_nullable as u64);
    sink.put_u64(fk_table_id);
    sink.put_u64(fk_col_idx);
    sink.put_u64(r.col.is_hidden as u64);
    sink.put_u64(r.col.ty.scale as u64);
    sink.end_row();
}

// ---------------------------------------------------------------------------
// TABLE_TAB
// ---------------------------------------------------------------------------

/// One `TABLE_TAB` row. `pk_col_idx` is the packed PK column list
/// (`pack_pk_cols`); `flags` is packed by `TableProps::pack`.
pub struct TableTabRow<'a> {
    pub table_id: u64,
    pub schema_id: u64,
    pub name: &'a str,
    pub pk_col_idx: u64,
    pub flags: u64,
}

pub fn write_table_tab_row(sink: &mut impl SysRowSink, r: &TableTabRow, weight: i64) {
    sink.begin_row(&[r.table_id as u128], weight);
    sink.put_u64(r.schema_id);
    sink.put_string(r.name);
    sink.put_u64(r.pk_col_idx);
    sink.put_u64(r.flags);
    sink.end_row();
}

// ---------------------------------------------------------------------------
// VIEW_TAB
// ---------------------------------------------------------------------------

/// One `VIEW_TAB` row. `pk_col_idx` is the packed view-PK column list
/// (`pack_pk_cols`).
pub struct ViewTabRow<'a> {
    pub view_id: u64,
    pub schema_id: u64,
    pub name: &'a str,
    pub pk_col_idx: u64,
    /// The view's `WITH (…)` budgets; both absent on a chain segment.
    pub props: crate::ViewProps,
    /// The user view this row is an internal chain segment of; `0` is a user
    /// view.
    pub owner_view_id: u64,
    /// [`crate::ViewFlags::pk_repeats`].
    pub pk_repeats: bool,
}

pub fn write_view_tab_row(sink: &mut impl SysRowSink, r: &ViewTabRow, weight: i64) {
    sink.begin_row(&[r.view_id as u128], weight);
    sink.put_u64(r.schema_id);
    sink.put_string(r.name);
    sink.put_u64(r.pk_col_idx);
    let (capacity, delta) = r.props.row_words();
    sink.put_u64(capacity);
    sink.put_u64(delta);
    sink.put_u64(r.owner_view_id);
    sink.put_u64(crate::ViewFlags { pk_repeats: r.pk_repeats }.pack());
    sink.end_row();
}

// ---------------------------------------------------------------------------
// IDX_TAB
// ---------------------------------------------------------------------------

/// One `IDX_TAB` row. `source_col_idx` carries `pack_pk_cols(&col_indices)` for
/// every index, single- and multi-column alike.
///
/// `flags` is the packed word (`IndexProps::pack`), not a decoded struct: a
/// `-1` echoes back exactly what the reader gave it, and only a byte-equal
/// payload cancels.
pub struct IdxTabRow<'a> {
    pub index_id: u64,
    pub owner_id: u64,
    pub source_col_idx: u64,
    pub name: &'a str,
    pub flags: u64,
}

pub fn write_idx_tab_row(sink: &mut impl SysRowSink, r: &IdxTabRow, weight: i64) {
    sink.begin_row(&[r.index_id as u128], weight);
    sink.put_u64(r.owner_id);
    sink.put_u64(r.source_col_idx);
    sink.put_string(r.name);
    sink.put_u64(r.flags);
    sink.end_row();
}

// ---------------------------------------------------------------------------
// CIRCUIT_TAB
// ---------------------------------------------------------------------------

/// `view_id`'s circuit as its one `CIRCUIT_TAB` row, at `+1`.
pub fn write_circuit_row(sink: &mut impl SysRowSink, view_id: u64, circuit: &crate::Circuit) {
    sink.begin_row(&[view_id as u128], 1);
    sink.put_bytes(&circuit.encode());
    sink.end_row();
}

#[cfg(test)]
#[path = "tests/sys_rows.rs"]
mod tests;
