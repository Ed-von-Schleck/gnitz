//! The system-catalog **row codecs**: one [`SysRow`] per family a client may
//! write, so the client's rows and the engine's seed rows are laid out by the
//! same code. `SEQ_TAB` is engine-built and has none.

/// Where a row codec writes. Both sides already build batches this way — begin a
/// row with its key and weight, push one value per payload column in schema
/// order, close it — so this is the shape they have, not a new one.
///
/// `end_row` is where a builder that defers per-row bookkeeping (the engine's
/// row count) does it; a builder that writes eagerly implements it as a no-op.
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

/// One row of a system family, as the client's DDL and the engine's seed rows
/// write it.
pub trait SysRow {
    /// The family the row belongs to.
    const FAMILY: u64;
    fn write(&self, sink: &mut impl SysRowSink, weight: i64);
}

// ---------------------------------------------------------------------------
// SCHEMA_TAB
// ---------------------------------------------------------------------------

/// One `SCHEMA_TAB` row: the schema `schema_id`, named `name`.
pub struct SchemaTabRow<'a> {
    pub schema_id: u64,
    pub name: &'a str,
}

impl SysRow for SchemaTabRow<'_> {
    const FAMILY: u64 = crate::SCHEMA_TAB;
    fn write(&self, sink: &mut impl SysRowSink, weight: i64) {
        sink.begin_row(&[self.schema_id as u128], weight);
        sink.put_string(self.name);
        sink.end_row();
    }
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

impl SysRow for ColTabRow<'_> {
    const FAMILY: u64 = crate::COL_TAB;
    fn write(&self, sink: &mut impl SysRowSink, weight: i64) {
        // COL_TAB stores "no FK" as table id 0, which no relation has.
        let (fk_table_id, fk_col_idx) = self.fk.map_or((0, 0), |fk| (fk.table_id, fk.col as u64));
        sink.begin_row(&[self.owner_id as u128, self.col_idx as u128], weight);
        sink.put_string(&self.col.name);
        sink.put_u64(self.col.ty.tc.as_wire() as u64);
        sink.put_u64(self.col.is_nullable as u64);
        sink.put_u64(fk_table_id);
        sink.put_u64(fk_col_idx);
        sink.put_u64(self.col.is_hidden as u64);
        sink.put_u64(self.col.ty.scale as u64);
        sink.end_row();
    }
}

// ---------------------------------------------------------------------------
// TABLE_TAB
// ---------------------------------------------------------------------------

/// One `TABLE_TAB` row.
pub struct TableTabRow<'a> {
    pub table_id: u64,
    pub schema_id: u64,
    pub name: &'a str,
    pub pk: crate::PkColList,
    pub props: crate::TableProps,
}

impl SysRow for TableTabRow<'_> {
    const FAMILY: u64 = crate::TABLE_TAB;
    fn write(&self, sink: &mut impl SysRowSink, weight: i64) {
        sink.begin_row(&[self.table_id as u128], weight);
        sink.put_u64(self.schema_id);
        sink.put_string(self.name);
        sink.put_u64(self.pk.pack());
        sink.put_u64(self.props.pack());
        sink.end_row();
    }
}

// ---------------------------------------------------------------------------
// VIEW_TAB
// ---------------------------------------------------------------------------

/// One `VIEW_TAB` row.
pub struct ViewTabRow<'a> {
    pub view_id: u64,
    pub schema_id: u64,
    pub name: &'a str,
    pub pk: crate::PkColList,
    /// The view's `WITH (…)` budgets; both absent on a chain segment.
    pub props: crate::ViewProps,
    /// The user view this row is an internal chain segment of; `0` is a user
    /// view.
    pub owner_view_id: u64,
    /// Two of the view's rows may carry the same PK, or one may stand at weight
    /// above 1, so its PK region identifies no single row.
    pub pk_repeats: bool,
}

impl SysRow for ViewTabRow<'_> {
    const FAMILY: u64 = crate::VIEW_TAB;
    fn write(&self, sink: &mut impl SysRowSink, weight: i64) {
        sink.begin_row(&[self.view_id as u128], weight);
        sink.put_u64(self.schema_id);
        sink.put_string(self.name);
        sink.put_u64(self.pk.pack());
        let (capacity, delta) = self.props.row_words();
        sink.put_u64(capacity);
        sink.put_u64(delta);
        sink.put_u64(self.owner_view_id);
        sink.put_u64(self.pk_repeats as u64);
        sink.end_row();
    }
}

// ---------------------------------------------------------------------------
// IDX_TAB
// ---------------------------------------------------------------------------

/// One `IDX_TAB` row: the index `index_id` on `cols` of `owner_id`.
pub struct IdxTabRow<'a> {
    pub index_id: u64,
    pub owner_id: u64,
    pub cols: crate::PkColList,
    pub name: &'a str,
    pub is_unique: bool,
}

impl SysRow for IdxTabRow<'_> {
    const FAMILY: u64 = crate::IDX_TAB;
    fn write(&self, sink: &mut impl SysRowSink, weight: i64) {
        sink.begin_row(&[self.index_id as u128], weight);
        sink.put_u64(self.owner_id);
        sink.put_u64(self.cols.pack());
        sink.put_string(self.name);
        sink.put_u64(self.is_unique as u64);
        sink.end_row();
    }
}

// ---------------------------------------------------------------------------
// CIRCUIT_TAB
// ---------------------------------------------------------------------------

/// `view_id`'s circuit as its one `CIRCUIT_TAB` row.
pub struct CircuitRow<'a> {
    pub view_id: u64,
    pub circuit: &'a crate::Circuit,
}

impl SysRow for CircuitRow<'_> {
    const FAMILY: u64 = crate::CIRCUIT_TAB;
    fn write(&self, sink: &mut impl SysRowSink, weight: i64) {
        sink.begin_row(&[self.view_id as u128], weight);
        sink.put_bytes(&self.circuit.encode());
        sink.end_row();
    }
}

#[cfg(test)]
#[path = "tests/sys_rows.rs"]
mod tests;
