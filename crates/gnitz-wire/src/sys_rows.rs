//! The system-catalog **row codecs**: one writer per family, taking a struct of
//! named fields and emitting against the same `*_PAY_*` payload positions the
//! readers on both sides resolve by name. A family's column list can therefore
//! be reordered without transposing a writer against its readers.
//!
//! Exactly one writer per family, because a `-1` row must reproduce its `+1`'s
//! payload byte-for-byte: the engine's retraction CAS rejects a mismatch, and
//! only byte-equal `(PK, payload)` rows cancel in the Z-set. A drop and its
//! create cannot diverge if they are the same code.

use crate::pack_col_id;

/// Where a row codec writes. Both sides already build batches this way — begin a
/// row with its key and weight, push one value per payload column in schema
/// order, close it — so this is the shape they have, not a new one.
///
/// `end_row` is where a builder that defers per-row bookkeeping (the engine's
/// null word, its row count) does it; a builder that writes eagerly implements
/// it as a no-op.
pub trait SysRowSink {
    /// Begin a row keyed by `pk`: the family's PK columns in PK-list order, each
    /// as its native value widened to `u128`. The sink packs them into its own
    /// layout — big-endian OPK in the engine, little-endian on the wire.
    fn begin_row(&mut self, pk: &[u128], weight: i64);
    fn put_u64(&mut self, v: u64);
    fn put_string(&mut self, s: &str);
    /// A variable-length column that is not UTF-8 (a circuit node's encoded
    /// parameters).
    fn put_bytes(&mut self, b: &[u8]);
    /// A NULL in the next payload slot. The slot still consumes its position —
    /// the writers below emit one value per payload column either way.
    fn put_null(&mut self);
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

/// One `COL_TAB` row: column `col_idx` of the table or view `owner_id`.
///
/// `fk_table_id` is the **resolved** parent id — the client's
/// `SELF_FK_TABLE_ID` placeholder is a client-side policy and is substituted
/// before a row reaches here.
pub struct ColTabRow<'a> {
    pub owner_id: u64,
    pub owner_kind: u64,
    pub col_idx: u64,
    pub name: &'a str,
    pub type_code: u64,
    pub is_nullable: bool,
    pub fk_table_id: u64,
    pub fk_col_idx: u64,
    pub is_serial: bool,
    pub is_hidden: bool,
}

/// Write one `COL_TAB` row. The key is `pack_col_id(owner_id, col_idx)`, which
/// rejects an out-of-range owner or column index rather than aliasing another
/// column's record; each side decides whether that is an error to propagate or
/// a corruption to abort on.
pub fn write_col_tab_row(sink: &mut impl SysRowSink, r: &ColTabRow, weight: i64) -> Result<(), String> {
    sink.begin_row(&[pack_col_id(r.owner_id, r.col_idx)? as u128], weight);
    sink.put_u64(r.owner_id);
    sink.put_u64(r.owner_kind);
    sink.put_u64(r.col_idx);
    sink.put_string(r.name);
    sink.put_u64(r.type_code);
    sink.put_u64(r.is_nullable as u64);
    sink.put_u64(r.fk_table_id);
    sink.put_u64(r.fk_col_idx);
    sink.put_u64(r.is_serial as u64);
    sink.put_u64(r.is_hidden as u64);
    sink.end_row();
    Ok(())
}

// ---------------------------------------------------------------------------
// TABLE_TAB
// ---------------------------------------------------------------------------

/// One `TABLE_TAB` row. `pk_col_idx` is the packed PK column list
/// (`pack_pk_cols`); `flags` is packed by `pack_table_flags`.
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

/// One `VIEW_TAB` row. `pk_col_idx` is the packed view-PK column list; a bare
/// `0` decodes back to the single-column PK `[0]`.
pub struct ViewTabRow<'a> {
    pub view_id: u64,
    pub schema_id: u64,
    pub name: &'a str,
    pub pk_col_idx: u64,
    /// `WITH (capacity = …)` in bytes; `0` is unbounded.
    pub capacity_bytes: u64,
    /// `WITH (delta = …)` in bytes; `0` is no delta feed.
    pub delta_bytes: u64,
    /// The user view this row is an internal chain segment of; `0` is a user
    /// view.
    pub owner_view_id: u64,
}

pub fn write_view_tab_row(sink: &mut impl SysRowSink, r: &ViewTabRow, weight: i64) {
    sink.begin_row(&[r.view_id as u128], weight);
    sink.put_u64(r.schema_id);
    sink.put_string(r.name);
    sink.put_u64(r.pk_col_idx);
    sink.put_u64(r.capacity_bytes);
    sink.put_u64(r.delta_bytes);
    sink.put_u64(r.owner_view_id);
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
// CIRCUIT_NODES
// ---------------------------------------------------------------------------

/// One `CircuitNodes` row: node `node_id` of view `view_id`. Keyed by
/// `(view_id, node_id)`, so the per-view prefix seek reads the leading half.
pub struct CircuitNodeRow<'a> {
    pub view_id: u64,
    pub node_id: u64,
    pub opcode: u64,
    /// `None` for every opcode but `ScanDelta`.
    pub source_table: Option<u64>,
    /// The producers feeding this node's input slots, in port order: `None` past
    /// the operator's arity. A port takes one producer, which is why it is a
    /// column and not a row of its own.
    pub inputs: [Option<u64>; 2],
    /// The node's per-opcode parameters (`encode_op_node`), or `None` for an
    /// operator that carries none.
    pub params: Option<&'a [u8]>,
}

pub fn write_circuit_node_row(sink: &mut impl SysRowSink, r: &CircuitNodeRow, weight: i64) {
    sink.begin_row(&[r.view_id as u128, r.node_id as u128], weight);
    sink.put_u64(r.opcode);
    // Every nullable column still takes its payload slot when absent.
    match r.source_table {
        Some(t) => sink.put_u64(t),
        None => sink.put_null(),
    }
    for input in r.inputs {
        match input {
            Some(src) => sink.put_u64(src),
            None => sink.put_null(),
        }
    }
    match r.params {
        Some(b) => sink.put_bytes(b),
        None => sink.put_null(),
    }
    sink.end_row();
}

#[cfg(test)]
#[path = "tests/sys_rows.rs"]
mod tests;
