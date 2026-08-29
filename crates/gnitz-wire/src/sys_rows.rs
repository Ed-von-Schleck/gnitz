//! The system-catalog **row codecs**: one writer per family, expressed against
//! the same `*_PAY_*` payload positions the readers on both sides already use.
//!
//! A catalog row used to be written twice — once in the client, once in the
//! engine — and both writers were positional while both readers were
//! name-resolved. Reordering a family's column list therefore kept every reader
//! correct and silently transposed both writers, in two crates. Here each row is
//! a struct with named fields and there is one writer per family, checked by
//! `values_land_in_their_named_payload_slots` — which sees both the column list
//! and the emit sequence, so it catches a reorder of either.
//!
//! A `-1` row must reproduce its `+1`'s payload byte-for-byte — the engine's
//! retraction CAS rejects a mismatch, and only byte-equal `(PK, payload)` rows
//! cancel in the Z-set — which is the other reason each family has exactly one
//! writer: a drop and its create cannot diverge if they are the same code.

use crate::pack_col_id;

/// Where a row codec writes. Both sides already build batches this way — begin a
/// row with its key and weight, push one value per payload column in schema
/// order, close it — so this is the shape they have, not a new one.
///
/// `end_row` is where a builder that defers per-row bookkeeping (the engine's
/// null word, its row count) does it; a builder that writes eagerly implements
/// it as a no-op.
pub trait SysRowSink {
    fn begin_row(&mut self, pk: u128, weight: i64);
    fn put_u64(&mut self, v: u64);
    fn put_string(&mut self, s: &str);
    /// A variable-length column that is not UTF-8 (the circuit families' encoded
    /// expression programs).
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
    sink.begin_row(r.schema_id as u128, weight);
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
    sink.begin_row(pack_col_id(r.owner_id, r.col_idx)? as u128, weight);
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
    sink.begin_row(r.table_id as u128, weight);
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
    pub sql_definition: &'a str,
    pub pk_col_idx: u64,
    /// `WITH (capacity = …)` in bytes; `0` is unbounded.
    pub capacity_bytes: u64,
    /// `WITH (delta = …)` in bytes; `0` is no delta feed.
    pub delta_bytes: u64,
}

pub fn write_view_tab_row(sink: &mut impl SysRowSink, r: &ViewTabRow, weight: i64) {
    sink.begin_row(r.view_id as u128, weight);
    sink.put_u64(r.schema_id);
    sink.put_string(r.name);
    sink.put_string(r.sql_definition);
    sink.put_u64(r.pk_col_idx);
    sink.put_u64(r.capacity_bytes);
    sink.put_u64(r.delta_bytes);
    sink.end_row();
}

// ---------------------------------------------------------------------------
// IDX_TAB
// ---------------------------------------------------------------------------

/// One `IDX_TAB` row. `source_col_idx` carries `pack_pk_cols(&col_indices)` for
/// every index, single- and multi-column alike.
///
/// `is_unique` is the stored word, not a `bool`: a `-1` echoes back exactly what
/// the reader gave it, and only a byte-equal payload cancels.
pub struct IdxTabRow<'a> {
    pub index_id: u64,
    pub owner_id: u64,
    pub source_col_idx: u64,
    pub name: &'a str,
    pub is_unique: u64,
}

pub fn write_idx_tab_row(sink: &mut impl SysRowSink, r: &IdxTabRow, weight: i64) {
    sink.begin_row(r.index_id as u128, weight);
    sink.put_u64(r.owner_id);
    sink.put_u64(r.source_col_idx);
    sink.put_string(r.name);
    sink.put_u64(r.is_unique);
    sink.end_row();
}

// ---------------------------------------------------------------------------
// The circuit families (CIRCUIT_NODES / CIRCUIT_EDGES / CIRCUIT_NODE_COLUMNS)
// ---------------------------------------------------------------------------

/// The compound `(view_id, sub)` key of every circuit family, with `sub` packed
/// from the per-family fields (widest first). **view_id takes the LOW u128 half**:
/// the PK region OPK-encodes each 8-byte column independently, low bytes first, so
/// that is what puts view_id in the leading at-rest bytes the engine's per-view
/// prefix seek reads. Packing `(view_id << 64) | sub` instead puts `sub` there and
/// breaks every view load.
///
/// `Err` when a field overflows the width it was given: it would alias another
/// row's record, so the writer emits nothing rather than a colliding key.
fn circuit_pk(view_id: u64, fields: &[(&str, u64, u32)]) -> Result<u128, String> {
    let mut sub: u128 = 0;
    for &(name, value, bits) in fields {
        if bits < 64 && value >= 1 << bits {
            return Err(format!("CircuitTables: {name} {value} exceeds maximum"));
        }
        sub = (sub << bits) | value as u128;
    }
    Ok((view_id as u128) | (sub << 64))
}

/// One `CircuitNodes` row: node `node_id` of view `view_id`.
pub struct CircuitNodeRow<'a> {
    pub view_id: u64,
    pub node_id: u64,
    pub opcode: u64,
    /// `None` for every opcode but `ScanDelta`.
    pub source_table: Option<u64>,
    /// The encoded expression program, for the opcodes that carry one.
    pub expr_program: Option<&'a [u8]>,
}

pub fn write_circuit_node_row(sink: &mut impl SysRowSink, r: &CircuitNodeRow, weight: i64) -> Result<(), String> {
    sink.begin_row(circuit_pk(r.view_id, &[("node_id", r.node_id, 64)])?, weight);
    sink.put_u64(r.node_id);
    sink.put_u64(r.opcode);
    // Both nullable columns still take their payload slot when absent.
    match r.source_table {
        Some(t) => sink.put_u64(t),
        None => sink.put_null(),
    }
    match r.expr_program {
        Some(b) => sink.put_bytes(b),
        None => sink.put_null(),
    }
    sink.end_row();
    Ok(())
}

/// One `CircuitEdges` row. Keyed by its **destination** port, which is unique: a
/// port takes one producer.
pub struct CircuitEdgeRow {
    pub view_id: u64,
    pub dst_node: u64,
    pub dst_port: u64,
    pub src_node: u64,
}

pub fn write_circuit_edge_row(sink: &mut impl SysRowSink, r: &CircuitEdgeRow, weight: i64) -> Result<(), String> {
    let pk = circuit_pk(r.view_id, &[("dst_node", r.dst_node, 40), ("dst_port", r.dst_port, 8)])?;
    sink.begin_row(pk, weight);
    sink.put_u64(r.dst_node);
    sink.put_u64(r.dst_port);
    sink.put_u64(r.src_node);
    sink.end_row();
    Ok(())
}

/// One `CircuitNodeColumns` row: the `position`-th entry of `kind` on `node_id`.
pub struct CircuitNodeColumnRow {
    pub view_id: u64,
    pub node_id: u64,
    pub kind: u64,
    pub position: u64,
    pub value1: u64,
    pub value2: u64,
}

pub fn write_circuit_node_column_row(
    sink: &mut impl SysRowSink,
    r: &CircuitNodeColumnRow,
    weight: i64,
) -> Result<(), String> {
    let pk = circuit_pk(
        r.view_id,
        &[
            ("node_id", r.node_id, 40),
            ("kind", r.kind, 8),
            ("position", r.position, 16),
        ],
    )?;
    sink.begin_row(pk, weight);
    sink.put_u64(r.node_id);
    sink.put_u64(r.kind);
    sink.put_u64(r.position);
    sink.put_u64(r.value1);
    sink.put_u64(r.value2);
    sink.end_row();
    Ok(())
}

#[cfg(test)]
#[path = "tests/sys_rows.rs"]
mod tests;
