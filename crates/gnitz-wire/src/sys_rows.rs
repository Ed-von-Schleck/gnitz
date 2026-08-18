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
}

pub fn write_view_tab_row(sink: &mut impl SysRowSink, r: &ViewTabRow, weight: i64) {
    sink.begin_row(r.view_id as u128, weight);
    sink.put_u64(r.schema_id);
    sink.put_string(r.name);
    sink.put_string(r.sql_definition);
    sink.put_u64(r.pk_col_idx);
    sink.put_u64(r.capacity_bytes);
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
mod tests {
    use super::*;
    use crate::{
        CIRCEDGES_PAY_DST_NODE, CIRCEDGES_PAY_DST_PORT, CIRCEDGES_PAY_SRC_NODE, CIRCNCOL_PAY_KIND,
        CIRCNCOL_PAY_NODE_ID, CIRCNCOL_PAY_POSITION, CIRCNCOL_PAY_VALUE1, CIRCNCOL_PAY_VALUE2,
        CIRCNODES_PAY_EXPR_PROGRAM, CIRCNODES_PAY_NODE_ID, CIRCNODES_PAY_OPCODE, CIRCNODES_PAY_SOURCE_TABLE,
        CIRCUIT_EDGES_COLS, CIRCUIT_NODES_COLS, CIRCUIT_NODE_COLUMNS_COLS, COLTAB_PAY_COL_IDX, COLTAB_PAY_FK_COL_IDX,
        COLTAB_PAY_FK_TABLE_ID, COLTAB_PAY_IS_HIDDEN, COLTAB_PAY_IS_NULLABLE, COLTAB_PAY_IS_SERIAL, COLTAB_PAY_NAME,
        COLTAB_PAY_OWNER_ID, COLTAB_PAY_OWNER_KIND, COLTAB_PAY_TYPE_CODE, COL_TAB_COLS, IDXTAB_PAY_IS_UNIQUE,
        IDXTAB_PAY_NAME, IDXTAB_PAY_OWNER_ID, IDXTAB_PAY_SOURCE_COLS, IDX_TAB_COLS, SCHEMA_TAB_COLS, TABLE_TAB_COLS,
        TABTAB_PAY_FLAGS, TABTAB_PAY_NAME, TABTAB_PAY_PK_COL_IDX, TABTAB_PAY_SCHEMA_ID, VIEWTAB_PAY_CAPACITY,
        VIEWTAB_PAY_NAME, VIEWTAB_PAY_PK_COL_IDX, VIEWTAB_PAY_SCHEMA_ID, VIEWTAB_PAY_SQL, VIEW_TAB_COLS,
    };

    /// A sink that records what a writer emitted, so the tests below read the
    /// row back as a sequence rather than through either side's batch type.
    #[derive(Default, PartialEq, Eq, Debug)]
    struct Recorder {
        pk: u128,
        weight: i64,
        vals: Vec<Val>,
        closed: bool,
    }

    #[derive(PartialEq, Eq, Debug)]
    enum Val {
        U64(u64),
        Str(String),
        Bytes(Vec<u8>),
        Null,
    }

    impl SysRowSink for Recorder {
        fn begin_row(&mut self, pk: u128, weight: i64) {
            self.pk = pk;
            self.weight = weight;
        }
        fn put_u64(&mut self, v: u64) {
            self.vals.push(Val::U64(v));
        }
        fn put_string(&mut self, s: &str) {
            self.vals.push(Val::Str(s.to_string()));
        }
        fn put_bytes(&mut self, b: &[u8]) {
            self.vals.push(Val::Bytes(b.to_vec()));
        }
        fn put_null(&mut self) {
            self.vals.push(Val::Null);
        }
        fn end_row(&mut self) {
            self.closed = true;
        }
    }

    /// Every writer must emit exactly one value per payload column and close the
    /// row — an under-filled row desyncs a columnar builder and only surfaces
    /// later as an un-attributed length error.
    #[test]
    fn every_writer_fills_every_payload_column() {
        let mut r = Recorder::default();
        write_col_tab_row(
            &mut r,
            &ColTabRow {
                owner_id: 16,
                owner_kind: 0,
                col_idx: 2,
                name: "c",
                type_code: 4,
                is_nullable: true,
                fk_table_id: 17,
                fk_col_idx: 1,
                is_serial: false,
                is_hidden: true,
            },
            1,
        )
        .unwrap();
        assert_eq!(r.vals.len(), COL_TAB_COLS.len() - 1);
        assert!(r.closed);

        let mut r = Recorder::default();
        write_table_tab_row(
            &mut r,
            &TableTabRow {
                table_id: 16,
                schema_id: 3,
                name: "t",
                pk_col_idx: 0,
                flags: 0,
            },
            1,
        );
        assert_eq!(r.vals.len(), TABLE_TAB_COLS.len() - 1);
        assert!(r.closed);

        let mut r = Recorder::default();
        write_view_tab_row(
            &mut r,
            &ViewTabRow {
                view_id: 20,
                schema_id: 3,
                name: "v",
                sql_definition: "SELECT 1",
                pk_col_idx: 0,
                capacity_bytes: 0,
            },
            1,
        );
        assert_eq!(r.vals.len(), VIEW_TAB_COLS.len() - 1);
        assert!(r.closed);

        let mut r = Recorder::default();
        write_idx_tab_row(
            &mut r,
            &IdxTabRow {
                index_id: 100,
                owner_id: 16,
                source_col_idx: 1,
                name: "idx",
                is_unique: 1,
            },
            1,
        );
        assert_eq!(r.vals.len(), IDX_TAB_COLS.len() - 1);
        assert!(r.closed);
    }

    /// Each value must land in the payload slot the readers look for it in.
    #[test]
    fn values_land_in_their_named_payload_slots() {
        let mut r = Recorder::default();
        write_col_tab_row(
            &mut r,
            &ColTabRow {
                owner_id: 16,
                owner_kind: 1,
                col_idx: 2,
                name: "score",
                type_code: 10,
                is_nullable: true,
                fk_table_id: 17,
                fk_col_idx: 3,
                is_serial: false,
                is_hidden: true,
            },
            -1,
        )
        .unwrap();
        assert_eq!(r.pk, pack_col_id(16, 2).unwrap() as u128);
        assert_eq!(r.weight, -1);
        assert_eq!(r.vals[COLTAB_PAY_OWNER_ID], Val::U64(16));
        assert_eq!(r.vals[COLTAB_PAY_OWNER_KIND], Val::U64(1));
        assert_eq!(r.vals[COLTAB_PAY_COL_IDX], Val::U64(2));
        assert_eq!(r.vals[COLTAB_PAY_NAME], Val::Str("score".into()));
        assert_eq!(r.vals[COLTAB_PAY_TYPE_CODE], Val::U64(10));
        assert_eq!(r.vals[COLTAB_PAY_IS_NULLABLE], Val::U64(1));
        assert_eq!(r.vals[COLTAB_PAY_FK_TABLE_ID], Val::U64(17));
        assert_eq!(r.vals[COLTAB_PAY_FK_COL_IDX], Val::U64(3));
        assert_eq!(r.vals[COLTAB_PAY_IS_SERIAL], Val::U64(0));
        assert_eq!(r.vals[COLTAB_PAY_IS_HIDDEN], Val::U64(1));

        let mut r = Recorder::default();
        write_idx_tab_row(
            &mut r,
            &IdxTabRow {
                index_id: 100,
                owner_id: 16,
                source_col_idx: 9,
                name: "idx_t_b",
                is_unique: 1,
            },
            1,
        );
        assert_eq!(r.pk, 100);
        assert_eq!(r.vals[IDXTAB_PAY_OWNER_ID], Val::U64(16));
        assert_eq!(r.vals[IDXTAB_PAY_SOURCE_COLS], Val::U64(9));
        assert_eq!(r.vals[IDXTAB_PAY_NAME], Val::Str("idx_t_b".into()));
        assert_eq!(r.vals[IDXTAB_PAY_IS_UNIQUE], Val::U64(1));

        // Distinct values per field, so a transposed pair in the writer body
        // fails here rather than round-tripping unnoticed.
        let mut r = Recorder::default();
        write_table_tab_row(
            &mut r,
            &TableTabRow {
                table_id: 16,
                schema_id: 3,
                name: "t",
                pk_col_idx: 5,
                flags: 9,
            },
            1,
        );
        assert_eq!(r.pk, 16);
        assert_eq!(r.vals[TABTAB_PAY_SCHEMA_ID], Val::U64(3));
        assert_eq!(r.vals[TABTAB_PAY_NAME], Val::Str("t".into()));
        assert_eq!(r.vals[TABTAB_PAY_PK_COL_IDX], Val::U64(5));
        assert_eq!(r.vals[TABTAB_PAY_FLAGS], Val::U64(9));

        let mut r = Recorder::default();
        write_view_tab_row(
            &mut r,
            &ViewTabRow {
                view_id: 20,
                schema_id: 4,
                name: "v",
                sql_definition: "SELECT 1",
                pk_col_idx: 6,
                capacity_bytes: 4096,
            },
            1,
        );
        assert_eq!(r.pk, 20);
        assert_eq!(r.vals[VIEWTAB_PAY_SCHEMA_ID], Val::U64(4));
        assert_eq!(r.vals[VIEWTAB_PAY_NAME], Val::Str("v".into()));
        assert_eq!(r.vals[VIEWTAB_PAY_SQL], Val::Str("SELECT 1".into()));
        assert_eq!(r.vals[VIEWTAB_PAY_PK_COL_IDX], Val::U64(6));
        assert_eq!(r.vals[VIEWTAB_PAY_CAPACITY], Val::U64(4096));

        let mut r = Recorder::default();
        write_schema_tab_row(
            &mut r,
            &SchemaTabRow {
                schema_id: 3,
                name: "public",
            },
            1,
        );
        assert_eq!(r.pk, 3);
        assert_eq!(r.vals.len(), SCHEMA_TAB_COLS.len() - 1);
        assert_eq!(r.vals[0], Val::Str("public".into()));
        assert!(r.closed);

        // The circuit families. `view_id` must occupy the LOW u128 half of the
        // compound key: the PK region OPK-encodes each column independently, low
        // bytes first, so that is what puts view_id in the leading at-rest bytes
        // the engine's per-view prefix seek reads.
        let mut r = Recorder::default();
        write_circuit_node_row(
            &mut r,
            &CircuitNodeRow {
                view_id: 7,
                node_id: 5,
                opcode: 9,
                source_table: Some(31),
                expr_program: Some(&[0xAB, 0xCD]),
            },
            1,
        )
        .unwrap();
        assert_eq!(r.pk, 7 | (5u128 << 64));
        assert_eq!(
            r.vals.len(),
            CIRCUIT_NODES_COLS.len() - 2,
            "compound PK: two key columns"
        );
        assert_eq!(r.vals[CIRCNODES_PAY_NODE_ID], Val::U64(5));
        assert_eq!(r.vals[CIRCNODES_PAY_OPCODE], Val::U64(9));
        assert_eq!(r.vals[CIRCNODES_PAY_SOURCE_TABLE], Val::U64(31));
        assert_eq!(r.vals[CIRCNODES_PAY_EXPR_PROGRAM], Val::Bytes(vec![0xAB, 0xCD]));

        // The two nullable columns still take their slots when absent, so an
        // omitted `put_null` would shift every later value.
        let mut r = Recorder::default();
        write_circuit_node_row(
            &mut r,
            &CircuitNodeRow {
                view_id: 7,
                node_id: 5,
                opcode: 9,
                source_table: None,
                expr_program: None,
            },
            1,
        )
        .unwrap();
        assert_eq!(r.vals[CIRCNODES_PAY_SOURCE_TABLE], Val::Null);
        assert_eq!(r.vals[CIRCNODES_PAY_EXPR_PROGRAM], Val::Null);

        let mut r = Recorder::default();
        write_circuit_edge_row(
            &mut r,
            &CircuitEdgeRow {
                view_id: 7,
                dst_node: 5,
                dst_port: 1,
                src_node: 4,
            },
            1,
        )
        .unwrap();
        assert_eq!(r.pk, 7 | (((5u128 << 8) | 1) << 64));
        assert_eq!(r.vals.len(), CIRCUIT_EDGES_COLS.len() - 2);
        assert_eq!(r.vals[CIRCEDGES_PAY_DST_NODE], Val::U64(5));
        assert_eq!(r.vals[CIRCEDGES_PAY_DST_PORT], Val::U64(1));
        assert_eq!(r.vals[CIRCEDGES_PAY_SRC_NODE], Val::U64(4));

        let mut r = Recorder::default();
        write_circuit_node_column_row(
            &mut r,
            &CircuitNodeColumnRow {
                view_id: 7,
                node_id: 5,
                kind: 3,
                position: 2,
                value1: 11,
                value2: 12,
            },
            1,
        )
        .unwrap();
        assert_eq!(r.pk, 7 | (((5u128 << 24) | (3u128 << 16) | 2u128) << 64));
        assert_eq!(r.vals.len(), CIRCUIT_NODE_COLUMNS_COLS.len() - 2);
        assert_eq!(r.vals[CIRCNCOL_PAY_NODE_ID], Val::U64(5));
        assert_eq!(r.vals[CIRCNCOL_PAY_KIND], Val::U64(3));
        assert_eq!(r.vals[CIRCNCOL_PAY_POSITION], Val::U64(2));
        assert_eq!(r.vals[CIRCNCOL_PAY_VALUE1], Val::U64(11));
        assert_eq!(r.vals[CIRCNCOL_PAY_VALUE2], Val::U64(12));
    }

    /// A `sub` field that overflows its bit range would alias another row's
    /// record, so `circuit_pk` refuses and the writer emits nothing.
    #[test]
    fn a_circuit_sub_field_that_would_alias_another_record_is_rejected() {
        let mut r = Recorder::default();
        let err = write_circuit_edge_row(
            &mut r,
            &CircuitEdgeRow {
                view_id: 1,
                dst_node: 1 << 40,
                dst_port: 0,
                src_node: 0,
            },
            1,
        )
        .unwrap_err();
        assert!(err.contains("dst_node"), "{err}");
        assert!(r.vals.is_empty(), "a rejected row must emit nothing");
    }

    /// A column index past the packed key's 9-bit field would alias another
    /// column's record, so the writer refuses it rather than emitting the row.
    #[test]
    fn a_col_idx_that_would_alias_another_record_is_rejected() {
        let mut r = Recorder::default();
        let err = write_col_tab_row(
            &mut r,
            &ColTabRow {
                owner_id: 16,
                owner_kind: 0,
                col_idx: 1 << 20,
                name: "c",
                type_code: 4,
                is_nullable: false,
                fk_table_id: 0,
                fk_col_idx: 0,
                is_serial: false,
                is_hidden: false,
            },
            1,
        )
        .unwrap_err();
        assert!(err.contains("exceeds maximum"), "{err}");
        assert!(r.vals.is_empty(), "a rejected row must emit nothing");
    }
}
