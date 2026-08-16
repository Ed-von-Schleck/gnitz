//! The system-catalog **row codecs**: one writer per family, expressed against
//! the same `*_PAY_*` payload positions the readers on both sides already use.
//!
//! A catalog row used to be written twice — once in the client, once in the
//! engine — and both writers were positional while both readers were
//! name-resolved. Reordering a family's column list therefore kept every reader
//! correct and silently transposed both writers, in two crates. Here each row is
//! a struct with named fields, there is one writer per family, and the order it
//! emits payload values in is pinned to the `*_PAY_*` constants at compile time.
//!
//! A `-1` row must reproduce its `+1`'s payload byte-for-byte — the engine's
//! retraction CAS rejects a mismatch, and only byte-equal `(PK, payload)` rows
//! cancel in the Z-set — which is the other reason each family has exactly one
//! writer: a drop and its create cannot diverge if they are the same code.

use crate::{
    pack_col_id, COLTAB_PAY_COL_IDX, COLTAB_PAY_FK_COL_IDX, COLTAB_PAY_FK_TABLE_ID, COLTAB_PAY_IS_HIDDEN,
    COLTAB_PAY_IS_NULLABLE, COLTAB_PAY_IS_SERIAL, COLTAB_PAY_NAME, COLTAB_PAY_OWNER_ID, COLTAB_PAY_OWNER_KIND,
    COLTAB_PAY_TYPE_CODE, COL_TAB_COLS, IDXTAB_PAY_IS_UNIQUE, IDXTAB_PAY_NAME, IDXTAB_PAY_OWNER_ID,
    IDXTAB_PAY_SOURCE_COLS, IDX_TAB_COLS, TABLE_TAB_COLS, TABTAB_PAY_FLAGS, TABTAB_PAY_NAME, TABTAB_PAY_PK_COL_IDX,
    TABTAB_PAY_SCHEMA_ID, VIEWTAB_PAY_NAME, VIEWTAB_PAY_PK_COL_IDX, VIEWTAB_PAY_SCHEMA_ID, VIEWTAB_PAY_SQL,
    VIEW_TAB_COLS,
};

/// True iff `pay` lists **every** payload column of a family whose column list
/// is `n_cols` long (the leading column being its key), in ascending payload
/// order. Applied below to each writer's emit sequence, so a reordered or
/// resized column list fails the build here instead of transposing rows that
/// still decode without error.
const fn emits_every_payload_column(pay: &[usize], n_cols: usize) -> bool {
    if pay.len() + 1 != n_cols {
        return false;
    }
    let mut i = 0;
    while i < pay.len() {
        if pay[i] != i {
            return false;
        }
        i += 1;
    }
    true
}

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
    fn end_row(&mut self);
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

const _: () = assert!(
    emits_every_payload_column(
        &[
            COLTAB_PAY_OWNER_ID,
            COLTAB_PAY_OWNER_KIND,
            COLTAB_PAY_COL_IDX,
            COLTAB_PAY_NAME,
            COLTAB_PAY_TYPE_CODE,
            COLTAB_PAY_IS_NULLABLE,
            COLTAB_PAY_FK_TABLE_ID,
            COLTAB_PAY_FK_COL_IDX,
            COLTAB_PAY_IS_SERIAL,
            COLTAB_PAY_IS_HIDDEN,
        ],
        COL_TAB_COLS.len()
    ),
    "the COL_TAB writer's emit order must match the payload layout"
);

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

const _: () = assert!(
    emits_every_payload_column(
        &[
            TABTAB_PAY_SCHEMA_ID,
            TABTAB_PAY_NAME,
            TABTAB_PAY_PK_COL_IDX,
            TABTAB_PAY_FLAGS,
        ],
        TABLE_TAB_COLS.len()
    ),
    "the TABLE_TAB writer's emit order must match the payload layout"
);

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
}

const _: () = assert!(
    emits_every_payload_column(
        &[
            VIEWTAB_PAY_SCHEMA_ID,
            VIEWTAB_PAY_NAME,
            VIEWTAB_PAY_SQL,
            VIEWTAB_PAY_PK_COL_IDX,
        ],
        VIEW_TAB_COLS.len()
    ),
    "the VIEW_TAB writer's emit order must match the payload layout"
);

pub fn write_view_tab_row(sink: &mut impl SysRowSink, r: &ViewTabRow, weight: i64) {
    sink.begin_row(r.view_id as u128, weight);
    sink.put_u64(r.schema_id);
    sink.put_string(r.name);
    sink.put_string(r.sql_definition);
    sink.put_u64(r.pk_col_idx);
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

const _: () = assert!(
    emits_every_payload_column(
        &[
            IDXTAB_PAY_OWNER_ID,
            IDXTAB_PAY_SOURCE_COLS,
            IDXTAB_PAY_NAME,
            IDXTAB_PAY_IS_UNIQUE,
        ],
        IDX_TAB_COLS.len()
    ),
    "the IDX_TAB writer's emit order must match the payload layout"
);

pub fn write_idx_tab_row(sink: &mut impl SysRowSink, r: &IdxTabRow, weight: i64) {
    sink.begin_row(r.index_id as u128, weight);
    sink.put_u64(r.owner_id);
    sink.put_u64(r.source_col_idx);
    sink.put_string(r.name);
    sink.put_u64(r.is_unique);
    sink.end_row();
}

#[cfg(test)]
mod tests {
    use super::*;

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
