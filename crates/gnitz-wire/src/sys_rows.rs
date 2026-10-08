//! The system-catalog rows: one [`sys_row!`] declaration per family, from which
//! the family's shape, its row, its payload slots, its writer and its reader
//! all come, so the client's rows and the engine's are laid out and read back by
//! the same list.

use crate::RowSource;

/// Where a row writer emits a row. Both sides already build batches this way —
/// begin a row with its key and weight, push one value per payload column in
/// schema order, close it.
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

/// One row of a system family: the cells it stores, as the client's DDL and the
/// engine's seed rows write them.
pub trait SysRow {
    /// The family the row belongs to.
    const FAMILY: u64;
    fn write(&self, sink: &mut (impl SysRowSink + ?Sized), weight: i64);
}

/// Declare one system family from one field list: its shape, the struct holding
/// the cells a row stores, its payload-slot enum, and the writer and reader.
macro_rules! sys_row {
    (
        $(#[$meta:meta])*
        $FAMILY:ident, $shape:ident, $Row:ident $(<$lt:lifetime>)?, $Slot:ident -> $E:ty,
        key { $($k:ident),+ $(,)? }
        payload { $($(#[$pmeta:meta])* $p:ident: $tc:ident),+ $(,)? }
    ) => {
        pub(crate) const $shape: crate::catalog::SysShape = crate::catalog::SysShape {
            cols: &[
                $(crate::catalog::col(stringify!($k), crate::TypeCode::U64),)+
                $(crate::catalog::col(stringify!($p), crate::TypeCode::$tc),)+
            ],
            key_len: [$(stringify!($k)),+].len(),
        };

        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub struct $Row $(<$lt>)? {
            $(pub $k: u64,)+
            $($(#[$pmeta])* pub $p: sys_row!(@ty $tc),)+
        }

        impl $(<$lt>)? SysRow for $Row $(<$lt>)? {
            const FAMILY: u64 = crate::$FAMILY;
            fn write(&self, sink: &mut (impl SysRowSink + ?Sized), weight: i64) {
                sink.begin_row(&[$(self.$k as u128),+], weight);
                $(sys_row!(@put sink, self.$p, $tc);)+
                sink.end_row();
            }
        }

        #[doc = concat!("Payload slots of [`", stringify!($Row), "`], in column order.")]
        #[allow(non_camel_case_types)]
        #[derive(Clone, Copy, Debug)]
        pub enum $Slot {
            $($(#[$pmeta])* $p,)+
        }

        impl $(<$lt>)? $Row $(<$lt>)? {
            /// The row [`SysRow::write`] wrote, read back from `row` of `src`.
            pub fn read<S: RowSource>(src: &$($lt)? S, row: usize) -> Result<Self, $E> {
                let mut key = src
                    .get_pk_bytes(row)
                    .as_chunks::<8>()
                    .0
                    .iter()
                    .map(|w| u64::from_be_bytes(*w));
                Ok($Row {
                    $($k: key.next().expect("a system key is one u64 word per key column"),)+
                    $($p: sys_row!(@get src, row, $Slot::$p as usize, $p, $tc),)+
                })
            }
        }
    };
    (@ty U64) => { u64 };
    (@ty String) => { &'a str };
    (@ty Blob) => { &'a [u8] };
    (@get $s:ident, $r:ident, $pi:expr, $p:ident, U64) => { crate::payload_u64($s, $r, $pi) };
    (@get $s:ident, $r:ident, $pi:expr, $p:ident, Blob) => { crate::payload_bytes($s, $r, $pi) };
    (@get $s:ident, $r:ident, $pi:expr, $p:ident, String) => {
        crate::payload_str($s, $r, $pi).map_err(|_| format!("{} is not UTF-8", stringify!($p)))?
    };
    (@put $s:ident, $v:expr, U64) => { $s.put_u64($v) };
    (@put $s:ident, $v:expr, String) => { $s.put_string($v) };
    (@put $s:ident, $v:expr, Blob) => { $s.put_bytes($v) };
}

sys_row! {
    /// One `SCHEMA_TAB` row: the schema `schema_id`, named `name`.
    SCHEMA_TAB, SCHEMA_TAB_SHAPE, SchemaTabRow<'a>, SchemaTabSlot -> String,
    key { schema_id }
    payload { name: String }
}

sys_row! {
    /// One `TABLE_TAB` row.
    TABLE_TAB, TABLE_TAB_SHAPE, TableTabRow<'a>, TableTabSlot -> String,
    key { table_id }
    payload {
        schema_id: U64,
        name: String,
        /// [`crate::PkColList::pack`].
        pk_col_idx: U64,
        /// [`crate::TableProps::pack`].
        flags: U64,
    }
}

sys_row! {
    /// One `VIEW_TAB` row.
    VIEW_TAB, VIEW_TAB_SHAPE, ViewTabRow<'a>, ViewTabSlot -> String,
    key { view_id }
    payload {
        schema_id: U64,
        name: String,
        /// [`crate::PkColList::pack`].
        pk_col_idx: U64,
        /// [`crate::ViewProps::row_words`]: `WITH (capacity = …)` in bytes, `0`
        /// absent.
        capacity_bytes: U64,
        /// [`crate::ViewProps::row_words`]: `WITH (delta = …)` in bytes, `0` absent.
        delta_bytes: U64,
        /// The user view this row is an internal chain segment of; `0` is a user
        /// view. A column, so the precheck can validate it against a forger.
        owner_view_id: U64,
        /// A boolean ([`crate::bool_word`]): two of the view's rows may carry the
        /// same PK, or one may stand at weight above 1, so its PK region
        /// identifies no single row. The planner that compiled the view states
        /// it; the engine stores it verbatim.
        pk_repeats: U64,
    }
}

sys_row! {
    /// One `COL_TAB` row: column `col_idx` of the table or view `owner_id`. The
    /// pair is a column record's identity.
    COL_TAB, COL_TAB_SHAPE, ColTabRow<'a>, ColTabSlot -> String,
    key { owner_id, col_idx }
    payload {
        name: String,
        /// [`crate::ColType`]'s type code.
        type_code: U64,
        is_nullable: U64,
        /// The parent table of a foreign key; `0` for none, which no relation has.
        fk_table_id: U64,
        fk_col_idx: U64,
        /// [`FkAction`]'s word: what a delete of the referenced row does to
        /// this one. `0` where the column references nothing.
        fk_on_delete: U64,
        /// `1` for a hidden key slot, echoed into reply schema blocks.
        is_hidden: U64,
        /// [`crate::ColType`]'s scale: a DECIMAL column's, else `0`.
        scale: U64,
    }
}

sys_row! {
    /// One `IDX_TAB` row: the index `index_id` on columns of `owner_id`.
    IDX_TAB, IDX_TAB_SHAPE, IdxTabRow<'a>, IdxTabSlot -> String,
    key { index_id }
    payload {
        owner_id: U64,
        /// The indexed columns' [`crate::PkColList::pack`], single- and
        /// multi-column indexes alike.
        source_col_idx: U64,
        name: String,
        /// A boolean ([`crate::bool_word`]): the index enforces uniqueness.
        is_unique: U64,
    }
}

sys_row! {
    /// One `SEQ_TAB` row.
    SEQ_TAB, SEQ_TAB_SHAPE, SeqTabRow, SeqTabSlot -> std::convert::Infallible,
    key { seq_id }
    payload { next_val: U64 }
}

sys_row! {
    /// `view_id`'s one `CIRCUIT_TAB` row.
    CIRCUIT_TAB, CIRCUIT_TAB_SHAPE, CircuitRow<'a>, CircuitSlot -> std::convert::Infallible,
    key { view_id }
    payload {
        /// The view's whole circuit, as [`crate::Circuit::encode`] lays it out.
        circuit: Blob,
    }
}

wire_enum! {
    /// What deleting a referenced row does to the rows referencing it.
    pub enum FkAction: u8 {
        /// The delete is refused while a referencing row remains.
        Restrict = 0,
        /// The referencing rows are deleted with it.
        Cascade = 1,
    }
}

/// The column a FOREIGN KEY column references, and what a delete there does here.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FkRef {
    pub table_id: u64,
    pub col: u32,
    pub on_delete: FkAction,
}

impl<'a> ColTabRow<'a> {
    /// Column `col_idx` of `owner_id` as `col` declares it, referencing `fk` —
    /// the **resolved** parent column: a client's deferred self-reference is
    /// substituted before a row reaches here.
    pub fn of(owner_id: u64, col_idx: u64, col: &'a crate::ColumnDef, fk: Option<FkRef>) -> Self {
        let (fk_table_id, fk_col_idx, fk_on_delete) = fk.map_or((0, 0, 0), |fk| {
            (fk.table_id, fk.col as u64, fk.on_delete.as_wire() as u64)
        });
        ColTabRow {
            owner_id,
            col_idx,
            name: &col.name,
            type_code: col.ty.tc.as_wire() as u64,
            is_nullable: col.is_nullable as u64,
            fk_table_id,
            fk_col_idx,
            fk_on_delete,
            is_hidden: col.is_hidden as u64,
            scale: col.ty.scale as u64,
        }
    }

    /// The column this row describes and the column it references:
    /// [`Self::of`]'s inverse. Every word must fit the width it is stored at and
    /// decode to a value `of` could emit: an overflowing one would leave a stored
    /// row no client can reproduce, and so no later `-1` can retract.
    pub fn column(&self) -> Result<(crate::ColumnDef, Option<FkRef>), String> {
        let word = |field: &str, w: u64, max: u64| {
            if w > max {
                return Err(format!(
                    "column record carries {field} = {w}, past the {max} its stored width holds"
                ));
            }
            Ok(w)
        };
        let flag = |field: &str, w: u64| word(field, w, 1).map(|w| w == 1);
        let code = word("type_code", self.type_code, u8::MAX as u64)? as u8;
        let scale = word("scale", self.scale, u8::MAX as u64)? as u8;
        let ty = crate::ColType::from_wire(code, scale)
            .ok_or_else(|| format!("column record carries an invalid column type {code}/{scale}"))?;
        let fk_col = word("fk_col_idx", self.fk_col_idx, u32::MAX as u64)? as u32;
        let on_delete = word("fk_on_delete", self.fk_on_delete, u8::MAX as u64)?;
        let on_delete = FkAction::from_wire(on_delete as u8)
            .ok_or_else(|| format!("column record carries an invalid fk_on_delete {on_delete}"))?;
        let fk = match (self.fk_table_id, fk_col) {
            (0, 0) if on_delete == FkAction::Restrict => None,
            (0, 0) => return Err("column record carries an FK action with no FK table".into()),
            (0, col) => return Err(format!("column record carries FK column {col} with no FK table")),
            (table_id, col) => Some(FkRef { table_id, col, on_delete }),
        };
        let def = crate::ColumnDef {
            name: self.name.to_owned(),
            ty,
            is_nullable: flag("is_nullable", self.is_nullable)?,
            is_hidden: flag("is_hidden", self.is_hidden)?,
        };
        Ok((def, fk))
    }
}

impl IdxTabRow<'_> {
    /// The row's `(owner_id, col_indices, is_unique)` — the one decoding of
    /// `source_col_idx`, so no consumer holds its undecoded word.
    pub fn parts(&self) -> Result<(u64, crate::PkColList, bool), String> {
        Ok((
            self.owner_id,
            crate::PkColList::unpack(self.source_col_idx)
                .map_err(|rule| rule.for_role(crate::PkListRole::ColumnList))?,
            crate::bool_word(self.is_unique).map_err(|e| format!("is_unique: {e}"))?,
        ))
    }
}

#[cfg(test)]
#[path = "tests/sys_rows.rs"]
mod tests;
