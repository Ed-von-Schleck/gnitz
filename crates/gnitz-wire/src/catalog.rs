//! The system catalog: the system families — their registry, and per family the
//! row, payload slots, writer and reader one `sys_row!` declaration yields, so
//! the client's rows and the engine's are laid out and read back by the same
//! list — the stored-shape digest and [`EXPR_BLOB_VERSION`], the system and user
//! id ranges, and [`bool_word`].

use std::sync::Arc;

use crate::{
    ColType, ColumnDef, PkColList, PkListRole, RowSource, TableDistribution, TableProps, TypeCode, ViewBudget,
    ViewProps,
};

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
    const FAMILY: SysFamily;
    fn write(&self, sink: &mut (impl SysRowSink + ?Sized), weight: i64);
}

/// One column of a system family: its name and type.
pub struct WireSysCol {
    pub name: &'static str,
    pub type_code: TypeCode,
}

/// One system family's wire identity: the table id both sides address it by,
/// its name, and the column shape they each build their schema type from.
/// Grouping these means a caller holding a table id can *derive* the shape
/// instead of being handed a separately-chosen one that may not match.
pub struct WireSysFamily {
    pub id: u64,
    /// The family's name in `TABLE_TAB`.
    pub name: &'static str,
    pub cols: &'static [WireSysCol],
    pub pk_cols: &'static [u32],
}

impl WireSysFamily {
    /// The family's columns as the column definitions every schema of it is
    /// built from: non-nullable, as every system column is.
    pub fn column_defs(&self) -> impl Iterator<Item = ColumnDef> {
        self.cols.iter().map(|c| ColumnDef::new(c.name, c.type_code, false))
    }
}

/// Declare the system families from one list in apply order: per family its
/// [`SysFamily`] variant, the [`WireSysFamily`] [`SysFamily::def`] returns, what
/// a row is called in a message, and the row the field list yields through
/// `sys_row!`.
macro_rules! sys_families {
    (
        $(#[$meta:meta])*
        pub enum SysFamily;
        $(
            $(#[$rmeta:meta])*
            $Variant:ident, id $id:literal, $name:literal, $noun:literal,
            $Row:ident $(<$lt:lifetime>)?, $Slot:ident -> $E:ty,
            key { $($k:ident),+ $(,)? }
            payload { $($(#[$pmeta:meta])* $p:ident: $tc:ident),+ $(,)? }
        )+
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, PartialEq, Eq, Debug)]
        pub enum SysFamily {
            $($Variant,)+
        }

        impl SysFamily {
            pub const COUNT: usize = [$(SysFamily::$Variant),+].len();

            /// Every family, in apply order.
            pub const ALL: [SysFamily; Self::COUNT] = [$(SysFamily::$Variant),+];

            /// This family's wire identity: its table id, its name and its column
            /// shape.
            pub const fn def(self) -> &'static WireSysFamily {
                match self {
                    $(SysFamily::$Variant => {
                        const DEF: WireSysFamily = WireSysFamily {
                            id: $id,
                            name: $name,
                            cols: &[
                                $(WireSysCol { name: stringify!($k), type_code: TypeCode::U64 },)+
                                $(WireSysCol { name: stringify!($p), type_code: sys_row!(@tc $tc) },)+
                            ],
                            pk_cols: SYS_KEY_COLS.split_at([$(stringify!($k)),+].len()).0,
                        };
                        &DEF
                    })+
                }
            }

            /// What one of this family's rows is called in a message.
            pub const fn row_noun(self) -> &'static str {
                match self {
                    $(SysFamily::$Variant => $noun,)+
                }
            }

            /// The family whose table id is `id`; `None` for any other id. Two
            /// families sharing an id would be an unreachable arm here, which the
            /// workspace lints refuse.
            pub const fn from_id(id: u64) -> Option<Self> {
                match id {
                    $($id => Some(SysFamily::$Variant),)+
                    _ => None,
                }
            }
        }

        $(sys_row! {
            $(#[$rmeta])*
            $Variant, $Row $(<$lt>)?, $Slot -> $E,
            key { $($k),+ }
            payload { $($(#[$pmeta])* $p: $tc),+ }
        })+
    };
}

/// One system family's row: the struct holding the cells a row stores, its
/// payload-slot enum, and the writer and the reader, which decodes every typed
/// cell.
macro_rules! sys_row {
    (
        $(#[$meta:meta])*
        $Variant:ident, $Row:ident $(<$lt:lifetime>)?, $Slot:ident -> $E:ty,
        key { $($k:ident),+ $(,)? }
        payload { $($(#[$pmeta:meta])* $p:ident: $tc:ident),+ $(,)? }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $Row $(<$lt>)? {
            $(pub $k: u64,)+
            $($(#[$pmeta])* pub $p: sys_row!(@ty $tc),)+
        }

        impl $(<$lt>)? SysRow for $Row $(<$lt>)? {
            const FAMILY: SysFamily = SysFamily::$Variant;
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
    // `Bool`, `Key` and `Cols` are stored as `U64` words: a boolean, and a
    // [`PkColList::pack`] read as a primary key or as an index's column list.
    (@tc String) => { TypeCode::String };
    (@tc Blob) => { TypeCode::Blob };
    (@tc $other:ident) => { TypeCode::U64 };
    (@ty U64) => { u64 };
    (@ty Bool) => { bool };
    (@ty Key) => { PkColList };
    (@ty Cols) => { PkColList };
    (@ty String) => { std::borrow::Cow<'a, str> };
    (@ty Blob) => { std::borrow::Cow<'a, [u8]> };
    (@get $s:ident, $r:ident, $pi:expr, $p:ident, U64) => { crate::payload_u64($s, $r, $pi) };
    (@get $s:ident, $r:ident, $pi:expr, $p:ident, Bool) => {
        bool_word(crate::payload_u64($s, $r, $pi)).map_err(|e| format!("{}: {e}", stringify!($p)))?
    };
    (@get $s:ident, $r:ident, $pi:expr, $p:ident, Key) => {
        PkColList::unpack_as(crate::payload_u64($s, $r, $pi), PkListRole::PrimaryKey)?
    };
    (@get $s:ident, $r:ident, $pi:expr, $p:ident, Cols) => {
        PkColList::unpack_as(crate::payload_u64($s, $r, $pi), PkListRole::ColumnList)?
    };
    (@get $s:ident, $r:ident, $pi:expr, $p:ident, Blob) => { crate::payload_bytes($s, $r, $pi) };
    (@get $s:ident, $r:ident, $pi:expr, $p:ident, String) => {
        crate::payload_str($s, $r, $pi).map_err(|_| format!("{} is not UTF-8", stringify!($p)))?
    };
    (@put $s:ident, $v:expr, U64) => { $s.put_u64($v) };
    (@put $s:ident, $v:expr, Bool) => { $s.put_u64($v as u64) };
    (@put $s:ident, $v:expr, Key) => { $s.put_u64($v.pack()) };
    (@put $s:ident, $v:expr, Cols) => { $s.put_u64($v.pack()) };
    (@put $s:ident, $v:expr, String) => { $s.put_string(&$v) };
    (@put $s:ident, $v:expr, Blob) => { $s.put_bytes(&$v) };
}

sys_families! {
    /// A system family. Declared in apply order — the engine applies a bundle's
    /// families, and a boot's replay, in this order — and the discriminant is the
    /// position, the index every per-family array on either side is laid out by.
    pub enum SysFamily;

    /// One `SCHEMA_TAB` row: the schema `schema_id`, named `name`.
    Schema, id 1, "schemas", "schema",
    SchemaTabRow<'a>, SchemaTabSlot -> String,
    key { schema_id }
    payload { name: String }

    /// One `COL_TAB` row: column `col_idx` of the table or view `owner_id`. The
    /// pair is a column record's identity.
    Column, id 4, "columns", "column",
    ColTabRow<'a>, ColTabSlot -> String,
    key { owner_id, col_idx }
    payload {
        name: String,
        /// [`ColType`]'s type code.
        type_code: U64,
        /// The column admits NULL.
        is_nullable: Bool,
        /// The parent table of a foreign key; `0` for none, which no relation has.
        fk_table_id: U64,
        fk_col_idx: U64,
        /// [`FkAction`]'s word: what a delete of the referenced row does to
        /// this one. `0` where the column references nothing.
        fk_on_delete: U64,
        /// The column is hidden — a hidden key slot or a dropped column — echoed
        /// into schema records.
        is_hidden: Bool,
        /// [`ColType`]'s scale: a DECIMAL column's, else `0`.
        scale: U64,
    }

    /// `view_id`'s one `CIRCUIT_TAB` row.
    Circuit, id 11, "circuits", "circuit",
    CircuitRow<'a>, CircuitSlot -> std::convert::Infallible,
    key { view_id }
    payload {
        /// The view's whole circuit, as [`crate::Circuit::encode`] lays it out.
        circuit: Blob,
    }

    /// One `TABLE_TAB` row.
    Table, id 2, "tables", "table",
    TableTabRow<'a>, TableTabSlot -> String,
    key { table_id }
    payload {
        schema_id: U64,
        name: String,
        pk_col_idx: Key,
        /// [`TableProps::stream`].
        stream: Bool,
        /// [`TableProps::serial`].
        serial: Bool,
        /// The table is [`TableDistribution::Replicated`].
        replicated: Bool,
        /// A keyed table's [`TableDistribution::Keyed`] prefix length, `0` for
        /// the full PK; `0` for a replicated table.
        dist_prefix: U64,
        /// [`TableProps::compress`].
        compress: Bool,
    }

    /// One `VIEW_TAB` row.
    View, id 3, "views", "view",
    ViewTabRow<'a>, ViewTabSlot -> String,
    key { view_id }
    payload {
        schema_id: U64,
        name: String,
        pk_col_idx: Key,
        /// `WITH (capacity = …)` in bytes, `0` absent.
        capacity_bytes: U64,
        /// `WITH (delta = …)` in bytes, `0` absent.
        delta_bytes: U64,
        /// The user view this row is an internal chain segment of; `0` is a user
        /// view.
        owner_view_id: U64,
        /// Two of the view's rows may carry the same PK, or one may stand at
        /// weight above 1, so its PK region identifies no single row. The
        /// planner that compiled the view states it; the engine stores it
        /// verbatim.
        pk_repeats: Bool,
        /// [`ViewProps::compress`].
        compress: Bool,
        /// The user view's body as its planner recorded it, for a client to
        /// write the view again; the engine stores it verbatim and reads none of
        /// it. Empty for a chain segment, and for a view no planner wrote.
        definition: Blob,
    }

    /// One `IDX_TAB` row: the index `index_id` on columns of `owner_id`.
    Index, id 5, "indices", "index",
    IdxTabRow<'a>, IdxTabSlot -> String,
    key { index_id }
    payload {
        owner_id: U64,
        /// The indexed columns, single- and multi-column indexes alike.
        source_col_idx: Cols,
        name: String,
        /// The index enforces uniqueness.
        is_unique: Bool,
    }

    /// One `SEQ_TAB` row.
    Sequence, id 7, "sequences", "sequence",
    SeqTabRow, SeqTabSlot -> std::convert::Infallible,
    key { seq_id }
    payload { next_val: U64 }
}

impl SysFamily {
    /// This family's position in [`Self::ALL`].
    #[inline]
    pub const fn index(self) -> usize {
        self as usize
    }

    /// This family's table id.
    #[inline]
    pub const fn id(self) -> u64 {
        self.def().id
    }

    /// This family's name in `TABLE_TAB`.
    #[inline]
    pub const fn name(self) -> &'static str {
        self.def().name
    }
}

/// A system family's key columns are a prefix of these: its leading one or two,
/// each a `U64`.
const SYS_KEY_COLS: &[u32] = &[0, 1];

/// The OPK of system id `id`: a single-column family's whole key, and the
/// leading-column prefix of a pair-keyed one's. Every system key column is a U64.
pub fn sys_key(id: u64) -> [u8; 8] {
    id.to_be_bytes()
}

/// The leading key column of a system row's OPK key — the inverse of [`sys_key`]
/// over its first eight bytes.
pub fn sys_leading_id(key: &[u8]) -> u64 {
    u64::from_be_bytes(key[..8].try_into().expect("a system key leads with a U64 column"))
}

// ---------------------------------------------------------------------------
// Payload slots TABLE_TAB and VIEW_TAB share
// ---------------------------------------------------------------------------
//
// An address valid in both families, for the readers that take either through
// one code path.

/// `schema_id`'s payload slot in TABLE_TAB and VIEW_TAB alike.
pub const RELTAB_PAY_SCHEMA_ID: usize = {
    let i = TableTabSlot::schema_id as usize;
    assert!(
        i == ViewTabSlot::schema_id as usize,
        "TABLE_TAB and VIEW_TAB disagree on schema_id"
    );
    i
};

/// `name`'s payload slot in TABLE_TAB and VIEW_TAB alike.
pub const RELTAB_PAY_NAME: usize = {
    let i = TableTabSlot::name as usize;
    assert!(
        i == ViewTabSlot::name as usize,
        "TABLE_TAB and VIEW_TAB disagree on name"
    );
    i
};

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

/// A catalog column: the logical column plus its FK, already resolved — what a
/// `COL_TAB` row states, as [`ColTabRow::column`] reads it back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogColumn {
    pub def: ColumnDef,
    pub fk: Option<FkRef>,
}

impl<'a> ColTabRow<'a> {
    /// Column `col_idx` of `owner_id` as `col` declares it, referencing `fk` —
    /// the **resolved** parent column: a client's deferred self-reference is
    /// substituted before a row reaches here.
    pub fn of(owner_id: u64, col_idx: u64, col: &'a ColumnDef, fk: Option<FkRef>) -> Self {
        let (fk_table_id, fk_col_idx, fk_on_delete) = fk.map_or((0, 0, 0), |fk| {
            (fk.table_id, fk.col as u64, fk.on_delete.as_wire() as u64)
        });
        ColTabRow {
            owner_id,
            col_idx,
            name: (&*col.name).into(),
            type_code: col.ty.tc.as_wire() as u64,
            is_nullable: col.is_nullable,
            fk_table_id,
            fk_col_idx,
            fk_on_delete,
            is_hidden: col.is_hidden,
            scale: col.ty.scale as u64,
        }
    }

    /// Push this row's column onto `defs`, the columns its owner's earlier rows
    /// described. Records must be keyed 0,1,2,… with no gap or duplicate: every
    /// consumer maps columns positionally.
    pub fn append_to(&self, defs: &mut Vec<CatalogColumn>) -> Result<(), String> {
        let expected = defs.len() as u64;
        if self.col_idx != expected {
            return Err(format!(
                "column records are non-contiguous; expected index {expected}, got {}",
                self.col_idx
            ));
        }
        defs.push(self.column()?);
        Ok(())
    }

    /// The column this row describes and the column it references:
    /// [`Self::of`]'s inverse. Every word must fit the width it is stored at and
    /// decode to a value `of` emits, so `of` over the result reproduces this
    /// row.
    pub fn column(&self) -> Result<CatalogColumn, String> {
        let word = |field: &str, w: u64, max: u64| {
            if w > max {
                return Err(format!(
                    "column record carries {field} = {w}, past the {max} its stored width holds"
                ));
            }
            Ok(w)
        };
        let code = word("type_code", self.type_code, u8::MAX as u64)? as u8;
        let scale = word("scale", self.scale, u8::MAX as u64)? as u8;
        let ty = ColType::from_wire(code, scale)
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
        let def = ColumnDef {
            name: Arc::from(&*self.name),
            ty,
            is_nullable: self.is_nullable,
            is_hidden: self.is_hidden,
        };
        Ok(CatalogColumn { def, fk })
    }
}

impl<'a> TableTabRow<'a> {
    /// Table `table_id` of schema `schema_id`, named `name`, keyed `pk_col_idx`,
    /// with `props`.
    pub fn of(
        table_id: u64,
        schema_id: u64,
        name: impl Into<std::borrow::Cow<'a, str>>,
        pk_col_idx: PkColList,
        props: TableProps,
    ) -> Self {
        let (replicated, dist_prefix) = match props.distribution {
            TableDistribution::Keyed { prefix_len } => (false, prefix_len as u64),
            TableDistribution::Replicated => (true, 0),
        };
        TableTabRow {
            table_id,
            schema_id,
            name: name.into(),
            pk_col_idx,
            stream: props.stream,
            serial: props.serial,
            replicated,
            dist_prefix,
            compress: props.compress,
        }
    }

    /// The row's properties: [`Self::of`]'s inverse, held to the rules they must
    /// meet against the row's key.
    pub fn props(&self) -> Result<TableProps, String> {
        let prefix_len = u8::try_from(self.dist_prefix).map_err(|_| {
            format!(
                "table record carries dist_prefix = {}, past {}",
                self.dist_prefix,
                u8::MAX
            )
        })?;
        let distribution = if self.replicated {
            TableDistribution::Replicated
        } else {
            TableDistribution::default()
        }
        .cluster_by(prefix_len)?;
        let props = TableProps {
            stream: self.stream,
            serial: self.serial,
            distribution,
            compress: self.compress,
        };
        props.validate(self.pk_col_idx.as_slice().len())?;
        Ok(props)
    }
}

/// What a `VIEW_TAB` row states about its view beyond its name and key, decoded.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ViewParts {
    pub props: ViewProps,
    /// The user view this row is a chain segment of; `None` for a user view.
    pub owner: Option<u64>,
    /// [`ViewTabRow::pk_repeats`].
    pub pk_repeats: bool,
}

impl<'a> ViewTabRow<'a> {
    /// The row for view `view_id`: [`Self::parts`]'s inverse.
    pub fn of(
        view_id: u64,
        schema_id: u64,
        name: impl Into<std::borrow::Cow<'a, str>>,
        pk_col_idx: PkColList,
        parts: ViewParts,
        definition: impl Into<std::borrow::Cow<'a, [u8]>>,
    ) -> Self {
        debug_assert_ne!(parts.owner, Some(0), "owner 0 is a user view's word");
        let (capacity_bytes, delta_bytes) = parts.props.budget.row_words();
        ViewTabRow {
            view_id,
            schema_id,
            name: name.into(),
            pk_col_idx,
            capacity_bytes,
            delta_bytes,
            owner_view_id: parts.owner.unwrap_or(0),
            pk_repeats: parts.pk_repeats,
            compress: parts.props.compress,
            definition: definition.into(),
        }
    }

    /// The row's words beyond its name and key, decoded and held to the rule that
    /// a chain segment, which the planner mints, carries no `WITH` option.
    pub fn parts(&self) -> Result<ViewParts, String> {
        let budget = ViewBudget::from_row(self.capacity_bytes, self.delta_bytes)?;
        let props = ViewProps { budget, compress: self.compress };
        let owner = (self.owner_view_id != 0).then_some(self.owner_view_id);
        if owner.is_some() && props != ViewProps::default() {
            return Err("is a chain segment carrying a WITH option".to_string());
        }
        if owner.is_some() && !self.definition.is_empty() {
            return Err("is a chain segment carrying a definition".to_string());
        }
        Ok(ViewParts {
            props,
            owner,
            pk_repeats: self.pk_repeats,
        })
    }
}

// ---------------------------------------------------------------------------
// Stored-shape digest
// ---------------------------------------------------------------------------

const FNV_PRIME: u64 = 0x100_0000_01b3;

/// One FNV-1a step per byte of `b`.
const fn fnv_bytes(mut h: u64, b: &[u8]) -> u64 {
    let mut i = 0;
    while i < b.len() {
        h = (h ^ b[i] as u64).wrapping_mul(FNV_PRIME);
        i += 1;
    }
    h
}

/// FNV-1a over one family's stored identity: its id, its `TABLE_TAB` name, each
/// column's name and type, and its key columns.
const fn fold_family(mut h: u64, f: &WireSysFamily) -> u64 {
    let (cols, pk) = (f.cols, f.pk_cols);
    h = (h ^ f.id).wrapping_mul(FNV_PRIME);
    h = fnv_bytes(h, f.name.as_bytes());
    let mut i = 0;
    while i < cols.len() {
        h = fnv_bytes(h, cols[i].name.as_bytes());
        h = (h ^ cols[i].type_code as u64).wrapping_mul(FNV_PRIME);
        i += 1;
    }
    let mut k = 0;
    while k < pk.len() {
        h = (h ^ pk[k] as u64).wrapping_mul(FNV_PRIME);
        k += 1;
    }
    h
}

/// Digest of every system family's stored shape, and of the blob layouts stored
/// *inside* one. Shards and SAL payloads are decoded against the *live* schema, so
/// a shape change silently reinterprets an existing data directory unless a
/// format word rejects it first. Both do: the engine's `SHARD_VERSION` and
/// [`crate::WIRE_VERSION`] each XOR this in, so each moves when a family's shape
/// does, with no hand bump.
///
/// The family half is folded straight over [`SysFamily::ALL`], so a family added
/// there is covered without a second edit. The version consts cover what that
/// fold cannot see: a `Blob` column's bytes are as durable as its neighbours',
/// but its internal layout is invisible to a fold over names and types.
pub const SYS_SCHEMA_DIGEST: u64 = {
    let mut h = 0xcbf2_9ce4_8422_2325;
    let mut i = 0;
    while i < SysFamily::COUNT {
        h = fold_family(h, SysFamily::ALL[i].def());
        i += 1;
    }
    h = (h ^ crate::circuit::CIRCUIT_VERSION as u64).wrapping_mul(FNV_PRIME);
    h = (h ^ EXPR_BLOB_VERSION as u64).wrapping_mul(FNV_PRIME);
    h
};

/// Version of the compiled expression-program blob `gnitz-expr` lays out, folded
/// into [`SYS_SCHEMA_DIGEST`].
pub const EXPR_BLOB_VERSION: u8 = 9;

// ---------------------------------------------------------------------------
// Object-id ranges
// ---------------------------------------------------------------------------

pub const FIRST_USER_TABLE_ID: u64 = 16;

/// A tripwire on catalog object-id allocation, not a live limit: reaching it
/// needs 2^31 durable CREATEs. Schema, relation and index ids all live below it,
/// and the engine rejects any id at or above it where one enters the catalog.
pub const CATALOG_ID_CEILING: u64 = 1 << 31;

// ---------------------------------------------------------------------------
// Boolean cells
// ---------------------------------------------------------------------------

/// A boolean carried as a u64 word (a catalog column or a request arg): `0` or
/// `1`, any other word refused.
pub fn bool_word(w: u64) -> Result<bool, String> {
    match w {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(format!("boolean word {w} is neither 0 nor 1")),
    }
}

#[cfg(test)]
#[path = "tests/catalog.rs"]
mod tests;
