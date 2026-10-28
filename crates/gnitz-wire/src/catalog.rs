//! The system catalog: the system families — their registry, and per family the
//! row, payload slots, writer and reader one `sys_row!` declaration yields, so
//! the client's rows and the engine's are laid out and read back by the same
//! list — the stored-shape digest and [`EXPR_BLOB_VERSION`], the system and user
//! id ranges, the `TABLE_TAB`/`VIEW_TAB` property codecs and [`bool_word`], and
//! the identifier and schema-name rules.

use std::num::NonZeroU64;

use crate::{ColType, ColumnDef, PkColList, PkListRole, RowSource, TypeCode};

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
    pub fn column_defs(&self) -> impl Iterator<Item = ColumnDef> + '_ {
        self.cols.iter().map(|c| ColumnDef::new(c.name, c.type_code, false))
    }
}

/// Declare one system family from one field list: its table id const, its
/// [`WireSysFamily`], the struct holding the cells a row stores, its payload-slot
/// enum, and the writer and the reader, which decodes every typed cell.
macro_rules! sys_row {
    (
        $(#[$meta:meta])*
        $FAMILY:ident = $id:literal, $name:literal, $fam:ident, $Row:ident $(<$lt:lifetime>)?, $Slot:ident -> $E:ty,
        key { $($k:ident),+ $(,)? }
        payload { $($(#[$pmeta:meta])* $p:ident: $tc:ident),+ $(,)? }
    ) => {
        pub const $FAMILY: u64 = $id;

        pub(crate) const $fam: WireSysFamily = WireSysFamily {
            id: $FAMILY,
            name: $name,
            cols: &[
                $(WireSysCol { name: stringify!($k), type_code: TypeCode::U64 },)+
                $(WireSysCol { name: stringify!($p), type_code: sys_row!(@tc $tc) },)+
            ],
            pk_cols: SYS_KEY_COLS.split_at([$(stringify!($k)),+].len()).0,
        };

        $(#[$meta])*
        #[derive(Clone, Debug, PartialEq, Eq)]
        pub struct $Row $(<$lt>)? {
            $(pub $k: u64,)+
            $($(#[$pmeta])* pub $p: sys_row!(@ty $tc),)+
        }

        impl $(<$lt>)? SysRow for $Row $(<$lt>)? {
            const FAMILY: u64 = $FAMILY;
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

sys_row! {
    /// One `SCHEMA_TAB` row: the schema `schema_id`, named `name`.
    SCHEMA_TAB = 1, "schemas", SCHEMA_TAB_FAMILY, SchemaTabRow<'a>, SchemaTabSlot -> String,
    key { schema_id }
    payload { name: String }
}

sys_row! {
    /// One `TABLE_TAB` row.
    TABLE_TAB = 2, "tables", TABLE_TAB_FAMILY, TableTabRow<'a>, TableTabSlot -> String,
    key { table_id }
    payload {
        schema_id: U64,
        name: String,
        pk_col_idx: Key,
        /// [`TableProps::pack`].
        flags: U64,
    }
}

sys_row! {
    /// One `VIEW_TAB` row.
    VIEW_TAB = 3, "views", VIEW_TAB_FAMILY, ViewTabRow<'a>, ViewTabSlot -> String,
    key { view_id }
    payload {
        schema_id: U64,
        name: String,
        pk_col_idx: Key,
        /// [`ViewBudget::row_words`]: `WITH (capacity = …)` in bytes, `0`
        /// absent.
        capacity_bytes: U64,
        /// [`ViewBudget::row_words`]: `WITH (delta = …)` in bytes, `0` absent.
        delta_bytes: U64,
        /// The user view this row is an internal chain segment of; `0` is a user
        /// view. A column, so the precheck can validate it against a forger.
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
}

sys_row! {
    /// One `COL_TAB` row: column `col_idx` of the table or view `owner_id`. The
    /// pair is a column record's identity.
    COL_TAB = 4, "columns", COL_TAB_FAMILY, ColTabRow<'a>, ColTabSlot -> String,
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
}

sys_row! {
    /// One `IDX_TAB` row: the index `index_id` on columns of `owner_id`.
    IDX_TAB = 5, "indices", IDX_TAB_FAMILY, IdxTabRow<'a>, IdxTabSlot -> String,
    key { index_id }
    payload {
        owner_id: U64,
        /// The indexed columns, single- and multi-column indexes alike.
        source_col_idx: Cols,
        name: String,
        /// The index enforces uniqueness.
        is_unique: Bool,
    }
}

sys_row! {
    /// One `SEQ_TAB` row.
    SEQ_TAB = 7, "sequences", SEQ_TAB_FAMILY, SeqTabRow, SeqTabSlot -> std::convert::Infallible,
    key { seq_id }
    payload { next_val: U64 }
}

sys_row! {
    /// `view_id`'s one `CIRCUIT_TAB` row.
    CIRCUIT_TAB = 11, "circuits", CIRCUIT_TAB_FAMILY, CircuitRow<'a>, CircuitSlot -> std::convert::Infallible,
    key { view_id }
    payload {
        /// The view's whole circuit, as [`crate::Circuit::encode`] lays it out.
        circuit: Blob,
    }
}

/// Every system family, in the order both sides index them by. The engine
/// applies a bundle's families in this order.
pub const SYS_FAMILIES: &[WireSysFamily] = &[
    SCHEMA_TAB_FAMILY,
    COL_TAB_FAMILY,
    CIRCUIT_TAB_FAMILY,
    TABLE_TAB_FAMILY,
    VIEW_TAB_FAMILY,
    IDX_TAB_FAMILY,
    SEQ_TAB_FAMILY,
];

/// A system family's key columns are a prefix of these: its leading one or two,
/// each a `U64`.
const SYS_KEY_COLS: &[u32] = &[0, 1];

/// Position of family `id` in [`SYS_FAMILIES`], or `None` for a non-family id.
pub const fn sys_family_index(id: u64) -> Option<usize> {
    let mut i = 0;
    while i < SYS_FAMILIES.len() {
        if SYS_FAMILIES[i].id == id {
            return Some(i);
        }
        i += 1;
    }
    None
}

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
            name: col.name.as_str().into(),
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
    /// decode to a value `of` could emit: an overflowing one would leave a stored
    /// row no client can reproduce, and so no later `-1` can retract.
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
            name: self.name.to_string(),
            ty,
            is_nullable: self.is_nullable,
            is_hidden: self.is_hidden,
        };
        Ok(CatalogColumn { def, fk })
    }
}

impl TableTabRow<'_> {
    /// The row's `flags`, decoded and held to the rules the word cannot encode
    /// against its key.
    pub fn props(&self) -> Result<TableProps, String> {
        let props = TableProps::from_flags(self.flags)?;
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

impl ViewTabRow<'_> {
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
/// *inside* one. Shards and SAL frames are decoded against the *live* schema, so
/// a shape change silently reinterprets an existing data directory unless a
/// format word rejects it first. Both do: the engine's `SHARD_VERSION` and
/// [`crate::WIRE_VERSION`] each XOR this in, so each moves when a family's shape
/// does, with no hand bump.
///
/// The family half is folded straight over [`SYS_FAMILIES`], so a family added
/// there is covered without a second edit. The version consts cover what that
/// fold cannot see: a `Blob` column's bytes are as durable as its neighbours',
/// but its internal layout is invisible to a fold over names and types.
pub const SYS_SCHEMA_DIGEST: u64 = {
    let mut h = 0xcbf2_9ce4_8422_2325;
    let mut i = 0;
    while i < SYS_FAMILIES.len() {
        h = fold_family(h, &SYS_FAMILIES[i]);
        i += 1;
    }
    h = (h ^ crate::circuit::CIRCUIT_VERSION as u64).wrapping_mul(FNV_PRIME);
    h = (h ^ EXPR_BLOB_VERSION as u64).wrapping_mul(FNV_PRIME);
    h
};

/// Version of the compiled expression-program blob `gnitz-expr` lays out, folded
/// into [`SYS_SCHEMA_DIGEST`].
pub const EXPR_BLOB_VERSION: u8 = 8;

// ---------------------------------------------------------------------------
// Object-id ranges
// ---------------------------------------------------------------------------

pub const FIRST_USER_TABLE_ID: u64 = 16;

/// A tripwire on catalog object-id allocation, not a live limit: reaching it
/// needs 2^31 durable CREATEs. Schema, relation and index ids all live below it,
/// and the engine rejects any id at or above it where one enters the catalog.
pub const CATALOG_ID_CEILING: u64 = 1 << 31;

// ---------------------------------------------------------------------------
// Identifier validation (shared between the SQL planner and the engine)
// ---------------------------------------------------------------------------

/// The character set every stored catalog name is drawn from. Public because the
/// engine applies the same charset at its own trust boundary without taking the
/// rest of [`validate_user_identifier`]'s client-side policy.
pub fn is_valid_ident_char(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_'
}

/// The first name `names` repeats, folded case-insensitively as SQL identifiers
/// are, and returned as it was spelled at the repeat.
pub fn first_duplicate<'a>(mut names: impl Iterator<Item = &'a str>) -> Option<&'a str> {
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    names.find(|n| !seen.insert(n.to_ascii_lowercase()))
}

/// The schema the system families live in. No user can create it: it starts
/// with `_`, which [`validate_user_identifier`] refuses every name a user gives.
pub const SYSTEM_SCHEMA: &str = "_system";

/// The schema every database holds from its first boot, where an unqualified
/// name resolves unless a session names another.
pub const PUBLIC_SCHEMA: &str = "public";

/// Reject empty names, names starting with `_` (reserved for the engine's own
/// internal relation and index names) and names outside `[A-Za-z0-9_]`.
pub fn validate_user_identifier(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Identifier cannot be empty".into());
    }
    if name.starts_with('_') {
        return Err(format!(
            "User identifiers cannot start with '_' (reserved for system prefix): {name}"
        ));
    }
    if !name.bytes().all(is_valid_ident_char) {
        return Err(format!("Identifier contains invalid characters: {name}"));
    }
    Ok(())
}

/// [`validate_user_identifier`], then the canonical stored form: an ASCII
/// lowercase fold. Validating first is what makes the fold safe — over
/// `[A-Za-z0-9_]` it is injective on the classes SQL already considers equal,
/// which it is not over arbitrary bytes.
///
/// The single definition of catalog-name case-insensitivity: every user-supplied
/// relation, schema or index name crosses it on the way to a catalog row.
pub fn canonical_identifier(name: &str) -> Result<String, String> {
    validate_user_identifier(name)?;
    Ok(name.to_ascii_lowercase())
}

/// `"schema.relation"`: the catalog key when both names are canonical. `.`
/// is outside [`is_valid_ident_char`], so no pair can produce another pair's key.
pub fn qualified_key(schema_name: &str, name: &str) -> String {
    [schema_name, ".", name].concat()
}

// ---------------------------------------------------------------------------
// TABLE_TAB.flags layout. Every bit outside the constants below is refused on
// decode. `replicated` and a non-default prefix length are mutually exclusive (a
// CLUSTER BY prefix is meaningless when every worker holds the full copy):
// [`TableDistribution`] cannot represent both, and `TableProps::from_flags`
// refuses a word that carries both.
// ---------------------------------------------------------------------------

/// Bit 0: the table is **replicated** — every worker holds an identical full
/// copy (writes broadcast, reads single-source).
const TABLE_FLAG_REPLICATED: u64 = 1;
/// Bit 1: the table is a **stream** — a storeless, append-only ingestion point.
/// Independent of the distribution: a stream may be replicated or CLUSTER BY'd.
const TABLE_FLAG_STREAM: u64 = 1 << 1;
/// Bit 2: the table's single PK column is **SERIAL** — INSERT draws it from the
/// table's sequence.
const TABLE_FLAG_SERIAL: u64 = 1 << 2;
/// Bit 3: the table's store **compresses** its heaps — `WITH (compress = true)`.
const TABLE_FLAG_COMPRESS: u64 = 1 << 3;
/// Bit position of the distribution-prefix-length byte in `TABLE_TAB.flags`;
/// `0` there is the default, the full PK.
const TABLE_FLAG_DIST_SHIFT: u32 = 8;
/// Mask for the distribution-prefix-length byte (one byte: 0..=255). An
/// explicit prefix is `1..=PK_LIST_MAX_COLS`, well within the byte.
const TABLE_FLAG_DIST_MASK: u64 = 0xFF;
/// Every bit a `TABLE_TAB.flags` word may carry.
const TABLE_FLAGS_DEFINED: u64 = TABLE_FLAG_REPLICATED
    | TABLE_FLAG_STREAM
    | TABLE_FLAG_SERIAL
    | TABLE_FLAG_COMPRESS
    | (TABLE_FLAG_DIST_MASK << TABLE_FLAG_DIST_SHIFT);

/// A boolean carried as a u64 word (a catalog column or a request arg): `0` or
/// `1`, any other word refused.
pub fn bool_word(w: u64) -> Result<bool, String> {
    match w {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(format!("boolean word {w} is neither 0 nor 1")),
    }
}

/// How a table's rows are spread over the workers.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum TableDistribution {
    /// Hash-distributed by the first `prefix_len` PK columns; `0` = the full PK.
    Keyed { prefix_len: u8 },
    /// A full copy on every worker.
    Replicated,
}

/// Hash-distributed by the full PK.
impl Default for TableDistribution {
    fn default() -> Self {
        TableDistribution::Keyed { prefix_len: 0 }
    }
}

/// The logical content of `TABLE_TAB.flags`: the non-column properties of a
/// `CREATE TABLE`.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub struct TableProps {
    /// A storeless, append-only ingestion point rather than a table: it holds no
    /// rows and nothing it ingests survives a restart.
    pub stream: bool,
    /// INSERT draws the table's single PK column from a sequence.
    pub serial: bool,
    /// Where the rows live. A `Keyed` prefix is `CLUSTER BY` the PK's leading
    /// prefix; the SQL planner validates it against the PK before packing.
    pub distribution: TableDistribution,
    /// The table's store codes its shards' heaps under a symbol table where
    /// that pays.
    pub compress: bool,
}

impl TableProps {
    /// Pack the persisted `TABLE_TAB.flags` u64. Inverse of `from_flags`.
    #[inline]
    pub fn pack(self) -> u64 {
        let dist = match self.distribution {
            TableDistribution::Keyed { prefix_len } => (prefix_len as u64) << TABLE_FLAG_DIST_SHIFT,
            TableDistribution::Replicated => TABLE_FLAG_REPLICATED,
        };
        let bit = |set: bool, flag: u64| if set { flag } else { 0 };
        dist | bit(self.stream, TABLE_FLAG_STREAM)
            | bit(self.serial, TABLE_FLAG_SERIAL)
            | bit(self.compress, TABLE_FLAG_COMPRESS)
    }

    /// Decode a persisted `TABLE_TAB.flags` u64, refusing a bit outside the
    /// defined set and a replicated table that also carries a prefix.
    pub(crate) fn from_flags(flags: u64) -> Result<TableProps, String> {
        if flags & !TABLE_FLAGS_DEFINED != 0 {
            return Err(format!("table flags {flags:#x} carry unknown bits"));
        }
        let prefix_len = ((flags >> TABLE_FLAG_DIST_SHIFT) & TABLE_FLAG_DIST_MASK) as u8;
        let distribution = if flags & TABLE_FLAG_REPLICATED == 0 {
            TableDistribution::Keyed { prefix_len }
        } else if prefix_len == 0 {
            TableDistribution::Replicated
        } else {
            return Err(replicated_with_prefix(prefix_len));
        };
        Ok(TableProps {
            stream: flags & TABLE_FLAG_STREAM != 0,
            serial: flags & TABLE_FLAG_SERIAL != 0,
            distribution,
            compress: flags & TABLE_FLAG_COMPRESS != 0,
        })
    }

    /// The rules against a PK of `pk_len` columns that the flags word cannot
    /// encode.
    pub fn validate(&self, pk_len: usize) -> Result<(), String> {
        if let TableDistribution::Keyed { prefix_len } = self.distribution {
            if prefix_len as usize > pk_len {
                return Err(format!(
                    "distribution prefix length {prefix_len} exceeds PK column count {pk_len}"
                ));
            }
        }
        if self.compress && self.stream {
            return Err("a stream cannot be compressed: it holds no rows".to_string());
        }
        if self.serial && self.stream {
            return Err("a stream cannot be SERIAL: it holds no rows to seed the generator from".to_string());
        }
        if self.serial && pk_len != 1 {
            return Err("a SERIAL table's primary key is its one SERIAL column".to_string());
        }
        Ok(())
    }
}

/// The refusal for a table that is both replicated and `CLUSTER BY`'d.
pub fn replicated_with_prefix(prefix_len: u8) -> String {
    format!(
        "REPLICATED and CLUSTER BY are mutually exclusive: a replicated table keeps \
         a full copy on every worker, so a hash-distribution prefix (k={prefix_len}) is meaningless"
    )
}

/// The `WITH (…)` options of one user-named view.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub struct ViewProps {
    pub budget: ViewBudget,
    /// `WITH (compress = …)`: the view's output store codes its heaps.
    pub compress: bool,
}

/// The byte budget a view's `WITH (…)` names, if any.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub enum ViewBudget {
    #[default]
    Plain,
    /// `WITH (capacity = …)`: the view's output store is bounded, dehydrating past it.
    Bounded { capacity_bytes: NonZeroU64 },
    /// `WITH (delta = …)`: the view keeps its recent deltas in a store of its own.
    Fed { delta_bytes: NonZeroU64 },
}

impl ViewBudget {
    /// The props two optional budgets name.
    pub fn from_budgets(
        capacity_bytes: Option<NonZeroU64>,
        delta_bytes: Option<NonZeroU64>,
    ) -> Result<ViewBudget, String> {
        match (capacity_bytes, delta_bytes) {
            (None, None) => Ok(ViewBudget::Plain),
            (Some(capacity_bytes), None) => Ok(ViewBudget::Bounded { capacity_bytes }),
            (None, Some(delta_bytes)) => Ok(ViewBudget::Fed { delta_bytes }),
            (Some(_), Some(_)) => Err("a capacity-bounded view cannot carry a delta feed: its bootstrap read \
                                       hydrates from the source relation's live store, which no tick round governs"
                .to_string()),
        }
    }

    /// Decode the `(capacity, delta)` `VIEW_TAB` words, where `0` is absent.
    pub(crate) fn from_row(capacity: u64, delta: u64) -> Result<ViewBudget, String> {
        Self::from_budgets(NonZeroU64::new(capacity), NonZeroU64::new(delta))
    }

    /// The `(capacity, delta)` `VIEW_TAB` words.
    pub fn row_words(self) -> (u64, u64) {
        (self.capacity_bytes().unwrap_or(0), self.delta_bytes().unwrap_or(0))
    }

    pub fn capacity_bytes(self) -> Option<u64> {
        match self {
            ViewBudget::Bounded { capacity_bytes } => Some(capacity_bytes.get()),
            _ => None,
        }
    }

    pub fn delta_bytes(self) -> Option<u64> {
        match self {
            ViewBudget::Fed { delta_bytes } => Some(delta_bytes.get()),
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "tests/catalog.rs"]
mod tests;
