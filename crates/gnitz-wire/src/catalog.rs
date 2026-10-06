//! System-catalog wire layout: the shared `WireSysCol` descriptor, system
//! table column lists and IDs, schema sizing caps, and the compound-PK
//! column-list codec for the persisted `TABLE_TAB.pk_col_idx` u64.

use std::num::NonZeroU64;

use crate::{ColType, PkListRole, PkRule, TypeCode, WireProbeMode};

// ---------------------------------------------------------------------------
// System table column descriptors — shared single source of truth
// ---------------------------------------------------------------------------

pub struct WireSysCol {
    pub name: &'static str,
    pub type_code: TypeCode,
}

/// Terse `WireSysCol` constructor so the column tables read as one line per
/// column.
pub(crate) const fn col(name: &'static str, type_code: TypeCode) -> WireSysCol {
    WireSysCol { name, type_code }
}

// Every system table's column shape is declared once, by its `sys_row!` in
// `sys_rows`, and reached through `SYS_FAMILIES` (by `sys_family_index(id)`),
// which pairs each shape with the key that goes with it: the engine builds its
// `SchemaDescriptor`s and the COL_TAB self-description rows from a family, the
// client builds its `Schema`s. The two must agree on both halves, and a
// disagreement on the key is a `pk_stride` mismatch the wire decode rejects — so
// neither half is offered separately for a consumer to pair up itself.

/// The two halves of a two-column key (COL_TAB's `(owner_id, col_idx)`), off the
/// widened `u128` a PK region reads back to. Every system key column is `U64`,
/// so the split is exact.
#[inline]
pub const fn unpack_pair_pk(pk: u128) -> (u64, u64) {
    ((pk >> 64) as u64, pk as u64)
}

// ---------------------------------------------------------------------------
// Payload slots TABLE_TAB and VIEW_TAB share
// ---------------------------------------------------------------------------
//
// An address valid in both families, for the readers that take either through
// one code path.

/// `schema_id`'s payload slot in TABLE_TAB and VIEW_TAB alike.
pub const RELTAB_PAY_SCHEMA_ID: usize = {
    let i = crate::sys_rows::TableTabSlot::schema_id as usize;
    assert!(
        i == crate::sys_rows::ViewTabSlot::schema_id as usize,
        "TABLE_TAB and VIEW_TAB disagree on schema_id"
    );
    i
};

/// `name`'s payload slot in TABLE_TAB and VIEW_TAB alike.
pub const RELTAB_PAY_NAME: usize = {
    let i = crate::sys_rows::TableTabSlot::name as usize;
    assert!(
        i == crate::sys_rows::ViewTabSlot::name as usize,
        "TABLE_TAB and VIEW_TAB disagree on name"
    );
    i
};

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
/// [`crate::wal::WAL_FORMAT_VERSION`] each XOR this in, so each moves when a
/// family's shape does, with no hand bump.
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
pub const EXPR_BLOB_VERSION: u8 = 7;

// ---------------------------------------------------------------------------
// System table IDs
// ---------------------------------------------------------------------------

pub const SCHEMA_TAB: u64 = 1;
pub const TABLE_TAB: u64 = 2;
pub const VIEW_TAB: u64 = 3;
pub const COL_TAB: u64 = 4;
pub const IDX_TAB: u64 = 5;
pub const SEQ_TAB: u64 = 7;
pub const CIRCUIT_TAB: u64 = 11;

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

/// What a `sys_row!` declaration fixes about a family: its columns, of which the
/// first `key_len` are its key, each a `U64`.
pub(crate) struct SysShape {
    pub(crate) cols: &'static [WireSysCol],
    pub(crate) key_len: usize,
}

const fn fam(id: u64, name: &'static str, shape: SysShape) -> WireSysFamily {
    const LEADING: &[u32] = &[0, 1];
    WireSysFamily {
        id,
        name,
        cols: shape.cols,
        pk_cols: LEADING.split_at(shape.key_len).0,
    }
}

/// Every system family, in the order both sides index them by. The engine
/// applies a bundle's families in this order.
pub const SYS_FAMILIES: &[WireSysFamily] = &[
    fam(SCHEMA_TAB, "_schemas", crate::sys_rows::SCHEMA_TAB_SHAPE),
    fam(COL_TAB, "_columns", crate::sys_rows::COL_TAB_SHAPE),
    fam(CIRCUIT_TAB, "_circuits", crate::sys_rows::CIRCUIT_TAB_SHAPE),
    fam(TABLE_TAB, "_tables", crate::sys_rows::TABLE_TAB_SHAPE),
    fam(VIEW_TAB, "_views", crate::sys_rows::VIEW_TAB_SHAPE),
    fam(IDX_TAB, "_indices", crate::sys_rows::IDX_TAB_SHAPE),
    fam(SEQ_TAB, "_sequences", crate::sys_rows::SEQ_TAB_SHAPE),
];

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

/// Reject empty names, names starting with `_` (reserved for the engine's own
/// internal relation and index names) and names outside `[A-Za-z0-9_]`.
pub fn validate_user_identifier(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("Identifier cannot be empty".into());
    }
    if name.as_bytes()[0] == b'_' {
        return Err(format!(
            "User identifiers cannot start with '_' (reserved for system prefix): {name}"
        ));
    }
    for &ch in name.as_bytes() {
        if !is_valid_ident_char(ch) {
            return Err(format!("Identifier contains invalid characters: {name}"));
        }
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
    let mut q = String::with_capacity(schema_name.len() + 1 + name.len());
    q.push_str(schema_name);
    q.push('.');
    q.push_str(name);
    q
}

// ---------------------------------------------------------------------------
// Schema sizing caps
// ---------------------------------------------------------------------------

/// Maximum number of columns (PK + payload) in any table or view schema.
/// Capped at 65 by the row-major null bitmap: each row stores one u64 word
/// with one bit per payload column, so payload columns ≤ 64.
pub const MAX_COLUMNS: usize = 65;

/// Payload slot of column `ci`: its position among the columns not in `pk`, or
/// `None` for a PK column. Inverse of [`payload_col_idx`].
pub const fn payload_slot(pk: &[u32], ci: usize) -> Option<usize> {
    if contains_col(pk, ci) {
        return None;
    }
    let mut below = 0;
    let mut k = 0;
    while k < pk.len() {
        below += ((pk[k] as usize) < ci) as usize;
        k += 1;
    }
    Some(ci - below)
}

/// Column index of payload slot `pi`: the `pi`-th column not in `pk`.
pub const fn payload_col_idx(pk: &[u32], pi: usize) -> usize {
    let mut ci = 0;
    let mut slot = 0;
    loop {
        if !contains_col(pk, ci) {
            if slot == pi {
                return ci;
            }
            slot += 1;
        }
        ci += 1;
    }
}

const fn contains_col(cols: &[u32], ci: usize) -> bool {
    let mut k = 0;
    while k < cols.len() {
        if cols[k] as usize == ci {
            return true;
        }
        k += 1;
    }
    false
}

/// Sizing cap for an engine schema's PK: the widest user-declared PK plus the one
/// indexed-column prefix of a secondary index schema — modeled as
/// `(indexed_col, src_pk_0, …, src_pk_{k-1})`.
pub const MAX_PK_COLUMNS: usize = PK_LIST_MAX_COLS + 1;

/// Maximum byte width of a PK region per row. Product of `MAX_PK_COLUMNS`
/// and the per-column ceiling (16 == max wire stride of any type valid as a
/// PK column — U128, UUID, I128; STRING and BLOB are rejected by schema
/// validation). Auto-tracks growth of `MAX_PK_COLUMNS`.
pub const MAX_PK_BYTES: usize = MAX_PK_COLUMNS * 16;

// ---------------------------------------------------------------------------
// Compound-PK list encoding for the persisted `TABLE_TAB.pk_col_idx` u64.
//
// The only form. A word without the tag is malformed, not a second spelling:
//        bit 63        : PK_LIST_PACKED_FLAG
//        bit 62        : reserved
//        bits [0..4)   : decoded count (1..=PK_LIST_MAX_COLS valid)
//        bits [4+7i..) : i-th column index, 7 bits each
//
// Both client (gnitz-core) and engine (gnitz-server catalog) MUST share this
// encoder/decoder so they cannot drift on the encoding.
// ---------------------------------------------------------------------------

/// Capacity of the persisted PK-list codec ([`PkColList`]): the most columns a
/// table or view PK, or an index column list, may declare. The static guards
/// below catch a value the encoding can't hold.
///
/// Distinct from [`MAX_PK_COLUMNS`], which sizes the engine's in-memory PK
/// arrays.
pub const PK_LIST_MAX_COLS: usize = 4;

/// Width of the packed decoded-count field (bits `[0..4)`).
const PK_LIST_COUNT_BITS: u32 = 4;
/// Width of each packed column-index field (bits `[4 + 7i..)`), and so the
/// exclusive ceiling on a column index the list can carry.
const PK_LIST_COL_BITS: u32 = 7;
const PK_LIST_COL_MAX: u32 = (1 << PK_LIST_COL_BITS) - 1;

// The packed u64 lays the decoded count in bits [0..4) and each column index in
// a 7-bit field at bit 4 + 7*i; the packed flag occupies bit 63. Guard the
// ceilings at compile time so bumping PK_LIST_MAX_COLS past what the encoding
// can hold fails to build instead of silently corrupting the catalog word.
const _: () = assert!(PK_LIST_MAX_COLS >= 1);
const _: () = assert!(
    PK_LIST_MAX_COLS < (1 << PK_LIST_COUNT_BITS),
    "PK_LIST_MAX_COLS overflows the packed count field"
);
const _: () = assert!(
    PK_LIST_COUNT_BITS as usize + PK_LIST_COL_BITS as usize * PK_LIST_MAX_COLS <= 62,
    "PK_LIST_MAX_COLS overflows the packed u64 column region" // bits 62/63 are flags
);
const _: () = assert!(
    MAX_COLUMNS <= 1 << PK_LIST_COL_BITS,
    "a schema column index no longer fits the packed 7-bit field"
);

/// Bit 63: the tag whose absence [`PkColList::unpack`] refuses. An `arg1` means
/// different things per message kind, so the decode insists on a tag rather than
/// reading a shape.
pub const PK_LIST_PACKED_FLAG: u64 = 1 << 63;

/// One `HasPk` probe: the store it reads, and what it answers each matched key
/// with.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum Probe {
    /// The relation's own PK store: each key a live row carries, echoed.
    Pk,
    /// The own PK store: each key a live row carries, with that row's column.
    PkColumn(u32),
    /// The secondary index on this exact column list: each key answered with up
    /// to this many of the stored entries under its span, each
    /// `[span ‖ holder PK]`.
    Index(PkColList, NonZeroU64),
}

impl Probe {
    /// The probe mode, `arg0` and `arg1` a `HasPk` group carries this as.
    pub fn wire(self) -> (WireProbeMode, u64, u64) {
        match self {
            Probe::Pk => (WireProbeMode::Pk, 0, 0),
            Probe::PkColumn(col) => (WireProbeMode::PkColumn, col as u64, 0),
            Probe::Index(cols, cap) => (WireProbeMode::Index, cap.get(), cols.pack()),
        }
    }

    /// The inverse of [`Self::wire`], refusing every other triple.
    pub fn from_wire(mode: WireProbeMode, arg0: u64, arg1: u64) -> Result<Self, String> {
        let refused = || format!("no probe is carried as ({mode:?}, {arg0}, {arg1:#x})");
        match (mode, arg0, arg1) {
            (WireProbeMode::Pk, 0, 0) => Ok(Probe::Pk),
            (WireProbeMode::PkColumn, col, 0) => Ok(Probe::PkColumn(u32::try_from(col).map_err(|_| refused())?)),
            (WireProbeMode::Index, cap, cols) => {
                let cap = NonZeroU64::new(cap).ok_or_else(refused)?;
                let cols = PkColList::unpack(cols).map_err(|e| e.for_role(PkListRole::ColumnList))?;
                Ok(Probe::Index(cols, cap))
            }
            _ => Err(refused()),
        }
    }
}

/// A column list — a PK's or an index's: `1..=PK_LIST_MAX_COLS` distinct columns,
/// each below `MAX_COLUMNS`, inline. [`Self::checked`] is the one way to build
/// one, so a holder never re-checks it. The tail past the list is zero, so the
/// derived `Hash` and `PartialEq` see no padding and a list can key a map.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct PkColList {
    cols: [u32; PK_LIST_MAX_COLS],
    len: usize,
}

impl PkColList {
    /// `cols` as a column list of a relation with `ncols` columns: `Err` where it
    /// is empty, longer than `PK_LIST_MAX_COLS`, names a column twice, or names
    /// one at or past `ncols`. A caller holding no column count passes
    /// `MAX_COLUMNS`.
    pub fn checked(cols: &[u32], ncols: usize) -> Result<Self, PkRule> {
        crate::validate_pk_indices(cols, ncols.min(MAX_COLUMNS), PK_LIST_MAX_COLS)?;
        let mut arr = [0u32; PK_LIST_MAX_COLS];
        arr[..cols.len()].copy_from_slice(cols);
        Ok(PkColList { cols: arr, len: cols.len() })
    }

    /// [`Self::checked`] for a list the caller already knows is one. Panics
    /// otherwise.
    pub fn from_slice(cols: &[u32]) -> Self {
        Self::checked(cols, MAX_COLUMNS).unwrap_or_else(|rule| {
            panic!(
                "PkColList::from_slice({cols:?}): {}",
                rule.for_role(PkListRole::ColumnList)
            )
        })
    }

    /// The column indices, in list order.
    pub fn as_slice(&self) -> &[u32] {
        &self.cols[..self.len]
    }

    /// The persisted `u64` form.
    pub fn pack(self) -> u64 {
        let mut w = PK_LIST_PACKED_FLAG | self.len as u64;
        for (i, &c) in self.as_slice().iter().enumerate() {
            w |= (c as u64) << (PK_LIST_COUNT_BITS + PK_LIST_COL_BITS * i as u32);
        }
        w
    }

    /// Decode the persisted `u64` form.
    pub fn unpack(word: u64) -> Result<Self, PkRule> {
        if word & PK_LIST_PACKED_FLAG == 0 {
            return Err(PkRule::NotPacked);
        }
        let n = (word & ((1 << PK_LIST_COUNT_BITS) - 1)) as usize;
        if n > PK_LIST_MAX_COLS {
            return Err(PkRule::TooManyColumns { count: n, max: PK_LIST_MAX_COLS });
        }
        let mut cols = [0u32; PK_LIST_MAX_COLS];
        for (i, slot) in cols[..n].iter_mut().enumerate() {
            *slot = ((word >> (PK_LIST_COUNT_BITS + PK_LIST_COL_BITS * i as u32)) & PK_LIST_COL_MAX as u64) as u32;
        }
        Self::checked(&cols[..n], MAX_COLUMNS)
    }
}

// ---------------------------------------------------------------------------
// TABLE_TAB.flags layout. Every bit outside the constants below is refused on
// decode. `replicated` and a non-default prefix length are mutually exclusive (a
// CLUSTER BY prefix is meaningless when every worker holds the full copy):
// [`TableDistribution`] cannot represent both, and [`TableProps::from_flags`]
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
/// Bit position of the distribution-prefix-length byte in `TABLE_TAB.flags`;
/// `0` there is the default, the full PK.
const TABLE_FLAG_DIST_SHIFT: u32 = 8;
/// Mask for the distribution-prefix-length byte (one byte: 0..=255). An
/// explicit prefix is `1..=PK_LIST_MAX_COLS`, well within the byte.
const TABLE_FLAG_DIST_MASK: u64 = 0xFF;
/// Every bit a `TABLE_TAB.flags` word may carry.
const TABLE_FLAGS_DEFINED: u64 =
    TABLE_FLAG_REPLICATED | TABLE_FLAG_STREAM | TABLE_FLAG_SERIAL | (TABLE_FLAG_DIST_MASK << TABLE_FLAG_DIST_SHIFT);

/// A catalog column holding a boolean: `0` or `1`, any other word refused.
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
}

impl TableProps {
    /// Pack the persisted `TABLE_TAB.flags` u64. Inverse of [`Self::from_flags`].
    #[inline]
    pub fn pack(self) -> u64 {
        let dist = match self.distribution {
            TableDistribution::Keyed { prefix_len } => (prefix_len as u64) << TABLE_FLAG_DIST_SHIFT,
            TableDistribution::Replicated => TABLE_FLAG_REPLICATED,
        };
        dist | if self.stream { TABLE_FLAG_STREAM } else { 0 } | if self.serial { TABLE_FLAG_SERIAL } else { 0 }
    }

    /// Decode a persisted `TABLE_TAB.flags` u64, refusing a bit outside the
    /// defined set and a replicated table that also carries a prefix.
    pub fn from_flags(flags: u64) -> Result<TableProps, String> {
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
        if self.serial && self.stream {
            return Err("a stream cannot be SERIAL: it holds no rows to seed the generator from".to_string());
        }
        if self.serial && pk_len != 1 {
            return Err("a SERIAL table's primary key is its one SERIAL column".to_string());
        }
        Ok(())
    }
}

/// The type rule on a SERIAL table's PK columns, given as `(name, type)`.
pub fn validate_serial_key<'a>(pk: impl IntoIterator<Item = (&'a str, ColType)>) -> Result<(), String> {
    match pk.into_iter().find(|(_, ty)| crate::FixedInt::exact(ty.tc).is_none()) {
        Some((name, ty)) => Err(format!(
            "SERIAL primary key column '{name}' is {ty}; SERIAL needs an integer of at most 8 bytes"
        )),
        None => Ok(()),
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
pub enum ViewProps {
    #[default]
    Plain,
    /// `WITH (capacity = …)`: the view's output store is bounded, dehydrating past it.
    Bounded { capacity_bytes: NonZeroU64 },
    /// `WITH (delta = …)`: the view keeps its recent deltas in a store of its own.
    Fed { delta_bytes: NonZeroU64 },
}

impl ViewProps {
    /// The props two optional budgets name.
    pub fn from_budgets(
        capacity_bytes: Option<NonZeroU64>,
        delta_bytes: Option<NonZeroU64>,
    ) -> Result<ViewProps, String> {
        match (capacity_bytes, delta_bytes) {
            (None, None) => Ok(ViewProps::Plain),
            (Some(capacity_bytes), None) => Ok(ViewProps::Bounded { capacity_bytes }),
            (None, Some(delta_bytes)) => Ok(ViewProps::Fed { delta_bytes }),
            (Some(_), Some(_)) => Err("a capacity-bounded view cannot carry a delta feed: its bootstrap read \
                                       hydrates from the source relation's live store, which no tick round governs"
                .to_string()),
        }
    }

    /// Decode the `(capacity, delta)` `VIEW_TAB` words, where `0` is absent.
    pub fn from_row(capacity: u64, delta: u64) -> Result<ViewProps, String> {
        Self::from_budgets(NonZeroU64::new(capacity), NonZeroU64::new(delta))
    }

    /// The `(capacity, delta)` `VIEW_TAB` words.
    pub fn row_words(self) -> (u64, u64) {
        (self.capacity_bytes().unwrap_or(0), self.delta_bytes().unwrap_or(0))
    }

    pub fn capacity_bytes(self) -> Option<u64> {
        match self {
            ViewProps::Bounded { capacity_bytes } => Some(capacity_bytes.get()),
            _ => None,
        }
    }

    pub fn delta_bytes(self) -> Option<u64> {
        match self {
            ViewProps::Fed { delta_bytes } => Some(delta_bytes.get()),
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "tests/catalog.rs"]
mod tests;
