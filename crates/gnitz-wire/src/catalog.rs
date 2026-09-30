//! System-catalog wire layout: the shared `WireSysCol` descriptor, system
//! table column lists and IDs, schema sizing caps, and the compound-PK
//! column-list codec for the persisted `TABLE_TAB.pk_col_idx` u64.

use crate::{ColType, TypeCode};

// ---------------------------------------------------------------------------
// System table column descriptors — shared single source of truth
// ---------------------------------------------------------------------------

pub struct WireSysCol {
    pub name: &'static str,
    pub type_code: TypeCode,
    pub nullable: bool,
}

/// Terse `WireSysCol` constructor so the column tables read as one line per
/// column. `pub(crate)` — internal to the wire crate;
/// not part of the public surface. `const` so it is callable in the `pub const`
/// table initializers (visibility does not affect const-eval).
pub(crate) const fn col(name: &'static str, type_code: TypeCode, nullable: bool) -> WireSysCol {
    WireSysCol { name, type_code, nullable }
}

/// Index of the column named `name` in `cols`, resolved at compile time.
/// Panics (const-eval failure) if absent, so a renamed/removed column fails the
/// build rather than silently mis-indexing. `==` on `&str` is not const-stable,
/// hence the manual byte compare.
const fn col_index_in(cols: &[WireSysCol], name: &str) -> usize {
    let mut i = 0;
    while i < cols.len() {
        let a = cols[i].name.as_bytes();
        let b = name.as_bytes();
        if a.len() == b.len() {
            let mut j = 0;
            let mut matched = true;
            while j < a.len() {
                if a[j] != b[j] {
                    matched = false;
                    break;
                }
                j += 1;
            }
            if matched {
                return i;
            }
        }
        i += 1;
    }
    panic!("column not found")
}

/// Payload slot of column `name` in system family `id`, whose column list and
/// key are paired in one place. A PK column or an unknown name is a const-eval
/// panic.
pub(crate) const fn pay_index_in_fam(id: u64, name: &str) -> usize {
    let f = &SYS_FAMILIES[match sys_family_index(id) {
        Some(i) => i,
        None => panic!("not a system family"),
    }];
    match payload_slot(f.pk_cols, col_index_in(f.cols, name)) {
        Some(pi) => pi,
        None => panic!("a PK column has no payload slot"),
    }
}

// Every system table's column shape is defined once, here, and reached through
// `SYS_FAMILIES` (by `sys_family_index(id)`), which pairs each shape with the
// key that goes with it: the engine builds its `SchemaDescriptor`s and the COL_TAB
// self-description rows from a family, the client builds its `Schema`s. The two
// must agree on both halves, and a disagreement on the key is a `pk_stride`
// mismatch the wire decode rejects — so neither half is offered separately for a
// consumer to pair up itself.

/// The primary key of every system table whose key is its single leading column.
pub(crate) const LEADING_COL_PK: &[u32] = &[0];

/// The primary key of every system table keyed by its two leading columns:
/// COL_TAB's `(owner_id, col_idx)` and the circuit family's `(view_id, node_id)`.
pub(crate) const LEADING_PAIR_PK: &[u32] = &[0, 1];

/// The two halves of a `LEADING_PAIR_PK` key, off the widened `u128` a PK
/// region reads back to. Both columns are `U64` — asserted over
/// [`SYS_FAMILIES`] below — so the split is exact.
#[inline]
pub const fn unpack_pair_pk(pk: u128) -> (u64, u64) {
    ((pk >> 64) as u64, pk as u64)
}

pub(crate) const SCHEMA_TAB_COLS: &[WireSysCol] = &[
    col("schema_id", TypeCode::U64, false),
    col("name", TypeCode::String, false),
];

pub(crate) const TABLE_TAB_COLS: &[WireSysCol] = &[
    col("table_id", TypeCode::U64, false),
    col("schema_id", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    // Packed PK column list (`pack_pk_cols`).
    col("pk_col_idx", TypeCode::U64, false),
    // See `TableProps::pack` for the bit layout.
    col("flags", TypeCode::U64, false),
];

pub(crate) const VIEW_TAB_COLS: &[WireSysCol] = &[
    col("view_id", TypeCode::U64, false),
    col("schema_id", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    // Packed view-PK column list (`pack_pk_cols`).
    col("pk_col_idx", TypeCode::U64, false),
    // `ViewProps::row_words`: `WITH (capacity = …)` in bytes, `0` absent.
    col("capacity_bytes", TypeCode::U64, false),
    // `ViewProps::row_words`: `WITH (delta = …)` in bytes, `0` absent.
    col("delta_bytes", TypeCode::U64, false),
    // The user view this row is an internal chain segment of; `0` is a user
    // view. A column rather than a name prefix, because the precheck can
    // validate it against a forger. Appended, not inserted: the `RELTAB_*`
    // constants below pin `name` to the same slot in TABLE_TAB and VIEW_TAB.
    col("owner_view_id", TypeCode::U64, false),
    // See `ViewFlags` for the bit layout.
    col("flags", TypeCode::U64, false),
];

// Keyed by the compound `(owner_id, col_idx)` — the pair *is* a column record's
// identity.
pub(crate) const COL_TAB_COLS: &[WireSysCol] = &[
    col("owner_id", TypeCode::U64, false),
    col("col_idx", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    col("type_code", TypeCode::U64, false),
    col("is_nullable", TypeCode::U64, false),
    col("fk_table_id", TypeCode::U64, false),
    col("fk_col_idx", TypeCode::U64, false),
    // is_hidden marker: 1 for a hidden key slot (synthetic view keys and
    // unprojected passthrough PKs), else 0. Echoed into reply schema blocks as
    // the record's `hidden` flag.
    col("is_hidden", TypeCode::U64, false),
    // A DECIMAL column's scale, else 0. Echoed into a reply schema block's
    // column type; the engine never branches on it.
    col("scale", TypeCode::U64, false),
];

pub(crate) const IDX_TAB_COLS: &[WireSysCol] = &[
    col("index_id", TypeCode::U64, false),
    col("owner_id", TypeCode::U64, false),
    // Holds `pack_pk_cols(&col_indices)` for every row (single- and
    // multi-column indexes alike); decoded via `unpack_pk_cols`.
    col("source_col_idx", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    // See `IndexProps::pack` for the bit layout. One word rather than a bool
    // column per property: the family is scanned whole by `DROP INDEX` and the
    // planner's name/column probe, so a second `U64` would widen every row for
    // one bit.
    col("flags", TypeCode::U64, false),
];

pub(crate) const SEQ_TAB_COLS: &[WireSysCol] = &[
    col("seq_id", TypeCode::U64, false),
    col("next_val", TypeCode::U64, false),
];

// The circuit table uses a real compound primary key `(view_id, node_id)`
// instead of hand-packing both halves into one U128 column. PK = columns [0, 1].
//
// `opcode` and `source_table` stay scannable columns, so a node's scan edge reads
// without the `params` codec. `input_0`/`input_1` are
// port-indexed, so the column name *is* the port; keeping them out of `params`
// leaves a parameterless node with no blob cell at all.
pub(crate) const CIRCUIT_NODES_COLS: &[WireSysCol] = &[
    col("view_id", TypeCode::U64, false),
    col("node_id", TypeCode::U64, false),
    col("opcode", TypeCode::U64, false),
    col("source_table", TypeCode::U64, true),
    col("input_0", TypeCode::U64, true),
    col("input_1", TypeCode::U64, true),
    // The node's per-opcode parameters (`encode_op_node`); NULL for an operator
    // that carries none.
    col("params", TypeCode::Blob, true),
];

// ---------------------------------------------------------------------------
// Catalog column positions
// ---------------------------------------------------------------------------
//
// Resolved by name from the lists above, so a dropped or renamed column fails
// the build instead of shifting a literal. Both the engine and the client read
// catalog batches and must agree on every position, so they are stated once,
// here, rather than derived again in each crate.
//
// The indices address the payload region — the space every catalog read, engine
// and client alike, uses.

pub const SCHEMATAB_PAY_NAME: usize = pay_index_in_fam(SCHEMA_TAB, "name");

pub const TABTAB_PAY_PK_COL_IDX: usize = pay_index_in_fam(TABLE_TAB, "pk_col_idx");
pub const TABTAB_PAY_FLAGS: usize = pay_index_in_fam(TABLE_TAB, "flags");

pub const VIEWTAB_PAY_PK_COL_IDX: usize = pay_index_in_fam(VIEW_TAB, "pk_col_idx");
pub const VIEWTAB_PAY_CAPACITY: usize = pay_index_in_fam(VIEW_TAB, "capacity_bytes");
pub const VIEWTAB_PAY_DELTA: usize = pay_index_in_fam(VIEW_TAB, "delta_bytes");
pub const VIEWTAB_PAY_OWNER_VIEW_ID: usize = pay_index_in_fam(VIEW_TAB, "owner_view_id");
pub const VIEWTAB_PAY_FLAGS: usize = pay_index_in_fam(VIEW_TAB, "flags");

// `RELTAB_*`: an address valid in TABLE_TAB and VIEW_TAB alike, for the readers
// that take either family through one code path. A column at the same slot in
// both but read through neither stays two constants.

/// `name`'s payload index in two families that must agree on it — each half
/// carries its own key.
const fn shared_pay_index(a: u64, b: u64, name: &str) -> usize {
    let i = pay_index_in_fam(a, name);
    assert!(
        i == pay_index_in_fam(b, name),
        "the two families disagree on this column"
    );
    i
}

pub const RELTAB_PAY_SCHEMA_ID: usize = shared_pay_index(TABLE_TAB, VIEW_TAB, "schema_id");
pub const RELTAB_PAY_NAME: usize = shared_pay_index(TABLE_TAB, VIEW_TAB, "name");

pub const COLTAB_PAY_NAME: usize = pay_index_in_fam(COL_TAB, "name");
pub const COLTAB_PAY_TYPE_CODE: usize = pay_index_in_fam(COL_TAB, "type_code");
pub const COLTAB_PAY_FK_TABLE_ID: usize = pay_index_in_fam(COL_TAB, "fk_table_id");
pub const COLTAB_PAY_FK_COL_IDX: usize = pay_index_in_fam(COL_TAB, "fk_col_idx");
pub const COLTAB_PAY_IS_NULLABLE: usize = pay_index_in_fam(COL_TAB, "is_nullable");
pub const COLTAB_PAY_IS_HIDDEN: usize = pay_index_in_fam(COL_TAB, "is_hidden");
pub const COLTAB_PAY_SCALE: usize = pay_index_in_fam(COL_TAB, "scale");

pub const CIRCNODES_PAY_OPCODE: usize = pay_index_in_fam(CIRCUIT_NODES_TAB, "opcode");
pub const CIRCNODES_PAY_SOURCE_TABLE: usize = pay_index_in_fam(CIRCUIT_NODES_TAB, "source_table");
pub const CIRCNODES_PAY_INPUT_0: usize = pay_index_in_fam(CIRCUIT_NODES_TAB, "input_0");
pub const CIRCNODES_PAY_INPUT_1: usize = pay_index_in_fam(CIRCUIT_NODES_TAB, "input_1");
pub const CIRCNODES_PAY_PARAMS: usize = pay_index_in_fam(CIRCUIT_NODES_TAB, "params");

pub const IDXTAB_PAY_OWNER_ID: usize = pay_index_in_fam(IDX_TAB, "owner_id");
pub const IDXTAB_PAY_SOURCE_COLS: usize = pay_index_in_fam(IDX_TAB, "source_col_idx");
pub const IDXTAB_PAY_NAME: usize = pay_index_in_fam(IDX_TAB, "name");
pub const IDXTAB_PAY_FLAGS: usize = pay_index_in_fam(IDX_TAB, "flags");

pub const SEQTAB_PAY_VALUE: usize = pay_index_in_fam(SEQ_TAB, "next_val");

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

/// FNV-1a over one family's stored identity: its id, its shard directory
/// (`name`), each column's name/type/nullability, and its key columns. Renaming
/// a family orphans its shards and boots on an empty store, which is why `name`
/// is in here.
const fn fold_family(mut h: u64, f: &WireSysFamily) -> u64 {
    let (cols, pk) = (f.cols, f.pk_cols);
    h = (h ^ f.id).wrapping_mul(FNV_PRIME);
    h = fnv_bytes(h, f.name.as_bytes());
    let mut i = 0;
    while i < cols.len() {
        h = fnv_bytes(h, cols[i].name.as_bytes());
        h = (h ^ cols[i].type_code as u64).wrapping_mul(FNV_PRIME);
        h = (h ^ cols[i].nullable as u64).wrapping_mul(FNV_PRIME);
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
    h = (h ^ crate::circuit::CIRCUIT_PARAMS_VERSION as u64).wrapping_mul(FNV_PRIME);
    h = (h ^ EXPR_BLOB_VERSION as u64).wrapping_mul(FNV_PRIME);
    h
};

/// Version of the compiled expression-program blob, whose layout and encoder
/// live in `gnitz-expr`. It sits here because [`SYS_SCHEMA_DIGEST`] folds it in
/// at `const` time and this crate cannot depend on that one.
///
/// Carried by no blob — each rides a slot of an already-versioned container.
/// Bumping it rejects both carriers of a stale one: a stored
/// `CIRCUIT_NODES.params` cell, and an old client's live `ReadSpec` predicate.
pub const EXPR_BLOB_VERSION: u8 = 6;

// ---------------------------------------------------------------------------
// System table IDs
// ---------------------------------------------------------------------------

pub const SCHEMA_TAB: u64 = 1;
pub const TABLE_TAB: u64 = 2;
pub const VIEW_TAB: u64 = 3;
pub const COL_TAB: u64 = 4;
pub const IDX_TAB: u64 = 5;
pub const SEQ_TAB: u64 = 7;
pub const CIRCUIT_NODES_TAB: u64 = 11;

/// One system family's wire identity: the table id both sides address it by,
/// its name, and the column shape they each build their schema type from.
/// Grouping these means a caller holding a table id can *derive* the shape
/// instead of being handed a separately-chosen one that may not match.
pub struct WireSysFamily {
    pub id: u64,
    /// The table's name, and its subdirectory under the engine's catalog root.
    pub name: &'static str,
    pub cols: &'static [WireSysCol],
    pub pk_cols: &'static [u32],
}

const fn fam(id: u64, name: &'static str, cols: &'static [WireSysCol], pk_cols: &'static [u32]) -> WireSysFamily {
    WireSysFamily { id, name, cols, pk_cols }
}

/// Every system family, in the order both sides index them by.
pub const SYS_FAMILIES: &[WireSysFamily] = &[
    fam(SCHEMA_TAB, "_schemas", SCHEMA_TAB_COLS, LEADING_COL_PK),
    fam(TABLE_TAB, "_tables", TABLE_TAB_COLS, LEADING_COL_PK),
    fam(VIEW_TAB, "_views", VIEW_TAB_COLS, LEADING_COL_PK),
    fam(COL_TAB, "_columns", COL_TAB_COLS, LEADING_PAIR_PK),
    fam(IDX_TAB, "_indices", IDX_TAB_COLS, LEADING_COL_PK),
    fam(SEQ_TAB, "_sequences", SEQ_TAB_COLS, LEADING_COL_PK),
    fam(CIRCUIT_NODES_TAB, "_circuit_nodes", CIRCUIT_NODES_COLS, LEADING_PAIR_PK),
];

// What `unpack_pair_pk`'s `>> 64` split is written against.
const _: () = {
    let mut f = 0;
    while f < SYS_FAMILIES.len() {
        let (cols, pk) = (SYS_FAMILIES[f].cols, SYS_FAMILIES[f].pk_cols);
        let mut i = 0;
        while i < pk.len() {
            assert!(
                matches!(cols[pk[i] as usize].type_code, TypeCode::U64),
                "a system family's key column is not U64"
            );
            i += 1;
        }
        f += 1;
    }
};

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
pub const FIRST_USER_SCHEMA_ID: u64 = 3;

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

/// The canonical `"schema.relation"` key, from names **already canonical**.
///
/// It must not fold: a mixed-case name that slipped past the engine's
/// `reject_unstorable_name` would then key the cache while mismatching the store,
/// turning a loud rejection into silent divergence. `.` is outside
/// [`is_valid_ident_char`], so no pair can produce another pair's key.
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

/// Sizing cap for the compound-PK column list.
///
/// Set to 5 to cover the user-facing PK cap (4 columns, planner-enforced)
/// plus one indexed-column prefix used by secondary index schemas — modeled
/// as `(indexed_col, src_pk_0, …, src_pk_{k-1})`.
pub const MAX_PK_COLUMNS: usize = 5;

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
//        bits [0..4)   : decoded count (1..=PK_LIST_MAX_COLS valid; larger
//                        counts are reserved for tests / malformed payloads)
//        bits [4+7i..) : i-th column index, 7 bits each
//
// Both client (gnitz-core) and engine (gnitz-server catalog) MUST share this
// encoder/decoder so they cannot drift on the encoding.
// ---------------------------------------------------------------------------

/// Capacity of the persisted PK-list codec (`pack_pk_cols` / `PkColList`):
/// the most PK columns a table or view PK may declare. The codec, both
/// validators, and the two planner admission caps all derive from this one
/// constant — raise or lower it and every dependent site follows, with the
/// static guards below catching a value the encoding or schema layout can't
/// hold.
///
/// Distinct from `MAX_PK_COLUMNS` (5), which sizes the engine's in-memory PK
/// arrays and reserves one extra slot for the secondary-index column prefix.
/// Keeping `PK_LIST_MAX_COLS < MAX_PK_COLUMNS` leaves that slot free for any
/// table.
pub const PK_LIST_MAX_COLS: usize = 4;

/// Width of the packed decoded-count field (bits `[0..4)`).
const PK_LIST_COUNT_BITS: u32 = 4;
/// Width of each packed column-index field (bits `[4 + 7i..)`), and so the
/// exclusive ceiling on a column index the list can carry.
const PK_LIST_COL_BITS: u32 = 7;
const PK_LIST_COL_MAX: u32 = (1 << PK_LIST_COL_BITS) - 1;
/// The exclusive ceiling on a column index a packed list can carry — the bound a
/// [`validate_pk_col_list`] caller passes when it holds no column count of its own.
pub const PK_LIST_COL_LIMIT: usize = 1 << PK_LIST_COL_BITS;

/// The **user-facing** PK-list arity rule: `1..=PK_LIST_MAX_COLS` columns — what
/// the persisted `u64` word can round-trip. One predicate behind every spelling
/// of *that* bound, so they cannot disagree on it.
#[inline]
pub(crate) const fn pk_list_arity_ok(n: usize) -> bool {
    n >= 1 && n <= PK_LIST_MAX_COLS
}

// The packed u64 lays the decoded count in bits [0..4) and each column index in
// a 7-bit field at bit 4 + 7*i; the packed flag occupies bit 63. Guard the
// ceilings at compile time so bumping PK_LIST_MAX_COLS past what the encoding
// (or the index-prefix reservation) can hold fails to build instead of
// silently corrupting the catalog word.
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
    PK_LIST_MAX_COLS < MAX_PK_COLUMNS, // leave the index-prefix slot
    "PK_LIST_MAX_COLS leaves no MAX_PK_COLUMNS slot for the secondary-index prefix"
);
const _: () = assert!(
    MAX_COLUMNS <= 1 << PK_LIST_COL_BITS,
    "a schema column index no longer fits the packed 7-bit field"
);

/// Bit 63: the tag whose absence [`unpack_pk_cols`] refuses. An `arg1` means
/// different things per message kind, so the decode insists on a tag rather than
/// reading a shape.
pub const PK_LIST_PACKED_FLAG: u64 = 1 << 63;

/// The store a `HasPk` probe reads, as its group's `arg1` carries it.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub enum ProbeKeyspace {
    /// The relation's own PK store, carried as `0`.
    OwnPk,
    /// The secondary index on this exact column list, carried packed.
    Index(PkColList),
}

impl ProbeKeyspace {
    /// The `arg1` word. [`pack_pk_cols`] sets [`PK_LIST_PACKED_FLAG`], so an index
    /// word is never `0`.
    pub fn arg1(self) -> u64 {
        match self {
            ProbeKeyspace::OwnPk => 0,
            ProbeKeyspace::Index(cols) => pack_pk_cols(cols.as_slice()),
        }
    }

    /// Decode an `arg1` word; a nonzero word must be a packed column list.
    pub fn from_arg1(arg1: u64) -> Result<Self, String> {
        if arg1 == 0 {
            return Ok(ProbeKeyspace::OwnPk);
        }
        unpack_pk_cols(arg1)
            .map(ProbeKeyspace::Index)
            .map_err(|e| e.for_role(crate::PkListRole::ColumnList))
    }
}

/// A PK column list, `1..=PK_LIST_MAX_COLS` entries, inline. Constructing one is
/// the arity check, so a holder never gates on the count. `from_slice` zero-fills
/// the tail, so the derived `Hash` and `PartialEq` see no padding and a list can
/// key a map.
#[derive(Copy, Clone, PartialEq, Eq, Hash, Debug)]
pub struct PkColList {
    cols: [u32; PK_LIST_MAX_COLS],
    len: usize,
}

impl PkColList {
    /// Construct from a column-index slice. Panics on an out-of-range length
    /// (`1..=PK_LIST_MAX_COLS`) — like `pack_pk_cols`, callers must validate the
    /// arity before constructing, because a silent clamp here would desync the
    /// list from the persisted/packed form it round-trips with. Wire input goes
    /// through [`unpack_pk_cols`], which returns that arity as an `Err`.
    pub fn from_slice(cols: &[u32]) -> Self {
        assert!(
            pk_list_arity_ok(cols.len()),
            "PkColList::from_slice: count {} out of range 1..={PK_LIST_MAX_COLS}",
            cols.len(),
        );
        let mut arr = [0u32; PK_LIST_MAX_COLS];
        arr[..cols.len()].copy_from_slice(cols);
        PkColList { cols: arr, len: cols.len() }
    }
    /// The column indices, in list order.
    pub fn as_slice(&self) -> &[u32] {
        &self.cols[..self.len]
    }
}

/// The `Err`-returning form of [`pack_pk_cols`]' panicking contract, plus the
/// no-duplicates rule both consumers (table PKs and index column lists) share:
/// count in `1..=PK_LIST_MAX_COLS`, every index below `ncols`, no repeated
/// column. Call this at user-input boundaries so `pack_pk_cols` and
/// `PkColList::from_slice` can never panic downstream.
///
/// `ncols` is the source relation's column count, or [`PK_LIST_COL_LIMIT`] — the
/// packed field's own bound — for a caller that holds no column count.
pub fn validate_pk_col_list(cols: &[u32], ncols: usize) -> Result<(), String> {
    // The rule itself is `validate_pk_indices`. Only the wording differs — these
    // lists are also secondary-index column lists, which "primary key ..." would
    // misname.
    crate::validate_pk_indices(cols, ncols, PK_LIST_MAX_COLS)
        .map_err(|rule| rule.for_role(crate::PkListRole::ColumnList))
}

/// Validate a `CLUSTER BY` column list against the table's PK, returning the
/// distribution prefix length `k` (`= cols.len()`) when `cols` is exactly the
/// PK's leading prefix in PK order. The distribution key is constrained to a
/// **leading PK prefix** so write-side routing is a pure byte-slice of the OPK
/// region (no gather, no separate packer). No type/null/width re-checks — those
/// are inherited from the PK validation, since `dist ⊆ pk`. Shared by the SQL
/// planner (pre-engine `CLUSTER BY` check) so the surface error names PK column
/// order.
pub fn validate_dist_prefix(pk: &[u32], cols: &[u32]) -> Result<usize, String> {
    if cols.is_empty() || cols.len() > pk.len() {
        return Err(format!(
            "CLUSTER BY expects 1..={} leading PRIMARY KEY columns, got {}",
            pk.len(),
            cols.len()
        ));
    }
    if let Some(c) = cols.iter().find(|c| !pk.contains(c)) {
        return Err(format!("CLUSTER BY column {c} is not a PRIMARY KEY column"));
    }
    if cols != &pk[..cols.len()] {
        return Err("CLUSTER BY columns must be a leading prefix of the PRIMARY KEY, \
                    in PK order; reorder the PK so the distribution column(s) lead"
            .into());
    }
    Ok(cols.len())
}

/// Pack a PK column-index list into the persisted `u64` form. Panics on a
/// violated contract because a silent truncation here corrupts the
/// catalog encoding; callers (client + engine) must reject out-of-range
/// lists before calling this (see [`validate_pk_col_list`]).
pub fn pack_pk_cols(pk_cols: &[u32]) -> u64 {
    assert!(
        pk_list_arity_ok(pk_cols.len()),
        "pack_pk_cols: count {} out of range 1..={PK_LIST_MAX_COLS}",
        pk_cols.len(),
    );
    let mut v = pk_cols.len() as u64; // the count field
    for (i, &c) in pk_cols.iter().enumerate() {
        assert!(c <= PK_LIST_COL_MAX, "pack_pk_cols: column index {c} exceeds its field");
        v |= (c as u64) << (PK_LIST_COUNT_BITS + PK_LIST_COL_BITS * i as u32);
    }
    v | PK_LIST_PACKED_FLAG
}

/// Decode the persisted `u64` PK-list form. The one crossing from wire bits into
/// [`PkColList`], and so where an untagged word or a crafted count is refused.
pub fn unpack_pk_cols(packed: u64) -> Result<PkColList, crate::PkRule> {
    if packed & PK_LIST_PACKED_FLAG == 0 {
        return Err(crate::PkRule::NotPacked);
    }
    let n = (packed & ((1 << PK_LIST_COUNT_BITS) - 1)) as usize;
    if !pk_list_arity_ok(n) {
        return Err(if n == 0 {
            crate::PkRule::Empty
        } else {
            crate::PkRule::TooManyColumns { count: n, max: PK_LIST_MAX_COLS }
        });
    }
    let mut cols = [0u32; PK_LIST_MAX_COLS];
    for (i, slot) in cols.iter_mut().enumerate().take(n) {
        *slot = ((packed >> (PK_LIST_COUNT_BITS + PK_LIST_COL_BITS * i as u32)) & PK_LIST_COL_MAX as u64) as u32;
    }
    Ok(PkColList { cols, len: n })
}

// ---------------------------------------------------------------------------
// TABLE_TAB.flags layout — the single source of truth shared by the gnitz-core
// writer and the gnitz-server reader, so the bit packing cannot drift.
//
//   bit 0        replicated (TABLE_FLAG_REPLICATED) — full copy on every worker
//   bit 1        stream (TABLE_FLAG_STREAM) — storeless append-only ingestion point
//   bit 2        serial (TABLE_FLAG_SERIAL) — INSERT draws the lone PK column from a sequence
//   bits [8..16) distribution prefix length k (0 = default = full PK)
//
// Every other bit is refused on decode. `replicated` and a non-default `k` are
// mutually exclusive (a CLUSTER BY prefix is meaningless when every worker
// holds the full copy): [`TableDistribution`] cannot represent both, and
// [`TableProps::from_flags`] refuses a word that carries both.
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
/// Bit position of the distribution-prefix-length byte in `TABLE_TAB.flags`.
const TABLE_FLAG_DIST_SHIFT: u32 = 8;
/// Mask for the distribution-prefix-length byte (one byte: 0..=255). An
/// explicit prefix is `1..=PK_LIST_MAX_COLS`, well within the byte.
const TABLE_FLAG_DIST_MASK: u64 = 0xFF;
/// Every bit a `TABLE_TAB.flags` word may carry.
const TABLE_FLAGS_DEFINED: u64 =
    TABLE_FLAG_REPLICATED | TABLE_FLAG_STREAM | TABLE_FLAG_SERIAL | (TABLE_FLAG_DIST_MASK << TABLE_FLAG_DIST_SHIFT);

/// `VIEW_TAB.flags` bit 0: two of the view's rows may carry the same PK, so its
/// PK region identifies no row — a view over a stream, one keyed on a join key or
/// a source-PK pair, or a top-N holding more than one slot per partition. The
/// planner that compiled the view states it; the engine stores it verbatim.
const VIEW_FLAG_PK_REPEATS: u64 = 1 << 0;

/// The logical content of `VIEW_TAB.flags`.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub struct ViewFlags {
    pub pk_repeats: bool,
}

impl ViewFlags {
    /// Pack the persisted `VIEW_TAB.flags` u64. Inverse of [`Self::from_flags`].
    #[inline]
    pub fn pack(self) -> u64 {
        if self.pk_repeats {
            VIEW_FLAG_PK_REPEATS
        } else {
            0
        }
    }

    /// Decode a persisted `VIEW_TAB.flags` u64; a bit outside the defined set is
    /// refused.
    pub fn from_flags(flags: u64) -> Result<ViewFlags, String> {
        if flags & !VIEW_FLAG_PK_REPEATS != 0 {
            return Err(format!("flags {flags:#x} carry unknown bits"));
        }
        Ok(ViewFlags {
            pk_repeats: flags & VIEW_FLAG_PK_REPEATS != 0,
        })
    }
}

/// `IDX_TAB.flags` bit 0: the index enforces uniqueness.
const INDEX_FLAG_UNIQUE: u64 = 1 << 0;

/// The logical content of `IDX_TAB.flags`.
#[derive(Copy, Clone, Default, PartialEq, Eq, Debug)]
pub struct IndexProps {
    pub is_unique: bool,
}

impl IndexProps {
    /// Pack the persisted `IDX_TAB.flags` u64. Inverse of [`Self::from_flags`].
    #[inline]
    pub fn pack(self) -> u64 {
        if self.is_unique {
            INDEX_FLAG_UNIQUE
        } else {
            0
        }
    }

    /// Decode a persisted `IDX_TAB.flags` u64; a bit outside the defined set is
    /// refused.
    pub fn from_flags(flags: u64) -> Result<IndexProps, String> {
        if flags & !INDEX_FLAG_UNIQUE != 0 {
            return Err(format!("index flags {flags:#x} carry unknown bits"));
        }
        Ok(IndexProps {
            is_unique: flags & INDEX_FLAG_UNIQUE != 0,
        })
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
    /// prefix; the SQL planner validates it against the PK before packing, and
    /// the schema constructor normalizes the default `0` to `|PK|`.
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
    match pk.into_iter().find(|(_, ty)| !ty.tc.is_serial_eligible()) {
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
    Bounded { capacity_bytes: u64 },
    /// `WITH (delta = …)`: the view keeps its recent deltas in a store of its own.
    Fed { delta_bytes: u64 },
}

impl ViewProps {
    /// The props two optional budgets name.
    pub fn from_budgets(capacity_bytes: Option<u64>, delta_bytes: Option<u64>) -> Result<ViewProps, String> {
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
        Self::from_budgets((capacity != 0).then_some(capacity), (delta != 0).then_some(delta))
    }

    /// The `(capacity, delta)` `VIEW_TAB` words.
    pub(crate) fn row_words(self) -> (u64, u64) {
        (self.capacity_bytes().unwrap_or(0), self.delta_bytes().unwrap_or(0))
    }

    pub fn capacity_bytes(self) -> Option<u64> {
        match self {
            ViewProps::Bounded { capacity_bytes } => Some(capacity_bytes),
            _ => None,
        }
    }

    pub fn delta_bytes(self) -> Option<u64> {
        match self {
            ViewProps::Fed { delta_bytes } => Some(delta_bytes),
            _ => None,
        }
    }
}

#[cfg(test)]
#[path = "tests/catalog.rs"]
mod tests;
