//! System-catalog wire layout: the shared `WireSysCol` descriptor, system
//! table column lists and IDs, schema sizing caps, and the compound-PK
//! column-list codec for the persisted `TABLE_TAB.pk_col_idx` u64.

use crate::TypeCode;

// ---------------------------------------------------------------------------
// System table column descriptors — shared single source of truth
// ---------------------------------------------------------------------------

pub struct WireSysCol {
    pub name: &'static str,
    pub type_code: TypeCode,
    pub nullable: bool,
}

/// Terse `WireSysCol` constructor so the column tables read as one line per
/// column. `pub(crate)` — internal to the wire crate (also used by `control.rs`);
/// not part of the public surface. `const` so it is callable in the `pub const`
/// table initializers (visibility does not affect const-eval).
pub(crate) const fn col(name: &'static str, type_code: TypeCode, nullable: bool) -> WireSysCol {
    WireSysCol {
        name,
        type_code,
        nullable,
    }
}

/// Index of the column named `name` in `cols`, resolved at compile time.
/// Panics (const-eval failure) if absent, so a renamed/removed column fails the
/// build rather than silently mis-indexing. `==` on `&str` is not const-stable,
/// hence the manual byte compare.
pub const fn col_index_in(cols: &[WireSysCol], name: &str) -> usize {
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

/// Dense **payload** index of the column named `name` in `cols` — its position
/// among the non-PK columns, which is the slot the null bitmap and the payload
/// region directory address it by (§6).
///
/// Valid only for a column list whose primary key is the single leading column,
/// where the payload index collapses to `col_index_in(..) - 1`. Every list this
/// is called with is one (`SCHEMA_TAB`, `TABLE_TAB`, `VIEW_TAB`, `COL_TAB`,
/// `IDX_TAB`, `SEQ_TAB`, and the IPC control block); the compound-PK lists
/// (the circuit families) renumber around *every* PK position, so the closed
/// form does not hold for them and this must not be used there. The
/// `assert!` rejects the PK column itself, and `col_index_in`'s own const-eval
/// panic covers a renamed column — both at compile time.
pub(crate) const fn pay_index_in(cols: &[WireSysCol], name: &str) -> usize {
    let ci = col_index_in(cols, name);
    assert!(ci != 0, "the leading PK column has no payload index");
    ci - 1
}

// Every system table's column shape is defined once, here, and derived by both
// sides: the engine builds its `SchemaDescriptor`s and the COL_TAB
// self-description rows from these slices, the client builds its `Schema`s. Each
// table's `*_PK` names the columns that key it — the client and the engine must
// agree on both halves, and a disagreement on the key is a `pk_stride` mismatch
// the wire decode rejects, so neither half is left for a consumer to restate.

/// The primary key of every system table whose key is its single leading column.
pub(crate) const LEADING_COL_PK: &[u32] = &[0];

pub const SCHEMA_TAB_PK: &[u32] = LEADING_COL_PK;
pub const TABLE_TAB_PK: &[u32] = LEADING_COL_PK;
pub const VIEW_TAB_PK: &[u32] = LEADING_COL_PK;
pub const COL_TAB_PK: &[u32] = LEADING_COL_PK;
pub const IDX_TAB_PK: &[u32] = LEADING_COL_PK;
pub const SEQ_TAB_PK: &[u32] = LEADING_COL_PK;

pub const SCHEMA_TAB_COLS: &[WireSysCol] = &[
    col("schema_id", TypeCode::U64, false),
    col("name", TypeCode::String, false),
];

pub const TABLE_TAB_COLS: &[WireSysCol] = &[
    col("table_id", TypeCode::U64, false),
    col("schema_id", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    // Packed PK column list (`pack_pk_cols`); a bare index (flag bit clear)
    // decodes as a single-column PK.
    col("pk_col_idx", TypeCode::U64, false),
    // See `pack_table_flags` for the bit layout.
    col("flags", TypeCode::U64, false),
];

pub const VIEW_TAB_COLS: &[WireSysCol] = &[
    col("view_id", TypeCode::U64, false),
    col("schema_id", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    col("sql_definition", TypeCode::String, false),
    // Packed view-PK column list (`pack_pk_cols`). A bare `0` (flag bit clear)
    // decodes as the single-column PK `[0]`.
    col("pk_col_idx", TypeCode::U64, false),
];

pub const COL_TAB_COLS: &[WireSysCol] = &[
    col("column_id", TypeCode::U64, false),
    col("owner_id", TypeCode::U64, false),
    col("owner_kind", TypeCode::U64, false),
    col("col_idx", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    col("type_code", TypeCode::U64, false),
    col("is_nullable", TypeCode::U64, false),
    col("fk_table_id", TypeCode::U64, false),
    col("fk_col_idx", TypeCode::U64, false),
    // is_serial marker: 1 for a SERIAL PK column, else 0. Lets a connection
    // that only fetched the schema distinguish an auto-assigned SERIAL PK from
    // a user-supplied non-null integer PK. Stored verbatim by the engine.
    col("is_serial", TypeCode::U64, false),
    // is_hidden marker: 1 for a hidden key slot (synthetic view keys and
    // unprojected passthrough PKs), else 0. Echoed into reply schema blocks as
    // META_FLAG_HIDDEN; the engine never branches on it.
    col("is_hidden", TypeCode::U64, false),
];

pub const IDX_TAB_COLS: &[WireSysCol] = &[
    col("index_id", TypeCode::U64, false),
    col("owner_id", TypeCode::U64, false),
    // Holds `pack_pk_cols(&col_indices)` for every row (single- and
    // multi-column indexes alike); decoded via `unpack_pk_cols`.
    col("source_col_idx", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    col("is_unique", TypeCode::U64, false),
];

/// The reply **schema block**'s column shape. Not a system table — it is the
/// per-message block describing a reply's columns — but it is a wire schema both
/// ends build, so it belongs with them: the engine encodes it, the client decodes
/// it, and the `flags` word is packed by [`crate::pack_col_meta_flags`].
pub const META_SCHEMA_COLS: &[WireSysCol] = &[
    col("col_idx", TypeCode::U64, false),
    col("type_code", TypeCode::U64, false),
    col("flags", TypeCode::U64, false),
    col("name", TypeCode::String, false),
];
pub const META_SCHEMA_PK: &[u32] = LEADING_COL_PK;

pub const SEQ_TAB_COLS: &[WireSysCol] = &[
    col("seq_id", TypeCode::U64, false),
    col("next_val", TypeCode::U64, false),
];

// Circuit catalog tables use a real compound primary key `(view_id, sub)`
// instead of hand-packing both halves into one U128 column. `sub` is the
// per-view secondary key (node_id, an (dst_node,dst_port) pack, or a
// (node_id,kind,position) pack). The remaining columns denormalise the
// decoded fields as payload so the engine's logical-column readers are
// unchanged. PK = columns [0, 1].
pub const CIRCUIT_NODES_COLS: &[WireSysCol] = &[
    col("view_id", TypeCode::U64, false),
    col("sub", TypeCode::U64, false),
    col("node_id", TypeCode::U64, false),
    col("opcode", TypeCode::U64, false),
    col("source_table", TypeCode::U64, true),
    col("expr_program", TypeCode::Blob, true),
];

pub const CIRCUIT_EDGES_COLS: &[WireSysCol] = &[
    col("view_id", TypeCode::U64, false),
    col("sub", TypeCode::U64, false),
    col("dst_node", TypeCode::U64, false),
    col("dst_port", TypeCode::U64, false),
    col("src_node", TypeCode::U64, false),
];

pub const CIRCUIT_NODE_COLUMNS_COLS: &[WireSysCol] = &[
    col("view_id", TypeCode::U64, false),
    col("sub", TypeCode::U64, false),
    col("node_id", TypeCode::U64, false),
    col("kind", TypeCode::U64, false),
    col("position", TypeCode::U64, false),
    col("value1", TypeCode::U64, false),
    col("value2", TypeCode::U64, false),
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
// `*_COL_*` index a full schema (`ZSetBatch::columns[..]` / a cursor read, PK
// included); `*_PAY_*` index the payload region only (§6).

pub const SCHEMATAB_COL_NAME: usize = col_index_in(SCHEMA_TAB_COLS, "name");
pub const SCHEMATAB_PAY_NAME: usize = pay_index_in(SCHEMA_TAB_COLS, "name");

pub const TABTAB_COL_SCHEMA_ID: usize = col_index_in(TABLE_TAB_COLS, "schema_id");
pub const TABTAB_COL_NAME: usize = col_index_in(TABLE_TAB_COLS, "name");
pub const TABTAB_COL_PK_COL_IDX: usize = col_index_in(TABLE_TAB_COLS, "pk_col_idx");
pub const TABTAB_COL_FLAGS: usize = col_index_in(TABLE_TAB_COLS, "flags");
pub const TABTAB_PAY_SCHEMA_ID: usize = pay_index_in(TABLE_TAB_COLS, "schema_id");
pub const TABTAB_PAY_NAME: usize = pay_index_in(TABLE_TAB_COLS, "name");
pub const TABTAB_PAY_PK_COL_IDX: usize = pay_index_in(TABLE_TAB_COLS, "pk_col_idx");
pub const TABTAB_PAY_FLAGS: usize = pay_index_in(TABLE_TAB_COLS, "flags");

pub const VIEWTAB_COL_SCHEMA_ID: usize = col_index_in(VIEW_TAB_COLS, "schema_id");
pub const VIEWTAB_COL_NAME: usize = col_index_in(VIEW_TAB_COLS, "name");
pub const VIEWTAB_COL_SQL: usize = col_index_in(VIEW_TAB_COLS, "sql_definition");
pub const VIEWTAB_COL_PK_COL_IDX: usize = col_index_in(VIEW_TAB_COLS, "pk_col_idx");
pub const VIEWTAB_PAY_SCHEMA_ID: usize = pay_index_in(VIEW_TAB_COLS, "schema_id");
pub const VIEWTAB_PAY_NAME: usize = pay_index_in(VIEW_TAB_COLS, "name");
pub const VIEWTAB_PAY_PK_COL_IDX: usize = pay_index_in(VIEW_TAB_COLS, "pk_col_idx");

/// One code path decodes TABLE_TAB and VIEW_TAB on both sides — the engine's
/// `apply_entity_caches`, the client's `collect_schema_member_names` — reading
/// either family through the `TABTAB_*` positions. That needs the two to agree
/// on where `(schema_id, name)` sits.
const _: () = {
    assert!(TABTAB_COL_SCHEMA_ID == VIEWTAB_COL_SCHEMA_ID);
    assert!(TABTAB_COL_NAME == VIEWTAB_COL_NAME);
};

pub const COLTAB_COL_OWNER_ID: usize = col_index_in(COL_TAB_COLS, "owner_id");
pub const COLTAB_COL_OWNER_KIND: usize = col_index_in(COL_TAB_COLS, "owner_kind");
pub const COLTAB_COL_COL_IDX: usize = col_index_in(COL_TAB_COLS, "col_idx");
pub const COLTAB_COL_NAME: usize = col_index_in(COL_TAB_COLS, "name");
pub const COLTAB_COL_TYPE_CODE: usize = col_index_in(COL_TAB_COLS, "type_code");
pub const COLTAB_COL_IS_NULLABLE: usize = col_index_in(COL_TAB_COLS, "is_nullable");
pub const COLTAB_COL_FK_TABLE_ID: usize = col_index_in(COL_TAB_COLS, "fk_table_id");
pub const COLTAB_COL_FK_COL_IDX: usize = col_index_in(COL_TAB_COLS, "fk_col_idx");
pub const COLTAB_COL_IS_SERIAL: usize = col_index_in(COL_TAB_COLS, "is_serial");
pub const COLTAB_COL_IS_HIDDEN: usize = col_index_in(COL_TAB_COLS, "is_hidden");
pub const COLTAB_PAY_OWNER_ID: usize = pay_index_in(COL_TAB_COLS, "owner_id");
pub const COLTAB_PAY_OWNER_KIND: usize = pay_index_in(COL_TAB_COLS, "owner_kind");
pub const COLTAB_PAY_COL_IDX: usize = pay_index_in(COL_TAB_COLS, "col_idx");
pub const COLTAB_PAY_NAME: usize = pay_index_in(COL_TAB_COLS, "name");
pub const COLTAB_PAY_TYPE_CODE: usize = pay_index_in(COL_TAB_COLS, "type_code");
pub const COLTAB_PAY_IS_SERIAL: usize = pay_index_in(COL_TAB_COLS, "is_serial");
pub const COLTAB_PAY_FK_TABLE_ID: usize = pay_index_in(COL_TAB_COLS, "fk_table_id");
pub const COLTAB_PAY_FK_COL_IDX: usize = pay_index_in(COL_TAB_COLS, "fk_col_idx");
pub const COLTAB_PAY_IS_NULLABLE: usize = pay_index_in(COL_TAB_COLS, "is_nullable");
pub const COLTAB_PAY_IS_HIDDEN: usize = pay_index_in(COL_TAB_COLS, "is_hidden");

pub const IDXTAB_COL_OWNER_ID: usize = col_index_in(IDX_TAB_COLS, "owner_id");
pub const IDXTAB_COL_SOURCE_COLS: usize = col_index_in(IDX_TAB_COLS, "source_col_idx");
pub const IDXTAB_COL_NAME: usize = col_index_in(IDX_TAB_COLS, "name");
pub const IDXTAB_COL_IS_UNIQUE: usize = col_index_in(IDX_TAB_COLS, "is_unique");
pub const IDXTAB_PAY_OWNER_ID: usize = pay_index_in(IDX_TAB_COLS, "owner_id");
pub const IDXTAB_PAY_SOURCE_COLS: usize = pay_index_in(IDX_TAB_COLS, "source_col_idx");
pub const IDXTAB_PAY_NAME: usize = pay_index_in(IDX_TAB_COLS, "name");
pub const IDXTAB_PAY_IS_UNIQUE: usize = pay_index_in(IDX_TAB_COLS, "is_unique");

pub const SEQTAB_COL_VALUE: usize = col_index_in(SEQ_TAB_COLS, "next_val");
pub const SEQTAB_PAY_VALUE: usize = pay_index_in(SEQ_TAB_COLS, "next_val");

// ---------------------------------------------------------------------------
// Stored-shape digest
// ---------------------------------------------------------------------------

/// FNV-1a over one family's column shape — each column's name, type and
/// nullability, then its key columns.
const fn fold_family(mut h: u64, cols: &[WireSysCol], pk: &[u32]) -> u64 {
    const PRIME: u64 = 0x100_0000_01b3;
    let mut i = 0;
    while i < cols.len() {
        let name = cols[i].name.as_bytes();
        let mut j = 0;
        while j < name.len() {
            h = (h ^ name[j] as u64).wrapping_mul(PRIME);
            j += 1;
        }
        h = (h ^ cols[i].type_code as u64).wrapping_mul(PRIME);
        h = (h ^ cols[i].nullable as u64).wrapping_mul(PRIME);
        i += 1;
    }
    let mut k = 0;
    while k < pk.len() {
        h = (h ^ pk[k] as u64).wrapping_mul(PRIME);
        k += 1;
    }
    h
}

/// Digest of every system family's stored shape. Shards and SAL frames are
/// decoded against the *live* schema, so a shape change silently reinterprets
/// an existing data directory unless a format word rejects it first — see the
/// pin in [`crate::wal::WAL_FORMAT_VERSION`]'s test.
pub const SYS_SCHEMA_DIGEST: u64 = {
    let mut h = 0xcbf2_9ce4_8422_2325;
    h = fold_family(h, SCHEMA_TAB_COLS, SCHEMA_TAB_PK);
    h = fold_family(h, TABLE_TAB_COLS, TABLE_TAB_PK);
    h = fold_family(h, VIEW_TAB_COLS, VIEW_TAB_PK);
    h = fold_family(h, COL_TAB_COLS, COL_TAB_PK);
    h = fold_family(h, IDX_TAB_COLS, IDX_TAB_PK);
    h = fold_family(h, SEQ_TAB_COLS, SEQ_TAB_PK);
    h = fold_family(h, CIRCUIT_NODES_COLS, CIRCUIT_FAMILY_PK);
    h = fold_family(h, CIRCUIT_EDGES_COLS, CIRCUIT_FAMILY_PK);
    h = fold_family(h, CIRCUIT_NODE_COLUMNS_COLS, CIRCUIT_FAMILY_PK);
    h
};

// The one reply block that is a wire schema rather than a system table.
pub const METASCHEMA_COL_TYPE_CODE: usize = col_index_in(META_SCHEMA_COLS, "type_code");
pub const METASCHEMA_COL_FLAGS: usize = col_index_in(META_SCHEMA_COLS, "flags");
pub const METASCHEMA_COL_NAME: usize = col_index_in(META_SCHEMA_COLS, "name");
pub const METASCHEMA_PAY_TYPE_CODE: usize = pay_index_in(META_SCHEMA_COLS, "type_code");
pub const METASCHEMA_PAY_FLAGS: usize = pay_index_in(META_SCHEMA_COLS, "flags");
pub const METASCHEMA_PAY_NAME: usize = pay_index_in(META_SCHEMA_COLS, "name");

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
pub const CIRCUIT_EDGES_TAB: u64 = 12;
pub const CIRCUIT_NODE_COLUMNS_TAB: u64 = 13;

/// The circuit families' compound primary key: columns `(view_id, sub)`. Shared
/// by all three, which is why it is named once rather than per family.
pub const CIRCUIT_FAMILY_PK: &[u32] = &[0, 1];

pub const FIRST_USER_TABLE_ID: u64 = 16;
pub const FIRST_USER_SCHEMA_ID: u64 = 3;

pub const OWNER_KIND_TABLE: u64 = 0;
pub const OWNER_KIND_VIEW: u64 = 1;

// ---------------------------------------------------------------------------
// COL_TAB primary-key packing
// ---------------------------------------------------------------------------

/// Bit width of the column-index field in a packed COL_TAB PK.
pub(crate) const COL_ID_IDX_BITS: u32 = 9;

/// Pack an owner (table/view) id and column index into the COL_TAB PK word:
/// `(owner_id << 9) | col_idx`. Rejects a column index that overflows its
/// 9-bit field or an owner id that overflows the remaining 55 bits — either
/// would silently alias another column's record.
pub fn pack_col_id(owner_id: u64, col_idx: u64) -> Result<u64, String> {
    if col_idx >= (1 << COL_ID_IDX_BITS) {
        return Err(format!(
            "column index {col_idx} exceeds maximum {}",
            (1u64 << COL_ID_IDX_BITS) - 1
        ));
    }
    if owner_id > (u64::MAX >> COL_ID_IDX_BITS) {
        return Err(format!(
            "owner_id {owner_id} exceeds {}-bit maximum for column ID packing",
            64 - COL_ID_IDX_BITS
        ));
    }
    Ok((owner_id << COL_ID_IDX_BITS) | col_idx)
}

/// Inverse of [`pack_col_id`]: `(owner_id, col_idx)`.
pub const fn unpack_col_id(packed: u64) -> (u64, u64) {
    (packed >> COL_ID_IDX_BITS, packed & ((1 << COL_ID_IDX_BITS) - 1))
}

// ---------------------------------------------------------------------------
// Identifier validation (shared between the SQL planner and the engine)
// ---------------------------------------------------------------------------

/// Infix marking an index as an internal FK-backing index. User identifiers may
/// not contain it — such a name would be undroppable (`drop_index` rejects it).
pub const FK_INDEX_INFIX: &str = "__fk_";

fn is_valid_ident_char(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_'
}

/// Reject empty names, names starting with `_` (reserved for the system prefix),
/// and names with characters outside `[A-Za-z0-9_]`.
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

// ---------------------------------------------------------------------------
// Schema sizing caps
// ---------------------------------------------------------------------------

/// Maximum number of columns (PK + payload) in any table or view schema.
/// Capped at 65 by the row-major null bitmap: each row stores one u64 word
/// with one bit per payload column (§6), so payload columns ≤ 64.
pub const MAX_COLUMNS: usize = 65;

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
// Two forms share the same column slot:
//   * Bare scalar (flag bit clear): a single PK column index in bits [0..63).
//     Written by an unmodified single-PK client and by engine-side bootstrap
//     for system tables.
//   * Packed list (flag bit set):
//        bit 63        : PK_LIST_PACKED_FLAG
//        bit 62        : reserved — HAS_PK_WANT_HOLDER, when the word is a
//                        FLAG_HAS_PK `seek_col_idx` rather than a catalog cell
//        bits [0..4)   : decoded count (1..=PK_LIST_MAX_COLS valid; larger
//                        counts are reserved for tests / malformed payloads)
//        bits [4+7i..) : i-th column index, 7 bits each
//
// Both client (gnitz-core) and engine (gnitz-engine catalog) MUST share this
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

/// The PK-list arity rule: `1..=PK_LIST_MAX_COLS` columns. One predicate behind
/// every spelling of it — the panicking constructors, the fallible decode, and
/// the validator — so they cannot disagree on the bound.
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

pub const PK_LIST_PACKED_FLAG: u64 = 1 << 63;

/// Directive bit riding the FLAG_HAS_PK `seek_col_idx` word next to the packed
/// column list: the worker's secondary-index arm answers each matched probe with
/// the **stored** index entry key `[span ‖ holder PK]` instead of echoing the
/// probe key, so the caller learns which committed row holds the span without a
/// second round trip. It rides the column-list word rather than `wire_flags` so
/// the dispatch arms that forward the list forward the directive with it.
/// [`pk_cols_word`] masks it off; the guard above keeps bit 62 clear of every
/// packed field, so that mask recovers the column list exactly.
pub const HAS_PK_WANT_HOLDER: u64 = 1 << 62;

/// The packed column list carried in a `seek_col_idx`, with the directive bits
/// that ride alongside it stripped. The one place that knows which bits are not
/// part of the list, so a decoder never has to spell the mask itself.
#[inline]
pub fn pk_cols_word(seek_col_idx: u64) -> u64 {
    seek_col_idx & !HAS_PK_WANT_HOLDER
}

/// Decoded PK column list — backing storage sized to `PK_LIST_MAX_COLS`
/// entries. `decoded_count()` returns the raw decoded count from the wire
/// (may be 0 or out of range for a crafted packed value); `as_slice()` is
/// panic-free and clamps the slice to at most `PK_LIST_MAX_COLS` entries.
/// Out-of-range counts must reach schema-validation code as `Err`, not as a
/// panic here.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct PkColList {
    cols: [u32; PK_LIST_MAX_COLS],
    len: usize,
}

impl PkColList {
    /// Single-column PK with `len = 1`.
    pub fn single(idx: u32) -> Self {
        let mut cols = [0u32; PK_LIST_MAX_COLS];
        cols[0] = idx;
        PkColList { cols, len: 1 }
    }
    /// Construct from a column-index slice. Panics on an out-of-range length
    /// (`1..=PK_LIST_MAX_COLS`) — like `pack_pk_cols`, callers must validate the
    /// arity before constructing, because a silent clamp here would desync the
    /// list from the persisted/packed form it round-trips with.
    pub fn from_slice(cols: &[u32]) -> Self {
        Self::try_from_slice(cols).unwrap_or_else(|| {
            panic!(
                "PkColList::from_slice: count {} out of range 1..={PK_LIST_MAX_COLS}",
                cols.len(),
            )
        })
    }
    /// Fallible [`Self::from_slice`] for untrusted (wire-decoded) input: `None`
    /// on an out-of-range length instead of a panic. The arity rule lives here,
    /// so decode boundaries need no mirrored pre-check.
    pub(crate) fn try_from_slice(cols: &[u32]) -> Option<Self> {
        if !pk_list_arity_ok(cols.len()) {
            return None;
        }
        let mut arr = [0u32; PK_LIST_MAX_COLS];
        arr[..cols.len()].copy_from_slice(cols);
        Some(PkColList {
            cols: arr,
            len: cols.len(),
        })
    }
    /// The count exactly as decoded from the wire. May be 0 or larger than
    /// `PK_LIST_MAX_COLS` for a malformed/crafted packed value — deliberately
    /// NOT clamped, because `is_well_formed` gates on this raw value to
    /// reject out-of-range counts. Not a safe slice length: iterate
    /// `as_slice()` instead.
    pub fn decoded_count(&self) -> usize {
        self.len
    }
    /// True iff the decoded count is a valid list length
    /// (`1..=PK_LIST_MAX_COLS`). Every consumer of a wire-decoded list must
    /// gate on this before trusting `as_slice()`: a crafted packed value can
    /// carry a zero or over-range count, and `as_slice()` silently clamps —
    /// so without this check an over-range list reads back truncated and an
    /// empty one reads back as zero columns.
    pub fn is_well_formed(&self) -> bool {
        pk_list_arity_ok(self.len)
    }
    /// Always in bounds: indexes at most the `PK_LIST_MAX_COLS`-element
    /// backing array even when the decoded count is out of range. A crafted
    /// over-range wire count must NOT panic here — it has to survive long
    /// enough to reach `validate_pk_cols` and be returned as `Err`.
    pub fn as_slice(&self) -> &[u32] {
        &self.cols[..self.len.min(PK_LIST_MAX_COLS)]
    }
}

/// The `Err`-returning form of [`pack_pk_cols`]' panicking contract, plus the
/// no-duplicates rule both consumers (table PKs and index column lists) share:
/// count in `1..=PK_LIST_MAX_COLS`, every index within the 7-bit field, no
/// repeated column. Call this at user-input boundaries so `pack_pk_cols` and
/// `PkColList::from_slice` can never panic downstream.
pub fn validate_pk_col_list(cols: &[u32]) -> Result<(), String> {
    // The rule itself is `validate_pk_indices`; the packed 7-bit field is this
    // list's column-count bound. Only the wording differs — these lists are also
    // secondary-index column lists, which "primary key ..." would misname.
    crate::validate_pk_indices(cols, 1 << PK_LIST_COL_BITS).map_err(|rule| match rule {
        crate::PkRule::Empty | crate::PkRule::TooManyColumns { .. } => {
            format!("column count {} out of range 1..={PK_LIST_MAX_COLS}", cols.len())
        }
        crate::PkRule::IndexOutOfRange { col } => format!("column index {col} exceeds {PK_LIST_COL_MAX}"),
        crate::PkRule::Duplicate { col } => format!("duplicate column {col} in list"),
        // `validate_pk_indices` reports only the structural rules above.
        other => other.to_string(),
    })
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

/// Decode the persisted `u64` PK-list form. Handles both the bare scalar
/// (flag bit clear → single index) and packed list forms. Out-of-range
/// counts are returned as-is via `decoded_count()` so the catalog
/// validator can reject them.
pub fn unpack_pk_cols(packed: u64) -> PkColList {
    if packed & PK_LIST_PACKED_FLAG == 0 {
        // Bare single index: an unmodified gnitz-core client, or an
        // engine-written system-table row (always bare `0`). Saturate rather
        // than truncate — a word too wide for a column index is malformed, and
        // `u32::MAX` fails every downstream range check where the low 32 bits
        // might not have.
        return PkColList::single(u32::try_from(packed).unwrap_or(u32::MAX));
    }
    let n = (packed & ((1 << PK_LIST_COUNT_BITS) - 1)) as usize; // validated later
    let mut cols = [0u32; PK_LIST_MAX_COLS];
    for (i, slot) in cols.iter_mut().enumerate().take(n.min(PK_LIST_MAX_COLS)) {
        *slot = ((packed >> (PK_LIST_COUNT_BITS + PK_LIST_COL_BITS * i as u32)) & PK_LIST_COL_MAX as u64) as u32;
    }
    PkColList { cols, len: n }
}

/// Width of one `seek_by_index` key slot on the wire: a native `u128`, LE.
pub(crate) const INDEX_KEY_SLOT: usize = 16;

/// Pack K native index-key values into the `PkTuple` a `seek_by_index` request
/// carries — one 16-byte LE slot each, which `split_wire` then routes as slot 0
/// → `seek_pk` and slots 1..K → `seek_pk_extra`. A prefix seek supplies K < the
/// index's arity. Returns the packed bytes; K is bounded by [`PK_LIST_MAX_COLS`],
/// so at most `PK_LIST_MAX_COLS * INDEX_KEY_SLOT` of the buffer is ever written.
pub fn pack_index_key_slots(key_vals: &[u128]) -> ([u8; MAX_PK_BYTES], usize) {
    assert!(
        pk_list_arity_ok(key_vals.len()),
        "pack_index_key_slots: count {} out of range 1..={PK_LIST_MAX_COLS}",
        key_vals.len(),
    );
    let mut buf = [0u8; MAX_PK_BYTES];
    for (i, &v) in key_vals.iter().enumerate() {
        buf[i * INDEX_KEY_SLOT..(i + 1) * INDEX_KEY_SLOT].copy_from_slice(&v.to_le_bytes());
    }
    (buf, key_vals.len() * INDEX_KEY_SLOT)
}

/// Recover the K key values a `seek_by_index` request packed, from the wire pair
/// `seek_pk` (slot 0) + `seek_pk_extra` (slots 1..K). Validates at the trust
/// boundary: a misaligned `extra` or a K beyond `arity` is rejected rather than
/// silently dropping trailing bytes or over-reading the index.
pub fn unpack_index_key_slots(seek_pk: u128, extra: &[u8], arity: usize) -> Result<PkKeyVals, String> {
    if !extra.len().is_multiple_of(INDEX_KEY_SLOT) {
        return Err(format!(
            "seek_by_index: key tail of {} bytes is not a multiple of {INDEX_KEY_SLOT}",
            extra.len()
        ));
    }
    let len = 1 + extra.len() / INDEX_KEY_SLOT;
    if len > arity.min(PK_LIST_MAX_COLS) {
        return Err(format!("seek_by_index: {len} key values exceed index arity {arity}"));
    }
    let mut vals = [0u128; PK_LIST_MAX_COLS];
    vals[0] = seek_pk;
    for (i, slot) in extra.chunks_exact(INDEX_KEY_SLOT).enumerate() {
        vals[i + 1] = u128::from_le_bytes(slot.try_into().expect("chunks_exact yields 16 bytes"));
    }
    Ok(PkKeyVals { vals, len })
}

/// The K native key values of a `seek_by_index` request, inline.
pub struct PkKeyVals {
    vals: [u128; PK_LIST_MAX_COLS],
    len: usize,
}

impl PkKeyVals {
    pub fn as_slice(&self) -> &[u128] {
        &self.vals[..self.len]
    }
}

// ---------------------------------------------------------------------------
// TABLE_TAB.flags layout — the single source of truth shared by the gnitz-core
// writer and the gnitz-engine reader, so the bit packing cannot drift.
//
//   bit 0        replicated (TABLE_FLAG_REPLICATED) — full copy on every worker
//   bits [1..8)  reserved for future boolean flags
//   bits [8..16) distribution prefix length k (0 = default = full PK)
//
// `k` is byte-aligned so the boolean flag bits stay free for future flags
// without colliding with it. `replicated` and a non-default `k` are mutually
// exclusive (a CLUSTER BY prefix is meaningless when every worker holds the
// full copy); the packing cannot represent that constraint, so DDL validation
// must enforce it.
// ---------------------------------------------------------------------------

/// Bit 0: the table is **replicated** — every worker holds an identical full
/// copy (writes broadcast, reads single-source). Mutually exclusive with a
/// non-default `dist_prefix_len` (enforced at DDL, not by this packing).
const TABLE_FLAG_REPLICATED: u64 = 1;
/// Bit position of the distribution-prefix-length byte in `TABLE_TAB.flags`.
const TABLE_FLAG_DIST_SHIFT: u32 = 8;
/// Mask for the distribution-prefix-length byte (one byte: 0..=255). An
/// explicit prefix is `1..=PK_LIST_MAX_COLS`, well within the byte; the full
/// byte is deliberate headroom.
const TABLE_FLAG_DIST_MASK: u64 = 0xFF;

/// Pack the persisted `TABLE_TAB.flags` u64 from its logical fields.
#[inline]
pub fn pack_table_flags(replicated: bool, dist_prefix_len: usize) -> u64 {
    (((dist_prefix_len as u64) & TABLE_FLAG_DIST_MASK) << TABLE_FLAG_DIST_SHIFT)
        | if replicated { TABLE_FLAG_REPLICATED } else { 0 }
}

/// Decode the `replicated` bit from `TABLE_TAB.flags`.
#[inline]
pub fn table_flags_replicated(flags: u64) -> bool {
    flags & TABLE_FLAG_REPLICATED != 0
}

/// Decode the distribution prefix length `k` from `TABLE_TAB.flags`. `0` means
/// "default = full PK"; the schema constructor normalizes that to `k = |PK|`.
#[inline]
pub fn table_flags_dist_prefix(flags: u64) -> usize {
    ((flags >> TABLE_FLAG_DIST_SHIFT) & TABLE_FLAG_DIST_MASK) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every system table's key must be admissible for its own column list. The
    /// pair is the thing both crates build from, so it is validated here rather
    /// than trusted at each derivation site.
    #[test]
    fn system_table_keys_are_valid_for_their_columns() {
        let families: &[(&str, &[WireSysCol], &[u32])] = &[
            ("_schemas", SCHEMA_TAB_COLS, SCHEMA_TAB_PK),
            ("_tables", TABLE_TAB_COLS, TABLE_TAB_PK),
            ("_views", VIEW_TAB_COLS, VIEW_TAB_PK),
            ("_columns", COL_TAB_COLS, COL_TAB_PK),
            ("_indices", IDX_TAB_COLS, IDX_TAB_PK),
            ("_sequences", SEQ_TAB_COLS, SEQ_TAB_PK),
            ("_circuit_nodes", CIRCUIT_NODES_COLS, CIRCUIT_FAMILY_PK),
            ("_circuit_edges", CIRCUIT_EDGES_COLS, CIRCUIT_FAMILY_PK),
            ("_circuit_node_columns", CIRCUIT_NODE_COLUMNS_COLS, CIRCUIT_FAMILY_PK),
            ("meta_schema", META_SCHEMA_COLS, META_SCHEMA_PK),
        ];
        for &(name, cols, pk) in families {
            assert!(cols.len() <= MAX_COLUMNS, "{name}: too many columns");
            crate::validate_pk_tuple(pk, cols.len(), |c| {
                let col = &cols[c as usize];
                (col.type_code as u8, col.nullable)
            })
            .unwrap_or_else(|rule| panic!("{name}: invalid primary key: {rule}"));
        }
    }

    #[test]
    fn from_slice_roundtrips_as_slice() {
        for cols in [
            vec![0u32],
            vec![3u32],
            vec![1u32, 2],
            vec![2u32, 5, 7],
            vec![9u32, 1, 4, 6],
        ] {
            let list = PkColList::from_slice(&cols);
            assert_eq!(list.as_slice(), cols.as_slice());
            assert_eq!(list.decoded_count(), cols.len());
        }
    }

    #[test]
    fn from_slice_matches_pack_unpack_roundtrip() {
        // from_slice and pack→unpack must agree for every 1..=PK_LIST_MAX_COLS list.
        for cols in [vec![0u32], vec![1u32, 127], vec![5u32, 6, 7], vec![1u32, 2, 3, 4]] {
            let via_slice = PkColList::from_slice(&cols);
            let via_wire = unpack_pk_cols(pack_pk_cols(&cols));
            assert_eq!(via_slice, via_wire);
        }
    }

    #[test]
    #[should_panic(expected = "out of range")]
    fn from_slice_panics_on_empty() {
        let _ = PkColList::from_slice(&[]);
    }

    #[test]
    fn validate_dist_prefix_accepts_leading_rejects_rest() {
        // Exact leading prefixes of PK (0, 1) are accepted, returning k.
        assert_eq!(validate_dist_prefix(&[0, 1], &[0]), Ok(1));
        assert_eq!(validate_dist_prefix(&[0, 1], &[0, 1]), Ok(2));
        // A single-column PK: only the whole PK is a valid prefix.
        assert_eq!(validate_dist_prefix(&[3], &[3]), Ok(1));
        // Reordered PK so the distribution column leads: prefix is the new lead.
        assert_eq!(validate_dist_prefix(&[2, 1], &[2]), Ok(1));

        // Non-leading PK column, non-contiguous-prefix, wrong order, empty, and
        // over-long lists are all rejected.
        assert!(validate_dist_prefix(&[0, 1], &[1]).is_err(), "non-leading PK column");
        assert!(validate_dist_prefix(&[0, 1, 2], &[0, 2]).is_err(), "skips col 1");
        assert!(validate_dist_prefix(&[0, 1], &[1, 0]).is_err(), "wrong order");
        assert!(validate_dist_prefix(&[0, 1], &[]).is_err(), "empty");
        assert!(validate_dist_prefix(&[0, 1], &[0, 1, 2]).is_err(), "longer than PK");
        assert!(validate_dist_prefix(&[0, 1], &[5]).is_err(), "non-PK column");
    }

    #[test]
    fn table_flags_roundtrip() {
        // Default (not replicated, k = 0 = full PK) is the all-clear word.
        assert_eq!(pack_table_flags(false, 0), 0);
        // k rides in byte 1; the replicated bit is untouched.
        for &repl in &[false, true] {
            for k in 0..=PK_LIST_MAX_COLS {
                let f = pack_table_flags(repl, k);
                assert_eq!(table_flags_dist_prefix(f), k);
                assert_eq!(table_flags_replicated(f), repl);
            }
        }
        // `replicated` is bit 0; the reserved bits [1..8) stay clear of the k byte.
        assert_eq!(pack_table_flags(true, 0) & 0xFF, TABLE_FLAG_REPLICATED);
        assert_eq!(pack_table_flags(true, 2) >> TABLE_FLAG_DIST_SHIFT, 2);
        assert_eq!(
            pack_table_flags(true, 2) & 0xFF & !TABLE_FLAG_REPLICATED,
            0,
            "reserved bits are free"
        );
    }

    /// The two halves of the `seek_by_index` key wire format are inverses at
    /// every arity, including a prefix seek that supplies fewer values than the
    /// index has columns.
    #[test]
    fn index_key_slots_roundtrip() {
        let all: [u128; PK_LIST_MAX_COLS] = [1, u128::MAX, 1 << 100, 0];
        for k in 1..=PK_LIST_MAX_COLS {
            let vals = &all[..k];
            let (buf, len) = pack_index_key_slots(vals);
            assert_eq!(len, k * INDEX_KEY_SLOT);
            // `split_wire` routes slot 0 to seek_pk and the rest to the tail.
            let seek_pk = u128::from_le_bytes(buf[..INDEX_KEY_SLOT].try_into().unwrap());
            let extra = &buf[INDEX_KEY_SLOT..len];
            let back = unpack_index_key_slots(seek_pk, extra, PK_LIST_MAX_COLS).expect("well-formed");
            assert_eq!(back.as_slice(), vals, "arity {k}");
            // A prefix seek is accepted; more values than the arity is not.
            assert!(unpack_index_key_slots(seek_pk, extra, k).is_ok());
            if k > 1 {
                assert!(unpack_index_key_slots(seek_pk, extra, k - 1).is_err(), "over-arity");
            }
        }
    }

    /// A tail that is not a whole number of slots is rejected rather than
    /// silently dropping its trailing bytes.
    #[test]
    fn index_key_slots_reject_a_misaligned_tail() {
        assert!(unpack_index_key_slots(7, &[0u8; 15], PK_LIST_MAX_COLS).is_err());
        assert!(unpack_index_key_slots(7, &[0u8; 17], PK_LIST_MAX_COLS).is_err());
        assert!(
            unpack_index_key_slots(7, &[], PK_LIST_MAX_COLS).is_ok(),
            "K=1 has no tail"
        );
    }
}
