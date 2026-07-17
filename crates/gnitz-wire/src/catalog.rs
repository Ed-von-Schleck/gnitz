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

// Every system table's column shape is defined once, here, and derived by
// both sides: the engine builds its `SchemaDescriptor`s and the COL_TAB
// self-description rows from these slices, the client builds its `Schema`s.
// PK = column [0] unless noted otherwise.

pub const SCHEMA_TAB_COLS: &[WireSysCol] = &[
    col("schema_id", TypeCode::U64, false),
    col("name", TypeCode::String, false),
];

pub const TABLE_TAB_COLS: &[WireSysCol] = &[
    col("table_id", TypeCode::U64, false),
    col("schema_id", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    col("directory", TypeCode::String, false),
    // Packed PK column list (`pack_pk_cols`); a bare index (flag bit clear)
    // decodes as a single-column PK.
    col("pk_col_idx", TypeCode::U64, false),
    col("created_lsn", TypeCode::U64, false),
    // See `pack_table_flags` for the bit layout.
    col("flags", TypeCode::U64, false),
];

pub const VIEW_TAB_COLS: &[WireSysCol] = &[
    col("view_id", TypeCode::U64, false),
    col("schema_id", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    col("sql_definition", TypeCode::String, false),
    col("cache_directory", TypeCode::String, false),
    col("created_lsn", TypeCode::U64, false),
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
    col("owner_kind", TypeCode::U64, false),
    // Holds `pack_pk_cols(&col_indices)` for every row (single- and
    // multi-column indexes alike); decoded via `unpack_pk_cols`.
    col("source_col_idx", TypeCode::U64, false),
    col("name", TypeCode::String, false),
    col("is_unique", TypeCode::U64, false),
    col("cache_directory", TypeCode::String, false),
];

// PK = columns [0, 1]; dep_view_id is the only payload.
pub const DEP_TAB_COLS: &[WireSysCol] = &[
    col("view_id", TypeCode::U64, false),
    col("dep_table_id", TypeCode::U64, false),
    col("dep_view_id", TypeCode::U64, false),
];

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
// System table IDs
// ---------------------------------------------------------------------------

pub const SCHEMA_TAB: u64 = 1;
pub const TABLE_TAB: u64 = 2;
pub const VIEW_TAB: u64 = 3;
pub const COL_TAB: u64 = 4;
pub const IDX_TAB: u64 = 5;
pub const DEP_TAB: u64 = 6;
pub const SEQ_TAB: u64 = 7;
pub const CIRCUIT_NODES_TAB: u64 = 11;
pub const CIRCUIT_EDGES_TAB: u64 = 12;
pub const CIRCUIT_NODE_COLUMNS_TAB: u64 = 13;
// IDs 14 and 15 were previously CIRCUIT_PARAMS_TAB and CIRCUIT_GROUP_COLS_TAB,
// now folded into CircuitNodes / CircuitNodeColumns.

/// The three circuit families in canonical `[nodes, edges, node_columns]`
/// order, each with its column slice — the ONE definition of the family list
/// and its order, which client (`run_query` ships the families in this order)
/// and engine (the worker buffers them in a 3-slot array indexed by position
/// here) must agree on.
pub const CIRCUIT_FAMILIES: [(u64, &[WireSysCol]); 3] = [
    (CIRCUIT_NODES_TAB, CIRCUIT_NODES_COLS),
    (CIRCUIT_EDGES_TAB, CIRCUIT_EDGES_COLS),
    (CIRCUIT_NODE_COLUMNS_TAB, CIRCUIT_NODE_COLUMNS_COLS),
];

/// The circuit families' compound primary key: columns `(view_id, sub)`.
pub const CIRCUIT_FAMILY_PK: &[u32] = &[0, 1];

pub const FIRST_USER_TABLE_ID: u64 = 16;
pub const FIRST_USER_SCHEMA_ID: u64 = 3;

pub const OWNER_KIND_TABLE: u64 = 0;
pub const OWNER_KIND_VIEW: u64 = 1;

// ---------------------------------------------------------------------------
// COL_TAB primary-key packing
// ---------------------------------------------------------------------------

/// Bit width of the column-index field in a packed COL_TAB PK.
pub const COL_ID_IDX_BITS: u32 = 9;

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
/// with one bit per nullable payload column, so payload columns ≤ 64.
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

// The packed u64 lays the decoded count in bits [0..4) and each column index in
// a 7-bit field at bit 4 + 7*i; the packed flag occupies bit 63. Guard the
// ceilings at compile time so bumping PK_LIST_MAX_COLS past what the encoding
// (or the index-prefix reservation) can hold fails to build instead of
// silently corrupting the catalog word.
const _: () = assert!(PK_LIST_MAX_COLS >= 1);
const _: () = assert!(
    PK_LIST_MAX_COLS <= 0xf, // count field is 4 bits
    "PK_LIST_MAX_COLS overflows the 4-bit packed count field"
);
const _: () = assert!(
    4 + 7 * PK_LIST_MAX_COLS <= 63, // column fields clear the flag bit
    "PK_LIST_MAX_COLS overflows the packed u64 column region"
);
const _: () = assert!(
    PK_LIST_MAX_COLS < MAX_PK_COLUMNS, // leave the index-prefix slot
    "PK_LIST_MAX_COLS leaves no MAX_PK_COLUMNS slot for the secondary-index prefix"
);

pub const PK_LIST_PACKED_FLAG: u64 = 1 << 63;

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
    pub fn try_from_slice(cols: &[u32]) -> Option<Self> {
        if !(1..=PK_LIST_MAX_COLS).contains(&cols.len()) {
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
        (1..=PK_LIST_MAX_COLS).contains(&self.len)
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
    if !(1..=PK_LIST_MAX_COLS).contains(&cols.len()) {
        return Err(format!(
            "column count {} out of range 1..={PK_LIST_MAX_COLS}",
            cols.len()
        ));
    }
    for (i, &c) in cols.iter().enumerate() {
        if c >= 128 {
            return Err(format!("column index {c} exceeds 127"));
        }
        if cols[..i].contains(&c) {
            return Err(format!("duplicate column {c} in list"));
        }
    }
    Ok(())
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
        (1..=PK_LIST_MAX_COLS).contains(&pk_cols.len()),
        "pack_pk_cols: count {} out of range 1..={PK_LIST_MAX_COLS}",
        pk_cols.len(),
    );
    let mut v = pk_cols.len() as u64; // bits [0..4)
    for (i, &c) in pk_cols.iter().enumerate() {
        assert!(c < 128, "pack_pk_cols: column index {c} exceeds 7-bit field");
        v |= (c as u64 & 0x7f) << (4 + 7 * i);
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
        // engine-written system-table row (always bare `0`).
        return PkColList::single(packed as u32);
    }
    let n = (packed & 0xf) as usize; // 0..=15, validated later
    let mut cols = [0u32; PK_LIST_MAX_COLS];
    for (i, slot) in cols.iter_mut().enumerate().take(n.min(PK_LIST_MAX_COLS)) {
        *slot = ((packed >> (4 + 7 * i)) & 0x7f) as u32;
    }
    PkColList { cols, len: n }
}

// ---------------------------------------------------------------------------
// TABLE_TAB.flags layout — the single source of truth shared by the gnitz-core
// writer and the gnitz-engine reader, so the bit packing cannot drift.
//
//   bit 0        unique_pk (TABLE_FLAG_UNIQUE_PK)
//   bit 1        replicated (TABLE_FLAG_REPLICATED) — full copy on every worker
//   bits [2..8)  reserved for future boolean flags
//   bits [8..16) distribution prefix length k (0 = default = full PK)
//
// `k` is byte-aligned (not bit-1-adjacent) so the boolean flag bits [1..8)
// stay free for future flags without colliding with `k`. `replicated` and a
// non-default `k` are mutually exclusive (a CLUSTER BY prefix is meaningless
// when every worker holds the full copy); the packing cannot represent that
// constraint, so DDL validation must enforce it.
// ---------------------------------------------------------------------------

/// Bit 0: the table's PK is unique (DML-enforced upsert / dedup). Set for every
/// SQL-created table.
const TABLE_FLAG_UNIQUE_PK: u64 = 1;
/// Bit 1: the table is **replicated** — every worker holds an identical full
/// copy (writes broadcast, reads single-source). Mutually exclusive with a
/// non-default `dist_prefix_len` (enforced at DDL, not by this packing).
const TABLE_FLAG_REPLICATED: u64 = 1 << 1;
/// Bit position of the distribution-prefix-length byte in `TABLE_TAB.flags`.
const TABLE_FLAG_DIST_SHIFT: u32 = 8;
/// Mask for the distribution-prefix-length byte (one byte: 0..=255). An
/// explicit prefix is `1..=PK_LIST_MAX_COLS`, well within the byte; the full
/// byte is deliberate headroom.
const TABLE_FLAG_DIST_MASK: u64 = 0xFF;

/// Pack the persisted `TABLE_TAB.flags` u64 from its logical fields. With
/// `replicated == false` and `dist_prefix_len == 0` (the default = full PK) this
/// is byte-identical to the pre-distribution-key encoding `unique_pk as u64`.
#[inline]
pub fn pack_table_flags(unique_pk: bool, replicated: bool, dist_prefix_len: usize) -> u64 {
    (((dist_prefix_len as u64) & TABLE_FLAG_DIST_MASK) << TABLE_FLAG_DIST_SHIFT)
        | if unique_pk { TABLE_FLAG_UNIQUE_PK } else { 0 }
        | if replicated { TABLE_FLAG_REPLICATED } else { 0 }
}

/// Decode the `unique_pk` bit from `TABLE_TAB.flags`.
#[inline]
pub fn table_flags_unique(flags: u64) -> bool {
    flags & TABLE_FLAG_UNIQUE_PK != 0
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
        // Default (not replicated, k = 0 = full PK) is byte-identical to the old
        // `unique_pk as u64`.
        assert_eq!(pack_table_flags(false, false, 0), 0);
        assert_eq!(pack_table_flags(true, false, 0), 1);
        // k rides in byte 1; the unique and replicated bits are untouched.
        for &uniq in &[false, true] {
            for &repl in &[false, true] {
                for k in 0..=PK_LIST_MAX_COLS {
                    let f = pack_table_flags(uniq, repl, k);
                    assert_eq!(table_flags_dist_prefix(f), k);
                    assert_eq!(table_flags_unique(f), uniq);
                    assert_eq!(table_flags_replicated(f), repl);
                }
            }
        }
        // `replicated` is bit 1; reserved bits [2..8) stay clear of the k byte.
        assert_eq!(pack_table_flags(true, true, 0) & 0xFF, 0b11);
        assert_eq!(pack_table_flags(true, true, 2) >> TABLE_FLAG_DIST_SHIFT, 2);
        assert_eq!(
            pack_table_flags(true, true, 2) & 0xFC,
            0,
            "reserved bits [2..8) are free"
        );
    }
}
