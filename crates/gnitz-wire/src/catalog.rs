//! System-catalog wire layout: the shared `WireSysCol` descriptor, system
//! table column lists and IDs, schema sizing caps, and the compound-PK
//! column-list codec for the persisted `TABLE_TAB.pk_col_idx` u64.

use crate::TypeCode;

// ---------------------------------------------------------------------------
// System table column descriptors — shared single source of truth
// ---------------------------------------------------------------------------

pub struct WireSysCol {
    pub name:      &'static str,
    pub type_code: TypeCode,
    pub nullable:  bool,
}

// Circuit catalog tables use a real compound primary key `(view_id, sub)`
// instead of hand-packing both halves into one U128 column. `sub` is the
// per-view secondary key (node_id, an (dst_node,dst_port) pack, or a
// (node_id,kind,position) pack). The remaining columns denormalise the
// decoded fields as payload so the engine's logical-column readers are
// unchanged. PK = columns [0, 1].
pub const CIRCUIT_NODES_COLS: &[WireSysCol] = &[
    WireSysCol { name: "view_id",      type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "sub",          type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "node_id",      type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "opcode",       type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "source_table", type_code: TypeCode::U64,  nullable: true  },
    WireSysCol { name: "reindex_col",  type_code: TypeCode::U64,  nullable: true  },
    WireSysCol { name: "expr_program", type_code: TypeCode::Blob, nullable: true  },
];

pub const CIRCUIT_EDGES_COLS: &[WireSysCol] = &[
    WireSysCol { name: "view_id",  type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "sub",      type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "dst_node", type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "dst_port", type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "src_node", type_code: TypeCode::U64,  nullable: false },
];

pub const CIRCUIT_NODE_COLUMNS_COLS: &[WireSysCol] = &[
    WireSysCol { name: "view_id",     type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "sub",         type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "node_id",     type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "kind",        type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "position",    type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "value1",      type_code: TypeCode::U64,  nullable: false },
    WireSysCol { name: "value2",      type_code: TypeCode::U64,  nullable: false },
];

// ---------------------------------------------------------------------------
// System table IDs
// ---------------------------------------------------------------------------

pub const SCHEMA_TAB:           u64 = 1;
pub const TABLE_TAB:            u64 = 2;
pub const VIEW_TAB:             u64 = 3;
pub const COL_TAB:              u64 = 4;
pub const IDX_TAB:              u64 = 5;
pub const DEP_TAB:              u64 = 6;
pub const SEQ_TAB:              u64 = 7;
pub const CIRCUIT_NODES_TAB:        u64 = 11;
pub const CIRCUIT_EDGES_TAB:        u64 = 12;
pub const CIRCUIT_NODE_COLUMNS_TAB: u64 = 13;
// IDs 14 and 15 were previously CIRCUIT_PARAMS_TAB and CIRCUIT_GROUP_COLS_TAB,
// now folded into CircuitNodes / CircuitNodeColumns.

pub const FIRST_USER_TABLE_ID:  u64 = 16;
pub const FIRST_USER_SCHEMA_ID: u64 = 3;

pub const OWNER_KIND_TABLE: u64 = 0;
pub const OWNER_KIND_VIEW:  u64 = 1;

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
            "User identifiers cannot start with '_' (reserved for system prefix): {name}"));
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
/// PK column — U128, UUID; STRING and BLOB are rejected by schema
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
const _: () = assert!(PK_LIST_MAX_COLS <= 0xf,                 // count field is 4 bits
    "PK_LIST_MAX_COLS overflows the 4-bit packed count field");
const _: () = assert!(4 + 7 * PK_LIST_MAX_COLS <= 63,         // column fields clear the flag bit
    "PK_LIST_MAX_COLS overflows the packed u64 column region");
const _: () = assert!(PK_LIST_MAX_COLS < MAX_PK_COLUMNS,      // leave the index-prefix slot
    "PK_LIST_MAX_COLS leaves no MAX_PK_COLUMNS slot for the secondary-index prefix");

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
        assert!(
            (1..=PK_LIST_MAX_COLS).contains(&cols.len()),
            "PkColList::from_slice: count {} out of range 1..={PK_LIST_MAX_COLS}",
            cols.len(),
        );
        let mut arr = [0u32; PK_LIST_MAX_COLS];
        arr[..cols.len()].copy_from_slice(cols);
        PkColList { cols: arr, len: cols.len() }
    }
    /// The count exactly as decoded from the wire. May be 0 or larger than
    /// `PK_LIST_MAX_COLS` for a malformed/crafted packed value — deliberately
    /// NOT clamped, because `is_well_formed` gates on this raw value to
    /// reject out-of-range counts. Not a safe slice length: iterate
    /// `as_slice()` instead.
    pub fn decoded_count(&self) -> usize { self.len }
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
    pub fn as_slice(&self) -> &[u32] { &self.cols[..self.len.min(PK_LIST_MAX_COLS)] }
}

/// The `Err`-returning form of [`pack_pk_cols`]' panicking contract, plus the
/// no-duplicates rule both consumers (table PKs and index column lists) share:
/// count in `1..=PK_LIST_MAX_COLS`, every index within the 7-bit field, no
/// repeated column. Call this at user-input boundaries so `pack_pk_cols` and
/// `PkColList::from_slice` can never panic downstream.
pub fn validate_pk_col_list(cols: &[u32]) -> Result<(), String> {
    if !(1..=PK_LIST_MAX_COLS).contains(&cols.len()) {
        return Err(format!(
            "column count {} out of range 1..={PK_LIST_MAX_COLS}", cols.len()));
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

/// Pack a PK column-index list into the persisted `u64` form. Panics on a
/// violated contract because a silent truncation here corrupts the
/// catalog encoding; callers (client + engine) must reject out-of-range
/// lists before calling this (see [`validate_pk_col_list`]).
pub fn pack_pk_cols(pk_cols: &[u32]) -> u64 {
    assert!(
        (1..=PK_LIST_MAX_COLS).contains(&pk_cols.len()),
        "pack_pk_cols: count {} out of range 1..={PK_LIST_MAX_COLS}", pk_cols.len(),
    );
    let mut v = pk_cols.len() as u64;            // bits [0..4)
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
    let n = (packed & 0xf) as usize;             // 0..=15, validated later
    let mut cols = [0u32; PK_LIST_MAX_COLS];
    for (i, slot) in cols.iter_mut().enumerate().take(n.min(PK_LIST_MAX_COLS)) {
        *slot = ((packed >> (4 + 7 * i)) & 0x7f) as u32;
    }
    PkColList { cols, len: n }
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
}
