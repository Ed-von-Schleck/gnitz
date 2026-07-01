//! Wire protocol flags, packed wire-level fields, conflict mode, status codes,
//! and per-column metadata flags.

// ---------------------------------------------------------------------------
// Wire protocol flags
// ---------------------------------------------------------------------------

/// Bits 0-15 are the SAL-level flags carried verbatim from the on-disk log
/// into every control block.  Bits 16+ are wire-level fields (conflict mode,
/// schema version) that are encoded by the sender and decoded by the receiver.
const SAL_FLAGS_MASK: u64 = 0x0000_FFFF;

pub const FLAG_SHUTDOWN: u64 = 4;
pub const FLAG_DDL_SYNC: u64 = 8;
pub const FLAG_EXCHANGE: u64 = 16;
pub const FLAG_PUSH: u64 = 32;
pub const FLAG_HAS_PK: u64 = 64;
pub const FLAG_SEEK: u64 = 128;
pub const FLAG_SEEK_BY_INDEX: u64 = 256;
pub const FLAG_HAS_SCHEMA: u64 = 1 << 48;
pub const FLAG_HAS_DATA: u64 = 1 << 49;
/// Set on every per-worker scan response frame. Absent on the terminal
/// frame sent by the master after all worker frames. Clients loop on
/// `recv_message` until they see a frame without this bit.
pub const FLAG_CONTINUATION: u64 = 1 << 52;

/// GET_INDICES request flag. Client-only and never written to SAL, so it sits
/// above the SAL mirror (bits 0-15) and the wire packed fields rather than in
/// the request-flag run at 4..256 — all of bits 0-15 are already allocated.
/// Bit 54 is the lowest free request bit (50/51/53 are the engine-internal
/// FLAG_BATCH_SORTED / FLAG_BATCH_CONSOLIDATED / FLAG_SCAN_LAST defined in
/// `runtime/wire.rs`).
pub const FLAG_GET_INDICES: u64 = 1 << 54;

/// SEEK_BY_INDEX_RANGE request flag. The client→master leg of an ordered
/// range scan over a secondary index. Like GET_INDICES it is a high request
/// bit (54 taken, 55 the next free) that rides above the SAL mirror and the
/// bit-16–47 packed fields, so it never collides with a packed field. Unlike
/// GET_INDICES (answered master-locally) a range seek *fans out* to workers,
/// so the master→worker leg carries a *separate* `u32` SAL dispatch flag
/// (`runtime::sal::FLAG_SEEK_BY_INDEX_RANGE_SAL`); this high bit is never
/// written to the SAL group header.
pub const FLAG_SEEK_BY_INDEX_RANGE: u64 = 1 << 55;

/// ALLOCATE_SERIAL_RANGE request flag. The client→master leg of a user-table
/// SERIAL sequence range reservation. Like the other high request bits it rides
/// above the SAL mirror (bits 0-15) and the bit-16–47 packed fields; bit 55 is
/// the current top, so 56 is the next free. The request carries
/// `target_id = table_id` (the sequence key) and the range `count` in
/// `seek_col_idx`; this high bit is never written to the SAL group header.
pub const FLAG_ALLOCATE_SERIAL_RANGE: u64 = 1 << 56;

// ---------------------------------------------------------------------------
// Wire-level packed fields: bits 16-39 of wire_flags
// ---------------------------------------------------------------------------

/// Bits 16-23: conflict mode (8 bits). Value 0 = Update (default).
const WIRE_CONFLICT_MODE_SHIFT: u32 = 16;
const WIRE_CONFLICT_MODE_MASK: u64 = 0xFF_u64 << WIRE_CONFLICT_MODE_SHIFT;
/// Bits 24-39: schema version (16 bits). Value 0 = client has no cached schema.
const WIRE_SCHEMA_VERSION_SHIFT: u32 = 24;
const WIRE_SCHEMA_VERSION_MASK: u64 = 0xFFFF_u64 << WIRE_SCHEMA_VERSION_SHIFT;
/// Bits 40-47: index-metadata version (8 bits). 0 = client has no cached list.
const WIRE_INDEX_VERSION_SHIFT: u32 = 40;
const WIRE_INDEX_VERSION_MASK: u64 = 0xFF_u64 << WIRE_INDEX_VERSION_SHIFT;

// Compile-time guard: SAL flags (bits 0-15) must stay clear of the wire-level
// packed fields, those fields must not overlap each other, the packed fields
// must not collide with the high boolean flags, and the GET_INDICES request
// flag must avoid all of the above (catches a regression to a colliding bit at
// compile time).
const _: () = {
    let packed = WIRE_CONFLICT_MODE_MASK | WIRE_SCHEMA_VERSION_MASK | WIRE_INDEX_VERSION_MASK;
    assert!(SAL_FLAGS_MASK & packed == 0);
    assert!(WIRE_CONFLICT_MODE_MASK & WIRE_SCHEMA_VERSION_MASK == 0);
    assert!(WIRE_SCHEMA_VERSION_MASK & WIRE_INDEX_VERSION_MASK == 0);
    assert!(packed & (FLAG_HAS_SCHEMA | FLAG_HAS_DATA | FLAG_CONTINUATION) == 0);
    assert!(FLAG_GET_INDICES & (SAL_FLAGS_MASK | packed | FLAG_HAS_SCHEMA | FLAG_HAS_DATA | FLAG_CONTINUATION) == 0);
    assert!(
        FLAG_SEEK_BY_INDEX_RANGE
            & (SAL_FLAGS_MASK | packed | FLAG_HAS_SCHEMA | FLAG_HAS_DATA | FLAG_CONTINUATION | FLAG_GET_INDICES)
            == 0
    );
    assert!(
        FLAG_ALLOCATE_SERIAL_RANGE
            & (SAL_FLAGS_MASK
                | packed
                | FLAG_HAS_SCHEMA
                | FLAG_HAS_DATA
                | FLAG_CONTINUATION
                | FLAG_GET_INDICES
                | FLAG_SEEK_BY_INDEX_RANGE)
            == 0
    );
};

#[inline]
pub fn wire_flags_set_conflict_mode(flags: u64, mode: WireConflictMode) -> u64 {
    (flags & !WIRE_CONFLICT_MODE_MASK) | ((mode as u64) << WIRE_CONFLICT_MODE_SHIFT)
}
#[inline]
pub fn wire_flags_get_conflict_mode(flags: u64) -> WireConflictMode {
    WireConflictMode::from_u8(((flags & WIRE_CONFLICT_MODE_MASK) >> WIRE_CONFLICT_MODE_SHIFT) as u8)
}
#[inline]
pub fn wire_flags_set_schema_version(flags: u64, version: u16) -> u64 {
    (flags & !WIRE_SCHEMA_VERSION_MASK) | ((version as u64) << WIRE_SCHEMA_VERSION_SHIFT)
}
#[inline]
pub fn wire_flags_get_schema_version(flags: u64) -> u16 {
    ((flags & WIRE_SCHEMA_VERSION_MASK) >> WIRE_SCHEMA_VERSION_SHIFT) as u16
}
#[inline]
pub fn wire_flags_set_index_version(flags: u64, version: u8) -> u64 {
    (flags & !WIRE_INDEX_VERSION_MASK) | ((version as u64) << WIRE_INDEX_VERSION_SHIFT)
}
#[inline]
pub fn wire_flags_get_index_version(flags: u64) -> u8 {
    ((flags & WIRE_INDEX_VERSION_MASK) >> WIRE_INDEX_VERSION_SHIFT) as u8
}
/// Returns true when the server should include a schema block in its response.
/// `client_version == 0` means the client has no cached schema; any non-zero
/// mismatch means the server's schema has changed since the client last saw it.
#[inline]
pub fn wire_should_include_schema(client_version: u16, server_version: u16) -> bool {
    client_version == 0 || client_version != server_version
}

// ---------------------------------------------------------------------------
// Wire-level conflict mode for INSERT / UPSERT semantics
// ---------------------------------------------------------------------------

/// Conflict-resolution mode packed into bits 16-23 of `wire_flags` on
/// FLAG_PUSH messages. Discriminant 0 = Update (default), so zero-filled
/// flags resolve to the upsert default without explicit encoding.
#[repr(u8)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum WireConflictMode {
    /// Retract-and-insert on PK conflict. Used for SQL `UPDATE`,
    /// `INSERT ... ON CONFLICT ... DO UPDATE` (after client-side
    /// merging), and explicit Python `push(conflict_mode="update")`.
    #[default]
    Update = 0,
    /// Reject the batch on any PK conflict. The master runs both an
    /// intra-batch duplicate check and an against-store PK existence
    /// check, and returns a PG-style `duplicate key value violates
    /// unique constraint` error.
    Error = 1,
}

impl WireConflictMode {
    #[inline]
    pub const fn as_u8(self) -> u8 {
        self as u8
    }

    /// Unknown bytes decode as `Update` so forward-compatible decoders
    /// still see last-write-wins semantics.
    #[inline]
    pub const fn from_u8(v: u8) -> Self {
        match v {
            1 => WireConflictMode::Error,
            _ => WireConflictMode::Update,
        }
    }
}

pub const STATUS_OK: u32 = 0;
pub const STATUS_ERROR: u32 = 1;
/// Server-side version mismatch on schema-less PUSH: client must evict its
/// schema cache entry for the target table and retry with the full schema.
pub const STATUS_SCHEMA_MISMATCH: u32 = 2;
/// SEEK_BY_INDEX against a column with no secondary index. Control-only frame
/// (no schema/data/error payload); the SQL planner uses it to fall back to a
/// scan or a CREATE INDEX hint without a prior catalog probe.
pub const STATUS_NO_INDEX: u32 = 3;

pub const META_FLAG_NULLABLE: u64 = 1;
pub const META_FLAG_IS_PK: u64 = 2;

/// PK position (0-indexed) within the PK tuple for the column carrying
/// `META_FLAG_IS_PK`. Bits 8..16 of the per-column flags word. Single-PK
/// schemas leave this at 0; compound-PK schemas encode each PK column's
/// position so the decoder can reconstruct `pk_indices` in declaration
/// order rather than column-position order (e.g. `PRIMARY KEY (b, a)`
/// with `a` at column 1 and `b` at column 2 must decode to `[2, 1]`).
pub const META_FLAG_PK_POS_SHIFT: u32 = 8;
pub const META_FLAG_PK_POS_MASK: u64 = 0xFF << META_FLAG_PK_POS_SHIFT;
