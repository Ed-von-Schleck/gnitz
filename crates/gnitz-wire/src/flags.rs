//! Wire protocol flags, packed wire-level fields, conflict mode, status codes,
//! and per-column metadata flags.

// ---------------------------------------------------------------------------
// Wire protocol flags
// ---------------------------------------------------------------------------

/// Bits 0-15: the SAL-level flags carried verbatim from the on-disk log into
/// every control block. Bits 16+ are wire-level fields (conflict mode, schema
/// version) encoded by the sender and decoded by the receiver.
///
/// Exported so the engine can assert its own group-header flags — which live in
/// a `u32` above this block — stay clear of it.
pub const SAL_FLAGS_MASK: u64 = 0x0000_FFFF;

pub const FLAG_SHUTDOWN: u64 = 4;
pub const FLAG_DDL_SYNC: u64 = 8;
pub const FLAG_EXCHANGE: u64 = 16;
/// Marks a frame as a data push, on both the client→master and master→SAL/
/// worker legs. Client push frames carry it so push-vs-scan routing never
/// depends on data presence: an empty batch (a legitimate empty Z-set delta)
/// is ACKed as a no-op push (LSN 0) instead of being mistaken for a scan.
pub const FLAG_PUSH: u64 = 32;
pub const FLAG_HAS_PK: u64 = 64;
pub const FLAG_SEEK: u64 = 128;
pub const FLAG_SEEK_BY_INDEX: u64 = 256;
/// SCAN_SPEC request flag. The client→master leg of a parameterized bounded
/// read (`ReadSpec`). Unlike the high request bits (GET_INDICES, SEEK_BY_INDEX_
/// RANGE, …) this lives *inside* the SAL flag block (bits 0-15, `SAL_FLAGS_MASK`)
/// so it is carried verbatim from the wire frame into the SAL group header — one
/// allocation, no separate u32 dispatch flag. The engine mirrors it as a `u32`.
pub const FLAG_SCAN_SPEC: u64 = 1 << 10;

// ---------------------------------------------------------------------------
// Engine-internal SAL group-header flags that nonetheless live in the shared
// 0-15 block.
//
// The engine never sends these to a client, but it allocates them out of the
// same sixteen bits the wire request flags above use, because a group header
// carries the block verbatim. Two crates allocating one bit range needs one
// table, or the second allocator has to read the first's source to find a gap.
// So the *bit* is reserved here — covered by the disjointness guard below,
// exactly as `FLAG_BATCH_SORTED` and friends are — while the engine keeps the
// meaning and mirrors each as the `u32` its group header stores.
//
// Bits 16+ of the group header are a different word from the wire `u64` (the
// header's flags field is `u32`, and the packed wire fields at bits 16-47 never
// reach it), so the engine's flags above bit 15 are its own namespace and are
// not reserved here.
// ---------------------------------------------------------------------------

/// Relay leg of a distributed exchange (bit 9).
pub const FLAG_EXCHANGE_RELAY: u64 = 1 << 9;
/// Initial full-source scan feeding a newly created view (bit 11).
pub const FLAG_BACKFILL: u64 = 1 << 11;
/// Drive one view-maintenance tick (bit 12).
pub const FLAG_TICK: u64 = 1 << 12;
/// Base round of a checkpoint: flush base and system tables (bit 14).
pub const FLAG_FLUSH: u64 = 1 << 14;
/// Closing commit sentinel of an atomic write zone (bit 15).
pub const FLAG_TXN_COMMIT: u64 = 1 << 15;
pub const FLAG_HAS_SCHEMA: u64 = 1 << 48;
pub const FLAG_HAS_DATA: u64 = 1 << 49;
/// Set on every per-worker scan response frame. Absent on the terminal
/// frame sent by the master after all worker frames. Clients loop on
/// `recv_message` until they see a frame without this bit.
pub const FLAG_CONTINUATION: u64 = 1 << 52;

/// GET_INDICES request flag. Client-only and never written to SAL, so it sits
/// above the SAL mirror (bits 0-15) and the wire packed fields rather than in
/// the request-flag run at 4..256 — all of bits 0-15 are already allocated.
pub const FLAG_GET_INDICES: u64 = 1 << 54;

/// ALLOCATE_SERIAL_RANGE request flag. The client→master leg of a user-table
/// SERIAL sequence range reservation. Like the other high request bits it rides
/// above the SAL mirror (bits 0-15) and the bit-16–47 packed fields. The request carries
/// `target_id = table_id` (the sequence key) and the range `count` in
/// `seek_col_idx`; this high bit is never written to the SAL group header.
pub const FLAG_ALLOCATE_SERIAL_RANGE: u64 = 1 << 56;

/// DDL_TXN request flag. The client→master frame carrying an atomic bundle of
/// system-table family batches for one catalog write (CREATE/DROP of
/// table/view/index/schema, CREATE SCHEMA/INDEX). Purely a wire-level decode
/// hint: it is consumed at `handle_message` routing and is NEVER written to the
/// SAL — each family is broadcast under its own `FLAG_DDL_SYNC` group and the
/// zone's `FLAG_TXN_COMMIT` sentinel is unrelated `sal.rs` state — so it takes a
/// high client-only bit, above the SAL mirror at bits 0-15 and the bit-16–47
/// packed fields. Disjoint from every other flag by the compile-time guard below.
pub const FLAG_DDL_TXN: u64 = 1 << 57;

/// ID-allocation request flags (client→master, answered master-locally,
/// never written to the SAL). High client-only bits above FLAG_DDL_TXN.
pub const FLAG_ALLOCATE_TABLE_ID: u64 = 1 << 58;
pub const FLAG_ALLOCATE_SCHEMA_ID: u64 = 1 << 59;
pub const FLAG_ALLOCATE_INDEX_ID: u64 = 1 << 60;

/// PUSH_TXN request flag. The client→master frame carrying an atomic bundle of
/// **user-table** family batches for one durable write zone (the client-facing
/// analogue of `FLAG_DDL_TXN`, which is exclusive to system families). Purely a
/// wire-level decode hint consumed at `handle_message` routing; it is NEVER
/// written to the SAL — each family is emitted under its own `FLAG_PUSH` group
/// inside one zone closed by the engine-internal `FLAG_TXN_COMMIT` sentinel.
pub const FLAG_PUSH_TXN: u64 = 1 << 61;

/// SCAN_MULTI request flag. Client→master frame naming N relations to snapshot
/// at one SAL cut. Wire-level routing hint consumed at `handle_message`; never
/// written to the SAL.
pub const FLAG_SCAN_MULTI: u64 = 1 << 62;

/// Maximum relations in one SCAN_MULTI request. A product / master-state limit
/// (the master holds N `ScanLease`s and N reply trains of bookkeeping); a
/// handful of related tables covers realistic consistent snapshots. Shared by
/// the client encoder's shape check and the engine's decode/handler so both
/// sides agree on the ceiling.
pub const SCAN_MULTI_MAX_RELATIONS: usize = 16;

/// Validate a `scan_multi` / `scan_many` relation list against the SCAN_MULTI
/// wire-shape rules: non-empty, at most `SCAN_MULTI_MAX_RELATIONS`, no duplicate
/// tid. The single source of truth for these checks, shared by every entry point
/// — the sync `Session::scan_multi`, the async `PyAsyncTransport::scan_many`, and
/// the server's authoritative `scan_multi_body` — so all three reject an identical
/// set with identical wording.
///
/// The empty check is load-bearing on the client, not cosmetic: an empty list
/// encodes a count=0 frame whose single server error frame the positional N-train
/// read loop (N=0) never consumes, leaving it unread and desyncing the
/// connection. Over-cap / duplicate are non-empty, so their round-tripped error
/// frame is always consumed by the first `recv_scan`; validating them
/// client-side is a fast-fail nicety. The O(n²) duplicate scan is over n ≤ 16.
pub fn validate_scan_multi_tids(tids: &[u64]) -> Result<(), String> {
    if tids.is_empty() {
        return Err("SCAN_MULTI: empty relation list".to_string());
    }
    if tids.len() > SCAN_MULTI_MAX_RELATIONS {
        return Err(format!(
            "SCAN_MULTI: too many relations ({}, max {SCAN_MULTI_MAX_RELATIONS})",
            tids.len()
        ));
    }
    for (i, &tid) in tids.iter().enumerate() {
        if tids[..i].contains(&tid) {
            return Err(format!("SCAN_MULTI: duplicate relation {tid}"));
        }
    }
    Ok(())
}

/// FIFO-reply directive on a master→worker scan group. Set by
/// `dispatch_scan_multi_fanout` on every group of a multi-scan so the worker
/// routes that relation's reply through `pending_streams` (strict FIFO ring
/// order) instead of the immediate-emit fast path — the multi-scan drain
/// requires ring order to equal request order.
///
/// **Deliberately aliases `FLAG_SCAN_MULTI`** (defined as its value so the two
/// can never silently drift apart); disambiguated purely by frame direction. Bit
/// 62 on a *client→master* frame is the SCAN_MULTI request (consumed at
/// `handle_message`, never propagated into a SAL group); on a *master→worker scan
/// group* it is this FIFO directive. The full u64 survives into the group's
/// control block, so the worker reads it back as an ordinary wire flag. It is
/// therefore deliberately **not** added to the `high_flags` disjointness guard
/// below (doing so would trip the collision assert against `FLAG_SCAN_MULTI`,
/// which the guard already covers).
pub const FLAG_SCAN_FIFO_REPLY: u64 = FLAG_SCAN_MULTI;

/// Engine-internal batch-layout claims stamped on SAL / W2M frames
/// (sorted / consolidated); never sent to clients. Defined here so the
/// disjointness guard below covers them against every wire flag.
pub const FLAG_BATCH_SORTED: u64 = 1 << 50;
pub const FLAG_BATCH_CONSOLIDATED: u64 = 1 << 51;
/// Engine-internal W2M flag set on the last (or only) scan chunk from a
/// worker; stripped before reaching clients. See the engine's reply path
/// for why FLAG_CONTINUATION cannot carry this meaning.
pub const FLAG_SCAN_LAST: u64 = 1 << 53;

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

// Compile-time guard over the whole `u64`: no two flags may share a bit, and no
// flag may land in the wire-level packed fields.
//
// The sweep runs in two passes because the 0-15 block and the 48+ block are
// allocated by different rules, but both accumulate into one mask, so a
// collision anywhere fails the build. The 0-15 pass covers the engine-owned
// group-header bits too — that is the whole reason they are declared here.
const _: () = {
    let packed = WIRE_CONFLICT_MODE_MASK | WIRE_SCHEMA_VERSION_MASK | WIRE_INDEX_VERSION_MASK;
    assert!(SAL_FLAGS_MASK & packed == 0);
    assert!(WIRE_CONFLICT_MODE_MASK & WIRE_SCHEMA_VERSION_MASK == 0);
    assert!(WIRE_SCHEMA_VERSION_MASK & WIRE_INDEX_VERSION_MASK == 0);

    // Bits 0-15: the block a SAL group header carries verbatim, co-allocated by
    // the client request flags and the engine's group-header flags.
    let sal_block = [
        FLAG_SHUTDOWN,
        FLAG_DDL_SYNC,
        FLAG_EXCHANGE,
        FLAG_PUSH,
        FLAG_HAS_PK,
        FLAG_SEEK,
        FLAG_SEEK_BY_INDEX,
        FLAG_EXCHANGE_RELAY,
        FLAG_SCAN_SPEC,
        FLAG_BACKFILL,
        FLAG_TICK,
        FLAG_FLUSH,
        FLAG_TXN_COMMIT,
    ];
    // Bits 48+: booleans that never enter a SAL group header.
    let high_flags = [
        FLAG_HAS_SCHEMA,
        FLAG_HAS_DATA,
        FLAG_BATCH_SORTED,
        FLAG_BATCH_CONSOLIDATED,
        FLAG_CONTINUATION,
        FLAG_SCAN_LAST,
        FLAG_GET_INDICES,
        FLAG_ALLOCATE_SERIAL_RANGE,
        FLAG_DDL_TXN,
        FLAG_ALLOCATE_TABLE_ID,
        FLAG_ALLOCATE_SCHEMA_ID,
        FLAG_ALLOCATE_INDEX_ID,
        FLAG_PUSH_TXN,
        FLAG_SCAN_MULTI,
    ];

    let mut acc = 0u64;
    let mut i = 0;
    while i < sal_block.len() {
        assert!(
            sal_block[i] & SAL_FLAGS_MASK == sal_block[i],
            "SAL-block flag outside bits 0-15"
        );
        assert!(sal_block[i] & acc == 0, "wire flag bit collision");
        acc |= sal_block[i];
        i += 1;
    }
    acc = SAL_FLAGS_MASK | packed;
    i = 0;
    while i < high_flags.len() {
        assert!(high_flags[i] & acc == 0, "wire flag bit collision");
        acc |= high_flags[i];
        i += 1;
    }
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

impl std::str::FromStr for WireConflictMode {
    type Err = String;

    /// The two modes' user-facing names — the vocabulary every client binding
    /// exposes as `push(conflict_mode=...)`. Owned here, next to the
    /// discriminants, so no binding gets to invent a third spelling.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "update" => Ok(WireConflictMode::Update),
            "error" => Ok(WireConflictMode::Error),
            other => Err(format!("invalid conflict mode '{other}', expected 'update' or 'error'")),
        }
    }
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
/// A user-table TXN failed an OCC precondition: some table it declared a basis
/// for was written since that basis. Control-only frame carrying the fresh basis
/// (`published()`) in `seek_pk` and an empty message; the client synthesizes any
/// human-readable text from the tid it sent and either retries (autocommit RMW)
/// or surfaces the conflict (BEGIN/COMMIT). Cleanly retryable — nothing validated,
/// nothing written.
pub const STATUS_TXN_CONFLICT: u32 = 4;

pub const META_FLAG_NULLABLE: u64 = 1;
pub const META_FLAG_IS_PK: u64 = 2;
/// The column is a hidden key slot: a physical schema column (it holds a real
/// PK/routing value) that no presentation surface exposes — excluded from
/// wildcard expansion, name resolution, duplicate-name checks, and client rows.
/// Purely a presentation marker: the PK region, routing, sort, and
/// consolidation are all blind to it. Bit 2 (value 4), between `META_FLAG_IS_PK`
/// (bit 1) and the PK-pos byte at bits 8..16.
pub const META_FLAG_HIDDEN: u64 = 4;

/// PK position (0-indexed) within the PK tuple for the column carrying
/// `META_FLAG_IS_PK`. Bits 8..16 of the per-column flags word. Single-PK
/// schemas leave this at 0; compound-PK schemas encode each PK column's
/// position so the decoder can reconstruct `pk_indices` in declaration
/// order rather than column-position order (e.g. `PRIMARY KEY (b, a)`
/// with `a` at column 1 and `b` at column 2 must decode to `[2, 1]`).
const META_FLAG_PK_POS_SHIFT: u32 = 8;
const META_FLAG_PK_POS_MASK: u64 = 0xFF << META_FLAG_PK_POS_SHIFT;

/// Pack a schema block's per-column metadata flags word from its logical
/// fields. `pk_pos` is the column's 0-indexed position in the PK tuple, or
/// `None` for a non-PK column.
///
/// This word is written by both ends (the client encodes a schema it pushes,
/// the engine encodes the schema it replies with) and read by both, so the bit
/// layout lives here with its accessors rather than being re-spelled per codec —
/// the same rule [`pack_table_flags`] follows for `TABLE_TAB.flags`.
#[inline]
pub fn pack_col_meta_flags(nullable: bool, hidden: bool, pk_pos: Option<u8>) -> u64 {
    let pk = match pk_pos {
        Some(p) => META_FLAG_IS_PK | ((p as u64) << META_FLAG_PK_POS_SHIFT),
        None => 0,
    };
    pk | if nullable { META_FLAG_NULLABLE } else { 0 } | if hidden { META_FLAG_HIDDEN } else { 0 }
}

/// Decode the `nullable` bit from a per-column metadata flags word.
#[inline]
pub fn col_meta_nullable(flags: u64) -> bool {
    flags & META_FLAG_NULLABLE != 0
}

/// Decode the `hidden` bit (see [`META_FLAG_HIDDEN`]).
#[inline]
pub fn col_meta_hidden(flags: u64) -> bool {
    flags & META_FLAG_HIDDEN != 0
}

/// The column's 0-indexed position within the PK tuple, or `None` when it is
/// not a PK column. Decoders sort their PK columns by this to rebuild the
/// declared PK order.
#[inline]
pub fn col_meta_pk_pos(flags: u64) -> Option<u8> {
    (flags & META_FLAG_IS_PK != 0).then_some(((flags & META_FLAG_PK_POS_MASK) >> META_FLAG_PK_POS_SHIFT) as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The per-column metadata word both ends of the schema block encode and
    /// decode: every field round-trips, and a non-PK column reports no position
    /// however the other bits are set.
    #[test]
    fn col_meta_flags_roundtrip() {
        for &nullable in &[false, true] {
            for &hidden in &[false, true] {
                for pk_pos in [None, Some(0u8), Some(1), Some(3), Some(u8::MAX)] {
                    let f = pack_col_meta_flags(nullable, hidden, pk_pos);
                    assert_eq!(col_meta_nullable(f), nullable);
                    assert_eq!(col_meta_hidden(f), hidden);
                    assert_eq!(col_meta_pk_pos(f), pk_pos);
                }
            }
        }
        // The pre-compound spelling: a single-PK column is `IS_PK` with a zero
        // position byte.
        assert_eq!(pack_col_meta_flags(false, false, Some(0)), META_FLAG_IS_PK);
        assert_eq!(pack_col_meta_flags(true, false, None), META_FLAG_NULLABLE);
    }

    #[test]
    fn validate_scan_multi_tids_accepts_valid_lists() {
        assert!(validate_scan_multi_tids(&[7]).is_ok());
        let full: Vec<u64> = (0..SCAN_MULTI_MAX_RELATIONS as u64).collect();
        assert!(validate_scan_multi_tids(&full).is_ok(), "a full-cap list is valid");
    }

    #[test]
    fn validate_scan_multi_tids_rejects_empty() {
        assert_eq!(
            validate_scan_multi_tids(&[]).unwrap_err(),
            "SCAN_MULTI: empty relation list"
        );
    }

    #[test]
    fn validate_scan_multi_tids_rejects_over_cap() {
        let over: Vec<u64> = (0..=SCAN_MULTI_MAX_RELATIONS as u64).collect();
        let err = validate_scan_multi_tids(&over).unwrap_err();
        assert!(err.starts_with("SCAN_MULTI: too many relations"), "got: {err}");
    }

    #[test]
    fn validate_scan_multi_tids_rejects_duplicate() {
        assert_eq!(
            validate_scan_multi_tids(&[7, 8, 7]).unwrap_err(),
            "SCAN_MULTI: duplicate relation 7"
        );
    }
}
