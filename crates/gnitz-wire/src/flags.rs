//! Wire protocol flags, packed wire-level fields, conflict mode, status codes,
//! and per-column metadata flags.

// ---------------------------------------------------------------------------
// Wire protocol flags
// ---------------------------------------------------------------------------

// Bits 0-15 hold the request verbs and the W2M exchange flag; bits 16-41 are
// wire-level fields (conflict mode, schema version, probe mode) encoded by the
// sender and decoded by the receiver; bits 48+ are booleans.

pub const FLAG_EXCHANGE: u64 = 16;
/// Marks a frame as a data push, on both the client→master and master→SAL/
/// worker legs. Client push frames carry it so push-vs-scan routing never
/// depends on data presence: an empty batch (a legitimate empty Z-set delta)
/// is ACKed as a no-op push (LSN 0) instead of being mistaken for a scan.
pub const FLAG_PUSH: u64 = 32;
pub const FLAG_SEEK: u64 = 128;
pub const FLAG_SEEK_BY_INDEX: u64 = 256;
/// SCAN_SPEC request flag. The client→master leg of a parameterized bounded
/// read (`ReadSpec`).
pub const FLAG_SCAN_SPEC: u64 = 1 << 10;
/// DELTA_POLL request flag. The batched form of [`FLAG_SCAN_SPEC`]: one frame
/// naming N mirrored views, each with its own delta-bounded `ReadSpec` and
/// client-authored reply schema.
pub const FLAG_DELTA_POLL: u64 = 1 << 11;

pub const FLAG_HAS_SCHEMA: u64 = 1 << 48;
pub const FLAG_HAS_DATA: u64 = 1 << 49;
/// Set on every per-worker scan response frame. Absent on the terminal
/// frame sent by the master after all worker frames. A client accumulates
/// frames into one reply until it sees one without this bit.
pub const FLAG_CONTINUATION: u64 = 1 << 52;

/// RESOLVE request flag. Answers "what is the shape of relation X?" in one
/// master-local round trip: the schema block plus a `RelDescriptorBlob` (kind,
/// placement, foreign keys, secondary indexes). Addressed by qualified name
/// (`target_id = 0`, the name in the control block's BLOB cell) or by id (empty
/// blob, `target_id` = the relation).
pub const FLAG_RESOLVE: u64 = 1 << 54;

/// ALLOCATE_SERIAL_RANGE request flag. The client→master leg of a user-table
/// SERIAL sequence range reservation. The request carries `target_id = table_id`
/// (the sequence key) and the range `count` in `seek_col_idx`.
pub const FLAG_ALLOCATE_SERIAL_RANGE: u64 = 1 << 56;

/// DDL_TXN request flag. The client→master frame carrying an atomic bundle of
/// system-table family batches for one catalog write (CREATE/DROP of
/// table/view/index/schema, CREATE SCHEMA/INDEX). Purely a wire-level decode
/// hint: it is consumed at `handle_message` routing and is NEVER written to the
/// SAL — each family is broadcast as its own `DdlSync` group and the zone's
/// commit sentinel is unrelated engine state.
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
/// written to the SAL — each family is emitted as its own `Push` group inside
/// one zone closed by the engine-internal commit sentinel.
pub const FLAG_PUSH_TXN: u64 = 1 << 61;

/// SCAN_MULTI request flag. Client→master frame naming N relations to snapshot
/// at one SAL cut. Wire-level routing hint consumed at `handle_message`; never
/// written to the SAL.
pub const FLAG_SCAN_MULTI: u64 = 1 << 62;

/// FIFO-reply directive on a master→worker scan group. Set by
/// `dispatch_scan_multi_fanout` on every group it writes as one of several, so
/// the worker routes that reply through `pending_streams` (strict FIFO ring
/// order) instead of the immediate-emit fast path — those drains take one train
/// at a time and need ring order to equal request order.
///
/// Sits with the other engine-internal booleans, `FLAG_SCAN_LAST` and
/// `FLAG_RESOLVE`, and is covered by the disjointness guard below like every
/// other flag.
pub const FLAG_SCAN_FIFO_REPLY: u64 = 1 << 55;

/// Engine-internal batch-layout claim stamped on SAL / W2M frames; never sent to
/// clients. Defined here so the disjointness guard below covers it against every
/// wire flag.
pub const FLAG_BATCH_CONSOLIDATED: u64 = 1 << 51;
/// Engine-internal W2M flag set on the last (or only) scan chunk from a
/// worker. Frames are forwarded verbatim, so the bit does reach clients; no
/// client reads it. See the engine's reply path for why FLAG_CONTINUATION
/// cannot carry this meaning.
pub const FLAG_SCAN_LAST: u64 = 1 << 53;

// ---------------------------------------------------------------------------
// Wire-level packed fields: bits 16-41 of wire_flags
// ---------------------------------------------------------------------------

/// Bits 16-23: conflict mode (8 bits). Value 0 = Update (default).
const WIRE_CONFLICT_MODE_SHIFT: u32 = 16;
const WIRE_CONFLICT_MODE_MASK: u64 = 0xFF_u64 << WIRE_CONFLICT_MODE_SHIFT;
/// Bits 24-39: schema version (16 bits). Value 0 = client has no cached schema.
const WIRE_SCHEMA_VERSION_SHIFT: u32 = 24;
const WIRE_SCHEMA_VERSION_MASK: u64 = 0xFFFF_u64 << WIRE_SCHEMA_VERSION_SHIFT;
/// Bits 40-41: probe mode (2 bits) on a `HasPk` group. Value 0 = Exists.
const WIRE_PROBE_MODE_SHIFT: u32 = 40;
const WIRE_PROBE_MODE_MASK: u64 = 0x3_u64 << WIRE_PROBE_MODE_SHIFT;

// Compile-time guard over the whole `u64`: no two flags may share a bit, and no
// flag may land in the wire-level packed fields. One sweep over every flag
// declared here, seeded with every packed-field mask.
const _: () = {
    assert!(WIRE_CONFLICT_MODE_MASK & WIRE_SCHEMA_VERSION_MASK == 0);
    assert!(WIRE_PROBE_MODE_MASK & (WIRE_CONFLICT_MODE_MASK | WIRE_SCHEMA_VERSION_MASK) == 0);
    let flags = [
        FLAG_EXCHANGE,
        FLAG_PUSH,
        FLAG_SEEK,
        FLAG_SEEK_BY_INDEX,
        FLAG_SCAN_SPEC,
        FLAG_DELTA_POLL,
        FLAG_HAS_SCHEMA,
        FLAG_HAS_DATA,
        FLAG_BATCH_CONSOLIDATED,
        FLAG_CONTINUATION,
        FLAG_SCAN_LAST,
        FLAG_RESOLVE,
        FLAG_SCAN_FIFO_REPLY,
        FLAG_ALLOCATE_SERIAL_RANGE,
        FLAG_DDL_TXN,
        FLAG_ALLOCATE_TABLE_ID,
        FLAG_ALLOCATE_SCHEMA_ID,
        FLAG_ALLOCATE_INDEX_ID,
        FLAG_PUSH_TXN,
        FLAG_SCAN_MULTI,
    ];

    let mut acc = WIRE_CONFLICT_MODE_MASK | WIRE_SCHEMA_VERSION_MASK | WIRE_PROBE_MODE_MASK;
    let mut i = 0;
    while i < flags.len() {
        assert!(flags[i] & acc == 0, "wire flag bit collision");
        acc |= flags[i];
        i += 1;
    }
};

// ---------------------------------------------------------------------------
// Client request verbs
// ---------------------------------------------------------------------------

wire_enum! {
    /// The one verb a client request frame names. A plain SCAN sets no verb bit,
    /// so absence *is* a verb: [`ClientVerb::Scan`] encodes as `0`.
    pub enum ClientVerb: u64 {
        /// No verb bit set: stream a whole relation.
        Scan = 0,
        Push = FLAG_PUSH,
        Seek = FLAG_SEEK,
        SeekByIndex = FLAG_SEEK_BY_INDEX,
        ScanSpec = FLAG_SCAN_SPEC,
        DeltaPoll = FLAG_DELTA_POLL,
        Resolve = FLAG_RESOLVE,
        DdlTxn = FLAG_DDL_TXN,
        PushTxn = FLAG_PUSH_TXN,
        ScanMulti = FLAG_SCAN_MULTI,
        AllocSerialRange = FLAG_ALLOCATE_SERIAL_RANGE,
        AllocTableId = FLAG_ALLOCATE_TABLE_ID,
        AllocSchemaId = FLAG_ALLOCATE_SCHEMA_ID,
        AllocIndexId = FLAG_ALLOCATE_INDEX_ID,
    }
}

impl ClientVerb {
    /// The verb `flags` names, or an error for a frame no client can legitimately
    /// have encoded: two verb bits (the wire spells a choice as independent bits,
    /// so branch order must not get to pick), or a data block on any verb but
    /// PUSH — only a push carries rows, so a data-carrying frame naming no verb
    /// would otherwise resolve to `Scan` and be answered with a table dump.
    pub fn from_flags(flags: u64) -> Result<Self, &'static str> {
        let named = flags & CLIENT_VERB_MASK;
        if named.count_ones() > 1 {
            return Err("frame names more than one request verb");
        }
        // `named` is zero (Scan) or exactly one bit of `CLIENT_VERB_MASK`, which
        // is the union of exactly the verbs' own bits, so the lookup is total.
        // Refusing anyway keeps the trust boundary free of a panic path.
        let Some(verb) = Self::from_wire(named) else {
            return Err("frame names no request verb");
        };
        if flags & FLAG_HAS_DATA != 0 && verb != ClientVerb::Push {
            return Err("frame carries a data block on a verb other than PUSH");
        }
        Ok(verb)
    }
}

/// The union of every verb bit — folded from [`ClientVerb::ALL`] rather than
/// written out, so it cannot fall behind the enum.
const CLIENT_VERB_MASK: u64 = {
    let mut acc = 0u64;
    let mut i = 0;
    while i < ClientVerb::ALL.len() {
        acc |= ClientVerb::ALL[i].as_wire();
        i += 1;
    }
    acc
};

// Every verb names at most one bit, no two verbs share one, and no verb lands in
// the wire-level packed fields. The bits themselves are already swept for
// collision by the guard above; this covers the *derived* mask, which that guard
// cannot see.
const _: () = {
    let packed = WIRE_CONFLICT_MODE_MASK | WIRE_SCHEMA_VERSION_MASK | WIRE_PROBE_MODE_MASK;
    assert!(CLIENT_VERB_MASK & packed == 0, "a verb bit lands in a packed field");

    let mut acc = 0u64;
    let mut i = 0;
    while i < ClientVerb::ALL.len() {
        let bit = ClientVerb::ALL[i].as_wire();
        assert!(bit.count_ones() <= 1, "a verb names more than one bit");
        assert!(bit & acc == 0, "two verbs share a bit");
        acc |= bit;
        i += 1;
    }
};

#[inline]
pub fn wire_flags_set_conflict_mode(flags: u64, mode: WireConflictMode) -> u64 {
    (flags & !WIRE_CONFLICT_MODE_MASK) | ((mode as u64) << WIRE_CONFLICT_MODE_SHIFT)
}
/// The conflict mode packed into `flags`, or `None` when those bits name no
/// mode — the caller is a trust boundary and must reject rather than default.
#[inline]
pub fn wire_flags_get_conflict_mode(flags: u64) -> Option<WireConflictMode> {
    WireConflictMode::from_wire(((flags & WIRE_CONFLICT_MODE_MASK) >> WIRE_CONFLICT_MODE_SHIFT) as u8)
}
#[inline]
pub fn wire_flags_set_probe_mode(flags: u64, mode: WireProbeMode) -> u64 {
    (flags & !WIRE_PROBE_MODE_MASK) | ((mode as u64) << WIRE_PROBE_MODE_SHIFT)
}
/// The probe mode packed into `flags`, or `None` when those bits name no mode.
/// The worker is a trust boundary and must reject rather than default, exactly
/// as it does for the conflict mode.
#[inline]
pub fn wire_flags_get_probe_mode(flags: u64) -> Option<WireProbeMode> {
    WireProbeMode::from_wire(((flags & WIRE_PROBE_MODE_MASK) >> WIRE_PROBE_MODE_SHIFT) as u8)
}
#[inline]
pub fn wire_flags_set_schema_version(flags: u64, version: u16) -> u64 {
    (flags & !WIRE_SCHEMA_VERSION_MASK) | ((version as u64) << WIRE_SCHEMA_VERSION_SHIFT)
}
#[inline]
pub fn wire_flags_get_schema_version(flags: u64) -> u16 {
    ((flags & WIRE_SCHEMA_VERSION_MASK) >> WIRE_SCHEMA_VERSION_SHIFT) as u16
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

wire_enum! {
    /// Conflict-resolution mode packed into bits 16-23 of `wire_flags` on
    /// FLAG_PUSH messages. Discriminant 0 = Update (default), so zero-filled
    /// flags resolve to the upsert default without explicit encoding.
    pub enum WireConflictMode: u8 {
        /// Retract-and-insert on PK conflict. Used for SQL `UPDATE`,
        /// `INSERT ... ON CONFLICT ... DO UPDATE` (after client-side
        /// merging), and explicit Python `push(mode="update")`.
        Update = 0,
        /// Reject the batch on any PK conflict. The master runs both an
        /// intra-batch duplicate check and an against-store PK existence
        /// check, and returns a PG-style `duplicate key value violates
        /// unique constraint` error.
        Error = 1,
    }
}

wire_enum! {
    /// What a `HasPk` probe answers each matched key with, packed into bits
    /// 40-41 of `wire_flags`. Discriminant 0 = `Exists`, so a zero-filled flags
    /// word is the plain existence probe. It rides `wire_flags` so
    /// `seek_col_idx` stays one thing: the keyspace to probe.
    pub enum WireProbeMode: u8 {
        /// Echo the probe key back; the caller asked only whether it is
        /// occupied.
        Exists = 0,
        /// Answer with the matched STORED index entry `[span ‖ holder PK]`, so
        /// the caller learns which committed row holds the span. Index only.
        FirstHolder = 1,
        /// [`Self::FirstHolder`] for EVERY committed holder of the span, capped
        /// per value at the count in `seek_pk`. Index only.
        AllHolders = 2,
        /// Answer each matched key with that key plus ONE of the stored row's
        /// columns, named by `seek_pk`. PK store only.
        Project = 3,
    }
}

impl std::str::FromStr for WireConflictMode {
    type Err = String;

    /// The two modes' user-facing names, for the bindings that let a caller
    /// choose one (Python's `push(mode=...)`; the async clients always push
    /// `Update`). Owned here, so no binding invents a third spelling.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "update" => Ok(WireConflictMode::Update),
            "error" => Ok(WireConflictMode::Error),
            other => Err(format!("invalid conflict mode '{other}', expected 'update' or 'error'")),
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
/// A `Delta { after_tick }` read whose cursor is at or below the refusing
/// worker's retention floor: the rounds it asks for were dropped by that store's
/// capacity sweep. Control-only frame; the subscriber's recovery is the read it
/// made on its first day, `after_tick = 0`. The one status a *worker* mints —
/// `worker_error` carries the code to the client rather than flattening it into
/// a string.
pub const STATUS_DELTA_EXPIRED: u32 = 5;
/// The SAL had no room for the group this request needed to write. Transient by
/// construction: an ordinary group must leave the sentinel headroom and the
/// checkpoint band untouched, so it is refused while the log is near full, and
/// the watchdog's reclaim frees the whole mapping within one 100 ms tick. A
/// refused read or push is therefore retryable, unlike every other server error
/// — which is why it is a code rather than a phrase in a message.
pub const STATUS_SAL_FULL: u32 = 6;
/// The relation a request names does not exist — minted where the catalog probe
/// comes back empty and nowhere else, so it says *gone* rather than *refused*.
/// A code for the same reason [`STATUS_NO_INDEX`] is one: a subscriber whose
/// view vanished takes a recovery no other refusal calls for, and prose is all
/// it could otherwise branch on.
pub const STATUS_NOT_FOUND: u32 = 7;

/// A failure as the reply frame carries it: one of the `STATUS_*` words above
/// plus its message. The decoded form of a control block's `(status, error_msg)`
/// pair, defined here because both halves of that hop need to name it — the
/// engine's `scan_spec_family` mints one, the worker splits it onto the wire,
/// and the master's `worker_error` puts it back together.
///
/// `From<S: Into<String>>` defaults the status to [`STATUS_ERROR`], which is what
/// every rejection that is not a typed refusal is, and what keeps a plain
/// `Err(format!(…))?` compiling unchanged. There is deliberately no conversion
/// *back* to `String`: a caller that has no typed status to forward discards the
/// code explicitly, so the sites where a status is dropped stay visible.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireFault {
    pub status: u32,
    pub text: String,
}

impl<S: Into<String>> From<S> for WireFault {
    fn from(text: S) -> Self {
        WireFault { status: STATUS_ERROR, text: text.into() }
    }
}

impl std::fmt::Display for WireFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

pub const META_FLAG_NULLABLE: u64 = 1;
pub(crate) const META_FLAG_IS_PK: u64 = 2;
/// The column is a hidden key slot: a physical schema column (it holds a real
/// PK/routing value) that no presentation surface exposes — excluded from
/// wildcard expansion, name resolution, duplicate-name checks, and client rows.
/// Purely a presentation marker: the PK region, routing, sort, and
/// consolidation are all blind to it. Bit 2 (value 4), between `META_FLAG_IS_PK`
/// (bit 1) and the PK-pos byte at bits 8..16.
pub const META_FLAG_HIDDEN: u64 = 4;
/// The column's values are assigned from a server-side sequence (SQL `SERIAL`).
/// Like [`META_FLAG_HIDDEN`] this is a per-column catalog fact the block's
/// *decoders* — the engine's `decode_schema_block` and the client's
/// `batch_to_schema` — need but the storage layer does not: the PK region,
/// routing, sort, and consolidation are all blind to it. Bit 3 (value 8),
/// between `META_FLAG_HIDDEN` (bit 2) and the PK-pos byte at bits 8..16.
pub const META_FLAG_SERIAL: u64 = 8;

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
/// the same rule [`crate::TableProps::pack`] follows for `TABLE_TAB.flags`.
#[inline]
pub fn pack_col_meta_flags(nullable: bool, hidden: bool, serial: bool, pk_pos: Option<u8>) -> u64 {
    let pk = match pk_pos {
        Some(p) => META_FLAG_IS_PK | ((p as u64) << META_FLAG_PK_POS_SHIFT),
        None => 0,
    };
    pk | if nullable { META_FLAG_NULLABLE } else { 0 }
        | if hidden { META_FLAG_HIDDEN } else { 0 }
        | if serial { META_FLAG_SERIAL } else { 0 }
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

/// Decode the `serial` bit (see [`META_FLAG_SERIAL`]).
#[inline]
pub fn col_meta_serial(flags: u64) -> bool {
    flags & META_FLAG_SERIAL != 0
}

/// The column's 0-indexed position within the PK tuple, or `None` when it is
/// not a PK column. Decoders sort their PK columns by this to rebuild the
/// declared PK order.
#[inline]
pub(crate) fn col_meta_pk_pos(flags: u64) -> Option<u8> {
    (flags & META_FLAG_IS_PK != 0).then_some(((flags & META_FLAG_PK_POS_MASK) >> META_FLAG_PK_POS_SHIFT) as u8)
}

#[cfg(test)]
#[path = "tests/flags.rs"]
mod tests;
