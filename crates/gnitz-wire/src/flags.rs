//! The control block's `flags` word, the request verbs and modes it carries, and
//! the reply status codes.

// ---------------------------------------------------------------------------
// Client request verbs
// ---------------------------------------------------------------------------

wire_enum! {
    /// The verb a client request frame names.
    #[derive(Default)]
    pub enum ClientVerb: u8 {
        /// Stream a whole relation.
        #[default]
        Scan = 0,
        /// A data push. An empty batch is still a push, ACKed at LSN 0.
        Push = 1,
        /// Every row of one PK group, the key in `seek_pk` / `seek_pk_extra`.
        Seek = 2,
        /// A parameterized bounded read (`ReadSpec`).
        ScanSpec = 3,
        /// A delta read of N views: one frame naming N fed views, each with its
        /// own cursor and client-authored reply schema.
        DeltaPoll = 4,
        /// A relation's schema block and `RelDescriptorBlob`, named by the qualified
        /// name in the BLOB cell (`target_id = 0`) or by `target_id`.
        Resolve = 5,
        /// System-table batches committed as one SAL zone.
        DdlTxn = 6,
        /// User-table batches and their OCC preconditions, committed as one SAL zone.
        PushTxn = 7,
        /// N relations read at one SAL cut.
        ScanMulti = 8,
        /// `seek_col_idx` SERIAL ids of table `target_id`.
        AllocSerialRange = 9,
        AllocTableId = 10,
        AllocSchemaId = 11,
        AllocIndexId = 12,
    }
}

// ---------------------------------------------------------------------------
// The flags word
// ---------------------------------------------------------------------------

/// The control block's `flags` word. `pack`/`unpack` are the only code that knows
/// the bit layout, so no two fields can overlap and no consumer re-derives one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WireFlags {
    /// Bits 0-7.
    pub verb: ClientVerb,
    /// Bits 8-15: a push's conflict mode.
    pub conflict_mode: WireConflictMode,
    /// Bits 16-31: the relation schema version the frame speaks, 0 = none.
    pub schema_version: u16,
    /// Bit 32.
    pub has_schema: bool,
    /// Bit 33.
    pub has_data: bool,
    /// Bit 34: more frames of this reply follow. Set on every per-worker reply
    /// frame, absent on the master's terminal frame.
    pub continuation: bool,
    // Engine-authored.
    /// Bit 35: the frame's batch claims the consolidated layout.
    pub batch_consolidated: bool,
    /// Bit 36: a worker's last frame of a train.
    pub scan_last: bool,
    /// Bit 37: queue the reply behind the worker's earlier trains, so ring order is
    /// request order.
    pub scan_fifo_reply: bool,
    /// Bits 38-39: what a `HasPk` probe answers a matched key with.
    pub probe_mode: WireProbeMode,
}

const RESERVED_BITS: u64 = !crate::low_bits_mask(40);

impl WireFlags {
    /// A frame of a worker train: `continuation` always, since the master's terminal
    /// frame ends the client's reply; `scan_last` on the train's last frame.
    pub const fn train_frame(schema_version: u16, last: bool) -> Self {
        WireFlags {
            verb: ClientVerb::Scan,
            conflict_mode: WireConflictMode::Update,
            schema_version,
            has_schema: false,
            has_data: false,
            continuation: true,
            batch_consolidated: false,
            scan_last: last,
            scan_fifo_reply: false,
            probe_mode: WireProbeMode::Exists,
        }
    }

    pub const fn pack(self) -> u64 {
        self.verb as u64
            | (self.conflict_mode as u64) << 8
            | (self.schema_version as u64) << 16
            | (self.has_schema as u64) << 32
            | (self.has_data as u64) << 33
            | (self.continuation as u64) << 34
            | (self.batch_consolidated as u64) << 35
            | (self.scan_last as u64) << 36
            | (self.scan_fifo_reply as u64) << 37
            | (self.probe_mode as u64) << 38
    }

    /// Rejects a word naming a verb or mode this build does not define, or setting a
    /// bit outside the layout.
    pub fn unpack(w: u64) -> Result<Self, &'static str> {
        if w & RESERVED_BITS != 0 {
            return Err("flags: reserved bits set");
        }
        let bit = |b: u32| (w >> b) & 1 != 0;
        Ok(WireFlags {
            verb: ClientVerb::from_wire(w as u8).ok_or("flags: unknown request verb")?,
            conflict_mode: WireConflictMode::from_wire((w >> 8) as u8).ok_or("flags: unknown conflict mode")?,
            schema_version: (w >> 16) as u16,
            has_schema: bit(32),
            has_data: bit(33),
            continuation: bit(34),
            batch_consolidated: bit(35),
            scan_last: bit(36),
            scan_fifo_reply: bit(37),
            probe_mode: WireProbeMode::from_wire(((w >> 38) & 3) as u8).ok_or("flags: unknown probe mode")?,
        })
    }

    /// The verb a client frame names. Only a push carries rows, so a data block on
    /// any other verb — `Scan` included, which would answer it with a table dump — is
    /// malformed.
    pub fn client_verb(self) -> Result<ClientVerb, &'static str> {
        if self.has_data && self.verb != ClientVerb::Push {
            return Err("frame carries a data block on a verb other than PUSH");
        }
        Ok(self.verb)
    }
}

// ---------------------------------------------------------------------------
// Wire-level conflict mode for INSERT / UPSERT semantics
// ---------------------------------------------------------------------------

wire_enum! {
    /// Conflict-resolution mode a PUSH carries in [`WireFlags::conflict_mode`].
    #[derive(Default)]
    pub enum WireConflictMode: u8 {
        /// Retract-and-insert on PK conflict. Used for SQL `UPDATE`,
        /// `INSERT ... ON CONFLICT ... DO UPDATE` (after client-side
        /// merging), and explicit Python `push(mode="update")`.
        #[default]
        Update = 0,
        /// Reject the batch on any PK conflict. The master runs both an
        /// intra-batch duplicate check and an against-store PK existence
        /// check, and returns a PG-style `duplicate key value violates
        /// unique constraint` error.
        Error = 1,
    }
}

wire_enum! {
    /// What a `HasPk` probe answers each matched key with, carried in
    /// [`WireFlags::probe_mode`] so `seek_col_idx` stays one thing: the keyspace
    /// to probe.
    #[derive(Default)]
    pub enum WireProbeMode: u8 {
        /// Echo the probe key back; the caller asked only whether it is
        /// occupied.
        #[default]
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

// ---------------------------------------------------------------------------
// Reply status
// ---------------------------------------------------------------------------

wire_enum! {
    /// A control block's `status` word.
    #[derive(Default)]
    pub enum WireStatus: u32 {
        #[default]
        Ok = 0,
        Error = 1,
        /// A warm push's schema version is stale: evict the cached schema, push cold.
        SchemaMismatch = 2,
        /// An OCC precondition failed; `seek_pk` carries the fresh basis. Nothing
        /// was written, so it is retryable.
        TxnConflict = 3,
        /// A delta cursor below a worker's retention floor: re-read at `after_tick = 0`.
        DeltaExpired = 4,
        /// The SAL had no room for this request's group. A reclaim frees it within
        /// a tick, so it is retryable.
        SalFull = 5,
        /// The named relation does not exist.
        NotFound = 6,
    }
}

/// A control block's `(status, error_msg)` pair. A plain message converts in as
/// [`WireStatus::Error`]; nothing converts back, so dropping a status is always
/// a visible `.text`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireFault {
    pub status: WireStatus,
    pub text: String,
}

impl<S: Into<String>> From<S> for WireFault {
    fn from(text: S) -> Self {
        WireFault {
            status: WireStatus::Error,
            text: text.into(),
        }
    }
}

impl std::fmt::Display for WireFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

#[cfg(test)]
#[path = "tests/flags.rs"]
mod tests;
