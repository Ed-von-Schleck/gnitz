//! The control header's `flags` word, the request verbs and modes it carries, and
//! the reply status codes.

// ---------------------------------------------------------------------------
// Client request verbs
// ---------------------------------------------------------------------------

wire_enum! {
    /// The verb a client request frame names.
    #[derive(Default)]
    pub enum ClientVerb: u8 {
        /// A parameterized bounded read (`ReadSpec`) of one relation, replied in
        /// the layout whose digest is `arg0`.
        #[default]
        ScanSpec = 0,
        /// A data push. An empty batch is still a push, ACKed at LSN 0.
        Push = 1,
        /// A delta read of N views: one frame naming N fed views, each with its
        /// own cursor and client-authored reply schema.
        DeltaPoll = 4,
        /// A relation's schema block and `RelDescriptorBlob`, named by the qualified
        /// name in the blob (`target_id = 0`) or by `target_id`.
        Resolve = 5,
        /// System-table batches committed as one SAL zone.
        DdlTxn = 6,
        /// User-table batches committed as one SAL zone; each family carries
        /// its own OCC basis.
        PushTxn = 7,
        /// N relations read at one SAL cut.
        ScanMulti = 8,
        /// `arg1` SERIAL ids of table `target_id`; the reply's `target_id` is the
        /// run's base.
        AllocSerialRange = 9,
        /// `arg1` catalog object ids (schema, relation or index); the reply's
        /// `target_id` is the run's base.
        AllocIds = 10,
    }
}

// ---------------------------------------------------------------------------
// The flags word
// ---------------------------------------------------------------------------

/// The control header's `flags` word. `pack`/`unpack` are the only code that knows
/// the bit layout, so no two fields can overlap and no consumer re-derives one.
///
/// Bits 32/33 — which blocks follow the header — belong to the control codec, so
/// `pack` leaves them clear and `unpack` ignores them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WireFlags {
    /// Bits 0-7.
    pub verb: ClientVerb,
    /// Bits 8-15: a push's conflict mode.
    pub conflict_mode: WireConflictMode,
    /// Bit 34: more frames of this reply follow. Set on every per-worker reply
    /// frame, absent on the master's terminal frame.
    pub continuation: bool,
    // Engine-authored.
    /// Bit 35: a worker's last frame of a train.
    pub scan_last: bool,
    /// Bits 36-37: what a `HasPk` probe answers a matched key with.
    pub probe_mode: WireProbeMode,
}

pub(crate) const FLAG_HAS_SCHEMA: u64 = 1 << 32;
pub(crate) const FLAG_HAS_DATA: u64 = 1 << 33;

/// Bits 16-31 and 38-63: no field.
const RESERVED_BITS: u64 = !crate::low_bits_mask(38) | (crate::low_bits_mask(32) & !crate::low_bits_mask(16));

impl WireFlags {
    /// A frame of a worker's reply train: `continuation` always, since the master's terminal
    /// frame ends the client's reply; `scan_last` on the train's last frame.
    pub const fn train_frame(last: bool) -> Self {
        WireFlags {
            verb: ClientVerb::ScanSpec,
            conflict_mode: WireConflictMode::Update,
            continuation: true,
            scan_last: last,
            probe_mode: WireProbeMode::Exists,
        }
    }

    pub const fn pack(self) -> u64 {
        self.verb as u64
            | (self.conflict_mode as u64) << 8
            | (self.continuation as u64) << 34
            | (self.scan_last as u64) << 35
            | (self.probe_mode as u64) << 36
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
            continuation: bit(34),
            scan_last: bit(35),
            probe_mode: WireProbeMode::from_wire(((w >> 36) & 3) as u8).ok_or("flags: unknown probe mode")?,
        })
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
    /// The code a [`Probe`](crate::Probe) rides [`WireFlags::probe_mode`] as.
    #[derive(Default)]
    pub enum WireProbeMode: u8 {
        #[default]
        Exists = 0,
        FirstHolder = 1,
        AllHolders = 2,
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
    /// A control header's `status` word. Under any status but `Ok`, the frame's
    /// blob is the error text.
    #[derive(Default)]
    pub enum WireStatus: u32 {
        #[default]
        Ok = 0,
        Error = 1,
        /// An OCC precondition failed. Nothing was written, so it is retryable.
        TxnConflict = 3,
        /// A delta cursor below a worker's retention floor: re-read at `after_tick = 0`.
        DeltaExpired = 4,
        /// The SAL had no room for this request's group. A reclaim frees it within
        /// a tick, so it is retryable.
        SalFull = 5,
        /// The named relation does not exist.
        NotFound = 6,
        /// The write would break a PK, unique-index or foreign-key constraint.
        /// Nothing was written.
        IntegrityViolation = 7,
    }
}

/// A refusal's class and message: what a non-`Ok` control header carries, and
/// what a client-side check raises in the same terms. A plain message converts in
/// as [`WireStatus::Error`].
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
