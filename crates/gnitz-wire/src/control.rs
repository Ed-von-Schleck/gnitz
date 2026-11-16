//! The frame control header: a fixed-width, little-endian header followed by the
//! frame's blob and its optional data block. Both
//! `gnitz-server` and `gnitz-core` build and parse it through this one codec.
//!
//! The header carries a reply status ([`WireStatus`]; a refusal is a
//! [`WireFault`]), the relation a request names ([`Target`]), two per-verb
//! arguments, and the `flags` word ([`WireFlags`]): the request verb
//! ([`ClientVerb`]), a push's conflict mode, the lane a server frame answers
//! on, and the continuation bits of a reply train.

use std::ops::Range;

use crate::codec::Reader;
use crate::{write_u32_le, write_u64_le};

// Layout (all little-endian):
//   [0,4)   STATUS     u32   `WireStatus`
//   [4,8)   BLOB_LEN   u32
//   [8,16)  FLAGS      u64   `WireFlags::pack`
//   [16,24) TARGET_ID  u64
//   [24,32) ARG0       u64
//   [32,40) ARG1       u64
//   [40, 40+BLOB_LEN)  blob; under a non-`Ok` STATUS, the UTF-8 error text
//   then a self-sizing data block, present iff FLAGS bit 33 is set
const OFF_STATUS: usize = 0;
const OFF_BLOB_LEN: usize = 4;
const OFF_FLAGS: usize = 8;
const OFF_TARGET_ID: usize = 16;
const OFF_ARG0: usize = 24;
const OFF_ARG1: usize = 32;

/// The FLAGS bit the codec owns: a data block follows the blob.
const FLAG_HAS_DATA: u64 = 1 << 33;

pub const CTRL_HEADER_SIZE: usize = 40;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ControlHeader {
    pub status: WireStatus,
    pub target_id: u64,
    pub flags: WireFlags,
    /// Per-verb arguments; each `ClientVerb` and `WireStatus` variant documents
    /// what it reads here.
    pub arg0: u64,
    pub arg1: u64,
}

/// The relation a request names: its id, and the token of the RESOLVE answer
/// the request was built from — `0` for a request built from none, which is what
/// a bare id converts to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Target {
    pub tid: u64,
    pub token: u64,
}

impl From<u64> for Target {
    fn from(tid: u64) -> Self {
        Target { tid, token: 0 }
    }
}

impl From<crate::SysFamily> for Target {
    fn from(family: crate::SysFamily) -> Self {
        Target::from(family.id())
    }
}

impl ControlHeader {
    /// A `verb` request naming `target`, whose token rides `arg1`.
    pub fn naming(verb: ClientVerb, target: Target, arg0: u64) -> Self {
        ControlHeader {
            flags: WireFlags { verb: Some(verb), ..Default::default() },
            target_id: target.tid,
            arg0,
            arg1: target.token,
            ..Default::default()
        }
    }

    /// The relation a [`Self::naming`] header names.
    pub fn target(&self) -> Target {
        Target { tid: self.target_id, token: self.arg1 }
    }

    /// Refuses a header no client writes: a status other than `Ok`, or a flag
    /// only the engine sets.
    pub(crate) fn check_client_authored(&self) -> Result<(), &'static str> {
        let WireFlags { continuation, scan_last, lane, .. } = self.flags;
        if self.status != WireStatus::Ok {
            return Err("a request carries a status");
        }
        if continuation || scan_last || lane != WireLane::Reply {
            return Err("a request sets a flag only a reply carries");
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct DecodedControl {
    pub hdr: ControlHeader,
    /// Where the frame's blob sits in the peeked buffer.
    pub blob: Range<usize>,
    /// The bytes after the last declared section — blob, data block — to the
    /// end of the frame: a multi-item frame's items.
    pub body: Range<usize>,
    /// Where the frame's data block sits in the peeked buffer.
    pub data: Option<Range<usize>>,
}

impl DecodedControl {
    /// The control of a frame peeked at `at` in a larger buffer, its ranges
    /// moved onto that buffer.
    pub(crate) fn placed_at(self, at: usize) -> Self {
        let shift = |r: Range<usize>| r.start + at..r.end + at;
        DecodedControl {
            hdr: self.hdr,
            blob: shift(self.blob),
            body: shift(self.body),
            data: self.data.map(shift),
        }
    }

    /// The verb a client frame names, refusing a header no client writes and
    /// a section or prologue word that verb does not carry. A multi-item
    /// frame's items are checked as they are decoded.
    pub fn client_verb(&self) -> Result<ClientVerb, &'static str> {
        let hdr = &self.hdr;
        let verb = hdr.flags.verb.ok_or("a request names no verb")?;
        hdr.check_client_authored()?;
        let shape = VerbShape::of(verb);
        match shape.items {
            None => {
                shape.sections.check(self)?;
                if !self.body.is_empty() {
                    return Err("frame carries bytes past its last section");
                }
            }
            Some(items) => {
                if !self.blob.is_empty()
                    || self.data.is_some()
                    || hdr.target_id != 0
                    || hdr.flags.conflict_mode != WireConflictMode::default()
                {
                    return Err("a multi-item frame carries nothing but its items");
                }
                if hdr.arg1 != 0 || hdr.arg0 != 0 && !items.reads_arg0 {
                    return Err("a multi-item prologue sets an argument its verb does not read");
                }
            }
        }
        Ok(verb)
    }

    /// The error a non-`Ok` frame carries: its status and its blob, read out of
    /// the peeked `frame`, as text.
    pub fn fault(&self, frame: &[u8]) -> Option<WireFault> {
        (self.hdr.status != WireStatus::Ok).then(|| WireFault {
            status: self.hdr.status,
            text: String::from_utf8_lossy(&frame[self.blob.clone()]).into_owned(),
        })
    }
}

/// Bytes of one frame with these sections.
pub(crate) fn frame_size(blob: &[u8], data: Option<&[&[u8]]>) -> usize {
    CTRL_HEADER_SIZE + blob.len() + data.map_or(0, crate::wal::block_size)
}

/// Append one frame — header, blob, optional data block over a canonical region
/// list — to `out`.
pub fn append_frame(out: &mut Vec<u8>, hdr: &ControlHeader, blob: &[u8], data: Option<&[&[u8]]>) {
    out.reserve(frame_size(blob, data));
    let at = out.len();
    out.resize(at + CTRL_HEADER_SIZE + blob.len(), 0);
    encode_frame_head(&mut out[at..], hdr, blob, data.is_some());
    if let Some(regions) = data {
        // A client sends 0 as its dead-heap bound whatever its arena holds; every reader of a
        // client block recomputes it.
        crate::wal::append_block(regions, 0, out);
    }
}

/// Write the control header and `blob` into the front of `out`, returning the
/// offset a data block follows at.
#[inline]
pub fn encode_frame_head(out: &mut [u8], hdr: &ControlHeader, blob: &[u8], has_data: bool) -> usize {
    let (head, tail) = out
        .split_first_chunk_mut::<CTRL_HEADER_SIZE>()
        .expect("the caller sized `out` for the header and blob");
    debug_assert!(
        hdr.status == WireStatus::Ok || !blob.is_empty(),
        "a fault frame names its cause"
    );
    let flags = hdr.flags.pack() | if has_data { FLAG_HAS_DATA } else { 0 };
    write_u32_le(head, OFF_STATUS, hdr.status.as_wire());
    write_u32_le(head, OFF_BLOB_LEN, blob.len() as u32);
    write_u64_le(head, OFF_FLAGS, flags);
    write_u64_le(head, OFF_TARGET_ID, hdr.target_id);
    write_u64_le(head, OFF_ARG0, hdr.arg0);
    write_u64_le(head, OFF_ARG1, hdr.arg1);
    tail[..blob.len()].copy_from_slice(blob);
    CTRL_HEADER_SIZE + blob.len()
}

/// Decode the control header at the front of the whole frame `data`, and locate
/// the blocks behind it.
pub fn peek_control_block(data: &[u8]) -> Result<DecodedControl, String> {
    if data.len() < CTRL_HEADER_SIZE {
        return Err("control header truncated".into());
    }
    let mut r = Reader::new(data);
    // The header's words in layout order, each read whole: none can run short.
    let [status, blob_len] = [(); 2].map(|()| r.u32().expect("a whole header"));
    let [word, target_id, arg0, arg1] = [(); 4].map(|()| r.u64().expect("a whole header"));
    debug_assert_eq!(r.pos(), CTRL_HEADER_SIZE);
    r.take(blob_len as usize)
        .map_err(|_| "control blob runs past the frame")?;
    let blob = CTRL_HEADER_SIZE..r.pos();
    #[cfg(feature = "field-map")]
    crate::fields::note_section(&data[OFF_BLOB_LEN..OFF_FLAGS], &data[blob.clone()]);
    let status = WireStatus::from_wire(status).ok_or("control header names no status")?;
    let flags = WireFlags::unpack(word)?;
    let data_block = if word & FLAG_HAS_DATA != 0 {
        let at = r.pos();
        r.take(crate::wal::block_slice(&data[at..])?.len())?;
        Some(at..r.pos())
    } else {
        None
    };
    Ok(DecodedControl {
        hdr: ControlHeader { status, flags, target_id, arg0, arg1 },
        blob,
        body: r.pos()..data.len(),
        data: data_block,
    })
}

// ---------------------------------------------------------------------------
// Client request verbs
// ---------------------------------------------------------------------------

wire_enum! {
    /// The verb a client request frame names.
    pub enum ClientVerb: u8 {
        /// A data push under the descriptor token `arg1`, ACKed with its LSN in
        /// `arg0`. Its blob is the schema record of its rows. An empty batch is
        /// still a push, ACKed at LSN 0.
        Push = 1,
        /// A delta read of N views: one frame naming N fed views, each with its
        /// own cursor and client-authored reply schema. Each view's terminal
        /// carries the cursor to read from next: the view's own last round.
        ///
        /// A prologue `arg0` other than `0` keeps item `i`, once answered with
        /// a terminal, as the connection's subscription `arg0 + i` from that
        /// terminal's cursor. The rounds reaching it afterwards are sent as
        /// pushed trains ([`WireLane::PushedTrain`]); a fault ends it.
        DeltaPoll = 4,
        /// A relation's `RelDescriptorBlob`, its schema record included, named by
        /// the qualified name in the blob; the reply's `arg0` is its descriptor
        /// token.
        ///
        /// A descriptor token names one answer to a RESOLVE. A request built
        /// from that answer carries it, and is refused
        /// [`WireStatus::StaleCatalog`] once a RESOLVE would answer otherwise;
        /// `0` is a request built from no RESOLVE, which nothing compares.
        Resolve = 5,
        /// System-table batches committed as one SAL zone.
        DdlTxn = 6,
        /// User-table batches committed as one SAL zone; each family carries
        /// its own OCC basis in `arg0` and its descriptor token in `arg1`.
        PushTxn = 7,
        /// N relations read at one SAL cut, one item each: a parameterized
        /// bounded read (`ReadSpec`) in the blob, replied in the layout whose
        /// digest is the item's `arg0`, under the descriptor token `arg1`.
        ScanMulti = 8,
        /// `arg0` SERIAL ids of table `target_id`, under the descriptor token
        /// `arg1`; the reply's `arg0` is the run's base.
        AllocSerialRange = 9,
        /// `arg0` catalog object ids (schema, relation or index); the reply's
        /// `arg0` is the run's base.
        AllocIds = 10,
        /// Send every subscription the blob names ([`crate::multi_frame::encode_held`])
        /// the pushes acknowledged before this request, and end the others. The
        /// [`WireLane::SyncAnswer`] is held up to `arg0` ms while none was sent a row.
        SyncPushed = 13,
        /// Pin every base table and system family as one point of the log holds
        /// them; the reply's `arg0` names the snapshot. It is the connection's,
        /// and lives until a [`Self::SnapshotClose`] names it or the connection
        /// ends.
        SnapshotOpen = 14,
        /// A page of relation `target_id` as snapshot `arg0` holds it — the rows
        /// and the key the blob names ([`crate::snapshot::PageRequest`]) —
        /// replied as a scan is: one train per worker, or one frame where the
        /// master holds the relation, each in key order and full but for the
        /// last.
        SnapshotPage = 15,
        /// Release snapshot `arg0`.
        SnapshotClose = 16,
        /// Copy the data directory, at a checkpoint, under the backup directory
        /// the server was started with, as the entry the blob names.
        Backup = 17,
    }
}

/// Whether a frame of a verb carries a section. A blob is present when it is
/// not empty.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Section {
    Never,
    May,
    Must,
}

/// What a single-item verb's frame, or each item of a multi-item verb, carries.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Sections {
    pub(crate) blob: Section,
    pub(crate) data: Section,
    /// Whether `flags.conflict_mode` is read: the frame pushes rows to a user
    /// relation.
    pub(crate) conflict_mode: bool,
}

impl Sections {
    /// Refuse a frame carrying a section or a conflict mode its verb never
    /// reads, or lacking a section its verb requires.
    pub(crate) fn check(self, ctrl: &DecodedControl) -> Result<(), &'static str> {
        if !self.conflict_mode && ctrl.hdr.flags.conflict_mode != WireConflictMode::default() {
            return Err("a conflict mode on a request that pushes nothing");
        }
        match (self.blob, !ctrl.blob.is_empty()) {
            (Section::Never, true) => return Err("frame carries a blob its verb does not read"),
            (Section::Must, false) => return Err("frame lacks the blob its verb requires"),
            _ => {}
        }
        match (self.data, ctrl.data.is_some()) {
            (Section::Never, true) => Err("frame carries a data block its verb does not read"),
            (Section::Must, false) => Err("frame lacks the data block its verb requires"),
            _ => Ok(()),
        }
    }
}

/// A multi-item verb's frame: a bare prologue, then its items.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Items {
    /// The most items one frame holds.
    pub(crate) cap: usize,
    /// Whether the prologue's `arg0` is read; every other prologue word is zero.
    pub(crate) reads_arg0: bool,
}

/// What a frame of a verb carries.
#[derive(Clone, Copy, Debug)]
pub(crate) struct VerbShape {
    pub(crate) sections: Sections,
    /// `None` for a single-item verb.
    pub(crate) items: Option<Items>,
}

/// Maximum relations in one `SCAN_MULTI`. The master holds one scan lease and
/// one reply train of bookkeeping per relation; a handful of related tables
/// covers a realistic consistent snapshot.
const SCAN_MULTI_MAX_RELATIONS: usize = 16;

impl VerbShape {
    pub(crate) const fn of(verb: ClientVerb) -> VerbShape {
        use Section::*;
        const fn items(cap: usize, reads_arg0: bool) -> Option<Items> {
            Some(Items { cap, reads_arg0 })
        }
        // (blob, data block, conflict mode read, items)
        let (blob, data, conflict_mode, items) = match verb {
            // The blob is the schema record of the rows.
            ClientVerb::Push => (Must, May, true, None),
            ClientVerb::Resolve | ClientVerb::SyncPushed | ClientVerb::SnapshotPage | ClientVerb::Backup => {
                (May, Never, false, None)
            }
            ClientVerb::AllocIds
            | ClientVerb::AllocSerialRange
            | ClientVerb::SnapshotOpen
            | ClientVerb::SnapshotClose => (Never, Never, false, None),
            ClientVerb::DdlTxn => (Never, Must, false, items(usize::MAX, false)),
            // The blob is the schema record of the family's rows.
            ClientVerb::PushTxn => (Must, Must, true, items(usize::MAX, false)),
            ClientVerb::ScanMulti => (May, Never, false, items(SCAN_MULTI_MAX_RELATIONS, false)),
            // The blob is the item's cursor, then its `ReadSpec`.
            ClientVerb::DeltaPoll => (Must, Never, false, items(usize::MAX, true)),
        };
        VerbShape {
            sections: Sections { blob, data, conflict_mode },
            items,
        }
    }
}

// ---------------------------------------------------------------------------
// The flags word
// ---------------------------------------------------------------------------

/// The control header's `flags` word. `pack`/`unpack` are the only code that knows
/// the bit layout, so no two fields can overlap and no consumer re-derives one.
///
/// Bit 33 says a data block follows the blob; the frame codec sets and reads it,
/// so `pack` leaves it clear and `unpack` ignores it.
///
/// A request sets `verb` and, on a push, `conflict_mode`; the other fields are
/// a reply's, and [`DecodedControl::client_verb`] refuses a request setting one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct WireFlags {
    /// Bits 0-7: a request's verb; `0`, which no verb is, on a reply.
    pub verb: Option<ClientVerb>,
    /// Bits 8-15: a push's conflict mode.
    pub conflict_mode: WireConflictMode,
    /// Bit 34: more frames of this reply follow. Set on every per-worker reply
    /// frame, absent on the master's terminal frame.
    pub continuation: bool,
    /// Bit 35: a worker's last frame of a train.
    pub scan_last: bool,
    /// Bits 38-39.
    pub lane: WireLane,
}

const CONFLICT_MODE_SHIFT: u32 = 8;
const CONTINUATION_SHIFT: u32 = 34;
const SCAN_LAST_SHIFT: u32 = 35;
const LANE_SHIFT: u32 = 38;

/// The bits a field occupies: `verb` and `conflict_mode` (0-15), the data
/// block bit (33), `continuation`, `scan_last` and the two `lane` bits.
const DEFINED_BITS: u64 =
    crate::low_bits_mask(16) | FLAG_HAS_DATA | 1 << CONTINUATION_SHIFT | 1 << SCAN_LAST_SHIFT | 3 << LANE_SHIFT;
/// Every bit no field occupies.
const RESERVED_BITS: u64 = !DEFINED_BITS;

impl WireFlags {
    /// A frame of a worker's reply train: `continuation` always, since the master's terminal
    /// frame ends the client's reply; `scan_last` on the train's last frame.
    pub const fn train_frame(last: bool) -> Self {
        WireFlags {
            verb: None,
            conflict_mode: WireConflictMode::Update,
            continuation: true,
            scan_last: last,
            lane: WireLane::Reply,
        }
    }

    pub(crate) const fn pack(self) -> u64 {
        let verb = match self.verb {
            Some(v) => v.as_wire(),
            None => 0,
        };
        verb as u64
            | (self.conflict_mode as u64) << CONFLICT_MODE_SHIFT
            | (self.continuation as u64) << CONTINUATION_SHIFT
            | (self.scan_last as u64) << SCAN_LAST_SHIFT
            | (self.lane as u64) << LANE_SHIFT
    }

    /// Rejects a word naming a verb or mode this build does not define, or setting a
    /// bit outside the layout.
    pub(crate) fn unpack(w: u64) -> Result<Self, &'static str> {
        if w & RESERVED_BITS != 0 {
            return Err("flags: reserved bits set");
        }
        let bit = |b: u32| (w >> b) & 1 != 0;
        Ok(WireFlags {
            verb: match w as u8 {
                0 => None,
                v => Some(ClientVerb::from_wire(v).ok_or("flags: unknown request verb")?),
            },
            conflict_mode: WireConflictMode::from_wire((w >> CONFLICT_MODE_SHIFT) as u8)
                .ok_or("flags: unknown conflict mode")?,
            continuation: bit(CONTINUATION_SHIFT),
            scan_last: bit(SCAN_LAST_SHIFT),
            lane: WireLane::from_wire(((w >> LANE_SHIFT) & 3) as u8).ok_or("flags: unknown lane")?,
        })
    }
}

// ---------------------------------------------------------------------------
// The modes the flags word carries
// ---------------------------------------------------------------------------

wire_enum! {
    /// Conflict-resolution mode a PUSH carries in [`WireFlags::conflict_mode`].
    #[derive(Default)]
    pub enum WireConflictMode: u8 {
        /// Retract-and-insert on PK conflict.
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
    /// What a frame from the server answers ([`WireFlags::lane`]).
    #[derive(Default)]
    pub enum WireLane: u8 {
        /// The connection's oldest unanswered request other than a sync — or,
        /// while a pushed train is open, that train: only its opening frame
        /// names [`Self::PushedTrain`].
        #[default]
        Reply = 0,
        /// No request: it opens a train of the subscription `arg0`, the frames
        /// after it to the first without `continuation`.
        PushedTrain = 1,
        /// The connection's oldest unanswered SYNC_PUSHED, whole in this frame.
        SyncAnswer = 2,
    }
}

impl std::str::FromStr for WireConflictMode {
    type Err = String;

    /// The two modes' user-facing names, for the bindings that let a caller
    /// choose one. Owned here, so no binding invents a third spelling.
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
        /// The request carries a descriptor token its relation no longer
        /// answers a RESOLVE with. Nothing was read or written, so the request
        /// is retryable once it is built again from a fresh RESOLVE.
        StaleCatalog = 8,
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

impl WireFault {
    /// Prefix the text with what the caller was doing; the status is kept.
    pub fn in_context(self, what: impl std::fmt::Display) -> Self {
        WireFault {
            text: format!("{what}: {}", self.text),
            status: self.status,
        }
    }
}

impl std::fmt::Display for WireFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

#[cfg(test)]
#[path = "tests/control.rs"]
mod tests;
