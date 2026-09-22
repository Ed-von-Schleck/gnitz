//! SAL (shared append-only log): master→worker broadcast channel.
//!
//! Owns the mmap layout, group-header write/read helpers, SalWriter and its
//! SalScope, SalMessage, SalLog/SalReader.
//!
//! `wal.sal` is an [`ANCHOR_BYTES`] anchor page followed by the ring. The
//! recovery walk over it is the [`zone`] child module.
//!
//! A group's kind byte is this module's alone: callers name a
//! [`SalMessageKind`], and the encode/decode below is the only code entitled to
//! know how it is stored.

pub(crate) mod zone;

use std::cell::Cell;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::runtime::reactor::{AsyncRwLock, Reactor, WriteGuard};
use crate::runtime::w2m::SalWake;
use crate::runtime::wire::{WireData, WireMsg};
use gnitz_foundation::fault::Seam;
use gnitz_foundation::posix_io;
use gnitz_wire::control::frame_head_size;
use gnitz_wire::{low_bits_mask, read_u32_le, read_u64_le, write_u32_le, write_u64_le, BitIter};
use gnitz_wire::{WireFault, WireStatus};

/// `GNITZ_INJECT_SAL_ZONE_PANIC=<scope tag>`: crash the master between a zone's
/// groups publishing and its closing member. The tag (`"ddl"` / `"commit"`)
/// picks which scope aborts.
static ZONE_PANIC: Seam = Seam::new("GNITZ_INJECT_SAL_ZONE_PANIC");

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The publication prefix in front of every group header, one atomically stored
/// u64 ([`epoch_word`]); also the alignment of every group base.
const PREFIX_BYTES: usize = std::mem::size_of::<AtomicU64>();

// Group header field offsets, relative to the start of the header:
//
//   [0,8)   LSN
//   [8]     KIND
//   [9]     FLAGS
//   [10,12) 0
//   [12,16) TARGET_ID
//   [16,20) SLOT_COUNT
//   [20,24) EPOCH
//   [24,32) DIGEST
//   [32,36) REQUEST_ID
//   [36,40) 0
//   [40,..) DIRECTORY, 8-byte-padded

/// The zone LSN of a zone member, `0` outside every zone.
const OFF_LSN: usize = 0;
/// The kind's ordinal (`SalMessageKind::as_wire`), one byte.
const OFF_KIND: usize = 8;
/// `FLAG_*` bits, one byte. A bit outside them is corruption.
const OFF_FLAGS: usize = 9;
/// On a zone's last member: the zone is closed.
const FLAG_ZONE_END: u8 = 1;
/// Each reply must reach the ring in request order.
const FLAG_IN_REQUEST_ORDER: u8 = 2;
const KNOWN_FLAGS: u8 = FLAG_ZONE_END | FLAG_IN_REQUEST_ORDER;
const OFF_TARGET_ID: usize = 12;
const OFF_SLOT_COUNT: usize = 16;
const OFF_EPOCH: usize = 20;
/// The digest's own eight bytes, excluded from the span it covers.
const OFF_DIGEST: usize = 24;
/// `0` = unaddressed.
const OFF_REQUEST_ID: usize = 32;
const OFF_DIRECTORY: usize = 40;

/// One directory entry: `[u32 size][u64 slot checksum]`. The checksum is 0 outside
/// a zone member.
const DIR_ENTRY_BYTES: usize = 12;
const DIR_CHECKSUM_AT: usize = 4;

/// The header's fixed prefix — every scalar field, ending where the directory
/// begins. Read as a `&[u8; HDR_PREFIX]` so the field reads are provably in
/// bounds at const offsets.
const HDR_PREFIX: usize = OFF_DIRECTORY;

/// Header + directory bytes for `slots` slots.
///
/// Sized by the group's own slot count rather than by `MAX_WORKERS`, which makes
/// a group self-describing: `wal.sal` is never truncated, so a group can land on
/// an offset a wider group used before it, and the recorded count is what tells a
/// reader that slot 5 of a 2-slot group is empty rather than a leftover.
#[inline]
const fn group_header_size(slots: usize) -> usize {
    OFF_DIRECTORY + (slots * DIR_ENTRY_BYTES).next_multiple_of(8)
}

// Group bases stay aligned for the publication prefix's atomic store.
const _: () = {
    let mut slots = 0;
    while slots <= MAX_WORKERS {
        assert!(
            group_header_size(slots).is_multiple_of(PREFIX_BYTES),
            "a group header must be a multiple of PREFIX_BYTES, or group bases stop being aligned"
        );
        slots += 1;
    }
};

/// Each **written** slot as `(worker, payload-relative offset, size)`, in
/// directory order. The offset is a pure function of the sizes before it — slot
/// `w` starts where the 8-byte-padded slots ahead of it end — so the directory
/// stores sizes alone and an inconsistent offset is unrepresentable.
fn slot_spans(sizes: impl Iterator<Item = u32>) -> impl Iterator<Item = (usize, usize, usize)> {
    let mut off = 0;
    sizes.enumerate().filter_map(move |(w, sz)| {
        let sz = sz as usize;
        let at = off;
        off += sz.next_multiple_of(8);
        (sz > 0).then_some((w, at, sz))
    })
}

/// The SAL bytes a group occupies: prefix, header, directory and 8-byte-padded
/// slots — what the write cursor advances by, and where [`slot_spans`] leaves off.
fn group_total_size(slots: usize, sizes: impl Iterator<Item = u32>) -> usize {
    PREFIX_BYTES + group_header_size(slots) + sizes.map(|sz| (sz as usize).next_multiple_of(8)).sum::<usize>()
}

/// A group's directory bytes read back as slot sizes.
fn dir_sizes(dir: &[u8]) -> impl Iterator<Item = u32> + '_ {
    dir.as_chunks::<DIR_ENTRY_BYTES>().0.iter().map(|c| read_u32_le(c, 0))
}

/// The most workers a cluster runs: [`WorkerSet`] holds one bit per worker.
pub(crate) const MAX_WORKERS: usize = 64;

/// A subset of the workers. `ALL` is unbounded; every other set holds only
/// launched workers.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct WorkerSet(u64);

const _: () = assert!(MAX_WORKERS <= u64::BITS as usize);

impl WorkerSet {
    pub(crate) const EMPTY: Self = WorkerSet(0);
    pub(crate) const ALL: Self = WorkerSet(u64::MAX);

    pub(crate) const fn one(w: usize) -> Self {
        WorkerSet(1 << w)
    }

    pub(crate) const fn with(self, w: usize) -> Self {
        WorkerSet(self.0 | 1 << w)
    }

    pub(crate) const fn without(self, w: usize) -> Self {
        WorkerSet(self.0 & !(1 << w))
    }

    pub(crate) const fn within(self, nw: usize) -> Self {
        WorkerSet(self.0 & low_bits_mask(nw))
    }

    pub(crate) const fn contains(self, w: usize) -> bool {
        self.0 >> w & 1 == 1
    }

    pub(crate) const fn len(self) -> usize {
        self.0.count_ones() as usize
    }

    /// Whether the set holds every one of the `nw` launched workers.
    pub(crate) const fn covers(self, nw: usize) -> bool {
        self.0 & low_bits_mask(nw) == low_bits_mask(nw)
    }

    /// How many members of the set are below `w`: `w`'s position among them.
    pub(crate) const fn rank(self, w: usize) -> usize {
        (self.0 & low_bits_mask(w)).count_ones() as usize
    }

    pub(crate) fn iter(self) -> BitIter {
        BitIter(self.0)
    }
}

/// Which of a group's slots are written, the request id they answer on, and
/// whether each reply must reach the ring in request order.
#[derive(Clone, Copy)]
pub(crate) enum GroupTargets {
    /// Every slot, on request id 0: nothing answers.
    Unaddressed,
    /// `set`'s slots, every worker answering on `request_id`.
    Leased {
        set: WorkerSet,
        request_id: u32,
        in_request_order: bool,
    },
}

impl GroupTargets {
    /// Every worker, replies in no required order.
    pub(crate) const fn all(request_id: u32) -> Self {
        Self::Leased {
            set: WorkerSet::ALL,
            request_id,
            in_request_order: false,
        }
    }

    fn set(&self) -> WorkerSet {
        match *self {
            Self::Unaddressed => WorkerSet::ALL,
            Self::Leased { set, .. } => set,
        }
    }

    /// The request id replies answer on, and the header flags saying how.
    fn request(&self) -> (u32, u8) {
        match *self {
            Self::Unaddressed => (0, 0),
            Self::Leased { request_id, in_request_order, .. } => {
                (request_id, if in_request_order { FLAG_IN_REQUEST_ORDER } else { 0 })
            }
        }
    }
}

/// What a group's slots carry.
#[derive(Clone, Copy)]
pub(crate) enum GroupData<'a> {
    /// Every slot carries this payload.
    Same(WireData<'a>),
    /// Slot `w` carries `d[w]`. `d.len()` must be `nw`.
    PerWorker(&'a [WireData<'a>]),
}

impl<'a> GroupData<'a> {
    /// A control-only group: every slot is a bare control block.
    pub(crate) const NONE: Self = Self::Same(WireData::None);

    /// Worker `w`'s payload `slots[w]`, or — for a single slot — the one
    /// payload every worker is sent.
    pub(crate) fn of(slots: &'a [WireData<'a>]) -> Self {
        match slots {
            [one] => GroupData::Same(*one),
            each => GroupData::PerWorker(each),
        }
    }

    /// True when no slot carries a row — the one shape allowed to omit a schema.
    fn is_dataless(&self) -> bool {
        match *self {
            GroupData::Same(d) => d.row_count() == 0,
            GroupData::PerWorker(d) => d.iter().all(|d| d.row_count() == 0),
        }
    }
}

/// One SAL group as [`SalWriter::write`] emits it: its header fields, the
/// [`WireMsg`] every slot shares, plus what varies by worker.
///
/// [`SalWriter::footprint`] sizes the same value through the same
/// [`DirectGroup::msg`], so a slot's size and its bytes cannot disagree.
#[derive(Clone, Copy)]
pub(crate) struct DirectGroup<'a> {
    pub(crate) kind: SalMessageKind,
    /// Every slot's message; `msg` fills in `data` per slot, and drops
    /// `schema_block` from a rowless one unless
    /// [`SalMessageKind::schema_survives_a_rowless_slot`].
    pub(crate) template: WireMsg<'a>,
    pub(crate) data: GroupData<'a>,
    /// Slot `w`'s blob in place of the template's, when every worker is sent
    /// its own.
    pub(crate) extras: Option<&'a [Vec<u8>]>,
    pub(crate) targets: GroupTargets,
}

impl<'a> DirectGroup<'a> {
    /// A control-only silent broadcast of `kind`. Callers override what varies
    /// by struct update.
    pub(crate) fn new(kind: SalMessageKind) -> Self {
        DirectGroup {
            kind,
            template: WireMsg::default(),
            data: GroupData::NONE,
            extras: None,
            targets: GroupTargets::Unaddressed,
        }
    }

    /// Worker `w`'s message. The one definition — sizing and encoding both go
    /// through it, so a slot's size and its bytes cannot disagree.
    ///
    /// `#[inline]` so the sizing caller's dead field stores fold away.
    #[inline]
    fn msg(&self, w: usize) -> WireMsg<'a> {
        debug_assert!(
            matches!(self.template.data, WireData::None),
            "DirectGroup template must leave `data` to the per-worker fill"
        );
        let data = match self.data {
            GroupData::Same(d) => d,
            GroupData::PerWorker(d) => d[w],
        };
        let keeps_schema = data.row_count() > 0 || self.kind.schema_survives_a_rowless_slot();
        WireMsg {
            data,
            schema_block: self.template.schema_block.filter(|_| keeps_schema),
            blob: self.extras.map_or(self.template.blob, |e| &e[w]),
            ..self.template
        }
    }

    /// Every slot's size into `out`, zero for the ones this group does not
    /// write — `out.len()` is the group's slot count, and every element is
    /// assigned, so no stale entry can claim a slot the group never filled.
    ///
    /// A written slot is never zero-size — every slot carries a control block
    /// whatever else it does — which is what lets [`SalReader::next`] read an
    /// empty slot as "not for this worker". The assert below is where that would
    /// break.
    fn slot_sizes_into(&self, out: &mut [u32]) {
        let set = self.targets.set();
        for (w, size) in out.iter_mut().enumerate() {
            if !set.contains(w) {
                *size = 0;
                continue;
            }
            *size = self.msg(w).size() as u32;
            debug_assert!(*size > 0, "a written slot always carries at least a control block");
        }
    }
}

/// XXH3-64 over a SAL group header — its own eight bytes excluded — seeded with
/// the group's byte offset in the ring, so a header only verifies where it was
/// published. The epoch is not a seed: it sits in the hashed span, so verifying a
/// header also authenticates the generation it claims.
fn group_digest(base: u64, hdr: &[u8]) -> u64 {
    gnitz_wire::digest_with_hole(&base.to_le_bytes(), hdr, OFF_DIGEST)
}

/// Stamp the digest of `hdr`, the header of a group at `base`.
fn stamp_digest(base: u64, hdr: &mut [u8]) {
    let digest = group_digest(base, hdr);
    write_u64_le(hdr, OFF_DIGEST, digest);
}

const SAL_MMAP_SIZE: usize = 1 << 30;

/// The page at the head of `wal.sal` holding the anchor: the walk epoch, and the
/// ring offset a completed `fdatasync` covers. At the head, so it does not move
/// when `GNITZ_SAL_BYTES` does.
pub(crate) const ANCHOR_BYTES: usize = 4096;

const _: () = assert!(
    SAL_MMAP_SIZE as u64 <= 1 << 32,
    "the anchor packs a ring offset into 32 bits"
);

/// The epoch after `epoch`: what a checkpoint moves the writer and every reader
/// to, and what a boot starts at above the recovered tail's epoch.
const fn next_epoch(epoch: u32) -> u32 {
    epoch + 1
}

/// Space only a terminal group — a checkpoint round's `Flush`/`FlushEph`, the
/// `Shutdown` broadcast — may spend, so those always fit.
const CHECKPOINT_RESERVE: usize = 64 << 10;

const _: () = {
    let terminal =
        PREFIX_BYTES + group_header_size(MAX_WORKERS) + MAX_WORKERS * frame_head_size(0, None).next_multiple_of(8);
    assert!(
        CHECKPOINT_RESERVE >= 2 * terminal,
        "the checkpoint band must hold two control-only MAX_WORKERS groups"
    );
};

/// Bytes a group of `kind` leaves free behind it: its end prefix, plus the
/// checkpoint band unless nothing follows it in its epoch.
const fn held_back(kind: SalMessageKind) -> usize {
    PREFIX_BYTES
        + match kind {
            SalMessageKind::Shutdown | SalMessageKind::Flush | SalMessageKind::FlushEph => 0,
            _ => CHECKPOINT_RESERVE,
        }
}

/// The highest byte a group of this kind may occupy. Saturating because nothing
/// structurally bounds a [`SalWriter::new`] below `CHECKPOINT_RESERVE`, and a
/// release build would otherwise wrap to a cap past the end of the mapping.
fn effective_max(kind: SalMessageKind, ring_len: usize) -> usize {
    ring_len.saturating_sub(held_back(kind))
}

/// Whether a group of a given size can be written, and if not, whether a
/// checkpoint would change that.
///
/// A **pre-write** verdict, and the only place the distinction is visible: a
/// group already handed to the writer comes back as a [`WireFault`], so no write
/// path can tell "retry in pieces" from "checkpoint and retry whole".
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum SalFit {
    /// Fits the remaining space — emit it.
    Fits,
    /// Fits an empty SAL but not the current remainder: a checkpoint reclaims
    /// enough space, so a retry can succeed.
    Transient,
    /// Exceeds the SAL outright — no checkpoint can help; retrying is futile.
    Terminal,
}

impl SalFit {
    /// The client-facing refusal for a group of `kind` that did not fit. Carries
    /// [`WireStatus::SalFull`], which is what a caller matches to tell this refusal
    /// from a real failure; the transient/terminal distinction stays typed and is
    /// not spelled into the text. The write cursor is deliberately absent — this
    /// reaches clients verbatim — and goes to the operator log instead.
    fn refusal(self, kind: SalMessageKind) -> WireFault {
        debug_assert_ne!(self, SalFit::Fits, "a fitting group has no refusal");
        WireFault {
            status: WireStatus::SalFull,
            text: format!("SAL full: {kind:?} group did not fit"),
        }
    }
}

/// Floor for a `GNITZ_SAL_BYTES` override — must comfortably exceed one DDL zone
/// plus the checkpoint headroom.
const MIN_SAL_BYTES: usize = 16 << 20;

/// `SAL_MMAP_SIZE` (1 GiB), or `GNITZ_SAL_BYTES` clamped into
/// `[MIN_SAL_BYTES, SAL_MMAP_SIZE]` and logged when the clamp moves it. The
/// override exists because each server `fallocate`s its whole SAL at startup, so
/// dozens of test servers sharing a tmpfs would exhaust it. Called once, by the
/// boot that maps the SAL; every consumer carries the length beside the pointer.
pub(in crate::runtime) fn sal_mmap_size() -> usize {
    let asked = gnitz_foundation::env::env_num("GNITZ_SAL_BYTES", SAL_MMAP_SIZE);
    let size = asked.clamp(MIN_SAL_BYTES, SAL_MMAP_SIZE);
    if size != asked {
        gnitz_info!("GNITZ_SAL_BYTES={asked} is outside [{MIN_SAL_BYTES}, {SAL_MMAP_SIZE}]; using {size}");
    }
    size
}

/// The fraction of the mapping the relay reclaim margin is: `mmap >> 3`, one
/// eighth. How much must be **free**.
const RECLAIM_FRACTION_SHIFT: u32 = 3;

// ---------------------------------------------------------------------------
// Group kinds
// ---------------------------------------------------------------------------

gnitz_wire::wire_enum! {
    /// What a SAL group asks a worker to do. Stored as one ordinal byte in the
    /// group header; `ALL`/`from_wire` come from the one variant list, so a
    /// decode cannot fall behind the enum.
    pub(crate) enum SalMessageKind: u8 {
        /// Full table scan.
        Scan = 0,
        Shutdown = 1,
        /// Base round of a checkpoint: flush base and system tables.
        Flush = 2,
        /// Ephemeral round of a checkpoint: flush every view's traces and output
        /// stores, stamped with the generation in `arg0`.
        FlushEph = 3,
        /// Catalog mutation.
        DdlSync = 4,
        /// `arg0` = the source relation.
        ExchangeRelay = 5,
        /// Initial full-source scan feeding a newly created view: `target_id` =
        /// the source, `arg0` = the view.
        Backfill = 6,
        /// Probe a relation's PK store or one of its secondary indexes for a
        /// scattered/broadcast key list; `WireProbeMode` names what a matched
        /// key is answered with, up to and including one projected column.
        /// `arg1` = the keyspace, `arg0` = the mode's parameter.
        HasPk = 7,
        /// CREATE UNIQUE INDEX pre-flight: stream the sorted key spans of the
        /// column list in `arg1` for the master's merge.
        UniquePreflight = 8,
        Push = 9,
        /// Drive one view-maintenance tick: `arg0` = the tick round.
        Tick = 10,
        /// A parameterized bounded read (`ReadSpec`).
        ScanSpec = 11,
        /// One DELTA_POLL view: `arg1` = `after_tick`, `arg0` = the cut round, the
        /// blob = the reply block.
        DeltaRead = 12,
    }
}

impl SalMessageKind {
    /// Whether a slot of this kind still needs the group's schema block when it
    /// carries no rows — the *handler's* behaviour on an empty slot decides, so
    /// this is per-kind and not per-writer: an `ExchangeRelay` builds its batch
    /// from the block alone, a `Push` reaches a no-op, a `HasPk` never has one.
    fn schema_survives_a_rowless_slot(self) -> bool {
        use SalMessageKind::*;
        match self {
            Push | HasPk => false,
            Scan | Shutdown | Flush | FlushEph | DdlSync | ExchangeRelay | Backfill | UniquePreflight | Tick
            | ScanSpec | DeltaRead => true,
        }
    }

    /// Whether the template's blob is a `ReadSpec` whose bound routes the read.
    pub(crate) const fn carries_read_bound(self) -> bool {
        matches!(self, SalMessageKind::ScanSpec)
    }
}

/// `(epoch << 32) | low`: `low` = [`PRESENT`] for a prefix, the synced offset
/// for the anchor.
#[inline]
const fn epoch_word(epoch: u32, low: u64) -> u64 {
    (epoch as u64) << 32 | low
}

/// The low half of every publication prefix word: a presence marker, so zeroing a
/// published word takes at least 33 bit flips.
const PRESENT: u64 = u32::MAX as u64;

/// The epoch half of an [`epoch_word`].
#[inline]
const fn word_epoch(word: u64) -> u32 {
    (word >> 32) as u32
}

/// The prefix word at `offset` as an atomic, for the Acquire/Release pair that
/// publishes a group across processes.
///
/// # Safety
/// `offset + PREFIX_BYTES` must lie within the mapping.
#[inline]
unsafe fn prefix_atomic<'a>(sal_ptr: *const u8, offset: usize) -> &'a AtomicU64 {
    AtomicU64::from_ptr(sal_ptr.add(offset).cast_mut().cast())
}

/// Zero the prefix word at `offset`, ending the log there.
///
/// # Safety
/// `offset + PREFIX_BYTES` must lie within the mapping.
#[inline]
unsafe fn write_end_prefix(sal_ptr: *mut u8, offset: usize) {
    prefix_atomic(sal_ptr, offset).store(0, Ordering::Release);
}

// ---------------------------------------------------------------------------
// SAL read
// ---------------------------------------------------------------------------

/// One SAL group as a reader sees it. Slot resolution is
/// [`SalMessage::slot`]'s: the seeded digest means a header is valid only where
/// it was published, so the bytes and the header that describes them are one
/// value and no slot read costs a second digest.
pub(crate) struct SalMessage {
    /// The zone LSN of a zone member, `0` outside every zone.
    pub(crate) lsn: u64,
    pub(crate) kind: SalMessageKind,
    /// Whether this group is its zone's last member, the one that closes it.
    zone_end: bool,
    pub(crate) target_id: u32,
    /// The group's byte offset in the ring — what a recovery error names.
    pub(crate) base: u64,
    /// The offset past the group.
    end: u64,
    /// `0` = unaddressed.
    pub(crate) request_id: u32,
    pub(crate) in_request_order: bool,
    /// The group's slot bytes, from slot 0's offset to the end of the group.
    payload: &'static [u8],
    /// The directory: one `DIR_ENTRY_BYTES` entry per slot, which is also what
    /// says how many slots the group has.
    dir: &'static [u8],
}

impl SalMessage {
    /// Whether every slot is what the writer checksummed; only a zone member's are.
    fn intact(&self) -> bool {
        self.slots_written().all(|(w, b)| self.slot_intact(w, b))
    }

    /// Whether `bytes` are what the writer checksummed for zone member slot `w`.
    fn slot_intact(&self, w: u32, bytes: &[u8]) -> bool {
        debug_assert!(self.lsn != 0, "only a zone member's slots carry checksums");
        gnitz_wire::checksum(bytes) == read_u64_le(self.dir, w as usize * DIR_ENTRY_BYTES + DIR_CHECKSUM_AT)
    }

    /// The slot count the group was written with.
    pub(crate) fn slots(&self) -> u32 {
        (self.dir.len() / DIR_ENTRY_BYTES) as u32
    }

    /// Slot `w`'s bytes, or `None` when this group did not write it (`w` at or
    /// past the slot count included).
    ///
    /// A pure function of the message: no log, no re-read, and no re-digest.
    pub(crate) fn slot(&self, w: u32) -> Option<&'static [u8]> {
        self.slots_written().find(|&(i, _)| i == w).map(|(_, b)| b)
    }

    /// Every written slot as `(worker, bytes)`, in directory order.
    pub(crate) fn slots_written(&self) -> impl Iterator<Item = (u32, &'static [u8])> + '_ {
        let payload = self.payload;
        slot_spans(dir_sizes(self.dir)).map(move |(w, off, sz)| (w as u32, &payload[off..off + sz]))
    }
}

/// What the bytes at an offset are.
enum SalStep {
    /// Nothing published here, the group's own stride runs past this mapping
    /// (the log was written under a larger `GNITZ_SAL_BYTES`), or a verified
    /// header stamped with another epoch — the ring's leftovers. Either way, the
    /// end of the log.
    Absent,
    /// A published header at this offset that fails its digest, or does not
    /// decode.
    Corrupt,
    Group(SalMessage),
}

/// How a reader treats the epoch a group is stamped with.
#[derive(Clone, Copy)]
enum EpochGate {
    /// Live worker drain. Rejects on the prefix's epoch copy **before a single
    /// header byte is read**: a parked slot may be overwritten by the master
    /// under us, so its bytes are unreadable until the prefix proves the slot is
    /// ours. The header's own epoch is then checked too, since the prefix copy is
    /// outside the digest and a leftover whose copy flipped up would otherwise be
    /// consumed as current.
    Live(u32),
    /// Quiescent recovery walk. The prefix is not consulted, so a flipped prefix
    /// epoch changes nothing; only the digested header's epoch decides.
    Walk(u32),
}

impl EpochGate {
    fn epoch(self) -> u32 {
        match self {
            EpochGate::Live(e) | EpochGate::Walk(e) => e,
        }
    }
}

/// A group header that passed its digest and decoded.
struct GroupHeader {
    lsn: u64,
    kind: SalMessageKind,
    /// `FLAG_*` bits, none outside [`KNOWN_FLAGS`].
    flags: u8,
    target_id: u32,
    epoch: u32,
    request_id: u32,
    /// One `DIR_ENTRY_BYTES` entry per slot.
    dir: &'static [u8],
}

impl GroupHeader {
    /// The group's slot count, off the directory it was written with.
    fn slots(&self) -> usize {
        self.dir.len() / DIR_ENTRY_BYTES
    }

    /// The SAL bytes the group occupies, prefix included — its stride.
    fn total_size(&self) -> usize {
        group_total_size(self.slots(), dir_sizes(self.dir))
    }
}

/// The anchor's bytes: its [`epoch_word`], then that word's checksum.
const ANCHOR_RECORD: usize = 16;

/// The SAL mapping, as anything that reads it sees it.
///
/// A non-owning view: the mapping is created in `bootstrap` and inherited across
/// the `fork()`, so this has no `Drop`.
#[derive(Clone, Copy)]
pub(crate) struct SalLog {
    ring: *mut u8,
    ring_len: usize,
}

impl SalLog {
    /// # Safety
    /// `ptr` must be a valid mmap pointer of `len > ANCHOR_BYTES` bytes, live for
    /// as long as this view and everything read through it.
    pub(crate) unsafe fn new(ptr: *const u8, len: usize) -> Self {
        SalLog {
            ring: ptr.add(ANCHOR_BYTES).cast_mut(),
            ring_len: len - ANCHOR_BYTES,
        }
    }

    fn anchor_record(&self) -> *mut [u8; ANCHOR_RECORD] {
        self.ring.wrapping_sub(ANCHOR_BYTES).cast()
    }

    /// The walk epoch, and the ring offset a completed `fdatasync` covers.
    /// All-zero is a fresh file.
    fn anchor(&self) -> Result<(u32, u64), String> {
        // SAFETY: the anchor page is mapped ahead of the ring.
        let a = unsafe { &*self.anchor_record() };
        let (word, sum) = (read_u64_le(a, 0), read_u64_le(a, 8));
        if word == 0 && sum == 0 {
            return Ok((0, 0));
        }
        if gnitz_wire::checksum(&a[..8]) != sum {
            return Err(format!(
                "SAL anchor fails its checksum (word={word:#x}): the head of wal.sal is damaged"
            ));
        }
        Ok((word_epoch(word), word & u64::from(u32::MAX)))
    }

    /// The publication prefix word at `base`, or 0 when nothing was published
    /// there — which is also the answer for a `base` the mapping cannot hold.
    /// Acquire-loaded, so a group whose prefix is visible has its whole header
    /// visible too (the publishing store lands last).
    #[inline]
    fn prefix_word(&self, base: u64) -> u64 {
        if base as usize + PREFIX_BYTES > self.ring_len {
            return 0;
        }
        unsafe { prefix_atomic(self.ring, base as usize).load(Ordering::Acquire) }
    }

    /// The digest-verified, decoded header at `base`, or `None`. Each bound
    /// precedes the read it guards. The publication prefix is not consulted.
    fn probe_header(&self, base: u64) -> Option<GroupHeader> {
        let hdr_off = base as usize + PREFIX_BYTES;
        let m = self.ring_len;
        if hdr_off + HDR_PREFIX > m {
            return None;
        }
        // SAFETY: the bound above admits the fixed prefix.
        let prefix: &'static [u8; HDR_PREFIX] = unsafe { &*(self.ring.add(hdr_off) as *const [u8; HDR_PREFIX]) };
        let slots = read_u32_le(prefix, OFF_SLOT_COUNT) as usize;
        if slots > MAX_WORKERS {
            return None;
        }
        let hdr_len = group_header_size(slots);
        if hdr_off + hdr_len > m {
            return None;
        }
        // SAFETY: bounded directly above.
        let hdr = unsafe { std::slice::from_raw_parts(self.ring.add(hdr_off), hdr_len) };
        if group_digest(base, hdr) != read_u64_le(hdr, OFF_DIGEST) {
            return None;
        }
        let kind = SalMessageKind::from_wire(prefix[OFF_KIND])?;
        let flags = prefix[OFF_FLAGS];
        let lsn = read_u64_le(prefix, OFF_LSN);
        if flags & !KNOWN_FLAGS != 0 || (flags & FLAG_ZONE_END != 0 && lsn == 0) {
            return None;
        }
        Some(GroupHeader {
            lsn,
            kind,
            flags,
            target_id: read_u32_le(prefix, OFF_TARGET_ID),
            epoch: read_u32_le(prefix, OFF_EPOCH),
            request_id: read_u32_le(prefix, OFF_REQUEST_ID),
            dir: &hdr[OFF_DIRECTORY..OFF_DIRECTORY + slots * DIR_ENTRY_BYTES],
        })
    }

    /// Classify the bytes at `cursor`, building the message and the next cursor
    /// when they are a readable group.
    ///
    /// The caller decides what a `Corrupt` verdict means: the live drain
    /// fail-stops (see [`SalReader::next`]), the recovery walk stops there.
    fn read_at(&self, cursor: u64, gate: EpochGate) -> SalStep {
        let word = self.prefix_word(cursor);
        if word == 0 {
            return SalStep::Absent;
        }
        if let EpochGate::Live(exp) = gate {
            if word_epoch(word) != exp {
                return SalStep::Absent;
            }
        }

        let Some(hdr) = self.probe_header(cursor) else {
            return SalStep::Corrupt;
        };
        if hdr.epoch != gate.epoch() {
            return SalStep::Absent;
        }

        let end = cursor as usize + hdr.total_size();
        if end > self.ring_len {
            return SalStep::Absent;
        }

        // SAFETY: bounded directly above; the payload runs from the header's end
        // to the group's end.
        let payload_off = cursor as usize + PREFIX_BYTES + group_header_size(hdr.slots());
        let payload = unsafe { std::slice::from_raw_parts(self.ring.add(payload_off), end - payload_off) };
        SalStep::Group(SalMessage {
            lsn: hdr.lsn,
            kind: hdr.kind,
            zone_end: hdr.flags & FLAG_ZONE_END != 0,
            target_id: hdr.target_id,
            request_id: hdr.request_id,
            in_request_order: hdr.flags & FLAG_IN_REQUEST_ORDER != 0,
            base: cursor,
            end: end as u64,
            payload,
            dir: hdr.dir,
        })
    }

    /// Every group readable from offset 0 at `epoch`, up to the first that is not.
    fn walk(self, epoch: u32) -> impl Iterator<Item = SalMessage> {
        let mut at = 0;
        std::iter::from_fn(move || match self.read_at(at, EpochGate::Walk(epoch)) {
            SalStep::Group(msg) => {
                at = msg.end;
                Some(msg)
            }
            SalStep::Absent | SalStep::Corrupt => None,
        })
    }
}

// ---------------------------------------------------------------------------
// SalWriter
// ---------------------------------------------------------------------------

/// The cursor and zone state a group laid out in a scope can be rolled back to.
/// Taken before a group is laid out and consumed only by
/// [`SalScope::roll_back`].
#[derive(Clone, Copy)]
pub(crate) struct Savepoint {
    cursor: u64,
    last_member: Option<u64>,
}

pub(crate) struct SalWriter {
    log: SalLog,
    fd: i32,
    write_cursor: Cell<u64>,
    epoch: Cell<u32>,
    /// The anchored synced offset of the live epoch.
    synced: Cell<u64>,
    checkpoint_threshold: u64,
    /// A group was refused as [`SalFit::Transient`] since the last reset: a
    /// checkpoint would admit it, so one is warranted whatever the write cursor
    /// says. Cleared by [`Self::rewind`].
    refused_transient: Cell<bool>,
    /// Slots per group: the group header's `slot_count`, the directory's length
    /// and every slot offset are all this. It is the log's framing, so it is the
    /// log writer's own field — never read off some other per-worker resource.
    num_workers: usize,
    /// Taken by every [`SalExcl`].
    excl: AsyncRwLock,
    /// One wake per worker, in worker order.
    wakes: Vec<SalWake>,
    /// The workers groups written since the last [`SalExcl::wake`] reached.
    reached: Cell<u64>,
}

impl SalWriter {
    /// Starts at epoch 0, which [`Self::write_slots`] refuses: nothing can be
    /// written before the boot [`SalExcl::boot_rewind`] sets the live epoch.
    pub(crate) fn new(log: SalLog, fd: i32, num_workers: usize, wakes: Vec<SalWake>) -> Self {
        assert_eq!(wakes.len(), num_workers, "one SAL wake per worker");
        let checkpoint_threshold =
            gnitz_foundation::env::env_num("GNITZ_CHECKPOINT_BYTES", (log.ring_len as u64 * 3) >> 2);
        SalWriter {
            log,
            fd,
            write_cursor: Cell::new(0),
            epoch: Cell::new(0),
            synced: Cell::new(0),
            checkpoint_threshold,
            refused_transient: Cell::new(false),
            num_workers,
            excl: AsyncRwLock::default(),
            wakes,
            reached: Cell::new(0),
        }
    }

    /// Sole write access, once no other task holds it.
    pub(crate) async fn lock(&self) -> SalExcl<'_> {
        SalExcl {
            writer: self,
            _guard: self.excl.write().await,
        }
    }

    /// For a caller that polls no other task: panics if a task holds the writer.
    pub(crate) fn lock_exclusive(&self) -> SalExcl<'_> {
        SalExcl {
            writer: self,
            _guard: self
                .excl
                .try_write()
                .expect("an exclusive SAL write found a task holding the writer"),
        }
    }

    /// Slots per group — the log's framing.
    pub(crate) fn num_workers(&self) -> usize {
        self.num_workers
    }

    /// Classify `need` bytes against `cap` and the live cursor: past the cap
    /// outright no checkpoint can help, past what is left of it one can. The one
    /// spelling of the rule — [`Self::write_slots`] admits a group by it, and
    /// [`Self::fit_relay`] answers for a caller that has not written one yet.
    fn fit_within(&self, cap: usize, need: usize) -> SalFit {
        if need > cap {
            SalFit::Terminal
        } else if need > cap.saturating_sub(self.write_cursor.get() as usize) {
            SalFit::Transient
        } else {
            SalFit::Fits
        }
    }

    /// Whether relay group `g` may be written, refusing also while less than the
    /// reclaim margin is free, so a reclaim lands on a writer with room to spare.
    pub(crate) fn fit_relay(&self, g: &DirectGroup) -> SalFit {
        self.fit_relay_bytes(self.footprint(g))
    }

    /// [`Self::fit_relay`] for a relay of `need` bytes.
    fn fit_relay_bytes(&self, need: usize) -> SalFit {
        self.fit_within(
            effective_max(SalMessageKind::ExchangeRelay, self.log.ring_len),
            need.max(self.reclaim_margin()),
        )
    }

    /// Less than the reclaim margin is free, so a relay-sized group would be
    /// refused.
    pub(crate) fn below_reclaim_margin(&self) -> bool {
        self.fit_relay_bytes(0) != SalFit::Fits
    }

    /// The margin below which the SAL is "running low": one eighth of the
    /// mapping. The single name for it — a relay is refused below it so the
    /// reclaim lands on a writer with room to spare rather than on one that has
    /// run out.
    fn reclaim_margin(&self) -> usize {
        self.log.ring_len >> RECLAIM_FRACTION_SHIFT
    }

    /// Lay one group out at the write cursor, `fill(worker, slot)` per non-empty
    /// slot, and return its base — the group is complete on the log but not yet
    /// published, which [`Self::publish`] does. A refused group
    /// leaves the log untouched. A zone member (`lsn != 0`) checksums every slot.
    #[allow(clippy::too_many_arguments)]
    fn write_slots(
        &self,
        target_id: u32,
        lsn: u64,
        kind: SalMessageKind,
        flags: u8,
        request_id: u32,
        sizes: &[u32],
        mut fill: impl FnMut(usize, &mut [u8]),
    ) -> Result<usize, SalFit> {
        assert!(
            sizes.len() <= MAX_WORKERS,
            "a SAL group cannot carry more than MAX_WORKERS slots"
        );
        let epoch = self.epoch.get();
        assert!(
            epoch >= 1,
            "SAL group epoch must be >= 1 — epoch 0 is indistinguishable from the empty prefix"
        );

        let hdr_size = group_header_size(sizes.len());
        let total = group_total_size(sizes.len(), sizes.iter().copied());
        let fit = self.fit_within(effective_max(kind, self.log.ring_len), total);
        if fit != SalFit::Fits {
            // On the writer, not on the scope: the group did not fit whether or
            // not a rolled-back transaction contained it.
            if fit == SalFit::Transient {
                self.refused_transient.set(true);
            }
            gnitz_debug!(
                "SAL group refused as {:?}: cursor={} mmap={} epoch={} need={}",
                fit,
                self.write_cursor.get(),
                self.log.ring_len,
                epoch,
                total
            );
            return Err(fit);
        }

        let base = self.write_cursor.get() as usize;
        let hdr_off = base + PREFIX_BYTES;
        // SAFETY: `cursor + total <= ring_len`, so the whole group is mapped, and
        // nothing else references the span past the write cursor. One
        // `&mut [u8]` over the header, so every field write below is
        // bounds-checked against it rather than argued in prose.
        let hdr = unsafe { std::slice::from_raw_parts_mut(self.log.ring.add(hdr_off), hdr_size) };
        write_u64_le(hdr, OFF_LSN, lsn);
        hdr[OFF_KIND..OFF_KIND + 4].copy_from_slice(&[kind.as_wire(), flags, 0, 0]);
        write_u32_le(hdr, OFF_TARGET_ID, target_id);
        write_u32_le(hdr, OFF_SLOT_COUNT, sizes.len() as u32);
        write_u32_le(hdr, OFF_EPOCH, epoch);
        write_u32_le(hdr, OFF_REQUEST_ID, request_id);
        write_u32_le(hdr, OFF_REQUEST_ID + 4, 0);
        // Every entry is written, empty ones included, and the 8-byte pad
        // behind them zeroed: `wal.sal` is never truncated, so an unwritten byte
        // inside the digested span would read as whatever group last occupied
        // this offset.
        for (w, &sz) in sizes.iter().enumerate() {
            let entry = OFF_DIRECTORY + w * DIR_ENTRY_BYTES;
            write_u32_le(hdr, entry, sz);
            write_u64_le(hdr, entry + DIR_CHECKSUM_AT, 0);
        }
        hdr[OFF_DIRECTORY + sizes.len() * DIR_ENTRY_BYTES..].fill(0);

        for (w, off, sz) in slot_spans(sizes.iter().copied()) {
            // SAFETY: `off + sz <= total - PREFIX_BYTES - hdr_size`, inside the mapped
            // span, and the spans are disjoint.
            let slot = unsafe { std::slice::from_raw_parts_mut(self.log.ring.add(hdr_off + hdr_size + off), sz) };
            fill(w, &mut *slot);
            if lsn != 0 {
                let at = OFF_DIRECTORY + w * DIR_ENTRY_BYTES + DIR_CHECKSUM_AT;
                write_u64_le(hdr, at, gnitz_wire::checksum(slot));
            }
        }

        let end = base + total;
        unsafe { write_end_prefix(self.log.ring, end) };
        // Stamped before anything can publish the group, so no readable group
        // carries an unstamped digest. Slots start at `hdr_size`, so these are
        // final bytes.
        stamp_digest(base as u64, hdr);

        self.write_cursor.set(end as u64);
        Ok(base)
    }

    /// The Release store that makes one laid-out group visible.
    fn publish(&self, base: usize) {
        debug_assert_eq!(self.laid_out(base as u64).epoch, self.epoch.get());
        let word = epoch_word(self.epoch.get(), PRESENT);
        unsafe { prefix_atomic(self.log.ring, base).store(word, Ordering::Release) };
    }

    /// The header of a group this writer laid out at `base`.
    fn laid_out(&self, base: u64) -> GroupHeader {
        self.log
            .probe_header(base)
            .expect("a group laid out in this scope verifies at its own offset")
    }

    /// Publish the group laid out at `base` and return its end.
    fn publish_at(&self, base: u64) -> u64 {
        self.publish(base as usize);
        base + self.laid_out(base).total_size() as u64
    }

    /// Publish every group laid out in `[from, end)` in layout order.
    fn publish_range(&self, from: u64, end: u64) {
        let mut off = from;
        while off < end {
            off = self.publish_at(off);
        }
    }

    /// Mark the unpublished group at `base` as its zone's last member and
    /// re-stamp its digest.
    fn mark_zone_end(&self, base: u64) {
        let hdr_len = group_header_size(self.laid_out(base).slots());
        // SAFETY: laid out by this writer, so mapped, and unpublished, so no
        // reader holds it.
        let hdr = unsafe { std::slice::from_raw_parts_mut(self.log.ring.add(base as usize + PREFIX_BYTES), hdr_len) };
        hdr[OFF_FLAGS] |= FLAG_ZONE_END;
        stamp_digest(base, hdr);
    }

    /// Open a publication scope at zone LSN `lsn`: groups laid out in it are
    /// invisible until [`SalScope::commit`], and an uncommitted drop discards
    /// the span. `tag` names the scope for [`ZONE_PANIC`].
    fn begin(&self, lsn: u64, tag: &'static str) -> SalScope<'_> {
        debug_assert!(lsn != 0, "LSN 0 marks a group outside every zone");
        SalScope {
            writer: self,
            from: self.write_cursor.get(),
            lsn,
            tag,
            last_member: Cell::new(None),
            committed: false,
        }
    }

    /// Encode a group's per-worker wire messages directly into the SAL mmap and
    /// publish it, outside every zone.
    fn write(&self, g: &DirectGroup) -> Result<(), WireFault> {
        let base = self.lay_out(g, 0)?;
        self.publish(base);
        Ok(())
    }

    /// Lay `g` out at the write cursor, unpublished, stamped with `lsn`. The one
    /// encode path, for both writers above.
    fn lay_out(&self, g: &DirectGroup, lsn: u64) -> Result<usize, WireFault> {
        let nw = self.num_workers;
        if let GroupData::PerWorker(d) = g.data {
            assert_eq!(d.len(), nw, "worker_data.len()={} != num_workers={}", d.len(), nw);
        }
        debug_assert!(
            g.template.schema_block.is_some() || g.data.is_dataless(),
            "data without a schema — `decode_sal_slot` rejects a data block without a schema block",
        );

        let mut sizes = [0u32; MAX_WORKERS];
        g.slot_sizes_into(&mut sizes[..nw]);
        let (request_id, flags) = g.targets.request();
        let laid_out = self
            .write_slots(
                g.template.target_id as u32,
                lsn,
                g.kind,
                flags,
                request_id,
                &sizes[..nw],
                |w, slot| g.msg(w).encode(slot),
            )
            .map_err(|fit| fit.refusal(g.kind))?;
        self.reached.set(self.reached.get() | g.targets.set().0);
        Ok(laid_out)
    }

    /// The exact number of SAL bytes [`Self::write`] will consume for `g`. The
    /// checkpoint band is not included; the fit check holds it back.
    fn footprint(&self, g: &DirectGroup) -> usize {
        let nw = self.num_workers;
        let mut sizes = [0u32; MAX_WORKERS];
        g.slot_sizes_into(&mut sizes[..nw]);
        group_total_size(nw, sizes[..nw].iter().copied())
    }

    /// Whether a checkpoint is warranted: the write cursor has crossed the
    /// configured threshold, or a group has been refused as
    /// [`SalFit::Transient`] since the last reset — one a checkpoint would admit,
    /// on a log whose cursor may still sit below the threshold.
    pub(crate) fn needs_checkpoint(&self) -> bool {
        self.write_cursor.get() >= self.checkpoint_threshold || self.refused_transient.get()
    }

    /// Rewind to cursor 0 of `epoch`, which is anchored durably before any of
    /// its groups can exist.
    fn rewind(&self, epoch: u32) {
        debug_assert!(epoch >= 1, "the first live SAL epoch is 1");
        self.write_cursor.set(0);
        self.epoch.set(epoch);
        self.synced.set(0);
        self.refused_transient.set(false);
        unsafe { write_end_prefix(self.log.ring, 0) };
        self.write_anchor(epoch, 0);
        if let Err(e) = posix_io::retry_eintr(|| unsafe { libc::fdatasync(self.fd) }) {
            gnitz_fatal_abort!("SAL fdatasync (rewind to epoch {epoch}) failed: {e}");
        }
    }

    fn write_anchor(&self, epoch: u32, synced: u64) {
        // SAFETY: the anchor page is mapped ahead of the ring, and only this
        // writer writes it.
        let a = unsafe { &mut *self.log.anchor_record() };
        write_u64_le(a, 0, epoch_word(epoch, synced));
        let sum = gnitz_wire::checksum(&a[..8]);
        write_u64_le(a, 8, sum);
    }

    /// Raise the anchored offset to `through`, unless the log was rewound out of
    /// `epoch` since.
    fn mark_synced(&self, epoch: u32, through: u64) {
        if self.epoch.get() == epoch && through > self.synced.get() {
            self.synced.set(through);
            self.write_anchor(epoch, through);
        }
    }

    pub(crate) fn epoch(&self) -> u32 {
        self.epoch.get()
    }
}

/// Sole write access. Dropping it wakes each worker a group written under it
/// reached. Held across no await except a checkpoint round's ACKs.
pub(crate) struct SalExcl<'a> {
    writer: &'a SalWriter,
    _guard: WriteGuard,
}

impl<'a> SalExcl<'a> {
    /// Write and publish one group.
    pub(crate) fn write(&self, g: &DirectGroup) -> Result<(), WireFault> {
        self.writer.write(g)
    }

    /// See [`SalWriter::begin`]. `&mut`, so no scope overlaps another, a rewind
    /// or a [`Self::sync`].
    pub(crate) fn begin(&mut self, lsn: u64, tag: &'static str) -> SalScope<'_> {
        self.writer.begin(lsn, tag)
    }

    /// `fdatasync` the log, fatal on failure; once done, anchor the cursor at
    /// submission as synced. No scope is open, so every byte below it is published.
    pub(crate) fn sync(&self, reactor: &Reactor, op: &'static str) -> impl Future<Output = ()> + 'a {
        let w = self.writer;
        let (epoch, through) = (w.epoch.get(), w.write_cursor.get());
        let done = reactor.fsync(w.fd);
        async move {
            let rc = done.await;
            if rc < 0 {
                gnitz_fatal_abort!("SAL fdatasync ({op}) failed rc={rc}");
            }
            w.mark_synced(epoch, through);
        }
    }

    /// Rewind into the next epoch — the checkpoint's reclaim.
    pub(crate) fn checkpoint_reset(&mut self) {
        self.writer.rewind(next_epoch(self.writer.epoch.get()));
    }

    /// The boot rewind, to the live epoch the recovered tail names.
    pub(crate) fn boot_rewind(&mut self, live_epoch: u32) {
        self.writer.rewind(live_epoch);
    }

    /// Wake every worker a group written since the last wake reached.
    pub(crate) fn wake(&self) {
        let w = self.writer;
        let reached = WorkerSet(w.reached.replace(0)).within(w.wakes.len());
        for worker in reached.iter() {
            w.wakes[worker].wake();
        }
    }
}

impl Drop for SalExcl<'_> {
    fn drop(&mut self) {
        self.wake()
    }
}

/// One publication span on the SAL, holding at most one zone: its `zoned`
/// groups. Nothing is visible until [`Self::commit`].
pub(crate) struct SalScope<'a> {
    writer: &'a SalWriter,
    /// Where the scope opened — the start of its publish range, and where an
    /// uncommitted drop puts the write cursor back.
    from: u64,
    /// The zone LSN every zoned group in the scope is stamped with.
    lsn: u64,
    /// Which scope this is, for [`ZONE_PANIC`]: `"ddl"` or `"commit"`.
    tag: &'static str,
    /// The base of the latest zoned group; `Some` iff a zone is open.
    last_member: Cell<Option<u64>>,
    committed: bool,
}

impl SalScope<'_> {
    /// Lay `g` out inside the scope, unpublished. `zoned` puts it in the
    /// scope's atomic zone at the scope's LSN; otherwise it is written at LSN 0.
    pub(crate) fn write(&self, g: &DirectGroup, zoned: bool) -> Result<(), WireFault> {
        let base = self.writer.lay_out(g, if zoned { self.lsn } else { 0 })?;
        if zoned {
            self.last_member.set(Some(base as u64));
        }
        Ok(())
    }

    /// The point a group laid out in this scope can be rolled back to, taken
    /// before laying it out.
    pub(crate) fn savepoint(&self) -> Savepoint {
        Savepoint {
            cursor: self.writer.write_cursor.get(),
            last_member: self.last_member.get(),
        }
    }

    /// Take back every group laid out since `sp`: nothing was published, so
    /// restoring the cursor and the zone's last member is the whole undo.
    pub(crate) fn roll_back(&self, sp: Savepoint) {
        self.writer.write_cursor.set(sp.cursor);
        self.last_member.set(sp.last_member);
    }

    /// Publish the span in layout order. Answers whether a zone closed.
    pub(crate) fn commit(mut self) -> bool {
        self.committed = true;
        let end = self.writer.write_cursor.get();
        let Some(last) = self.last_member.get() else {
            self.writer.publish_range(self.from, end);
            return false;
        };
        self.writer.mark_zone_end(last);
        self.writer.publish_range(self.from, last);
        if ZONE_PANIC.at(self.tag) {
            // SAFETY: `libc::abort` is the whole reason this block is unsafe; it
            // takes no argument and cannot violate an invariant.
            unsafe { libc::abort() };
        }
        self.writer.publish_range(last, end);
        true
    }
}

impl Drop for SalScope<'_> {
    /// An uncommitted scope publishes nothing: the write cursor goes back to
    /// where it opened and the bytes stay for the next group to overwrite.
    fn drop(&mut self) {
        if !self.committed {
            self.writer.write_cursor.set(self.from);
        }
    }
}

// ---------------------------------------------------------------------------
// SalReader — one worker's live drain over a SalLog
// ---------------------------------------------------------------------------

pub(crate) struct SalReader {
    log: SalLog,
    worker_id: u32,
    /// The live drain's position and epoch — the read-side mirror of
    /// `SalWriter`'s `write_cursor` / `epoch`, so no caller does SAL address
    /// arithmetic.
    read_cursor: Cell<u64>,
    expected_epoch: Cell<u32>,
}

impl SalReader {
    /// A live drain from cursor 0 of `live_epoch`.
    pub(crate) fn new(log: SalLog, worker_id: u32, live_epoch: u32) -> Self {
        SalReader {
            log,
            worker_id,
            read_cursor: Cell::new(0),
            expected_epoch: Cell::new(live_epoch),
        }
    }

    /// The next group carrying a slot for this worker, with that slot's bytes;
    /// groups with none (another worker's unicast) are stepped over. A group
    /// from another epoch parks until [`rewind`](Self::rewind).
    /// A digest failure under a passing epoch gate can only be corruption, and
    /// parking on it would read as end-of-drain, so it fail-stops.
    pub(crate) fn next(&self) -> Option<(SalMessage, &'static [u8])> {
        loop {
            let cursor = self.read_cursor.get();
            match self.log.read_at(cursor, EpochGate::Live(self.expected_epoch.get())) {
                SalStep::Group(msg) => {
                    self.read_cursor.set(msg.end);
                    if let Some(slot) = msg.slot(self.worker_id) {
                        return Some((msg, slot));
                    }
                }
                SalStep::Corrupt => {
                    gnitz_fatal_abort!("SAL group header at offset={cursor} is corrupt — the log is damaged")
                }
                SalStep::Absent => return None,
            }
        }
    }

    /// No group is readable or corrupt at the cursor.
    pub(crate) fn is_empty(&self) -> bool {
        matches!(
            self.log
                .read_at(self.read_cursor.get(), EpochGate::Live(self.expected_epoch.get())),
            SalStep::Absent
        )
    }

    /// Rewind to the start of the next epoch, mirroring
    /// [`SalExcl::checkpoint_reset`].
    pub(crate) fn rewind(&self) {
        self.read_cursor.set(0);
        self.expected_epoch.set(next_epoch(self.expected_epoch.get()));
    }
}

/// The SAL fixture.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(crate) mod fixtures;

#[cfg(test)]
#[path = "tests/sal.rs"]
mod tests;
