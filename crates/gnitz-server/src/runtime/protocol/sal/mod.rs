//! SAL (shared append-only log): master→worker broadcast channel.
//!
//! Owns the mmap layout, group-header write/read helpers, SalWriter,
//! SalMessage, SalLog/SalReader.
//!
//! The **zone protocol** — both directions of it, the commit sentinel a writer
//! emits and the recovery walk a reader decides zone commitment with — lives in
//! the [`zone`] child module.
//!
//! A group's kind byte is this module's alone: callers name a
//! [`SalMessageKind`], and the encode/decode below is the only code entitled to
//! know how it is stored.

pub(crate) mod zone;

use std::cell::Cell;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::runtime::wire::{WireData, WireMsg};
use gnitz_wire::{align8, read_u32_le, read_u64_le, write_u32_le, write_u64_le};
use gnitz_wire::{WireFault, MAX_WORKERS, STATUS_SAL_FULL};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// The publication prefix in front of every group header, one atomically stored
/// u64 (`pack_prefix`); also the alignment of every group base.
const PREFIX_BYTES: usize = std::mem::size_of::<AtomicU64>();

// Group header field offsets, relative to the start of the header, followed by
// one u32 size per slot.

/// Three unsynchronised counters share this field, and their values can collide
/// numerically: a zone LSN, a tick round, and a checkpoint generation. Nothing
/// disambiguates them here — the kind does, so a reader tests the kind before
/// ever comparing an LSN.
const OFF_LSN: usize = 0;
/// The kind's ordinal (`SalMessageKind::as_wire`), one byte.
const OFF_KIND: usize = 8;
/// `1` on the group that opens an atomic zone, else `0`, one byte. The two bytes
/// after it are zero and inside the digest.
const OFF_ZONE_START: usize = 9;
const OFF_TARGET_ID: usize = 12;
const OFF_SLOT_COUNT: usize = 16;
const OFF_EPOCH: usize = 20;
/// The digest's own eight bytes, excluded from the span it covers.
const OFF_DIGEST: usize = 24;
const OFF_DIRECTORY: usize = 32;

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
    OFF_DIRECTORY + align8(slots * 4)
}

// Every group base is `PREFIX_BYTES`-aligned, because `payload_size` is a
// multiple of it and bases start at 0. Lose that and `valid_headers_from`'s
// resync scan steps over a base, and the publication prefix's atomic store
// becomes an unaligned — hence non-atomic, hence UB — store.
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
/// `w` starts where the `align8`-padded slots ahead of it end — so the directory
/// stores sizes alone and an inconsistent offset is unrepresentable.
fn slot_spans(sizes: impl Iterator<Item = u32>) -> impl Iterator<Item = (usize, usize, usize)> {
    let mut off = 0;
    sizes.enumerate().filter_map(move |(w, sz)| {
        let sz = sz as usize;
        let at = off;
        off += align8(sz);
        (sz > 0).then_some((w, at, sz))
    })
}

/// A group's payload bytes: header + directory + `align8`-padded slots — where
/// [`slot_spans`] would leave off. The publication prefix in front is NOT
/// included; [`group_total_size`] is the footprint with it.
fn group_payload_size(hdr_size: usize, sizes: impl Iterator<Item = u32>) -> usize {
    hdr_size + sizes.map(|sz| align8(sz as usize)).sum::<usize>()
}

/// The SAL bytes a group occupies: prefix, header, directory and padded slots —
/// what the write cursor advances by.
fn group_total_size(hdr_size: usize, sizes: impl Iterator<Item = u32>) -> usize {
    PREFIX_BYTES + group_payload_size(hdr_size, sizes)
}

/// A group's directory bytes read back as slot sizes.
fn dir_sizes(dir: &[u8]) -> impl Iterator<Item = u32> + '_ {
    dir.chunks_exact(4).map(|c| read_u32_le(c, 0))
}

/// Which of a group's slots are written, and the request id each answers on.
#[derive(Clone, Copy)]
pub(crate) enum GroupTargets<'a> {
    /// Broadcast; no slot answers. Every slot's request id is 0.
    AllSilent,
    /// Broadcast; slot `w` answers on `ids[w]`. `ids.len()` must be `nw`.
    All(&'a [u64]),
    /// Only `worker`'s slot is written, and it answers on `req_id`.
    One { worker: usize, req_id: u64 },
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
    pub(crate) const NONE: Self = Self::Same(WireData::Whole(None));

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
    /// The header's `lsn` field: a zone LSN, a tick round or a checkpoint
    /// generation by kind; `0` for every other command.
    pub(crate) lsn: u64,
    /// Whether this group belongs to the atomic zone at `lsn`. The writer opens
    /// the zone on the first zoned group it admits and closes it on
    /// [`SalWriter::close_zone`]; callers never say which group is first.
    pub(crate) zoned: bool,
    /// Every slot's message but its `data` and `request_id`, which `msg` fills
    /// from `data`/`targets` — set either of those here and the per-worker fill
    /// overwrites it (debug-asserted in `msg`).
    pub(crate) template: WireMsg<'a>,
    pub(crate) data: GroupData<'a>,
    pub(crate) targets: GroupTargets<'a>,
}

impl<'a> DirectGroup<'a> {
    /// A control-only silent broadcast of `kind` at `lsn = 0`, outside any zone.
    /// Callers override what varies by struct update.
    pub(crate) fn new(kind: SalMessageKind) -> Self {
        DirectGroup {
            kind,
            lsn: 0,
            zoned: false,
            template: WireMsg::default(),
            data: GroupData::NONE,
            targets: GroupTargets::AllSilent,
        }
    }

    /// Worker `w`'s message. The one definition — sizing and encoding both go
    /// through it, so a slot's size and its bytes cannot disagree.
    fn msg(&self, w: usize) -> WireMsg<'a> {
        debug_assert!(
            matches!(self.template.data, WireData::Whole(None)) && self.template.request_id == 0,
            "DirectGroup template must leave `data` and `request_id` to the per-worker fill"
        );
        let data = match self.data {
            GroupData::Same(d) => d,
            GroupData::PerWorker(d) => d[w],
        };
        let keeps_schema = data.row_count() > 0 || self.kind.schema_survives_a_rowless_slot();
        WireMsg {
            data,
            request_id: match self.targets {
                GroupTargets::AllSilent => 0,
                GroupTargets::All(ids) => ids[w],
                GroupTargets::One { req_id, .. } => req_id,
            },
            schema_block: self.template.schema_block.filter(|_| keeps_schema),
            ..self.template
        }
    }

    /// Every slot's size, zero for the ones this group does not write.
    ///
    /// A written slot is never zero-size — every slot carries a control block
    /// whatever else it does — which is what lets [`SalReader::next`] read an
    /// empty slot as "not for this worker". The assert below is where that would
    /// break.
    fn slot_sizes(&self, nw: usize) -> [u32; MAX_WORKERS] {
        let mut sizes = [0u32; MAX_WORKERS];
        for (w, size) in sizes.iter_mut().enumerate().take(nw) {
            if matches!(self.targets, GroupTargets::One { worker, .. } if w != worker) {
                continue;
            }
            *size = self.msg(w).size() as u32;
            debug_assert!(*size > 0, "a written slot always carries at least a control block");
        }
        sizes
    }
}

/// XXH3-64 over a SAL group header — its own eight bytes excluded — seeded with
/// the group's byte offset in the ring, so a header only verifies where it was
/// published. The epoch is not a seed: it sits in the hashed span, so verifying a
/// header also authenticates the generation it claims.
fn group_digest(base: u64, hdr: &[u8]) -> u64 {
    gnitz_store::foundation::xxh::digest_with_hole(&base.to_le_bytes(), hdr, OFF_DIGEST)
}

const SAL_MMAP_SIZE: usize = 1 << 30;

/// The epoch after `epoch`: what a checkpoint moves the writer and every reader
/// to, and what a boot starts at above the recovered walk epoch.
const fn next_epoch(epoch: u32) -> u32 {
    epoch + 1
}

/// The exact SAL footprint of a zone-closing commit sentinel — a slotless group,
/// header only. Every non-sentinel group keeps this much headroom, so the
/// sentinel — written last — always fits: a data group at the boundary is refused
/// gracefully rather than aborting the node. Spelled out rather than through
/// `group_total_size`, which is not `const`.
const SENTINEL_SIZE: usize = PREFIX_BYTES + group_header_size(0);

/// Space held back from ordinary groups so the groups that must not fail always
/// fit: a checkpoint round's `Flush`/`FlushEph` group, and the `Shutdown`
/// broadcast. The band holds two terminal groups plus a sentinel; the test
/// derives that bound from the constants, so a wider control block or
/// `MAX_WORKERS` trips there. Negligible against both the 1 GiB default and the
/// `MIN_SAL_BYTES` floor.
const CHECKPOINT_RESERVE: usize = 64 << 10;

/// Bytes a group of this kind may not spend. A terminal kind (nothing follows it
/// in its epoch) keeps only the sentinel headroom; the sentinel keeps only the
/// checkpoint band; everything else keeps both. Every arm is >= PREFIX_BYTES,
/// which is what keeps a group's terminating prefix inside the mapping.
const fn held_back(kind: SalMessageKind) -> usize {
    match kind {
        SalMessageKind::Shutdown | SalMessageKind::Flush | SalMessageKind::FlushEph => SENTINEL_SIZE,
        SalMessageKind::ZoneCommit => CHECKPOINT_RESERVE,
        // The safe default for a new kind.
        _ => SENTINEL_SIZE + CHECKPOINT_RESERVE,
    }
}

/// The highest byte a group of this kind may occupy. Saturating: the master
/// unit-test fixture maps 4 KiB, below `CHECKPOINT_RESERVE`.
fn effective_max(kind: SalMessageKind, mmap_size: usize) -> usize {
    mmap_size.saturating_sub(held_back(kind))
}

/// Whether a group of a given size can be written, and if not, whether a
/// checkpoint would change that.
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
    /// [`STATUS_SAL_FULL`], which is what a caller matches to tell this refusal
    /// from a real failure; the transient/terminal distinction stays typed and is
    /// not spelled into the text. The write cursor is deliberately absent — this
    /// reaches clients verbatim — and goes to the operator log instead.
    pub(crate) fn refusal(self, kind: SalMessageKind) -> WireFault {
        debug_assert_ne!(self, SalFit::Fits, "a fitting group has no refusal");
        WireFault {
            status: STATUS_SAL_FULL,
            text: format!("SAL full: {kind:?} group did not fit"),
        }
    }
}

/// Floor for a `GNITZ_SAL_BYTES` override — must comfortably exceed one DDL zone
/// plus the checkpoint headroom.
const MIN_SAL_BYTES: usize = 16 << 20;

/// The SAL mmap size in bytes. `SAL_MMAP_SIZE` (1 GiB) is the production default;
/// `GNITZ_SAL_BYTES` overrides it downward. This exists because each server
/// eagerly `fallocate`s the *whole* SAL at startup, so on a shared data_dir
/// filesystem (e.g. a tmpfs `/tmp`) many servers spawning in parallel can
/// exhaust it — the integration test harness spawns dozens at once, so it sets
/// a small value. The size is read once and cached: the master and its `fork()`ed
/// workers inherit both the env and this cache, so they can never disagree on the
/// wrap arithmetic. The override is clamped to `[MIN_SAL_BYTES, SAL_MMAP_SIZE]`.
pub(crate) fn sal_mmap_size() -> usize {
    use std::sync::OnceLock;
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| {
        gnitz_store::foundation::env::env_num("GNITZ_SAL_BYTES", SAL_MMAP_SIZE).clamp(MIN_SAL_BYTES, SAL_MMAP_SIZE)
    })
}

/// The fraction of the mapping the reclaim margin is: `mmap >> 3`, one eighth.
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
        /// stores, stamped with the generation in the header's `lsn`.
        FlushEph = 3,
        /// Catalog mutation.
        DdlSync = 4,
        ExchangeRelay = 5,
        /// Initial full-source scan feeding a newly created view.
        Backfill = 6,
        HasPk = 7,
        /// Read the stored rows for a scattered set of PKs, projected to the
        /// column in `seek_col_idx`.
        Gather = 8,
        /// CREATE UNIQUE INDEX pre-flight: stream the sorted key spans of the
        /// column list in `seek_col_idx` for the master's merge.
        UniquePreflight = 9,
        Push = 10,
        /// Drive one view-maintenance tick.
        Tick = 11,
        SeekByIndex = 12,
        Seek = 13,
        /// A parameterized bounded read (`ReadSpec`).
        ScanSpec = 14,
        /// A `ScanSpec` with a delta bound. Its own kind because the worker's
        /// inline-vs-defer matrix is a function of `(context, kind)` alone.
        DeltaScanSpec = 15,
        /// The zone-closing commit sentinel: a slotless group no worker acts on.
        /// All preceding groups at the same LSN belong to the zone; recovery
        /// applies them only when this reaches disk.
        ZoneCommit = 16,
    }
}

impl SalMessageKind {
    /// Whether a slot of this kind still needs the group's schema block when it
    /// carries no rows. Only a `Push` does not: its rowless slots are the ones a
    /// scatter left empty, and every consumer reaches its own no-op before asking
    /// for a schema. An `ExchangeRelay`'s block *is* the payload of an empty slot
    /// — the worker builds its batch from it and `fatal_shutdown`s without one —
    /// and so is every other kind's, until one says otherwise here.
    fn schema_survives_a_rowless_slot(self) -> bool {
        self != SalMessageKind::Push
    }
}

/// The publication prefix word: `(epoch << 32) | payload_size`.
#[inline]
const fn pack_prefix(epoch: u32, payload_size: usize) -> u64 {
    (epoch as u64) << 32 | payload_size as u64
}

/// The epoch half of a publication prefix word. The other half has no accessor
/// on purpose: the stride comes from the authenticated directory, never from
/// this unauthenticated copy.
#[inline]
const fn prefix_epoch(word: u64) -> u32 {
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
    pub(crate) lsn: u64,
    pub(crate) kind: SalMessageKind,
    /// Whether this group opens an atomic zone. With the closing sentinel it
    /// delimits the zone's byte span, which is how recovery tells damage that
    /// cost a committed transaction a group from damage in one of the `lsn = 0`
    /// command groups between zones.
    pub(crate) zone_start: bool,
    pub(crate) target_id: u32,
    /// The group's byte offset in the ring — what a recovery error names, and
    /// what the zone-span rule tests damage against.
    pub(crate) base: u64,
    /// The group's slot bytes, from slot 0's offset to the end of the group.
    pub(crate) payload: &'static [u8],
    /// The directory: one little-endian `u32` size per slot, which is also what
    /// says how many slots the group has.
    pub(crate) dir: &'static [u8],
}

impl SalMessage {
    /// The slot count the group was written with. Recovery validates a zone's
    /// blocks across all of them, not only the reader's own, so its verdict does
    /// not depend on which worker asked.
    pub(crate) fn slots(&self) -> u32 {
        (self.dir.len() / 4) as u32
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

/// What the bytes at an offset are. `Group` carries the message and the cursor
/// past it.
enum SalStep {
    /// Nothing published here, the group's own stride runs past this mapping
    /// (the log was written under a larger `GNITZ_SAL_BYTES`), or a verified
    /// header stamped with another epoch — the ring's leftovers. Either way, the
    /// end of the log.
    Absent,
    /// A published header at this offset that fails its digest, or names no
    /// kind or no zone-start value.
    Corrupt(u64),
    Group(SalMessage, u64),
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

/// A group header that passed its digest, decoded. What [`SalLog::probe_header`]
/// vouches for and nothing more: the kind and zone-start bytes are still raw,
/// because decoding them can fail and that verdict is [`SalLog::read_at`]'s.
struct GroupHeader {
    lsn: u64,
    kind: u8,
    zone_start: u8,
    target_id: u32,
    epoch: u32,
    /// One little-endian `u32` size per slot, `slots * 4` bytes.
    dir: &'static [u8],
}

/// The SAL mapping, as anything that reads it sees it.
///
/// A non-owning view: the mapping is created in `bootstrap` and inherited across
/// the `fork()`, so this has no `Drop`.
#[derive(Clone, Copy)]
pub(crate) struct SalLog {
    ptr: *const u8,
    mmap_size: usize,
}

impl SalLog {
    /// # Safety
    /// `ptr` must be a valid mmap pointer of at least `mmap_size` bytes, live for
    /// as long as this view and everything read through it.
    pub(crate) unsafe fn new(ptr: *const u8, mmap_size: usize) -> Self {
        SalLog { ptr, mmap_size }
    }

    /// The publication prefix word at `base`, or 0 when nothing was published
    /// there — which is also the answer for a `base` the mapping cannot hold.
    /// Acquire-loaded, so a group whose prefix is visible has its whole header
    /// visible too (the publishing store lands last).
    ///
    /// The **whole word** is the presence test, not its `payload_size` half:
    /// every published group has `epoch >= 1`, so a zero word means "nothing
    /// here", while a small `payload_size` — a commit sentinel's is one set bit —
    /// is single-bit-zeroable and would end a walk before that transaction's
    /// sentinel.
    #[inline]
    fn prefix_word(&self, base: u64) -> u64 {
        if base as usize + PREFIX_BYTES > self.mmap_size {
            return 0;
        }
        unsafe { prefix_atomic(self.ptr, base as usize).load(Ordering::Acquire) }
    }

    /// The digest-verified header at `base`, or `None`. Each bound precedes the
    /// read it guards. The publication prefix is not consulted, so a reset ring
    /// still yields the epoch of its last occupant.
    fn probe_header(&self, base: u64) -> Option<GroupHeader> {
        let hdr_off = base as usize + PREFIX_BYTES;
        let m = self.mmap_size;
        if hdr_off + HDR_PREFIX > m {
            return None;
        }
        // SAFETY: the bound above admits the fixed prefix.
        let prefix: &'static [u8; HDR_PREFIX] = unsafe { &*(self.ptr.add(hdr_off) as *const [u8; HDR_PREFIX]) };
        let slots = read_u32_le(prefix, OFF_SLOT_COUNT) as usize;
        if slots > MAX_WORKERS {
            return None;
        }
        let hdr_len = group_header_size(slots);
        if hdr_off + hdr_len > m {
            return None;
        }
        // SAFETY: bounded directly above.
        let hdr = unsafe { std::slice::from_raw_parts(self.ptr.add(hdr_off), hdr_len) };
        if group_digest(base, hdr) != read_u64_le(hdr, OFF_DIGEST) {
            return None;
        }
        Some(GroupHeader {
            lsn: read_u64_le(prefix, OFF_LSN),
            kind: prefix[OFF_KIND],
            zone_start: prefix[OFF_ZONE_START],
            target_id: read_u32_le(prefix, OFF_TARGET_ID),
            epoch: read_u32_le(prefix, OFF_EPOCH),
            dir: &hdr[OFF_DIRECTORY..OFF_DIRECTORY + slots * 4],
        })
    }

    /// Classify the bytes at `cursor`, building the message and the next cursor
    /// when they are a readable group.
    ///
    /// The caller decides what a `Corrupt` verdict means: the live drain
    /// fail-stops (see [`SalReader::next`]), the recovery walk resyncs past it.
    fn read_at(&self, cursor: u64, gate: EpochGate) -> SalStep {
        let word = self.prefix_word(cursor);
        if word == 0 {
            return SalStep::Absent;
        }
        if let EpochGate::Live(exp) = gate {
            if prefix_epoch(word) != exp {
                return SalStep::Absent;
            }
        }

        let Some(hdr) = self.probe_header(cursor) else {
            return SalStep::Corrupt(cursor);
        };
        if hdr.epoch != gate.epoch() {
            return SalStep::Absent;
        }

        // A digest-verified header naming no kind, or no zone-start value, was
        // written by a build whose header layout differs from this one's;
        // guessing at it is not an option.
        let Some(kind) = SalMessageKind::from_wire(hdr.kind) else {
            return SalStep::Corrupt(cursor);
        };
        let zone_start = match hdr.zone_start {
            0 => false,
            1 => true,
            _ => return SalStep::Corrupt(cursor),
        };

        // The stride comes from the authenticated directory, not from the
        // prefix's `payload_size` copy, so a flipped `payload_size` changes no
        // verdict.
        let hdr_len = group_header_size(hdr.dir.len() / 4);
        let total = group_total_size(hdr_len, dir_sizes(hdr.dir));
        if cursor as usize + total > self.mmap_size {
            return SalStep::Absent;
        }

        // SAFETY: bounded directly above; the payload runs from the header's end
        // to the group's end.
        let payload_off = cursor as usize + PREFIX_BYTES + hdr_len;
        let payload =
            unsafe { std::slice::from_raw_parts(self.ptr.add(payload_off), cursor as usize + total - payload_off) };
        SalStep::Group(
            SalMessage {
                lsn: hdr.lsn,
                kind,
                zone_start,
                target_id: hdr.target_id,
                base: cursor,
                payload,
                dir: hdr.dir,
            },
            (cursor as usize + total) as u64,
        )
    }

    /// Every aligned candidate at or after `from` whose header verifies, as
    /// `(base, epoch)`. The prefix test before the digest keeps the zero run of a
    /// partly-used ring free of hashing.
    fn valid_headers_from(&self, from: u64) -> impl Iterator<Item = (u64, u32)> + '_ {
        (from..self.mmap_size as u64)
            .step_by(PREFIX_BYTES)
            .filter_map(move |base| {
                if self.prefix_word(base) == 0 {
                    return None;
                }
                Some((base, self.probe_header(base)?.epoch))
            })
    }

    /// The epoch a quiescent walk from offset 0 must accept, and the floor the
    /// next boot's writer starts above.
    ///
    /// Group 0's, taken from its digested header: both reset paths rewind the
    /// cursor to 0 in the same call that changes the epoch, so every group such a
    /// walk should read carries it. An all-zero prefix with no header behind it
    /// is the empty ring and answers 0; a reset ring is told apart from it by
    /// that header, which survives the reset. Only a damaged offset 0 sweeps the
    /// ring, and takes the *maximum* epoch — a page-level revert can leave an
    /// intact older header at a low offset, and epochs only rise across resets.
    pub(crate) fn walk_epoch(&self) -> u32 {
        if let Some(hdr) = self.probe_header(0) {
            return hdr.epoch;
        }
        if self.prefix_word(0) == 0 {
            return 0;
        }
        self.valid_headers_from(0).map(|(_, e)| e).max().unwrap_or(0)
    }
}

// ---------------------------------------------------------------------------
// SalWriter
// ---------------------------------------------------------------------------

/// The cursor and zone state a deferred group can be rolled back to. Taken
/// before a group is laid out and consumed only by
/// [`DeferredPublication::roll_back`].
#[derive(Clone, Copy)]
pub(crate) struct Savepoint {
    cursor: u64,
    zone: Option<u64>,
}

pub(crate) struct SalWriter {
    ptr: *mut u8,
    fd: i32,
    mmap_size: usize,
    write_cursor: Cell<u64>,
    epoch: Cell<u32>,
    /// `Some(lsn)` while the atomic zone at `lsn` is open: the first zoned group
    /// admitted opens it, [`Self::close_zone`] closes it. A zoned group written
    /// while it is `None` is the zone's start group.
    zone: Cell<Option<u64>>,
    /// `Some(cursor)` while a [`Self::defer_publication`] scope is open: every
    /// group laid out from `cursor` on is unpublished until the scope ends.
    deferred_from: Cell<Option<u64>>,
    checkpoint_threshold: u64,
    /// A group was refused as [`SalFit::Transient`] since the last reset: a
    /// checkpoint would admit it, so one is warranted whatever the write cursor
    /// says. Cleared by [`Self::rewind`].
    refused_transient: Cell<bool>,
    /// Slots per group: the group header's `slot_count`, the directory's length
    /// and every slot offset are all this. It is the log's framing, so it is the
    /// log writer's own field — never read off some other per-worker resource.
    num_workers: usize,
}

impl SalWriter {
    /// Starts at epoch 0, which [`Self::write_slots`] refuses: nothing can be
    /// written before the boot [`Self::rewind`] sets the live epoch.
    pub(crate) fn new(ptr: *mut u8, fd: i32, mmap_size: usize, num_workers: usize) -> Self {
        let checkpoint_threshold =
            gnitz_store::foundation::env::env_num("GNITZ_CHECKPOINT_BYTES", (mmap_size as u64 * 3) >> 2);
        SalWriter {
            ptr,
            fd,
            mmap_size,
            write_cursor: Cell::new(0),
            epoch: Cell::new(0),
            zone: Cell::new(None),
            deferred_from: Cell::new(None),
            checkpoint_threshold,
            refused_transient: Cell::new(false),
            num_workers,
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

    /// Whether a relay of `need` bytes may be written, refusing also while less
    /// than the reclaim margin is free, so a reclaim lands on a writer with room
    /// to spare.
    pub(crate) fn fit_relay(&self, need: usize) -> SalFit {
        self.fit_within(
            effective_max(SalMessageKind::ExchangeRelay, self.mmap_size),
            need.max(self.reclaim_margin()),
        )
    }

    /// Whether more than the reclaim margin is already written — the opposite
    /// sense to [`Self::below_reclaim_margin`], which asks for that much *free*.
    pub(crate) fn past_reclaim_margin(&self) -> bool {
        self.write_cursor.get() as usize > self.reclaim_margin()
    }

    /// Less than the reclaim margin is free, so a relay-sized group would be
    /// refused — the opposite sense to [`Self::past_reclaim_margin`], which asks
    /// whether that much is already *written*.
    pub(crate) fn below_reclaim_margin(&self) -> bool {
        self.fit_relay(0) != SalFit::Fits
    }

    /// The margin below which the SAL is "running low": one eighth of the
    /// mapping. The single name for it — a relay is refused below it so the
    /// reclaim lands on a writer with room to spare rather than on one that has
    /// run out. Strictly under every kind's cap, so `fit_relay` still answers
    /// [`SalFit::Terminal`] only when `need` itself is too large.
    fn reclaim_margin(&self) -> usize {
        self.mmap_size >> RECLAIM_FRACTION_SHIFT
    }

    /// Lay one group out at the write cursor, `fill(worker, slot)` per non-empty
    /// slot, and publish it unless a [`Self::defer_publication`] scope is open.
    /// A refused group leaves the log untouched. Zone state is [`Self::write`]'s.
    fn write_slots(
        &self,
        target_id: u32,
        lsn: u64,
        kind: SalMessageKind,
        zone_start: bool,
        sizes: &[u32],
        mut fill: impl FnMut(usize, &mut [u8]),
    ) -> Result<(), SalFit> {
        assert!(
            sizes.len() <= MAX_WORKERS,
            "a SAL group cannot carry more than MAX_WORKERS slots"
        );
        let epoch = self.epoch.get();
        assert!(
            epoch >= 1,
            "SAL group epoch must be >= 1 — epoch 0 is indistinguishable from the empty prefix"
        );

        // The header size is a multiple of 8 and every slot contributes
        // align8(sz), so `payload_size` is always a multiple of 8.
        let hdr_size = group_header_size(sizes.len());
        let payload_size = group_payload_size(hdr_size, sizes.iter().copied());
        let total = PREFIX_BYTES + payload_size;
        let fit = self.fit_within(effective_max(kind, self.mmap_size), total);
        if fit != SalFit::Fits {
            // On the writer, not on the deferred-publication scope: the group did
            // not fit whether or not a rolled-back transaction contained it.
            if fit == SalFit::Transient {
                self.refused_transient.set(true);
            }
            gnitz_debug!(
                "SAL group refused as {:?}: cursor={} mmap={} epoch={} need={}",
                fit,
                self.write_cursor.get(),
                self.mmap_size,
                epoch,
                total
            );
            return Err(fit);
        }

        let base = self.write_cursor.get() as usize;
        let hdr_off = base + PREFIX_BYTES;
        // SAFETY: `cursor + total <= mmap_size`, so the whole group is mapped, and
        // nothing else references the span past the write cursor. One
        // `&mut [u8]` over the header, so every field write below is
        // bounds-checked against it rather than argued in prose.
        let hdr = unsafe { std::slice::from_raw_parts_mut(self.ptr.add(hdr_off), hdr_size) };
        write_u64_le(hdr, OFF_LSN, lsn);
        hdr[OFF_KIND..OFF_KIND + 4].copy_from_slice(&[kind.as_wire(), zone_start as u8, 0, 0]);
        write_u32_le(hdr, OFF_TARGET_ID, target_id);
        write_u32_le(hdr, OFF_SLOT_COUNT, sizes.len() as u32);
        write_u32_le(hdr, OFF_EPOCH, epoch);
        // Every entry is written, empty ones included, and the `align8` pad
        // behind them zeroed: `wal.sal` is never truncated, so an unwritten byte
        // inside the digested span would read as whatever group last occupied
        // this offset.
        for (w, &sz) in sizes.iter().enumerate() {
            write_u32_le(hdr, OFF_DIRECTORY + w * 4, sz);
        }
        hdr[OFF_DIRECTORY + sizes.len() * 4..].fill(0);

        for (w, off, sz) in slot_spans(sizes.iter().copied()) {
            // SAFETY: `off + sz <= payload_size - hdr_size`, inside the mapped
            // span, and the spans are disjoint.
            let slot = unsafe { std::slice::from_raw_parts_mut(self.ptr.add(hdr_off + hdr_size + off), sz) };
            fill(w, slot);
        }

        let end = base + total;
        unsafe { write_end_prefix(self.ptr, end) };
        // Stamped before anything can publish the group, so no readable group
        // carries an unstamped digest. Slots start at `hdr_size`, so these are
        // final bytes.
        let digest = group_digest(base as u64, hdr);
        write_u64_le(hdr, OFF_DIGEST, digest);

        self.write_cursor.set(end as u64);
        if self.deferred_from.get().is_none() {
            self.publish(base, pack_prefix(epoch, payload_size));
        }
        Ok(())
    }

    /// The Release store that makes one laid-out group visible.
    fn publish(&self, base: usize, word: u64) {
        unsafe { prefix_atomic(self.ptr, base).store(word, Ordering::Release) };
    }

    /// Open a deferred-publication scope: groups laid out while the guard lives
    /// are invisible and can be rolled back; dropping it (unwinding included)
    /// publishes them in layout order. Hold `sal_writer_excl` and do not suspend
    /// inside the scope.
    pub(crate) fn defer_publication(&self) -> DeferredPublication<'_> {
        debug_assert!(
            self.deferred_from.get().is_none(),
            "a deferred-publication scope is already open"
        );
        self.deferred_from.set(Some(self.write_cursor.get()));
        DeferredPublication { writer: self }
    }

    /// Encode a group's per-worker wire messages directly into the SAL mmap.
    /// Does NOT sync/signal.
    ///
    /// A zoned group opens the zone when none is open — that is the group
    /// recovery reads as the zone's start — and otherwise must carry the open
    /// zone's LSN. A refused group opens nothing.
    pub(crate) fn write(&self, g: &DirectGroup) -> Result<(), SalFit> {
        let nw = self.num_workers;
        if let GroupTargets::All(ids) = g.targets {
            assert_eq!(ids.len(), nw, "req_ids.len()={} != num_workers={}", ids.len(), nw);
        }
        if let GroupData::PerWorker(d) = g.data {
            assert_eq!(d.len(), nw, "worker_data.len()={} != num_workers={}", d.len(), nw);
        }
        debug_assert!(
            g.template.schema_block.is_some() || g.data.is_dataless(),
            "data without a schema — `decode_wire` rejects FLAG_HAS_DATA without FLAG_HAS_SCHEMA",
        );
        let zone_start = g.zoned && self.zone.get().is_none();
        if g.zoned {
            if let Some(open) = self.zone.get() {
                debug_assert_eq!(open, g.lsn, "a zoned group must carry the open zone's LSN");
            }
        }

        let sizes = g.slot_sizes(nw);
        self.write_slots(
            g.template.target_id as u32,
            g.lsn,
            g.kind,
            zone_start,
            &sizes[..nw],
            |w, slot| {
                let written = g.msg(w).encode(slot, 0);
                debug_assert_eq!(written, slot.len());
            },
        )?;
        if g.zoned {
            self.zone.set(Some(g.lsn));
        }
        Ok(())
    }

    /// The exact number of SAL bytes [`Self::write`] will consume for `g`.
    /// Neither the sentinel nor the checkpoint band is included; both are held
    /// back by the fit check.
    pub(crate) fn footprint(&self, g: &DirectGroup) -> usize {
        let nw = self.num_workers;
        group_total_size(group_header_size(nw), g.slot_sizes(nw)[..nw].iter().copied())
    }

    /// Whether a checkpoint is warranted: the write cursor has crossed the
    /// configured threshold, or a group has been refused as
    /// [`SalFit::Transient`] since the last reset — one a checkpoint would admit,
    /// on a log whose cursor may still sit below the threshold.
    pub(crate) fn needs_checkpoint(&self) -> bool {
        self.write_cursor.get() >= self.checkpoint_threshold || self.refused_transient.get()
    }

    /// Rewind to cursor 0 at `epoch` and end the log there, so readers at cursor
    /// 0 see nothing until this epoch writes.
    fn rewind(&self, epoch: u32) {
        debug_assert!(epoch >= 1, "the first live SAL epoch is 1");
        debug_assert!(
            self.deferred_from.get().is_none(),
            "a rewind inside a deferred scope loses its groups"
        );
        debug_assert!(self.zone.get().is_none(), "a rewind inside an open zone loses it");
        self.write_cursor.set(0);
        self.epoch.set(epoch);
        self.refused_transient.set(false);
        unsafe { write_end_prefix(self.ptr, 0) };
    }

    /// Rewind into the next epoch — the checkpoint's reclaim.
    pub(crate) fn checkpoint_reset(&self) {
        self.rewind(next_epoch(self.epoch.get()));
    }

    /// The boot rewind, above the recovered `walk_epoch` — the same epoch
    /// [`SalReader::new`] derives for every worker from that same value.
    pub(crate) fn boot_rewind(&self, walk_epoch: u32) {
        self.rewind(next_epoch(walk_epoch));
    }

    pub(crate) fn epoch(&self) -> u32 {
        self.epoch.get()
    }

    pub(crate) fn sal_fd(&self) -> i32 {
        self.fd
    }
}

/// An open deferred-publication scope; see [`SalWriter::defer_publication`].
pub(crate) struct DeferredPublication<'a> {
    writer: &'a SalWriter,
}

impl DeferredPublication<'_> {
    /// The point a deferred group can be rolled back to, taken before laying it
    /// out.
    pub(crate) fn savepoint(&self) -> Savepoint {
        Savepoint {
            cursor: self.writer.write_cursor.get(),
            zone: self.writer.zone.get(),
        }
    }

    /// Take back every group laid out since `sp`: nothing was published, so
    /// restoring the cursor and the zone state is the whole undo. The bytes stay
    /// where they are and the next group overwrites them.
    pub(crate) fn roll_back(&self, sp: Savepoint) {
        self.writer.write_cursor.set(sp.cursor);
        self.writer.zone.set(sp.zone);
    }
}

impl Drop for DeferredPublication<'_> {
    /// Publish every group laid out inside the scope, in layout order. Each is
    /// re-read off its own digested header rather than remembered: the log
    /// already determines a laid-out group's stride.
    fn drop(&mut self) {
        let w = self.writer;
        let from = w.deferred_from.take().expect("the scope is open while its guard lives");
        let end = w.write_cursor.get();
        let log = unsafe { SalLog::new(w.ptr, w.mmap_size) };
        let mut off = from;
        while off < end {
            let hdr = log
                .probe_header(off)
                .expect("a group laid out in this scope verifies at its own offset");
            let payload = group_payload_size(group_header_size(hdr.dir.len() / 4), dir_sizes(hdr.dir));
            w.publish(off as usize, pack_prefix(hdr.epoch, payload));
            off += (PREFIX_BYTES + payload) as u64;
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
    /// The live drain accepts the epoch above the recovered `walk_epoch` — the
    /// one `SalWriter::boot_rewind` starts the master at from that same value.
    ///
    /// # Safety
    /// `ptr` must be a valid mmap pointer of at least `mmap_size` bytes.
    pub(crate) unsafe fn new(ptr: *const u8, worker_id: u32, mmap_size: usize, walk_epoch: u32) -> Self {
        SalReader {
            log: SalLog::new(ptr, mmap_size),
            worker_id,
            read_cursor: Cell::new(0),
            expected_epoch: Cell::new(next_epoch(walk_epoch)),
        }
    }

    /// The next group carrying a slot for this worker, with that slot's bytes;
    /// groups with none (another worker's unicast, the sentinel) are stepped
    /// over. A group from another epoch parks until [`rewind`](Self::rewind).
    /// A digest failure under a passing epoch gate can only be corruption, and
    /// parking on it would read as end-of-drain, so it fail-stops.
    pub(crate) fn next(&self) -> Option<(SalMessage, &'static [u8])> {
        loop {
            let cursor = self.read_cursor.get();
            match self.log.read_at(cursor, EpochGate::Live(self.expected_epoch.get())) {
                SalStep::Group(msg, new_cursor) => {
                    self.read_cursor.set(new_cursor);
                    if let Some(slot) = msg.slot(self.worker_id) {
                        return Some((msg, slot));
                    }
                }
                SalStep::Corrupt(at) => {
                    gnitz_fatal_abort!("SAL group header failed its digest at offset={at} — the log is damaged")
                }
                SalStep::Absent => return None,
            }
        }
    }

    /// Rewind to the start of the next epoch, mirroring the writer's
    /// `SalWriter::checkpoint_reset` on the read side. Groups the master writes
    /// post-reset (at `write_cursor == 0`, in the bumped epoch) are then
    /// accepted, and any pre-reset group parks.
    pub(crate) fn rewind(&self) {
        self.read_cursor.set(0);
        self.expected_epoch.set(next_epoch(self.expected_epoch.get()));
    }
}

/// The SAL fixture, shared by `sal`'s own suites and by the `runtime` tests
/// that need a real log to drain or to corrupt.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(crate) mod fixtures;

#[cfg(test)]
#[path = "tests/sal.rs"]
mod tests;
