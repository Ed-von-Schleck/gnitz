//! SAL (shared append-only log): master→worker broadcast channel.
//!
//! Owns the mmap layout, group-header write/read helpers, SalWriter,
//! SalMessage, SalLog/SalReader, and the atomic primitives used by the SAL.
//!
//! The **zone protocol** — both directions of it, the commit sentinel a writer
//! emits and the recovery walk a reader decides zone commitment with — lives in
//! the [`zone`] child module. A child, so the walk keeps reading [`SalLog`]'s
//! `read_at` and `valid_headers_from` without either becoming part of this
//! module's surface.
//!
//! A group's flag word is this module's alone: callers name a
//! [`SalMessageKind`] and a [`ZoneMark`], and the encode/decode below is the
//! only code entitled to know the word exists.
//!
//! Unit tests live in `tests/<module>.rs`, attached with `#[path]` to the module
//! they cover, so each stays that module's own `tests` child and reaches its
//! private items.

pub(crate) mod zone;

use std::cell::{Cell, RefCell};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::runtime::m2w::{self, Wake};
use crate::runtime::wire::{WireData, WireMsg};
use gnitz_wire::{align8, read_u32_le, read_u64_le, write_u32_le, write_u64_le};
use gnitz_wire::{WireFault, STATUS_SAL_FULL};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

use gnitz_wire::MAX_WORKERS;

// Group header field offsets, relative to the start of the header, followed by
// one u32 size per slot. The u64 prefix in front of the header is the
// publication marker, `(epoch << 32) | payload_size`; readers take the epoch and
// the stride from the header instead, inside the digested span.

/// Three unsynchronised counters share this field, and their values can collide
/// numerically: a zone LSN, a tick round, and a checkpoint generation. Nothing
/// disambiguates them here — the kind does, which is why `CommittedTail::applies`
/// tests it before ever comparing an LSN.
const OFF_LSN: usize = 0;
const OFF_FLAGS: usize = 8;
const OFF_TARGET_ID: usize = 12;
const OFF_SLOT_COUNT: usize = 16;
const OFF_EPOCH: usize = 20;
/// The digest's own eight bytes, excluded from the span it covers.
const OFF_DIGEST: usize = 24;
const OFF_DIRECTORY: usize = 32;

/// The header's fixed prefix — every scalar field, ending where the directory
/// begins. Read as a `&[u8; HDR_PREFIX]` so the five field reads are provably in
/// bounds at const offsets.
const HDR_PREFIX: usize = OFF_DIRECTORY;

/// Header + directory bytes for `slots` slots.
///
/// Sized by the group's own slot count rather than by `MAX_WORKERS`, which makes
/// a group self-describing: `wal.sal` is never truncated, so a group can land on
/// an offset a wider group used before it, and the recorded count is what tells a
/// reader that slot 5 of a 2-slot group is empty rather than a leftover.
#[inline]
pub(crate) const fn group_header_size(slots: usize) -> usize {
    OFF_DIRECTORY + align8(slots * 4)
}

// Every group base is 8-aligned, because `payload_size` is a multiple of 8 and
// bases start at 0. Lose that and `valid_headers_from`'s `.step_by(8)` resync
// scan steps over every base, and the publication prefix's `atomic_store_u64`
// becomes an unaligned — hence non-atomic, hence UB — store.
const _: () = {
    let mut slots = 0;
    while slots <= MAX_WORKERS {
        assert!(
            group_header_size(slots).is_multiple_of(8),
            "a group header must be a multiple of 8 bytes, or group bases stop being 8-aligned"
        );
        slots += 1;
    }
};

/// Each **written** slot as `(worker, offset-from-header-start, size)`, in
/// directory order. The offset is a pure function of the sizes before it — slot
/// `w` starts where the `align8`-padded slots ahead of it end — so the directory
/// stores sizes alone and an inconsistent offset is unrepresentable.
fn slot_spans(hdr_size: usize, sizes: impl Iterator<Item = u32>) -> impl Iterator<Item = (usize, usize, usize)> {
    let mut off = hdr_size;
    sizes.enumerate().filter_map(move |(w, sz)| {
        let sz = sz as usize;
        let at = off;
        off += align8(sz);
        (sz > 0).then_some((w, at, sz))
    })
}

/// A group's payload bytes: header + directory + `align8`-padded slots — where
/// [`slot_spans`] would leave off. The 8-byte publication prefix in front is NOT
/// included.
///
/// Takes the sizes as an iterator for the same reason [`slot_spans`] does: the
/// writer holds a `&[u32]` and the reader holds directory bytes, and this walk
/// must not be spelled twice.
fn group_payload_size(hdr_size: usize, sizes: impl Iterator<Item = u32>) -> usize {
    hdr_size + sizes.map(|sz| align8(sz as usize)).sum::<usize>()
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

/// One group's worth of per-worker wire messages, as
/// [`SalWriter::write_group_direct`] emits them: the [`WireMsg`] every slot
/// shares, plus what varies by worker.
///
/// [`SalWriter::group_footprint_direct`] sizes the same value, so a caller that
/// checks fit and then writes measures one group, not two.
#[derive(Clone, Copy)]
pub(crate) struct DirectGroup<'a> {
    /// Every slot's message but its `data` and `request_id`, which `msg` fills
    /// from `data`/`targets` — set either of those here and the per-worker fill
    /// overwrites it (debug-asserted in `msg`).
    pub template: WireMsg<'a>,
    pub data: GroupData<'a>,
    pub targets: GroupTargets<'a>,
}

impl<'a> DirectGroup<'a> {
    /// Worker `w`'s message. The one definition — sizing and encoding both go
    /// through it, so a slot's size and its bytes cannot disagree.
    fn msg(&self, w: usize) -> WireMsg<'a> {
        debug_assert!(
            matches!(self.template.data, WireData::Whole(None)) && self.template.request_id == 0,
            "DirectGroup template must leave `data` and `request_id` to the per-worker fill"
        );
        WireMsg {
            data: match self.data {
                GroupData::Same(d) => d,
                GroupData::PerWorker(d) => d[w],
            },
            request_id: match self.targets {
                GroupTargets::AllSilent => 0,
                GroupTargets::All(ids) => ids[w],
                GroupTargets::One { req_id, .. } => req_id,
            },
            ..self.template
        }
    }

    /// Every slot's size, zero for the ones this group does not write.
    ///
    /// A written slot is never zero-size — every slot carries a control block
    /// whatever else it does — which is what lets worker dispatch read an empty
    /// slot as "not for us". The assert below is where that would break.
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

/// The exact SAL footprint of a zone-closing commit sentinel — a slotless group,
/// header only. `SalWriter::begin` reserves this much headroom for every
/// non-sentinel group, so the sentinel — written last — always fits: a data
/// group at the boundary is refused gracefully rather than aborting the node.
pub(crate) const SENTINEL_SIZE: usize = 8 + group_header_size(0);

/// Space held back from ordinary groups so the groups that must not fail always
/// fit: a checkpoint round's `Flush`/`FlushEph` group, and the `Shutdown`
/// broadcast (whose error `shutdown_workers` discards before blocking in
/// `waitpid`). The band holds **two** of them: the watchdog's crash arm
/// broadcasts `Shutdown` without the SAL mutex exactly while a committer flush
/// round is parked awaiting the dead worker's ACK.
/// `checkpoint_reserve_holds_two_terminal_groups` derives that bound from the
/// constants, so a wider control block or `MAX_WORKERS` trips there. Negligible
/// against both the 1 GiB default and the `MIN_SAL_BYTES` floor.
pub(crate) const CHECKPOINT_RESERVE: usize = 64 << 10;

/// The highest byte a group of this kind and framing may occupy.
///
/// The terminal kinds — the checkpoint rounds and the shutdown broadcast — may
/// spend the `CHECKPOINT_RESERVE` that exists for them, but not the sentinel
/// headroom; nothing follows them in the epoch. A commit sentinel may spend the
/// sentinel headroom but not the reserve. Every other group must leave both,
/// which keeps the bound structural: an ordinary group always leaves room for one
/// sentinel, and `emit_zone_to_sal` writes no bare sentinel, so no run of
/// sentinels can reach the reserve. Capping the sentinel below `mmap_size` also
/// stops one that ends exactly at the mapping's end from skipping its own
/// terminating prefix.
pub(crate) fn effective_max(kind: SalMessageKind, mark: ZoneMark, mmap_size: usize) -> usize {
    use SalMessageKind::{Flush, FlushEph, Shutdown};
    if matches!(kind, Shutdown | Flush | FlushEph) {
        mmap_size.saturating_sub(SENTINEL_SIZE)
    } else if mark == ZoneMark::Commit {
        mmap_size.saturating_sub(CHECKPOINT_RESERVE)
    } else {
        mmap_size.saturating_sub(SENTINEL_SIZE + CHECKPOINT_RESERVE)
    }
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
    /// The client-facing refusal for a group of `what` that did not fit. Carries
    /// [`STATUS_SAL_FULL`], which is what a caller matches to tell this refusal
    /// from a real failure; the transient/terminal distinction stays typed and is
    /// not spelled into the text. The write cursor is deliberately absent — this
    /// reaches clients verbatim — and goes to the operator log instead.
    pub(crate) fn refusal(self, what: &str) -> WireFault {
        debug_assert_ne!(self, SalFit::Fits, "a fitting group has no refusal");
        WireFault {
            status: STATUS_SAL_FULL,
            text: format!("SAL full: {what} did not fit"),
        }
    }
}

/// Floor for a `GNITZ_SAL_BYTES` override — must comfortably exceed one DDL zone
/// plus the checkpoint headroom.
pub(crate) const MIN_SAL_BYTES: usize = 16 << 20;

/// The SAL mmap size in bytes. `SAL_MMAP_SIZE` (1 GiB) is the production default;
/// `GNITZ_SAL_BYTES` overrides it downward. This exists because each server
/// eagerly `fallocate`s the *whole* SAL at startup, so on a shared data_dir
/// filesystem (e.g. a tmpfs `/tmp`) many servers spawning in parallel can
/// exhaust it — the integration test harness spawns dozens at once, so it sets
/// a small value. The size is read once and cached: the master and its `fork()`ed
/// workers inherit both the env and this cache, so they can never disagree on the
/// wrap arithmetic. The override is clamped to `[MIN_SAL_BYTES, SAL_MMAP_SIZE]`.
///
/// Keep the value consistent across restarts on a given data_dir: recovery walks
/// only the first `sal_mmap_size()` bytes of the SAL file, so restarting with a
/// smaller size after a crash could skip committed-but-unflushed groups written
/// past the new bound (do a clean shutdown/checkpoint before shrinking).
pub fn sal_mmap_size() -> usize {
    use std::sync::OnceLock;
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| {
        gnitz_store::foundation::env::env_num("GNITZ_SAL_BYTES", SAL_MMAP_SIZE).clamp(MIN_SAL_BYTES, SAL_MMAP_SIZE)
    })
}

/// The fraction of the mapping the reclaim margin is: `mmap >> 3`, one eighth.
/// The single name for "the SAL is running low" — a relay is refused below it so
/// the reclaim lands on a writer with room to spare rather than on one that has
/// run out.
const RECLAIM_FRACTION_SHIFT: u32 = 3;

// ---------------------------------------------------------------------------
// Group kinds
//
// Every SAL group names exactly one kind, optionally with a framing mark. The
// kind is an argument at the write boundary, so two kind bits are
// unrepresentable rather than tie-broken by a table's order, and the decode
// below is total and order-free.
// ---------------------------------------------------------------------------

// The engine's own group-header bits. They sit strictly above `gnitz_wire`'s
// shared 0-15 block — that separation is what lets the two crates allocate
// independently — and are private here, because after the flag word stops
// leaving this module nothing outside it can name one.
const BIT_GATHER: u32 = 1 << 16;
const BIT_UNIQUE_PREFLIGHT: u32 = 1 << 17;
const BIT_ZONE_START: u32 = 1 << 18;
const BIT_FLUSH_EPH: u32 = 1 << 19;
const BIT_DELTA_SCAN: u32 = 1 << 20;

/// What a SAL group asks a worker to do. Mutually exclusive by construction:
/// the writer names one, and the reader decodes the word back to it.
///
/// `Scan` names no bit — a group with no kind bit set is a full table scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SalMessageKind {
    Shutdown,
    /// Base round of a checkpoint: flush base and system tables.
    Flush,
    /// Ephemeral-state flush round of the checkpoint sequence: flush every
    /// view's operator-trace tables and output stores (traces before outputs),
    /// stamping their manifests with the checkpoint generation carried in the
    /// group header's `lsn` field. Dispatched inline in both worker contexts
    /// like `Flush`, but distinct so the base round (`SalReplay` user tables)
    /// and the ephemeral round (`Rederive` view state) stay separate handlers.
    FlushEph,
    /// Catalog mutation. The commit sentinel rides this kind with
    /// [`ZoneMark::Commit`]: the worker's DDL_SYNC branch already no-ops on a
    /// group with no batch.
    DdlSync,
    ExchangeRelay,
    /// Initial full-source scan feeding a newly created view.
    Backfill,
    HasPk,
    /// Batched stored-row gather: scatter a set of PKs to their owning workers,
    /// each worker reads the committed rows for the PKs it owns and replies with
    /// the rows projected to the single column index carried in the control
    /// block's `seek_col_idx`. Distinct from `Seek` (single key) and `HasPk`
    /// (existence echo of the caller's payload); the gather returns the *stored*
    /// value of a column the caller does not have.
    Gather,
    /// CREATE UNIQUE INDEX global pre-flight: each worker projects its committed
    /// partition of `target_id` to the OPK leading-key spans of the column list
    /// packed in `seek_col_idx`, sorts them, and streams the SORTED spans back as
    /// continuation frames for the master's k-way merge (see
    /// `validate_unique_index_create`). Unicast-shaped like a Scan: every worker
    /// gets its own req_id slot and answers with a frame train.
    UniquePreflight,
    Push,
    /// Drive one view-maintenance tick.
    Tick,
    SeekByIndex,
    Seek,
    /// A parameterized bounded read (`ReadSpec`). `delta` marks one whose bound
    /// is a delta bound — a modifier the master stamps after peeking the bound
    /// once (`peek_delta_bound`), not a separate verb: the client still sends
    /// `FLAG_SCAN_SPEC`.
    ///
    /// It is part of the kind because the worker's inline-vs-defer matrix is a
    /// function of `(context, kind)` and nothing else.
    ScanSpec {
        delta: bool,
    },
    Scan,
}

/// The kind's bits in a group's flag word.
const fn kind_bit(kind: SalMessageKind) -> u32 {
    match kind {
        SalMessageKind::Shutdown => gnitz_wire::FLAG_SHUTDOWN as u32,
        SalMessageKind::Flush => gnitz_wire::FLAG_FLUSH as u32,
        SalMessageKind::FlushEph => BIT_FLUSH_EPH,
        SalMessageKind::DdlSync => gnitz_wire::FLAG_DDL_SYNC as u32,
        SalMessageKind::ExchangeRelay => gnitz_wire::FLAG_EXCHANGE_RELAY as u32,
        SalMessageKind::Backfill => gnitz_wire::FLAG_BACKFILL as u32,
        SalMessageKind::HasPk => gnitz_wire::FLAG_HAS_PK as u32,
        SalMessageKind::Gather => BIT_GATHER,
        SalMessageKind::UniquePreflight => BIT_UNIQUE_PREFLIGHT,
        SalMessageKind::Push => gnitz_wire::FLAG_PUSH as u32,
        SalMessageKind::Tick => gnitz_wire::FLAG_TICK as u32,
        SalMessageKind::SeekByIndex => gnitz_wire::FLAG_SEEK_BY_INDEX as u32,
        SalMessageKind::Seek => gnitz_wire::FLAG_SEEK as u32,
        SalMessageKind::ScanSpec { delta } => {
            (gnitz_wire::FLAG_SCAN_SPEC as u32) | if delta { BIT_DELTA_SCAN } else { 0 }
        }
        SalMessageKind::Scan => 0,
    }
}

impl SalMessageKind {
    /// Every kind, so the mask below and the decode cannot fall behind the enum.
    const ALL: [SalMessageKind; 15] = [
        SalMessageKind::Shutdown,
        SalMessageKind::Flush,
        SalMessageKind::FlushEph,
        SalMessageKind::DdlSync,
        SalMessageKind::ExchangeRelay,
        SalMessageKind::Backfill,
        SalMessageKind::HasPk,
        SalMessageKind::Gather,
        SalMessageKind::UniquePreflight,
        SalMessageKind::Push,
        SalMessageKind::Tick,
        SalMessageKind::SeekByIndex,
        SalMessageKind::Seek,
        SalMessageKind::ScanSpec { delta: false },
        SalMessageKind::ScanSpec { delta: true },
        // `Scan` is the zero pattern and is the decode's fallthrough; listing it
        // here would fold nothing into the mask.
    ];

    /// The kind `bits` names, or `None` when they name none — unreachable for a
    /// header this build wrote, since the write boundary takes a kind.
    const fn from_wire(bits: u32) -> Option<SalMessageKind> {
        if bits == 0 {
            return Some(SalMessageKind::Scan);
        }
        let mut i = 0;
        while i < Self::ALL.len() {
            if kind_bit(Self::ALL[i]) == bits {
                return Some(Self::ALL[i]);
            }
            i += 1;
        }
        None
    }
}

/// The union of every kind's bits — folded from [`SalMessageKind::ALL`] rather
/// than written out, so it cannot fall behind the enum.
const KIND_MASK: u32 = {
    let mut acc = 0u32;
    let mut i = 0;
    while i < SalMessageKind::ALL.len() {
        acc |= kind_bit(SalMessageKind::ALL[i]);
        i += 1;
    }
    acc
};

/// Where a group sits in an atomic zone. Framing, not dispatch: no worker arm
/// branches on it, and recovery is its only reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ZoneMark {
    /// Not a zone boundary.
    Plain,
    /// The zone's first group. With the closing sentinel it delimits the zone's
    /// byte span, which is how recovery tells damage that cost a committed
    /// transaction a group from damage in one of the `lsn = 0` command groups
    /// between zones.
    Start,
    /// The zone's closing commit sentinel. All preceding groups at the same LSN
    /// belong to the zone; recovery applies them only when this reaches disk.
    Commit,
}

impl ZoneMark {
    const fn bits(self) -> u32 {
        match self {
            ZoneMark::Plain => 0,
            ZoneMark::Start => BIT_ZONE_START,
            ZoneMark::Commit => gnitz_wire::FLAG_TXN_COMMIT as u32,
        }
    }
}

/// The union of the framing bits.
const FRAMING_MASK: u32 = ZoneMark::Start.bits() | ZoneMark::Commit.bits();

// The flag word holds two disjoint namespaces, and `read_at`'s decode rests on
// both. `gnitz_wire`'s own guard covers the co-allocated 0-15 block; this covers
// the engine's side of the line.
const _: () = {
    assert!(KIND_MASK & FRAMING_MASK == 0, "a kind bit collides with a framing bit");
    assert!(
        ZoneMark::Start.bits() & ZoneMark::Commit.bits() == 0,
        "the two framing marks share a bit"
    );

    // The engine's own bits stay clear of the block `gnitz_wire` allocates. A
    // collision *among* them needs no check of its own: it surfaces either as two
    // kinds encoding alike or as a kind bit landing on a framing bit, both
    // asserted here.
    assert!(
        (BIT_GATHER | BIT_UNIQUE_PREFLIGHT | BIT_ZONE_START | BIT_FLUSH_EPH | BIT_DELTA_SCAN)
            & gnitz_wire::SAL_FLAGS_MASK as u32
            == 0,
        "an engine group-header bit collides with the wire-allocated 0-15 block"
    );

    // Distinct, not merely disjoint: `ScanSpec { delta: true }` is a superset of
    // `ScanSpec { delta: false }` by construction, and distinctness is what makes
    // `from_wire` well-defined.
    let mut i = 0;
    while i < SalMessageKind::ALL.len() {
        assert!(
            kind_bit(SalMessageKind::ALL[i]) != 0,
            "only `Scan` may name no bit, and it is not in ALL"
        );
        let mut j = i + 1;
        while j < SalMessageKind::ALL.len() {
            assert!(
                kind_bit(SalMessageKind::ALL[i]) != kind_bit(SalMessageKind::ALL[j]),
                "two kinds encode to the same bits"
            );
            j += 1;
        }
        i += 1;
    }
};

// ---------------------------------------------------------------------------
// Atomics (acquire/release for cross-process shared memory)
// ---------------------------------------------------------------------------

/// Atomic load with Acquire ordering from a raw pointer.
///
/// # Safety
/// `ptr` must point to a naturally-aligned u64 in shared memory.
pub(crate) unsafe fn atomic_load_u64(ptr: *const u8) -> u64 {
    let atomic = &*(ptr as *const AtomicU64);
    atomic.load(Ordering::Acquire)
}

/// Atomic store with Release ordering to a raw pointer.
///
/// # Safety
/// `ptr` must point to a naturally-aligned u64 in shared memory.
unsafe fn atomic_store_u64(ptr: *mut u8, val: u64) {
    let atomic = &*(ptr as *const AtomicU64);
    atomic.store(val, Ordering::Release);
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

/// Zero the eight bytes that terminate the log at `offset` — the "nothing
/// published here" word every walk and every live drain stops on. Always in
/// bounds: `effective_max` leaves every group at least `SENTINEL_SIZE` (40) of
/// mapping behind it.
#[inline]
unsafe fn write_end_prefix(sal_ptr: *mut u8, offset: usize) {
    atomic_store_u64(sal_ptr.add(offset), 0);
}

// ---------------------------------------------------------------------------
// SAL write (master→workers)
// ---------------------------------------------------------------------------

/// Handle returned by [`SalWriter::begin`]. Header and per-worker directory are
/// already written; caller fills per-worker data, then hands it back to
/// `SalWriter::finish`.
#[must_use = "SalGroup must be passed to finish(); dropping it leaves the log's end prefix unwritten"]
pub(crate) struct SalGroup<'a> {
    sal_ptr: *mut u8,
    /// Offset of the group's 8-byte size prefix; the header follows it.
    base: usize,
    /// The slot sizes this group was laid out from. Held rather than re-passed
    /// so the slot walk reads the directory it wrote: a caller handing over a
    /// different slice would hand out a `&mut [u8]` at the wrong offset inside a
    /// live mmap, with nothing to catch it.
    sizes: &'a [u32],
    epoch: u32,
    laid_out: bool,
}

impl Drop for SalGroup<'_> {
    fn drop(&mut self) {
        debug_assert!(
            self.laid_out,
            "SalGroup dropped without finish — the log's end prefix was never written"
        );
    }
}

impl SalGroup<'_> {
    #[inline]
    fn hdr_size(&self) -> usize {
        group_header_size(self.sizes.len())
    }

    #[inline]
    fn payload_size(&self) -> usize {
        group_payload_size(self.hdr_size(), self.sizes.iter().copied())
    }

    #[inline]
    unsafe fn data_ptr(&self, offset: usize) -> *mut u8 {
        self.sal_ptr.add(self.base + 8 + offset)
    }

    /// Hand every non-empty worker slot to `f` as a mutable byte slice, in the
    /// `align8` directory order [`SalWriter::begin`] laid out.
    ///
    /// # Safety
    /// The slots must be unaliased for the duration — nothing else may hold a
    /// reference into this group's payload span.
    pub(crate) unsafe fn for_each_slot(&self, mut f: impl FnMut(usize, &mut [u8])) {
        for (w, off, sz) in slot_spans(self.hdr_size(), self.sizes.iter().copied()) {
            f(w, std::slice::from_raw_parts_mut(self.data_ptr(off), sz));
        }
    }

    /// Finalise the group: terminate the log behind it and stamp its digest.
    /// Returns the cursor past it and the publication `(base, word)` still owed —
    /// until that Release store lands the group is invisible, because this base
    /// still holds the zero the group before it wrote.
    ///
    /// # Safety
    /// Nothing else may hold a reference into this group's span.
    unsafe fn lay_out(mut self) -> (u64, (usize, u64)) {
        let payload_size = self.payload_size();
        let end = self.base + 8 + payload_size;
        write_end_prefix(self.sal_ptr, end);
        // Stamped before anything can publish the group, so no readable group
        // carries an unstamped digest. Nothing writes into the header between
        // `begin` and here — slots start at `off >= hdr_size` — so these are
        // final bytes.
        let hdr_off = self.base + 8;
        let hdr = std::slice::from_raw_parts_mut(self.sal_ptr.add(hdr_off), self.hdr_size());
        let digest = group_digest(self.base as u64, hdr);
        write_u64_le(hdr, OFF_DIGEST, digest);
        self.laid_out = true;
        (end as u64, (self.base, pack_prefix(self.epoch, payload_size)))
    }
}

// ---------------------------------------------------------------------------
// SAL read
// ---------------------------------------------------------------------------

/// One SAL group as a reader sees it. Slot resolution is
/// [`SalMessage::slot`]'s: the seeded digest means a header is valid only where
/// it was published, so the bytes and the header that describes them are one
/// value and no slot read costs a second digest.
pub struct SalMessage {
    pub lsn: u64,
    pub kind: SalMessageKind,
    /// The zone's first group ([`ZoneMark::Start`]).
    pub zone_start: bool,
    /// The zone's closing commit sentinel ([`ZoneMark::Commit`]).
    pub txn_commit: bool,
    pub target_id: u32,
    /// The group's byte offset in the ring — what a recovery error names, and
    /// what the zone-span rule tests damage against.
    pub base: u64,
    /// The group's slot bytes, from slot 0's offset to the end of the group.
    pub payload: &'static [u8],
    /// The directory: one little-endian `u32` size per slot, which is also what
    /// says how many slots the group has.
    pub dir: &'static [u8],
}

impl SalMessage {
    /// The slot count the group was written with. Recovery validates a zone's
    /// blocks across all of them, not only the reader's own, so its verdict does
    /// not depend on which worker asked.
    pub fn slots(&self) -> u32 {
        (self.dir.len() / 4) as u32
    }

    /// Slot `w`'s bytes, or `None` when this group did not write it (`w` at or
    /// past the slot count included).
    ///
    /// A pure function of the message: no log, no re-read, and no re-digest.
    pub fn slot(&self, w: u32) -> Option<&'static [u8]> {
        self.slots_written().find(|&(i, _)| i == w).map(|(_, b)| b)
    }

    /// Every written slot as `(worker, bytes)`, in directory order.
    pub fn slots_written(&self) -> impl Iterator<Item = (u32, &'static [u8])> + '_ {
        let payload = self.payload;
        let hdr = group_header_size(self.slots() as usize);
        slot_spans(hdr, dir_sizes(self.dir)).map(move |(w, off, sz)| {
            // `off` is measured from the header start; the payload begins at the
            // header's end, and both sides derive that from the same directory.
            (w as u32, &payload[off - hdr..off - hdr + sz])
        })
    }
}

impl Default for SalMessage {
    fn default() -> Self {
        SalMessage {
            lsn: 0,
            kind: SalMessageKind::Scan,
            zone_start: false,
            txn_commit: false,
            target_id: 0,
            base: 0,
            payload: &[],
            dir: &[],
        }
    }
}

/// What the bytes at an offset are. `Group` carries the message and the cursor
/// past it.
pub enum SalStep {
    /// Nothing published here, the group's own stride runs past this mapping
    /// (the log was written under a larger `GNITZ_SAL_BYTES`), or a verified
    /// header stamped with another epoch — the ring's leftovers. Either way, the
    /// end of the log.
    Absent,
    /// A published header that fails its digest, or names no kind.
    Corrupt,
    Group(SalMessage, u64),
}

/// How a reader treats the epoch a group is stamped with.
#[derive(Clone, Copy)]
pub(crate) enum EpochGate {
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

/// The SAL mapping, as anything that reads it sees it.
///
/// A non-owning view: the mapping is created in `bootstrap` and inherited across
/// the `fork()`, so this has no `Drop`.
#[derive(Clone, Copy)]
pub(crate) struct SalLog {
    ptr: *const u8,
    mmap_size: u64,
}

impl SalLog {
    /// # Safety
    /// `ptr` must be a valid mmap pointer of at least `mmap_size` bytes, live for
    /// as long as this view and everything read through it.
    pub(crate) unsafe fn new(ptr: *const u8, mmap_size: usize) -> Self {
        SalLog {
            ptr,
            mmap_size: mmap_size as u64,
        }
    }

    /// The publication prefix word at `base`, or 0 when nothing was published
    /// there — which is also the answer for a `base` the mapping cannot hold.
    /// Acquire-loaded, so a group whose prefix is visible has its whole header
    /// visible too (`lay_out`'s store lands last).
    ///
    /// The **whole word** is the presence test, not its `payload_size` half:
    /// every published group has `epoch >= 1`, so a zero word means "nothing
    /// here", while a small `payload_size` — a commit sentinel's is one set bit —
    /// is single-bit-zeroable and would end a walk before that transaction's
    /// sentinel.
    #[inline]
    fn prefix_word(&self, base: u64) -> u64 {
        if base + 8 > self.mmap_size {
            return 0;
        }
        unsafe { atomic_load_u64(self.ptr.add(base as usize)) }
    }

    /// The verified header at `base`, split into its fixed prefix and its
    /// directory (`slots * 4` bytes), or `None`. Each bound below precedes the
    /// read it guards.
    ///
    /// Split here so callers read fields off a fixed-size array at const offsets
    /// and walk the directory over an exactly-sized sub-slice — one bounds check
    /// for the header, none in the loop.
    ///
    /// The publication prefix is not consulted, so a reset ring still yields the
    /// epoch of its last occupant and a resyncing walk can probe any candidate.
    fn probe_header(&self, base: u64) -> Option<(&'static [u8; HDR_PREFIX], &'static [u8])> {
        let b = base as usize;
        let m = self.mmap_size as usize;
        if b + 8 + group_header_size(0) > m {
            return None;
        }
        let hdr_off = b + 8;
        // SAFETY: the bound above admits the fixed prefix, which is exactly
        // `group_header_size(0)` bytes.
        let prefix: &'static [u8; HDR_PREFIX] = unsafe { &*(self.ptr.add(hdr_off) as *const [u8; HDR_PREFIX]) };
        let slots = read_u32_le(prefix, OFF_SLOT_COUNT) as usize;
        if slots > MAX_WORKERS {
            return None;
        }
        let hdr_len = group_header_size(slots);
        if b + 8 + hdr_len > m {
            return None;
        }
        // SAFETY: bounded directly above.
        let hdr = unsafe { std::slice::from_raw_parts(self.ptr.add(hdr_off), hdr_len) };
        if group_digest(base, hdr) != read_u64_le(hdr, OFF_DIGEST) {
            return None;
        }
        Some((prefix, &hdr[OFF_DIRECTORY..OFF_DIRECTORY + slots * 4]))
    }

    /// Classify the bytes at `cursor`, building the message and the next cursor
    /// when they are a readable group.
    ///
    /// The caller decides what a `Corrupt` verdict means: the live drain
    /// fail-stops (see [`SalReader::next`]), the recovery walk resyncs past it.
    pub(crate) fn read_at(&self, cursor: u64, gate: EpochGate) -> SalStep {
        let word = self.prefix_word(cursor);
        if word == 0 {
            return SalStep::Absent;
        }
        if let EpochGate::Live(exp) = gate {
            if prefix_epoch(word) != exp {
                return SalStep::Absent;
            }
        }

        let Some((prefix, dir)) = self.probe_header(cursor) else {
            return SalStep::Corrupt;
        };
        if read_u32_le(prefix, OFF_EPOCH) != gate.epoch() {
            return SalStep::Absent;
        }

        let flags = read_u32_le(prefix, OFF_FLAGS);
        // A digest-verified header naming no kind was written by a build whose
        // kind set differs from this one's; guessing at it is not an option.
        let Some(kind) = SalMessageKind::from_wire(flags & KIND_MASK) else {
            return SalStep::Corrupt;
        };

        // The stride comes from the authenticated directory, not from the
        // prefix's `payload_size` copy, so a flipped `payload_size` changes no
        // verdict.
        let hdr_len = group_header_size(dir.len() / 4);
        let stride = group_payload_size(hdr_len, dir_sizes(dir));
        if cursor + 8 + stride as u64 > self.mmap_size {
            return SalStep::Absent;
        }

        // SAFETY: bounded directly above; the payload runs from the header's end
        // to the group's end.
        let payload =
            unsafe { std::slice::from_raw_parts(self.ptr.add(cursor as usize + 8 + hdr_len), stride - hdr_len) };
        SalStep::Group(
            SalMessage {
                lsn: read_u64_le(prefix, OFF_LSN),
                kind,
                zone_start: flags & ZoneMark::Start.bits() != 0,
                txn_commit: flags & ZoneMark::Commit.bits() != 0,
                target_id: read_u32_le(prefix, OFF_TARGET_ID),
                base: cursor,
                payload,
                dir,
            },
            cursor + (8 + stride) as u64,
        )
    }

    /// Every 8-byte-aligned candidate at or after `from` whose header verifies,
    /// as `(base, epoch)`. Group bases are 8-aligned — `payload_size` is always a
    /// multiple of 8 and bases start at 0 — so the stride cannot step over one.
    /// Testing the prefix word before the digest is what bounds the cost: the
    /// zero run filling a partly-used ring pays no hashing at all.
    pub(crate) fn valid_headers_from(&self, from: u64) -> impl Iterator<Item = (u64, u32)> + '_ {
        (from..self.mmap_size).step_by(8).filter_map(move |base| {
            if self.prefix_word(base) == 0 {
                return None;
            }
            let (prefix, _) = self.probe_header(base)?;
            Some((base, read_u32_le(prefix, OFF_EPOCH)))
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
        if let Some((prefix, _)) = self.probe_header(0) {
            return read_u32_le(prefix, OFF_EPOCH);
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

/// The cursor and pending-publication state a deferred group can be rolled back
/// to. Taken before a group is laid out and consumed only by
/// [`SalWriter::roll_back`].
#[derive(Clone, Copy)]
pub(crate) struct Savepoint {
    cursor: u64,
    pending: usize,
}

pub struct SalWriter {
    ptr: *mut u8,
    fd: i32,
    mmap_size: u64,
    write_cursor: Cell<u64>,
    epoch: Cell<u32>,
    /// Groups laid out but not published, in layout order. Non-empty only inside
    /// a [`Self::defer_publication`] scope, which its one holder runs to
    /// completion with no suspension point inside it — so this is ordinary local
    /// state, not shared state.
    pending: RefCell<Vec<(usize, u64)>>,
    deferring: Cell<bool>,
    checkpoint_threshold: u64,
    /// Slots per group: the group header's `slot_count`, the directory's length
    /// and every slot offset are all this. It is the log's framing, so it is the
    /// log writer's own field — never read off some other per-worker resource.
    num_workers: usize,
}

impl SalWriter {
    /// The writer starts at epoch 0, which [`Self::begin`] refuses, so it cannot
    /// write until the boot [`Self::rewind`] sets the live epoch. Taking that
    /// epoch here instead would make the window writable, and such a group would
    /// land at cursor 0, be consumed by the workers, and then be overwritten by
    /// the rewind.
    pub fn new(ptr: *mut u8, fd: i32, mmap_size: u64, num_workers: usize) -> Self {
        let checkpoint_threshold =
            gnitz_store::foundation::env::env_num("GNITZ_CHECKPOINT_BYTES", (mmap_size * 3) >> 2);
        SalWriter {
            ptr,
            fd,
            mmap_size,
            write_cursor: Cell::new(0),
            epoch: Cell::new(0),
            pending: RefCell::new(Vec::new()),
            deferring: Cell::new(false),
            checkpoint_threshold,
            num_workers,
        }
    }

    /// SAL bytes an ordinary group may occupy: the mapping minus the headroom
    /// reserved for the zone-closing sentinel and minus the band held back for
    /// the checkpoint and shutdown groups.
    fn effective_capacity(&self) -> usize {
        effective_max(SalMessageKind::Scan, ZoneMark::Plain, self.mmap_size as usize)
    }

    /// Classify `need` bytes against `cap` and the live cursor: past the cap
    /// outright no checkpoint can help, past what is left of it one can. The one
    /// spelling of the rule — [`Self::begin`] admits a group by it, and
    /// [`Self::fit`] answers for a caller that has not written one yet.
    fn fit_within(&self, cap: usize, need: usize) -> SalFit {
        if need > cap {
            SalFit::Terminal
        } else if need > cap.saturating_sub(self.write_cursor.get() as usize) {
            SalFit::Transient
        } else {
            SalFit::Fits
        }
    }

    /// [`Self::fit_within`] against the space an ordinary group may occupy — the
    /// verdict for a caller sizing a group before it builds one.
    pub(crate) fn fit(&self, need: usize) -> SalFit {
        self.fit_within(self.effective_capacity(), need)
    }

    /// Bytes already written. Paired with [`Self::reclaim_margin`] by every
    /// caller that asks how full the log is.
    pub(crate) fn used_bytes(&self) -> u64 {
        self.write_cursor.get()
    }

    /// The margin below which the SAL is "running low": one eighth of the
    /// mapping. Strictly under `effective_capacity`, so `fit(need.max(margin))`
    /// still answers [`SalFit::Terminal`] only when `need` itself is too large.
    pub(crate) fn reclaim_margin(&self) -> usize {
        (self.mmap_size >> RECLAIM_FRACTION_SHIFT) as usize
    }

    /// Reserve a group for `worker_sizes.len()` workers at the write cursor and
    /// write its header + directory. The group must be handed to
    /// [`Self::finish`]. The only failure is that it does not fit the space its
    /// kind and framing may occupy, so the verdict is [`SalFit`] itself.
    fn begin<'a>(
        &self,
        target_id: u32,
        lsn: u64,
        kind: SalMessageKind,
        mark: ZoneMark,
        worker_sizes: &'a [u32],
    ) -> Result<SalGroup<'a>, SalFit> {
        assert!(
            worker_sizes.len() <= MAX_WORKERS,
            "a SAL group cannot carry more than MAX_WORKERS slots"
        );
        let epoch = self.epoch.get();
        assert!(
            epoch >= 1,
            "SAL group epoch must be >= 1 — epoch 0 is indistinguishable from the empty prefix"
        );

        // The header size is a multiple of 8 and every slot contributes
        // align8(sz), so `payload_size` is always a multiple of 8.
        let hdr_size = group_header_size(worker_sizes.len());
        let total = 8 + group_payload_size(hdr_size, worker_sizes.iter().copied());
        // Against the cap this kind and framing may spend, not the ordinary one.
        let fit = self.fit_within(effective_max(kind, mark, self.mmap_size as usize), total);
        if fit != SalFit::Fits {
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
        let hdr_off = base + 8;
        // SAFETY: `cursor + total <= mmap_size`, so the whole header is mapped.
        // One `&mut [u8]` over it, so every field write below is bounds-checked
        // against the header rather than argued in prose.
        let hdr = unsafe { std::slice::from_raw_parts_mut(self.ptr.add(hdr_off), hdr_size) };
        write_u64_le(hdr, OFF_LSN, lsn);
        write_u32_le(hdr, OFF_FLAGS, kind_bit(kind) | mark.bits());
        write_u32_le(hdr, OFF_TARGET_ID, target_id);
        write_u32_le(hdr, OFF_SLOT_COUNT, worker_sizes.len() as u32);
        write_u32_le(hdr, OFF_EPOCH, epoch);
        // Every entry is written, empty ones included, and the `align8` pad
        // behind them zeroed: `wal.sal` is never truncated, so an unwritten byte
        // inside the digested span would read as whatever group last occupied
        // this offset.
        for (w, &sz) in worker_sizes.iter().enumerate() {
            write_u32_le(hdr, OFF_DIRECTORY + w * 4, sz);
        }
        hdr[OFF_DIRECTORY + worker_sizes.len() * 4..].fill(0);

        Ok(SalGroup {
            sal_ptr: self.ptr,
            base,
            sizes: worker_sizes,
            epoch,
            laid_out: false,
        })
    }

    /// Lay `group` out, advance the write cursor past it, and publish it unless a
    /// [`Self::defer_publication`] scope is open.
    fn finish(&self, group: SalGroup<'_>) {
        // SAFETY: taking the group by value ends every slice `for_each_slot`
        // handed out, so nothing references its span any more.
        let (end, publication) = unsafe { group.lay_out() };
        self.write_cursor.set(end);
        if self.deferring.get() {
            self.pending.borrow_mut().push(publication);
        } else {
            self.publish(publication);
        }
    }

    /// The Release store that makes one laid-out group visible.
    fn publish(&self, (base, word): (usize, u64)) {
        unsafe { atomic_store_u64(self.ptr.add(base), word) };
    }

    /// Open a deferred-publication scope: a group laid out until
    /// [`Self::publish_pending`] gets its bytes and its digest but no prefix
    /// word, so no reader can see it and it can still be taken back.
    ///
    /// The caller must hold `sal_writer_excl` and must not suspend inside the
    /// scope, which is what makes the pending list ordinary local state.
    pub(crate) fn defer_publication(&self) {
        debug_assert!(!self.deferring.get(), "a deferred-publication scope is already open");
        debug_assert!(
            self.pending.borrow().is_empty(),
            "a previous scope left groups unpublished"
        );
        self.deferring.set(true);
    }

    /// The point a deferred group can be rolled back to, taken before laying it
    /// out.
    pub(crate) fn savepoint(&self) -> Savepoint {
        debug_assert!(
            self.deferring.get(),
            "a savepoint outside a deferred scope cannot be rolled back"
        );
        Savepoint {
            cursor: self.write_cursor.get(),
            pending: self.pending.borrow().len(),
        }
    }

    /// Take back every group laid out since `sp`: nothing was published, so
    /// restoring the cursor is the whole undo. The bytes stay where they are and
    /// the next group overwrites them.
    pub(crate) fn roll_back(&self, sp: Savepoint) {
        self.pending.borrow_mut().truncate(sp.pending);
        self.write_cursor.set(sp.cursor);
    }

    /// Publish every deferred group in layout order and close the scope.
    pub(crate) fn publish_pending(&self) {
        for publication in self.pending.borrow_mut().drain(..) {
            self.publish(publication);
        }
        self.deferring.set(false);
    }

    /// Encode per-worker wire data directly into the SAL mmap. Does NOT
    /// sync/signal. `lsn` is supplied by the caller; the SAL writer does not own
    /// the counter.
    ///
    /// The template's `schema_block` is copied verbatim into every slot — one
    /// encoding, `nw` memcpys — or omitted entirely when it is `None`, which the
    /// command verbs do: their worker arm resolves its own schema. A group
    /// carrying data must carry one, as the `debug_assert` below states.
    pub(crate) fn write_group_direct(
        &self,
        g: &DirectGroup,
        lsn: u64,
        kind: SalMessageKind,
        mark: ZoneMark,
    ) -> Result<(), SalFit> {
        let nw = self.num_workers;
        if let GroupTargets::All(ids) = g.targets {
            assert_eq!(
                ids.len(),
                nw,
                "write_group_direct: req_ids.len()={} != num_workers={}",
                ids.len(),
                nw
            );
        }
        if let GroupData::PerWorker(d) = g.data {
            assert_eq!(
                d.len(),
                nw,
                "write_group_direct: worker_data.len()={} != num_workers={}",
                d.len(),
                nw
            );
        }
        debug_assert!(
            g.template.schema_block.is_some() || g.data.is_dataless(),
            "write_group_direct: data without a schema — `decode_wire` rejects FLAG_HAS_DATA \
             without FLAG_HAS_SCHEMA",
        );

        let worker_sizes = g.slot_sizes(nw);
        let group = self.begin(g.template.target_id as u32, lsn, kind, mark, &worker_sizes[..nw])?;

        unsafe {
            group.for_each_slot(|w, slot| {
                let written = g.msg(w).encode(slot, 0);
                debug_assert_eq!(written, slot.len());
            });
        }

        self.finish(group);
        Ok(())
    }

    /// The exact number of SAL bytes [`Self::write_group_direct`] will consume
    /// for `g`. Neither the sentinel nor the checkpoint band is included; both
    /// are held back globally by `effective_capacity`.
    pub(crate) fn group_footprint_direct(&self, g: &DirectGroup) -> usize {
        let nw = self.num_workers;
        8 + group_payload_size(group_header_size(nw), g.slot_sizes(nw)[..nw].iter().copied())
    }

    pub fn needs_checkpoint(&self) -> bool {
        self.write_cursor.get() >= self.checkpoint_threshold
    }

    /// Rewind to cursor 0 at `epoch` and clear the slot-0 prefix, so readers at
    /// cursor 0 see "no message" until this epoch writes there.
    ///
    /// The one reset — a checkpoint passes the successor of the current epoch,
    /// boot the recovered walk epoch plus one — so epochs are monotone across
    /// both and any leftover carries a strictly lower one. A fresh ring recovers
    /// 0 and starts at 1, the first live epoch.
    pub fn rewind(&self, epoch: u32) {
        debug_assert!(epoch >= 1, "the first live SAL epoch is 1");
        debug_assert!(
            !self.deferring.get(),
            "a rewind inside a deferred scope loses its groups"
        );
        self.write_cursor.set(0);
        self.epoch.set(epoch);
        unsafe {
            atomic_store_u64(self.ptr, 0);
        }
    }

    /// Rewind into the next epoch — the checkpoint's reclaim.
    pub fn checkpoint_reset(&self) {
        self.rewind(self.epoch.get() + 1);
    }

    /// Place the cursor and epoch by hand. `sal_epoch_fence` lays two groups at
    /// consecutive offsets under *different* epochs to prove each group's epoch
    /// is read from its own header — a shape no `rewind` can produce.
    #[cfg(test)]
    pub fn reset(&self, cursor: u64, epoch: u32) {
        self.write_cursor.set(cursor);
        self.epoch.set(epoch);
    }

    #[cfg(test)]
    pub fn cursor(&self) -> u64 {
        self.write_cursor.get()
    }

    pub fn epoch(&self) -> u32 {
        self.epoch.get()
    }

    pub fn sal_fd(&self) -> i32 {
        self.fd
    }

    /// Write one group whose slots are the raw `payloads` (test helper), through
    /// the production `begin` / `for_each_slot` / `finish` so the writer's own
    /// cursor advances with it.
    #[cfg(test)]
    pub(crate) fn write_raw_slots(
        &self,
        target_id: u32,
        lsn: u64,
        kind: SalMessageKind,
        mark: ZoneMark,
        payloads: &[&[u8]],
    ) -> Result<(), SalFit> {
        let sizes: Vec<u32> = payloads.iter().map(|p| p.len() as u32).collect();
        let group = self.begin(target_id, lsn, kind, mark, &sizes)?;
        unsafe { group.for_each_slot(|w, slot| slot.copy_from_slice(payloads[w])) };
        self.finish(group);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SalReader — one worker's live drain over a SalLog
// ---------------------------------------------------------------------------

pub struct SalReader {
    log: SalLog,
    worker_id: u32,
    m2w_efd: i32,
    /// The live drain's position and epoch — the read-side mirror of
    /// `SalWriter`'s `write_cursor` / `epoch`, so no caller does SAL address
    /// arithmetic.
    read_cursor: Cell<u64>,
    expected_epoch: Cell<u32>,
}

impl SalReader {
    /// `expected_epoch` is the generation the live drain accepts: the recovered
    /// walk epoch plus one, matching what `SalWriter::rewind` starts the master
    /// at.
    ///
    /// # Safety
    /// `ptr` must be a valid mmap pointer of at least `mmap_size` bytes.
    pub unsafe fn new(ptr: *const u8, worker_id: u32, mmap_size: usize, m2w_efd: i32, expected_epoch: u32) -> Self {
        SalReader {
            log: SalLog::new(ptr, mmap_size),
            worker_id,
            m2w_efd,
            read_cursor: Cell::new(0),
            expected_epoch: Cell::new(expected_epoch),
        }
    }

    /// The next group for this worker, advancing the cursor only on a clean
    /// read. A group from another epoch stays parked at the cursor until
    /// [`rewind`](Self::rewind) catches up.
    ///
    /// A digest failure here is corruption, and the response is a fail-stop: a
    /// live reader is never behind the writer's epoch, so every leftover is
    /// rejected by [`EpochGate::Live`]'s prefix gate before a header byte is
    /// read, and a gate-passing prefix implies a fully published header (the
    /// publication store lands last). Parking instead would be indistinguishable
    /// from having caught up — `next_sal_message` maps `None` to end-of-drain —
    /// so the worker would go quiet and the committer would wait forever on an
    /// ACK nobody will send.
    pub fn next(&self) -> Option<SalMessage> {
        let cursor = self.read_cursor.get();
        match self.log.read_at(cursor, EpochGate::Live(self.expected_epoch.get())) {
            SalStep::Group(msg, new_cursor) => {
                self.read_cursor.set(new_cursor);
                Some(msg)
            }
            SalStep::Corrupt => {
                gnitz_fatal_abort!("SAL group header failed its digest at offset={cursor} — the log is damaged")
            }
            SalStep::Absent => None,
        }
    }

    /// This worker's slot of `msg`, or `None` when the group did not write one
    /// for it.
    pub fn my_slot(&self, msg: &SalMessage) -> Option<&'static [u8]> {
        msg.slot(self.worker_id)
    }

    /// Rewind to the start of the next epoch, mirroring the writer's
    /// `SalWriter::checkpoint_reset` on the read side. Groups the master writes
    /// post-reset (at `write_cursor == 0`, in the bumped epoch) are then
    /// accepted, and any pre-reset group parks.
    pub fn rewind(&self) {
        self.read_cursor.set(0);
        self.expected_epoch.set(self.expected_epoch.get() + 1);
    }

    /// Park until the master signals, or `timeout_ms` elapses.
    pub fn wait(&self, timeout_ms: i32) -> Wake {
        m2w::eventfd_wait(self.m2w_efd, timeout_ms)
    }
}
