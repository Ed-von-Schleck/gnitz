//! SAL (shared append-only log): master→worker broadcast channel.
//!
//! Owns the mmap layout, group-header write/read helpers, SalWriter,
//! SalMessage, SalReader, and the atomic primitives used by the SAL.
//!
//! The **zone protocol** — both directions of it, the commit sentinel a writer
//! emits and the recovery walk a reader decides zone commitment with — lives in
//! the [`zone`] child module. A child, so the walk keeps reading `read_at` and
//! `valid_headers_from` without either becoming part of this module's surface.

pub(crate) mod zone;

use std::sync::atomic::{AtomicU64, Ordering};

use crate::runtime::posix;
use crate::runtime::posix::{read_u32_raw, read_u64_raw, write_u32_raw, write_u64_raw};
use crate::runtime::wire::{WireData, WireMsg, WireSchema};
use gnitz_engine::storage::Batch;
use gnitz_wire::align8;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

use gnitz_wire::MAX_WORKERS;

/// Leading marker of every out-of-space refusal from `SalWriter::begin`. It is a
/// **wire-text** contract, not a Rust one: no Rust caller matches it — they all
/// read [`SalFit`], which carries every internal decision — and its one consumer
/// is the E2E reader loop in `gnitz-py/tests/test_sal_read_reclaim.py`, which
/// sees the refusal only as the error string. The condition is transient by
/// construction: an ordinary group must leave the sentinel headroom and the
/// checkpoint band untouched (`effective_capacity`), so it is refused while the
/// log is near full, and the watchdog's reclaim — a fire-and-forget barrier once
/// less than 1/8 of the mapping is free — frees the whole mapping within one
/// 100 ms tick. A refused read or push is therefore retryable, unlike every
/// other server error.
pub const SAL_FULL: &str = "SAL full";

// Group header field offsets, relative to the start of the header, followed by
// one u32 offset and one u32 size per slot. The u64 prefix in front of the
// header is the publication marker, `(epoch << 32) | payload_size`; readers take
// the epoch and the stride from the header instead, inside the digested span.
const OFF_LSN: usize = 0;
const OFF_FLAGS: usize = 8;
const OFF_TARGET_ID: usize = 12;
const OFF_SLOT_COUNT: usize = 16;
const OFF_EPOCH: usize = 20;
/// The digest's own eight bytes, excluded from the span it covers.
const OFF_DIGEST: usize = 24;
const OFF_DIRECTORY: usize = 32;

/// Always a multiple of 8, so `payload_size` is 8-aligned and the resync scan's
/// 8-byte stride cannot step over a group base.
///
/// The directory is sized by the group's own slot count rather than by
/// `MAX_WORKERS`, which makes a group self-describing: `wal.sal` is never
/// truncated, so a group can land on an offset a wider group used before it, and
/// the recorded count is what tells a reader that slot 5 of a 2-slot group is
/// empty rather than a leftover. It also keeps the fixed overhead proportional to
/// the worker count, which matters most for the commit sentinel — one per
/// transaction, and otherwise all header.
#[inline]
pub(crate) const fn group_header_size(slots: usize) -> usize {
    OFF_DIRECTORY + 2 * slots * 4
}

/// A group's payload bytes: header + directory + `align8`-padded slots. The
/// 8-byte publication prefix in front is NOT included — `SalGroup::payload_size`
/// is exactly this.
///
/// `sal_begin_group` lays a group out by it and `SalWriter::group_footprint_direct`
/// predicts one by it. A two-site duplication would normally not be worth a
/// function; this one is, because when the two disagree the committer fail-stops
/// the node — a transaction family that no longer fits after an earlier family
/// already wrote to the SAL cannot be unwound.
fn group_payload_size(worker_sizes: &[u32]) -> usize {
    group_header_size(worker_sizes.len()) + worker_sizes.iter().map(|&sz| align8(sz as usize)).sum::<usize>()
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

    fn slot_sizes(&self, nw: usize) -> [u32; MAX_WORKERS] {
        let mut sizes = [0u32; MAX_WORKERS];
        for (w, size) in sizes.iter_mut().enumerate().take(nw) {
            if matches!(self.targets, GroupTargets::One { worker, .. } if w != worker) {
                continue;
            }
            *size = self.msg(w).size() as u32;
        }
        sizes
    }
}

/// XXH3-64 over a SAL group header — its own eight bytes excluded — seeded with
/// the group's byte offset in the ring, so a header only verifies where it was
/// published. The epoch is not a seed: it sits in the hashed span, so verifying a
/// header also authenticates the generation it claims.
fn group_digest(base: u64, hdr: &[u8]) -> u64 {
    gnitz_engine::foundation::xxh::digest_with_hole(&base.to_le_bytes(), hdr, OFF_DIGEST)
}

const SAL_MMAP_SIZE: usize = 1 << 30;

/// The exact SAL footprint of a zone-closing `FLAG_TXN_COMMIT` sentinel — a
/// slotless group, header only. `sal_begin_group` reserves this much headroom for
/// every non-sentinel group, so the sentinel — written last — always fits: a data
/// group at the boundary fails `sal_begin_group` gracefully (`write_err` + skip)
/// rather than aborting the node.
pub(crate) const SENTINEL_SIZE: usize = 8 + group_header_size(0);

/// Space held back from ordinary groups so the groups that must not fail always
/// fit: a checkpoint round's `FLAG_FLUSH`/`FLAG_FLUSH_EPH` group, and the
/// `FLAG_SHUTDOWN` broadcast (whose error `shutdown_workers` discards before
/// blocking in `waitpid`). The band holds **two** of them: the watchdog's crash
/// arm broadcasts `FLAG_SHUTDOWN` without the SAL mutex exactly while a
/// committer flush round is parked awaiting the dead worker's ACK.
/// `checkpoint_reserve_holds_two_terminal_groups` derives that bound from the
/// constants, so a wider control block or `MAX_WORKERS` trips there. Negligible
/// against both the 1 GiB default and the `MIN_SAL_BYTES` floor.
pub(crate) const CHECKPOINT_RESERVE: usize = 64 << 10;

/// The highest byte a group with `flags` may occupy.
///
/// The terminal groups — the checkpoint rounds and the shutdown broadcast — may
/// spend the `CHECKPOINT_RESERVE` that exists for them, but not the sentinel
/// headroom; nothing follows them in the epoch. A `FLAG_TXN_COMMIT` sentinel may
/// spend the sentinel headroom but not the reserve. Every other group must leave
/// both, which keeps the bound structural: an ordinary group always leaves room
/// for one sentinel, and `emit_zone_to_sal` writes no bare sentinel, so no run of
/// sentinels can reach the reserve. Capping the sentinel below `mmap_size` also
/// stops one that ends exactly at the mapping's end from skipping its own
/// terminating prefix.
pub(crate) fn effective_max(flags: u32, mmap_size: usize) -> usize {
    if flags & (FLAG_FLUSH | FLAG_FLUSH_EPH | FLAG_SHUTDOWN) != 0 {
        mmap_size.saturating_sub(SENTINEL_SIZE)
    } else if flags & FLAG_TXN_COMMIT != 0 {
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
        gnitz_engine::foundation::env::env_num("GNITZ_SAL_BYTES", SAL_MMAP_SIZE).clamp(MIN_SAL_BYTES, SAL_MMAP_SIZE)
    })
}

// SAL group header flags (u32): the shared `gnitz_wire` flag bits re-declared
// as `u32` (the SAL group header's flag width).
pub const FLAG_SHUTDOWN: u32 = gnitz_wire::FLAG_SHUTDOWN as u32;
pub const FLAG_DDL_SYNC: u32 = gnitz_wire::FLAG_DDL_SYNC as u32;
pub const FLAG_PUSH: u32 = gnitz_wire::FLAG_PUSH as u32;
pub const FLAG_HAS_PK: u32 = gnitz_wire::FLAG_HAS_PK as u32;
pub const FLAG_SEEK: u32 = gnitz_wire::FLAG_SEEK as u32;
pub const FLAG_SEEK_BY_INDEX: u32 = gnitz_wire::FLAG_SEEK_BY_INDEX as u32;
/// Parameterized bounded read (`ReadSpec`). Lives inside the wire SAL flag block
/// (bit 10) and is carried verbatim from the client wire frame into the SAL group
/// header — one allocation, mirrored here as a `u32` (unlike the high client-only
/// request bits, which need a distinct u32 SAL dispatch flag).
pub const FLAG_SCAN_SPEC: u32 = gnitz_wire::FLAG_SCAN_SPEC as u32;
// The next four are engine-internal in meaning but live in the shared 0-15
// block, so `gnitz_wire` owns the bit (and its collision guard) while the
// meaning stays here.
pub const FLAG_EXCHANGE_RELAY: u32 = gnitz_wire::FLAG_EXCHANGE_RELAY as u32;
pub const FLAG_BACKFILL: u32 = gnitz_wire::FLAG_BACKFILL as u32;
pub const FLAG_TICK: u32 = gnitz_wire::FLAG_TICK as u32;
pub const FLAG_FLUSH: u32 = gnitz_wire::FLAG_FLUSH as u32;
/// Marks an empty broadcast group as the closing "commit sentinel" of an
/// atomic zone. All preceding groups at the same LSN belong to the zone;
/// recovery applies them only when this sentinel is on disk. The flag
/// rides on top of FLAG_DDL_SYNC for the worker's dispatch loop, which
/// already no-ops on a DDL_SYNC group with `count == 0`.
pub const FLAG_TXN_COMMIT: u32 = gnitz_wire::FLAG_TXN_COMMIT as u32;
/// Batched stored-row gather: scatter a set of PKs to their owning workers,
/// each worker reads the committed rows for the PKs it owns and replies with
/// the rows projected to the single column index carried in the control
/// block's `seek_col_idx`. Distinct from `FLAG_SEEK` (single key) and
/// `FLAG_HAS_PK` (existence echo of the caller's payload); the gather returns
/// the *stored* value of a column the caller does not have.
pub const FLAG_GATHER: u32 = 1 << 16;
/// CREATE UNIQUE INDEX global pre-flight: each worker projects its committed
/// partition of `target_id` to the OPK leading-key spans of the column list
/// packed in `seek_col_idx`, sorts them, and streams the SORTED spans back as
/// continuation frames for the master's k-way merge (see
/// `validate_unique_index_create`). Unicast-shaped like a Scan: every
/// worker gets its own req_id slot and answers with a frame train.
pub const FLAG_UNIQUE_PREFLIGHT: u32 = 1 << 17;
/// The first group of an atomic zone. With the closing `FLAG_TXN_COMMIT`
/// sentinel it delimits the zone's byte span, which is how recovery tells damage
/// that cost a committed transaction a group from damage in one of the `lsn = 0`
/// command groups between zones. `KIND_BY_FLAG` does not list it, so it changes
/// no dispatch.
pub const FLAG_ZONE_START: u32 = 1 << 18;
/// Ephemeral-state flush round of the checkpoint sequence: flush every view's
/// operator-trace tables and output stores (traces before outputs), stamping
/// their manifests with the checkpoint generation carried in the group header's
/// `lsn` field. Dispatched inline in both worker contexts like `FLAG_FLUSH`, but
/// distinct so the base round (`FLAG_FLUSH`, `SalReplay` user tables) and the
/// ephemeral round (`Rederive` view state) stay separate handlers.
pub const FLAG_FLUSH_EPH: u32 = 1 << 19;
/// A `FLAG_SCAN_SPEC` read whose bound is a delta bound. A dispatch
/// classification, not a verb: the client still sends `FLAG_SCAN_SPEC`, and the
/// master peeks the bound once (`peek_delta_bound`) and stamps this alongside it.
///
/// It exists because the worker's inline-vs-defer matrix is a function of
/// `(context, kind)` and nothing else — deciding inside the `ScanSpec` arm by
/// peeking the bound there would break that, and deferring *every* `ScanSpec`
/// would make an ad-hoc point read wait out an exchange round-trip it has no
/// stake in.
pub const FLAG_DELTA_SCAN: u32 = 1 << 20;

// The flags above that `gnitz_wire` does not allocate are the engine's own, and
// stay strictly above the shared 0-15 block — that separation is what lets the
// two crates allocate independently. `gnitz_wire`'s guard covers the block
// itself; this covers the engine's side of the line.
const _: () = {
    let engine_only = [
        FLAG_GATHER,
        FLAG_UNIQUE_PREFLIGHT,
        FLAG_ZONE_START,
        FLAG_FLUSH_EPH,
        FLAG_DELTA_SCAN,
    ];
    let mut acc = 0u32;
    let mut i = 0;
    while i < engine_only.len() {
        assert!(
            engine_only[i] & gnitz_wire::SAL_FLAGS_MASK as u32 == 0,
            "engine SAL flag collides with the wire-allocated 0-15 block"
        );
        assert!(engine_only[i] & acc == 0, "engine SAL flag bit collision");
        acc |= engine_only[i];
        i += 1;
    }
};

// ---------------------------------------------------------------------------
// SalMessageKind — receive-side classification of a SAL group's flag bits.
//
// The master encodes with raw FLAG_* writes; this enum is consumed by the
// worker dispatch loop, so the compiler checks that every kind the worker can
// observe has an arm. Add a variant here whenever a dispatch arm is added.
// ---------------------------------------------------------------------------

/// Classification of a SAL group's flag bits, used by the worker to
/// dispatch. Mutually exclusive: every well-formed SAL group classifies
/// into exactly one variant.
///
/// `Scan` is the default when no kind-specific flag is set; the worker
/// answers with a full table scan. `FLAG_TXN_COMMIT` rides on top of
/// `FLAG_DDL_SYNC` (zero-count sentinel); classification stops at
/// `DdlSync` and the worker handles it as a no-op DDL_SYNC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SalMessageKind {
    Shutdown,
    Flush,
    FlushEph,
    DdlSync,
    ExchangeRelay,
    Backfill,
    HasPk,
    Gather,
    UniquePreflight,
    Push,
    Tick,
    SeekByIndex,
    Seek,
    /// A `ReadSpec` read carrying a delta bound. Answered by the same handler a
    /// plain `ScanSpec` is, and separate only so the defer matrix can name it: a
    /// delta read answered from inside an in-flight evaluation would see a
    /// half-ingested round, and a client that advanced its cursor to that round
    /// would lose the rest of it silently.
    DeltaScanSpec,
    ScanSpec,
    Scan,
}

/// Flag bit → kind, in priority order; the first bit set on the group wins.
///
/// Two positions constrain the order. `Shutdown` leads and the two flush rounds
/// precede the rest, as before. And `FLAG_DELTA_SCAN` must sit **above**
/// `FLAG_SCAN_SPEC`: a delta read's group carries both bits, so the other order
/// would classify every one of them as a plain `ScanSpec` and silently give up
/// the deferral. Every other kind owns a distinct bit and is unconstrained.
const KIND_BY_FLAG: [(u32, SalMessageKind); 15] = [
    (FLAG_SHUTDOWN, SalMessageKind::Shutdown),
    (FLAG_FLUSH, SalMessageKind::Flush),
    (FLAG_FLUSH_EPH, SalMessageKind::FlushEph),
    (FLAG_DDL_SYNC, SalMessageKind::DdlSync),
    (FLAG_EXCHANGE_RELAY, SalMessageKind::ExchangeRelay),
    (FLAG_BACKFILL, SalMessageKind::Backfill),
    (FLAG_HAS_PK, SalMessageKind::HasPk),
    (FLAG_GATHER, SalMessageKind::Gather),
    (FLAG_UNIQUE_PREFLIGHT, SalMessageKind::UniquePreflight),
    (FLAG_PUSH, SalMessageKind::Push),
    (FLAG_TICK, SalMessageKind::Tick),
    (FLAG_SEEK_BY_INDEX, SalMessageKind::SeekByIndex),
    (FLAG_SEEK, SalMessageKind::Seek),
    (FLAG_DELTA_SCAN, SalMessageKind::DeltaScanSpec),
    (FLAG_SCAN_SPEC, SalMessageKind::ScanSpec),
];

impl SalMessageKind {
    /// Classify a SAL group by its flag word; `Scan` when no kind bit is set.
    pub fn classify(flags: u32) -> SalMessageKind {
        KIND_BY_FLAG
            .iter()
            .find(|(bit, _)| flags & bit != 0)
            .map_or(SalMessageKind::Scan, |&(_, kind)| kind)
    }

    /// True when the worker must act on the group even if its per-worker
    /// data slot is empty (broadcast / control / data-rebroadcast kinds).
    /// Unicast kinds (SEEK, SEEK_BY_INDEX, Scan, ScanSpec, DeltaScanSpec,
    /// UniquePreflight, ExchangeRelay) return false: a missing slot means the
    /// message wasn't for us — which is also what keeps a replicated view's
    /// worker-0 delta unicast from being acted on by the workers that got no
    /// slot.
    pub fn is_broadcast(self) -> bool {
        matches!(
            self,
            SalMessageKind::Shutdown
                | SalMessageKind::Flush
                | SalMessageKind::FlushEph
                | SalMessageKind::DdlSync
                | SalMessageKind::Backfill
                | SalMessageKind::HasPk
                | SalMessageKind::Gather
                | SalMessageKind::Push
                | SalMessageKind::Tick
        )
    }
}

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

#[inline]
unsafe fn sal_write_sentinel(sal_ptr: *mut u8, offset: usize, mmap_size: usize) {
    if offset + 8 <= mmap_size {
        atomic_store_u64(sal_ptr.add(offset), 0);
    }
}

// ---------------------------------------------------------------------------
// SAL write (master→workers)
// ---------------------------------------------------------------------------

/// Handle returned by `sal_begin_group`. Header and per-worker directory
/// are already written; caller fills per-worker data, then calls `commit()`.
#[must_use = "SalGroup must be passed to commit(); dropping it leaves the SAL sentinel unwritten"]
pub(crate) struct SalGroup {
    sal_ptr: *mut u8,
    /// Offset of the group's 8-byte size prefix; the header follows it.
    base: usize,
    /// Header + directory bytes; also the offset of slot 0's payload.
    hdr_size: usize,
    payload_size: usize,
    /// The slot sizes this group was laid out with. Held rather than re-passed
    /// so `for_each_slot` walks the directory it wrote: a caller handing over a
    /// different slice would hand out a `&mut [u8]` at the wrong offset inside a
    /// live mmap, with nothing to catch it.
    sizes: [u32; MAX_WORKERS],
    slots: usize,
    epoch: u32,
    mmap_size: usize,
    committed: bool,
}

impl Drop for SalGroup {
    fn drop(&mut self) {
        debug_assert!(
            self.committed,
            "SalGroup dropped without commit — SAL sentinel never written"
        );
    }
}

impl SalGroup {
    #[inline]
    pub(crate) unsafe fn data_ptr(&self, offset: usize) -> *mut u8 {
        self.sal_ptr.add(self.base + 8 + offset)
    }

    /// Hand every non-empty worker slot to `f` as a mutable byte slice, in the
    /// `align8` directory order `sal_begin_group` laid out.
    ///
    /// # Safety
    /// The slots must be unaliased for the duration — nothing else may hold a
    /// reference into this group's payload span.
    pub(crate) unsafe fn for_each_slot(&self, mut f: impl FnMut(usize, &mut [u8])) {
        let mut off = self.hdr_size;
        for (w, &sz) in self.sizes[..self.slots].iter().enumerate() {
            let sz = sz as usize;
            if sz == 0 {
                continue;
            }
            f(w, std::slice::from_raw_parts_mut(self.data_ptr(off), sz));
            off += align8(sz);
        }
    }

    pub(crate) unsafe fn commit(mut self) -> u64 {
        let end = self.base + 8 + self.payload_size;
        sal_write_sentinel(self.sal_ptr, end, self.mmap_size);
        // Stamped before the Release store publishes the group, so no readable
        // group carries an unstamped digest. Nothing writes into the header
        // between `sal_begin_group` and here — slots start at `off >= hdr_size` —
        // so these are final bytes.
        let hdr_off = self.base + 8;
        let hdr = std::slice::from_raw_parts(self.sal_ptr.add(hdr_off) as *const u8, self.hdr_size);
        let digest = group_digest(self.base as u64, hdr);
        write_u64_raw(self.sal_ptr, hdr_off + OFF_DIGEST, digest);
        atomic_store_u64(
            self.sal_ptr.add(self.base),
            (self.epoch as u64) << 32 | self.payload_size as u64,
        );
        self.committed = true;
        end as u64
    }
}

/// Reserve SAL space, write group header + per-worker directory. One entry per
/// worker in `worker_sizes`. Returns `None` if the group doesn't fit or there
/// are more than `MAX_WORKERS` entries.
///
/// # Safety
/// `sal_ptr` must be a valid mmap pointer of at least `mmap_size` bytes.
#[allow(clippy::too_many_arguments)]
pub(crate) unsafe fn sal_begin_group(
    sal_ptr: *mut u8,
    write_cursor: usize,
    mmap_size: usize,
    target_id: u32,
    lsn: u64,
    flags: u32,
    epoch: u32,
    worker_sizes: &[u32],
) -> Option<SalGroup> {
    if worker_sizes.len() > MAX_WORKERS {
        return None;
    }
    debug_assert!(
        epoch >= 1,
        "SAL group epoch must be >= 1 — epoch 0 is indistinguishable from the empty-slot sentinel prefix"
    );

    // The header size is a multiple of 8 and every slot contributes align8(sz),
    // so `payload_size` is always a multiple of 8.
    let hdr_size = group_header_size(worker_sizes.len());
    let payload_size = group_payload_size(worker_sizes);
    let total = 8 + payload_size;
    let cap = effective_max(flags, mmap_size);
    if write_cursor + total > cap {
        return None;
    }

    let base = write_cursor;
    let hdr_off = base + 8;

    let slots = worker_sizes.len();
    write_u64_raw(sal_ptr, hdr_off + OFF_LSN, lsn);
    write_u32_raw(sal_ptr, hdr_off + OFF_FLAGS, flags);
    write_u32_raw(sal_ptr, hdr_off + OFF_TARGET_ID, target_id);
    write_u32_raw(sal_ptr, hdr_off + OFF_SLOT_COUNT, slots as u32);
    write_u32_raw(sal_ptr, hdr_off + OFF_EPOCH, epoch);

    // Every entry is written, empty ones included: `wal.sal` is never truncated,
    // so an unwritten entry would read as whatever group last occupied this
    // offset.
    let mut data_offset = hdr_size;
    for (w, &sz) in worker_sizes.iter().enumerate() {
        let off = if sz > 0 { data_offset } else { 0 };
        write_u32_raw(sal_ptr, hdr_off + OFF_DIRECTORY + w * 4, off as u32);
        write_u32_raw(sal_ptr, hdr_off + OFF_DIRECTORY + slots * 4 + w * 4, sz);
        data_offset += align8(sz as usize);
    }

    let mut sizes = [0u32; MAX_WORKERS];
    sizes[..slots].copy_from_slice(worker_sizes);
    Some(SalGroup {
        sal_ptr,
        base,
        hdr_size,
        payload_size,
        sizes,
        slots,
        epoch,
        mmap_size,
        committed: false,
    })
}

/// Write a message group into the SAL for N workers (test helper).
/// `payloads[w]` is worker w's slot; an empty slice means no data for w.
/// Returns the new cursor, or `None` when the group doesn't fit.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) unsafe fn sal_write_group(
    sal_ptr: *mut u8,
    write_cursor: u64,
    target_id: u32,
    lsn: u64,
    flags: u32,
    epoch: u32,
    mmap_size: u64,
    payloads: &[&[u8]],
) -> Option<u64> {
    let nw = payloads.len();
    let mut sizes = [0u32; MAX_WORKERS];
    for (w, p) in payloads.iter().enumerate() {
        sizes[w] = p.len() as u32;
    }

    let group = sal_begin_group(
        sal_ptr,
        write_cursor as usize,
        mmap_size as usize,
        target_id,
        lsn,
        flags,
        epoch,
        &sizes[..nw],
    )?;

    group.for_each_slot(|w, slot| slot.copy_from_slice(payloads[w]));

    Some(group.commit())
}

// ---------------------------------------------------------------------------
// SAL read (worker reads its data from a group)
// ---------------------------------------------------------------------------

/// One SAL group as a reader sees it, for the worker slot it asked about.
pub struct SalMessage<'a> {
    pub lsn: u64,
    pub kind: SalMessageKind,
    pub flags: u32,
    pub target_id: u32,
    /// The group's byte offset in the ring — what a recovery error names, and
    /// what the zone-span rule tests damage against.
    pub base: u64,
    /// The slot count the group was written with. Recovery validates a zone's
    /// blocks across all of them, not only the reader's own, so its verdict does
    /// not depend on which worker asked. A reader asking for a slot at or past
    /// it gets an empty slot.
    pub slots: u32,
    /// None = no data for this worker in this group.
    pub wire_data: Option<&'a [u8]>,
}

/// What the bytes at an offset are. `Group` carries the message and the cursor
/// past it.
pub enum SalStep {
    /// Nothing published here, or the group's own stride runs past this mapping
    /// (the log was written under a larger `GNITZ_SAL_BYTES`). Either way, the
    /// end of the log.
    Absent,
    /// A published header that fails its digest.
    Corrupt,
    /// A verified header stamped with another epoch — the ring's leftovers.
    OtherEpoch,
    Group(SalMessage<'static>, u64),
}

/// The `(slot_count, epoch)` of the verified header at `base`, or `None`. A
/// header that verifies was published at this `base` by the epoch it carries.
///
/// Each bound below precedes the read it guards. The publication prefix is not
/// consulted, so a reset ring still yields the epoch of its last occupant and a
/// resyncing walk can probe an arbitrary candidate.
///
/// # Safety
/// `sal_ptr` must be a valid mmap pointer of at least `mmap_size` bytes.
pub(crate) unsafe fn sal_probe_header(sal_ptr: *const u8, base: u64, mmap_size: u64) -> Option<(u32, u32)> {
    let b = base as usize;
    let m = mmap_size as usize;
    if b + 8 + group_header_size(0) > m {
        return None;
    }
    let hdr_off = b + 8;
    let slots = read_u32_raw(sal_ptr, hdr_off + OFF_SLOT_COUNT) as usize;
    if slots > MAX_WORKERS {
        return None;
    }
    let hdr_len = group_header_size(slots);
    if b + 8 + hdr_len > m {
        return None;
    }
    let hdr = std::slice::from_raw_parts(sal_ptr.add(hdr_off), hdr_len);
    if group_digest(base, hdr) != read_u64_raw(sal_ptr, hdr_off + OFF_DIGEST) {
        return None;
    }
    Some((slots as u32, read_u32_raw(sal_ptr, hdr_off + OFF_EPOCH)))
}

/// The publication prefix word at `base`, or 0 when nothing was published there.
/// Acquire-loaded, so a group whose prefix is visible has its whole header
/// visible too (`commit` Release-stores it last).
///
/// The **whole word** is the presence test, not its `payload_size` half: every
/// published group has `epoch >= 1`, so a zero word means "nothing here", while
/// a small `payload_size` — a commit sentinel's is one set bit — is
/// single-bit-zeroable and would end a walk before that transaction's sentinel.
///
/// # Safety
/// `sal_ptr` must be a valid mmap pointer with `base + 8 <= mmap_size`.
#[inline]
unsafe fn sal_prefix_word(sal_ptr: *const u8, base: u64) -> u64 {
    atomic_load_u64(sal_ptr.add(base as usize))
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
    /// No epoch filter — reading one group at a known-good offset.
    Any,
}

impl EpochGate {
    fn epoch(self) -> Option<u32> {
        match self {
            EpochGate::Live(e) | EpochGate::Walk(e) => Some(e),
            EpochGate::Any => None,
        }
    }
}

/// Read a SAL group header and extract `worker_id`'s data pointer/size.
///
/// The caller decides what a `Corrupt` verdict means: the live drain fail-stops
/// (see [`SalReader::next`]), the recovery walk resyncs past it.
///
/// # Safety
/// `sal_ptr` must be a valid mmap pointer of at least `mmap_size` bytes.
pub(crate) unsafe fn sal_read_group_header(
    sal_ptr: *const u8,
    read_cursor: u64,
    worker_id: u32,
    gate: EpochGate,
    mmap_size: u64,
) -> SalStep {
    let rc = read_cursor as usize;
    let wid = worker_id as usize;

    let word = sal_prefix_word(sal_ptr, read_cursor);
    if word == 0 {
        return SalStep::Absent;
    }
    if let EpochGate::Live(exp) = gate {
        if (word >> 32) as u32 != exp {
            return SalStep::OtherEpoch;
        }
    }

    let Some((slots, epoch)) = sal_probe_header(sal_ptr, read_cursor, mmap_size) else {
        return SalStep::Corrupt;
    };
    if gate.epoch().is_some_and(|exp| epoch != exp) {
        return SalStep::OtherEpoch;
    }

    let slots = slots as usize;
    let hdr_off = rc + 8;
    let lsn = read_u64_raw(sal_ptr, hdr_off + OFF_LSN);
    let flags = read_u32_raw(sal_ptr, hdr_off + OFF_FLAGS);
    let target_id = read_u32_raw(sal_ptr, hdr_off + OFF_TARGET_ID);

    // The stride comes from the authenticated directory, not from the prefix's
    // `payload_size` copy, so a flipped `payload_size` changes no verdict.
    let hdr_len = group_header_size(slots);
    let mut stride = hdr_len;
    for w in 0..slots {
        stride += align8(read_u32_raw(sal_ptr, hdr_off + OFF_DIRECTORY + slots * 4 + w * 4) as usize);
    }
    if (rc + 8 + stride) as u64 > mmap_size {
        return SalStep::Absent;
    }
    let advance = (8 + stride) as u64;

    let (my_offset, my_size) = if wid < slots {
        (
            read_u32_raw(sal_ptr, hdr_off + OFF_DIRECTORY + wid * 4) as usize,
            read_u32_raw(sal_ptr, hdr_off + OFF_DIRECTORY + slots * 4 + wid * 4) as usize,
        )
    } else {
        (0, 0)
    };

    let (data_ptr, data_size) = if my_size > 0 && my_offset > 0 && my_offset + my_size <= stride {
        (sal_ptr.add(hdr_off + my_offset), my_size as u32)
    } else {
        (std::ptr::null(), 0)
    };
    SalStep::Group(
        SalMessage {
            lsn,
            kind: SalMessageKind::classify(flags),
            flags,
            target_id,
            base: read_cursor,
            slots: slots as u32,
            wire_data: (data_size > 0).then(|| std::slice::from_raw_parts(data_ptr, data_size as usize)),
        },
        read_cursor + advance,
    )
}

/// The slot count the SAL tail at `sal_ptr` was written with, or `None` when the
/// tail is empty (or its first group's header does not verify). The SAL is
/// rewound at boot and at every checkpoint, so one master at one worker count
/// writes a whole tail: the first group's count speaks for all of them.
///
/// # Safety
/// `sal_ptr` must be a valid mmap pointer of at least `mmap_size` bytes, and the
/// SAL quiescent.
pub(crate) unsafe fn sal_tail_slot_count(sal_ptr: *const u8, mmap_size: u64) -> Option<u32> {
    if sal_prefix_word(sal_ptr, 0) == 0 {
        return None;
    }
    sal_probe_header(sal_ptr, 0, mmap_size).map(|(slots, _)| slots)
}

// ---------------------------------------------------------------------------
// SalWriter
// ---------------------------------------------------------------------------

pub struct SalWriter {
    ptr: *mut u8,
    fd: i32,
    mmap_size: u64,
    write_cursor: std::cell::Cell<u64>,
    epoch: std::cell::Cell<u32>,
    checkpoint_threshold: u64,
    /// Slots per group: the group header's `slot_count`, the directory's length
    /// and every slot offset are all this. It is the log's framing, so it is the
    /// log writer's own field — never read off some other per-worker resource.
    num_workers: usize,
}

unsafe impl Send for SalWriter {}

impl SalWriter {
    pub fn new(ptr: *mut u8, fd: i32, mmap_size: u64, num_workers: usize) -> Self {
        let checkpoint_threshold =
            gnitz_engine::foundation::env::env_num("GNITZ_CHECKPOINT_BYTES", (mmap_size * 3) >> 2);
        SalWriter {
            ptr,
            fd,
            mmap_size,
            write_cursor: std::cell::Cell::new(0),
            epoch: std::cell::Cell::new(0),
            checkpoint_threshold,
            num_workers,
        }
    }

    /// SAL bytes an ordinary group may occupy: the mapping minus the headroom
    /// `sal_begin_group` reserves for the zone-closing sentinel and minus the
    /// band held back for the checkpoint and shutdown groups.
    fn effective_capacity(&self) -> usize {
        effective_max(0, self.mmap_size as usize)
    }

    /// Classify `need` bytes against the capacity and the live cursor. The one
    /// verdict: the committer's per-transaction check and the exchange relay's
    /// pre-write check both read it, so neither can drift from the rule
    /// `sal_begin_group` enforces.
    pub(crate) fn fit(&self, need: usize) -> SalFit {
        let capacity = self.effective_capacity();
        if need > capacity {
            SalFit::Terminal
        } else if need > capacity.saturating_sub(self.write_cursor.get() as usize) {
            SalFit::Transient
        } else {
            SalFit::Fits
        }
    }

    /// Reserve a group for `worker_sizes.len()` workers at the write cursor and
    /// write its header + directory. `what` names the caller in the
    /// out-of-space error. The group must be handed to `finish`.
    ///
    /// The only way this fails is that the group does not fit the space an
    /// ordinary group may occupy (`effective_capacity`), so the message says
    /// exactly that and carries [`SAL_FULL`] — the stable marker a caller may
    /// match to tell this transient, reclaim-clears-it refusal from a real
    /// failure. It reaches clients verbatim, so the write cursor stays out of it
    /// and goes to the operator log instead.
    fn begin(
        &self,
        what: &str,
        target_id: u32,
        lsn: u64,
        sal_flags: u32,
        worker_sizes: &[u32],
    ) -> Result<SalGroup, String> {
        unsafe {
            sal_begin_group(
                self.ptr,
                self.write_cursor.get() as usize,
                self.mmap_size as usize,
                target_id,
                lsn,
                sal_flags,
                self.epoch.get(),
                worker_sizes,
            )
        }
        .ok_or_else(|| {
            gnitz_debug!(
                "SAL {} refused: cursor={} mmap={} epoch={}",
                what,
                self.write_cursor.get(),
                self.mmap_size,
                self.epoch.get()
            );
            format!("{SAL_FULL}: {what} did not fit")
        })
    }

    /// Publish `group` and advance the write cursor past it.
    fn finish(&self, group: SalGroup) {
        self.write_cursor.set(unsafe { group.commit() });
    }

    /// Encode per-worker wire data directly into SAL mmap. Does NOT sync/signal.
    /// `lsn` is supplied by the caller; the SAL writer no longer owns the
    /// counter (Design 2: caller controls zone-LSN allocation).
    ///
    /// The template's `schema_block` is copied verbatim into every slot — one
    /// encoding, `nw` memcpys — or omitted entirely when it is `None`, which the
    /// command verbs do: their worker arm resolves its own schema. A group
    /// carrying data must carry one, as the `debug_assert` below states.
    pub fn write_group_direct(&self, g: &DirectGroup, lsn: u64, sal_flags: u32) -> Result<(), String> {
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
        let group = self.begin(
            "write_group_direct",
            g.template.target_id as u32,
            lsn,
            sal_flags,
            &worker_sizes[..nw],
        )?;

        unsafe {
            group.for_each_slot(|w, slot| {
                let written = g.msg(w).encode(slot, 0);
                debug_assert_eq!(written, slot.len());
            });
        }

        self.finish(group);
        Ok(())
    }

    /// The exact number of SAL bytes `write_group_direct` will consume for `g`.
    /// Neither the sentinel nor the checkpoint band is included; both are held
    /// back globally by `effective_capacity`.
    pub(crate) fn group_footprint_direct(&self, g: &DirectGroup) -> usize {
        8 + group_payload_size(&g.slot_sizes(self.num_workers)[..self.num_workers])
    }

    /// Build the [`DirectGroup`] a scatter emission of `input_batch` under
    /// `worker_indices` produces, and hand it to `f`. The one construction: both
    /// emitters (`write_scatter_group`, `write_commit_group`) write what it
    /// builds and `scatter_group_footprint` sizes the same thing, so a
    /// transaction's fit check measures the group that is later written.
    ///
    /// Rows go into the slots two ways. A `wire_safe` schema — fixed-width
    /// 8-aligned strides, no German-string columns — scatters straight into the
    /// SAL slot ([`WireData::Scattered`]), skipping the two-copy path
    /// (scatter→intermediate `Batch`, then `Batch`→slot). Any other schema
    /// carries out-of-line string bytes and has no scatter encoder, so its slots
    /// materialize a per-worker sub-`Batch` first.
    ///
    /// `relation` supplies the target id, the schema block every slot carries,
    /// and the descriptor the scatter encoder reads region strides off; the
    /// caller's `template` contributes only its own header fields.
    pub(crate) fn with_scatter_group<R>(
        &self,
        input_batch: &Batch,
        worker_indices: &[Vec<u32>],
        relation: &WireSchema,
        template: WireMsg<'_>,
        targets: GroupTargets<'_>,
        f: impl FnOnce(&DirectGroup) -> R,
    ) -> R {
        let nw = self.num_workers;
        let schema = relation.descriptor();
        let wire_safe = relation.wire_safe();

        // The sub-batches must outlive the group that borrows them, so they are
        // built here rather than inside the `worker_data` map.
        let mb = input_batch.as_mem_batch();
        let sub_batches: Vec<Batch> = if wire_safe {
            Vec::new()
        } else {
            worker_indices
                .iter()
                .take(nw)
                .map(|indices| {
                    if indices.is_empty() {
                        Batch::empty_with_schema(schema)
                    } else {
                        Batch::from_indexed_rows(&mb, indices, schema)
                    }
                })
                .collect()
        };
        let worker_data: Vec<WireData> = if wire_safe {
            worker_indices
                .iter()
                .take(nw)
                .map(|indices| WireData::Scattered {
                    batch: input_batch,
                    indices,
                    schema,
                })
                .collect()
        } else {
            sub_batches.iter().map(|b| WireData::Whole(Some(b))).collect()
        };

        f(&DirectGroup {
            template: relation.frame(template),
            data: GroupData::PerWorker(&worker_data),
            targets,
        })
    }

    /// The exact number of SAL bytes a [`Self::with_scatter_group`] emission
    /// will consume for this batch and partitioning. Neither the sentinel nor
    /// the checkpoint band is included; both are held back globally by
    /// `effective_capacity`.
    ///
    /// The group is built by the emission's own constructor, so the slot sizes
    /// are the emitted ones. `wire_flags`, `seek_col_idx` and the request ids
    /// are left at zero here: a slot's size is its control block (fixed-width
    /// unless `error_msg` or `seek_pk_extra` is non-empty, and this path sets
    /// neither), its schema block, and its data block over `schema` — none of
    /// which those three scalars reach.
    pub(crate) fn scatter_group_footprint(
        &self,
        input_batch: &Batch,
        worker_indices: &[Vec<u32>],
        relation: &WireSchema,
    ) -> usize {
        self.with_scatter_group(
            input_batch,
            worker_indices,
            relation,
            WireMsg::default(),
            GroupTargets::AllSilent,
            |g| self.group_footprint_direct(g),
        )
    }

    pub fn needs_checkpoint(&self) -> bool {
        self.write_cursor.get() >= self.checkpoint_threshold
    }

    pub fn checkpoint_reset(&self) {
        self.epoch.set(self.epoch.get() + 1);
        self.write_cursor.set(0);
        unsafe {
            atomic_store_u64(self.ptr, 0);
        }
    }

    /// Boot-time reset after all workers finish recovery: clear the slot-0
    /// sentinel prefix (readers at cursor 0 then see "no message") and rewind
    /// to cursor 0 at `epoch`. The mmap-prefix store lives here, next to
    /// `checkpoint_reset`, so the SAL slot layout never leaks to callers.
    ///
    /// `epoch` is the recovered walk epoch plus one, so epochs are monotone
    /// across boots — a leftover from a previous boot always carries a strictly
    /// lower epoch than anything this boot writes. A fresh ring recovers 0 and so
    /// starts at 1, the first live epoch; epoch 0 is the empty-slot prefix.
    pub fn boot_reset(&self, epoch: u32) {
        debug_assert!(epoch >= 1, "the first live SAL epoch is 1");
        self.write_cursor.set(0);
        self.epoch.set(epoch);
        unsafe {
            atomic_store_u64(self.ptr, 0);
        }
    }

    #[cfg(test)]
    pub fn reset(&self, cursor: u64, epoch: u32) {
        self.write_cursor.set(cursor);
        self.epoch.set(epoch);
    }

    pub fn cursor(&self) -> u64 {
        self.write_cursor.get()
    }
    pub fn epoch(&self) -> u32 {
        self.epoch.get()
    }
    pub fn mmap_size(&self) -> u64 {
        self.mmap_size
    }
    pub fn sal_fd(&self) -> i32 {
        self.fd
    }
}

// ---------------------------------------------------------------------------
// SalReader
// ---------------------------------------------------------------------------

pub struct SalReader {
    ptr: *const u8,
    worker_id: u32,
    mmap_size: u64,
    m2w_efd: i32,
    /// The live drain's position and epoch — the read-side mirror of
    /// `SalWriter`'s `write_cursor` / `epoch`, so no caller does SAL address
    /// arithmetic. Only `next` and `checkpoint_reset` touch them; recovery walks
    /// are stateless and drive their own offset.
    read_cursor: std::cell::Cell<u64>,
    expected_epoch: std::cell::Cell<u32>,
}

unsafe impl Send for SalReader {}

impl SalReader {
    /// `expected_epoch` is the generation the live drain accepts: the recovered
    /// walk epoch plus one, matching what `SalWriter::boot_reset` starts the
    /// master at. Recovery walkers never consult it — they read at
    /// [`EpochGate::Walk`] / [`EpochGate::Any`].
    pub fn new(ptr: *const u8, worker_id: u32, mmap_size: usize, m2w_efd: i32, expected_epoch: u32) -> Self {
        SalReader {
            ptr,
            worker_id,
            mmap_size: mmap_size as u64,
            m2w_efd,
            read_cursor: std::cell::Cell::new(0),
            expected_epoch: std::cell::Cell::new(expected_epoch),
        }
    }

    /// The next group for this worker, advancing the cursor only on a clean
    /// read. A group from another epoch stays parked at the cursor until
    /// [`checkpoint_reset`](Self::checkpoint_reset) catches up.
    ///
    /// A digest failure here is corruption, and the response is a fail-stop: a
    /// live reader is never behind the writer's epoch, so every leftover is
    /// rejected by [`EpochGate::Live`]'s prefix gate before a header byte is
    /// read, and a gate-passing prefix implies a fully published header (`commit`
    /// Release-stores the prefix last). Parking instead would be indistinguishable
    /// from having caught up — `next_sal_message` maps `None` to end-of-drain — so
    /// the worker would go quiet and the committer would wait forever on an ACK
    /// nobody will send.
    pub fn next(&self) -> Option<SalMessage<'static>> {
        let cursor = self.read_cursor.get();
        match self.read_at(cursor, EpochGate::Live(self.expected_epoch.get())) {
            SalStep::Group(msg, new_cursor) => {
                self.read_cursor.set(new_cursor);
                Some(msg)
            }
            SalStep::Corrupt => {
                gnitz_fatal_abort!("SAL group header failed its digest at offset={cursor} — the log is damaged")
            }
            SalStep::Absent | SalStep::OtherEpoch => None,
        }
    }

    /// Rewind to the start of the next epoch, mirroring the writer's
    /// `SalWriter::checkpoint_reset` on the read side. Groups the master writes
    /// post-reset (at `write_cursor == 0`, in the bumped epoch) are then
    /// accepted, and any pre-reset group parks.
    pub fn checkpoint_reset(&self) {
        self.read_cursor.set(0);
        self.expected_epoch.set(self.expected_epoch.get() + 1);
    }

    /// The SAL mmap size this reader was opened with — not necessarily the
    /// process-global `sal_mmap_size()`, since tests open readers over regions
    /// smaller than its floor.
    #[inline]
    pub fn mmap_size(&self) -> u64 {
        self.mmap_size
    }

    /// A reader for a quiescent recovery walk over `worker_id`'s slots: it never
    /// waits on the eventfd and never consults its own epoch — the walk supplies
    /// one through [`EpochGate`].
    pub fn for_walk(ptr: *const u8, worker_id: u32, mmap_size: usize) -> Self {
        SalReader::new(ptr, worker_id, mmap_size, -1, 0)
    }

    /// Classify the bytes at `cursor` for this reader's worker slot, building
    /// the message and the next cursor when they are a readable group.
    fn read_at(&self, cursor: u64, gate: EpochGate) -> SalStep {
        if cursor + 8 > self.mmap_size {
            return SalStep::Absent;
        }
        unsafe { sal_read_group_header(self.ptr, cursor, self.worker_id, gate, self.mmap_size) }
    }

    /// The group at `cursor` and the cursor past it, or `None` when the bytes are
    /// not a group this gate accepts.
    #[cfg(test)]
    pub fn try_read(&self, cursor: u64, gate: EpochGate) -> Option<(SalMessage<'static>, u64)> {
        match self.read_at(cursor, gate) {
            SalStep::Group(msg, next) => Some((msg, next)),
            _ => None,
        }
    }

    /// Every 8-byte-aligned candidate at or after `from` whose header verifies,
    /// as `(base, epoch)`. Group bases are 8-aligned — `payload_size` is
    /// always a multiple of 8 and bases start at 0 — so the stride cannot step
    /// over one. Testing the prefix word before the digest is what bounds the
    /// cost: the zero run filling a partly-used ring pays no hashing at all.
    fn valid_headers_from(&self, from: u64) -> impl Iterator<Item = (u64, u32)> + '_ {
        (from..self.mmap_size).step_by(8).filter_map(move |base| {
            if base + 8 > self.mmap_size || unsafe { sal_prefix_word(self.ptr, base) } == 0 {
                return None;
            }
            let (_, epoch) = unsafe { sal_probe_header(self.ptr, base, self.mmap_size) }?;
            Some((base, epoch))
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
    pub fn walk_epoch(&self) -> u32 {
        if let Some((_, epoch)) = unsafe { sal_probe_header(self.ptr, 0, self.mmap_size) } {
            return epoch;
        }
        if unsafe { sal_prefix_word(self.ptr, 0) } == 0 {
            return 0;
        }
        self.valid_headers_from(0).map(|(_, e)| e).max().unwrap_or(0)
    }

    /// Slot `w` of the group published at `base`, independent of this reader's
    /// own worker id — group headers are slot-independent, so recovery can
    /// validate a zone across every slot its groups declare.
    pub fn slot_at(&self, base: u64, w: u32) -> Option<&'static [u8]> {
        match unsafe { sal_read_group_header(self.ptr, base, w, EpochGate::Any, self.mmap_size) } {
            SalStep::Group(msg, _) => msg.wire_data,
            _ => None,
        }
    }

    pub fn wait(&self, timeout_ms: i32) -> i32 {
        posix::eventfd_wait(self.m2w_efd, timeout_ms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `SAL_FULL` reaches clients as text, and the E2E reader loop in
    /// `crates/gnitz-py/tests/test_sal_read_reclaim.py` matches that text to tell
    /// a transient refusal (retry) from a real failure (fail the test). Nothing
    /// links the two literals, so editing this one silently turns those tests
    /// back into the coin flip they were: the reader would treat every refusal as
    /// fatal. Change both together.
    #[test]
    fn sal_full_marker_is_the_literal_the_e2e_readers_match() {
        assert_eq!(SAL_FULL, "SAL full");
    }
}
