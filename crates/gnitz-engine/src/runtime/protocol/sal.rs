//! SAL (shared append-only log): master→worker broadcast channel.
//!
//! Owns the mmap layout, group-header write/read helpers, SalWriter,
//! SalMessage, SalReader, and the atomic primitives used by the SAL.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::foundation::posix_io;
use crate::foundation::posix_io::{read_u32_raw, read_u64_raw, write_u32_raw, write_u64_raw};
use crate::runtime::wire::{
    build_schema_wire_block, encode_ctrl_block_direct, layout_to_wire_flags, WireData, WireMsg,
    CTRL_BLOCK_SIZE_NO_BLOB, FLAG_HAS_DATA, FLAG_HAS_SCHEMA, STATUS_OK,
};
use crate::schema::SchemaDescriptor;
use crate::storage::{carve_writer_slices, scatter_copy, wire_header_dir_size, wire_region_sizes, Batch, DirectWriter};
use gnitz_wire::align8;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const MAX_WORKERS: usize = 64;

/// Leading marker of every out-of-space refusal from `SalWriter::begin`, and
/// the whole contract a caller may match on. The condition is transient by
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

/// XXH3-64 over a SAL group header — its own eight bytes excluded — seeded with
/// the group's byte offset in the ring, so a header only verifies where it was
/// published. The epoch is not a seed: it sits in the hashed span, so verifying a
/// header also authenticates the generation it claims.
fn group_digest(base: u64, hdr: &[u8]) -> u64 {
    crate::foundation::xxh::digest_with_hole(&base.to_le_bytes(), hdr, OFF_DIGEST)
}

pub(crate) const SAL_MMAP_SIZE: usize = 1 << 30;

/// The SAL slot size for one worker's share of a **wire-safe** group: the
/// control block, the schema block, and — only if the worker gets rows — the
/// columnar data block. The single formula behind both `scatter_wire_group`'s
/// emission and `wire_group_footprint`'s fit check, which must agree
/// byte-for-byte: the committer fail-stops (aborts the node) if a transaction
/// family fails to fit after an earlier family already hit the SAL.
fn wire_safe_slot_size(
    schema: &SchemaDescriptor,
    count_w: usize,
    wire_row_stride: u32,
    schema_block_len: usize,
) -> usize {
    let data_sz = if count_w > 0 {
        data_wire_block_size_cached(schema, count_w, wire_row_stride)
    } else {
        0
    };
    CTRL_BLOCK_SIZE_NO_BLOB + schema_block_len + data_sz
}

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
        crate::foundation::env::env_usize("GNITZ_SAL_BYTES", SAL_MMAP_SIZE).clamp(MIN_SAL_BYTES, SAL_MMAP_SIZE)
    })
}

// SAL group header flags (u32): the shared `gnitz_wire` flag bits re-declared
// as `u32` (the SAL group header's flag width).
pub const FLAG_SHUTDOWN: u32 = gnitz_wire::FLAG_SHUTDOWN as u32;
pub const FLAG_DDL_SYNC: u32 = gnitz_wire::FLAG_DDL_SYNC as u32;
pub const FLAG_EXCHANGE: u32 = gnitz_wire::FLAG_EXCHANGE as u32;
pub const FLAG_PUSH: u32 = gnitz_wire::FLAG_PUSH as u32;
pub const FLAG_HAS_PK: u32 = gnitz_wire::FLAG_HAS_PK as u32;
pub const FLAG_SEEK: u32 = gnitz_wire::FLAG_SEEK as u32;
pub const FLAG_SEEK_BY_INDEX: u32 = gnitz_wire::FLAG_SEEK_BY_INDEX as u32;
/// Parameterized bounded read (`ReadSpec`). Lives inside the wire SAL flag block
/// (bit 10) and is carried verbatim from the client wire frame into the SAL group
/// header — one allocation, mirrored here as a `u32` (unlike the high client-only
/// request bits, which need a distinct u32 SAL dispatch flag).
pub const FLAG_SCAN_SPEC: u32 = gnitz_wire::FLAG_SCAN_SPEC as u32;
// The next five are engine-internal in meaning but live in the shared 0-15
// block, so `gnitz_wire` owns the bit (and its collision guard) while the
// meaning stays here.
pub const FLAG_EXCHANGE_RELAY: u32 = gnitz_wire::FLAG_EXCHANGE_RELAY as u32;
pub const FLAG_BACKFILL: u32 = gnitz_wire::FLAG_BACKFILL as u32;
pub const FLAG_TICK: u32 = gnitz_wire::FLAG_TICK as u32;
pub const FLAG_FLUSH: u32 = gnitz_wire::FLAG_FLUSH as u32;
/// Ephemeral-state flush round of the checkpoint sequence: flush every view's
/// operator-trace tables and output stores (traces before outputs), stamping
/// their manifests with the checkpoint generation carried in the group header's
/// `lsn` field. Dispatched inline in both worker contexts like `FLAG_FLUSH`, but
/// distinct so the base round (`FLAG_FLUSH`, `SalReplay` user tables) and the
/// ephemeral round (`Rederive` view state) stay separate handlers. Bit 19.
pub const FLAG_FLUSH_EPH: u32 = 1 << 19;
/// Marks an empty broadcast group as the closing "commit sentinel" of an
/// atomic zone. All preceding groups at the same LSN belong to the zone;
/// recovery applies them only when this sentinel is on disk. The flag
/// rides on top of FLAG_DDL_SYNC for the worker's dispatch loop, which
/// already no-ops on a DDL_SYNC group with `count == 0`.
pub const FLAG_TXN_COMMIT: u32 = gnitz_wire::FLAG_TXN_COMMIT as u32;
/// Batched stored-row gather: scatter a set of PKs to their owning workers,
/// each worker reads the committed rows for the PKs it owns and replies with
/// the rows projected to the columns named in the control block's
/// `seek_col_idx` mask (see `pack_gather_cols`). Distinct from `FLAG_SEEK`
/// (single key) and `FLAG_HAS_PK` (existence echo of the caller's payload);
/// the gather returns the *stored* value of columns the caller does not have.
pub const FLAG_GATHER: u32 = 65536;
/// CREATE UNIQUE INDEX global pre-flight: each worker projects its committed
/// partition of `target_id` to the OPK leading-key spans of the column list
/// packed in `seek_col_idx`, sorts them, and streams the SORTED spans back as
/// continuation frames for the master's k-way merge (see
/// `validate_unique_index_create`). Unicast-shaped like a Scan: every
/// worker gets its own req_id slot and answers with a frame train.
pub const FLAG_UNIQUE_PREFLIGHT: u32 = 131072;
/// The first group of an atomic zone. With the closing `FLAG_TXN_COMMIT`
/// sentinel it delimits the zone's byte span, which is how recovery tells damage
/// that cost a committed transaction a group from damage in one of the `lsn = 0`
/// command groups between zones. `KIND_BY_FLAG` does not list it, so it changes
/// no dispatch.
pub const FLAG_ZONE_START: u32 = 262144;

// The flags above that `gnitz_wire` does not allocate are the engine's own, and
// stay strictly above the shared 0-15 block — that separation is what lets the
// two crates allocate independently. `gnitz_wire`'s guard covers the block
// itself; this covers the engine's side of the line.
const _: () = {
    let engine_only = [FLAG_FLUSH_EPH, FLAG_GATHER, FLAG_UNIQUE_PREFLIGHT, FLAG_ZONE_START];
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
// Chunked distributed-backfill exchange coordination
// ---------------------------------------------------------------------------
//
// A distributed CREATE-VIEW backfill streams the source partition through the
// incremental plan one chunk at a time, issuing one exchange round per chunk
// per exchanging view. All workers must issue the SAME number of rounds (short
// partitions pad with empty rounds), so termination and SAL reclamation are
// decided collectively by the master and stamped back on each relay. Both legs
// reuse the otherwise-unused `seek_col_idx` control field — no new SAL flag and
// no wire-format change. The value `0` doubles as "no backfill coordination",
// so steady-state exchanges (which already pass a literal `0`) are unaffected.

/// Up-leg (worker→master, on `FLAG_EXCHANGE`): the per-chunk PAD bit. Set when
/// this worker's `drain_chunk` returned `None` — its partition is exhausted and
/// the chunk it is participating in is an empty pad. The master ANDs this bit
/// across all workers for a round; an all-pad round is the final round.
pub const BACKFILL_PAD_BIT: u64 = 1;

/// Down-leg (master→worker, on `FLAG_EXCHANGE_RELAY`): the collective decision
/// the master stamps onto a round's relay after ANDing the round's pad bits and
/// checking SAL space. `CONTINUE` keeps the loop going; `STOP` ends every
/// worker's loop on the same (all-pad) round; `CHECKPOINT` is a continue that
/// also tells the worker to advance its SAL read epoch + reset its read cursor
/// inline (the master reclaims the SAL write side at the next round barrier).
pub const BACKFILL_DECISION_CONTINUE: u64 = 0;
pub const BACKFILL_DECISION_STOP: u64 = 1;
pub const BACKFILL_DECISION_CHECKPOINT: u64 = 2;

/// Wire schema of every unique pre-flight reply frame: the leading `n_promoted`
/// columns of the index schema, all marked PK, schema version 0. Its `pk_stride`
/// is exactly `idx_key_size`, and the OPK leading-key span fills that PK region
/// verbatim — there is no single fixed-width column to represent a composite
/// (e.g. 24-byte) span, so the schema is built per-index from `idx_schema` (the
/// width is known at pre-flight time). The single-column ≤16-byte case is the
/// `n_promoted == 1` degenerate, replacing the old fixed `U128` column.
///
/// Index columns are always non-nullable (`make_index_schema` builds each with
/// `nullable = 0`, and a NULL-valued row never enters the index), so
/// `SchemaDescriptor::new`'s "PK columns must be non-nullable" assertion holds.
/// The single definition shared by the worker's encoder
/// (`send_unique_preflight_keys`) and the master's merge decoder, so the frame
/// layout agrees by construction.
pub(crate) fn unique_preflight_wire_schema(idx_schema: &SchemaDescriptor, n_promoted: usize) -> SchemaDescriptor {
    let cols = &idx_schema.columns[..n_promoted];
    let pks: Vec<u32> = (0..n_promoted as u32).collect();
    SchemaDescriptor::new(cols, &pks)
}

/// Pack up to 8 projected column indices into the gather control block's
/// `seek_col_idx`. Each index is stored as `col_idx + 1` in one byte (valid
/// because MAX_COLUMNS = 65 ≤ 255); a zero byte terminates the list and
/// keeps column index 0 representable. Returns `None` if more than 8 distinct
/// columns must be projected (caller falls back to a wider gather).
pub(crate) fn pack_gather_cols(cols: &[u8]) -> Option<u64> {
    if cols.len() > 8 {
        return None;
    }
    let mut packed = 0u64;
    for (i, &c) in cols.iter().enumerate() {
        debug_assert!((c as usize) < crate::schema::MAX_COLUMNS);
        packed |= ((c as u64) + 1) << (8 * i);
    }
    Some(packed)
}

/// Inverse of `pack_gather_cols`: yields the projected column indices.
pub(crate) fn unpack_gather_cols(packed: u64) -> impl Iterator<Item = u8> {
    (0..8)
        .map(move |i| ((packed >> (8 * i)) & 0xff) as u8)
        .take_while(|&b| b != 0)
        .map(|b| b - 1)
}

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
    ScanSpec,
    Scan,
}

/// Flag bit → kind, in priority order; the first bit set on the group wins.
/// Each kind owns a distinct bit, so only `Shutdown` leading and the two flush
/// rounds preceding the rest actually constrains anything.
const KIND_BY_FLAG: [(u32, SalMessageKind); 14] = [
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
    /// Unicast kinds (SEEK, SEEK_BY_INDEX, Scan, UniquePreflight,
    /// ExchangeRelay) return false: a missing slot means the message
    /// wasn't for us.
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
pub(crate) unsafe fn atomic_store_u64(ptr: *mut u8, val: u64) {
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
// Scatter-to-wire helpers
// ---------------------------------------------------------------------------

/// Compute the byte size of the data WAL block for `count` rows on `schema`
/// with a precomputed `wire_row_fixed_stride` (see
/// `crate::storage::compute_wire_props`). Only correct when the schema is
/// `wire_safe` (caller's responsibility) — no alignment padding, so the size
/// is the fixed header/directory prefix plus `count` linear rows.
#[inline]
fn data_wire_block_size_cached(schema: &SchemaDescriptor, count: usize, stride: u32) -> usize {
    wire_header_dir_size(schema) + count * stride as usize
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
    /// `worker_sizes` must be the slice this group was begun with.
    pub(crate) unsafe fn for_each_slot(&self, worker_sizes: &[u32], mut f: impl FnMut(usize, &mut [u8])) {
        let mut off = self.hdr_size;
        for (w, &sz) in worker_sizes.iter().enumerate() {
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

    // payload_size starts as the header size (a multiple of 8) and grows only by
    // align8(sz) increments, so it is always a multiple of 8.
    let hdr_size = group_header_size(worker_sizes.len());
    let mut payload_size = hdr_size;
    for &sz in worker_sizes {
        if sz > 0 {
            payload_size += align8(sz as usize);
        }
    }
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

    Some(SalGroup {
        sal_ptr,
        base,
        hdr_size,
        payload_size,
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

    let mut off = group_header_size(nw);
    for p in payloads {
        if !p.is_empty() {
            std::ptr::copy_nonoverlapping(p.as_ptr(), group.data_ptr(off), p.len());
            off += align8(p.len());
        }
    }

    Some(group.commit())
}

// ---------------------------------------------------------------------------
// SAL read (worker reads its data from a group)
// ---------------------------------------------------------------------------

pub(crate) struct SalReadResult {
    pub advance: u64,
    pub lsn: u64,
    pub flags: u32,
    pub target_id: u32,
    /// The slot count the group was written with. A reader asking for a slot at
    /// or past it gets an empty slot.
    pub slots: u32,
    /// This worker's payload slot; null/0 when the group carries no data
    /// for this worker (control broadcast, other-worker unicast).
    pub data_ptr: *const u8,
    pub data_size: u32,
}

impl SalReadResult {
    /// The requested slot's bytes, or `None` when the group left it empty.
    fn wire_data(&self) -> Option<&'static [u8]> {
        (self.data_size > 0 && !self.data_ptr.is_null())
            .then(|| unsafe { std::slice::from_raw_parts(self.data_ptr, self.data_size as usize) })
    }
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
pub(crate) unsafe fn sal_prefix_word(sal_ptr: *const u8, base: u64) -> u64 {
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

/// What the bytes at an offset are.
pub(crate) enum SalRead {
    /// Nothing published here, or the group's own stride runs past this mapping
    /// (the log was written under a larger `GNITZ_SAL_BYTES`). Either way, the
    /// end of the log.
    Absent,
    /// A published header that fails its digest.
    Corrupt,
    /// A verified header stamped with another epoch — the ring's leftovers.
    OtherEpoch,
    Group(SalReadResult),
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
) -> SalRead {
    let rc = read_cursor as usize;
    let wid = worker_id as usize;

    let word = sal_prefix_word(sal_ptr, read_cursor);
    if word == 0 {
        return SalRead::Absent;
    }
    if let EpochGate::Live(exp) = gate {
        if (word >> 32) as u32 != exp {
            return SalRead::OtherEpoch;
        }
    }

    let Some((slots, epoch)) = sal_probe_header(sal_ptr, read_cursor, mmap_size) else {
        return SalRead::Corrupt;
    };
    if gate.epoch().is_some_and(|exp| epoch != exp) {
        return SalRead::OtherEpoch;
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
        return SalRead::Absent;
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
    SalRead::Group(SalReadResult {
        advance,
        lsn,
        flags,
        target_id,
        slots: slots as u32,
        data_ptr,
        data_size,
    })
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

/// Write a WAL data block for `count` rows into `data_slot` by scattering
/// `indices` from `batch`. Assumes `schema_wire_safe` — no German-string columns,
/// all strides are multiples of 8, so all align8 calls are no-ops.
fn write_scattered_data_block(
    batch: &crate::storage::MemBatch<'_>,
    indices: &[u32],
    schema: &SchemaDescriptor,
    count: usize,
    table_id: u32,
    data_slot: &mut [u8],
) {
    let total_size = data_slot.len();

    // Region sizes in canonical order: pk, weight, null_bmp, payload…, blob(0).
    // All strides % 8 == 0 (schema_wire_safe), so the shared header/directory
    // writer's align8 padding never fires and the body is one contiguous run.
    let (sizes, nr) = wire_region_sizes(schema, count, 0);
    crate::storage::wal_write_header_and_directory(data_slot, table_id, count as u32, &sizes[..nr], total_size);

    // The writer carves `rest` (body after header+directory) into per-region
    // slices: [pk | weight | null | col_0 | ...], each sized for `count` rows.
    let (_, rest) = data_slot.split_at_mut(wire_header_dir_size(schema));
    let (pk, weight, null_bmp, col_slices) = carve_writer_slices(rest, schema, count);
    // No German-string columns on this fast path; DirectWriter still wants a blob
    // arena, so hand it a 0-cap stack-local that scatter_copy must not grow.
    let mut empty_blob: Vec<u8> = Vec::new();
    let mut writer = DirectWriter::new(pk, weight, null_bmp, col_slices, &mut empty_blob, schema, 0);
    scatter_copy(batch, indices, &[], &mut writer);
    debug_assert!(
        empty_blob.is_empty(),
        "non-string SAL fast path must not write blob bytes"
    );

    gnitz_wire::wal::stamp_checksum(data_slot, total_size);
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
    m2w_efds: Vec<i32>,
}

unsafe impl Send for SalWriter {}

impl SalWriter {
    pub fn new(ptr: *mut u8, fd: i32, mmap_size: u64, m2w_efds: Vec<i32>) -> Self {
        let checkpoint_threshold = crate::foundation::env::env_u64("GNITZ_CHECKPOINT_BYTES", (mmap_size * 3) >> 2);
        SalWriter {
            ptr,
            fd,
            mmap_size,
            write_cursor: std::cell::Cell::new(0),
            epoch: std::cell::Cell::new(0),
            checkpoint_threshold,
            m2w_efds,
        }
    }

    /// SAL bytes an ordinary group may occupy: the mapping minus the headroom
    /// `sal_begin_group` reserves for the zone-closing sentinel and minus the
    /// band held back for the checkpoint and shutdown groups. A transaction whose
    /// footprint exceeds this can never be written on any cursor, so `txn_fit`
    /// must call it `Terminal` rather than loop on `Transient`.
    pub(crate) fn effective_capacity(&self) -> usize {
        effective_max(0, self.mmap_size as usize)
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
            crate::gnitz_debug!(
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
    /// `prebuilt_schema_block`: when `Some`, the bytes are copied into each
    /// slot's schema region instead of building one from `schema`.
    ///
    /// `schema: None` (with no prebuilt block) emits no schema block at all —
    /// the command verbs whose worker arm resolves its own schema from its own
    /// catalog. A group carrying data must always name its schema: that is what
    /// stamps `Batch.schema` on the worker side, and `decode_wire` hard-errors on
    /// FLAG_HAS_DATA without FLAG_HAS_SCHEMA, so the reply's request_id would fall
    /// back to 0 and the master's `ReplyFuture` would never resolve.
    #[allow(clippy::too_many_arguments, clippy::needless_range_loop)]
    pub fn write_group_direct(
        &self,
        target_id: u32,
        lsn: u64,
        sal_flags: u32,
        wire_flags: u64,
        worker_batches: &[Option<&Batch>],
        schema: Option<&SchemaDescriptor>,
        seek_pk: u128,
        seek_col_idx: u64,
        req_ids: &[u64],
        unicast_worker: i32,
        client_id: u64,
        prebuilt_schema_block: Option<&[u8]>,
        seek_pk_extra: &[u8],
    ) -> Result<(), String> {
        let nw = self.m2w_efds.len();
        assert_eq!(
            req_ids.len(),
            nw,
            "write_group_direct: req_ids.len()={} != num_workers={}",
            req_ids.len(),
            nw
        );
        debug_assert!(
            schema.is_some() || prebuilt_schema_block.is_some() || worker_batches.iter().all(|b| b.is_none()),
            "write_group_direct: data without a schema — `decode_wire` rejects FLAG_HAS_DATA \
             without FLAG_HAS_SCHEMA",
        );

        // One message per worker slot — the slot's size and its bytes come from
        // the same value, so they cannot disagree.
        let msg_for = |w: usize| WireMsg {
            target_id: target_id as u64,
            client_id,
            flags: wire_flags,
            seek_pk,
            seek_col_idx,
            request_id: req_ids[w],
            schema,
            data: WireData::Whole(worker_batches.get(w).and_then(|opt| *opt)),
            prebuilt_schema_block,
            seek_pk_extra,
            ..Default::default()
        };

        let mut worker_sizes = [0u32; MAX_WORKERS];
        for w in 0..nw {
            if unicast_worker >= 0 && w != unicast_worker as usize {
                continue;
            }
            worker_sizes[w] = msg_for(w).size() as u32;
        }

        let group = self.begin("write_group_direct", target_id, lsn, sal_flags, &worker_sizes[..nw])?;

        unsafe {
            group.for_each_slot(&worker_sizes[..nw], |w, slot| {
                let written = msg_for(w).encode(slot, 0);
                debug_assert_eq!(written, slot.len());
            });
        }

        self.finish(group);
        Ok(())
    }

    /// The exact number of SAL bytes a `scatter_wire_group` (or its
    /// `write_group_direct` fallback) emission of `input_batch` partitioned by
    /// `worker_indices` will consume: `8 + GROUP_HEADER_SIZE + Σ_w align8(slot_w)`,
    /// where every one of the `num_workers` slots is emitted (a zero-row worker
    /// still gets a schema-only `ctrl + schema_block` slot). Byte-exact by
    /// construction — each slot is sized through the identical formula the
    /// matching emission path uses (the wire-safe closed form, or `Batch`'s wire
    /// size over a materialized per-worker sub-batch for string/blob schemas), so
    /// the committer's per-transaction fit check cannot drift from what emission
    /// writes. `schema_block_len` is the prebuilt schema block length (paid per
    /// slot, hence once per worker for a replicated family); `wire_props` is
    /// `(wire_safe, wire_row_stride)` as from `cached_schema_block`.
    ///
    /// Neither the sentinel nor the checkpoint band is included — both are
    /// globally held back (`effective_capacity`), not counted per group.
    pub(crate) fn wire_group_footprint(
        &self,
        input_batch: &Batch,
        worker_indices: &[Vec<u32>],
        schema: &SchemaDescriptor,
        schema_block_len: usize,
        wire_props: (bool, u32),
    ) -> usize {
        let nw = self.m2w_efds.len();
        let (wire_safe, wire_row_stride) = wire_props;
        let mut total = 8 + group_header_size(nw);
        if wire_safe {
            for wi in worker_indices.iter().take(nw) {
                total += align8(wire_safe_slot_size(schema, wi.len(), wire_row_stride, schema_block_len));
            }
        } else {
            let mb = input_batch.as_mem_batch();
            for wi in worker_indices.iter().take(nw) {
                let slot = if wi.is_empty() {
                    CTRL_BLOCK_SIZE_NO_BLOB + schema_block_len
                } else {
                    let sub = Batch::from_indexed_rows(&mb, wi, &[], schema);
                    CTRL_BLOCK_SIZE_NO_BLOB + schema_block_len + sub.wire_byte_size()
                };
                total += align8(slot);
            }
        }
        total
    }

    /// Scatter rows from `input_batch` directly into per-worker SAL slots using
    /// pre-computed `worker_indices`. Eliminates the two-copy path
    /// (scatter→intermediate Batch, then Batch→SAL slot) for schemas where
    /// every column has a fixed-width 8-aligned stride and no German-string columns.
    ///
    /// Falls back to `write_group_direct` (two-copy) for other schemas.
    /// Does NOT sync/signal. `lsn` is supplied by the caller.
    ///
    /// `prebuilt_schema_block`: when `Some`, the bytes are copied into each
    /// slot's schema region instead of building a nameless one from `schema`.
    /// `wire_props`: `(wire_safe, wire_row_fixed_stride)` derived from the
    /// schema. When `Some`, the values are reused directly so the function
    /// avoids the per-call column iteration; when `None`, they're computed
    /// inline. `wire_row_fixed_stride` is only meaningful when `wire_safe`.
    #[allow(clippy::too_many_arguments)]
    pub fn scatter_wire_group(
        &self,
        input_batch: &Batch,
        worker_indices: &[Vec<u32>],
        schema: &SchemaDescriptor,
        target_id: u32,
        lsn: u64,
        sal_flags: u32,
        wire_flags: u64,
        seek_col_idx: u64,
        req_ids: &[u64],
        prebuilt_schema_block: Option<&[u8]>,
        wire_props: Option<(bool, u32)>,
    ) -> Result<(), String> {
        let nw = self.m2w_efds.len();
        assert_eq!(
            req_ids.len(),
            nw,
            "scatter_wire_group: req_ids.len()={} != num_workers={}",
            req_ids.len(),
            nw
        );

        let (wire_safe, wire_row_stride) = wire_props.unwrap_or_else(|| crate::storage::compute_wire_props(schema));

        if !wire_safe {
            // Fallback: reconstruct per-worker Batches and use existing path.
            let mb = input_batch.as_mem_batch();
            let sub_batches: Vec<Batch> = worker_indices
                .iter()
                .map(|indices| {
                    if !indices.is_empty() {
                        Batch::from_indexed_rows(&mb, indices, &[], schema)
                    } else {
                        Batch::empty_with_schema(schema)
                    }
                })
                .collect();
            let refs: Vec<Option<&Batch>> = sub_batches
                .iter()
                .map(|b| if b.count > 0 { Some(b) } else { None })
                .collect();
            return self.write_group_direct(
                target_id,
                lsn,
                sal_flags,
                wire_flags,
                &refs,
                Some(schema),
                0,
                seek_col_idx,
                req_ids,
                -1,
                0,
                prebuilt_schema_block,
                &[],
            );
        }

        // Fast path: scatter directly into SAL slots. The schema block is
        // either supplied prebuilt (cached at the caller) or built once here
        // (nameless) and reused across all worker slots.
        let owned_block: Vec<u8>;
        let schema_block: &[u8] = match prebuilt_schema_block {
            Some(b) => b,
            None => {
                owned_block = build_schema_wire_block(schema, target_id);
                &owned_block
            }
        };
        let mut worker_sizes = [0u32; MAX_WORKERS];
        for w in 0..nw {
            worker_sizes[w] =
                wire_safe_slot_size(schema, worker_indices[w].len(), wire_row_stride, schema_block.len()) as u32;
        }

        let group = self.begin("scatter_wire_group", target_id, lsn, sal_flags, &worker_sizes[..nw])?;

        let mb = input_batch.as_mem_batch();
        // The ctrl block size on the no-error fast path is a compile-time constant.
        let ctrl_size = CTRL_BLOCK_SIZE_NO_BLOB;
        unsafe {
            group.for_each_slot(&worker_sizes[..nw], |w, slot| {
                let count_w = worker_indices[w].len();

                // a. Schema block immediately after the ctrl slot.
                slot[ctrl_size..ctrl_size + schema_block.len()].copy_from_slice(schema_block);

                // b. Data block when there are rows for this worker.
                if count_w > 0 {
                    let data_start = ctrl_size + schema_block.len();
                    let data_sz = data_wire_block_size_cached(schema, count_w, wire_row_stride);
                    let data_slot = &mut slot[data_start..data_start + data_sz];
                    write_scattered_data_block(&mb, &worker_indices[w], schema, count_w, target_id, data_slot);
                }

                // c. Ctrl block last (needs full_wire_flags which depends on count_w).
                let full_wire_flags = wire_flags
                    | FLAG_HAS_SCHEMA
                    | if count_w > 0 { FLAG_HAS_DATA } else { 0 }
                    | layout_to_wire_flags(input_batch.layout());
                encode_ctrl_block_direct(
                    slot,
                    0,
                    &gnitz_wire::control::ControlHeader {
                        status: STATUS_OK,
                        target_id: target_id as u64,
                        flags: full_wire_flags,
                        seek_col_idx,
                        request_id: req_ids[w],
                        ..Default::default()
                    },
                    b"",
                    &[],
                    // Checksummed like every other block in the slot: SAL replay
                    // verifies the control block it decodes, and one XXH3 over it
                    // is nothing against the transaction's own `fdatasync`.
                    true,
                );
            });
        }

        self.finish(group);
        Ok(())
    }

    /// Encode once into worker 0's slot, memcpy to workers 1..N-1.
    /// Does NOT sync/signal. `lsn` is supplied by the caller.
    ///
    /// `prebuilt_schema_block`: when `Some`, the bytes are copied into the
    /// schema region instead of being built from `schema`.
    ///
    /// `schema: None` emits no schema block, as in `write_group_direct` — the
    /// control-only broadcasts (`FLAG_FLUSH`/`FLAG_FLUSH_EPH`/`FLAG_SHUTDOWN`)
    /// whose worker arm takes neither a schema nor a batch.
    #[allow(clippy::too_many_arguments)]
    pub fn write_broadcast_direct(
        &self,
        target_id: u32,
        lsn: u64,
        sal_flags: u32,
        batch: Option<&Batch>,
        schema: Option<&SchemaDescriptor>,
        seek_pk: u128,
        prebuilt_schema_block: Option<&[u8]>,
    ) -> Result<(), String> {
        let nw = self.m2w_efds.len();
        debug_assert!(
            schema.is_some() || prebuilt_schema_block.is_some() || batch.is_none(),
            "write_broadcast_direct: data without a schema — `decode_wire` rejects FLAG_HAS_DATA \
             without FLAG_HAS_SCHEMA",
        );

        let msg = WireMsg {
            target_id: target_id as u64,
            seek_pk,
            schema,
            data: WireData::Whole(batch),
            prebuilt_schema_block,
            ..Default::default()
        };
        let wsz = msg.size() as u32;
        let mut worker_sizes = [0u32; MAX_WORKERS];
        worker_sizes[..nw].fill(wsz);

        let group = self.begin("write_broadcast_direct", target_id, lsn, sal_flags, &worker_sizes[..nw])?;

        if wsz > 0 {
            let wsz = wsz as usize;
            let slot0_off = group.hdr_size;
            let slot0 = unsafe { std::slice::from_raw_parts_mut(group.data_ptr(slot0_off), wsz) };
            let written = msg.encode(slot0, 0);
            debug_assert_eq!(written, wsz);
            let mut off = slot0_off + align8(wsz);
            for _ in 1..nw {
                unsafe {
                    std::ptr::copy_nonoverlapping(group.data_ptr(slot0_off), group.data_ptr(off), wsz);
                }
                off += align8(wsz);
            }
        }

        self.finish(group);
        Ok(())
    }

    /// Write an empty commit sentinel for an atomic zone.
    ///
    /// A slotless group carrying `FLAG_DDL_SYNC | FLAG_TXN_COMMIT`. Recovery uses
    /// the sentinel as the "this LSN is closed" mark — without it, all groups at
    /// this LSN are skipped. Every worker still sees it (the flags live in the
    /// group header, which is not per-slot) and it is inert under the worker's hot
    /// path: the FLAG_DDL_SYNC branch no-ops on a group with no batch.
    pub fn write_commit_sentinel(&self, lsn: u64) -> Result<(), String> {
        let group = self.begin("write_commit_sentinel", 0, lsn, FLAG_DDL_SYNC | FLAG_TXN_COMMIT, &[])?;
        self.finish(group);
        Ok(())
    }

    pub fn signal_all(&self) {
        for w in 0..self.m2w_efds.len() {
            posix_io::eventfd_signal(self.m2w_efds[w]);
        }
    }

    pub fn signal_one(&self, worker: usize) {
        posix_io::eventfd_signal(self.m2w_efds[worker]);
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
// SalMessage + SalReader
// ---------------------------------------------------------------------------

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
    /// not depend on which worker asked.
    pub slots: u32,
    /// None = no data for this worker in this group.
    pub wire_data: Option<&'a [u8]>,
}

/// [`SalRead`] at message level: what a reader found at a cursor, with the next
/// cursor alongside a readable group.
pub enum SalStep {
    Absent,
    Corrupt,
    OtherEpoch,
    Group(SalMessage<'static>, u64),
}

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
    /// Release-stores the prefix last). Parking instead would read as a silent
    /// end-of-drain and the committer would wait forever on its ACK. The argument
    /// in full is in `async-invariants.md`.
    pub fn next(&self) -> Option<SalMessage<'static>> {
        let cursor = self.read_cursor.get();
        match self.read_at(cursor, EpochGate::Live(self.expected_epoch.get())) {
            SalStep::Group(msg, new_cursor) => {
                self.read_cursor.set(new_cursor);
                Some(msg)
            }
            SalStep::Corrupt => {
                crate::gnitz_fatal_abort!("SAL group header failed its digest at offset={cursor} — the log is damaged")
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
    pub fn read_at(&self, cursor: u64, gate: EpochGate) -> SalStep {
        if cursor + 8 > self.mmap_size {
            return SalStep::Absent;
        }
        match unsafe { sal_read_group_header(self.ptr, cursor, self.worker_id, gate, self.mmap_size) } {
            SalRead::Absent => SalStep::Absent,
            SalRead::Corrupt => SalStep::Corrupt,
            SalRead::OtherEpoch => SalStep::OtherEpoch,
            SalRead::Group(r) => SalStep::Group(
                SalMessage {
                    lsn: r.lsn,
                    kind: SalMessageKind::classify(r.flags),
                    flags: r.flags,
                    target_id: r.target_id,
                    base: cursor,
                    slots: r.slots,
                    wire_data: r.wire_data(),
                },
                cursor + r.advance,
            ),
        }
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
    /// as `(base, slots, epoch)`. Group bases are 8-aligned — `payload_size` is
    /// always a multiple of 8 and bases start at 0 — so the stride cannot step
    /// over one. Testing the prefix word before the digest is what bounds the
    /// cost: the zero run filling a partly-used ring pays no hashing at all.
    pub fn valid_headers_from(&self, from: u64) -> impl Iterator<Item = (u64, u32, u32)> + '_ {
        (from..self.mmap_size).step_by(8).filter_map(move |base| {
            if base + 8 > self.mmap_size || unsafe { sal_prefix_word(self.ptr, base) } == 0 {
                return None;
            }
            let (slots, epoch) = unsafe { sal_probe_header(self.ptr, base, self.mmap_size) }?;
            Some((base, slots, epoch))
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
        self.valid_headers_from(0).map(|(_, _, e)| e).max().unwrap_or(0)
    }

    /// Slot `w` of the group published at `base`, independent of this reader's
    /// own worker id — group headers are slot-independent, so recovery can
    /// validate a zone across every slot its groups declare.
    pub fn slot_at(&self, base: u64, w: u32) -> Option<&'static [u8]> {
        match unsafe { sal_read_group_header(self.ptr, base, w, EpochGate::Any, self.mmap_size) } {
            SalRead::Group(r) => r.wire_data(),
            _ => None,
        }
    }

    pub fn wait(&self, timeout_ms: i32) -> i32 {
        posix_io::eventfd_wait(self.m2w_efd, timeout_ms)
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
