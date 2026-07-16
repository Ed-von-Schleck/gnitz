//! SAL (shared append-only log): master→worker broadcast channel.
//!
//! Owns the mmap layout, group-header write/read helpers, SalWriter,
//! SalMessage, SalReader, and the atomic primitives used by the SAL.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::foundation::codec::{align8, read_u32_raw, read_u64_raw, write_u32_raw, write_u64_raw};
use crate::foundation::posix_io;
use crate::runtime::wire::{
    build_schema_wire_block, encode_ctrl_block_direct, encode_wire_into, layout_to_wire_flags, wire_size,
    CTRL_BLOCK_SIZE_NO_BLOB, FLAG_HAS_DATA, FLAG_HAS_SCHEMA, STATUS_OK,
};
use crate::schema::SchemaDescriptor;
use crate::storage::{carve_writer_slices, scatter_copy, wire_header_dir_size, wire_region_sizes, Batch, DirectWriter};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const MAX_WORKERS: usize = 64;
/// Group header: 16 fixed (lsn u64, flags u32, target_id u32) +
/// MAX_WORKERS*4 offsets + MAX_WORKERS*4 sizes = 528, a multiple of 8.
/// The group's size and epoch live in the atomically-published u64 prefix
/// preceding the header (`(epoch << 32) | payload_size`), not in the header.
pub(crate) const GROUP_HEADER_SIZE: usize = 16 + 2 * MAX_WORKERS * 4;
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

/// The exact SAL footprint of a zone-closing `FLAG_TXN_COMMIT` sentinel — an
/// all-zero-worker group (`8 + GROUP_HEADER_SIZE` bytes, no per-worker slots).
/// `sal_begin_group` reserves this much headroom for **every non-sentinel
/// group**, so the sentinel — written last, after all family groups and any
/// zone-sharing single pushes — is guaranteed to fit. This makes `commit_zone`'s
/// space-abort unreachable: a data group at the boundary degrades to a graceful
/// `sal_begin_group` failure (`write_err` + skip) instead of aborting the node.
/// Negligible (~536 B) against the 1 GiB SAL.
pub(crate) const SENTINEL_SIZE: usize = 8 + GROUP_HEADER_SIZE;

/// Floor for a `GNITZ_SAL_BYTES` override — must comfortably exceed one DDL zone
/// plus the 2 MiB prefault-ahead window and the checkpoint headroom.
const MIN_SAL_BYTES: usize = 16 << 20;

/// The SAL mmap size in bytes. `SAL_MMAP_SIZE` (1 GiB) is the production default;
/// `GNITZ_SAL_BYTES` overrides it downward. This exists because each server
/// eagerly `fallocate`s + pre-faults the *whole* SAL at startup, so on a shared
/// data_dir filesystem (e.g. a tmpfs `/tmp`) many servers spawning in parallel
/// can exhaust it and take a `SIGBUS` on the forced page-fault — the integration
/// test harness spawns dozens at once, so it sets a small value. The size is read
/// once and cached: the master and its `fork()`ed workers inherit both the env
/// and this cache, so they can never disagree on the wrap arithmetic. The
/// override is clamped to `[MIN_SAL_BYTES, SAL_MMAP_SIZE]`.
///
/// Keep the value consistent across restarts on a given data_dir: recovery walks
/// only the first `sal_mmap_size()` bytes of the SAL file, so restarting with a
/// smaller size after a crash could skip committed-but-unflushed groups written
/// past the new bound (do a clean shutdown/checkpoint before shrinking).
pub fn sal_mmap_size() -> usize {
    use std::sync::OnceLock;
    static SIZE: OnceLock<usize> = OnceLock::new();
    *SIZE.get_or_init(|| {
        std::env::var("GNITZ_SAL_BYTES")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .map(|v| v.clamp(MIN_SAL_BYTES, SAL_MMAP_SIZE))
            .unwrap_or(SAL_MMAP_SIZE)
    })
}

const PAGE_SIZE: u64 = 4096;

// SAL group header flags (u32): the shared `gnitz_wire` flag bits re-declared
// as `u32` (the SAL group header's flag width).
pub const FLAG_SHUTDOWN: u32 = gnitz_wire::FLAG_SHUTDOWN as u32;
pub const FLAG_DDL_SYNC: u32 = gnitz_wire::FLAG_DDL_SYNC as u32;
pub const FLAG_EXCHANGE: u32 = gnitz_wire::FLAG_EXCHANGE as u32;
pub const FLAG_PUSH: u32 = gnitz_wire::FLAG_PUSH as u32;
pub const FLAG_HAS_PK: u32 = gnitz_wire::FLAG_HAS_PK as u32;
pub const FLAG_SEEK: u32 = gnitz_wire::FLAG_SEEK as u32;
pub const FLAG_SEEK_BY_INDEX: u32 = gnitz_wire::FLAG_SEEK_BY_INDEX as u32;
pub const FLAG_EXCHANGE_RELAY: u32 = 512;
pub const FLAG_BACKFILL: u32 = 2048;
pub const FLAG_TICK: u32 = 4096;
pub const FLAG_FLUSH: u32 = 16384;
/// Ephemeral-state flush round of the checkpoint sequence: flush every view's
/// operator-trace tables and output stores (traces before outputs), stamping
/// their manifests with the checkpoint generation carried in the group header's
/// `lsn` field. Dispatched inline in both worker contexts like `FLAG_FLUSH`, but
/// distinct so the base round (`FLAG_FLUSH`, `SalReplay` user tables) and the
/// ephemeral round (`Rederive` view state) stay separate handlers. Bit 19 — the
/// next free bit above `FLAG_SEEK_BY_INDEX_RANGE_SAL` (1<<18).
pub const FLAG_FLUSH_EPH: u32 = 1 << 19;
/// Marks an empty broadcast group as the closing "commit sentinel" of an
/// atomic zone. All preceding groups at the same LSN belong to the zone;
/// recovery applies them only when this sentinel is on disk. The flag
/// rides on top of FLAG_DDL_SYNC for the worker's dispatch loop, which
/// already no-ops on a DDL_SYNC group with `count == 0`.
pub const FLAG_TXN_COMMIT: u32 = 32768;
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
/// `validate_unique_index_create_async`). Unicast-shaped like a Scan: every
/// worker gets its own req_id slot and answers with a frame train.
pub const FLAG_UNIQUE_PREFLIGHT: u32 = 131072;
/// Ordered range scan over a secondary index (master→worker leg). The
/// `u32`-space dispatch flag for `SalMessageKind::SeekByIndexRange`; bit 18,
/// the next free SAL group-header bit above `FLAG_UNIQUE_PREFLIGHT` (1<<17).
/// Distinct from the client→master `u64` wire flag `FLAG_SEEK_BY_INDEX_RANGE`
/// (bit 55), which lives among the wire packed fields and is never classified
/// here. A range seek must fan out (its matches scatter by source PK), so the
/// worker classifies it from this `u32` flag.
pub const FLAG_SEEK_BY_INDEX_RANGE_SAL: u32 = 1 << 18;

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
// Master-side encoding still uses raw FLAG_* writes (master.rs) and the
// reactor only reads one bit (reactor/mod.rs). This enum is consumed by
// the worker dispatch loop to give the compiler an exhaustiveness check
// over every kind of message the worker can observe. Add a new variant
// here whenever a new dispatch arm is added on the worker side.
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
    SeekByIndexRange,
    Seek,
    Scan,
}

impl SalMessageKind {
    /// Classify a SAL group by its flag word.
    ///
    /// Priority order matches the worker's existing if-chain (see
    /// `worker::dispatch_inner`): SHUTDOWN > FLUSH > DDL_SYNC >
    /// EXCHANGE_RELAY > BACKFILL > HAS_PK > GATHER > UNIQUE_PREFLIGHT >
    /// PUSH > TICK > SEEK_BY_INDEX_RANGE > SEEK_BY_INDEX > SEEK > Scan.
    /// The first match wins. (Each kind owns a distinct bit, so the
    /// relative order of the disjoint range/point/seek arms is
    /// immaterial; it tracks the worker's if-chain for readability.)
    pub fn classify(flags: u32) -> SalMessageKind {
        if flags & FLAG_SHUTDOWN != 0 {
            return SalMessageKind::Shutdown;
        }
        if flags & FLAG_FLUSH != 0 {
            return SalMessageKind::Flush;
        }
        if flags & FLAG_FLUSH_EPH != 0 {
            return SalMessageKind::FlushEph;
        }
        if flags & FLAG_DDL_SYNC != 0 {
            return SalMessageKind::DdlSync;
        }
        if flags & FLAG_EXCHANGE_RELAY != 0 {
            return SalMessageKind::ExchangeRelay;
        }
        if flags & FLAG_BACKFILL != 0 {
            return SalMessageKind::Backfill;
        }
        if flags & FLAG_HAS_PK != 0 {
            return SalMessageKind::HasPk;
        }
        if flags & FLAG_GATHER != 0 {
            return SalMessageKind::Gather;
        }
        if flags & FLAG_UNIQUE_PREFLIGHT != 0 {
            return SalMessageKind::UniquePreflight;
        }
        if flags & FLAG_PUSH != 0 {
            return SalMessageKind::Push;
        }
        if flags & FLAG_TICK != 0 {
            return SalMessageKind::Tick;
        }
        if flags & FLAG_SEEK_BY_INDEX_RANGE_SAL != 0 {
            return SalMessageKind::SeekByIndexRange;
        }
        if flags & FLAG_SEEK_BY_INDEX != 0 {
            return SalMessageKind::SeekByIndex;
        }
        if flags & FLAG_SEEK != 0 {
            return SalMessageKind::Seek;
        }
        SalMessageKind::Scan
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
    hdr_off: usize,
    base: usize,
    total: usize,
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
        self.sal_ptr.add(self.hdr_off + offset)
    }

    pub(crate) unsafe fn commit(mut self) -> u64 {
        sal_write_sentinel(self.sal_ptr, self.base + self.total, self.mmap_size);
        atomic_store_u64(
            self.sal_ptr.add(self.base),
            (self.epoch as u64) << 32 | self.payload_size as u64,
        );
        let cursor = (self.base + self.total) as u64;
        self.committed = true;
        cursor
    }
}

/// Reserve SAL space, write group header + per-worker directory.
/// Returns `None` if the group doesn't fit or `num_workers > MAX_WORKERS`.
///
/// # Safety
/// `sal_ptr` must be a valid mmap pointer of at least `mmap_size` bytes.
#[allow(clippy::too_many_arguments, clippy::needless_range_loop)]
pub(crate) unsafe fn sal_begin_group(
    sal_ptr: *mut u8,
    write_cursor: usize,
    mmap_size: usize,
    num_workers: usize,
    target_id: u32,
    lsn: u64,
    flags: u32,
    epoch: u32,
    worker_sizes: &[u32],
) -> Option<SalGroup> {
    if num_workers > MAX_WORKERS {
        return None;
    }
    debug_assert!(
        epoch >= 1,
        "SAL group epoch must be >= 1 — epoch 0 is indistinguishable from the empty-slot sentinel prefix"
    );

    // payload_size starts as GROUP_HEADER_SIZE (528, a multiple of 8) and grows
    // only by align8(sz) increments, so it is always a multiple of 8.
    let mut payload_size = GROUP_HEADER_SIZE;
    for &sz in &worker_sizes[..num_workers] {
        if sz > 0 {
            payload_size += align8(sz as usize);
        }
    }
    let total = 8 + payload_size;
    // Global sentinel reservation: every non-sentinel group must leave
    // SENTINEL_SIZE headroom so the zone-closing FLAG_TXN_COMMIT sentinel always
    // fits at the end of the zone. Only the sentinel group itself may consume
    // that reserve (it fits in exactly SENTINEL_SIZE bytes).
    let effective_max = if flags & FLAG_TXN_COMMIT != 0 {
        mmap_size
    } else {
        mmap_size.saturating_sub(SENTINEL_SIZE)
    };
    if write_cursor + total > effective_max {
        return None;
    }

    let base = write_cursor;
    let hdr_off = base + 8;

    write_u64_raw(sal_ptr, hdr_off, lsn);
    write_u32_raw(sal_ptr, hdr_off + 8, flags);
    write_u32_raw(sal_ptr, hdr_off + 12, target_id);

    let mut data_offset = GROUP_HEADER_SIZE;
    for w in 0..num_workers {
        let sz = worker_sizes[w] as usize;
        if sz > 0 {
            write_u32_raw(sal_ptr, hdr_off + 16 + w * 4, data_offset as u32);
            write_u32_raw(sal_ptr, hdr_off + 16 + MAX_WORKERS * 4 + w * 4, worker_sizes[w]);
            data_offset += align8(sz);
        } else {
            write_u32_raw(sal_ptr, hdr_off + 16 + w * 4, 0);
            write_u32_raw(sal_ptr, hdr_off + 16 + MAX_WORKERS * 4 + w * 4, 0);
        }
    }

    Some(SalGroup {
        sal_ptr,
        hdr_off,
        base,
        total,
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
        nw,
        target_id,
        lsn,
        flags,
        epoch,
        &sizes[..nw],
    )?;

    let mut off = GROUP_HEADER_SIZE;
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
    pub epoch: u32,
    /// This worker's payload slot; null/0 when the group carries no data
    /// for this worker (control broadcast, other-worker unicast).
    pub data_ptr: *const u8,
    pub data_size: u32,
}

/// Read a SAL group header and extract this worker's data pointer/size.
///
/// The group's `(epoch << 32) | payload_size` prefix is Acquire-loaded first;
/// `expected_epoch: Some(e)` returns `None` on any mismatch **before a
/// single header byte is read** (live worker path). `None` skips the gate —
/// only valid against a quiescent SAL (recovery walkers, no concurrent writer).
/// Returns `None` when no message is present at the cursor.
///
/// # Safety
/// `sal_ptr` must be a valid mmap pointer. `read_cursor` must be within bounds.
pub(crate) unsafe fn sal_read_group_header(
    sal_ptr: *const u8,
    read_cursor: u64,
    worker_id: u32,
    expected_epoch: Option<u32>,
) -> Option<SalReadResult> {
    let rc = read_cursor as usize;
    let wid = worker_id as usize;

    let word = atomic_load_u64(sal_ptr.add(rc));
    let payload_size = (word & 0xFFFF_FFFF) as usize;
    let epoch = (word >> 32) as u32;
    if payload_size == 0 {
        return None;
    }
    // The load-bearing gate: on an epoch mismatch, return before ANY plain
    // read of the header region. A stale slot at offset 0 after an epoch
    // transition may be concurrently overwritten by the master; its bytes
    // are unreadable until the prefix proves the slot belongs to our epoch.
    if let Some(exp) = expected_epoch {
        if epoch != exp {
            return None;
        }
    }

    let hdr_off = rc + 8;
    let lsn = read_u64_raw(sal_ptr, hdr_off);
    let flags = read_u32_raw(sal_ptr, hdr_off + 8);
    let target_id = read_u32_raw(sal_ptr, hdr_off + 12);
    let advance = (8 + align8(payload_size)) as u64;

    let my_offset = read_u32_raw(sal_ptr, hdr_off + 16 + wid * 4) as usize;
    let my_size = read_u32_raw(sal_ptr, hdr_off + 16 + MAX_WORKERS * 4 + wid * 4) as usize;

    let (data_ptr, data_size) = if my_size > 0 && my_offset > 0 {
        debug_assert!(
            my_offset + my_size <= payload_size,
            "SAL group header corrupt: my_offset={my_offset} my_size={my_size} payload_size={payload_size}"
        );
        if my_offset + my_size <= payload_size {
            (sal_ptr.add(hdr_off + my_offset), my_size as u32)
        } else {
            (std::ptr::null(), 0)
        }
    } else {
        (std::ptr::null(), 0)
    };
    Some(SalReadResult {
        advance,
        lsn,
        flags,
        target_id,
        epoch,
        data_ptr,
        data_size,
    })
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
    let mut writer = DirectWriter::new(pk, weight, null_bmp, col_slices, &mut empty_blob, *schema, 0);
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
    write_cursor: u64,
    epoch: u32,
    checkpoint_threshold: u64,
    m2w_efds: Vec<i32>,
    last_prefaulted: u64,
}

unsafe impl Send for SalWriter {}

impl SalWriter {
    pub fn new(ptr: *mut u8, fd: i32, mmap_size: u64, m2w_efds: Vec<i32>) -> Self {
        let checkpoint_threshold = std::env::var("GNITZ_CHECKPOINT_BYTES")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or((mmap_size * 3) >> 2);
        SalWriter {
            ptr,
            fd,
            mmap_size,
            write_cursor: 0,
            epoch: 0,
            checkpoint_threshold,
            m2w_efds,
            last_prefaulted: 0,
        }
    }

    fn prefault_ahead(&mut self) {
        const PREFAULT_AHEAD: u64 = 2 * 1024 * 1024;
        let target = (self.write_cursor + PREFAULT_AHEAD).min(self.mmap_size);
        if target <= self.last_prefaulted {
            return;
        }
        let raw_start = self.last_prefaulted.max(self.write_cursor);
        let start = raw_start & !(PAGE_SIZE - 1);
        let end_raw = (target + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);
        let end = end_raw.min(self.mmap_size);
        if end > start {
            posix_io::madvise_willneed(unsafe { self.ptr.add(start as usize) }, (end - start) as usize);
            self.last_prefaulted = target;
        }
    }

    /// Encode per-worker wire data directly into SAL mmap. Does NOT sync/signal.
    /// `lsn` is supplied by the caller; the SAL writer no longer owns the
    /// counter (Design 2: caller controls zone-LSN allocation).
    ///
    /// `prebuilt_schema_block`: when `Some`, the bytes are copied into each
    /// slot's schema region instead of building one from `schema` + names.
    /// Mutually exclusive with `col_names_opt`; passing both is a bug.
    #[allow(clippy::too_many_arguments, clippy::needless_range_loop)]
    pub fn write_group_direct(
        &mut self,
        target_id: u32,
        lsn: u64,
        sal_flags: u32,
        wire_flags: u64,
        worker_batches: &[Option<&Batch>],
        schema: &SchemaDescriptor,
        col_names_opt: Option<&[&[u8]]>,
        seek_pk: u128,
        seek_col_idx: u64,
        req_ids: &[u64],
        unicast_worker: i32,
        client_id: u64,
        prebuilt_schema_block: Option<&[u8]>,
        seek_pk_extra: &[u8],
    ) -> Result<(), String> {
        self.prefault_ahead();
        let nw = self.m2w_efds.len();
        assert_eq!(
            req_ids.len(),
            nw,
            "write_group_direct: req_ids.len()={} != num_workers={}",
            req_ids.len(),
            nw
        );
        debug_assert!(
            prebuilt_schema_block.is_none() || col_names_opt.is_none(),
            "write_group_direct: prebuilt_schema_block and col_names_opt are mutually exclusive",
        );

        let mut worker_sizes = [0u32; MAX_WORKERS];
        for w in 0..nw {
            if unicast_worker >= 0 && w != unicast_worker as usize {
                continue;
            }
            let data_batch = worker_batches.get(w).and_then(|opt| opt.as_ref());
            worker_sizes[w] = wire_size(
                STATUS_OK,
                b"",
                Some(schema),
                col_names_opt,
                data_batch.copied(),
                prebuilt_schema_block,
                seek_pk_extra,
            ) as u32;
        }

        let group = unsafe {
            sal_begin_group(
                self.ptr,
                self.write_cursor as usize,
                self.mmap_size as usize,
                nw,
                target_id,
                lsn,
                sal_flags,
                self.epoch,
                &worker_sizes[..nw],
            )
        }
        .ok_or_else(|| format!("SAL write_group_direct failed (cursor={})", self.write_cursor))?;

        let mut off = GROUP_HEADER_SIZE;
        for w in 0..nw {
            let wsz = worker_sizes[w] as usize;
            if wsz > 0 {
                let data_batch = worker_batches.get(w).and_then(|opt| opt.as_ref());
                let slot = unsafe { std::slice::from_raw_parts_mut(group.data_ptr(off), wsz) };
                let written = encode_wire_into(
                    slot,
                    0,
                    target_id as u64,
                    client_id,
                    wire_flags,
                    seek_pk,
                    seek_col_idx,
                    req_ids[w],
                    STATUS_OK,
                    b"",
                    Some(schema),
                    col_names_opt,
                    data_batch.copied(),
                    prebuilt_schema_block,
                    seek_pk_extra,
                );
                debug_assert_eq!(written, wsz);
                off += align8(wsz);
            }
        }

        self.write_cursor = unsafe { group.commit() };
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
    /// The sentinel is **not** included — it is globally reserved by
    /// `sal_begin_group` (`SENTINEL_SIZE`), not counted per group.
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
        let mut total = 8 + GROUP_HEADER_SIZE;
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
                    let sub = Batch::from_indexed_rows(&mb, wi, schema);
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
        &mut self,
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
        self.prefault_ahead();
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
                        Batch::from_indexed_rows(&mb, indices, schema)
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
                schema,
                None,
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
                owned_block = build_schema_wire_block(schema, &[], 0, target_id);
                &owned_block
            }
        };
        // ctrl block size for the no-error fast path is a compile-time constant.
        let ctrl_size = CTRL_BLOCK_SIZE_NO_BLOB;
        let mut worker_sizes = [0u32; MAX_WORKERS];
        for w in 0..nw {
            worker_sizes[w] =
                wire_safe_slot_size(schema, worker_indices[w].len(), wire_row_stride, schema_block.len()) as u32;
        }

        let group = unsafe {
            sal_begin_group(
                self.ptr,
                self.write_cursor as usize,
                self.mmap_size as usize,
                nw,
                target_id,
                lsn,
                sal_flags,
                self.epoch,
                &worker_sizes[..nw],
            )
        }
        .ok_or_else(|| format!("SAL scatter_wire_group failed (cursor={})", self.write_cursor))?;

        let mb = input_batch.as_mem_batch();
        let mut off = GROUP_HEADER_SIZE;
        for w in 0..nw {
            let wsz = worker_sizes[w] as usize;
            if wsz == 0 {
                continue;
            }

            let count_w = worker_indices[w].len();
            let slot = unsafe { std::slice::from_raw_parts_mut(group.data_ptr(off), wsz) };

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
                target_id as u64,
                0,
                full_wire_flags,
                0,
                seek_col_idx,
                req_ids[w],
                STATUS_OK,
                b"",
                &[],
                false,
            );

            off += align8(wsz);
        }

        self.write_cursor = unsafe { group.commit() };
        Ok(())
    }

    /// Encode once into worker 0's slot, memcpy to workers 1..N-1.
    /// Does NOT sync/signal. `lsn` is supplied by the caller.
    ///
    /// `prebuilt_schema_block`: when `Some`, the bytes are copied into the
    /// schema region instead of being built from `schema` + names. Mutually
    /// exclusive with `col_names_opt`; passing both is a bug.
    #[allow(clippy::too_many_arguments)]
    pub fn write_broadcast_direct(
        &mut self,
        target_id: u32,
        lsn: u64,
        sal_flags: u32,
        batch: Option<&Batch>,
        schema: &SchemaDescriptor,
        col_names_opt: Option<&[&[u8]]>,
        seek_pk: u128,
        prebuilt_schema_block: Option<&[u8]>,
    ) -> Result<(), String> {
        self.prefault_ahead();
        let nw = self.m2w_efds.len();
        debug_assert!(
            prebuilt_schema_block.is_none() || col_names_opt.is_none(),
            "write_broadcast_direct: prebuilt_schema_block and col_names_opt are mutually exclusive",
        );

        let wsz = wire_size(
            STATUS_OK,
            b"",
            Some(schema),
            col_names_opt,
            batch,
            prebuilt_schema_block,
            &[],
        ) as u32;
        let mut worker_sizes = [0u32; MAX_WORKERS];
        for item in worker_sizes.iter_mut().take(nw) {
            *item = wsz;
        }

        let group = unsafe {
            sal_begin_group(
                self.ptr,
                self.write_cursor as usize,
                self.mmap_size as usize,
                nw,
                target_id,
                lsn,
                sal_flags,
                self.epoch,
                &worker_sizes[..nw],
            )
        }
        .ok_or_else(|| format!("SAL write_broadcast_direct failed (cursor={})", self.write_cursor))?;

        if wsz > 0 {
            let wsz = wsz as usize;
            let slot0 = unsafe { std::slice::from_raw_parts_mut(group.data_ptr(GROUP_HEADER_SIZE), wsz) };
            let written = encode_wire_into(
                slot0,
                0,
                target_id as u64,
                0,
                0,
                seek_pk,
                0,
                0,
                STATUS_OK,
                b"",
                Some(schema),
                col_names_opt,
                batch,
                prebuilt_schema_block,
                &[],
            );
            debug_assert_eq!(written, wsz);
            let mut off = GROUP_HEADER_SIZE + align8(wsz);
            for _ in 1..nw {
                unsafe {
                    std::ptr::copy_nonoverlapping(group.data_ptr(GROUP_HEADER_SIZE), group.data_ptr(off), wsz);
                }
                off += align8(wsz);
            }
        }

        self.write_cursor = unsafe { group.commit() };
        Ok(())
    }

    /// Write an empty commit sentinel for an atomic zone.
    ///
    /// Header-only group (zero-byte payload for every worker) carrying
    /// `FLAG_DDL_SYNC | FLAG_TXN_COMMIT`. Recovery uses the sentinel as
    /// the "this LSN is closed" mark — without it, all groups at this
    /// LSN are skipped. The sentinel is inert under the worker's hot
    /// path: the FLAG_DDL_SYNC branch no-ops on a group with no batch.
    pub fn write_commit_sentinel(&mut self, lsn: u64) -> Result<(), String> {
        self.prefault_ahead();
        let nw = self.m2w_efds.len();
        let worker_sizes = [0u32; MAX_WORKERS];
        let group = unsafe {
            sal_begin_group(
                self.ptr,
                self.write_cursor as usize,
                self.mmap_size as usize,
                nw,
                0,
                lsn,
                FLAG_DDL_SYNC | FLAG_TXN_COMMIT,
                self.epoch,
                &worker_sizes[..nw],
            )
        }
        .ok_or_else(|| format!("SAL write_commit_sentinel failed (cursor={})", self.write_cursor))?;
        self.write_cursor = unsafe { group.commit() };
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
        self.write_cursor >= self.checkpoint_threshold
    }

    pub fn checkpoint_reset(&mut self) {
        self.epoch += 1;
        self.write_cursor = 0;
        self.last_prefaulted = 0;
        unsafe {
            atomic_store_u64(self.ptr, 0);
        }
    }

    /// Boot-time reset after all workers finish recovery: clear the slot-0
    /// sentinel prefix (readers at cursor 0 then see "no message") and rewind
    /// to cursor 0, epoch 1 — the first live epoch; epoch 0 is the empty-slot
    /// sentinel prefix. The mmap-prefix store lives here, next to
    /// `checkpoint_reset`, so the SAL slot layout never leaks to callers.
    pub fn boot_reset(&mut self) {
        self.write_cursor = 0;
        self.epoch = 1;
        self.last_prefaulted = 0;
        unsafe {
            atomic_store_u64(self.ptr, 0);
        }
    }

    #[cfg(test)]
    pub fn reset(&mut self, cursor: u64, epoch: u32) {
        self.write_cursor = cursor;
        self.epoch = epoch;
    }

    pub fn cursor(&self) -> u64 {
        self.write_cursor
    }
    pub fn epoch(&self) -> u32 {
        self.epoch
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
    pub epoch: u32,
    /// None = no data for this worker in this group.
    pub wire_data: Option<&'a [u8]>,
}

pub struct SalReader {
    ptr: *const u8,
    worker_id: u32,
    mmap_size: u64,
    m2w_efd: i32,
}

unsafe impl Send for SalReader {}

impl SalReader {
    pub fn new(ptr: *const u8, worker_id: u32, mmap_size: usize, m2w_efd: i32) -> Self {
        SalReader {
            ptr,
            worker_id,
            mmap_size: mmap_size as u64,
            m2w_efd,
        }
    }

    /// The SAL mmap size this reader was opened with. Stored rather than
    /// re-fetched from the `sal_mmap_size()` global so the hot per-group read
    /// loops do a field read, not an atomic `OnceLock` load. Mirrors
    /// `SalWriter::mmap_size`.
    #[inline]
    pub fn mmap_size(&self) -> u64 {
        self.mmap_size
    }

    /// Read next group at `cursor`. Returns None if no message, or — with
    /// `expected_epoch: Some(e)` — if the group's prefix epoch differs from
    /// `e` (the group stays parked at the cursor; its header bytes are never
    /// dereferenced). Pass `None` only for quiescent-SAL recovery walks.
    /// On success returns (message, new_cursor).
    pub fn try_read(&self, cursor: u64, expected_epoch: Option<u32>) -> Option<(SalMessage<'static>, u64)> {
        let result = unsafe { sal_read_group_header(self.ptr, cursor, self.worker_id, expected_epoch) }?;
        let new_cursor = cursor + result.advance;
        let wire_data = if result.data_size > 0 && !result.data_ptr.is_null() {
            Some(unsafe { std::slice::from_raw_parts(result.data_ptr, result.data_size as usize) })
        } else {
            None
        };
        Some((
            SalMessage {
                lsn: result.lsn,
                kind: SalMessageKind::classify(result.flags),
                flags: result.flags,
                target_id: result.target_id,
                epoch: result.epoch,
                wire_data,
            },
            new_cursor,
        ))
    }

    pub fn wait(&self, timeout_ms: i32) -> i32 {
        posix_io::eventfd_wait(self.m2w_efd, timeout_ms)
    }
}
