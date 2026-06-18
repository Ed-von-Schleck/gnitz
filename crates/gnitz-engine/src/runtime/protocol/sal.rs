//! SAL (shared append-only log): master→worker broadcast channel.
//!
//! Owns the mmap layout, group-header write/read helpers, SalWriter,
//! SalMessage, SalReader, and the atomic primitives used by the SAL.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::foundation::codec::{align8, read_u32_raw, read_u64_raw, write_u32_le, write_u32_raw, write_u64_raw};
use crate::foundation::posix_io;
use crate::foundation::syscall;
use crate::foundation::xxh;
use crate::runtime::wire::{
    build_schema_wire_block, encode_ctrl_block_direct, encode_wire_into, wire_size, CTRL_BLOCK_SIZE_NO_BLOB,
    FLAG_BATCH_CONSOLIDATED, FLAG_BATCH_SORTED, FLAG_HAS_DATA, FLAG_HAS_SCHEMA, STATUS_OK,
};
use crate::schema::SchemaDescriptor;
use crate::storage::{carve_writer_slices, scatter_copy, Batch, DirectWriter};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

pub const MAX_WORKERS: usize = 64;
/// Group header: 32 fixed + MAX_WORKERS*4 offsets + MAX_WORKERS*4 sizes + 32 pad
pub(crate) const GROUP_HEADER_SIZE: usize = 576;
pub const SAL_MMAP_SIZE: usize = 1 << 30;

const PAGE_SIZE: u64 = 4096;

// SAL group header flags (u32)
gnitz_wire::cast_consts! { pub u32;
    FLAG_SHUTDOWN, FLAG_DDL_SYNC, FLAG_EXCHANGE, FLAG_PUSH,
    FLAG_HAS_PK, FLAG_SEEK, FLAG_SEEK_BY_INDEX,
}
pub const FLAG_EXCHANGE_RELAY: u32 = 512;
pub const FLAG_PRELOADED_EXCHANGE: u32 = 1024;
pub const FLAG_BACKFILL: u32 = 2048;
pub const FLAG_TICK: u32 = 4096;
pub const FLAG_CHECKPOINT: u32 = 8192;
pub const FLAG_FLUSH: u32 = 16384;
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
/// answers with a full table scan. `FLAG_CHECKPOINT` is intentionally
/// absent — it is an ACK-only echo flag, never appears on a SAL group
/// inbound to the worker. `FLAG_TXN_COMMIT` rides on top of
/// `FLAG_DDL_SYNC` (zero-count sentinel); classification stops at
/// `DdlSync` and the worker handles it as a no-op DDL_SYNC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SalMessageKind {
    Shutdown,
    Flush,
    DdlSync,
    ExchangeRelay,
    PreloadedExchange,
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
    /// EXCHANGE_RELAY > PRELOADED_EXCHANGE > BACKFILL > HAS_PK >
    /// GATHER > UNIQUE_PREFLIGHT > PUSH > TICK > SEEK_BY_INDEX_RANGE >
    /// SEEK_BY_INDEX > SEEK > Scan. The first match wins. (Each kind owns a
    /// distinct bit, so the relative order of the disjoint range/point/seek
    /// arms is immaterial; it tracks the worker's if-chain for readability.)
    pub fn classify(flags: u32) -> SalMessageKind {
        if flags & FLAG_SHUTDOWN != 0 {
            return SalMessageKind::Shutdown;
        }
        if flags & FLAG_FLUSH != 0 {
            return SalMessageKind::Flush;
        }
        if flags & FLAG_DDL_SYNC != 0 {
            return SalMessageKind::DdlSync;
        }
        if flags & FLAG_EXCHANGE_RELAY != 0 {
            return SalMessageKind::ExchangeRelay;
        }
        if flags & FLAG_PRELOADED_EXCHANGE != 0 {
            return SalMessageKind::PreloadedExchange;
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
                | SalMessageKind::DdlSync
                | SalMessageKind::PreloadedExchange
                | SalMessageKind::Backfill
                | SalMessageKind::HasPk
                | SalMessageKind::Gather
                | SalMessageKind::Push
                | SalMessageKind::Tick
        )
    }

    /// All variants in classification priority order. Used by tests that
    /// walk every kind to exercise the dispatcher's full match.
    #[allow(dead_code)]
    pub const ALL: [SalMessageKind; 15] = [
        SalMessageKind::Shutdown,
        SalMessageKind::Flush,
        SalMessageKind::DdlSync,
        SalMessageKind::ExchangeRelay,
        SalMessageKind::PreloadedExchange,
        SalMessageKind::Backfill,
        SalMessageKind::HasPk,
        SalMessageKind::Gather,
        SalMessageKind::UniquePreflight,
        SalMessageKind::Push,
        SalMessageKind::Tick,
        SalMessageKind::SeekByIndex,
        SalMessageKind::SeekByIndexRange,
        SalMessageKind::Seek,
        SalMessageKind::Scan,
    ];
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

/// True when every column has a fixed-width 8-aligned stride and no German-string
/// (STRING or BLOB) columns. Batches satisfying this can be scatter-encoded
/// directly into SAL slots without intermediate per-worker Batch allocations.
/// BLOB shares STRING's 16-byte struct with out-of-line heap bytes, so it must
/// be excluded too — the fast path hands `scatter_copy` a 0-cap blob arena and
/// asserts no blob bytes are written.
pub(crate) fn schema_wire_safe(schema: &SchemaDescriptor) -> bool {
    (0..schema.num_columns()).all(|ci| {
        let c = &schema.columns[ci];
        !gnitz_wire::is_german_string(c.type_code) && c.size().is_multiple_of(8)
    })
}

/// Compute `(wire_safe, wire_row_fixed_stride)` for `schema`. The stride is
/// only meaningful when `wire_safe` is true: it's the byte cost of one row
/// in the SAL data block (`pk_stride + weight + null_bmp + payload strides`,
/// all already 8-aligned because `wire_safe` rejects non-8-aligned columns).
/// Caching the result lets `scatter_wire_group` skip the per-call iteration.
pub fn compute_wire_props(schema: &SchemaDescriptor) -> (bool, u32) {
    let safe = schema_wire_safe(schema);
    if !safe {
        return (false, 0);
    }
    let pk_stride = schema.pk_stride() as u32;
    let mut stride = pk_stride + 8 + 8;
    for (_pi, _ci, col) in schema.payload_columns() {
        stride += col.size() as u32;
    }
    (true, stride)
}

/// Header + directory size for a wire-safe data block of `npc` payload columns.
#[inline]
fn data_wire_header_dir_size(npc: usize) -> usize {
    let num_regions = 3 + npc + 1;
    gnitz_wire::WAL_HEADER_SIZE + num_regions * 8
}

/// Compute the byte size of the data WAL block for `count` rows on a schema
/// with `npc` payload columns and a precomputed `wire_row_fixed_stride`.
/// Only correct when the schema is `wire_safe` (caller's responsibility).
#[inline]
fn data_wire_block_size_cached(count: usize, npc: usize, stride: u32) -> usize {
    data_wire_header_dir_size(npc) + count * stride as usize
}

// ---------------------------------------------------------------------------
// SAL write (master→workers)
// ---------------------------------------------------------------------------

/// Result from SAL write (used by tests via `sal_write_group`).
#[cfg(test)]
pub(crate) struct SalWriteResult {
    pub status: i32,
    pub new_cursor: u64,
}

/// Handle returned by `sal_begin_group`. Header and per-worker directory
/// are already written; caller fills per-worker data, then calls `commit()`.
#[must_use = "SalGroup must be passed to commit(); dropping it leaves the SAL sentinel unwritten"]
pub(crate) struct SalGroup {
    sal_ptr: *mut u8,
    hdr_off: usize,
    base: usize,
    total: usize,
    payload_size: usize,
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
        atomic_store_u64(self.sal_ptr.add(self.base), self.payload_size as u64);
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

    // payload_size starts as GROUP_HEADER_SIZE (576, a multiple of 8) and grows
    // only by align8(sz) increments, so it is always a multiple of 8.
    let mut payload_size = GROUP_HEADER_SIZE;
    for &sz in &worker_sizes[..num_workers] {
        if sz > 0 {
            payload_size += align8(sz as usize);
        }
    }
    let total = 8 + payload_size;
    if write_cursor + total > mmap_size {
        return None;
    }

    let base = write_cursor;
    let hdr_off = base + 8;

    write_u64_raw(sal_ptr, hdr_off, payload_size as u64);
    write_u64_raw(sal_ptr, hdr_off + 8, lsn);
    write_u32_raw(sal_ptr, hdr_off + 16, num_workers as u32);
    write_u32_raw(sal_ptr, hdr_off + 20, flags);
    write_u32_raw(sal_ptr, hdr_off + 24, target_id);
    write_u32_raw(sal_ptr, hdr_off + 28, epoch);

    let mut data_offset = GROUP_HEADER_SIZE;
    for w in 0..num_workers {
        let sz = worker_sizes[w] as usize;
        if sz > 0 {
            write_u32_raw(sal_ptr, hdr_off + 32 + w * 4, data_offset as u32);
            write_u32_raw(sal_ptr, hdr_off + 32 + MAX_WORKERS * 4 + w * 4, worker_sizes[w]);
            data_offset += align8(sz);
        } else {
            write_u32_raw(sal_ptr, hdr_off + 32 + w * 4, 0);
            write_u32_raw(sal_ptr, hdr_off + 32 + MAX_WORKERS * 4 + w * 4, 0);
        }
    }

    Some(SalGroup {
        sal_ptr,
        hdr_off,
        base,
        total,
        payload_size,
        mmap_size,
        committed: false,
    })
}

/// Write a message group into the SAL for N workers (test helper).
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) unsafe fn sal_write_group(
    sal_ptr: *mut u8,
    write_cursor: u64,
    num_workers: u32,
    target_id: u32,
    lsn: u64,
    flags: u32,
    epoch: u32,
    mmap_size: u64,
    worker_ptrs: *const *const u8,
    worker_sizes: *const u32,
) -> SalWriteResult {
    let nw = num_workers as usize;
    let mut sizes = [0u32; MAX_WORKERS];
    for (w, slot) in sizes.iter_mut().enumerate().take(nw) {
        *slot = *worker_sizes.add(w);
    }

    let group = match sal_begin_group(
        sal_ptr,
        write_cursor as usize,
        mmap_size as usize,
        nw,
        target_id,
        lsn,
        flags,
        epoch,
        &sizes[..nw],
    ) {
        Some(g) => g,
        None => {
            return SalWriteResult {
                status: -1,
                new_cursor: write_cursor,
            }
        }
    };

    let mut off = GROUP_HEADER_SIZE;
    for (w, &sz_u32) in sizes.iter().enumerate().take(nw) {
        let sz = sz_u32 as usize;
        if sz > 0 {
            let p = *worker_ptrs.add(w);
            if !p.is_null() {
                std::ptr::copy_nonoverlapping(p, group.data_ptr(off), sz);
            }
            off += align8(sz);
        }
    }

    SalWriteResult {
        status: 0,
        new_cursor: group.commit(),
    }
}

// ---------------------------------------------------------------------------
// SAL read (worker reads its data from a group)
// ---------------------------------------------------------------------------

pub(crate) const SAL_STATUS_HAS_DATA: i32 = 0;
pub(crate) const SAL_STATUS_NO_DATA_FOR_WORKER: i32 = 1;
pub(crate) const SAL_STATUS_NO_MESSAGE: i32 = -1;

pub(crate) struct SalReadResult {
    pub status: i32,
    pub advance: u64,
    pub lsn: u64,
    pub flags: u32,
    pub target_id: u32,
    pub epoch: u32,
    pub _pad: u32,
    pub data_ptr: *const u8,
    pub data_size: u32,
    pub _pad2: u32,
}

/// Read a SAL group header and extract this worker's data pointer/size.
///
/// # Safety
/// `sal_ptr` must be a valid mmap pointer. `read_cursor` must be within bounds.
pub(crate) unsafe fn sal_read_group_header(sal_ptr: *const u8, read_cursor: u64, worker_id: u32) -> SalReadResult {
    let rc = read_cursor as usize;
    let wid = worker_id as usize;

    let payload_size = atomic_load_u64(sal_ptr.add(rc)) as usize;
    if payload_size == 0 {
        return SalReadResult {
            status: SAL_STATUS_NO_MESSAGE,
            advance: 0,
            lsn: 0,
            flags: 0,
            target_id: 0,
            epoch: 0,
            _pad: 0,
            data_ptr: std::ptr::null(),
            data_size: 0,
            _pad2: 0,
        };
    }

    let hdr_off = rc + 8;
    let lsn = read_u64_raw(sal_ptr, hdr_off + 8);
    let flags = read_u32_raw(sal_ptr, hdr_off + 20);
    let target_id = read_u32_raw(sal_ptr, hdr_off + 24);
    let epoch = read_u32_raw(sal_ptr, hdr_off + 28);
    let advance = (8 + align8(payload_size)) as u64;

    let my_offset = read_u32_raw(sal_ptr, hdr_off + 32 + wid * 4) as usize;
    let my_size = read_u32_raw(sal_ptr, hdr_off + 32 + MAX_WORKERS * 4 + wid * 4) as usize;

    if my_size > 0 && my_offset > 0 {
        debug_assert!(
            my_offset + my_size <= payload_size,
            "SAL group header corrupt: my_offset={my_offset} my_size={my_size} payload_size={payload_size}"
        );
        let data_size = if my_offset + my_size <= payload_size {
            my_size as u32
        } else {
            0
        };
        SalReadResult {
            status: SAL_STATUS_HAS_DATA,
            advance,
            lsn,
            flags,
            target_id,
            epoch,
            _pad: 0,
            data_ptr: sal_ptr.add(hdr_off + my_offset),
            data_size,
            _pad2: 0,
        }
    } else {
        SalReadResult {
            status: SAL_STATUS_NO_DATA_FOR_WORKER,
            advance,
            lsn,
            flags,
            target_id,
            epoch,
            _pad: 0,
            data_ptr: std::ptr::null(),
            data_size: 0,
            _pad2: 0,
        }
    }
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
    let pk_stride = schema.pk_stride() as usize;
    let npc = schema.num_payload_cols();
    let num_regions = 3 + npc + 1;
    let header_dir_size = gnitz_wire::WAL_HEADER_SIZE + num_regions * 8;
    let total_size = data_slot.len();

    // Write WAL header (zeroed first; checksum filled in after scatter).
    data_slot[..gnitz_wire::WAL_HEADER_SIZE].fill(0);
    write_u32_le(data_slot, 8, table_id);
    write_u32_le(data_slot, 12, count as u32);
    write_u32_le(data_slot, 16, total_size as u32);
    write_u32_le(data_slot, 20, gnitz_wire::WAL_FORMAT_VERSION);
    write_u32_le(data_slot, 32, num_regions as u32);
    // LSN=0, BLOB_SIZE=0 already zeroed.

    // Write directory. All strides % 8 == 0 (schema_wire_safe), so no align8 padding.
    let mut pos = header_dir_size;
    write_u32_le(data_slot, gnitz_wire::WAL_HEADER_SIZE, pos as u32);
    write_u32_le(data_slot, gnitz_wire::WAL_HEADER_SIZE + 4, (pk_stride * count) as u32);
    pos += pk_stride * count;
    write_u32_le(data_slot, gnitz_wire::WAL_HEADER_SIZE + 8, pos as u32);
    write_u32_le(data_slot, gnitz_wire::WAL_HEADER_SIZE + 12, (8 * count) as u32);
    pos += 8 * count;
    write_u32_le(data_slot, gnitz_wire::WAL_HEADER_SIZE + 16, pos as u32);
    write_u32_le(data_slot, gnitz_wire::WAL_HEADER_SIZE + 20, (8 * count) as u32);
    pos += 8 * count;
    for (pi, _ci, col) in schema.payload_columns() {
        let stride = col.size() as usize;
        let dir_off = gnitz_wire::WAL_HEADER_SIZE + (3 + pi) * 8;
        write_u32_le(data_slot, dir_off, pos as u32);
        write_u32_le(data_slot, dir_off + 4, (stride * count) as u32);
        pos += stride * count;
    }
    let blob_dir_off = gnitz_wire::WAL_HEADER_SIZE + (3 + npc) * 8;
    write_u32_le(data_slot, blob_dir_off, pos as u32);
    write_u32_le(data_slot, blob_dir_off + 4, 0u32);

    // The writer carves `rest` (body after header+directory) into per-region
    // slices: [pk | weight | null | col_0 | ...], each sized for `count` rows.
    let (_, rest) = data_slot.split_at_mut(header_dir_size);
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

    // Compute and write the XXH3 checksum over the body (same as wal::encode).
    let cs = xxh::checksum(&data_slot[gnitz_wire::WAL_HEADER_SIZE..total_size]);
    data_slot[24..32].copy_from_slice(&cs.to_le_bytes());
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
    num_workers: usize,
    last_prefaulted: u64,
}

unsafe impl Send for SalWriter {}

impl SalWriter {
    pub fn new(ptr: *mut u8, fd: i32, mmap_size: u64, m2w_efds: Vec<i32>) -> Self {
        let num_workers = m2w_efds.len();
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
            num_workers,
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
        let nw = self.num_workers;
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

    /// Scatter rows from `input_batch` directly into per-worker SAL slots using
    /// pre-computed `worker_indices`. Eliminates the two-copy path
    /// (scatter→intermediate Batch, then Batch→SAL slot) for schemas where
    /// every column has a fixed-width 8-aligned stride and no German-string columns.
    ///
    /// Falls back to `write_group_direct` (two-copy) for other schemas.
    /// Does NOT sync/signal. `lsn` is supplied by the caller.
    ///
    /// `prebuilt_schema_block`: when `Some`, the bytes are copied into each
    /// slot's schema region instead of building one from `schema` + names.
    /// Mutually exclusive with `col_names_opt`; passing both is a bug.
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
        col_names_opt: Option<&[&[u8]]>,
        target_id: u32,
        lsn: u64,
        sal_flags: u32,
        wire_flags: u64,
        seek_pk: u128,
        seek_col_idx: u64,
        req_ids: &[u64],
        unicast_worker: i32,
        prebuilt_schema_block: Option<&[u8]>,
        wire_props: Option<(bool, u32)>,
    ) -> Result<(), String> {
        self.prefault_ahead();
        let nw = self.num_workers;
        assert_eq!(
            req_ids.len(),
            nw,
            "scatter_wire_group: req_ids.len()={} != num_workers={}",
            req_ids.len(),
            nw
        );
        debug_assert!(
            prebuilt_schema_block.is_none() || col_names_opt.is_none(),
            "scatter_wire_group: prebuilt_schema_block and col_names_opt are mutually exclusive",
        );

        let (wire_safe, wire_row_stride) = wire_props.unwrap_or_else(|| compute_wire_props(schema));

        if !wire_safe {
            // Fallback: reconstruct per-worker Batches and use existing path.
            let npc = schema.num_payload_cols();
            let mb = input_batch.as_mem_batch();
            let sub_batches: Vec<Batch> = worker_indices
                .iter()
                .map(|indices| {
                    if !indices.is_empty() {
                        Batch::from_indexed_rows(&mb, indices, schema)
                    } else {
                        Batch::empty(npc, 16)
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
                col_names_opt,
                seek_pk,
                seek_col_idx,
                req_ids,
                unicast_worker,
                0,
                prebuilt_schema_block,
                &[],
            );
        }

        // Fast path: scatter directly into SAL slots. The schema block is
        // either supplied prebuilt (cached at the caller) or built once here
        // and reused across all worker slots.
        let owned_block: Vec<u8>;
        let schema_block: &[u8] = match prebuilt_schema_block {
            Some(b) => b,
            None => {
                let col_names = col_names_opt.unwrap_or(&[]);
                owned_block = build_schema_wire_block(schema, col_names, target_id);
                &owned_block
            }
        };
        // ctrl block size for the no-error fast path is a compile-time constant.
        let ctrl_size = CTRL_BLOCK_SIZE_NO_BLOB;
        let npc = schema.num_payload_cols();

        let mut worker_sizes = [0u32; MAX_WORKERS];
        for w in 0..nw {
            if unicast_worker >= 0 && w != unicast_worker as usize {
                continue;
            }
            let count_w = worker_indices[w].len();
            let data_sz = if count_w > 0 {
                data_wire_block_size_cached(count_w, npc, wire_row_stride)
            } else {
                0
            };
            worker_sizes[w] = (ctrl_size + schema_block.len() + data_sz) as u32;
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
                let data_sz = data_wire_block_size_cached(count_w, npc, wire_row_stride);
                let data_slot = &mut slot[data_start..data_start + data_sz];
                write_scattered_data_block(&mb, &worker_indices[w], schema, count_w, target_id, data_slot);
            }

            // c. Ctrl block last (needs full_wire_flags which depends on count_w).
            let full_wire_flags = wire_flags
                | FLAG_HAS_SCHEMA
                | if count_w > 0 { FLAG_HAS_DATA } else { 0 }
                | if input_batch.sorted { FLAG_BATCH_SORTED } else { 0 }
                | if input_batch.consolidated {
                    FLAG_BATCH_CONSOLIDATED
                } else {
                    0
                };
            encode_ctrl_block_direct(
                slot,
                0,
                target_id as u64,
                0,
                full_wire_flags,
                seek_pk,
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
        wire_flags: u64,
        batch: Option<&Batch>,
        schema: &SchemaDescriptor,
        col_names_opt: Option<&[&[u8]]>,
        seek_pk: u128,
        seek_col_idx: u64,
        request_id: u64,
        prebuilt_schema_block: Option<&[u8]>,
    ) -> Result<(), String> {
        self.prefault_ahead();
        let nw = self.num_workers;
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
                wire_flags,
                seek_pk,
                seek_col_idx,
                request_id,
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
        let nw = self.num_workers;
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
        for w in 0..self.num_workers {
            syscall::eventfd_signal(self.m2w_efds[w]);
        }
    }

    pub fn signal_one(&self, worker: usize) {
        syscall::eventfd_signal(self.m2w_efds[worker]);
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
    m2w_efd: i32,
}

unsafe impl Send for SalReader {}

impl SalReader {
    pub fn new(ptr: *const u8, worker_id: u32, _mmap_size: usize, m2w_efd: i32) -> Self {
        SalReader {
            ptr,
            worker_id,
            m2w_efd,
        }
    }

    /// Read next group at `cursor`. Returns None if no message.
    /// On success returns (message, new_cursor).
    pub fn try_read(&self, cursor: u64) -> Option<(SalMessage<'static>, u64)> {
        let result = unsafe { sal_read_group_header(self.ptr, cursor, self.worker_id) };
        if result.advance == 0 {
            return None;
        }
        let new_cursor = cursor + result.advance;
        let wire_data = if result.status == SAL_STATUS_HAS_DATA && result.data_size > 0 && !result.data_ptr.is_null() {
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
        syscall::eventfd_wait(self.m2w_efd, timeout_ms)
    }
}
