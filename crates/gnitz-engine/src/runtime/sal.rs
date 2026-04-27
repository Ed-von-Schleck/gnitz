//! SAL (shared append-only log): master→worker broadcast channel.
//!
//! Owns the mmap layout, group-header write/read helpers, SalWriter,
//! SalMessage, SalReader, and the atomic primitives used by the SAL.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::runtime::sys as ipc_sys;
use crate::sys;
use crate::schema::{SchemaDescriptor, type_code};
use crate::storage::{Batch, DirectWriter, scatter_copy};
use crate::util::{align8, read_u32_raw, read_u64_raw, write_u32_raw, write_u64_raw, write_u32_le};
use crate::runtime::wire::{
    encode_wire_into, wire_size, STATUS_OK,
    build_schema_wire_block, encode_ctrl_block_ipc,
    FLAG_HAS_SCHEMA, FLAG_HAS_DATA, FLAG_BATCH_SORTED, FLAG_BATCH_CONSOLIDATED,
};
use crate::xxh;

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
pub const FLAG_EXCHANGE_RELAY: u32      = 512;
pub const FLAG_PRELOADED_EXCHANGE: u32  = 1024;
pub const FLAG_BACKFILL: u32            = 2048;
pub const FLAG_TICK: u32                = 4096;
pub const FLAG_CHECKPOINT: u32          = 8192;
pub const FLAG_FLUSH: u32               = 16384;
/// Marks an empty broadcast group as the closing "commit sentinel" of an
/// atomic zone. All preceding groups at the same LSN belong to the zone;
/// recovery applies them only when this sentinel is on disk. The flag
/// rides on top of FLAG_DDL_SYNC for the worker's dispatch loop, which
/// already no-ops on a DDL_SYNC group with `count == 0`.
pub const FLAG_TXN_COMMIT: u32          = 32768;

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
    Push,
    Tick,
    SeekByIndex,
    Seek,
    Scan,
}

impl SalMessageKind {
    /// Classify a SAL group by its flag word.
    ///
    /// Priority order matches the worker's existing if-chain (see
    /// `worker::dispatch_inner`): SHUTDOWN > FLUSH > DDL_SYNC >
    /// EXCHANGE_RELAY > PRELOADED_EXCHANGE > BACKFILL > HAS_PK >
    /// PUSH > TICK > SEEK_BY_INDEX > SEEK > Scan. The first match wins.
    pub fn classify(flags: u32) -> SalMessageKind {
        if flags & FLAG_SHUTDOWN != 0           { return SalMessageKind::Shutdown; }
        if flags & FLAG_FLUSH != 0              { return SalMessageKind::Flush; }
        if flags & FLAG_DDL_SYNC != 0           { return SalMessageKind::DdlSync; }
        if flags & FLAG_EXCHANGE_RELAY != 0     { return SalMessageKind::ExchangeRelay; }
        if flags & FLAG_PRELOADED_EXCHANGE != 0 { return SalMessageKind::PreloadedExchange; }
        if flags & FLAG_BACKFILL != 0           { return SalMessageKind::Backfill; }
        if flags & FLAG_HAS_PK != 0             { return SalMessageKind::HasPk; }
        if flags & FLAG_PUSH != 0               { return SalMessageKind::Push; }
        if flags & FLAG_TICK != 0               { return SalMessageKind::Tick; }
        if flags & FLAG_SEEK_BY_INDEX != 0      { return SalMessageKind::SeekByIndex; }
        if flags & FLAG_SEEK != 0               { return SalMessageKind::Seek; }
        SalMessageKind::Scan
    }

    /// True when the worker must act on the group even if its per-worker
    /// data slot is empty (broadcast / control / data-rebroadcast kinds).
    /// Unicast kinds (SEEK, SEEK_BY_INDEX, Scan, ExchangeRelay) return
    /// false: a missing slot means the message wasn't for us.
    pub fn is_broadcast(self) -> bool {
        matches!(
            self,
            SalMessageKind::Shutdown
                | SalMessageKind::Flush
                | SalMessageKind::DdlSync
                | SalMessageKind::PreloadedExchange
                | SalMessageKind::Backfill
                | SalMessageKind::HasPk
                | SalMessageKind::Push
                | SalMessageKind::Tick
        )
    }

    /// All variants in classification priority order. Used by tests that
    /// walk every kind to exercise the dispatcher's full match.
    #[allow(dead_code)]
    pub const ALL: [SalMessageKind; 12] = [
        SalMessageKind::Shutdown,
        SalMessageKind::Flush,
        SalMessageKind::DdlSync,
        SalMessageKind::ExchangeRelay,
        SalMessageKind::PreloadedExchange,
        SalMessageKind::Backfill,
        SalMessageKind::HasPk,
        SalMessageKind::Push,
        SalMessageKind::Tick,
        SalMessageKind::SeekByIndex,
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

/// True when every column has a fixed-width 8-aligned stride and no STRING
/// columns. Batches satisfying this can be scatter-encoded directly into SAL
/// slots without intermediate per-worker Batch allocations.
fn schema_wire_safe(schema: &SchemaDescriptor) -> bool {
    (0..schema.num_columns as usize).all(|ci| {
        let c = &schema.columns[ci];
        c.type_code != type_code::STRING && c.size % 8 == 0
    })
}

/// Compute the byte size of the data WAL block for `count` rows of `schema`.
/// Only correct when `schema_wire_safe(schema)` holds (all strides are 8-aligned
/// so align8 is always a noop).
fn data_wire_block_size(count: usize, schema: &SchemaDescriptor) -> usize {
    let pk_stride = schema.columns[schema.pk_index as usize].size as usize;
    let npc = schema.num_columns as usize - 1;
    let num_regions = 3 + npc + 1;
    let mut size = gnitz_wire::WAL_HEADER_SIZE + num_regions * 8;
    size += pk_stride * count; // pk
    size += 8 * count;         // weight
    size += 8 * count;         // null_bmp
    for (_pi, _ci, col) in schema.payload_columns() {
        size += col.size as usize * count;
    }
    // blob = 0 bytes
    size
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
        debug_assert!(self.committed, "SalGroup dropped without commit — SAL sentinel never written");
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
    if num_workers > MAX_WORKERS { return None; }

    // payload_size starts as GROUP_HEADER_SIZE (576, a multiple of 8) and grows
    // only by align8(sz) increments, so it is always a multiple of 8.
    let mut payload_size = GROUP_HEADER_SIZE;
    for &sz in &worker_sizes[..num_workers] {
        if sz > 0 { payload_size += align8(sz as usize); }
    }
    let total = 8 + payload_size;
    if write_cursor + total > mmap_size { return None; }

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

    Some(SalGroup { sal_ptr, hdr_off, base, total, payload_size, mmap_size, committed: false })
}

/// Write a message group into the SAL for N workers (test helper).
#[cfg(test)]
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
    for w in 0..nw { sizes[w] = *worker_sizes.add(w); }

    let group = match sal_begin_group(
        sal_ptr, write_cursor as usize, mmap_size as usize,
        nw, target_id, lsn, flags, epoch, &sizes[..nw],
    ) {
        Some(g) => g,
        None => return SalWriteResult { status: -1, new_cursor: write_cursor },
    };

    let mut off = GROUP_HEADER_SIZE;
    for w in 0..nw {
        let sz = sizes[w] as usize;
        if sz > 0 {
            let p = *worker_ptrs.add(w);
            if !p.is_null() {
                std::ptr::copy_nonoverlapping(p, group.data_ptr(off), sz);
            }
            off += align8(sz);
        }
    }

    SalWriteResult { status: 0, new_cursor: group.commit() }
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
pub(crate) unsafe fn sal_read_group_header(
    sal_ptr: *const u8,
    read_cursor: u64,
    worker_id: u32,
) -> SalReadResult {
    let rc = read_cursor as usize;
    let wid = worker_id as usize;

    let payload_size = atomic_load_u64(sal_ptr.add(rc)) as usize;
    if payload_size == 0 {
        return SalReadResult {
            status: SAL_STATUS_NO_MESSAGE, advance: 0, lsn: 0, flags: 0,
            target_id: 0, epoch: 0, _pad: 0,
            data_ptr: std::ptr::null(), data_size: 0, _pad2: 0,
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
            "SAL group header corrupt: my_offset={} my_size={} payload_size={}",
            my_offset, my_size, payload_size
        );
        let data_size = if my_offset + my_size <= payload_size { my_size as u32 } else { 0 };
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
/// `indices` from `batch`. Assumes `schema_wire_safe` — no STRING columns,
/// all strides are multiples of 8, so all align8 calls are no-ops.
fn write_scattered_data_block(
    batch: &crate::storage::MemBatch<'_>,
    indices: &[u32],
    schema: &SchemaDescriptor,
    count: usize,
    table_id: u32,
    data_slot: &mut [u8],
) {
    let pk_stride = schema.columns[schema.pk_index as usize].size as usize;
    let npc = schema.num_columns as usize - 1;
    let num_regions = 3 + npc + 1;
    let header_dir_size = gnitz_wire::WAL_HEADER_SIZE + num_regions * 8;
    let total_size = data_slot.len();

    // Write WAL header (zeroed first; checksum filled in after scatter).
    data_slot[..gnitz_wire::WAL_HEADER_SIZE].fill(0);
    write_u32_le(data_slot,  8, table_id);
    write_u32_le(data_slot, 12, count as u32);
    write_u32_le(data_slot, 16, total_size as u32);
    write_u32_le(data_slot, 20, gnitz_wire::WAL_FORMAT_VERSION);
    write_u32_le(data_slot, 32, num_regions as u32);
    // LSN=0, BLOB_SIZE=0 already zeroed.

    // Write directory. All strides % 8 == 0 (schema_wire_safe), so no align8 padding.
    let mut pos = header_dir_size;
    write_u32_le(data_slot, gnitz_wire::WAL_HEADER_SIZE,     pos as u32);
    write_u32_le(data_slot, gnitz_wire::WAL_HEADER_SIZE + 4, (pk_stride * count) as u32);
    pos += pk_stride * count;
    write_u32_le(data_slot, gnitz_wire::WAL_HEADER_SIZE + 8,  pos as u32);
    write_u32_le(data_slot, gnitz_wire::WAL_HEADER_SIZE + 12, (8 * count) as u32);
    pos += 8 * count;
    write_u32_le(data_slot, gnitz_wire::WAL_HEADER_SIZE + 16, pos as u32);
    write_u32_le(data_slot, gnitz_wire::WAL_HEADER_SIZE + 20, (8 * count) as u32);
    pos += 8 * count;
    for (pi, _ci, col) in schema.payload_columns() {
        let stride = col.size as usize;
        let dir_off = gnitz_wire::WAL_HEADER_SIZE + (3 + pi) * 8;
        write_u32_le(data_slot, dir_off,     pos as u32);
        write_u32_le(data_slot, dir_off + 4, (stride * count) as u32);
        pos += stride * count;
    }
    let blob_dir_off = gnitz_wire::WAL_HEADER_SIZE + (3 + npc) * 8;
    write_u32_le(data_slot, blob_dir_off,     pos as u32);
    write_u32_le(data_slot, blob_dir_off + 4, 0u32);

    let (_, rest) = data_slot.split_at_mut(header_dir_size);
    let pk_sz = pk_stride * count;
    let (pk_slice, rest) = rest.split_at_mut(pk_sz);
    let (weight_slice, rest) = rest.split_at_mut(8 * count);
    let (null_slice, mut rest) = rest.split_at_mut(8 * count);
    let mut col_slices: Vec<&mut [u8]> = Vec::with_capacity(npc);
    for (_pi, _ci, col) in schema.payload_columns() {
        let col_sz = col.size as usize * count;
        let (col_slice, new_rest) = rest.split_at_mut(col_sz);
        col_slices.push(col_slice);
        rest = new_rest;
    }
    let mut writer = DirectWriter::new(pk_slice, weight_slice, null_slice, col_slices, rest, *schema);
    scatter_copy(batch, indices, &[], &mut writer);

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
        if target <= self.last_prefaulted { return; }
        let raw_start = self.last_prefaulted.max(self.write_cursor);
        let start = raw_start & !(PAGE_SIZE - 1);
        let end_raw = (target + PAGE_SIZE - 1) & !(PAGE_SIZE - 1);
        let end = end_raw.min(self.mmap_size);
        if end > start {
            sys::madvise_willneed(
                unsafe { self.ptr.add(start as usize) },
                (end - start) as usize,
            );
            self.last_prefaulted = target;
        }
    }

    /// Encode per-worker wire data directly into SAL mmap. Does NOT sync/signal.
    /// `lsn` is supplied by the caller; the SAL writer no longer owns the
    /// counter (Design 2: caller controls zone-LSN allocation).
    #[allow(clippy::too_many_arguments, clippy::needless_range_loop)]
    pub fn write_group_direct(
        &mut self,
        target_id: u32,
        lsn: u64,
        flags: u32,
        worker_batches: &[Option<&Batch>],
        schema: &SchemaDescriptor,
        col_names_opt: Option<&[&[u8]]>,
        seek_pk: u128,
        seek_col_idx: u64,
        req_ids: &[u64],
        unicast_worker: i32,
    ) -> Result<(), String> {
        self.prefault_ahead();
        let nw = self.num_workers;
        assert_eq!(req_ids.len(), nw,
            "write_group_direct: req_ids.len()={} != num_workers={}",
            req_ids.len(), nw);

        let mut worker_sizes = [0u32; MAX_WORKERS];
        for w in 0..nw {
            if unicast_worker >= 0 && w != unicast_worker as usize { continue; }
            let data_batch = worker_batches.get(w).and_then(|opt| opt.as_ref());
            worker_sizes[w] = wire_size(
                STATUS_OK, b"", Some(schema), col_names_opt, data_batch.copied(), None,
            ) as u32;
        }

        let group = unsafe {
            sal_begin_group(
                self.ptr, self.write_cursor as usize, self.mmap_size as usize,
                nw, target_id, lsn, flags, self.epoch, &worker_sizes[..nw],
            )
        }.ok_or_else(|| format!(
            "SAL write_group_direct failed (cursor={})", self.write_cursor
        ))?;

        let mut off = GROUP_HEADER_SIZE;
        for w in 0..nw {
            let wsz = worker_sizes[w] as usize;
            if wsz > 0 {
                let data_batch = worker_batches.get(w).and_then(|opt| opt.as_ref());
                let slot = unsafe { std::slice::from_raw_parts_mut(group.data_ptr(off), wsz) };
                let written = encode_wire_into(
                    slot, 0, target_id as u64, 0, flags as u64,
                    seek_pk, seek_col_idx, req_ids[w],
                    STATUS_OK, b"", Some(schema), col_names_opt, data_batch.copied(), None,
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
    /// every column has a fixed-width 8-aligned stride and no STRING columns.
    ///
    /// Falls back to `write_group_direct` (two-copy) for other schemas.
    /// Does NOT sync/signal. `lsn` is supplied by the caller.
    #[allow(clippy::too_many_arguments)]
    pub fn scatter_wire_group(
        &mut self,
        input_batch: &Batch,
        worker_indices: &[Vec<u32>],
        schema: &SchemaDescriptor,
        col_names_opt: Option<&[&[u8]]>,
        target_id: u32,
        lsn: u64,
        flags: u32,
        seek_pk: u128,
        seek_col_idx: u64,
        req_ids: &[u64],
        unicast_worker: i32,
    ) -> Result<(), String> {
        self.prefault_ahead();
        let nw = self.num_workers;
        assert_eq!(req_ids.len(), nw,
            "scatter_wire_group: req_ids.len()={} != num_workers={}",
            req_ids.len(), nw);

        if !schema_wire_safe(schema) {
            // Fallback: reconstruct per-worker Batches and use existing path.
            let npc = schema.num_columns as usize - 1;
            let mb = input_batch.as_mem_batch();
            let sub_batches: Vec<Batch> = worker_indices.iter()
                .map(|indices| {
                    if !indices.is_empty() {
                        Batch::from_indexed_rows(&mb, indices, schema)
                    } else {
                        Batch::empty(npc, 16)
                    }
                })
                .collect();
            let refs: Vec<Option<&Batch>> = sub_batches.iter()
                .map(|b| if b.count > 0 { Some(b) } else { None })
                .collect();
            return self.write_group_direct(
                target_id, lsn, flags, &refs, schema, col_names_opt,
                seek_pk, seek_col_idx, req_ids, unicast_worker,
            );
        }

        // Fast path: scatter directly into SAL slots.
        let col_names = col_names_opt.unwrap_or(&[]);
        let schema_block = build_schema_wire_block(schema, col_names, target_id);
        // ctrl block size = wire_size with no schema/data (just the ctrl WAL block).
        let ctrl_size = wire_size(STATUS_OK, b"", None, None, None, None);

        let mut worker_sizes = [0u32; MAX_WORKERS];
        for w in 0..nw {
            if unicast_worker >= 0 && w != unicast_worker as usize { continue; }
            let count_w = worker_indices[w].len();
            let data_sz = if count_w > 0 { data_wire_block_size(count_w, schema) } else { 0 };
            worker_sizes[w] = (ctrl_size + schema_block.len() + data_sz) as u32;
        }

        let group = unsafe {
            sal_begin_group(
                self.ptr, self.write_cursor as usize, self.mmap_size as usize,
                nw, target_id, lsn, flags, self.epoch, &worker_sizes[..nw],
            )
        }.ok_or_else(|| format!(
            "SAL scatter_wire_group failed (cursor={})", self.write_cursor
        ))?;

        let mb = input_batch.as_mem_batch();
        let mut off = GROUP_HEADER_SIZE;
        for w in 0..nw {
            let wsz = worker_sizes[w] as usize;
            if wsz == 0 { continue; }

            let count_w = worker_indices[w].len();
            let slot = unsafe { std::slice::from_raw_parts_mut(group.data_ptr(off), wsz) };

            // a. Schema block immediately after the ctrl slot.
            slot[ctrl_size..ctrl_size + schema_block.len()].copy_from_slice(&schema_block);

            // b. Data block when there are rows for this worker.
            if count_w > 0 {
                let data_start = ctrl_size + schema_block.len();
                let data_sz = data_wire_block_size(count_w, schema);
                let data_slot = &mut slot[data_start..data_start + data_sz];
                write_scattered_data_block(
                    &mb, &worker_indices[w], schema, count_w, target_id, data_slot,
                );
            }

            // c. Ctrl block last (needs wire_flags which depends on count_w).
            let wire_flags = flags as u64
                | FLAG_HAS_SCHEMA
                | if count_w > 0 { FLAG_HAS_DATA } else { 0 }
                | if input_batch.sorted { FLAG_BATCH_SORTED } else { 0 }
                | if input_batch.consolidated { FLAG_BATCH_CONSOLIDATED } else { 0 };
            encode_ctrl_block_ipc(
                slot, 0,
                target_id as u64, 0, wire_flags,
                seek_pk, seek_col_idx, req_ids[w],
                STATUS_OK, b"", false,
            );

            off += align8(wsz);
        }

        self.write_cursor = unsafe { group.commit() };
        Ok(())
    }

    /// Encode once into worker 0's slot, memcpy to workers 1..N-1.
    /// Does NOT sync/signal. `lsn` is supplied by the caller.
    #[allow(clippy::too_many_arguments)]
    pub fn write_broadcast_direct(
        &mut self,
        target_id: u32,
        lsn: u64,
        flags: u32,
        batch: Option<&Batch>,
        schema: &SchemaDescriptor,
        col_names_opt: Option<&[&[u8]]>,
        seek_pk: u128,
        seek_col_idx: u64,
        request_id: u64,
    ) -> Result<(), String> {
        self.prefault_ahead();
        let nw = self.num_workers;

        let wsz = wire_size(
            STATUS_OK, b"", Some(schema), col_names_opt, batch, None,
        ) as u32;
        let mut worker_sizes = [0u32; MAX_WORKERS];
        for item in worker_sizes.iter_mut().take(nw) { *item = wsz; }

        let group = unsafe {
            sal_begin_group(
                self.ptr, self.write_cursor as usize, self.mmap_size as usize,
                nw, target_id, lsn, flags, self.epoch, &worker_sizes[..nw],
            )
        }.ok_or_else(|| format!(
            "SAL write_broadcast_direct failed (cursor={})", self.write_cursor
        ))?;

        if wsz > 0 {
            let wsz = wsz as usize;
            let slot0 = unsafe { std::slice::from_raw_parts_mut(group.data_ptr(GROUP_HEADER_SIZE), wsz) };
            let written = encode_wire_into(
                slot0, 0, target_id as u64, 0, flags as u64,
                seek_pk, seek_col_idx, request_id,
                STATUS_OK, b"", Some(schema), col_names_opt, batch, None,
            );
            debug_assert_eq!(written, wsz);
            let mut off = GROUP_HEADER_SIZE + align8(wsz);
            for _ in 1..nw {
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        group.data_ptr(GROUP_HEADER_SIZE),
                        group.data_ptr(off),
                        wsz,
                    );
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
                self.ptr, self.write_cursor as usize, self.mmap_size as usize,
                nw, 0, lsn, FLAG_DDL_SYNC | FLAG_TXN_COMMIT, self.epoch,
                &worker_sizes[..nw],
            )
        }.ok_or_else(|| format!(
            "SAL write_commit_sentinel failed (cursor={})", self.write_cursor
        ))?;
        self.write_cursor = unsafe { group.commit() };
        Ok(())
    }

    pub fn signal_all(&self) {
        for w in 0..self.num_workers {
            ipc_sys::eventfd_signal(self.m2w_efds[w]);
        }
    }

    pub fn signal_one(&self, worker: usize) {
        ipc_sys::eventfd_signal(self.m2w_efds[worker]);
    }

    pub fn needs_checkpoint(&self) -> bool {
        self.write_cursor >= self.checkpoint_threshold
    }

    pub fn checkpoint_reset(&mut self) {
        self.epoch += 1;
        self.write_cursor = 0;
        self.last_prefaulted = 0;
        unsafe { atomic_store_u64(self.ptr, 0); }
    }

    pub fn reset(&mut self, cursor: u64, epoch: u32) {
        self.write_cursor = cursor;
        self.epoch = epoch;
    }

    pub fn cursor(&self) -> u64 { self.write_cursor }
    pub fn epoch(&self) -> u32 { self.epoch }
    pub fn mmap_size(&self) -> u64 { self.mmap_size }
    pub fn sal_fd(&self) -> i32 { self.fd }
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
        SalReader { ptr, worker_id, m2w_efd }
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
            Some(unsafe {
                std::slice::from_raw_parts(result.data_ptr, result.data_size as usize)
            })
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
        ipc_sys::eventfd_wait(self.m2w_efd, timeout_ms)
    }
}
