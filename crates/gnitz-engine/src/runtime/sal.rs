//! SAL (shared append-only log): master→worker broadcast channel.
//!
//! Owns the mmap layout, group-header write/read helpers, SalWriter,
//! SalMessage, SalReader, and the atomic primitives used by the SAL.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::runtime::sys as ipc_sys;
use crate::sys;
use crate::schema::SchemaDescriptor;
use crate::storage::Batch;
use crate::util::{align8, read_u32_raw, read_u64_raw, write_u32_raw, write_u64_raw};
use crate::runtime::wire::{encode_wire_into, wire_size, STATUS_OK};

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
pub(crate) struct SalGroup {
    sal_ptr: *mut u8,
    hdr_off: usize,
    base: usize,
    total: usize,
    payload_size: usize,
    mmap_size: usize,
}

impl SalGroup {
    #[inline]
    pub(crate) unsafe fn data_ptr(&self, offset: usize) -> *mut u8 {
        self.sal_ptr.add(self.hdr_off + offset)
    }

    pub(crate) unsafe fn commit(self) -> u64 {
        sal_write_sentinel(self.sal_ptr, self.base + self.total, self.mmap_size);
        atomic_store_u64(self.sal_ptr.add(self.base), self.payload_size as u64);
        (self.base + self.total) as u64
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

    Some(SalGroup { sal_ptr, hdr_off, base, total, payload_size, mmap_size })
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
