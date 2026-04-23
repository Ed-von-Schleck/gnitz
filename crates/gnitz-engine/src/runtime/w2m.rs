//! W2M (worker→master) SPSC ring: write side (W2mWriter) and read side (W2mReceiver).

use std::sync::atomic::{AtomicU32, Ordering};

use crate::runtime::sys as ipc_sys;
use crate::runtime::w2m_ring::{self, W2mRingHeader, FLAG_MASTER_PARKED, FLAG_WRITER_PARKED, TryReserve};
use crate::runtime::wire::{decode_wire, DecodedWire};

/// Worker's write side of a single W2M ring.
pub struct W2mWriter {
    region_ptr: *mut u8,
}

unsafe impl Send for W2mWriter {}

impl W2mWriter {
    pub fn new(region_ptr: *mut u8, region_size: u64) -> Self {
        let hdr = unsafe { W2mRingHeader::from_raw(region_ptr as *const u8) };
        assert_eq!(
            hdr.capacity(), region_size,
            "W2mWriter region_size must match ring header capacity",
        );
        W2mWriter { region_ptr }
    }

    /// Encode wire data into the W2M ring. Blocks on `writer_seq` if full.
    pub fn send_encoded(&self, sz: usize, encode_fn: impl FnOnce(&mut [u8])) {
        assert!(
            (sz as u64) <= w2m_ring::MAX_W2M_MSG,
            "W2mWriter::send_encoded: sz={} exceeds MAX_W2M_MSG={}",
            sz, w2m_ring::MAX_W2M_MSG,
        );
        let hdr = unsafe { W2mRingHeader::from_raw(self.region_ptr as *const u8) };

        let reservation = loop {
            let r = unsafe { w2m_ring::try_reserve(hdr, self.region_ptr, sz) };
            match r {
                TryReserve::Ok(r) => break r,
                TryReserve::Full => {
                    let expected = hdr.writer_seq().load(Ordering::Acquire);
                    hdr.waiter_flags()
                        .fetch_or(FLAG_WRITER_PARKED, Ordering::AcqRel);
                    let room_now = unsafe { w2m_ring::has_room(hdr, sz) };
                    if !room_now {
                        let _ = ipc_sys::futex_wait_u32(
                            hdr.writer_seq() as *const AtomicU32,
                            expected,
                            -1,
                        );
                    }
                    hdr.waiter_flags()
                        .fetch_and(!FLAG_WRITER_PARKED, Ordering::AcqRel);
                }
            }
        };

        unsafe {
            if reservation.slot_len > 0 {
                let slice = std::slice::from_raw_parts_mut(
                    reservation.slot_ptr, reservation.slot_len,
                );
                encode_fn(slice);
            }
            w2m_ring::commit(hdr, &reservation);
        }

        hdr.reader_seq().fetch_add(1, Ordering::Release);
        if hdr.waiter_flags().load(Ordering::Acquire) & FLAG_MASTER_PARKED != 0 {
            let _ = ipc_sys::futex_wake_u32(
                hdr.reader_seq() as *const AtomicU32, 1,
            );
        }
    }
}

/// Master's read side of W2M.
pub struct W2mReceiver {
    region_ptrs: Vec<*mut u8>,
}

unsafe impl Send for W2mReceiver {}

impl W2mReceiver {
    pub fn new(region_ptrs: Vec<*mut u8>) -> Self {
        W2mReceiver { region_ptrs }
    }

    /// # Safety
    /// `worker` must be < `num_workers`.
    #[inline]
    pub unsafe fn header(&self, worker: usize) -> &'static W2mRingHeader {
        W2mRingHeader::from_raw(self.region_ptrs[worker] as *const u8)
    }

    pub fn try_read_with<F, T>(&self, worker: usize, f: F) -> Option<T>
    where
        F: FnOnce(&[u8]) -> T,
    {
        let hdr = unsafe { self.header(worker) };
        let cursor = hdr.read_cursor().load(Ordering::Acquire);
        let (ptr, sz, new_rc) = unsafe {
            w2m_ring::try_consume(hdr, self.region_ptrs[worker] as *const u8, cursor)?
        };

        let result = {
            let slot = unsafe { std::slice::from_raw_parts(ptr, sz as usize) };
            f(slot)
        };

        hdr.read_cursor().store(new_rc, Ordering::Release);
        hdr.writer_seq().fetch_add(1, Ordering::Release);
        if hdr.waiter_flags().load(Ordering::Acquire) & FLAG_WRITER_PARKED != 0 {
            let _ = ipc_sys::futex_wake_u32(
                hdr.writer_seq() as *const AtomicU32, 1,
            );
        }

        Some(result)
    }

    pub fn try_read(&self, worker: usize) -> Option<DecodedWire> {
        #[allow(clippy::large_enum_variant)]
        enum Outcome {
            NoData(DecodedWire),
            HasData(Vec<u8>),
            BadWire,
        }
        let outcome = self.try_read_with(worker, |slot| match decode_wire(slot) {
            Err(_) => Outcome::BadWire,
            Ok(d) if d.data_batch.is_none() => Outcome::NoData(d),
            Ok(_) => Outcome::HasData(slot.to_vec()),
        })?;
        match outcome {
            Outcome::NoData(d) => Some(d),
            Outcome::HasData(owned) => {
                let mut d = decode_wire(&owned).ok()?;
                d.batch_backing = Some(owned);
                Some(d)
            }
            Outcome::BadWire => None,
        }
    }

    pub fn wait_for(&self, worker: usize, timeout_ms: i32) -> i32 {
        let hdr = unsafe { self.header(worker) };
        hdr.waiter_flags().fetch_or(FLAG_MASTER_PARKED, Ordering::AcqRel);
        let expected = hdr.reader_seq().load(Ordering::Acquire);
        let wc = hdr.write_cursor().load(Ordering::Acquire);
        let rc_cur = hdr.read_cursor().load(Ordering::Acquire);
        let rc = if wc != rc_cur {
            0
        } else {
            ipc_sys::futex_wait_u32(
                hdr.reader_seq() as *const AtomicU32,
                expected,
                timeout_ms,
            )
        };
        hdr.waiter_flags().fetch_and(!FLAG_MASTER_PARKED, Ordering::AcqRel);
        rc
    }

    pub fn num_workers(&self) -> usize { self.region_ptrs.len() }
}
