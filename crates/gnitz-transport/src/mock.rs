use std::collections::VecDeque;

use crate::ring::{Cqe, Ring};

/// Record of a prep_* call for test assertions.
#[derive(Debug, Clone)]
pub enum PrepRecord {
    Recv {
        fd: i32,
        buf: *mut u8,
        len: u32,
        user_data: u64,
    },
    Send {
        fd: i32,
        buf: *const u8,
        len: u32,
        user_data: u64,
    },
    Accept {
        fd: i32,
        user_data: u64,
    },
    PollAdd {
        fd: i32,
        mask: u32,
        user_data: u64,
    },
    Timeout {
        timeout_ns: u64,
        user_data: u64,
    },
}

// Safety: PrepRecord contains raw pointers but MockRing is only used in
// single-threaded tests. We never dereference these pointers in the mock.
unsafe impl Send for PrepRecord {}
unsafe impl Sync for PrepRecord {}

/// Deterministic Ring implementation for unit testing.
///
/// CQE sequences are scripted via `push_cqes`. SQ capacity is configurable
/// via `with_sq_cap` to test auto-flush behavior.
pub struct MockRing {
    sq_cap: u32,
    pending: u32,
    pub auto_flush_count: u32,
    scripted_cqes: VecDeque<Vec<Cqe>>,
    pub prepped: Vec<PrepRecord>,
    pub submit_count: u32,
}

impl MockRing {
    pub fn new() -> Self {
        MockRing {
            sq_cap: 256,
            pending: 0,
            auto_flush_count: 0,
            scripted_cqes: VecDeque::new(),
            prepped: Vec::new(),
            submit_count: 0,
        }
    }

    pub fn with_sq_cap(mut self, cap: u32) -> Self {
        self.sq_cap = cap;
        self
    }

    /// Script a batch of CQEs to be returned by the next `drain_cqes` call.
    pub fn push_cqes(&mut self, cqes: Vec<Cqe>) {
        self.scripted_cqes.push_back(cqes);
    }

    fn flush_pending(&mut self) {
        self.pending = 0;
        self.auto_flush_count += 1;
    }

    fn ensure_sq_room(&mut self) {
        if self.pending >= self.sq_cap {
            self.flush_pending();
        }
    }
}

impl Ring for MockRing {
    fn prep_recv(&mut self, fd: i32, buf: *mut u8, len: u32, user_data: u64) {
        self.ensure_sq_room();
        self.pending += 1;
        self.prepped.push(PrepRecord::Recv {
            fd,
            buf,
            len,
            user_data,
        });
    }

    fn prep_send(&mut self, fd: i32, buf: *const u8, len: u32, user_data: u64) {
        self.ensure_sq_room();
        self.pending += 1;
        self.prepped.push(PrepRecord::Send {
            fd,
            buf,
            len,
            user_data,
        });
    }

    fn prep_accept(&mut self, fd: i32, user_data: u64) {
        self.ensure_sq_room();
        self.pending += 1;
        self.prepped.push(PrepRecord::Accept { fd, user_data });
    }

    fn prep_poll_add(&mut self, fd: i32, mask: u32, user_data: u64) {
        self.ensure_sq_room();
        self.pending += 1;
        self.prepped.push(PrepRecord::PollAdd { fd, mask, user_data });
    }

    fn prep_timeout(&mut self, timeout_ns: u64, user_data: u64) {
        self.ensure_sq_room();
        self.pending += 1;
        self.prepped.push(PrepRecord::Timeout { timeout_ns, user_data });
    }

    fn submit_and_wait_timeout(
        &mut self,
        _min_complete: u32,
        _timeout_ms: i32,
    ) -> Result<i32, i32> {
        self.submit_count += 1;
        self.pending = 0;
        Ok(0)
    }

    fn drain_cqes(&mut self, out: &mut [Cqe]) -> usize {
        let Some(batch) = self.scripted_cqes.pop_front() else {
            return 0;
        };
        let n = batch.len().min(out.len());
        out[..n].copy_from_slice(&batch[..n]);
        n
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ring::*;

    #[test]
    fn test_scripted_cqes() {
        let mut ring = MockRing::new();
        ring.push_cqes(vec![
            Cqe { user_data: make_udata(TAG_RECV, 5), res: 4, flags: 0 },
            Cqe { user_data: make_udata(TAG_SEND, 6), res: 100, flags: 0 },
        ]);

        let mut buf = [Cqe::default(); 16];
        let n = ring.drain_cqes(&mut buf);
        assert_eq!(n, 2);
        assert_eq!(cqe_fd(buf[0].user_data), 5);
        assert_eq!(cqe_fd(buf[1].user_data), 6);

        // Second drain returns 0 (no more scripted)
        assert_eq!(ring.drain_cqes(&mut buf), 0);
    }

    #[test]
    fn test_sq_auto_flush() {
        let mut ring = MockRing::new().with_sq_cap(4);
        let dummy = std::ptr::null_mut();

        // 4 preps fill the SQ
        for i in 0..4 {
            ring.prep_recv(i, dummy, 4, make_udata(TAG_RECV, i));
        }
        assert_eq!(ring.auto_flush_count, 0);
        assert_eq!(ring.pending, 4);

        // 5th prep triggers auto-flush
        ring.prep_recv(4, dummy, 4, make_udata(TAG_RECV, 4));
        assert_eq!(ring.auto_flush_count, 1);
        assert_eq!(ring.pending, 1); // flushed, then 1 new

        assert_eq!(ring.prepped.len(), 5);
    }

    #[test]
    fn test_prep_recording() {
        let mut ring = MockRing::new();
        let buf = 0x1234 as *mut u8;
        ring.prep_recv(10, buf, 64, make_udata(TAG_RECV, 10));

        assert_eq!(ring.prepped.len(), 1);
        match &ring.prepped[0] {
            PrepRecord::Recv { fd, len, .. } => {
                assert_eq!(*fd, 10);
                assert_eq!(*len, 64);
            }
            _ => panic!("expected Recv"),
        }
    }
}
