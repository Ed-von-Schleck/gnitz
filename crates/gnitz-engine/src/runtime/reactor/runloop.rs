//! Reactor run-loop driver: `spawn` / `block_on` task scheduling, the
//! `tick` CQE-drain + task-poll loop, and `drain_injected_cqes`.

use super::*;

impl Reactor {
    /// Spawn a task that runs detached. Returns the task key (useful for
    /// tests that want to assert task lifecycle).
    pub fn spawn(&self, fut: impl Future<Output = ()> + 'static) -> usize {
        let key = self.inner.next_task_key.get();
        self.inner.next_task_key.set(key + 1);
        let task = Task { future: Box::pin(fut) };
        self.inner.tasks.borrow_mut().insert(key, task);
        // Schedule immediate first poll.
        self.inner.run_queue.borrow_mut().push(key);
        key
    }

    /// Drive `fut` to completion. Single-threaded, blocking. Spawns the
    /// future as a task internally and returns its output via a shared cell.
    #[cfg(test)]
    pub(super) fn block_on<F, T>(&self, fut: F) -> T
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let out: Rc<RefCell<Option<T>>> = Rc::new(RefCell::new(None));
        let out_capture = Rc::clone(&out);
        let root_key = self.spawn(async move {
            let v = fut.await;
            *out_capture.borrow_mut() = Some(v);
        });

        // Drive the reactor until the root task completes. The `tasks`
        // map removes the entry on completion, so `contains_key(root_key)`
        // returning false is the termination signal.
        loop {
            self.tick(true);
            if !self.inner.tasks.borrow().contains_key(&root_key) {
                break;
            }
        }

        // SAFETY: spawn ran the future to completion, so Some.
        let v = out.borrow_mut().take()
            .expect("block_on root task did not produce output");
        v
    }

    /// Single iteration of the event loop:
    ///   0. proactive W2M drain (lost-wake safety net)
    ///   1. drain CQEs (waking reply / timeout / fsync wakers)
    ///   2. poll all tasks in the run queue (each polled at most once)
    ///   3. submit pending SQEs; if `block` and the run queue is now
    ///      empty, sleep until the next CQE.
    pub(super) fn tick(&self, block: bool) {
        // 0. Proactive W2M drain — safety net for lost FUTEX_WAITV wakes.
        //
        // A narrow race exists between refresh_futex_waitv_vals (which sets
        // FLAG_MASTER_PARKED and snapshots reader_seq) and arm_futex_waitv
        // (which registers the FUTEX_WAITV SQE with the kernel): a worker
        // that publishes in that window calls FUTEX_WAKE against a waiter
        // that does not exist yet. The kernel's mismatch-detection (it
        // immediately completes a FUTEX_WAITV whose expected value is stale)
        // covers the case where the arm precedes the publish; this drain
        // covers the opposite case (publish beats the arm) in both the
        // spinning path (no blocking) and the blocking path (before we sleep).
        //
        // Cost: one try_consume call per worker per tick — just two Acquire
        // loads that return None when the ring is empty.
        if self.inner.futex_waitv_armed.get() {
            let nw = self.inner.w2m.get()
                .expect("futex_waitv_armed=true but w2m not attached")
                .num_workers();
            for w in 0..nw {
                self.drain_w2m_for_worker(w);
            }
        }

        // 1. CQEs (no syscall — reads memory-mapped CQ).
        self.drain_cqes_into_wakers();
        #[cfg(test)]
        self.drain_injected_cqes();
        self.reap_closing_conns();

        // 2. Drain the run queue. Swap into a thread-local scratch buffer
        // so wakes during poll schedule for the *next* tick rather than
        // re-entering this one. `Cell::take` releases any borrow before
        // poll_task runs so waker_wake can push freely.
        let mut buf = TICK_SCRATCH.with(|c| c.take());
        self.inner.run_queue.borrow_mut().swap_into(&mut buf);
        for key in buf.drain(..) {
            self.poll_task(key);
        }
        TICK_SCRATCH.with(|c| c.set(buf));

        // 3. Submit pending SQEs and optionally block until the next event.
        // Skip blocking when there is nothing to drive (slab empty) or when
        // a wake fired during this tick (run_queue non-empty); otherwise we
        // would sleep past the natural completion of the loop.
        let should_block = block
            && !self.inner.tasks.borrow().is_empty()
            && self.inner.run_queue.borrow().is_empty();
        if should_block {
            // Block indefinitely — outstanding timer SQEs guarantee a CQE
            // will arrive when the soonest timer fires.
            if let Err(e) = self.inner.ring.borrow_mut().submit_and_wait_timeout(1, -1) {
                crate::gnitz_error!("reactor: tick blocking submit failed (errno={})", e);
            }
        } else if let Err(e) = self.inner.ring.borrow_mut().submit_and_wait_timeout(0, 0) {
            crate::gnitz_error!("reactor: tick non-blocking submit failed (errno={})", e);
        }
    }

    /// Poll a single task. The future is removed from the map for the
    /// duration of the poll (so the poll can re-enter the reactor to
    /// spawn new tasks), then reinserted at the SAME key on Pending.
    /// HashMap removal + same-key reinsertion is stable regardless of
    /// what other keys are added/removed during poll.
    fn poll_task(&self, key: usize) {
        let mut task = match self.inner.tasks.borrow_mut().remove(&key) {
            Some(t) => t,
            None => return,
        };
        let waker = make_waker(key);
        let mut cx = Context::from_waker(&waker);
        match task.future.as_mut().poll(&mut cx) {
            Poll::Ready(()) => {}
            Poll::Pending   => { self.inner.tasks.borrow_mut().insert(key, task); }
        }
    }

    /// Look at all CQEs pending in the ring. For each:
    ///   - KIND_REPLY: wake the registered reply waker (if any).
    ///   - KIND_TIMEOUT: wake the registered timer waker (if not cancelled).
    ///   - KIND_FSYNC: wake the fsync waker and park `res`.
    ///   - KIND_POLL_EVENTFD: drain the associated W2M ring and re-arm.
    pub(super) fn drain_cqes_into_wakers(&self) {
        let mut buf = [Cqe::default(); 64];
        loop {
            let n = self.inner.ring.borrow_mut().drain_cqes(&mut buf);
            if n == 0 { break; }
            for cqe in &buf[..n] {
                self.dispatch_cqe(*cqe);
            }
        }
    }

    #[cfg(test)]
    pub(super) fn drain_injected_cqes(&self) {
        loop {
            let cqe = self.inner.injected_cqes.borrow_mut().pop_front();
            match cqe {
                Some(c) => self.dispatch_cqe(c),
                None => break,
            }
        }
    }

}
