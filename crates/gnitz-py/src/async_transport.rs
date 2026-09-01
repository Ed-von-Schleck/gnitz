//! The asyncio transport: a `Session` driven on a Python event loop, one
//! outstanding slot per loop future.
//!
//! Rust owns the session, the slots and what the loop is watching for; Python
//! owns the loop itself. The callbacks run *on* the loop thread, so futures
//! resolve directly: no thread, no channel, no cross-thread wake.
//!
//! It shares nothing with the blocking client but the error mappers — one
//! drives a `GnitzClient`, the other a sans-io `Session`.

use pyo3::prelude::*;

use gnitz_core::{ClientError, WireConflictMode};

use crate::read::triple_to_lazy;
use crate::write::{pk_tuple_from_py, PyZSetBatch};
use crate::{build_pylist, client_err, to_py_err};

// resolve directly: no thread, no channel, no cross-thread wake.

/// One outstanding operation. `include_hidden` is how the *result* is surfaced,
/// not part of the request, so it travels with the future.
struct Outstanding {
    future: Py<PyAny>,
    include_hidden: bool,
}

#[pyclass(name = "AsyncTransport")]
pub(crate) struct PyAsyncTransport {
    session: gnitz_core::Session,
    /// `event_loop.create_future`, bound once — the submit path calls it per
    /// operation, and resolving the attribute by name each time would build
    /// its name string every call.
    create_future: Py<PyAny>,
    slots: std::collections::HashMap<gnitz_core::SlotId, Outstanding>,
    /// Set by `close` and by a failed step; every later step is a no-op.
    closed: bool,
    /// The loop this transport is registered on, and the fd it is registered
    /// under. Rust owns the registration, so nothing above it can hold a second
    /// copy of what is armed.
    event_loop: Py<PyAny>,
    fd: i32,
    /// Exactly "the loop owes us an `on_writable`".
    writer_armed: bool,
    /// The two bound methods the loop holds, built by [`Self::install`] because
    /// `arm` has only a `&mut self` to build one from. They point back at this
    /// transport, so [`Self::shutdown`] drops them rather than leaving a cycle
    /// for the collector.
    on_readable: Option<Py<PyAny>>,
    on_writable: Option<Py<PyAny>>,
}

/// The Python value one reply resolves its future to. The spine already
/// narrowed it against the request, so nothing here needs the session.
fn narrow(py: Python<'_>, reply: gnitz_core::Reply, include_hidden: bool) -> PyResult<Py<PyAny>> {
    match reply {
        gnitz_core::Reply::Lsn(lsn) => Ok(lsn.into_pyobject(py)?.into_any().unbind()),
        gnitz_core::Reply::Scan(r) => Ok(triple_to_lazy(py, r, include_hidden)?.into_any()),
        // One PyScanResult per relation, in request order → a Python list,
        // resolving the single scan_many future.
        gnitz_core::Reply::Multi(replies) => {
            let per_rel = replies.into_iter().map(|r| triple_to_lazy(py, r, include_hidden));
            Ok(build_pylist(py, per_rel)?.into_any().unbind())
        }
        _ => unreachable!("this transport submits no verb with another reply shape"),
    }
}

/// Resolve one loop future, skipping one already `done()` — a cancelled future
/// refuses a result.
fn settle(py: Python<'_>, future: &Py<PyAny>, value: PyResult<Py<PyAny>>) {
    let _ = (|| -> PyResult<()> {
        let bound = future.bind(py);
        if bound.call_method0(pyo3::intern!(py, "done"))?.is_truthy()? {
            return Ok(());
        }
        match value {
            Ok(v) => bound.call_method1(pyo3::intern!(py, "set_result"), (v,))?,
            Err(e) => bound.call_method1(pyo3::intern!(py, "set_exception"), (e.into_value(py),))?,
        };
        Ok(())
    })();
}

impl PyAsyncTransport {
    /// Register a submitted slot against a fresh loop future, and ask the loop
    /// for a writer — a submitted frame is only queued, and deferring the flush
    /// to that callback keeps a whole turn's submits in one `writev`. Built
    /// after the submit, so a request the spine refuses raises where the caller
    /// made it.
    fn register(&mut self, py: Python<'_>, slot: gnitz_core::SlotId, include_hidden: bool) -> PyResult<Py<PyAny>> {
        let future = self.create_future.call0(py)?;
        self.slots.insert(
            slot,
            Outstanding {
                future: future.clone_ref(py),
                include_hidden,
            },
        );
        self.arm(py, true)?;
        Ok(future)
    }

    /// Add or remove the loop's writable callback, idempotently. Disarming
    /// matters as much as arming: a writer left on an always-writable fd spins
    /// the loop at 100%.
    fn arm(&mut self, py: Python<'_>, on: bool) -> PyResult<()> {
        if self.closed || on == self.writer_armed {
            return Ok(());
        }
        let event_loop = self.event_loop.bind(py);
        match (on, &self.on_writable) {
            (true, Some(cb)) => {
                event_loop.call_method1(pyo3::intern!(py, "add_writer"), (self.fd, cb))?;
            }
            (true, None) => return Ok(()), // never installed: nothing to arm
            (false, _) => {
                event_loop.call_method1(pyo3::intern!(py, "remove_writer"), (self.fd,))?;
            }
        }
        self.writer_armed = on;
        Ok(())
    }

    /// Step the spine for what the loop says the fd will accept, resolve every
    /// slot that completed, and reconcile what the loop is watching for.
    fn drive(&mut self, py: Python<'_>, ready: gnitz_core::Interest) -> PyResult<()> {
        if self.closed {
            return Ok(());
        }
        let session = &mut self.session;
        let stepped = py.detach(|| session.step(ready));
        let done = match stepped {
            Ok(d) => d,
            Err(e) => {
                self.shutdown(py, e);
                return Ok(());
            }
        };
        for (slot, result) in done {
            self.resolve(py, slot, result);
        }
        // Asked after every step, not only a write one: on TLS a read can
        // queue ciphertext too.
        let want = self.session.interest().write;
        self.arm(py, want)
    }

    fn resolve(&mut self, py: Python<'_>, slot: gnitz_core::SlotId, result: Result<gnitz_core::Reply, ClientError>) {
        let Some(o) = self.slots.remove(&slot) else {
            return;
        };
        let value = match result {
            Ok(reply) => narrow(py, reply, o.include_hidden),
            Err(e) => Err(client_err(e)),
        };
        settle(py, &o.future, value);
    }

    /// Deregister both callbacks and drop them, so the selector never holds a
    /// dead fd and the transport stops pointing at itself.
    fn deregister(&mut self, py: Python<'_>) {
        let event_loop = self.event_loop.bind(py);
        // A loop that is already closed has nothing left to deregister, which is
        // the only way either of these fails.
        if std::mem::take(&mut self.writer_armed) {
            let _ = event_loop.call_method1(pyo3::intern!(py, "remove_writer"), (self.fd,));
        }
        if self.on_readable.take().is_some() {
            let _ = event_loop.call_method1(pyo3::intern!(py, "remove_reader"), (self.fd,));
        }
        self.on_writable = None;
    }

    /// Abandon everything with `cause` and refuse further work. One `PyErr` for
    /// every slot: `ClientError` is not `Clone`, and N slots want N references
    /// to one failure, not N constructions of it.
    fn shutdown(&mut self, py: Python<'_>, cause: ClientError) {
        self.closed = true;
        self.deregister(py);
        self.session.close();
        let err = client_err(cause);
        for (_, o) in std::mem::take(&mut self.slots) {
            settle(py, &o.future, Err(err.clone_ref(py)));
        }
    }
}

#[pymethods]
impl PyAsyncTransport {
    #[new]
    fn new(py: Python<'_>, socket_path: &str, event_loop: Py<PyAny>) -> PyResult<Self> {
        // Connect + HELLO run on the calling (loop) thread, GIL dropped across
        // the blocking syscalls. A bare `Session`, not a `GnitzClient`: no OCC
        // basis to track, so the HELLO ACK's `published_lsn` is discarded.
        let (session, _published_lsn) = to_py_err(py.detach(|| gnitz_core::Session::connect(socket_path)))?;
        let fd = session.as_raw_fd();
        let create_future = event_loop.getattr(py, "create_future")?;
        Ok(PyAsyncTransport {
            session,
            create_future,
            slots: std::collections::HashMap::new(),
            closed: false,
            event_loop,
            fd,
            writer_armed: false,
            on_readable: None,
            on_writable: None,
        })
    }

    /// Bind the two loop callbacks and register the reader — a second step
    /// because binding them needs a `Bound<Self>`, which `#[new]` cannot
    /// produce. The reader stays armed for the connection's life: on a quiet
    /// socket it costs nothing, where arming per operation costs two
    /// `epoll_ctl`.
    fn install(slf: Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let on_readable = slf.getattr(pyo3::intern!(py, "on_readable"))?.unbind();
        let on_writable = slf.getattr(pyo3::intern!(py, "on_writable"))?.unbind();
        let (event_loop, fd) = {
            let t = slf.borrow();
            (t.event_loop.clone_ref(py), t.fd)
        };
        event_loop
            .bind(py)
            .call_method1(pyo3::intern!(py, "add_reader"), (fd, &on_readable))?;
        let mut t = slf.borrow_mut();
        t.on_readable = Some(on_readable);
        t.on_writable = Some(on_writable);
        Ok(())
    }

    fn push(&mut self, py: Python<'_>, target_id: u64, batch: PyRef<'_, PyZSetBatch>) -> PyResult<Py<PyAny>> {
        // `submit` packs warm against the session's own cache; a stale stamp's
        // mismatch fails this slot and the caller re-issues. No await here to
        // back-pressure on, so at the cap ship what the socket will take;
        // `submit` below raises only if that was not enough.
        if self.session.queued_bytes() >= gnitz_core::MAX_QUEUED_BYTES {
            self.drive(py, gnitz_core::Interest::WRITE)?;
        }
        let schema = batch.schema.as_ref();
        let b = &batch.batch;
        let session = &mut self.session;
        let slot = py.detach(|| {
            session.submit(gnitz_core::Request::Push {
                target_id,
                schema,
                batch: b,
                mode: WireConflictMode::Update,
            })
        });
        let slot = to_py_err(slot)?;
        self.register(py, slot, false)
    }

    #[pyo3(signature = (target_id, include_hidden = false))]
    fn scan(&mut self, py: Python<'_>, target_id: u64, include_hidden: bool) -> PyResult<Py<PyAny>> {
        let slot = to_py_err(self.session.submit(gnitz_core::Request::scan(target_id)))?;
        self.register(py, slot, include_hidden)
    }

    /// scan_many(target_ids, include_hidden=False) -> awaitable[list[ScanResult]]
    ///
    /// Consistent snapshot of N relations at one server-side SAL cut, resolved
    /// as a list in request order. A malformed list (empty, over-cap, duplicate
    /// tid) is rejected before any frame is written, and raises here.
    #[pyo3(signature = (target_ids, include_hidden = false))]
    fn scan_many(&mut self, py: Python<'_>, target_ids: Vec<u64>, include_hidden: bool) -> PyResult<Py<PyAny>> {
        let slot = to_py_err(self.session.submit(gnitz_core::Request::ScanMulti(&target_ids)))?;
        self.register(py, slot, include_hidden)
    }

    #[pyo3(signature = (target_id, pk, include_hidden = false))]
    fn seek(
        &mut self,
        py: Python<'_>,
        target_id: u64,
        pk: Bound<'_, PyAny>,
        include_hidden: bool,
    ) -> PyResult<Py<PyAny>> {
        let t = pk_tuple_from_py(&pk)?;
        let slot = to_py_err(self.session.submit(gnitz_core::Request::seek(target_id, &t)))?;
        self.register(py, slot, include_hidden)
    }

    /// One pyo3 crossing per readable event. It flushes as well as reads, so a
    /// submit a coroutine made earlier in this same loop turn ships here rather
    /// than arming a writer for one turn.
    fn on_readable(&mut self, py: Python<'_>) -> PyResult<()> {
        self.drive(py, gnitz_core::Interest::BOTH)
    }

    /// The writer callback: `step(WRITE)`.
    fn on_writable(&mut self, py: Python<'_>) -> PyResult<()> {
        self.drive(py, gnitz_core::Interest::WRITE)
    }

    #[getter]
    fn client_id(&self) -> u64 {
        self.session.client_id
    }

    fn close(&mut self, py: Python<'_>) {
        self.shutdown(py, ClientError::Closed);
    }
}
