//! The asyncio transport: a `Session` driven on a Python event loop, one
//! outstanding slot per loop future.
//!
//! Rust owns the session, the slots and what the loop is watching for; Python
//! owns the loop itself. The callbacks run *on* the loop thread, so futures
//! resolve directly: no thread, no channel, no cross-thread wake.
//!
//! The blocking client drives a `GnitzClient`; this drives a `Session`.

use std::collections::VecDeque;

use pyo3::intern;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use gnitz_core::{ClientError, Interest, Reply, Request, Session, SlotId};
use gnitz_wire::{ReadBound, ReadSpec, WireConflictMode};

use crate::client_err;
use crate::read::scan_result;
use crate::schema::PySchema;
use crate::write::{pk_point_spec, PyZSetBatch};

#[pyclass(name = "AsyncTransport")]
pub(crate) struct PyAsyncTransport {
    session: Session,
    /// The loop this transport is registered on, under the session's fd.
    event_loop: Py<PyAny>,
    /// The loop future of each in-flight slot, in submit order.
    slots: VecDeque<(SlotId, Py<PyAny>)>,
    /// A `call_soon(on_writable)` is queued and has not run.
    flush_scheduled: bool,
    /// The loop owes us an `on_writable` for bytes the fd refused.
    writer_armed: bool,
    /// Set by `release`.
    released: bool,
}

/// The Python value one reply resolves its future to. The spine already
/// narrowed it against the request, so nothing here needs the session.
fn narrow(py: Python<'_>, reply: Reply) -> PyResult<Py<PyAny>> {
    match reply {
        Reply::Ack(lsn) => lsn.into_py_any(py),
        Reply::Scan(r) => Ok(scan_result(py, r)?.into_any()),
        // One PyScanResult per relation, in request order → a Python list,
        // resolving the single scan_many future.
        Reply::Multi(replies) => replies
            .into_iter()
            .map(|r| scan_result(py, r))
            .collect::<PyResult<Vec<_>>>()?
            .into_py_any(py),
        Reply::Resolve(_) | Reply::Polled => {
            unreachable!("this transport submits no verb with another reply shape")
        }
    }
}

/// Resolve one loop future; a cancelled one's refusal is dropped.
fn settle(py: Python<'_>, future: &Py<PyAny>, value: PyResult<Py<PyAny>>) {
    let f = future.bind(py);
    let _ = match value {
        Ok(v) => f.call_method1(intern!(py, "set_result"), (v,)),
        Err(e) => f.call_method1(intern!(py, "set_exception"), (e.into_value(py),)),
    };
}

impl PyAsyncTransport {
    /// Submit one request and hand back the loop future it resolves, failures
    /// included.
    fn submit(slf: &Bound<'_, Self>, req: Request<'_>) -> PyResult<Py<PyAny>> {
        let py = slf.py();
        let mut this = slf.borrow_mut();
        if this.session.at_capacity() {
            this.drive(slf, Interest::BOTH)?;
        }
        let future = this.event_loop.call_method0(py, intern!(py, "create_future"))?;
        let slot = match this.session.submit(req) {
            Ok(slot) => slot,
            Err(e) => {
                settle(py, &future, Err(client_err(e)));
                return Ok(future);
            }
        };
        let idle = this.slots.is_empty();
        this.slots.push_back((slot, future.clone_ref(py)));
        if !this.flush_scheduled && !this.writer_armed {
            if idle {
                this.drive(slf, Interest::WRITE)?;
            } else {
                // The rest of a burst leaves in one `writev` next turn.
                this.event_loop.call_method1(
                    py,
                    intern!(py, "call_soon"),
                    (slf.getattr(intern!(py, "on_writable"))?,),
                )?;
                this.flush_scheduled = true;
            }
        }
        Ok(future)
    }

    fn drive(&mut self, slf: &Bound<'_, Self>, ready: Interest) -> PyResult<()> {
        let py = slf.py();
        for (slot, result) in self.session.step(ready) {
            self.resolve(py, slot, result);
        }
        if self.finished() {
            self.close(py);
            return Ok(());
        }
        let want = self.session.interest().write;
        self.arm(slf, want)
    }

    /// Nothing will ever again need the loop's callbacks.
    fn finished(&self) -> bool {
        self.session.is_closed() || (self.released && self.slots.is_empty())
    }

    fn resolve(&mut self, py: Python<'_>, slot: SlotId, result: Result<Reply, ClientError>) {
        let (want, future) = self.slots.pop_front().expect("a completion for an unregistered slot");
        assert_eq!(want, slot, "the spine completes slots in submit order");
        let value = match result {
            Ok(reply) => narrow(py, reply),
            Err(e) => Err(client_err(e)),
        };
        settle(py, &future, value);
    }

    /// Add or remove the loop's writable callback, idempotently.
    fn arm(&mut self, slf: &Bound<'_, Self>, on: bool) -> PyResult<()> {
        if on == self.writer_armed {
            return Ok(());
        }
        let py = slf.py();
        let fd = self.session.as_raw_fd();
        if on {
            self.event_loop.call_method1(
                py,
                intern!(py, "add_writer"),
                (fd, slf.getattr(intern!(py, "on_writable"))?),
            )?;
        } else {
            self.event_loop.call_method1(py, intern!(py, "remove_writer"), (fd,))?;
        }
        self.writer_armed = on;
        Ok(())
    }

    /// Take both callbacks off the loop; idempotent.
    fn deregister(&mut self, py: Python<'_>) {
        let event_loop = self.event_loop.bind(py);
        let fd = self.session.as_raw_fd();
        if std::mem::take(&mut self.writer_armed) {
            let _ = event_loop.call_method1(intern!(py, "remove_writer"), (fd,));
        }
        let _ = event_loop.call_method1(intern!(py, "remove_reader"), (fd,));
    }
}

#[pymethods]
impl PyAsyncTransport {
    /// Connect on the loop thread, GIL released, and register the reader on
    /// the running loop.
    #[staticmethod]
    fn connect<'py>(py: Python<'py>, target: &str) -> PyResult<Bound<'py, Self>> {
        let event_loop = py
            .import(intern!(py, "asyncio"))?
            .call_method0(intern!(py, "get_running_loop"))?
            .unbind();
        let session = py.detach(|| Session::connect(target)).map_err(client_err)?;
        let fd = session.as_raw_fd();
        let slf = Bound::new(
            py,
            PyAsyncTransport {
                session,
                event_loop: event_loop.clone_ref(py),
                slots: VecDeque::new(),
                flush_scheduled: false,
                writer_armed: false,
                released: false,
            },
        )?;
        event_loop.call_method1(
            py,
            intern!(py, "add_reader"),
            (fd, slf.getattr(intern!(py, "on_readable"))?),
        )?;
        Ok(slf)
    }

    fn push(slf: &Bound<'_, Self>, target_id: u64, batch: PyRef<'_, PyZSetBatch>) -> PyResult<Py<PyAny>> {
        Self::submit(
            slf,
            Request::Push {
                target: target_id.into(),
                schema: batch.schema.as_ref(),
                batch: &batch.batch,
                mode: WireConflictMode::Update,
            },
        )
    }

    fn scan(slf: &Bound<'_, Self>, target_id: u64, schema: PySchema) -> PyResult<Py<PyAny>> {
        let spec = ReadSpec::all_rows(ReadBound::None);
        Self::submit(
            slf,
            Request::ScanSpec {
                target: target_id.into(),
                spec: &spec,
                reply_schema: &schema.rust,
            },
        )
    }

    /// N `(table_id, schema)` relations at one server-side SAL cut, resolved as
    /// a list in request order.
    fn scan_many(slf: &Bound<'_, Self>, pairs: Vec<(u64, PySchema)>) -> PyResult<Py<PyAny>> {
        Self::submit(
            slf,
            Request::ScanMulti(pairs.into_iter().map(|(tid, s)| (tid, s.rust)).collect()),
        )
    }

    fn seek(slf: &Bound<'_, Self>, target_id: u64, schema: PySchema, pk: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let spec = pk_point_spec(&schema.rust, &pk)?;
        Self::submit(
            slf,
            Request::ScanSpec {
                target: target_id.into(),
                spec: &spec,
                reply_schema: &schema.rust,
            },
        )
    }

    fn on_readable(slf: &Bound<'_, Self>) -> PyResult<()> {
        slf.borrow_mut().drive(slf, Interest::BOTH)
    }

    /// The writer callback, and a burst's deferred flush.
    fn on_writable(slf: &Bound<'_, Self>) -> PyResult<()> {
        let mut this = slf.borrow_mut();
        this.flush_scheduled = false;
        this.drive(slf, Interest::WRITE)
    }

    /// Close the session, fail every outstanding operation, and leave the loop.
    fn close(&mut self, py: Python<'_>) {
        for (slot, result) in self.session.close() {
            self.resolve(py, slot, result);
        }
        self.deregister(py);
    }

    /// The Python handle is gone: close once nothing is in flight.
    fn release(&mut self, py: Python<'_>) {
        self.released = true;
        if self.finished() {
            self.close(py);
        }
    }
}
