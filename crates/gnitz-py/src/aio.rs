//! `AsyncGnitzClient`: a client on the running asyncio event loop, whose verbs
//! return a future of their result.
//!
//! The client lives inside a `serve` loop, polled from the event loop's own
//! callbacks: its reader and writer on the socket, and a flush the loop runs
//! on its next turn. A verb queues a call for `serve` to run and hands back
//! the loop future the call resolves.

use std::collections::VecDeque;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, TryLockError};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use pyo3::intern;
use pyo3::prelude::*;

use gnitz_core::{serve, BoxFut, ClientError, GnitzClient, Host, Interest, Job, Op, Sent};

use crate::client::{resolve_target, whole, Convert, Mode, PyClient, VerbOp};
use crate::reply::{none, synced};
use crate::{client_err, lock, sql_err};

/// Resolve one loop future; a cancelled one's refusal is dropped.
fn settle(py: Python<'_>, future: &Py<PyAny>, value: PyResult<Py<PyAny>>) {
    let f = future.bind(py);
    let _ = match value {
        Ok(v) => f.call_method1(intern!(py, "set_result"), (v,)),
        Err(e) => f.call_method1(intern!(py, "set_exception"), (e.into_value(py),)),
    };
}

/// What the loop's callbacks, the host inside the `serve` loop and the calls
/// it runs all share.
struct Shared {
    event_loop: Py<PyAny>,
    /// No Python runs while this is locked: a job's wake-up takes the GIL and
    /// then this lock, from a thread of its own.
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Calls not yet started.
    queue: VecDeque<Op>,
    /// Calls made whose loop future is unresolved.
    owed: usize,
    /// No further call is taken, and the client closes once nothing is owed.
    closing: bool,

    /// The client's socket, from its host's `attach` to that host's drop.
    fd: Option<OwnedFd>,
    /// What the loop reported the socket ready for, and no step has used up.
    ready: Interest,
    /// What the loop is watching the socket for.
    watched: Interest,
    /// `None` before the core exists and once it closed: the callbacks hold
    /// the core, which holds them.
    callbacks: Option<Callbacks>,
}

/// The core's bound methods the event loop calls.
struct Callbacks {
    readable: Py<PyAny>,
    writable: Py<PyAny>,
    flush: Py<PyAny>,
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    /// Have the loop watch the socket for what `to` makes of the state,
    /// starting and stopping only what changed.
    fn watch(&self, py: Python<'_>, to: impl FnOnce(&State) -> Interest) {
        let (fd, was, now, callbacks) = {
            let mut st = self.state();
            // With no socket attached, nothing is watched.
            let Some(fd) = st.fd.as_ref().map(AsRawFd::as_raw_fd) else {
                return;
            };
            let now = to(&st);
            let was = std::mem::replace(&mut st.watched, now);
            let callbacks = st
                .callbacks
                .as_ref()
                .map(|cb| (cb.readable.clone_ref(py), cb.writable.clone_ref(py)));
            (fd, was, now, callbacks)
        };
        let (on_read, on_write) = callbacks.unzip();
        let event_loop = self.event_loop.bind(py);
        let directions = [
            (
                was.read,
                now.read,
                intern!(py, "add_reader"),
                intern!(py, "remove_reader"),
                on_read,
            ),
            (
                was.write,
                now.write,
                intern!(py, "add_writer"),
                intern!(py, "remove_writer"),
                on_write,
            ),
        ];
        for (was, now, add, remove, callback) in directions {
            // A loop that is closed has nothing to deregister from.
            let _ = match (was, now) {
                (false, true) => event_loop.call_method1(add, (fd, callback)),
                (true, false) => event_loop.call_method1(remove, (fd,)),
                _ => continue,
            };
        }
    }

    /// The callback that pumps the core; `None` once it closed.
    fn flush(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.state().callbacks.as_ref().map(|cb| cb.flush.clone_ref(py))
    }

    /// Have the loop pump the core on its next turn.
    fn flush_soon(&self, py: Python<'_>) -> PyResult<()> {
        let Some(flush) = self.flush(py) else {
            return Ok(());
        };
        self.event_loop.call_method1(py, intern!(py, "call_soon"), (flush,))?;
        Ok(())
    }
}

/// Wakes the `serve` loop from the thread a job ran on.
impl Wake for Shared {
    fn wake(self: Arc<Self>) {
        // `move`: the last handle may be this one, and its `Py`s drop under the GIL.
        Python::attach(move |py| {
            let Some(flush) = self.flush(py) else {
                return;
            };
            // A loop that is closed has nobody left to wake.
            let _ = self
                .event_loop
                .call_method1(py, intern!(py, "call_soon_threadsafe"), (flush,));
        });
    }
}

/// The loop future one call owes, resolved exactly once: with the call's
/// result, or as `Closed` if the call is dropped before it has one.
///
/// It is resolved where the result arrives: inside the step of the `serve`
/// loop that reads the reply, or where the call is dropped — by whichever
/// pump holds the `serve` loop, GIL held. A `convert` therefore runs mid-step,
/// and a panic in one would take the `serve` loop down with it.
struct Owed {
    future: Option<Py<PyAny>>,
    shared: Arc<Shared>,
}

impl Owed {
    /// The call that runs `op` and resolves this with what `convert` makes of
    /// its answer.
    fn call<T, O, C>(self, op: O, convert: C) -> Op
    where
        T: Send + 'static,
        O: VerbOp<T>,
        C: Convert<T>,
    {
        Box::new(move |client| {
            Box::pin(async move {
                match op(client).await {
                    Ok(sent) => sent.then(move |reply| self.land(reply.map_err(client_err), convert)),
                    Err(fail) => self.land(Err(sql_err(fail)), convert),
                }
            })
        })
    }

    /// The caller cancelled the loop future: nobody takes what the call yields.
    fn abandoned(&self) -> bool {
        Python::attach(|py| {
            self.future.as_ref().is_some_and(|future| {
                let cancelled = future.bind(py).call_method0(intern!(py, "cancelled"));
                cancelled.and_then(|c| c.extract()).unwrap_or(false)
            })
        })
    }

    fn land<T>(mut self, reply: PyResult<T>, convert: impl FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>>) {
        let future = self.future.take().expect("landed once");
        Python::attach(move |py| settle(py, &future, reply.and_then(|t| convert(py, t))));
    }
}

/// An `Owed` dropped before it landed resolves as `Closed`; either way the
/// call is no longer owed.
impl Drop for Owed {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            Python::attach(move |py| settle(py, &future, Err(client_err(ClientError::Closed))));
        }
        self.shared.state().owed -= 1;
    }
}

/// Waits through the event loop's reader and writer callbacks, and runs jobs
/// on one thread of its own, started with the first job.
struct LoopHost {
    shared: Arc<Shared>,
    jobs: Option<mpsc::Sender<Job>>,
}

impl Host for LoopHost {
    fn attach(&mut self, fd: OwnedFd) -> std::io::Result<()> {
        Python::attach(|py| self.shared.watch(py, |_| Interest::NONE));
        let mut st = self.shared.state();
        st.fd = Some(fd);
        st.ready = Interest::NONE;
        Ok(())
    }

    fn poll_io(
        &mut self,
        want: Interest,
        _cx: &mut Context<'_>,
        io: &mut dyn FnMut(Interest) -> bool,
    ) -> Poll<Result<(), ClientError>> {
        let ready = {
            let mut st = self.shared.state();
            Interest {
                read: want.read && std::mem::take(&mut st.ready.read),
                write: want.write && std::mem::take(&mut st.ready.write),
            }
        };
        if ready.is_empty() {
            // The callbacks poll the loop that polled this; no waker is needed.
            Python::attach(|py| {
                self.shared.watch(py, |st| Interest {
                    read: st.watched.read || want.read,
                    write: st.watched.write || want.write,
                })
            });
            return Poll::Pending;
        }
        let refused = io(ready);
        if ready.write {
            // The loop watches for writability only while the socket refuses bytes.
            Python::attach(|py| {
                self.shared
                    .watch(py, |st| Interest { read: st.watched.read, write: refused })
            });
        }
        Poll::Ready(Ok(()))
    }

    fn spawn(&mut self, job: Job) {
        let jobs = self.jobs.get_or_insert_with(|| {
            let (tx, rx) = mpsc::channel::<Job>();
            std::thread::spawn(move || rx.into_iter().for_each(|job| job()));
            tx
        });
        // The thread ends only when this sender drops.
        let _ = jobs.send(job);
    }
}

/// Lets go of the socket, which the callbacks' share of the state outlives.
impl Drop for LoopHost {
    fn drop(&mut self) {
        Python::attach(|py| self.shared.watch(py, |_| Interest::NONE));
        self.shared.state().fd = None;
    }
}

/// The `serve` loop that owns the client; `None` once the client is closed.
type Actor = Option<BoxFut<'static, ()>>;

/// A client on an asyncio event loop: the `serve` loop that owns it, polled
/// from the loop's own callbacks.
#[pyclass(frozen, module = "gnitz._native")]
pub(crate) struct LoopCore {
    /// Locked by the pump that polls it, and by nothing else.
    actor: Mutex<Actor>,
    shared: Arc<Shared>,
    waker: Waker,
}

/// What a client class holds of its [`LoopCore`]: dropping it closes the
/// client once it owes nothing.
pub(crate) struct LoopHandle(Py<LoopCore>);

impl LoopHandle {
    pub(crate) fn get(&self) -> &LoopCore {
        self.0.get()
    }
}

impl Drop for LoopHandle {
    fn drop(&mut self) {
        Python::attach(|py| self.0.get().release(py));
    }
}

impl LoopCore {
    /// Connect on the calling thread, GIL released, to serve on the running loop.
    fn connect(py: Python<'_>, target: &str) -> PyResult<LoopHandle> {
        let event_loop = py
            .import(intern!(py, "asyncio"))?
            .call_method0(intern!(py, "get_running_loop"))?
            .unbind();
        let shared = Arc::new(Shared {
            event_loop,
            state: Mutex::new(State::default()),
        });
        let host = LoopHost { shared: Arc::clone(&shared), jobs: None };
        let client = py
            .detach(|| GnitzClient::connect_on(target, Box::new(host)))
            .map_err(client_err)?;
        let queue = Arc::clone(&shared);
        let next = move |_: &mut Context<'_>| {
            let mut st = queue.state();
            match st.queue.pop_front() {
                Some(op) => Poll::Ready(Some(op)),
                None if st.closing => Poll::Ready(None),
                None => Poll::Pending,
            }
        };
        let core = Bound::new(
            py,
            LoopCore {
                actor: Mutex::new(Some(Box::pin(serve(client, next)))),
                shared: Arc::clone(&shared),
                waker: Waker::from(Arc::clone(&shared)),
            },
        )?;
        // The callbacks are the core's own methods, so they exist only now.
        let callback = |name| core.getattr(name).map(Bound::unbind);
        let callbacks = Callbacks {
            readable: callback(intern!(py, "on_readable"))?,
            writable: callback(intern!(py, "on_writable"))?,
            flush: callback(intern!(py, "on_flush"))?,
        };
        shared.state().callbacks = Some(callbacks);
        Ok(LoopHandle(core.unbind()))
    }

    /// Queue the verb that runs `op` and converts its answer with `convert`,
    /// and hand back the loop future its result resolves.
    pub(crate) fn submit<T, O, C>(&self, py: Python<'_>, op: O, convert: C) -> PyResult<Py<PyAny>>
    where
        T: Send + 'static,
        O: VerbOp<T>,
        C: Convert<T>,
    {
        self.queue(py, |owed| owed.call(op, convert))
    }

    /// Queue a sync, which leaves the client free while the server holds it:
    /// its answer has no place among the replies. A loop future cancelled
    /// before the answer arrives never finishes the sync, so what it would
    /// take from the client stays there for the next sync to bring; one
    /// cancelled while the client finishes it hands its delivery to
    /// `GnitzClient::unclaimed`, for the same.
    pub(crate) fn sync(&self, py: Python<'_>, wait: Duration) -> PyResult<Py<PyAny>> {
        self.queue(py, |owed| {
            Box::new(move |client| {
                Box::pin(async move {
                    // The step that reads the answer queues the rest, and
                    // the `serve` loop takes it up before it waits again.
                    client.begin_sync(wait).then(move |reply| {
                        if owed.abandoned() {
                            return;
                        }
                        let shared = Arc::clone(&owed.shared);
                        let rest: Op = Box::new(move |client| {
                            Box::pin(async move {
                                match client.finish_sync(reply).await {
                                    Ok(unreceived) if owed.abandoned() => client.unclaimed(unreceived),
                                    done => owed.land(done.map_err(client_err), synced),
                                }
                            })
                        });
                        shared.state().queue.push_back(rest);
                    })
                })
            })
        })
    }

    /// Checkpoint and release the mirror store, then close: no call is taken
    /// after this one. A client already closing resolves to `None`.
    fn aclose(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.closing() {
            return self.settled(py, py.None());
        }
        let closed = self.submit(py, whole!(|c| c.close_mirror().await?), none)?;
        self.release(py);
        Ok(closed)
    }

    /// Whether the client was asked to close: it takes no call after that.
    pub(crate) fn closing(&self) -> bool {
        self.shared.state().closing
    }

    /// A loop future already resolved to `value`.
    pub(crate) fn settled(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let future = self.shared.event_loop.call_method0(py, intern!(py, "create_future"))?;
        settle(py, &future, Ok(value));
        Ok(future)
    }

    /// Close once nothing is owed.
    fn release(&self, py: Python<'_>) {
        self.shared.state().closing = true;
        self.pump(py);
    }

    /// Queue the call `build` makes of the loop future it owes, and hand that
    /// future back.
    fn queue(&self, py: Python<'_>, build: impl FnOnce(Owed) -> Op) -> PyResult<Py<PyAny>> {
        let future = self.shared.event_loop.call_method0(py, intern!(py, "create_future"))?;
        let (closing, idle, first) = {
            let st = self.shared.state();
            let idle = st.owed == 0;
            // A call queued behind none, while another is unresolved, is the
            // first of a burst: nothing is on its way to start it.
            (st.closing, idle, !idle && st.queue.is_empty())
        };
        if closing {
            settle(py, &future, Err(client_err(ClientError::Closed)));
            return Ok(future);
        }
        if first {
            self.shared.flush_soon(py)?;
        }
        let owed = Owed {
            future: Some(future.clone_ref(py)),
            shared: Arc::clone(&self.shared),
        };
        let call = build(owed);
        {
            let mut st = self.shared.state();
            st.queue.push_back(call);
            st.owed += 1;
        }
        // A call made while others are unresolved is one of a burst: it starts
        // on the loop's next turn, where the burst leaves together.
        if idle {
            self.pump(py);
        }
        Ok(future)
    }

    /// Poll the `serve` loop once, which runs every queued call as far as it
    /// goes without waiting and resolves what that completed.
    ///
    /// A pump that finds another running — Python the running one ran called
    /// back in — leaves the work to a pump on the loop's next turn: the
    /// running one may be past its poll, and would strand a call.
    fn pump(&self, py: Python<'_>) {
        let mut actor = match self.actor.try_lock() {
            Ok(actor) => actor,
            Err(TryLockError::Poisoned(actor)) => actor.into_inner(),
            Err(TryLockError::WouldBlock) => {
                // A loop that is closed has no turn left to pump on.
                let _ = self.shared.flush_soon(py);
                return;
            }
        };
        if let Some(serving) = actor.as_mut() {
            if serving.as_mut().poll(&mut Context::from_waker(&self.waker)).is_ready() {
                self.end(&mut actor);
            }
        }
        // Readiness nothing used: the `serve` loop is not stepping, and a
        // level-triggered loop would call back again at once.
        self.shared.watch(py, |st| Interest {
            read: st.watched.read && !st.ready.read,
            write: st.watched.write && !st.ready.write,
        });
        // Read last: Python run above can drop the handle. A handle released
        // on a closed loop has no callback left to finish the `serve` loop's
        // drain, and with nothing owed dropping it loses nothing.
        let abandoned = actor.is_some() && {
            let st = self.shared.state();
            st.closing && st.owed == 0
        };
        if abandoned {
            self.end(&mut actor);
        }
    }

    /// Drop the client, which closes its connection; every call still owed a
    /// result resolves as `Closed`.
    fn end(&self, actor: &mut Actor) {
        self.shared.state().closing = true;
        // Out of its slot before it drops: dropping it resolves what it owed,
        // which runs Python, and a call that was waiting on a reply queues the
        // rest of itself, which goes with the queue below.
        drop(actor.take());
        let (unstarted, callbacks) = {
            let mut st = self.shared.state();
            (std::mem::take(&mut st.queue), st.callbacks.take())
        };
        drop((unstarted, callbacks));
    }
}

#[pymethods]
impl LoopCore {
    fn on_readable(&self, py: Python<'_>) {
        self.shared.state().ready.read = true;
        self.pump(py);
    }

    fn on_writable(&self, py: Python<'_>) {
        self.shared.state().ready.write = true;
        self.pump(py);
    }

    /// A burst's deferred start, a finished job's wake-up, and a pump that
    /// found another running.
    fn on_flush(&self, py: Python<'_>) {
        self.pump(py);
    }
}

// ---------------------------------------------------------------------------
// The class
// ---------------------------------------------------------------------------

/// A connection on the running asyncio event loop: every verb submits when
/// called and returns a future of its result. It is connected when built, so
/// awaiting it yields itself, and `async with` closes it on exit.
#[pyclass(name = "AsyncGnitzClient", extends = PyClient, frozen, module = "gnitz")]
pub struct PyAsyncClient;

#[pymethods]
impl PyAsyncClient {
    /// Connect on the calling thread, and serve on the running loop.
    #[new]
    #[pyo3(signature = (target = None, schema = "public"))]
    fn new(py: Python<'_>, target: Option<String>, schema: &str) -> PyResult<PyClassInitializer<Self>> {
        let handle = LoopCore::connect(py, &resolve_target(target)?)?;
        Ok(PyClient::init(Mode::Loop(handle), schema).add_subclass(PyAsyncClient))
    }

    /// Checkpoint and release the mirror store behind every call already
    /// made, then close the connection, whether or not that checkpoint failed.
    /// Awaits to `None`; idempotent.
    fn aclose(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        slf.as_super().get().on_loop()?.aclose(slf.py())
    }

    fn __aenter__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let core = slf.as_super().get().on_loop()?;
        core.settled(slf.py(), slf.clone().into_any().unbind())
    }

    /// `await connect(p)`, beside `async with connect(p)`.
    fn __await__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        Self::__aenter__(slf)?.call_method0(slf.py(), intern!(slf.py(), "__await__"))
    }

    /// `aclose()`, whose `None` lets the block's exception through.
    fn __aexit__(
        slf: &Bound<'_, Self>,
        _exc_type: Py<PyAny>,
        _exc_val: Py<PyAny>,
        _exc_tb: Py<PyAny>,
    ) -> PyResult<Py<PyAny>> {
        Self::aclose(slf)
    }
}
