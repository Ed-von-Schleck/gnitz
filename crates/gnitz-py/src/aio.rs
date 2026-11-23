//! `AsyncGnitzClient`: a client on the running asyncio event loop, whose verbs
//! return a future of their result — `sync`, a coroutine over one.
//!
//! The client lives inside a `serve` loop, polled from the event loop's own
//! callbacks: its reader and writer on the socket, a flush the loop runs on
//! its next turn, and a reader on a wake-up socket a finished job writes. Its
//! connect runs on the [`pool`](crate::pool) first, and nothing of it blocks
//! the loop; a pool thread wakes the loop without taking the GIL. A verb queues a call
//! for `serve` to run and hands back the loop future the call resolves.

use std::collections::VecDeque;
use std::io::Write;
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, TryLockError};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use pyo3::IntoPyObjectExt;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;

use gnitz_core::{BoxFut, ClientError, Delivery, GnitzClient, Host, Interest, Job, Op, serve};

use crate::client::{Mode, PyClient, VerbOp, none, resolve_target, whole};
use crate::reply::PySynced;
use crate::{client_err, sql_err};
use gnitz_foundation::sync::lock;

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
    /// No Python runs while this is locked.
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Calls not yet started.
    queue: VecDeque<Op>,
    /// Calls made whose loop future is unresolved.
    owed: usize,
    /// Once set, no further call is taken, the client closes once nothing is
    /// owed, and this is what every call it will never run is answered with:
    /// `Closed`, or why the connect failed.
    closed: Option<ClientError>,

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
    /// Have the loop watch the socket for what `to` makes of the state,
    /// starting and stopping only what changed.
    fn watch(&self, py: Python<'_>, to: impl FnOnce(&State) -> Interest) {
        let (fd, was, now, callbacks) = {
            let mut st = lock(&self.state);
            // With no socket attached, nothing is watched.
            let Some(fd) = st.fd.as_ref().map(AsRawFd::as_raw_fd) else {
                return;
            };
            let now = to(&st);
            let was = std::mem::replace(&mut st.watched, now);
            if was == now {
                return;
            }
            let callbacks = (st.callbacks.as_ref()).map(|cb| (cb.readable.clone_ref(py), cb.writable.clone_ref(py)));
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

    /// Have the loop pump the core on its next turn; nothing once it closed.
    /// A burst's start writes no wake-up socket: `call_soon` costs no syscall.
    fn flush_soon(&self, py: Python<'_>) -> PyResult<()> {
        let Some(flush) = lock(&self.state).callbacks.as_ref().map(|cb| cb.flush.clone_ref(py)) else {
            return Ok(());
        };
        self.event_loop.call_method1(py, intern!(py, "call_soon"), (flush,))?;
        Ok(())
    }
}

/// Wakes the `serve` loop from whatever thread finished a job, GIL or not:
/// a byte written to `tx` makes `rx` readable, which the loop watches.
struct Wakeup {
    tx: UnixStream,
    rx: UnixStream,
}

impl Wake for Wakeup {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref()
    }

    fn wake_by_ref(self: &Arc<Self>) {
        // Refused only while the socket is full of wakes the loop has yet to
        // take.
        let _ = (&self.tx).write(&[1]);
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
    fn land<T, R>(mut self, reply: PyResult<T>, convert: impl FnOnce(Python<'_>, T) -> PyResult<R>)
    where
        R: for<'py> IntoPyObject<'py>,
    {
        let future = self.future.take().expect("landed once");
        Python::attach(move |py| settle(py, &future, reply.and_then(|t| convert(py, t)?.into_py_any(py))));
    }
}

/// An `Owed` dropped before it landed resolves as `Closed`, or as the
/// connect's failure; either way the call is no longer owed.
impl Drop for Owed {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            let why = lock(&self.shared.state).closed.clone().unwrap_or(ClientError::Closed);
            Python::attach(move |py| settle(py, &future, Err(client_err(why))));
        }
        lock(&self.shared.state).owed -= 1;
    }
}

/// A sync's [`Delivery`], from the step that finished the sync to the task
/// that awaits it. Dropped untaken, the delivery hands its reseeds back to the
/// client.
#[pyclass(name = "_SyncDelivery", frozen, module = "gnitz._native")]
struct SyncDelivery(Mutex<Option<Delivery>>);

#[pymethods]
impl SyncDelivery {
    /// The sync's result, taken once. Its trains decode with the GIL released.
    fn take(&self, py: Python<'_>) -> PyResult<PySynced> {
        match lock(&self.0).take() {
            Some(delivery) => PySynced::new(py, py.detach(|| delivery.take())),
            None => Err(client_err(ClientError::Closed)),
        }
    }

    /// Drop the delivery untaken, so the next sync reports its reseeds again.
    fn release(&self) {
        drop(lock(&self.0).take());
    }
}

/// Waits through the event loop's reader and writer callbacks, and runs jobs
/// on the [`pool`](crate::pool).
struct LoopHost {
    shared: Arc<Shared>,
}

impl Host for LoopHost {
    fn attach(&mut self, fd: OwnedFd) -> std::io::Result<()> {
        Python::attach(|py| self.shared.watch(py, |_| Interest::NONE));
        let mut st = lock(&self.shared.state);
        st.fd = Some(fd);
        st.ready = Interest::NONE;
        Ok(())
    }

    fn poll_io(&mut self, want: Interest, _cx: &mut Context<'_>) -> Poll<Result<Interest, ClientError>> {
        let ready = {
            let mut st = lock(&self.shared.state);
            Interest {
                read: want.read && std::mem::take(&mut st.ready.read),
                write: want.write && std::mem::take(&mut st.ready.write),
            }
        };
        if ready.is_empty() {
            // The callbacks poll the loop that polled this; no waker is needed.
            // The loop watches for writability only while the socket refuses
            // bytes, the one time a client waits to write.
            Python::attach(|py| {
                self.shared.watch(py, |st| Interest {
                    read: st.watched.read || want.read,
                    write: want.write,
                })
            });
            return Poll::Pending;
        }
        Poll::Ready(Ok(ready))
    }

    fn spawn(&mut self, job: Job) {
        crate::pool::spawn(job)
    }
}

/// Lets go of the socket, which the callbacks' share of the state outlives.
impl Drop for LoopHost {
    fn drop(&mut self) {
        Python::attach(|py| self.shared.watch(py, |_| Interest::NONE));
        lock(&self.shared.state).fd = None;
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
    /// Read by the loop from construction until the core ends.
    wakeup: Arc<Wakeup>,
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
    /// A client of `target` on the running loop, its connect started: the
    /// calls made meanwhile run once it is up, and are answered with its
    /// failure if it fails.
    fn connect(py: Python<'_>, target: String) -> PyResult<LoopHandle> {
        let event_loop = py
            .import(intern!(py, "asyncio"))?
            .call_method0(intern!(py, "get_running_loop"))?
            .unbind();
        let shared = Arc::new(Shared {
            event_loop,
            state: Mutex::new(State::default()),
        });
        let host = LoopHost { shared: Arc::clone(&shared) };
        let queue = Arc::clone(&shared);
        let actor = async move {
            match GnitzClient::connect_with(&target, Box::new(host)).await {
                Ok(client) => {
                    let next = move |_: &mut Context<'_>| {
                        let mut st = lock(&queue.state);
                        match st.queue.pop_front() {
                            Some(op) => Poll::Ready(Some(op)),
                            None if st.closed.is_some() => Poll::Ready(None),
                            None => Poll::Pending,
                        }
                    };
                    serve(client, next).await
                }
                Err(why) => lock(&queue.state).closed = Some(why),
            }
        };
        let (tx, rx) = UnixStream::pair()?;
        tx.set_nonblocking(true)?;
        rx.set_nonblocking(true)?;
        let wakeup = Arc::new(Wakeup { tx, rx });
        let core = Bound::new(
            py,
            LoopCore {
                actor: Mutex::new(Some(Box::pin(actor))),
                shared: Arc::clone(&shared),
                waker: Waker::from(Arc::clone(&wakeup)),
                wakeup,
            },
        )?;
        // The callbacks are the core's own methods, so they exist only now.
        let callback = |name| core.getattr(name).map(Bound::unbind);
        let callbacks = Callbacks {
            readable: callback(intern!(py, "on_readable"))?,
            writable: callback(intern!(py, "on_writable"))?,
            flush: callback(intern!(py, "on_flush"))?,
        };
        lock(&shared.state).callbacks = Some(callbacks);
        shared.event_loop.call_method1(
            py,
            intern!(py, "add_reader"),
            (core.get().wakeup.rx.as_raw_fd(), callback(intern!(py, "on_wakeup"))?),
        )?;
        // Hands the connect to the pool.
        core.get().pump(py);
        Ok(LoopHandle(core.unbind()))
    }

    /// Queue the verb that runs `op` and converts its answer with `convert`,
    /// and hand back the loop future its result resolves.
    pub(crate) fn submit<T, R, O, C>(&self, py: Python<'_>, op: O, convert: C) -> PyResult<Py<PyAny>>
    where
        T: Send + 'static,
        R: for<'py> IntoPyObject<'py>,
        O: VerbOp<T>,
        C: FnOnce(Python<'_>, T) -> PyResult<R> + Send + 'static,
    {
        self.queue(py, |owed| {
            Op::call(move |client| {
                Box::pin(async move {
                    match op(client).await {
                        Ok(sent) => sent.then(move |reply| owed.land(reply.map_err(client_err), convert)),
                        Err(fail) => owed.land(Err(sql_err(fail)), convert),
                    }
                })
            })
        })
    }

    /// Queue a sync, which leaves the client free while the server holds it:
    /// its answer has no place among the replies. Every answered sync is
    /// finished, and its `Delivery` goes to the loop future, held by a
    /// `_SyncDelivery`. What this
    /// returns is `gnitz.aio._claimed` over that future: the awaiting task takes
    /// the delivery when it resumes. What the sync brought a reader stays
    /// with the client until a delivery is taken; a delivery never taken (its
    /// future cancelled, or its task cancelled after the answer arrived) hands
    /// its reseeds back, and the next sync reports them.
    pub(crate) fn sync(&self, py: Python<'_>, wait: Duration) -> PyResult<Py<PyAny>> {
        let delivery = self.queue(py, |owed| {
            Op::sync(wait, move |synced| {
                owed.land(synced.map_err(client_err), |_, d| Ok(SyncDelivery(Mutex::new(Some(d)))))
            })
        })?;
        static CLAIMED: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
        let claimed = CLAIMED.import(py, "gnitz.aio", "_claimed")?;
        Ok(claimed.call1((delivery,))?.unbind())
    }

    /// Whether the client was asked to close: it takes no call after that.
    pub(crate) fn closing(&self) -> bool {
        lock(&self.shared.state).closed.is_some()
    }

    /// A loop future already resolved to `value`.
    pub(crate) fn settled(&self, py: Python<'_>, value: impl for<'py> IntoPyObject<'py>) -> PyResult<Py<PyAny>> {
        let future = self.shared.event_loop.call_method0(py, intern!(py, "create_future"))?;
        settle(py, &future, value.into_py_any(py));
        Ok(future)
    }

    /// Close once nothing is owed: the pump is what lets the `serve` loop see
    /// the close when no other callback would.
    fn release(&self, py: Python<'_>) {
        self.close();
        self.pump(py);
    }

    /// Queue the call `build` makes of the loop future it owes, and hand that
    /// future back.
    fn queue(&self, py: Python<'_>, build: impl FnOnce(Owed) -> Op) -> PyResult<Py<PyAny>> {
        let future = self.shared.event_loop.call_method0(py, intern!(py, "create_future"))?;
        let (closed, idle, first) = {
            let st = lock(&self.shared.state);
            let idle = st.owed == 0;
            // A call queued behind none, while another is unresolved, is the
            // first of a burst: nothing is on its way to start it.
            (st.closed.clone(), idle, !idle && st.queue.is_empty())
        };
        if let Some(why) = closed {
            settle(py, &future, Err(client_err(why)));
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
            let mut st = lock(&self.shared.state);
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
            // A panic in the `serve` loop ends it: polled again it would only
            // panic again, and the calls it owes would never resolve.
            let cx = &mut Context::from_waker(&self.waker);
            match std::panic::catch_unwind(AssertUnwindSafe(|| serving.as_mut().poll(cx))) {
                Ok(Poll::Pending) => {}
                Ok(Poll::Ready(())) => self.end(py, &mut actor),
                Err(panic) => {
                    self.end(py, &mut actor);
                    std::panic::resume_unwind(panic);
                }
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
            let st = lock(&self.shared.state);
            st.closed.is_some() && st.owed == 0
        };
        if abandoned {
            self.end(py, &mut actor);
        }
    }

    /// Drop the client, which closes its connection; every call still owed a
    /// result resolves as `Closed`, or as the connect's failure.
    fn end(&self, py: Python<'_>, actor: &mut Actor) {
        self.close();
        // Out of its slot before it drops: dropping it resolves what it owed,
        // which runs Python, and a call that was waiting on a reply queues the
        // rest of itself, which goes with the queue below.
        drop(actor.take());
        let (unstarted, callbacks) = {
            let mut st = lock(&self.shared.state);
            (std::mem::take(&mut st.queue), st.callbacks.take())
        };
        drop((unstarted, callbacks));
        // While the core still holds the wake-up socket, so no other
        // descriptor is ever read in its place. A loop that is closed has
        // nothing to deregister from.
        let remove = intern!(py, "remove_reader");
        let _ = (self.shared.event_loop).call_method1(py, remove, (self.wakeup.rx.as_raw_fd(),));
    }

    /// Take no further call; one the client will never run is `Closed`,
    /// unless the connect failed.
    fn close(&self) {
        lock(&self.shared.state).closed.get_or_insert(ClientError::Closed);
    }
}

#[pymethods]
impl LoopCore {
    fn on_readable(&self, py: Python<'_>) {
        lock(&self.shared.state).ready.read = true;
        self.pump(py);
    }

    fn on_writable(&self, py: Python<'_>) {
        lock(&self.shared.state).ready.write = true;
        self.pump(py);
    }

    /// A burst's deferred start, and a pump that found another running.
    fn on_flush(&self, py: Python<'_>) {
        self.pump(py);
    }

    /// A finished job's wake-up, the connect's among them.
    fn on_wakeup(&self, py: Python<'_>) {
        // Every wake so far, so the loop calls back again only on the next.
        gnitz_foundation::posix_io::drain(&self.wakeup.rx);
        self.pump(py);
    }
}

// ---------------------------------------------------------------------------
// The class
// ---------------------------------------------------------------------------

/// A connection on the running asyncio event loop: every verb submits when
/// called and returns a future of its result. Building it starts its connect
/// and blocks nothing; awaiting it, or entering it with `async with`, yields
/// it once connected and raises if the connect failed, and `async with`
/// closes it on exit.
#[pyclass(name = "AsyncGnitzClient", extends = PyClient, frozen, module = "gnitz")]
pub struct PyAsyncClient;

#[pymethods]
impl PyAsyncClient {
    /// Start connecting, to serve on the running loop.
    #[new]
    #[pyo3(signature = (target = None, schema = "public"))]
    fn new(py: Python<'_>, target: Option<String>, schema: &str) -> PyResult<PyClassInitializer<Self>> {
        let handle = LoopCore::connect(py, resolve_target(target)?)?;
        Ok(PyClient::init(Mode::Loop(handle), schema).add_subclass(PyAsyncClient))
    }

    /// Checkpoint and release the mirror store behind every call already
    /// made, then close the connection, whether or not that checkpoint failed.
    /// Awaits to `None`; idempotent.
    fn aclose(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let (core, py) = (slf.as_super().get().on_loop()?, slf.py());
        if core.closing() {
            return core.settled(py, py.None());
        }
        let closed = core.submit(py, whole!(|c| c.close_mirror().await?), none)?;
        core.release(py);
        Ok(closed)
    }

    /// This client, once every call made before is answered — the connect
    /// first among them; its failure raises.
    fn __aenter__(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        let core = slf.as_super().get().on_loop()?;
        let me = slf.clone().into_any().unbind();
        core.submit(slf.py(), whole!(|_c| ()), move |_, ()| Ok(me))
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
