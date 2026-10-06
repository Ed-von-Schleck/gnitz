//! The two ways a client's verb runs: [`Blocking`], on the calling thread with
//! the GIL released, and [`LoopCore`], queued for an asyncio event loop.

use std::collections::VecDeque;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::sync::mpsc;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Wake, Waker};

use pyo3::intern;
use pyo3::prelude::*;

use gnitz_core::{
    block_on, serve, BlockingHost, BoxFut, ClientError, GnitzClient, Host, Interest, Job, Op, Sent, Session,
    MAX_IN_FLIGHT,
};

use gnitz_sql::GnitzSqlError;

use crate::{client_err, gnitz_err, sql_err};

/// A verb's result, ready to become a Python object once the GIL is held.
type Landed = Box<dyn FnOnce(Python<'_>) -> PyResult<Py<PyAny>> + Send>;

fn landed<T, C>(reply: Result<T, GnitzSqlError>, convert: C) -> Landed
where
    T: Send + 'static,
    C: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
{
    Box::new(move |py| convert(py, reply.map_err(sql_err)?))
}

/// A lock that guards plain state: a panic under it leaves that state whole.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

// ---------------------------------------------------------------------------
// The blocking driver
// ---------------------------------------------------------------------------

/// A client driven on the calling thread.
pub(crate) struct Blocking {
    pub(crate) client: GnitzClient,
    /// The replies an open pipeline has yet to wait for, in call order.
    pub(crate) pipeline: Option<Vec<Py<PyPending>>>,
}

impl Blocking {
    /// A connection whose waits check for Ctrl-C.
    pub(crate) fn connect(py: Python<'_>, target: &str) -> PyResult<Self> {
        let host = BlockingHost::with_hook(Box::new(|| Python::attach(|py| py.check_signals()).map_err(Into::into)));
        let client = py
            .detach(|| block_on(GnitzClient::connect_with(target, Box::new(host))))
            .map_err(client_err)?;
        Ok(Blocking { client, pipeline: None })
    }

    /// Run `f` on the client with the GIL released.
    pub(crate) fn call<T: Send>(
        &mut self,
        py: Python<'_>,
        f: impl AsyncFnOnce(&mut GnitzClient) -> Result<T, ClientError> + Send,
    ) -> PyResult<T> {
        let client = &mut self.client;
        py.detach(|| block_on(f(client))).map_err(client_err)
    }

    pub(crate) fn run<T, O, C>(
        &mut self,
        slf: &Bound<'_, crate::client::PyClient>,
        op: O,
        convert: C,
    ) -> PyResult<Py<PyAny>>
    where
        T: Send + 'static,
        O: for<'a> FnOnce(&'a mut GnitzClient) -> BoxFut<'a, Result<Sent<T>, GnitzSqlError>> + Send + 'static,
        C: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    {
        let py = slf.py();
        let client = &mut self.client;
        let Some(owed) = self.pipeline.as_mut() else {
            let value = py.detach(|| {
                block_on(async {
                    let sent = op(client).await?;
                    Ok::<T, GnitzSqlError>(client.wait(sent).await?)
                })
            });
            return convert(py, value.map_err(sql_err)?);
        };
        let sent = py
            .detach(|| {
                block_on(async {
                    client.make_room().await?;
                    op(client).await
                })
            })
            .map_err(sql_err)?;
        let (mut sent, mut convert) = (sent, Some(convert));
        let arrived: Arrived = Box::new(move || {
            let reply = sent.try_take()?;
            let convert = convert.take().expect("a reply is handed out once");
            Some(landed(reply.map_err(Into::into), convert))
        });
        let state = Mutex::new(PendingState::Outstanding(arrived));
        let pending = Py::new(py, PyPending { client: slf.clone().unbind(), state })?;
        owed.push(pending.clone_ref(py));
        Ok(pending.into_any())
    }
}

/// A pipelined verb's reply, once the session has read it.
type Arrived = Box<dyn FnMut() -> Option<Landed> + Send>;

enum PendingState {
    Outstanding(Arrived),
    /// The reply, and whether `result()` has handed it to anyone.
    Landed(PyResult<Py<PyAny>>, bool),
}

/// The result of a verb called inside `client.pipeline()`: its request is on
/// its way, and `result()` waits for the reply.
#[pyclass(name = "Pending", frozen, generic)]
pub(crate) struct PyPending {
    client: Py<crate::client::PyClient>,
    state: Mutex<PendingState>,
}

impl PyPending {
    /// Wait for the reply, if it is still outstanding.
    fn land(&self, py: Python<'_>) -> PyResult<()> {
        loop {
            {
                let mut state = lock(&self.state);
                let landed = match &mut *state {
                    PendingState::Outstanding(arrived) => arrived(),
                    PendingState::Landed(..) => return Ok(()),
                };
                if let Some(landed) = landed {
                    *state = PendingState::Landed(landed(py), false);
                    return Ok(());
                }
            }
            let mut client = self.client.bind(py).try_borrow_mut()?;
            client.blocking()?.call(py, async |c| c.turn().await)?;
        }
    }

    /// An exception the reply raised that `result()` never handed out.
    fn unread_failure(&self, py: Python<'_>) -> Option<PyErr> {
        match &*lock(&self.state) {
            PendingState::Landed(Err(e), false) => Some(e.clone_ref(py)),
            _ => None,
        }
    }
}

#[pymethods]
impl PyPending {
    /// The verb's result, waiting for its reply if it has not arrived. Replies
    /// arrive in call order, so this also completes every call made before it.
    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.land(py)?;
        match &mut *lock(&self.state) {
            PendingState::Landed(outcome, read) => {
                *read = true;
                match outcome {
                    Ok(value) => Ok(value.clone_ref(py)),
                    Err(e) => Err(e.clone_ref(py)),
                }
            }
            PendingState::Outstanding(_) => unreachable!("landed above"),
        }
    }
}

/// The context manager `GnitzClient.pipeline()` returns.
#[pyclass(name = "Pipeline", frozen)]
pub(crate) struct PyPipeline {
    pub(crate) client: Py<crate::client::PyClient>,
}

#[pymethods]
impl PyPipeline {
    fn __enter__(&self, py: Python<'_>) -> PyResult<Py<crate::client::PyClient>> {
        let mut client = self.client.bind(py).try_borrow_mut()?;
        let blocking = client.blocking()?;
        if blocking.pipeline.is_some() {
            return Err(gnitz_err("a pipeline is already open on this client"));
        }
        blocking.pipeline = Some(Vec::new());
        Ok(self.client.clone_ref(py))
    }

    /// Wait for every reply still outstanding and raise the first failure
    /// `result()` handed to nobody — unless the block itself raised.
    fn __exit__(&self, py: Python<'_>, exc_type: Py<PyAny>, _exc_val: Py<PyAny>, _exc_tb: Py<PyAny>) -> PyResult<bool> {
        let owed = {
            let mut client = self.client.bind(py).try_borrow_mut()?;
            client.blocking()?.pipeline.take().unwrap_or_default()
        };
        if !exc_type.is_none(py) {
            return Ok(false);
        }
        for pending in &owed {
            pending.get().land(py)?;
        }
        match owed.iter().find_map(|p| p.get().unread_failure(py)) {
            Some(e) => Err(e),
            None => Ok(false),
        }
    }
}

// ---------------------------------------------------------------------------
// The event-loop driver
// ---------------------------------------------------------------------------

/// Resolve one loop future; a cancelled one's refusal is dropped.
fn settle(py: Python<'_>, future: &Py<PyAny>, value: PyResult<Py<PyAny>>) {
    let f = future.bind(py);
    let _ = match value {
        Ok(v) => f.call_method1(intern!(py, "set_result"), (v,)),
        Err(e) => f.call_method1(intern!(py, "set_exception"), (e.into_value(py),)),
    };
}

/// What the loop's callbacks, the host inside the `serve` loop and the class
/// that owns it all share.
struct Shared {
    event_loop: Py<PyAny>,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// Calls not yet started.
    queue: VecDeque<Op>,
    /// Results whose loop future the next pump resolves.
    landed: Vec<(Py<PyAny>, Landed)>,
    /// Calls made whose loop future is unresolved.
    owed: usize,
    /// No further call is taken, and the loop ends once the queue is empty.
    closing: bool,

    fd: RawFd,
    /// The loop reported the socket ready, and no step has used that up.
    readable: bool,
    writable: bool,
    /// Whether the loop is watching the socket for each direction.
    reader_on: bool,
    writer_on: bool,
    /// The core's bound methods; `None` before it exists and once it closed.
    on_readable: Option<Py<PyAny>>,
    on_writable: Option<Py<PyAny>>,
    on_flush: Option<Py<PyAny>>,
}

/// The loop future one call owes, resolved exactly once: with the call's
/// result, or as `Closed` if the call is dropped before it has one.
struct Owed {
    future: Option<Py<PyAny>>,
    shared: Arc<Shared>,
}

impl Owed {
    fn land(mut self, result: Landed) {
        let future = self.future.take().expect("landed once");
        self.shared.state().landed.push((future, result));
    }
}

impl Drop for Owed {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            let closed: Landed = Box::new(|_| Err(client_err(ClientError::Closed)));
            self.shared.state().landed.push((future, closed));
        }
    }
}

impl Shared {
    fn state(&self) -> MutexGuard<'_, State> {
        lock(&self.state)
    }

    /// Start or stop the loop's watch on the socket, idempotently.
    fn watch(&self, py: Python<'_>, write: bool, on: bool) {
        let (fd, callback) = {
            let mut st = self.state();
            let flag = match write {
                true => &mut st.writer_on,
                false => &mut st.reader_on,
            };
            if std::mem::replace(flag, on) == on {
                return;
            }
            let callback = match write {
                true => st.on_writable.as_ref(),
                false => st.on_readable.as_ref(),
            };
            (st.fd, callback.map(|c| c.clone_ref(py)))
        };
        let event_loop = self.event_loop.bind(py);
        // A loop that is closed has nothing to deregister from.
        let _ = match (write, on) {
            (true, true) => event_loop.call_method1(intern!(py, "add_writer"), (fd, callback)),
            (false, true) => event_loop.call_method1(intern!(py, "add_reader"), (fd, callback)),
            (true, false) => event_loop.call_method1(intern!(py, "remove_writer"), (fd,)),
            (false, false) => event_loop.call_method1(intern!(py, "remove_reader"), (fd,)),
        };
    }
}

/// Waits through the event loop's reader and writer callbacks, and runs jobs
/// on one thread of its own, started with the first job.
struct LoopHost {
    shared: Arc<Shared>,
    jobs: Option<mpsc::Sender<Job>>,
}

impl Host for LoopHost {
    fn attach(&mut self, fd: BorrowedFd<'_>) -> std::io::Result<()> {
        Python::attach(|py| {
            self.shared.watch(py, false, false);
            self.shared.watch(py, true, false);
        });
        let mut st = self.shared.state();
        st.fd = fd.as_raw_fd();
        (st.readable, st.writable) = (false, false);
        Ok(())
    }

    fn poll_io(
        &mut self,
        want: Interest,
        _cx: &mut Context<'_>,
        io: &mut dyn FnMut(Interest) -> Interest,
    ) -> Poll<Result<(), ClientError>> {
        let ready = {
            let mut st = self.shared.state();
            Interest {
                read: want.read && std::mem::take(&mut st.readable),
                write: want.write && (std::mem::take(&mut st.writable) || !st.writer_on),
            }
        };
        if ready.is_empty() {
            // The callbacks poll the loop that polled this; no waker is needed.
            if want.read {
                Python::attach(|py| self.shared.watch(py, false, true));
            }
            return Poll::Pending;
        }
        let left = io(ready);
        if ready.write {
            // The loop watches for writability only while the socket refuses bytes.
            Python::attach(|py| self.shared.watch(py, true, left.write));
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

impl Drop for LoopHost {
    fn drop(&mut self) {
        Python::attach(|py| {
            self.shared.watch(py, false, false);
            self.shared.watch(py, true, false);
        });
    }
}

/// Wakes the `serve` loop from the thread a job ran on.
impl Wake for Shared {
    fn wake(self: Arc<Self>) {
        // `move`: the last handle may be this one, and its `Py`s drop under the GIL.
        Python::attach(move |py| {
            let Some(pump) = self.state().on_flush.as_ref().map(|f| f.clone_ref(py)) else {
                return;
            };
            // A loop that is closed has nobody left to wake.
            let _ = self
                .event_loop
                .call_method1(py, intern!(py, "call_soon_threadsafe"), (pump,));
        });
    }
}

/// A client on an asyncio event loop: the `serve` loop that owns it, polled
/// from the loop's own callbacks.
#[pyclass]
pub(crate) struct LoopCore {
    /// The `serve` loop; `None` once the client is closed. The `Mutex` is a
    /// `Sync` shim for the future, never locked.
    actor: Mutex<Option<BoxFut<'static, ()>>>,
    shared: Arc<Shared>,
    waker: Waker,
}

/// What a client class holds of its [`LoopCore`].
pub(crate) struct LoopHandle {
    core: Py<LoopCore>,
    shared: Arc<Shared>,
}

impl LoopHandle {
    /// Connect on the calling thread, GIL released, to serve on the running loop.
    pub(crate) fn connect(py: Python<'_>, target: &str) -> PyResult<LoopHandle> {
        let event_loop = py
            .import(intern!(py, "asyncio"))?
            .call_method0(intern!(py, "get_running_loop"))?
            .unbind();
        let session = py.detach(|| Session::connect(target)).map_err(client_err)?;
        let shared = Arc::new(Shared {
            event_loop,
            state: Mutex::new(State::default()),
        });
        let host = LoopHost { shared: Arc::clone(&shared), jobs: None };
        let client = GnitzClient::over(session, Box::new(host)).map_err(client_err)?;
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
        let on_readable = callback(intern!(py, "on_readable"))?;
        let on_writable = callback(intern!(py, "on_writable"))?;
        let on_flush = callback(intern!(py, "on_flush"))?;
        {
            let mut st = shared.state();
            st.on_readable = Some(on_readable);
            st.on_writable = Some(on_writable);
            st.on_flush = Some(on_flush);
        }
        Ok(LoopHandle { core: core.unbind(), shared })
    }

    /// Queue `op` and hand back the loop future its result resolves.
    pub(crate) fn submit<T, O, C>(&self, py: Python<'_>, op: O, convert: C) -> PyResult<Py<PyAny>>
    where
        T: Send + 'static,
        O: for<'a> FnOnce(&'a mut GnitzClient) -> BoxFut<'a, Result<Sent<T>, GnitzSqlError>> + Send + 'static,
        C: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    {
        let future = self.shared.event_loop.call_method0(py, intern!(py, "create_future"))?;
        let mut core = self.core.bind(py).try_borrow_mut()?;
        // At the cap, start what is queued before refusing: the session takes
        // what it has room for.
        if self.shared.state().queue.len() >= MAX_IN_FLIGHT {
            core.pump(py);
        }
        let refused = {
            let st = self.shared.state();
            if st.closing {
                Some(client_err(ClientError::Closed))
            } else if st.queue.len() >= MAX_IN_FLIGHT {
                Some(gnitz_err(format!(
                    "connection has {MAX_IN_FLIGHT} calls queued; await some before making more"
                )))
            } else {
                None
            }
        };
        if let Some(why) = refused {
            settle(py, &future, Err(why));
            return Ok(future);
        }
        let (idle, flush) = {
            let st = self.shared.state();
            let idle = st.owed == 0;
            // A call queued behind none, while another is unresolved, is the
            // first of a burst: nothing is on its way to start it.
            let first = !idle && st.queue.is_empty();
            (
                idle,
                first.then(|| st.on_flush.as_ref().map(|f| f.clone_ref(py))).flatten(),
            )
        };
        if let Some(on_flush) = flush {
            let event_loop = self.shared.event_loop.bind(py);
            event_loop.call_method1(intern!(py, "call_soon"), (on_flush,))?;
        }
        let owed = Owed {
            future: Some(future.clone_ref(py)),
            shared: Arc::clone(&self.shared),
        };
        let call: Op = Box::new(move |client| {
            Box::pin(async move {
                match op(client).await {
                    Ok(sent) => sent.then(move |reply| owed.land(landed(reply.map_err(Into::into), convert))),
                    Err(fail) => owed.land(landed(Err(fail), convert)),
                }
            })
        });
        {
            let mut st = self.shared.state();
            st.queue.push_back(call);
            st.owed += 1;
        }
        // A call made while others are unresolved is one of a burst: it starts
        // on the loop's next turn, where the burst leaves together.
        if idle {
            core.pump(py);
        }
        Ok(future)
    }

    /// Queue `last` as the final call: none is taken after it, and the client
    /// closes once it is done. A client already closing resolves to `None`.
    pub(crate) fn finish<O>(&self, py: Python<'_>, last: O) -> PyResult<Py<PyAny>>
    where
        O: for<'a> FnOnce(&'a mut GnitzClient) -> BoxFut<'a, Result<Sent<()>, GnitzSqlError>> + Send + 'static,
    {
        if self.shared.state().closing {
            return self.settled(py, py.None());
        }
        let closed = self.submit(py, last, |py, ()| Ok(py.None()))?;
        self.shared.state().closing = true;
        self.core.bind(py).try_borrow_mut()?.pump(py);
        Ok(closed)
    }

    /// A loop future already resolved to `value`.
    pub(crate) fn settled(&self, py: Python<'_>, value: Py<PyAny>) -> PyResult<Py<PyAny>> {
        let future = self.shared.event_loop.call_method0(py, intern!(py, "create_future"))?;
        settle(py, &future, Ok(value));
        Ok(future)
    }

    /// The Python handle is gone: close once nothing is in flight.
    pub(crate) fn release(&self, py: Python<'_>) {
        self.shared.state().closing = true;
        // Borrowed means a pump is running, which reads the flag as it ends.
        if let Ok(mut core) = self.core.bind(py).try_borrow_mut() {
            core.pump(py);
        }
    }
}

impl LoopCore {
    /// Poll the `serve` loop once, which runs every queued call as far as it
    /// goes without waiting, and resolve what that completed.
    fn pump(&mut self, py: Python<'_>) {
        let actor = self.actor.get_mut().unwrap_or_else(PoisonError::into_inner);
        if let Some(serving) = actor.as_mut() {
            if serving.as_mut().poll(&mut Context::from_waker(&self.waker)).is_ready() {
                self.close();
            }
        }
        let (readable, writable) = {
            let st = self.shared.state();
            (st.readable, st.writable)
        };
        // Readiness nothing used: the `serve` loop is not stepping, and a
        // level-triggered loop would call back again at once.
        if readable {
            self.shared.watch(py, false, false);
        }
        if writable {
            self.shared.watch(py, true, false);
        }
        // Resolving runs Python, which can drop the handle: `closing` is read
        // after it, and closing lands what is left to resolve. A handle
        // released on a closed loop has no callback left to finish the
        // `serve` loop's drain, and with nothing owed dropping it loses nothing.
        loop {
            let resolved = self.resolve(py);
            let live = self.actor.get_mut().unwrap_or_else(PoisonError::into_inner).is_some();
            let abandoned = {
                let st = self.shared.state();
                live && st.closing && st.owed == 0
            };
            match (abandoned, resolved) {
                (true, _) => self.close(),
                (false, true) => {}
                (false, false) => break,
            }
        }
    }

    /// Resolve the loop future of every call that has its result; false once
    /// there was none.
    fn resolve(&mut self, py: Python<'_>) -> bool {
        let landed = {
            let mut st = self.shared.state();
            st.owed -= st.landed.len();
            std::mem::take(&mut st.landed)
        };
        let any = !landed.is_empty();
        for (future, result) in landed {
            settle(py, &future, result(py));
        }
        any
    }

    /// Drop the client, which closes its connection; every call still owed a
    /// result lands as `Closed`.
    fn close(&mut self) {
        *self.actor.get_mut().unwrap_or_else(PoisonError::into_inner) = None;
        let (unstarted, callbacks) = {
            let mut st = self.shared.state();
            st.closing = true;
            // The callbacks hold this object, which holds them.
            let callbacks = (st.on_readable.take(), st.on_writable.take(), st.on_flush.take());
            (std::mem::take(&mut st.queue), callbacks)
        };
        drop((unstarted, callbacks));
    }
}

#[pymethods]
impl LoopCore {
    fn on_readable(&mut self, py: Python<'_>) {
        self.shared.state().readable = true;
        self.pump(py);
    }

    fn on_writable(&mut self, py: Python<'_>) {
        self.shared.state().writable = true;
        self.pump(py);
    }

    /// A burst's deferred start, and a finished job's wake-up.
    fn on_flush(&mut self, py: Python<'_>) {
        self.pump(py);
    }
}
