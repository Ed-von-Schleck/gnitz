//! How a client waits: the [`Host`] contract a client's futures are driven
//! through, the blocking host and [`block_on`], the [`serve`] loop an event
//! loop runs a client under, and the one-shot reply cell — a [`Sent`] and its
//! [`Promise`] — every wait ends on.

use std::future::{Future, poll_fn};
use std::io::Read;
use std::os::fd::{AsFd, OwnedFd};
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::client::GnitzClient;
use crate::error::ClientError;
use crate::transport::{Interest, poll_fd_or_cancel};

/// What [`BlockingHost::with_hook`] runs.
type ParkHook = Box<dyn FnMut() -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send>;

/// Work a [`Host`] runs where blocking is allowed.
pub type Job = Box<dyn FnOnce() + Send>;

/// How a client waits: for its socket, and for work that blocks.
pub trait Host: Send {
    /// The client's socket is `fd` from here on, until the next `attach`.
    fn attach(&mut self, fd: OwnedFd) -> std::io::Result<()>;

    /// Once the socket is ready for any of `want`, step the client on what is
    /// ready by calling `io`. Readiness is spent by
    /// the call: before it asks again, the client reads on by itself until the
    /// socket has no more, and writes by itself until the socket refuses bytes.
    fn poll_io(
        &mut self,
        want: Interest,
        cx: &mut Context<'_>,
        io: &mut dyn FnMut(Interest),
    ) -> Poll<Result<(), ClientError>>;

    /// Run `job` where it may block. A host that cannot run it drops it.
    fn spawn(&mut self, job: Job);

    /// Whether the client may step without waiting on the socket: on the rest
    /// of a read the last step left unfinished, or on a write the socket has
    /// not refused. A host whose scheduler budgets its tasks pends here once
    /// the task's budget is spent, and wakes `cx` to go on.
    fn poll_proceed(&mut self, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}

/// Waits in `poll(2)` and runs jobs in place, so a future over it never pends:
/// [`block_on`] drives one to completion.
#[derive(Default)]
pub struct BlockingHost {
    fd: Option<OwnedFd>,
    hook: Option<ParkHook>,
    cancel: Option<UnixStream>,
}

impl BlockingHost {
    /// A host that runs `hook` whenever a signal interrupts its wait; an `Err`
    /// aborts the call with [`ClientError::Interrupted`] and leaves the session
    /// as it stood.
    pub fn with_hook(
        hook: impl FnMut() -> Result<(), Box<dyn std::error::Error + Send + Sync>> + Send + 'static,
    ) -> Self {
        BlockingHost {
            hook: Some(Box::new(hook)),
            ..Default::default()
        }
    }

    /// A host whose wait gives up once `cancel` has bytes to read, and reads
    /// them all: its `poll(2)` waits on `cancel` too, so a byte written at any
    /// moment of the wait ends it. Whatever writes there must keep its end
    /// open, as a closed one would cancel every wait.
    pub fn cancelled_by(cancel: UnixStream) -> std::io::Result<Self> {
        cancel.set_nonblocking(true)?;
        Ok(BlockingHost {
            cancel: Some(cancel),
            ..Default::default()
        })
    }
}

/// Read what `cancel` holds, so it cancels no later wait.
pub fn drain_cancel(mut cancel: &UnixStream) {
    let mut buf = [0u8; 64];
    while matches!(cancel.read(&mut buf), Ok(n) if n > 0) {}
}

impl Host for BlockingHost {
    fn attach(&mut self, fd: OwnedFd) -> std::io::Result<()> {
        self.fd = Some(fd);
        Ok(())
    }

    fn poll_io(
        &mut self,
        want: Interest,
        _cx: &mut Context<'_>,
        io: &mut dyn FnMut(Interest),
    ) -> Poll<Result<(), ClientError>> {
        let fd = self
            .fd
            .as_ref()
            .expect("a client attaches its host before it waits")
            .as_fd();
        let cancel = self.cancel.as_ref();
        loop {
            match poll_fd_or_cancel(fd, want, cancel.map(AsFd::as_fd), None) {
                Ok(Some(ready)) => {
                    io(ready);
                    return Poll::Ready(Ok(()));
                }
                Ok(None) => {
                    drain_cancel(cancel.expect("only a cancel socket cancels"));
                    let why: Box<dyn std::error::Error + Send + Sync> = "cancelled".into();
                    return Poll::Ready(Err(ClientError::Interrupted(why.into())));
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {
                    if let Some(hook) = self.hook.as_mut()
                        && let Err(e) = hook()
                    {
                        return Poll::Ready(Err(ClientError::Interrupted(e.into())));
                    }
                }
                Err(e) => return Poll::Ready(Err(e.into())),
            }
        }
    }

    fn spawn(&mut self, job: Job) {
        job()
    }
}

/// Drive a future of a client whose host never pends — a [`BlockingHost`]'s.
pub fn block_on<F: Future>(fut: F) -> F::Output {
    let mut fut = std::pin::pin!(fut);
    match fut.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(out) => out,
        Poll::Pending => panic!("block_on drove a client whose host pends; that client belongs to an event loop"),
    }
}

/// Run `job` on `host`, where it may block, and wait for what it returns. A
/// panic in it resumes here; a job the host dropped is `Closed`.
pub(crate) async fn offload<R: Send + 'static>(
    host: &mut dyn Host,
    job: impl FnOnce() -> R + Send + 'static,
) -> Result<R, ClientError> {
    let (mut ended, job_end) = promise();
    host.spawn(Box::new(move || {
        ended.fulfil(Ok(std::panic::catch_unwind(std::panic::AssertUnwindSafe(job))))
    }));
    job_end.await?.map_err(|panic| std::panic::resume_unwind(panic))
}

/// A verb that is one request: awaited, it is that round trip;
/// [detached](Self::detach), the request is on its way and the client is free
/// for the next, which is what lets several share a round trip.
#[must_use = "a verb does nothing until awaited or detached"]
pub struct Pending<'a, T> {
    pub(crate) client: &'a mut GnitzClient,
    pub(crate) sent: Sent<T>,
}

impl<T> Pending<'_, T> {
    /// Let go of the client, keeping the reply.
    pub fn detach(self) -> Sent<T> {
        self.sent
    }
}

impl<T> Future for Pending<'_, T> {
    type Output = Result<T, ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let sent = &mut this.sent;
        this.client
            .session
            .poll_until(&mut *this.client.host, cx, |_| sent.try_take())
            .map(Result::flatten)
    }
}

/// One queued call of a [`serve`] loop: it has the client to itself until its
/// future completes.
pub type Op = Box<dyn for<'a> FnOnce(&'a mut GnitzClient) -> BoxFut<'a, ()> + Send>;

/// A boxed future a host can move between threads.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The future of an op that did all its work in its closure. Zero-sized, so
/// boxing it allocates nothing.
pub fn finished<'a>() -> BoxFut<'a, ()> {
    Box::pin(poll_fn(|_| Poll::Ready(())))
}

/// Run the ops `next` yields on `client`, in order and one at a time, after
/// any a reply queued on the client. Steps the session while none is ready,
/// and once `next` yields `None` until nothing is outstanding; then drops the
/// client.
pub async fn serve(mut client: GnitzClient, mut next: impl FnMut(&mut Context<'_>) -> Poll<Option<Op>>) {
    loop {
        let op = poll_fn(|cx| {
            loop {
                // Queued only from inside a step or an op, both run below, so
                // nothing has to wake this.
                if let Some(op) = client
                    .resumed
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .pop_front()
                {
                    return Poll::Ready(Some(op));
                }
                let drained = match next(cx) {
                    Poll::Ready(Some(op)) => return Poll::Ready(Some(op)),
                    Poll::Ready(None) => true,
                    Poll::Pending => false,
                };
                match client.session.turn(&mut *client.host, cx) {
                    Poll::Ready(Ok(())) => {}
                    // Nothing outstanding and nothing buffered.
                    Poll::Ready(Err(ClientError::Closed)) => {
                        return match drained {
                            true => Poll::Ready(None),
                            false => Poll::Pending,
                        };
                    }
                    // No call is waiting to hand an interrupt to.
                    Poll::Ready(Err(e)) => client.session.end(e),
                    Poll::Pending => return Poll::Pending,
                }
            }
        })
        .await;
        let Some(op) = op else { return };
        // A connection that is gone refuses the op's own requests, as what
        // ended it.
        if let Err(e) = client.make_room().await {
            client.session.end(e);
        }
        op(&mut client).await;
    }
}

#[cfg(test)]
#[path = "tests/host.rs"]
mod tests;

/// Where a reply is, between its writer and its reader.
#[derive(Default)]
enum Awaited<T> {
    #[default]
    Outstanding,
    Arrived(Result<T, ClientError>),
    /// Handed on as it arrives.
    Routed(Box<dyn FnOnce(Result<T, ClientError>) + Send>),
    /// Awaited by a task its arrival wakes.
    Watched(Waker),
}

struct Cell<T>(Mutex<Awaited<T>>);

impl<T> Cell<T> {
    fn new(awaited: Awaited<T>) -> Arc<Self> {
        Arc::new(Cell(Mutex::new(awaited)))
    }

    /// A panic under this lock leaves what it guards whole.
    fn lock(&self) -> MutexGuard<'_, Awaited<T>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A reply on its way, borrowing nothing. One to a request arrives as the
/// session is stepped: [`GnitzClient::wait`](crate::GnitzClient::wait) steps
/// until it has, and awaiting it directly leaves the stepping to someone else.
pub struct Sent<T>(Arc<Cell<T>>);

/// The writer of a [`Sent`]. Dropped unfulfilled, the reply is `Closed`.
pub struct Promise<T>(Option<Arc<Cell<T>>>);

/// A reply yet to arrive, and its writer.
pub fn promise<T>() -> (Promise<T>, Sent<T>) {
    let cell = Cell::new(Awaited::Outstanding);
    (Promise(Some(Arc::clone(&cell))), Sent(cell))
}

impl<T> Promise<T> {
    /// The reply arrives; a second one goes nowhere.
    pub fn fulfil(&mut self, reply: Result<T, ClientError>) {
        let Some(cell) = self.0.take() else {
            return;
        };
        let mut slot = cell.lock();
        match std::mem::take(&mut *slot) {
            Awaited::Routed(route) => {
                drop(slot);
                route(reply)
            }
            Awaited::Watched(waiter) => {
                *slot = Awaited::Arrived(reply);
                drop(slot);
                waiter.wake()
            }
            Awaited::Outstanding => *slot = Awaited::Arrived(reply),
            Awaited::Arrived(_) => unreachable!("a cell has one writer, which fulfils once"),
        }
    }
}

impl<T> Drop for Promise<T> {
    fn drop(&mut self) {
        self.fulfil(Err(ClientError::Closed));
    }
}

impl<T> Sent<T> {
    /// A reply that needed no request.
    pub fn ready(value: Result<T, ClientError>) -> Self {
        Sent(Cell::new(Awaited::Arrived(value)))
    }

    /// The reply, if it has arrived. It is handed out once.
    pub fn try_take(&mut self) -> Option<Result<T, ClientError>> {
        let mut slot = self.0.lock();
        match std::mem::take(&mut *slot) {
            Awaited::Arrived(reply) => Some(reply),
            other => {
                *slot = other;
                None
            }
        }
    }

    /// Hand the reply to `route` — now if it has arrived, else from wherever
    /// it does: for a request's, inside the step that reads it.
    pub fn then(self, route: impl FnOnce(Result<T, ClientError>) + Send + 'static) {
        let mut slot = self.0.lock();
        match std::mem::take(&mut *slot) {
            Awaited::Arrived(reply) => {
                drop(slot);
                route(reply)
            }
            _ => *slot = Awaited::Routed(Box::new(route)),
        }
    }

    /// The reply `f` makes of this one, arriving with it.
    pub fn map<U: Send + 'static>(self, f: impl FnOnce(T) -> U + Send + 'static) -> Sent<U>
    where
        T: 'static,
    {
        let (mut to, mapped) = promise();
        self.then(move |reply| to.fulfil(reply.map(f)));
        mapped
    }
}

impl<T> Future for Sent<T> {
    type Output = Result<T, ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut slot = self.0.lock();
        match std::mem::take(&mut *slot) {
            Awaited::Arrived(reply) => Poll::Ready(reply),
            _ => {
                *slot = Awaited::Watched(cx.waker().clone());
                Poll::Pending
            }
        }
    }
}
