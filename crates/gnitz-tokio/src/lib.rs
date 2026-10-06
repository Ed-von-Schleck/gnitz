//! The Rust async client: `gnitz-core`'s client on tokio's reactor.
//!
//! [`client`] is a `gnitz-core` client that waits on this runtime, for the task
//! that owns it; [`connect`] is the same client shared among tasks.

use std::future::Future;
use std::os::fd::{AsRawFd, BorrowedFd, RawFd};
use std::pin::Pin;
use std::task::{Context, Poll};

use gnitz_core::{serve, ClientError, GnitzClient, Host, Interest, Job, Op, Pending};
use tokio::io::unix::{AsyncFd, AsyncFdReadyGuard};
use tokio::sync::{mpsc, oneshot};

/// Waits on tokio's reactor, and runs jobs on its blocking pool.
#[derive(Default)]
struct TokioHost {
    /// The client's socket, registered until the next `attach` or this drop.
    fd: Option<AsyncFd<RawFd>>,
}

fn fired(
    polled: Poll<std::io::Result<AsyncFdReadyGuard<'_, RawFd>>>,
) -> std::io::Result<Option<AsyncFdReadyGuard<'_, RawFd>>> {
    match polled {
        Poll::Ready(r) => r.map(Some),
        Poll::Pending => Ok(None),
    }
}

impl Host for TokioHost {
    fn attach(&mut self, fd: BorrowedFd<'_>) -> std::io::Result<()> {
        // The old socket is still open, so its number is still its own, and
        // its registration goes only once the new one is made.
        self.fd = Some(AsyncFd::new(fd.as_raw_fd())?);
        Ok(())
    }

    /// Polls only the directions `want` names; one that did not fire registers
    /// its waker. Stepping a direction that did not fire would cost a syscall
    /// per wakeup to discover it was not ready.
    fn poll_io(
        &mut self,
        want: Interest,
        cx: &mut Context<'_>,
        io: &mut dyn FnMut(Interest) -> Interest,
    ) -> Poll<Result<(), ClientError>> {
        let fd = self.fd.as_ref().expect("a client attaches its host before waiting");
        let read = match want.read {
            true => fired(fd.poll_read_ready(cx))?,
            false => None,
        };
        let write = match want.write {
            true => fired(fd.poll_write_ready(cx))?,
            false => None,
        };
        let ready = Interest {
            read: read.is_some(),
            write: write.is_some(),
        };
        if ready.is_empty() {
            return Poll::Pending;
        }
        let left = io(ready);
        // `AsyncFd` reports edges: a read ran the socket dry, and a write did
        // only if bytes are left.
        if let Some(mut guard) = read {
            guard.clear_ready();
        }
        if let (Some(mut guard), true) = (write, left.write) {
            guard.clear_ready();
        }
        Poll::Ready(Ok(()))
    }

    /// A runtime shutting down drops the job.
    fn spawn(&mut self, job: Job) {
        drop(tokio::task::spawn_blocking(job));
    }
}

/// Connect to `target`, on the blocking pool, and hand back a client whose
/// verbs wait on this runtime.
pub async fn client(target: &str) -> Result<GnitzClient, ClientError> {
    GnitzClient::connect_with(target, Box::new(TokioHost::default())).await
}

/// Connect to `target` and hand back the handle paired with the driver that
/// serves it. Nothing reaches the connection until the [`Connection`] is
/// polled.
pub async fn connect(target: &str) -> Result<(AsyncClient, Connection), ClientError> {
    let client = client(target).await?;
    let (tx, mut rx) = mpsc::unbounded_channel::<Op>();
    let driver = async move { drop(serve(client, |cx| rx.poll_recv(cx)).await) };
    Ok((AsyncClient { tx }, Connection(Box::pin(driver))))
}

/// The handle to a [`Connection`], shared by cloning. Its calls run on the one
/// client behind it in the order they were made, one at a time.
#[derive(Clone)]
pub struct AsyncClient {
    tx: mpsc::UnboundedSender<Op>,
}

// The handle is shared by cloning, so this is what it promises.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<AsyncClient>();
};

impl AsyncClient {
    /// Run `f` with the client to itself, queued when called, and resolve to
    /// what it returns — [`ClientError::Closed`] if the driver is gone.
    ///
    /// ```ignore
    /// client.run(|c| Box::pin(c.create_schema("s"))).await??;
    /// ```
    pub fn run<T, F>(&self, f: F) -> impl Future<Output = Result<T, ClientError>>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut GnitzClient) -> Pin<Box<dyn Future<Output = T> + Send + 'a>> + Send + 'static,
    {
        let (reply, rx) = oneshot::channel();
        // A driver that is gone drops the op, and with it `reply`.
        let _ = self.tx.send(Box::new(move |client| {
            Box::pin(async move {
                // A dropped caller's result goes nowhere.
                let _ = reply.send(f(client).await);
            })
        }));
        async move { rx.await.map_err(|_| ClientError::Closed) }
    }

    /// Submit the one request `f` makes, queued when called, and resolve to
    /// its reply. The client is free once the request is submitted, so
    /// requests sent together share a round trip.
    ///
    /// ```ignore
    /// let lsn = client.send(move |c| c.push(tid, &schema, batch, mode)).await?;
    /// ```
    pub fn send<T, F>(&self, f: F) -> impl Future<Output = Result<T, ClientError>>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut GnitzClient) -> Pending<'a, T> + Send + 'static,
    {
        let (reply, rx) = oneshot::channel();
        let _ = self.tx.send(Box::new(move |client| {
            Box::pin(async move {
                f(client).detach().then(move |r| drop(reply.send(r)));
            })
        }));
        async move { rx.await.map_err(|_| ClientError::Closed)? }
    }
}

/// Owns the client and serves the calls its [`AsyncClient`]s make: poll it to
/// completion. It completes once every handle is dropped and nothing is
/// outstanding; a lost connection fails the calls and does not end it.
pub struct Connection(Pin<Box<dyn Future<Output = ()> + Send>>);

impl Future for Connection {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.0.as_mut().poll(cx)
    }
}
