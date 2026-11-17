//! The Rust async client: `gnitz-core`'s client on tokio's reactor.
//!
//! [`connect`] is a `gnitz-core` client that waits on this runtime, for the task
//! that owns it; [`share`] hands one to a driver, and tasks share the handle.

use std::os::fd::OwnedFd;
use std::task::{Context, Poll};
use std::time::Duration;

use gnitz_core::{
    BoxFut, ClientError, GnitzClient, Host, Interest, Job, Op, Pending, Sent, Synced, promise, serve, sync_op,
};
use tokio::io::unix::{AsyncFd, AsyncFdReadyGuard};
use tokio::sync::mpsc;

/// Waits on tokio's reactor, and runs jobs on its blocking pool.
#[derive(Default)]
struct TokioHost {
    /// The client's socket, registered until the next `attach` or this drop.
    fd: Option<AsyncFd<OwnedFd>>,
}

fn fired(
    polled: Poll<std::io::Result<AsyncFdReadyGuard<'_, OwnedFd>>>,
) -> std::io::Result<Option<AsyncFdReadyGuard<'_, OwnedFd>>> {
    match polled {
        Poll::Ready(r) => r.map(Some),
        Poll::Pending => Ok(None),
    }
}

impl Host for TokioHost {
    fn attach(&mut self, fd: OwnedFd) -> std::io::Result<()> {
        // Registered before the old registration drops, so a socket the
        // reactor refuses leaves this host on the old one.
        // SAFETY: the registration owns `fd`, which is open and stays the
        // same descriptor until the registration drops.
        self.fd = Some(unsafe { AsyncFd::register(fd) }?);
        Ok(())
    }

    /// Polls only the directions `want` names; one that did not fire registers
    /// its waker. Stepping a direction that did not fire would cost a syscall
    /// per wakeup to discover it was not ready.
    fn poll_io(
        &mut self,
        want: Interest,
        cx: &mut Context<'_>,
        io: &mut dyn FnMut(Interest) -> bool,
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
        let refused = io(ready);
        // `AsyncFd` reports edges. The client reads the socket dry before it
        // asks again; write readiness lasts until the socket refuses bytes.
        if let Some(mut guard) = read {
            guard.clear_ready();
        }
        if refused && let Some(mut guard) = write {
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
///
/// A panic in the connect resumes here; a connect the shutting-down runtime
/// dropped is `Closed`.
pub async fn connect(target: &str) -> Result<GnitzClient, ClientError> {
    GnitzClient::connect_with(target, Box::new(TokioHost::default())).await
}

/// Share `client` among tasks: the handle, paired with the driver that serves
/// it. Nothing reaches the connection until the [`Connection`] is polled.
///
/// The driver waits the way `client`'s host does, so `client` is one
/// [`connect`] made: a blocking host would park a runtime thread.
pub fn share(client: GnitzClient) -> (AsyncClient, Connection) {
    let (tx, mut rx) = mpsc::unbounded_channel::<Op>();
    let driver = serve(client, move |cx| rx.poll_recv(cx));
    (AsyncClient { tx }, Box::pin(driver))
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
    /// client.run(|c| Box::pin(c.create_schema("s", false))).await??;
    /// ```
    pub fn run<T, F>(&self, f: F) -> Sent<T>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut GnitzClient) -> BoxFut<'a, T> + Send + 'static,
    {
        let (mut to, sent) = promise();
        // A driver that is gone drops the op, and with it `to`.
        let _ = self.tx.send(Box::new(move |client| {
            Box::pin(async move { to.fulfil(Ok(f(client).await)) })
        }));
        sent
    }

    /// Submit the one request `f` makes, queued when called, and resolve to
    /// its reply. The client is free once the request is submitted, so
    /// requests sent together share a round trip.
    ///
    /// ```ignore
    /// let lsn = client.send(move |c| c.push(tid, &schema, batch, mode)).await?;
    /// ```
    pub fn send<T, F>(&self, f: F) -> Sent<T>
    where
        T: Send + 'static,
        F: for<'a> FnOnce(&'a mut GnitzClient) -> Pending<'a, T> + Send + 'static,
    {
        let (mut to, sent) = promise();
        let _ = self.tx.send(Box::new(move |client| {
            Box::pin(async move { f(client).detach().then(move |reply| to.fulfil(reply)) })
        }));
        sent
    }

    /// [`GnitzClient::sync`], leaving the client to the handle's other calls
    /// while the server holds the reply.
    ///
    /// Cancel-safe: a sync dropped at any point is still finished when its
    /// answer arrives, and what it brought a reader's subscriptions stays with
    /// the client for the next sync, with the reseeds of its
    /// [`Delivery`](gnitz_core::Delivery). An outstanding sync keeps the
    /// [`Connection`] serving until its answer arrives.
    pub async fn sync(&self, wait: Duration) -> Result<Synced, ClientError> {
        let (mut to, sent) = promise();
        let tx = self.tx.clone();
        let requeue = move |rest| {
            let _ = tx.send(rest);
        };
        let _ = self
            .tx
            .send(sync_op(wait, requeue, move |delivery| to.fulfil(delivery)));
        Ok(sent.await?.take())
    }
}

/// Owns the client and serves the calls its [`AsyncClient`]s make: poll it to
/// completion. It completes once every handle is dropped and nothing is
/// outstanding; a lost connection fails the calls and does not end it.
pub type Connection = BoxFut<'static, ()>;
