//! The Rust async client: a [`Connection`] future over tokio's reactor, and
//! the [`AsyncClient`] handle that feeds it.
//!
//! `gnitz-core`'s `Session` is the spine and owns all the protocol, down to
//! resolving each reply against the request that asked for it. This crate owns
//! how it waits.
//!
//! The wire verbs run on the driver. Every other `GnitzClient` verb, mirroring
//! included, runs on a [`BlockingClient`], which is a connection of its own.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use gnitz_core::{
    qualified_name, ClientError, Encoded, GnitzClient, Interest, RelDescriptor, Reply, Request, ScanReply, Schema,
    Session, SlotId, ZSetBatch,
};
use gnitz_wire::{ReadSpec, WireConflictMode};
use tokio::io::unix::{AsyncFd, AsyncFdReadyGuard};
use tokio::sync::{mpsc, oneshot};

/// One request as it crosses the channel.
struct Submission {
    request: Encoded,
    reply: oneshot::Sender<Result<Reply, ClientError>>,
}

/// The handle to a [`Connection`]: `Send + Sync + Clone`. Every method takes
/// `&self`, so sharing one is a clone.
///
/// Calling a wire verb encodes and submits it, so verbs reach the server in
/// call order; the future it returns only waits for the reply. A request waits
/// in the channel, encoded, while the connection is at one of
/// [`Session::enqueue`]'s caps.
#[derive(Clone)]
pub struct AsyncClient {
    tx: mpsc::UnboundedSender<Submission>,
}

/// A [`GnitzClient`] for async callers, shared by cloning: `Send + Sync +
/// Clone`. Its connection is its own, so it neither waits for a [`Connection`]
/// nor ends with one.
#[derive(Clone)]
pub struct BlockingClient(Arc<tokio::sync::Mutex<GnitzClient>>);

// Both handles are shared by cloning, so this is what they promise.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<AsyncClient>();
    assert_send_sync::<BlockingClient>();
};

/// Run `f` on a blocking pool thread. A panic in it resumes on the caller.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T, ClientError> {
    match tokio::task::spawn_blocking(f).await {
        Ok(v) => Ok(v),
        Err(e) => match e.try_into_panic() {
            Ok(payload) => std::panic::resume_unwind(payload),
            // A blocking task is cancelled only by runtime shutdown.
            Err(_) => Err(ClientError::Closed),
        },
    }
}

/// Connect to `target` and hand back the handle paired with the driver that
/// serves it. Nothing reaches the connection until the [`Connection`] is
/// polled.
///
/// The TCP connect, TLS handshake and HELLO block under one connect deadline,
/// after name resolution, so they run on `spawn_blocking`.
pub async fn connect(target: &str) -> Result<(AsyncClient, Connection), ClientError> {
    let target = target.to_owned();
    let session = blocking(move || Session::connect(&target)).await??;
    // A `dup`, so the reactor deregisters a descriptor whose life it owns
    // rather than a number the session may already have closed and the kernel
    // handed out again.
    let fd = AsyncFd::new(session.try_clone_fd()?)?;
    let (tx, rx) = mpsc::unbounded_channel();
    Ok((
        AsyncClient { tx },
        Connection {
            session,
            fd,
            rx,
            pending: VecDeque::new(),
        },
    ))
}

impl AsyncClient {
    /// Hand one request to the driver; the future is its reply.
    fn call<T>(&self, req: Request<'_>, narrow: fn(Reply) -> T) -> impl Future<Output = Result<T, ClientError>> {
        let (reply, rx) = oneshot::channel();
        match req.encode() {
            // A driver that is gone drops the submission, and with it `reply`.
            Ok(request) => {
                let _ = self.tx.send(Submission { request, reply });
            }
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
        async move { rx.await.map_err(|_| ClientError::Closed)?.map(narrow) }
    }

    /// Push a batch and resolve to its ingest LSN.
    pub fn push(&self, tid: u64, schema: &Schema, batch: &ZSetBatch) -> impl Future<Output = Result<u64, ClientError>> {
        let mode = WireConflictMode::Update;
        self.call(Request::Push { target_id: tid, schema, batch, mode }, Reply::into_lsn)
    }

    /// Read `tid` under `spec`, replied in `reply_schema`'s layout.
    pub fn scan_spec(
        &self,
        tid: u64,
        spec: &ReadSpec,
        reply_schema: &Arc<Schema>,
    ) -> impl Future<Output = Result<ScanReply, ClientError>> {
        self.call(
            Request::ScanSpec { target_id: tid, spec, reply_schema },
            Reply::into_scan,
        )
    }

    /// Snapshot N relations at one server-side SAL cut, in request order, each
    /// replied in the layout of the schema paired with it.
    pub fn scan_many(
        &self,
        relations: Vec<(u64, Arc<Schema>)>,
    ) -> impl Future<Output = Result<Vec<ScanReply>, ClientError>> {
        self.call(Request::ScanMulti(relations), Reply::into_multi)
    }

    /// Describe one relation. Always a round trip. On the surface because every
    /// other verb takes a `tid` and nothing else here can produce one.
    pub fn resolve(
        &self,
        schema_name: &str,
        name: &str,
    ) -> impl Future<Output = Result<Option<Arc<RelDescriptor>>, ClientError>> {
        let qname = qualified_name(schema_name, name);
        self.call(Request::Resolve(&qname), Reply::into_resolve)
    }
}

impl BlockingClient {
    /// Connect to `target`, on a blocking thread.
    pub async fn connect(target: &str) -> Result<Self, ClientError> {
        let target = target.to_owned();
        let client = blocking(move || GnitzClient::connect(&target)).await??;
        Ok(BlockingClient(Arc::new(tokio::sync::Mutex::new(client))))
    }

    /// Run `f` against the client on a blocking thread. Callers serialize for
    /// the whole of `f`, across every clone.
    pub async fn run<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut GnitzClient) -> Result<T, ClientError> + Send + 'static,
    ) -> Result<T, ClientError> {
        let mut client = Arc::clone(&self.0).lock_owned().await;
        blocking(move || f(&mut client)).await?
    }

    /// [`AsyncClient::scan_spec`] on `driver`, answered off this client's copy
    /// instead when it holds `tid`. It waits for this client first, so it
    /// submits when polled, not when called.
    pub async fn scan_spec_local_first(
        &self,
        driver: &AsyncClient,
        tid: u64,
        spec: ReadSpec,
        reply_schema: Arc<Schema>,
    ) -> Result<ScanReply, ClientError> {
        let mut client = Arc::clone(&self.0).lock_owned().await;
        if !client.mirrors(tid) {
            drop(client);
            return driver.scan_spec(tid, &spec, &reply_schema).await;
        }
        blocking(move || client.scan_spec_local_first(tid, spec, &reply_schema)).await?
    }
}

/// Owns the connection, its `AsyncFd` and the request channel; drains, steps,
/// resolves. Poll it to completion — as a spawned task, or joined beside the
/// work that feeds it. It completes once every [`AsyncClient`] is dropped
/// **and** the connection has quiesced.
///
/// A lost connection does not end it: every verb, outstanding or later,
/// resolves [`ClientError::ConnectionLost`].
///
/// Dropping a verb's future is not cancellation: the frame is written and the
/// server commits it; the driver just drops a result nobody is left to receive.
pub struct Connection {
    session: Session,
    fd: AsyncFd<OwnedFd>,
    rx: mpsc::UnboundedReceiver<Submission>,
    /// Registered slots in submit order. A queue, not a map: the spine holds one
    /// accumulator — the head slot's — and registers a slot only after every
    /// fallible step has passed, so completion is strictly FIFO.
    pending: VecDeque<(SlotId, oneshot::Sender<Result<Reply, ClientError>>)>,
}

impl Connection {
    /// Enqueue every submission the channel holds; `true` when the last handle is
    /// gone. At either of `enqueue`'s caps the channel is left unpolled, so a
    /// verb past a cap waits there instead of taking the error `enqueue` raises.
    /// Either cap implies an interest bit, so the next step brings the loop back
    /// round.
    fn drain_channel(&mut self, cx: &mut Context<'_>) -> bool {
        while !self.session.at_capacity() {
            match self.rx.poll_recv(cx) {
                Poll::Ready(Some(sub)) => self.submit(sub),
                Poll::Ready(None) => return true,
                Poll::Pending => break,
            }
        }
        false
    }

    /// Enqueue one submission and register its slot. A request the spine
    /// refuses fails that one future and reaches no wire.
    fn submit(&mut self, Submission { request, reply }: Submission) {
        match self.session.enqueue(request) {
            Ok(id) => self.pending.push_back((id, reply)),
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }
}

/// The read and write readiness guards of one poll.
type Guards<'a> = (
    Option<AsyncFdReadyGuard<'a, OwnedFd>>,
    Option<AsyncFdReadyGuard<'a, OwnedFd>>,
);

/// Poll only the directions `want` names; one that did not fire registers its
/// waker and stays `None`. Stepping a direction that did not fire would cost a
/// syscall per wakeup to discover it was not ready. Free rather than a `&self`
/// method, so the guards borrow `fd` alone and `step` can take `&mut session`.
fn poll_ready<'a>(fd: &'a AsyncFd<OwnedFd>, want: Interest, cx: &mut Context<'_>) -> io::Result<Guards<'a>> {
    Ok((
        if want.read {
            fired(fd.poll_read_ready(cx))?
        } else {
            None
        },
        if want.write {
            fired(fd.poll_write_ready(cx))?
        } else {
            None
        },
    ))
}

fn fired(
    polled: Poll<io::Result<AsyncFdReadyGuard<'_, OwnedFd>>>,
) -> io::Result<Option<AsyncFdReadyGuard<'_, OwnedFd>>> {
    match polled {
        Poll::Ready(r) => r.map(Some),
        Poll::Pending => Ok(None),
    }
}

impl Future for Connection {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            let handles_gone = this.drain_channel(cx);

            let want = this.session.interest();
            if want.is_empty() {
                // Quiesced: either no handle is left and this is done, or the
                // drain above registered the channel's waker.
                return if handles_gone { Poll::Ready(()) } else { Poll::Pending };
            }

            let done = match poll_ready(&this.fd, want, cx) {
                // Readiness fails only once the runtime is shutting down.
                Err(_) => this.session.close(),
                Ok((read, write)) => {
                    let ready = Interest {
                        read: read.is_some(),
                        write: write.is_some(),
                    };
                    if ready.is_empty() {
                        return Poll::Pending;
                    }
                    let done = this.session.step(ready);
                    // `AsyncFd` is edge-triggered, so clear only what the step
                    // spent: every read, and a write that left bytes queued.
                    let write_spent = this.session.interest().write;
                    if let Some(mut g) = read {
                        g.clear_ready();
                    }
                    if let (Some(mut g), true) = (write, write_spent) {
                        g.clear_ready();
                    }
                    done
                }
            };

            for (id, result) in done {
                let (want, reply) = this.pending.pop_front().expect("a completion for an unregistered slot");
                assert_eq!(want, id, "the spine completes slots in submit order");
                // A dropped verb future's result goes nowhere.
                let _ = reply.send(result);
            }
        }
    }
}
