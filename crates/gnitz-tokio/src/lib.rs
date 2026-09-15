//! The Rust async client: a [`Connection`] future over tokio's reactor, and
//! the [`AsyncClient`] handle that feeds it.
//!
//! `gnitz-core`'s `Session` is the spine and owns all the protocol, down to
//! resolving each reply against the request that asked for it. This crate owns
//! how it waits.
//!
//! The wire verbs run on the driver; every other `GnitzClient` verb, mirroring
//! included, runs through [`AsyncClient::with_blocking_client`].

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use gnitz_core::{
    qualified_name, ClientError, GnitzClient, Interest, RelDescriptor, RelTarget, Reply, Request, ScanReply, Schema,
    Session, SlotId, WireConflictMode, ZSetBatch,
};
use tokio::io::unix::{AsyncFd, AsyncFdReadyGuard};
use tokio::sync::{mpsc, oneshot};

/// Depth of the request channel. `MAX_IN_FLIGHT` is the real in-flight bound;
/// this only keeps a burst from round-tripping through the scheduler.
const REQUEST_CHANNEL_DEPTH: usize = 256;

/// One request's encode step, deferred because `Request<'_>` borrows and the
/// borrow ends at `submit`, while the session lives in the driver task. Named
/// because `clippy::type_complexity` refuses it inline.
type Encode = Box<dyn FnOnce(&mut Session) -> Result<SlotId, ClientError> + Send>;

/// One request as it crosses the channel.
struct Submission {
    encode: Encode,
    reply: oneshot::Sender<Result<Reply, ClientError>>,
}

/// A channel sender, plus the blocking client every clone shares:
/// `Send + Sync + Clone`. Every method takes `&self`, so sharing one
/// is a clone.
#[derive(Clone)]
pub struct AsyncClient {
    tx: mpsc::Sender<Submission>,
    /// What the blocking client connects to.
    target: Arc<str>,
    /// The blocking client, `None` until the first
    /// [`AsyncClient::with_blocking_client`].
    client: Arc<tokio::sync::Mutex<Option<GnitzClient>>>,
}

// The handle is shared by cloning, so this is what it promises; the client field
// is the one thing that could take it away silently.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<AsyncClient>();
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
    let target: Arc<str> = Arc::from(target);
    let connect_to = Arc::clone(&target);
    let (session, _published_lsn) = blocking(move || Session::connect(&connect_to)).await??;
    // A `dup`, so the reactor deregisters a descriptor whose life it owns
    // rather than a number the session may already have closed and the kernel
    // handed out again.
    let fd = AsyncFd::new(session.try_clone_fd()?)?;
    let (tx, rx) = mpsc::channel(REQUEST_CHANNEL_DEPTH);
    Ok((
        AsyncClient {
            tx,
            target,
            client: Arc::new(tokio::sync::Mutex::new(None)),
        },
        Connection {
            session,
            fd,
            rx,
            pending: VecDeque::new(),
        },
    ))
}

impl AsyncClient {
    /// Hand one request's encode step to the driver and wait for its reply.
    async fn call(
        &self,
        encode: impl FnOnce(&mut Session) -> Result<SlotId, ClientError> + Send + 'static,
    ) -> Result<Reply, ClientError> {
        let (reply, rx) = oneshot::channel();
        // A full channel suspends here: `Connection` drains only below
        // `Session::at_capacity`, so back-pressure is a wait, never its error.
        self.tx
            .send(Submission { encode: Box::new(encode), reply })
            .await
            .map_err(|_| ClientError::Closed)?;
        rx.await.map_err(|_| ClientError::Closed)?
    }

    /// Push a batch and resolve to its ingest LSN.
    ///
    /// A [`ClientError::SchemaMismatch`] is not retried here as the blocking
    /// client retries it: a re-submit would reorder the push behind everything
    /// sent since, and the spine already evicted the stale cache entry.
    pub async fn push(&self, tid: u64, schema: Arc<Schema>, batch: ZSetBatch) -> Result<u64, ClientError> {
        self.call(move |s| {
            s.submit(Request::Push {
                target_id: tid,
                schema: &schema,
                batch: &batch,
                mode: WireConflictMode::Update,
            })
        })
        .await
        .map(|r| r.into_lsn())
    }

    pub async fn scan(&self, tid: u64) -> Result<ScanReply, ClientError> {
        self.call(move |s| s.submit(Request::scan(tid)))
            .await
            .map(|r| r.into_scan())
    }

    /// A point SEEK by primary key: `key` is the packed native-LE PK columns.
    pub async fn seek(&self, tid: u64, key: &[u8]) -> Result<ScanReply, ClientError> {
        let key = key.to_vec();
        self.call(move |s| s.submit(Request::seek(tid, &key)))
            .await
            .map(|r| r.into_scan())
    }

    /// Snapshot N relations at one server-side SAL cut, in request order.
    pub async fn scan_many(&self, tids: &[u64]) -> Result<Vec<ScanReply>, ClientError> {
        let tids = tids.to_vec();
        self.call(move |s| s.submit(Request::ScanMulti(&tids)))
            .await
            .map(|r| r.into_multi())
    }

    /// Describe one relation. Always a round trip — an async handle has no
    /// statement bracket to scope a catalog snapshot to. On the surface because
    /// every other verb takes a `tid` and nothing else here can produce one.
    pub async fn resolve(&self, schema_name: &str, name: &str) -> Result<Option<Arc<RelDescriptor>>, ClientError> {
        let qname = qualified_name(schema_name, name);
        self.call(move |s| s.submit(Request::Resolve(RelTarget::Name(&qname))))
            .await
            .map(Reply::into_resolve)
    }

    /// Run `f` on a blocking thread against the handle's `GnitzClient`, which
    /// every clone shares and whose connection opens on first use. Callers
    /// serialize for the whole of `f`.
    pub async fn with_blocking_client<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut GnitzClient) -> Result<T, ClientError> + Send + 'static,
    ) -> Result<T, ClientError> {
        let target = Arc::clone(&self.target);
        let mut slot = Arc::clone(&self.client).lock_owned().await;
        blocking(move || {
            let client = match slot.take() {
                Some(c) => c,
                None => GnitzClient::connect(&target)?,
            };
            f(slot.insert(client))
        })
        .await?
    }

    /// [`Self::scan`], answered off the copy when it holds `tid`.
    pub async fn scan_local_first(&self, tid: u64) -> Result<ScanReply, ClientError> {
        let local = {
            let mut slot = Arc::clone(&self.client).lock_owned().await;
            if slot.as_ref().is_some_and(|c| c.mirrors(tid)) {
                blocking(move || slot.as_mut().map_or(Ok(None), |c| c.scan_local(tid))).await??
            } else {
                None
            }
        };
        match local {
            Some(reply) => Ok(reply),
            None => self.scan(tid).await,
        }
    }
}

/// Owns the connection, its `AsyncFd` and the request channel; drains, steps,
/// resolves. Poll it to completion — as a spawned task, or joined beside the
/// work that feeds it. It completes with `Ok(())` once every [`AsyncClient`] is
/// dropped **and** the connection has quiesced.
///
/// Dropping a verb's future is not cancellation: the frame is written and the
/// server commits it; the driver just drops a result nobody is left to receive.
pub struct Connection {
    session: Session,
    fd: AsyncFd<OwnedFd>,
    rx: mpsc::Receiver<Submission>,
    /// Registered slots in submit order. A queue, not a map: the spine holds one
    /// accumulator — the head slot's — and registers a slot only after every
    /// fallible step has passed, so completion is strictly FIFO.
    pending: VecDeque<(SlotId, oneshot::Sender<Result<Reply, ClientError>>)>,
}

impl Connection {
    /// Encode every submission the channel holds; `true` when the last handle is
    /// gone. At either of `submit`'s caps the channel is left unpolled — that is
    /// what makes them back-pressure rather than the error `submit` raises — and
    /// queued bytes arm `write`, so the next step brings the loop back round.
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

    /// Run one submission's encode step and register its slot. A request the
    /// spine refuses fails that one future and reaches no wire.
    fn submit(&mut self, sub: Submission) {
        let Submission { encode, reply } = sub;
        match encode(&mut self.session) {
            Ok(id) => self.pending.push_back((id, reply)),
            Err(e) => {
                let _ = reply.send(Err(e));
            }
        }
    }

    /// Fail every waiter, sent or not, with `Closed`; `cause` is the driver's
    /// own result.
    fn abort(&mut self, cause: ClientError) -> ClientError {
        self.session.close();
        for (_, reply) in self.pending.drain(..) {
            let _ = reply.send(Err(ClientError::Closed));
        }
        self.rx.close();
        while let Ok(sub) = self.rx.try_recv() {
            let _ = sub.reply.send(Err(ClientError::Closed));
        }
        cause
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
    type Output = Result<(), ClientError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        loop {
            let handles_gone = this.drain_channel(cx);

            let want = this.session.interest();
            if want.is_empty() {
                // Quiesced: either no handle is left and this is done, or the
                // drain above registered the channel's waker.
                return if handles_gone {
                    Poll::Ready(Ok(()))
                } else {
                    Poll::Pending
                };
            }

            let (read, write) = match poll_ready(&this.fd, want, cx) {
                Ok(guards) => guards,
                Err(e) => return Poll::Ready(Err(this.abort(e.into()))),
            };
            let ready = Interest {
                read: read.is_some(),
                write: write.is_some(),
            };
            if ready.is_empty() {
                return Poll::Pending;
            }

            let stepped = this.session.step(ready);
            // `AsyncFd` is edge-triggered: giving back readiness the step did
            // not spend waits for an edge that never comes. A read is always
            // spent — `step` reads until the source is drained — and a write is
            // spent exactly when the fd left bytes queued.
            let write_spent = this.session.interest().write;
            if let Some(mut g) = read {
                g.clear_ready();
            }
            if let (Some(mut g), true) = (write, write_spent) {
                g.clear_ready();
            }

            match stepped {
                Ok(done) => {
                    for (id, result) in done {
                        let (want, reply) = this.pending.pop_front().expect("a completion for an unregistered slot");
                        assert_eq!(want, id, "the spine completes slots in submit order");
                        // A slot whose receiver is gone — its verb's future was
                        // dropped — resolves to nothing.
                        let _ = reply.send(result);
                    }
                }
                // The byte stream's framing is no longer trustworthy.
                Err(e) => return Poll::Ready(Err(this.abort(e))),
            }
        }
    }
}
