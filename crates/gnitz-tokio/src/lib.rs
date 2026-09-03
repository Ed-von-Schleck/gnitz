//! The Rust async client: a [`Connection`] future over tokio's reactor, and
//! the [`AsyncClient`] handle that feeds it.
//!
//! `gnitz-core`'s `Session` is the spine and owns all the protocol, down to
//! resolving each reply against the request that asked for it. This crate owns
//! how it waits.
//!
//! Off the surface deliberately: `execute_sql`, transactions, DDL and id
//! allocation. Each is an interleaved read/compute/write sequence over
//! `GnitzClient` state rather than a wire verb, so serving one here would mean
//! blocking the driver task. They stay on the blocking client.
//!
//! **Mirroring is served by owning a blocking client that mirrors**, so the
//! reconciliation state machine and every ordering rule its correctness rests on
//! exist once and this crate runs that code instead of a second copy of it.
//! Driving the feed here instead would mean rewriting that state machine as
//! async: it interleaves round trips with blocking store calls, and the cursor
//! protocol under `delta_poll_raw` is `gnitz-core`-private. The crate still
//! links no engine: it takes an opened store, as the blocking client does.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::os::fd::OwnedFd;
use std::pin::Pin;
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll};

use gnitz_core::{
    qualified_name, ClientError, DeltaCursor, GnitzClient, Interest, LocalScanReply, MirrorStore, PkTuple, PollOutcome,
    RelDescriptor, RelTarget, Reply, Request, ScanReply, Schema, Session, SlotId, WireConflictMode, ZSetBatch,
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

/// A channel sender, plus the second client a copy lives inside once one is
/// attached: `Send + Sync + Clone`. Every method takes `&self`, so sharing one
/// is a clone.
#[derive(Clone)]
pub struct AsyncClient {
    tx: mpsc::Sender<Submission>,
    /// What [`AsyncClient::attach_mirror`] connects the feed's own client to.
    target: Arc<str>,
    /// The blocking client the copy lives inside, once a host attaches a store.
    /// `Arc` so a clone sees an install on any other; `Mutex<Option<_>>` so the
    /// `&self` methods every verb takes can install into it and reach it.
    mirror: Arc<Mutex<Option<GnitzClient>>>,
}

// The handle is shared by cloning, so this is what it promises; the mirror field
// is the one thing that could take it away silently.
const _: fn() = || {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<AsyncClient>();
};

/// The mirror slot, recovering a poisoned lock: what it guards is a client whose
/// own poison state is the real verdict, and a panicking task must not take
/// every later mirror call down with it.
fn lock(slot: &Mutex<Option<GnitzClient>>) -> MutexGuard<'_, Option<GnitzClient>> {
    slot.lock().unwrap_or_else(|e| e.into_inner())
}

/// Run `f` on a blocking pool thread. A panic in it reaches the caller as an
/// error rather than aborting the runtime.
async fn blocking<T: Send + 'static>(f: impl FnOnce() -> T + Send + 'static) -> Result<T, ClientError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| ClientError::ServerError(format!("mirror task failed: {e}")))
}

/// Connect to `target` and hand back the handle paired with the driver that
/// serves it. Nothing reaches the connection until the [`Connection`] is
/// polled.
///
/// The TCP connect and TLS handshake block — a dual-stack `tls://` host can
/// spend tens of seconds in them — so they run on `spawn_blocking`.
pub async fn connect(target: &str) -> Result<(AsyncClient, Connection), ClientError> {
    let target: Arc<str> = Arc::from(target);
    let connect_to = Arc::clone(&target);
    let session = tokio::task::spawn_blocking(move || Session::connect(&connect_to))
        .await
        .map_err(|e| ClientError::ServerError(format!("connect task failed: {e}")))?
        .map(|(session, _published_lsn)| session)?;
    // A `dup`, so the reactor deregisters a descriptor whose life it owns
    // rather than a number the session may already have closed and the kernel
    // handed out again.
    let fd = AsyncFd::new(session.try_clone_fd()?)?;
    let (tx, rx) = mpsc::channel(REQUEST_CHANNEL_DEPTH);
    Ok((
        AsyncClient {
            tx,
            target,
            mirror: Arc::new(Mutex::new(None)),
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
            .send(Submission {
                encode: Box::new(encode),
                reply,
            })
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

    pub async fn seek(&self, tid: u64, pk: PkTuple) -> Result<ScanReply, ClientError> {
        self.call(move |s| s.submit(Request::seek(tid, &pk)))
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
            .map(|r| {
                r.into_resolve()
                    .map(|(tid, schema, blob)| Arc::new(RelDescriptor::from_resolve(tid, schema, blob)))
            })
    }

    // ── Mirroring ──────────────────────────────────────────────────────────
    //
    // The verbs hop to a blocking-pool thread: the lock is held across a whole
    // poll, so waiting for it would stall a reactor thread that long. The
    // accessors hop only when the slot is contended. A mirroring handle also
    // holds a second connection — the feed's — and so a second server-side slot.

    /// Read a local copy of one or more views through `store`.
    ///
    /// Refused if this handle, or any clone of it, already mirrors — the store
    /// passed in is dropped, releasing its `flock`. Opening a store is itself
    /// blocking work, so a host should do that off the reactor too.
    ///
    /// The lock spans the whole connect-and-install, and has to: two clones
    /// calling this at once would otherwise both see no store, both connect, and
    /// one install would silently drop the other's client — a checkpoint and a
    /// released `flock`.
    pub async fn attach_mirror(&self, store: impl MirrorStore + 'static) -> Result<(), ClientError> {
        let slot = Arc::clone(&self.mirror);
        let target = Arc::clone(&self.target);
        blocking(move || {
            let mut held = lock(&slot);
            if held.is_some() {
                return Err(ClientError::ServerError(
                    "this handle already mirrors; close_mirror before attaching another store".to_string(),
                ));
            }
            let mut client = GnitzClient::connect(&target)?;
            client.attach_mirror(store)?;
            *held = Some(client);
            Ok(())
        })
        .await?
    }

    /// Checkpoint (unless poisoned) and release the store, reporting the final
    /// checkpoint. The handle can attach again afterwards.
    ///
    /// A drop forfeits the rounds since the last checkpoint — at most one
    /// bootstrap per view, when the feed no longer covers them.
    pub async fn close_mirror(&self) -> Result<(), ClientError> {
        let slot = Arc::clone(&self.mirror);
        blocking(move || {
            // Taken out in one statement, so the guard drops before the fsync
            // below and a close stalls no other clone.
            let mut taken = lock(&slot).take().ok_or(ClientError::NoMirrorStore)?;
            taken.close_mirror()
        })
        .await?
    }

    /// Mirror `schema_name.name` and bring its copy up to date.
    pub async fn mirror_view(&self, schema_name: &str, name: &str) -> Result<PollOutcome, ClientError> {
        let (schema_name, name) = (schema_name.to_string(), name.to_string());
        self.on_mirror(move |c| c.mirror_view(&schema_name, &name)).await
    }

    /// Advance every mirrored view by one poll each, one entry per view.
    ///
    /// One whole advance runs inside one `spawn_blocking` under the one lock and
    /// spans no `await`, so two clones polling the same view serialize instead of
    /// both reading cursor `c`, both fetching `(c, …]` and both applying it —
    /// which would double every weight in the interval while the row set stayed
    /// identical.
    pub async fn poll_mirror(&self) -> Result<Vec<PollOutcome>, ClientError> {
        self.on_mirror(GnitzClient::poll_mirror).await
    }

    /// Stop mirroring `table_id`.
    pub async fn forget_view(&self, table_id: u64) -> Result<(), ClientError> {
        self.on_mirror(move |c| c.forget_view(table_id)).await
    }

    /// Make every copy and its cursor durable.
    pub async fn checkpoint_mirror(&self) -> Result<(), ClientError> {
        self.on_mirror(GnitzClient::checkpoint_mirror).await
    }

    /// Whether a read of `table_id` is answered off the copy. `false` on a
    /// handle that mirrors nothing.
    pub async fn mirrors(&self, table_id: u64) -> Result<bool, ClientError> {
        self.on_mirror_or(false, move |c| c.mirrors(table_id)).await
    }

    /// The round a local read of `table_id` answers at. `None` on a handle that
    /// mirrors nothing.
    pub async fn cursor_of(&self, table_id: u64) -> Result<Option<DeltaCursor>, ClientError> {
        self.on_mirror_or(None, move |c| c.cursor_of(table_id)).await
    }

    /// Every registration the copy holds. Empty on a handle that mirrors
    /// nothing.
    pub async fn mirrored_ids(&self) -> Result<Vec<u64>, ClientError> {
        self.on_mirror_or(Vec::new(), |c| c.mirrored_ids()).await
    }

    /// The message that poisoned the copy, if any. `None` on a handle that
    /// mirrors nothing.
    pub async fn mirror_poisoned(&self) -> Result<Option<String>, ClientError> {
        self.on_mirror_or(None, |c| c.mirror_poisoned().map(str::to_string))
            .await
    }

    /// Run `f` on the installed client, under the lock, on a blocking thread.
    async fn on_mirror<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut GnitzClient) -> Result<T, ClientError> + Send + 'static,
    ) -> Result<T, ClientError> {
        let slot = Arc::clone(&self.mirror);
        blocking(move || {
            let mut held = lock(&slot);
            f(held.as_mut().ok_or(ClientError::NoMirrorStore)?)
        })
        .await?
    }

    /// Ask the installed client, or answer as a store-less `GnitzClient` does.
    /// Uncontended it answers on the caller's thread — an O(1) map read against
    /// a ~30 µs hop; a contended slot means a poll holds the lock, so it hops.
    async fn on_mirror_or<T: Send + 'static>(
        &self,
        none: T,
        f: impl FnOnce(&GnitzClient) -> T + Send + 'static,
    ) -> Result<T, ClientError> {
        if let Ok(held) = self.mirror.try_lock() {
            return Ok(held.as_ref().map_or(none, f));
        }
        let slot = Arc::clone(&self.mirror);
        blocking(move || lock(&slot).as_ref().map_or(none, f)).await
    }

    /// [`Self::scan`], answered off the copy when it holds `tid`.
    ///
    /// A handle that never attached takes the wire path with no thread hop.
    /// Otherwise one `spawn_blocking` asks the client's `scan_local`, and a
    /// relation it does not mirror is delegated on the async path, keeping the
    /// driver's back-pressure and its reply's LSN.
    ///
    /// [`Self::seek`] and [`Self::scan_many`] stay wire-only: no caller has
    /// needed a local-first form of either.
    pub async fn scan_local_first(&self, tid: u64) -> Result<LocalScanReply, ClientError> {
        // A slot we cannot read means an installed store — a poisoned lock
        // included, which `lock` below recovers. The guard drops with this
        // statement, so the hop does not wait on a lock this task holds.
        let attached = !matches!(self.mirror.try_lock().as_deref(), Ok(None));
        if attached {
            let slot = Arc::clone(&self.mirror);
            let local = blocking(move || match lock(&slot).as_mut() {
                Some(c) => c.scan_local(tid),
                None => Ok(None),
            })
            .await??;
            if let Some((schema, batch)) = local {
                return Ok((Some(schema), Some(batch), None));
            }
        }
        let (schema, batch, lsn) = self.scan(tid).await?;
        Ok((schema, batch, Some(lsn)))
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

    /// Abandon every pending slot with `cause`, then hand it back as this
    /// driver's own result. `ClientError` is not `Clone`, so each slot gets a
    /// rendering of it.
    ///
    /// The channel is closed and drained with them: a submission buffered here,
    /// or sent after this, has no driver left to answer it and never reached the
    /// wire.
    fn abort(&mut self, cause: ClientError) -> ClientError {
        self.session.close();
        let text = cause.to_string();
        for (_, reply) in self.pending.drain(..) {
            let _ = reply.send(Err(ClientError::ServerError(text.clone())));
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
