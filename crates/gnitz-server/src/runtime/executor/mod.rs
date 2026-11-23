//! Server executor: the serving loop (accept, the signal loop), the `Shared`
//! state, the request router, and the reply-frame vocabulary. Each child
//! module states its own scope.
//!
//! The master owns one `Reactor`: [`serve`] spawns its tasks on it and runs the
//! signal loop until a shutdown signal; `bootstrap` races it against a worker's
//! death.
//!
//! The router writes a request's last frame when it is the ACK naming the
//! request's own `target_id`, or a fault of the request as a whole. A scan, a
//! delta poll, a resolve and a sync write theirs.
//!
//! One connection's replies leave in request order, written by its own task.
//! The one thing another task sends a client is a pushed train, and it only
//! queues it: the connection's task ships it between two replies.

mod backup;
mod committer;
mod ddl;
mod delta;
mod family;
mod read;
mod snapshot;
mod tick;
mod validate;
mod write;

use gnitz_foundation::{gnitz_debug, gnitz_error, gnitz_info, gnitz_note, gnitz_warn};
use std::cell::{Cell, Ref};
use std::os::fd::{AsFd, OwnedFd};
use std::rc::Rc;
use std::time::Duration;

use self::committer::{CommitRequests, Committer};
use self::ddl::{allocate_ids, handle_ddl_txn, reserve_serial_range};
use self::delta::{Feeds, Subscriptions, handle_delta_poll};
use self::snapshot::HeldSnapshots;
use self::write::{OpenWrite, Writes};
use crate::catalog::CatalogEngine;
use crate::runtime::frame::WireMsg;
use crate::runtime::listen::ClientListener;
use crate::runtime::master::MasterDispatcher;
use crate::runtime::reactor::{Peer, RecvBuf, SendBody, Train};
use crate::runtime::signals;
use crate::runtime::sync::{AsyncRwLock, Either, WriteGuard, chan, oneshot, select2};
use crate::runtime::tls::{Served, TlsListener};
use gnitz_store::relation::{Relation, RelationKind};
use gnitz_wire::{ClientVerb, ControlHeader, Target, WireFault, WireStatus};

/// Park in 1 ms reactor ticks until `ready`, or until `polls` elapse. The shape
/// every hold-seam needs: it holds no lock while it waits, so the event it waits
/// for can actually happen, and the bound releases it if that event never comes
/// instead of wedging the node. It logs when it parks and which way it released,
/// so a test waits for the park and asserts the release rather than passing on a
/// hold that never engaged.
async fn park_until(shared: &Shared, polls: u32, what: &str, ready: impl Fn() -> bool) {
    gnitz_note!("{}: parked", what);
    for _ in 0..polls {
        if ready() {
            gnitz_note!("{}: released by its event", what);
            return;
        }
        shared.dispatcher.reactor().sleep(Duration::from_millis(1)).await;
    }
    gnitz_warn!("{}: seam armed but the event never arrived; releasing", what);
}

/// The tag of the catalog lock: a guard of it orders a task against DDL.
enum CatalogLock {}

/// Shared executor state held by every task. Never freed, as the tasks that
/// hold it never end.
///
/// Lock order: the catalog, then a table lock, then the apply lock, then the
/// SAL writer.
struct Shared {
    dispatcher: MasterDispatcher,
    committer: Committer,
    catalog_rwlock: AsyncRwLock<CatalogLock>,
    writes: Writes,
    feeds: Feeds,
    /// How long a connection may take from accept to its HELLO, handshake
    /// included (`GNITZ_HELLO_TIMEOUT_MS`). The default outlasts the client's own
    /// `CONNECT_TIMEOUT`, so only a client that has already given up is reaped.
    hello_timeout: Duration,
    /// The last snapshot id handed out.
    last_snapshot: Cell<u64>,
    /// The directory BACKUP writes its entries into (`--backup-dir`); `None`
    /// refuses BACKUP.
    backup_dir: Option<String>,
}

impl Shared {
    /// The state of an executor over `dispatcher`, and the request queues of
    /// its committer and its delta pump.
    fn new(
        dispatcher: MasterDispatcher,
        hello_timeout: Duration,
        backup_dir: Option<String>,
    ) -> (&'static Shared, CommitRequests, chan::Receiver<oneshot::Sender<()>>) {
        let boot_seed = dispatcher.sal().watermark();
        let (committer, committer_rx) = Committer::new();
        let (feeds, asks) = Feeds::new();
        let shared = Box::leak(Box::new(Shared {
            dispatcher,
            committer,
            catalog_rwlock: AsyncRwLock::default(),
            writes: Writes::new(boot_seed),
            feeds,
            hello_timeout,
            last_snapshot: Cell::new(0),
            backup_dir,
        }));
        (shared, committer_rx, asks)
    }

    fn cat(&self) -> Ref<'_, CatalogEngine> {
        self.dispatcher.cat()
    }

    /// Drop the per-relation state of a relation whose DROP is durable: every
    /// per-relation state a drop reclaims is cleared here. Every table lock is
    /// taken under a catalog read guard, so the write guard proves no task holds
    /// or awaits the lock being removed.
    fn forget_relation(&self, _catalog_write: &WriteGuard<CatalogLock>, id: u64) {
        self.writes.forget(id);
        self.dispatcher.forget_relation(id);
    }
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

/// Serve `listeners` until a shutdown signal, having run the final checkpoint.
pub(super) async fn serve(dispatcher: MasterDispatcher, backup_dir: Option<String>, listeners: Vec<ClientListener>) {
    let reactor = dispatcher.reactor();
    let hello_timeout = Duration::from_millis(gnitz_foundation::env::env_num(
        "GNITZ_HELLO_TIMEOUT_MS",
        (gnitz_wire::CONNECT_TIMEOUT * 3 / 2).as_millis() as u64,
    ));
    let patient_gap = Duration::from_millis(gnitz_foundation::env::env_num("GNITZ_PATIENT_TICK_GAP_MS", 10));
    let (shared, committer_rx, asks) = Shared::new(dispatcher, hello_timeout, backup_dir);

    let signals = signals::Signals::open();
    // Past the last env knob the boot reads, so one it refuses stops the
    // server before it reports ready.
    gnitz_note!("{}", gnitz_foundation::log::READY_LINE);

    let tls = listeners.iter().find_map(|l| l.tls.clone());
    reactor.spawn(shared.dispatcher.sal().run_syncs(reactor));
    reactor.spawn(committer::run(committer_rx, shared));
    for listener in listeners {
        reactor.spawn(accept_loop(shared, listener));
    }
    reactor.spawn(tick::tick_loop(shared, patient_gap));
    reactor.spawn(delta::pump(shared, asks));

    serve_until_signalled(shared, tls.as_deref(), signals).await
}

// ---------------------------------------------------------------------------
// Accept loop
// ---------------------------------------------------------------------------

async fn accept_loop(shared: &'static Shared, listener: ClientListener) {
    let ClientListener { fd, tls } = listener;
    // Leaked: the loop never ends, and its accept SQEs name the fd throughout.
    let fd: &'static OwnedFd = Box::leak(Box::new(fd));
    let reactor = shared.dispatcher.reactor();
    loop {
        let accepted = reactor.accept(fd.as_fd()).await;
        let served = tls.as_ref().map(|t| t.current());
        let peer = reactor.connect(accepted, served.as_ref().map(|s| s.session()));
        reactor.spawn(connection_loop(peer, shared, served));
    }
}

async fn connection_loop(mut peer: Peer, shared: &'static Shared, served: Option<Rc<Served>>) {
    serve_connection(&mut peer, shared, served).await;
    // The one exit: ship what is corked, a refusal included, then retire the fd.
    let _ = peer.flush_egress().await;
    peer.close().await;
}

/// One message is handled to completion before the next is received, so
/// replies leave in request order, which is how a client pairs each with its
/// request. A sync's answer is no reply, and leaves between two. A push is
/// handled once it is laid out; its ACK, and every byte behind it, waits for
/// the fdatasync that covers it. A write spanning several frames is one
/// request, answered after its last frame; any other frame among its frames
/// closes the connection.
///
/// Returns when the peer is gone or refused; the caller closes.
async fn serve_connection(peer: &mut Peer, shared: &'static Shared, served: Option<Rc<Served>>) {
    let deadline = shared.dispatcher.reactor().sleep(shared.hello_timeout);
    match select2(handshake(peer, served), deadline).await {
        Either::A(true) => peer.admit_full_frames(),
        Either::A(false) => return,
        // The deadline may have cut a session's handshake flight short.
        Either::B(()) => return peer.abort(),
    }

    let mut subs = Subscriptions::default();
    let mut snapshots = HeldSnapshots::default();
    loop {
        // Ahead of the next request: a client that always has one ready is
        // still answered its sync.
        if let Some(waiting) = subs.sync_wait(shared) {
            let Ok(synced) = peer.until_request(waiting.as_mut()).await else {
                break;
            };
            if let Some(synced) = synced {
                subs.answer_sync(peer, synced).await;
                continue;
            }
        }
        let Some(buf) = peer.next_request().await else { break };
        if !handle_message(peer, &mut subs, &mut snapshots, buf, shared).await {
            break;
        }
        // Behind the reply, where a pushed train splits none. A client that
        // is gone is found out by the next request.
        let _ = subs.ship(peer).await;
    }
    subs.leave(&shared.feeds, |_| true);
    snapshot::release_all(shared, snapshots).await;
}

/// Answer the client's HELLO, its verdict on the access token it carries; true
/// once the client is admitted.
async fn handshake(peer: &mut Peer, served: Option<Rc<Served>>) -> bool {
    let Some(hello) = peer.next_request().await else {
        return false;
    };
    let Ok(token) = gnitz_wire::read_client_hello(hello.as_slice()) else {
        // This build's HELLO, so a client of another version reports the
        // mismatch, and its release, which the client names.
        peer.cork_frame(&gnitz_wire::refusal_hello());
        return false;
    };
    let admitted = match served.as_deref().and_then(|s| s.tokens.as_ref()) {
        None => true,
        Some(tokens) => match tokens.check(token) {
            Some(name) => {
                gnitz_debug!("a connection authenticated with access token {name:?}");
                true
            }
            None => {
                gnitz_info!(
                    "refused the connection from {}: it holds no access token the server does",
                    peer.peer_addr()
                        .map_or_else(|| "a client".to_string(), |a| a.to_string())
                );
                false
            }
        },
    };
    peer.cork_frame(&gnitz_wire::server_hello(if admitted {
        gnitz_wire::AuthStatus::Accepted
    } else {
        gnitz_wire::AuthStatus::Refused
    }));
    admitted
}

// ---------------------------------------------------------------------------
// Signal loop: TLS reload and the final checkpoint
// ---------------------------------------------------------------------------

/// Until SIGTERM/SIGINT, read off `signals`: reload the TLS listener on each
/// SIGHUP. Then close the committer, which runs one final checkpoint behind
/// every write it admitted.
async fn serve_until_signalled(shared: &Shared, tls: Option<&TlsListener>, mut signals: signals::Signals) {
    loop {
        match signals.next(shared.dispatcher.reactor()).await {
            libc::SIGHUP => match tls {
                None => gnitz_note!("SIGHUP: no TLS listener, nothing to reload"),
                Some(tls) => match tls.reload() {
                    Ok(()) => gnitz_note!("SIGHUP: reloaded the TLS listener: {}", tls.describe()),
                    Err(e) => gnitz_error!("SIGHUP: the TLS reload failed, the previous setup is still served: {e}"),
                },
            },
            _ => {
                gnitz_info!("shutdown signal received; draining, checkpointing, and stopping");
                // The final checkpoint ticks every push no tick has taken yet.
                shared.committer.close().await;
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

/// Handle one request: a frame, or a write's every frame, which
/// [`receive_write`] takes; and end its reply. An arm that yields a value is
/// answered with the request's ACK carrying it; one that yields `None` has
/// written its last frame itself, a fault confined to one position of its
/// reply included. An `Err` is answered with one fault frame, which fails the
/// whole request at the client. `false` once the connection is to be closed: a
/// frame that is not the write's broke its frames off, which leaves no answer
/// the client could pair.
async fn handle_message(
    peer: &mut Peer,
    subs: &mut Subscriptions,
    snapshots: &mut HeldSnapshots,
    buf: RecvBuf,
    shared: &'static Shared,
) -> bool {
    // One control-header parse for the whole request.
    let ctrl = match gnitz_wire::peek_control_block(buf.as_slice()) {
        Ok(c) => c,
        Err(e) => {
            cork_fault(peer, 0, &decode_fault(e));
            return true;
        }
    };
    let (hdr, data) = (ctrl.hdr, buf.as_slice());
    let (body, blob) = (&data[ctrl.body.clone()], &data[ctrl.blob.clone()]);
    let answered = match ctrl.client_verb() {
        Ok(verb @ (ClientVerb::Push | ClientVerb::PushTxn)) => {
            let Some(frames) = receive_write(peer, verb, buf, ctrl).await else {
                let broken = "a write's frames were broken off by another request; nothing was written, and the \
                              connection is closed";
                cork_fault(peer, hdr.target_id, &broken.into());
                return false;
            };
            let followed = peer.request_queued();
            let lsn = match frames {
                Ok(frames) => write::handle_write(shared, verb, frames, followed).await,
                Err(why) => Err(why),
            };
            lsn.map(|lsn| {
                // The write is laid out, not yet durable: `peer` sends nothing
                // more until the fdatasync the committer asked for covers it,
                // and reads and runs the requests behind it meanwhile, so one
                // fdatasync answers a pipelined run.
                peer.fence(shared.dispatcher.sal().durable_level(), lsn);
                Some(lsn)
            })
        }
        // The multi-item frames name no single relation in `target_id`; each
        // decodes its items from the frame's body. A DDL's future is boxed: held
        // inline it would set the size of every connection's.
        Ok(ClientVerb::DdlTxn) => Box::pin(handle_ddl_txn(shared, data, ctrl.body.clone()))
            .await
            .map(Some),
        Ok(ClientVerb::ScanMulti) => read::handle_scan(shared, peer, body).await.map(|()| None),
        Ok(ClientVerb::DeltaPoll) => handle_delta_poll(shared, peer, subs, &hdr, body).await.map(|()| None),
        // `target_id` is the sequence key (= the owning table's id).
        Ok(ClientVerb::AllocSerialRange) => reserve_serial_range(shared, hdr.target(), hdr.arg0).await.map(Some),
        // An id allocation names no relation, so `target_id` is only echoed.
        Ok(ClientVerb::AllocIds) => allocate_ids(shared, hdr.arg0).await.map(Some),
        Ok(ClientVerb::Resolve) => read::handle_resolve(shared, peer, blob).await.map(|()| None),
        Ok(ClientVerb::SyncPushed) => {
            // Refused or not, it is answered as a sync is: out of the order of replies.
            subs.ask_sync(shared, blob, hdr.arg0);
            Ok(None)
        }
        // A snapshot and a backup name no relation of the live catalog in
        // `target_id`: a page names one of the snapshot's.
        Ok(ClientVerb::SnapshotOpen) => snapshot::open(shared, snapshots).await.map(Some),
        Ok(ClientVerb::SnapshotPage) => snapshot::page(shared, peer, snapshots, &hdr, blob)
            .await
            .map(|()| Some(0)),
        Ok(ClientVerb::SnapshotClose) => snapshot::close(shared, snapshots, hdr.arg0).await.map(|()| Some(0)),
        Ok(ClientVerb::Backup) => backup::backup(shared, blob).await.map(|()| Some(0)),
        Err(e) => Err(e.into()),
    };
    match answered {
        Ok(Some(value)) => cork_ack(peer, hdr.target_id, value),
        Ok(None) => {}
        Err(f) => cork_fault(peer, hdr.target_id, &f),
    }
    true
}

/// The frames of the `verb` write `buf` heads: `buf` alone, or, where it sets
/// `continuation`, it and each frame after it up to the first that does not,
/// held as [`OpenWrite`] holds them. `None` once a frame of another verb, or
/// one that does not decode, broke them off, or the client is gone.
async fn receive_write(
    peer: &mut Peer,
    verb: ClientVerb,
    mut buf: RecvBuf,
    mut ctrl: gnitz_wire::DecodedControl,
) -> Option<Result<write::WriteFrames, WireFault>> {
    if !ctrl.hdr.flags.continuation {
        return Some(Ok(smallvec::smallvec![(buf, ctrl)]));
    }
    let mut write = OpenWrite::new();
    while write.take(buf, ctrl, peer.write_spans()) {
        buf = peer.next_request().await?;
        ctrl = (gnitz_wire::peek_control_block(buf.as_slice()).ok()).filter(|c| c.hdr.flags.verb == Some(verb))?;
    }
    Some(write.finish())
}

/// The fault a request whose frame does not decode is refused with.
pub(super) fn decode_fault(e: impl std::fmt::Display) -> WireFault {
    format!("decode error: {e}").into()
}

/// Which end of a relation a request wants — the discriminator of [`target_kind`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum Access {
    /// Any read, a system catalog family included.
    Read,
    Write,
}

/// Resolve `target`'s kind, rejecting a request whose descriptor token is
/// stale and a target that cannot serve `access`. The token comes first: a stale
/// one names a relation whose absence or kind is not the client's error. `0` is
/// a request that carries none.
///
/// A stream holds no rows, so it may be written but not read. A view may be read
/// but not written: a push would commit rows its circuit never produced.
///
/// Enforced here even though the SQL binder refuses both: a client can push to
/// or read a relation id without going through the binder.
///
/// **Only the absent-relation arm carries a status of its own**
/// ([`WireStatus::NotFound`]): the arms below name a relation that exists, which a
/// client must not recover from the way it recovers from a vanished one.
fn target_kind(cat: &CatalogEngine, target: Target, access: Access) -> Result<RelationKind, WireFault> {
    cat.check_token(target)?;
    let target_id = target.tid;
    let Some(kind) = cat.registry.relation(target_id).map(Relation::kind) else {
        return Err(WireFault {
            status: WireStatus::NotFound,
            text: format!("relation {target_id} not found"),
        });
    };
    match access {
        Access::Read if kind == RelationKind::Stream => {
            Err(format!("table {target_id} is a stream: a stream holds no rows and cannot be read").into())
        }
        Access::Write if !kind.is_ingestion_point() => {
            Err(format!("table {target_id} is not writable: pushes must target a base table or a stream").into())
        }
        _ => Ok(kind),
    }
}

// ---------------------------------------------------------------------------
// Wire-protocol response helpers
// ---------------------------------------------------------------------------

/// Forward every frame of `train` that carries rows to `peer`, workers in ascending
/// order. Stops at the first failed send, which closes the peer; `Err` on the first
/// worker fault, leaving the rest undrained.
async fn forward_scan(peer: &mut Peer, train: Train<'_>) -> Result<(), WireFault> {
    while let Some(f) = train.next().await? {
        if peer.send(SendBody::Slot(f.slot)).await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}

/// Cork `msg` for the client; the connection loop ships it. Every frame the
/// master mints is a bare head or a fault, whose text [`WireMsg::fault`] cuts,
/// but a RESOLVE answer, sized before it is corked, and [`cork_train`]'s.
fn cork_msg(peer: &mut Peer, msg: WireMsg<'_>) {
    debug_assert!(
        msg.check_size().is_ok(),
        "an unchecked reply frame is over MAX_FRAME_PAYLOAD"
    );
    msg.encode_framed(peer.corked())
}

/// Cork `rows` as a train of `target_id` read off the master's own copy: frames
/// of at most `MAX_FRAME_PAYLOAD`, the last flagged `scan_last`, cut by
/// [`WireMsg::next_train_frame`] as a worker's are. The ACK that follows the
/// train is [`read::handle_scan`]'s for a scan and the router's for a snapshot
/// page. `Err` is a row too wide for any frame, the frames before it corked.
fn cork_train(peer: &mut Peer, target_id: u64, rows: &gnitz_zset::repr::Batch) -> Result<(), WireFault> {
    let mut next = 0;
    loop {
        let (msg, n) = WireMsg::next_train_frame(target_id, rows, next, gnitz_wire::MAX_FRAME_PAYLOAD, true)?;
        cork_msg(peer, msg);
        if msg.hdr.flags.scan_last {
            return Ok(());
        }
        next = n;
    }
}

/// A control-only ACK: the request's own `target_id`, and its one value — a
/// write's LSN, an allocation's base id, or the watermark of the read whose
/// train it ends — in `arg0`.
fn cork_ack(peer: &mut Peer, target_id: u64, value: u64) {
    let hdr = ControlHeader::ack(target_id, value);
    cork_msg(peer, WireMsg { hdr, ..Default::default() })
}

/// A failure carrying its own status, master-minted or forwarded from a worker.
fn cork_fault(peer: &mut Peer, target_id: u64, fault: &WireFault) {
    cork_msg(peer, WireMsg::fault(target_id, fault))
}

#[cfg(all(test, fuzzing))]
#[path = "fuzz/executor.rs"]
mod fuzz;

#[cfg(test)]
#[path = "tests/executor.rs"]
mod tests;
