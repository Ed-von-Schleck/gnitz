//! Snapshots: every base table and system family pinned as one point of the
//! log holds them, and read back a page at a time while writes go on.
//!
//! A pin is a read written to every worker at one SAL cut, so every worker pins
//! after exactly the commits ahead of it; the master pins its system families
//! inside the same cut. Nothing is copied: a pin keeps runs and shard mappings
//! alive past the folds and compactions that replace them, so what a snapshot
//! costs is that memory and disk come back only when it is released.
//!
//! A snapshot is its connection's: refused to any other, released by
//! SNAPSHOT_CLOSE or when the connection ends.

use std::cell::Cell;

use super::{send_ack, send_msg, Shared};
use crate::runtime::peer::Peer;
use crate::runtime::reactor::SendBody;
use crate::runtime::sal::Read;
use crate::runtime::wire as ipc;
use gnitz_store::relation::RelationKind;
use gnitz_wire::control::ControlHeader;
use gnitz_wire::snapshot::PageRequest;
use gnitz_wire::WireFault;
use gnitz_zset::algebra::Slot;

/// The snapshots one connection may hold at a time.
pub(super) const MAX_SNAPSHOTS_PER_CONNECTION: usize = 4;

/// The last snapshot id handed out.
#[derive(Default)]
pub(super) struct Snapshots {
    next: Cell<u64>,
}

/// The snapshots one connection holds.
#[derive(Default)]
pub(super) struct HeldSnapshots(Vec<u64>);

/// SNAPSHOT_OPEN: pin, and hold the snapshot for this connection.
pub(super) async fn open(shared: &Shared, held: &mut HeldSnapshots) -> Result<u64, WireFault> {
    if held.0.len() >= MAX_SNAPSHOTS_PER_CONNECTION {
        return Err(format!("this connection holds {MAX_SNAPSHOTS_PER_CONNECTION} snapshots; close one first").into());
    }
    let id = shared.snapshots.next.get() + 1;
    shared.snapshots.next.set(id);
    // No DDL moves the catalog between the master's pin and the workers'.
    let catalog = shared.catalog_rwlock.read().await;
    let leases = shared
        .disp()
        .scan_cut(|cut| {
            shared
                .cat_mut()
                .registry
                .pin(id, |k| k == RelationKind::SystemCatalog)
                .map_err(WireFault::from)?;
            cut.read_all(Read::Pin { snapshot: id })
        })
        .await;
    drop(catalog);
    let pinned = match leases {
        Ok(mut leases) => leases.pop().expect("one read, one lease").finish().await,
        Err(e) => Err(e),
    };
    if let Err(e) = pinned {
        release(shared, id).await;
        return Err(e);
    }
    held.0.push(id);
    Ok(id)
}

/// SNAPSHOT_PAGE: a page of `hdr.target_id` as snapshot `hdr.arg0` holds it.
/// The master answers a relation its own pin holds — a system family — and
/// every worker the rest, each worker's train forwarded in rank order, an empty
/// one included, so the client reads each worker's last key by its position.
pub(super) async fn page(
    shared: &Shared,
    peer: &Peer,
    held: &HeldSnapshots,
    hdr: &ControlHeader,
    blob: &[u8],
) -> Result<(), WireFault> {
    let (id, tid) = (hdr.arg0, hdr.target_id);
    if !held.0.contains(&id) {
        return Err(format!("snapshot {id} is not this connection's").into());
    }
    let request = PageRequest::decode(blob).map_err(|e| format!("decode error: {e}"))?;
    if shared.cat().registry.pins(id, tid) {
        let rows = shared.cat().registry.pinned_page(id, tid, &request, Slot::SOLO)?;
        send_msg(peer, ipc::WireMsg::train_frame(tid, true, rows.wire_whole()));
    } else {
        let read = Read::SnapshotPage { snapshot: id, tid, page: blob.into() };
        let lease = shared.disp().scan_all(read).await?;
        'forward: for w in lease.workers().iter() {
            let mut forwarded = false;
            while let Some(f) = lease.next_of(w).await? {
                forwarded = true;
                if peer.send(SendBody::Slot(f.slot)).await.is_err() {
                    break 'forward;
                }
            }
            // A train with no rows ends in a frame `next_of` does not hand out.
            if !forwarded {
                send_msg(peer, ipc::WireMsg::train_frame(tid, true, None));
            }
        }
    }
    send_ack(peer, tid, 0);
    Ok(())
}

/// SNAPSHOT_CLOSE: release snapshot `id` this connection holds.
pub(super) async fn close(shared: &Shared, held: &mut HeldSnapshots, id: u64) -> Result<(), WireFault> {
    let Some(at) = held.0.iter().position(|&h| h == id) else {
        return Err(format!("snapshot {id} is not this connection's").into());
    };
    held.0.swap_remove(at);
    release(shared, id).await;
    Ok(())
}

/// Release every snapshot a connection that has ended held.
pub(super) async fn release_all(shared: &Shared, held: HeldSnapshots) {
    for id in held.0 {
        release(shared, id).await;
    }
}

/// Release snapshot `id` here and on every worker. A worker that cannot answer
/// the release has left the cluster, which ends the server.
async fn release(shared: &Shared, id: u64) {
    shared.cat_mut().registry.unpin(id);
    let released = async {
        shared
            .disp()
            .scan_all(Read::Unpin { snapshot: id })
            .await?
            .finish()
            .await
    }
    .await;
    if let Err(e) = released {
        gnitz_warn!("snapshot {id}: release failed: {}", e.text);
    }
}
