//! The catalog-shape and row reads: RESOLVE, and SCAN_MULTI under one SAL cut.

use super::tick::fresh_read_lock;
use super::{Access, Shared, cork_ack, cork_msg, cork_train, decode_fault, forward_scan, target_kind};
use crate::runtime::frame::WireMsg;
use crate::runtime::reactor::{Peer, TrainLease};
use crate::runtime::sal::Read;
use gnitz_store::relation::RelationKind;
use gnitz_wire::{ControlHeader, ReadSpec, WireFault, WireStatus};
use gnitz_zset::repr::Batch;
use std::rc::Rc;

/// RESOLVE: answer with the relation's descriptor, its schema record included,
/// and its token.
///
/// Under a plain read guard, not `fresh_read_lock`: a resolve answers catalog
/// shape, and a view tick moves a view's rows, never its shape, so the tick
/// drain that one waits for buys nothing here.
pub(super) async fn handle_resolve(shared: &Shared, peer: &mut Peer, name_blob: &[u8]) -> Result<(), WireFault> {
    let qname = std::str::from_utf8(name_blob).map_err(|_| "RESOLVE: name is not valid UTF-8".to_string())?;
    let (schema_name, name) = qname
        .split_once('.')
        .ok_or_else(|| format!("RESOLVE: '{qname}' is not a qualified relation name"))?;
    let _catalog = shared.catalog_rwlock.read().await;
    let cat = shared.cat();
    let sid = cat.schema_id(schema_name).ok_or_else(|| WireFault {
        status: WireStatus::NotFound,
        text: format!("schema '{schema_name}' not found"),
    })?;
    let answer = cat
        .relation_id(sid, name)
        .and_then(|tid| cat.resolve_answer(tid).map(|a| (tid, a)));
    let msg = match &answer {
        // No such relation: a successful reply naming none.
        None => WireMsg::default(),
        Some((tid, (desc, token))) => WireMsg {
            hdr: ControlHeader::ack(*tid, *token),
            blob: desc,
            ..Default::default()
        },
    };
    msg.check_size()?;
    cork_msg(peer, msg);
    Ok(())
}

/// One scan of [`handle_scan`], resolved under the catalog guard.
struct Scanned {
    kind: RelationKind,
    /// A system catalog family's rows, read off the master's own copy; `None`
    /// for a scan its workers answer.
    local: Option<Rc<Batch>>,
    /// The read's watermark; a fanned scan's is sampled inside the cut.
    lsn: u64,
}

/// SCAN_MULTI: one reply per scan, in request order. The scans
/// its workers answer share one SAL cut, so an atomic commit is in all of
/// their replies or in none.
///
/// An `Err` ends the reply: nothing of the request follows the router's fault
/// frame.
pub(super) async fn handle_scan(shared: &Shared, peer: &mut Peer, body: &[u8]) -> Result<(), WireFault> {
    let scans = &gnitz_wire::multi_frame::decode_scan_multi(body).map_err(decode_fault)?;
    // Every target's kind is resolved under this guard, so a DDL during the
    // drain is caught there and an unknown tid is rejected there.
    let catalog = fresh_read_lock(
        shared,
        scans.iter().map(|s| s.target.tid),
        shared.dispatcher.reactor().now(),
    )
    .await?;
    let mut scanned: Vec<Scanned> = Vec::with_capacity(scans.len());
    for scan in scans {
        let cat = shared.cat();
        let kind = target_kind(&cat, scan.target, Access::Read)?;
        let (local, lsn) = match kind {
            RelationKind::SystemCatalog => {
                let spec = ReadSpec::decode(scan.spec).map_err(decode_fault)?;
                let rows = cat.registry.scan_spec(scan.target.tid, spec, scan.reply_layout, None)?;
                // Read off the master's own copy, under no SAL hold. The
                // master applies every zone in the step that publishes it, so
                // every zone at or below the SAL watermark is in this copy.
                (Some(rows), shared.dispatcher.sal().watermark())
            }
            _ => (None, 0),
        };
        scanned.push(Scanned { kind, local, lsn });
    }
    // The lease the fanned scans are answered on.
    let mut lease: Option<TrainLease> = None;
    if scanned.iter().any(|s| s.local.is_none()) {
        let fanned = scans.iter().zip(&mut scanned).filter(|(_, s)| s.local.is_none());
        let cut = shared.dispatcher.scan_cut(|cut| {
            for (scan, s) in fanned {
                s.lsn = cut.watermark(s.kind);
                cut.read(Read::ScanSpec {
                    tid: scan.target.tid,
                    reply_layout: scan.reply_layout,
                    spec: scan.spec,
                })?;
            }
            Ok(())
        });
        lease = Some(cut.await?);
    }
    // The replies read no catalog state, and a slow client's egress must not
    // hold the catalog read lock.
    drop(catalog);

    for (scan, s) in scans.iter().zip(&scanned) {
        let target_id = scan.target.tid;
        match &s.local {
            Some(rows) => cork_train(peer, target_id, rows)?,
            // This relation's train, all workers, before the next: each
            // worker sends a lease's trains in request order.
            None => {
                let lease = lease.as_mut().expect("a fanned scan's cut has a lease");
                let train = lease.next_train().await;
                forward_scan(peer, train).await?
            }
        }
        cork_ack(peer, target_id, s.lsn);
        // Carry no more than the budget into the next reply, and learn here
        // rather than at the end if the client is gone.
        if peer.flush_if_full().await.is_err() {
            return Ok(());
        }
    }
    Ok(())
}
