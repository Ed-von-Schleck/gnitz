//! A worker's live drain: the groups addressed to it, in log order, each
//! decoded to the request it carries.

use super::format::{next_epoch, payload_rows, SalMessageKind};
use super::log::{SalLog, SalMessage};
use super::request::{Apply, Ingest, Read, SalRequest};
use crate::runtime::w2m::RequestId;
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

/// A run of consecutive groups in the log: one that starts it, then each read
/// that [`SalExcl::read`](super::writer::SalExcl::read) writes directly behind a
/// read under the same hold, marked as joining it. A worker sends a cut's
/// replies in request order: the master drains a cut's leases in that order, and a W2M ring frees
/// only a released prefix. Replies of different cuts go to different leases,
/// and may pass each other.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct Cut(u64);

#[cfg(test)]
impl Cut {
    /// The `n`th cut a drain passes.
    pub(crate) fn nth(n: u64) -> Cut {
        Cut(n)
    }
}

/// Where the reply to a read goes.
#[derive(Clone, Copy)]
pub(crate) struct ReplyRoute {
    pub(crate) target_id: u64,
    pub(crate) request_id: RequestId,
    /// The cut the read was written in.
    pub(crate) cut: Cut,
}

/// One group addressed to a worker, decoded, by who answers it. A read borrows
/// its group's SAL bytes, and with them the reader, until the next group is
/// read; the others own what they carry.
pub(crate) enum Inbound<'a> {
    /// Answered by a reply train.
    Read { route: ReplyRoute, read: Read<'a> },
    /// Answered by one ACK on `ack`.
    Apply { ack: RequestId, apply: Apply<'static> },
    /// Answered by nobody.
    Ingest {
        ingest: Ingest,
        /// `None` is an empty delta. Boxed, so a request moves from its decode
        /// to where it is served as a few words: held inline, the batch would
        /// be copied whole at each of those moves, for a request that carries
        /// none as well.
        rows: Option<Box<Batch>>,
    },
}

pub(crate) struct SalReader {
    log: SalLog,
    worker_id: u32,
    /// The live drain's position and epoch — the read-side mirror of
    /// `SalWriter`'s `write_cursor` / `epoch`, so no caller does SAL address
    /// arithmetic.
    read_cursor: u64,
    expected_epoch: u32,
    /// The cut of the group [`Self::next`] last returned: how many groups
    /// opening a cut the drain has passed.
    cut: Cut,
}

impl SalReader {
    /// A live drain from cursor 0 of `live_epoch`.
    pub(crate) fn new(log: SalLog, worker_id: u32, live_epoch: u32) -> Self {
        SalReader {
            log,
            worker_id,
            read_cursor: 0,
            expected_epoch: live_epoch,
            cut: Cut(0),
        }
    }

    /// The next group addressed to this worker, with this worker's bytes; groups
    /// addressed to others are stepped over. Stepping past a `Flush`, the last
    /// group of its epoch, moves the drain to the start of the next one; a group
    /// from another epoch parks.
    ///
    /// A published header that fails verification behind a matching prefix epoch
    /// can only be damage, and parking on it would read as end-of-drain, so it
    /// fail-stops.
    fn next(&mut self) -> Option<(SalMessage<'_>, &[u8], Cut)> {
        loop {
            // Before any header byte: the master may rewrite a slot this drain is
            // parked on, so its bytes are unreadable until the prefix proves the
            // slot is this epoch's.
            if self.is_empty() {
                return None;
            }
            let cursor = self.read_cursor;
            let Some(msg) = self.log.group_at(cursor, self.expected_epoch) else {
                gnitz_fatal_abort!("SAL group header at offset={cursor} is corrupt — the log is damaged")
            };
            if msg.head.request.kind == SalMessageKind::Flush {
                self.read_cursor = 0;
                self.expected_epoch = next_epoch(self.expected_epoch);
            } else {
                self.read_cursor = msg.end;
            }
            // Counted whether or not the group addresses this worker: a joining
            // group must not attach to the last cut this worker was addressed in.
            if !msg.head.joins_cut() {
                self.cut.0 += 1;
            }
            if let Some(slot) = msg.slot(self.worker_id) {
                return Some((msg, slot, self.cut));
            }
        }
    }

    /// The next group addressed to this worker. The master authored its head
    /// and framing, so those failing to decode fail-stop, as a corrupt header
    /// does in [`Self::next`]; a blob of client bytes comes back undecoded.
    /// `catalog_schema` answers what a relation's catalog record decodes to.
    #[inline]
    pub(crate) fn next_request(
        &mut self,
        catalog_schema: impl FnOnce(u64, &[u8]) -> Option<SchemaDescriptor>,
    ) -> Option<Inbound<'_>> {
        let (msg, wire, cut) = self.next()?;
        // Fail-stop: a dropped group diverges this worker from the master.
        let undecodable = |e: String| -> ! {
            gnitz_fatal_abort!(
                "failed to decode {:?} for tid={}: {e}",
                msg.head.request.kind,
                msg.head.request.target_id
            )
        };
        // Read in place: the rows move once, out of the decoded payload.
        let mut decoded = payload_rows(wire, |record| catalog_schema(msg.head.request.target_id, record));
        let (blob, rows) = match &mut decoded {
            Ok(payload) => payload,
            Err(e) => undecodable(std::mem::take(e)),
        };
        let what = match SalRequest::decode(&msg.head.request, blob) {
            Ok(what) => what,
            Err(e) => undecodable(e),
        };
        // An ingest carries the group's rows and names no request id; every
        // other group names one and carries none.
        let answered = |rows: bool| -> RequestId {
            if rows {
                undecodable(format!("a {:?} group carries no rows", msg.head.request.kind));
            }
            let Some(id) = msg.head.reply else {
                undecodable(format!("a {:?} group carries no request id", msg.head.request.kind))
            };
            id
        };
        Some(match what {
            SalRequest::Read(read) => {
                let route = ReplyRoute {
                    target_id: msg.head.request.target_id,
                    request_id: answered(rows.is_some()),
                    cut,
                };
                Inbound::Read { route, read }
            }
            SalRequest::Apply(apply) => Inbound::Apply { ack: answered(rows.is_some()), apply },
            SalRequest::Ingest(_) if msg.head.reply.is_some() => {
                undecodable("a group of rows names a request id".into())
            }
            // Moved once, here: the batch is not copied out for a group that carries none.
            SalRequest::Ingest(ingest) => Inbound::Ingest { ingest, rows: rows.take().map(Box::new) },
        })
    }

    /// No group of this epoch is published at the cursor: the prefix word is zero
    /// or carries another epoch — a leftover the master has not rewound over. The
    /// live drain's one prefix test: [`Self::next`] reads no header byte until it
    /// fails. A published header that does not verify reads as non-empty, so
    /// `next` reaches it and fail-stops.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        !self.log.published_at(self.read_cursor, self.expected_epoch)
    }
}

#[cfg(test)]
#[path = "tests/reader.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/reader.rs"]
mod bench;
