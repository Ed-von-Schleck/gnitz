//! One group as the writer encodes it: the request its header carries, plus
//! the rows or blob each worker is sent.

use super::format::{PayloadRows, RequestHead, payload_size};
use super::request::{Apply, Ingest, Read};
use crate::catalog::{SysFamily, SysFamilyExt};
use crate::runtime::park::WorkerSet;
use crate::runtime::w2m::RequestId;
use gnitz_store::relation::PkRule;
use gnitz_wire::MAX_WORKERS;
use gnitz_zset::repr::{Batch, WireRows};

/// What a push sends each worker.
#[derive(Clone, Copy)]
pub(crate) enum Routed<'a> {
    /// Every worker is sent these rows.
    Same(Option<WireRows<'a>>),
    /// Worker `w` is sent the rows `each[w]`; one entry per worker.
    Each(&'a [Option<WireRows<'a>>]),
}

impl Routed<'_> {
    /// The workers these rows are for; every worker for the one payload all share.
    fn holders(&self) -> WorkerSet {
        match *self {
            Self::Same(_) => WorkerSet::ALL,
            Self::Each(each) => each
                .iter()
                .enumerate()
                .filter_map(|(w, rows)| rows.map(|_| w))
                .collect(),
        }
    }
}

/// What a group sends each worker.
#[derive(Clone, Copy)]
pub(super) enum Payloads<'a> {
    /// One payload, which every addressed worker reads: the request's blob, and
    /// any rows, whose shape record that blob then is.
    Shared(PayloadRows<'a>),
    /// Worker `w` is sent the request's blob, which is the rows' shape record,
    /// and the rows `each[w]`; one entry per worker.
    EachRows(&'a [Option<WireRows<'a>>]),
    /// Worker `w` is sent no rows and `blobs[w]` in place of the request's blob;
    /// one entry per worker.
    EachBlob(&'a [Vec<u8>]),
}

/// One SAL group as the writer's `lay_out` encodes it: the scalars and blob of
/// the request its header carries, plus what varies by worker. Built by the
/// constructors below, one per shape a group takes.
pub(super) struct Group<'a> {
    pub(super) request: RequestHead,
    /// The request's blob, which a worker is sent unless it is sent its own.
    pub(super) blob: &'a [u8],
    pub(super) data: Payloads<'a>,
    /// The workers the group addresses.
    pub(super) to: WorkerSet,
    /// The request id a read's reply train carries.
    pub(super) reply: Option<RequestId>,
}

impl<'a> Group<'a> {
    /// `read` sent to `to`, answered on `reply`; with `blobs`, worker `w` is
    /// sent `blobs[w]` in place of the read's own blob.
    pub(super) fn read(read: &'a Read<'_>, to: WorkerSet, blobs: Option<&'a [Vec<u8>]>, reply: RequestId) -> Self {
        let (request, blob) = read.encode();
        Group {
            request,
            blob,
            data: blobs.map_or(Payloads::Shared(None), Payloads::EachBlob),
            to,
            reply: Some(reply),
        }
    }

    /// `apply` sent to every worker with no rows.
    pub(super) fn apply(apply: &'a Apply<'_>) -> Self {
        let (request, blob) = apply.encode();
        Group {
            request,
            blob,
            data: Payloads::Shared(None),
            to: WorkerSet::ALL,
            reply: None,
        }
    }

    /// A push of `rows` into `tid`, held to `rule`, whose shape record is
    /// `record` and the group's blob: sent to the workers `rows` holds rows for,
    /// answered by none.
    pub(super) fn push(tid: u64, rule: PkRule, record: &'a [u8], rows: Routed<'a>) -> Self {
        Group {
            request: Ingest::Push { tid, rule }.encode(),
            blob: record,
            data: match rows {
                Routed::Same(d) => Payloads::Shared(d),
                Routed::Each(each) => Payloads::EachRows(each),
            },
            to: rows.holders(),
            reply: None,
        }
    }

    /// A catalog mutation of `family`: `batch`, sent whole to every worker under
    /// the family's shape record as the blob. A family that reaches no worker is
    /// logged and addresses none.
    pub(super) fn ddl_sync(family: SysFamily, batch: &'a Batch) -> Self {
        Group {
            request: Ingest::DdlSync { family }.encode(),
            blob: family.shape_record(),
            data: Payloads::Shared(batch.wire_whole()),
            to: if family.master_only() {
                WorkerSet::EMPTY
            } else {
                WorkerSet::ALL
            },
            reply: None,
        }
    }

    /// Whether the group holds one payload, which every addressed worker reads.
    pub(super) fn shared(&self) -> bool {
        matches!(self.data, Payloads::Shared(_))
    }

    /// The workers a payload of the group is written for at width `nw`, in
    /// directory order, with each one's size stored at its index in `sizes`: a
    /// shared group carries one payload, which [`Self::payload`] builds alike for
    /// every worker; any other carries one per addressed worker.
    ///
    /// Fills the caller's array: returned by value, it is copied out with a
    /// `memmove` call per group. Always inlined: the writer sizes every group
    /// through it, and as a call it costs more instructions per group.
    #[inline(always)]
    pub(super) fn sizes(&self, nw: usize, sizes: &mut [usize; MAX_WORKERS]) -> WorkerSet {
        let per_worker = match self.data {
            Payloads::Shared(_) => nw,
            Payloads::EachRows(each) => each.len(),
            Payloads::EachBlob(blobs) => blobs.len(),
        };
        assert_eq!(per_worker, nw, "per-worker entries={per_worker} != num_workers={nw}");
        let payloads = if self.shared() {
            WorkerSet::one(0)
        } else {
            self.to.within(nw)
        };
        for (i, w) in payloads.iter().enumerate() {
            let (blob, rows) = self.payload(w);
            sizes[i] = payload_size(blob, rows);
        }
        payloads
    }

    /// Worker `w`'s payload: the blob it is sent, and its rows. The one
    /// definition — sizing and encoding both go through it, so a payload's size
    /// and its bytes cannot disagree.
    #[inline]
    pub(super) fn payload(&self, w: usize) -> (&[u8], PayloadRows<'_>) {
        match self.data {
            Payloads::Shared(rows) => (self.blob, rows),
            Payloads::EachRows(each) => (self.blob, each[w]),
            Payloads::EachBlob(blobs) => (&blobs[w][..], None),
        }
    }
}
