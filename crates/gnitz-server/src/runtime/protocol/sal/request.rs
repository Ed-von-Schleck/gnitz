//! What a SAL group asks of a worker, typed: the master builds a [`SalRequest`]
//! borrowed, a worker decodes one owned, and [`SalRequest::template`] and
//! [`SalRequest::decode`] are the one mapping between its fields and a frame head.

use std::borrow::Cow;

use super::SalMessageKind;
use crate::runtime::wire::WireMsg;
use gnitz_wire::control::ControlHeader;
use gnitz_wire::{PkColList, PkListRole, Probe, WireFlags};

/// A request answered with rows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Read<'a> {
    /// `probe` at the keys the group carries.
    HasPk { tid: u64, probe: Probe },
    /// The sorted spans of `cols` over `tid`.
    KeySpans { tid: u64, cols: PkColList },
    /// The encoded `ReadSpec` `spec`, in the layout whose digest is `reply_layout`.
    ScanSpec {
        tid: u64,
        reply_layout: u64,
        spec: Cow<'a, [u8]>,
    },
    /// `view`'s deltas in rounds `(after_tick, cut_round]`.
    Delta {
        view: u64,
        after_tick: u64,
        cut_round: u64,
        /// The encoded `ReadSpec` the view is read under, then the reply
        /// layout's digest. The spec leads, so the read is routed by
        /// its bound as a `ScanSpec` is; [`Read::delta`] builds it and
        /// [`Read::delta_parts`] reads it.
        read: Cow<'a, [u8]>,
    },
}

/// A request that changes this worker's state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Apply<'a> {
    /// Base round of a checkpoint: flush base and system tables.
    Flush,
    /// Ephemeral round of a checkpoint: flush every view's traces and output
    /// stores, stamped with `generation`.
    FlushEph { generation: u64 },
    /// The catalog rows the group carries.
    DdlSync { family: u64 },
    /// The fill of each of `views`, in order, from the relations it scans.
    Backfill { views: Cow<'a, [u64]> },
    /// The rows the group carries.
    Push { tid: u64 },
    /// One tick per tid, at consecutive rounds from `first_round`.
    Tick { first_round: u64, tids: Cow<'a, [u64]> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SalRequest<'a> {
    Read(Read<'a>),
    Apply(Apply<'a>),
    Shutdown,
}

impl Read<'_> {
    /// `view`'s deltas in rounds `(after_tick, cut_round]`, under the encoded
    /// `spec`, in the layout whose digest is `reply_layout`.
    pub(crate) fn delta(view: u64, after_tick: u64, cut_round: u64, spec: &[u8], reply_layout: u64) -> Read<'static> {
        let read = [spec, &reply_layout.to_le_bytes()].concat();
        Read::Delta {
            view,
            after_tick,
            cut_round,
            read: read.into(),
        }
    }

    /// A [`Read::Delta`]'s encoded spec and its reply layout's digest.
    pub(crate) fn delta_parts(read: &[u8]) -> (&[u8], u64) {
        let (spec, layout) = read.split_last_chunk().expect("a delta read ends in its reply layout");
        (spec, u64::from_le_bytes(*layout))
    }

    /// The relation it reads.
    pub(crate) fn target(&self) -> u64 {
        match *self {
            Read::HasPk { tid, .. } | Read::KeySpans { tid, .. } | Read::ScanSpec { tid, .. } => tid,
            Read::Delta { view, .. } => view,
        }
    }
}

impl SalRequest<'_> {
    pub(crate) fn kind(&self) -> SalMessageKind {
        match self {
            SalRequest::Shutdown => SalMessageKind::Shutdown,
            SalRequest::Read(Read::HasPk { .. }) => SalMessageKind::HasPk,
            SalRequest::Read(Read::KeySpans { .. }) => SalMessageKind::KeySpans,
            SalRequest::Read(Read::ScanSpec { .. }) => SalMessageKind::ScanSpec,
            SalRequest::Read(Read::Delta { .. }) => SalMessageKind::DeltaRead,
            SalRequest::Apply(Apply::Flush) => SalMessageKind::Flush,
            SalRequest::Apply(Apply::FlushEph { .. }) => SalMessageKind::FlushEph,
            SalRequest::Apply(Apply::DdlSync { .. }) => SalMessageKind::DdlSync,
            SalRequest::Apply(Apply::Backfill { .. }) => SalMessageKind::Backfill,
            SalRequest::Apply(Apply::Push { .. }) => SalMessageKind::Push,
            SalRequest::Apply(Apply::Tick { .. }) => SalMessageKind::Tick,
        }
    }

    /// The frame head every payload of the request's group shares.
    pub(crate) fn template(&self) -> WireMsg<'_> {
        let head = WireMsg::default();
        match *self {
            SalRequest::Shutdown | SalRequest::Apply(Apply::Flush) => head,
            SalRequest::Apply(Apply::FlushEph { generation }) => WireMsg { arg0: generation, ..head },
            SalRequest::Apply(Apply::DdlSync { family: target_id } | Apply::Push { tid: target_id }) => {
                WireMsg { target_id, ..head }
            }
            SalRequest::Apply(Apply::Backfill { ref views }) => WireMsg {
                blob: gnitz_wire::as_le_bytes(views),
                ..head
            },
            SalRequest::Apply(Apply::Tick { first_round, ref tids }) => WireMsg {
                arg0: first_round,
                blob: gnitz_wire::as_le_bytes(tids),
                ..head
            },
            SalRequest::Read(Read::HasPk { tid, probe }) => {
                let (probe_mode, arg0, arg1) = probe.wire();
                WireMsg {
                    target_id: tid,
                    arg0,
                    arg1,
                    flags: WireFlags { probe_mode, ..Default::default() },
                    ..head
                }
            }
            SalRequest::Read(Read::KeySpans { tid, cols }) => WireMsg {
                target_id: tid,
                arg1: cols.pack(),
                ..head
            },
            SalRequest::Read(Read::ScanSpec { tid, reply_layout, ref spec }) => WireMsg {
                target_id: tid,
                arg0: reply_layout,
                blob: spec,
                ..head
            },
            SalRequest::Read(Read::Delta { view, after_tick, cut_round, ref read }) => WireMsg {
                target_id: view,
                arg0: cut_round,
                arg1: after_tick,
                blob: read,
                ..head
            },
        }
    }
}

impl SalRequest<'static> {
    /// The inverse of [`Self::kind`] + [`Self::template`], owning what it read
    /// of `blob`: a worker holds its request past the SAL bytes it came in.
    pub(super) fn decode(kind: SalMessageKind, hdr: &ControlHeader, blob: &[u8]) -> Result<Self, String> {
        let tid = hdr.target_id;
        Ok(match kind {
            SalMessageKind::Shutdown => SalRequest::Shutdown,
            SalMessageKind::Flush => Apply::Flush.into(),
            SalMessageKind::FlushEph => Apply::FlushEph { generation: hdr.arg0 }.into(),
            SalMessageKind::DdlSync => Apply::DdlSync { family: tid }.into(),
            SalMessageKind::Backfill => Apply::Backfill { views: ids_of(blob, "backfill")?.into() }.into(),
            SalMessageKind::Push => Apply::Push { tid }.into(),
            SalMessageKind::Tick => Apply::Tick {
                first_round: hdr.arg0,
                tids: ids_of(blob, "tick")?.into(),
            }
            .into(),
            SalMessageKind::HasPk => {
                let probe =
                    Probe::from_wire(hdr.flags.probe_mode, hdr.arg0, hdr.arg1).map_err(|e| format!("has_pk: {e}"))?;
                Read::HasPk { tid, probe }.into()
            }
            SalMessageKind::KeySpans => {
                let cols = PkColList::unpack(hdr.arg1)
                    .map_err(|e| format!("key spans of table {tid}: {}", e.for_role(PkListRole::ColumnList)))?;
                Read::KeySpans { tid, cols }.into()
            }
            SalMessageKind::ScanSpec => Read::ScanSpec {
                tid,
                reply_layout: hdr.arg0,
                spec: blob.to_vec().into(),
            }
            .into(),
            SalMessageKind::DeltaRead => {
                if blob.len() < size_of::<u64>() {
                    return Err("delta read: the blob holds no reply layout".into());
                }
                Read::Delta {
                    view: tid,
                    after_tick: hdr.arg1,
                    cut_round: hdr.arg0,
                    read: blob.to_vec().into(),
                }
                .into()
            }
        })
    }
}

/// The ids a `what` group's blob lists.
fn ids_of(blob: &[u8], what: &str) -> Result<Vec<u64>, String> {
    if !blob.len().is_multiple_of(8) {
        return Err(format!("{what}: the blob is not whole ids"));
    }
    let mut ids = Vec::new();
    gnitz_wire::extend_from_le_bytes(&mut ids, blob);
    Ok(ids)
}

impl<'a> From<Read<'a>> for SalRequest<'a> {
    fn from(read: Read<'a>) -> Self {
        SalRequest::Read(read)
    }
}

impl<'a> From<Apply<'a>> for SalRequest<'a> {
    fn from(apply: Apply<'a>) -> Self {
        SalRequest::Apply(apply)
    }
}

#[cfg(test)]
#[path = "tests/request.rs"]
mod tests;
