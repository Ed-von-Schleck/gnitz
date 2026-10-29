//! What a SAL group asks of a worker, typed: the master builds a [`SalRequest`]
//! borrowed, a worker decodes one owned, and [`SalRequest::encode`] and
//! [`SalRequest::decode`] are the one mapping between its fields and a frame head.

use std::borrow::Cow;
use std::num::NonZeroU64;

use super::SalMessageKind;
use crate::catalog::SysFamily;
use crate::runtime::frame::WireMsg;
use gnitz_store::read::Probe;
use gnitz_wire::{ControlHeader, PkColList, PkListRole};

/// A request answered with a reply train; its failure is a fault frame to the
/// asker, so it may change only state the asker can retry or release.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Read<'a> {
    /// `probe` at `keys`, encoded by [`gnitz_wire::PkKeys::encode_probe`].
    HasPk {
        tid: u64,
        probe: Probe,
        keys: Cow<'a, [u8]>,
    },
    /// The sorted spans of `cols` over `tid`, off its index on `cols`. With
    /// `claim`, the `CREATE UNIQUE INDEX` pre-flight's: that index is first
    /// entered under the pre-flight's claim, filled from `tid`'s rows if absent.
    KeySpans { tid: u64, cols: PkColList, claim: bool },
    /// Release every claim the pre-flight holds.
    ReleasePreflight,
    /// The encoded `ReadSpec` `spec`, in the layout whose digest is `reply_layout`.
    ScanSpec {
        tid: u64,
        reply_layout: u64,
        spec: Cow<'a, [u8]>,
    },
    /// `view`'s deltas in the rounds after `after_tick`, under the encoded
    /// `ReadSpec` `spec`, in the layout whose digest is `reply_layout`.
    Delta {
        view: u64,
        after_tick: u64,
        reply_layout: u64,
        spec: Cow<'a, [u8]>,
    },
    /// Pin every base table's store as snapshot `snapshot`, here in the log.
    Pin { snapshot: u64 },
    /// Release snapshot `snapshot`.
    Unpin { snapshot: u64 },
    /// A page of `tid` as `snapshot` pinned it: the encoded
    /// `gnitz_wire::snapshot::PageRequest` `page` names its rows.
    SnapshotPage {
        snapshot: u64,
        tid: u64,
        page: Cow<'a, [u8]>,
    },
    /// Link every store this process published into the data directory `dir`.
    Backup { dir: Cow<'a, [u8]> },
}

/// A request ACKed once applied; a worker that cannot apply it has diverged
/// from the log and fail-stops.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Apply<'a> {
    /// Base round of a checkpoint: flush base and system tables.
    Flush,
    /// Ephemeral round of a checkpoint: flush every view's traces and output
    /// stores, stamped with `generation`.
    FlushEph { generation: u64 },
    /// The catalog rows the group carries.
    DdlSync { family: SysFamily },
    /// The fill of each of `views`, in order, from the relations it scans.
    Backfill { views: Cow<'a, [u64]> },
    /// The rows the group carries.
    Push { tid: u64 },
    /// One tick per tid, at consecutive rounds from `first_round`.
    Tick { first_round: u64, tids: Cow<'a, [u64]> },
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SalRequest<'a> {
    Read(Read<'a>),
    Apply(Apply<'a>),
    Shutdown,
}

impl SalRequest<'_> {
    /// The kind byte of the request's group, and the frame head every payload of
    /// the group shares: the inverse of [`Self::decode`].
    pub(crate) fn encode(&self) -> (SalMessageKind, WireMsg<'_>) {
        use SalMessageKind as K;
        let head = WireMsg::default();
        match *self {
            SalRequest::Shutdown => (K::Shutdown, head),
            SalRequest::Apply(Apply::Flush) => (K::Flush, head),
            SalRequest::Apply(Apply::FlushEph { generation }) => (K::FlushEph, WireMsg { arg0: generation, ..head }),
            SalRequest::Apply(Apply::DdlSync { family }) => (K::DdlSync, WireMsg { target_id: family.id(), ..head }),
            SalRequest::Apply(Apply::Push { tid }) => (K::Push, WireMsg { target_id: tid, ..head }),
            SalRequest::Apply(Apply::Backfill { ref views }) => (
                K::Backfill,
                WireMsg {
                    blob: gnitz_wire::as_le_bytes(views),
                    ..head
                },
            ),
            SalRequest::Apply(Apply::Tick { first_round, ref tids }) => (
                K::Tick,
                WireMsg {
                    arg0: first_round,
                    blob: gnitz_wire::as_le_bytes(tids),
                    ..head
                },
            ),
            SalRequest::Read(Read::HasPk { tid, probe, ref keys }) => {
                let (kind, arg0, arg1) = match probe {
                    Probe::Pk => (K::HasPk, 0, 0),
                    Probe::PkColumn(col) => (K::HasPkColumn, col.into(), 0),
                    Probe::Index(cols, cap) => (K::HasPkIndex, cap.get(), cols.pack()),
                };
                (
                    kind,
                    WireMsg {
                        target_id: tid,
                        arg0,
                        arg1,
                        blob: keys,
                        ..head
                    },
                )
            }
            SalRequest::Read(Read::KeySpans { tid, cols, claim }) => (
                K::KeySpans,
                WireMsg {
                    target_id: tid,
                    arg0: claim.into(),
                    arg1: cols.pack(),
                    ..head
                },
            ),
            SalRequest::Read(Read::ReleasePreflight) => (K::ReleasePreflight, head),
            SalRequest::Read(Read::ScanSpec { tid, reply_layout, ref spec }) => (
                K::ScanSpec,
                WireMsg {
                    target_id: tid,
                    arg0: reply_layout,
                    blob: spec,
                    ..head
                },
            ),
            SalRequest::Read(Read::Delta { view, after_tick, reply_layout, ref spec }) => (
                K::Delta,
                WireMsg {
                    target_id: view,
                    arg0: reply_layout,
                    arg1: after_tick,
                    blob: spec,
                    ..head
                },
            ),
            SalRequest::Read(Read::Pin { snapshot }) => (K::Pin, WireMsg { arg0: snapshot, ..head }),
            SalRequest::Read(Read::Unpin { snapshot }) => (K::Unpin, WireMsg { arg0: snapshot, ..head }),
            SalRequest::Read(Read::SnapshotPage { snapshot, tid, ref page }) => (
                K::SnapshotPage,
                WireMsg {
                    target_id: tid,
                    arg0: snapshot,
                    blob: page,
                    ..head
                },
            ),
            SalRequest::Read(Read::Backup { ref dir }) => (K::Backup, WireMsg { blob: dir, ..head }),
        }
    }
}

impl SalRequest<'static> {
    /// The inverse of [`Self::encode`], owning what it read
    /// of `blob`: a worker holds its request past the SAL bytes it came in.
    pub(super) fn decode(kind: SalMessageKind, hdr: &ControlHeader, blob: &[u8]) -> Result<Self, String> {
        let tid = hdr.target_id;
        Ok(match kind {
            SalMessageKind::Shutdown => SalRequest::Shutdown,
            SalMessageKind::Flush => Apply::Flush.into(),
            SalMessageKind::FlushEph => Apply::FlushEph { generation: hdr.arg0 }.into(),
            SalMessageKind::DdlSync => Apply::DdlSync {
                family: SysFamily::from_id(tid).ok_or_else(|| format!("{tid} is not a system family"))?,
            }
            .into(),
            SalMessageKind::Backfill => Apply::Backfill {
                views: gnitz_wire::le_scalars(blob)?.into(),
            }
            .into(),
            SalMessageKind::Push => Apply::Push { tid }.into(),
            SalMessageKind::Tick => Apply::Tick {
                first_round: hdr.arg0,
                tids: gnitz_wire::le_scalars(blob)?.into(),
            }
            .into(),
            SalMessageKind::HasPk | SalMessageKind::HasPkColumn | SalMessageKind::HasPkIndex => {
                let refused = || format!("no probe is carried as ({}, {:#x})", hdr.arg0, hdr.arg1);
                let probe = match (kind, hdr.arg0, hdr.arg1) {
                    (SalMessageKind::HasPk, 0, 0) => Probe::Pk,
                    (SalMessageKind::HasPkColumn, col, 0) => {
                        Probe::PkColumn(u32::try_from(col).map_err(|_| refused())?)
                    }
                    (SalMessageKind::HasPkIndex, cap, cols) => {
                        let cap = NonZeroU64::new(cap).ok_or_else(refused)?;
                        Probe::Index(PkColList::unpack_as(cols, PkListRole::ColumnList)?, cap)
                    }
                    _ => return Err(refused()),
                };
                Read::HasPk { tid, probe, keys: blob.to_vec().into() }.into()
            }
            SalMessageKind::KeySpans => Read::KeySpans {
                tid,
                cols: PkColList::unpack_as(hdr.arg1, PkListRole::ColumnList)?,
                claim: gnitz_wire::bool_word(hdr.arg0)?,
            }
            .into(),
            SalMessageKind::ReleasePreflight => Read::ReleasePreflight.into(),
            SalMessageKind::ScanSpec => Read::ScanSpec {
                tid,
                reply_layout: hdr.arg0,
                spec: blob.to_vec().into(),
            }
            .into(),
            SalMessageKind::Delta => Read::Delta {
                view: tid,
                after_tick: hdr.arg1,
                reply_layout: hdr.arg0,
                spec: blob.to_vec().into(),
            }
            .into(),
            SalMessageKind::Pin => Read::Pin { snapshot: hdr.arg0 }.into(),
            SalMessageKind::Unpin => Read::Unpin { snapshot: hdr.arg0 }.into(),
            SalMessageKind::SnapshotPage => Read::SnapshotPage {
                snapshot: hdr.arg0,
                tid,
                page: blob.to_vec().into(),
            }
            .into(),
            SalMessageKind::Backup => Read::Backup { dir: blob.to_vec().into() }.into(),
        })
    }
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
