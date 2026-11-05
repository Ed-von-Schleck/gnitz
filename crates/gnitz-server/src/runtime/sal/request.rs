//! What a SAL group asks of a worker, typed: the master builds a [`SalRequest`]
//! over its bytes, a worker decodes one that borrows the SAL's, and
//! [`SalRequest::encode`] and [`SalRequest::decode`] are the one mapping between
//! its fields and a frame head.

use std::borrow::Cow;
use std::num::NonZeroU64;

use super::SalMessageKind;
use crate::catalog::SysFamily;
use crate::runtime::frame::WireMsg;
use gnitz_store::read::Probe;
use gnitz_store::relation::PkRule;
use gnitz_wire::{ControlHeader, PkColList, PkListRole};

/// A request answered with a reply train; its failure is a fault frame to the
/// asker, so it may change only state the asker can retry or release.
///
/// A decoded read borrows its byte fields from the SAL group it came in; whoever
/// keeps one past that group's dispatch takes [`Read::into_owned`] first.
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
    /// The rows the group carries, a base table's held to `rule`.
    Push { tid: u64, rule: PkRule },
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
    /// The kind byte of the request's group, the target its group head carries,
    /// and the frame head every payload of the group shares: the inverse of
    /// [`Self::decode`].
    pub(crate) fn encode(&self) -> (SalMessageKind, u64, WireMsg<'_>) {
        use SalMessageKind as K;
        let head = WireMsg::default();
        match *self {
            SalRequest::Shutdown => (K::Shutdown, 0, head),
            SalRequest::Apply(Apply::Flush) => (K::Flush, 0, head),
            SalRequest::Apply(Apply::FlushEph { generation }) => (K::FlushEph, 0, WireMsg { arg0: generation, ..head }),
            SalRequest::Apply(Apply::DdlSync { family }) => (K::DdlSync, family.id(), head),
            SalRequest::Apply(Apply::Push { tid, rule }) => (
                K::Push,
                tid,
                WireMsg {
                    arg0: u64::from(rule == PkRule::Fresh),
                    ..head
                },
            ),
            SalRequest::Apply(Apply::Backfill { ref views }) => (
                K::Backfill,
                0,
                WireMsg {
                    blob: gnitz_wire::as_le_bytes(views),
                    ..head
                },
            ),
            SalRequest::Apply(Apply::Tick { first_round, ref tids }) => (
                K::Tick,
                0,
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
                (kind, tid, WireMsg { arg0, arg1, blob: keys, ..head })
            }
            SalRequest::Read(Read::KeySpans { tid, cols, claim }) => (
                K::KeySpans,
                tid,
                WireMsg {
                    arg0: claim.into(),
                    arg1: cols.pack(),
                    ..head
                },
            ),
            SalRequest::Read(Read::ReleasePreflight) => (K::ReleasePreflight, 0, head),
            SalRequest::Read(Read::ScanSpec { tid, reply_layout, ref spec }) => {
                (K::ScanSpec, tid, WireMsg { arg0: reply_layout, blob: spec, ..head })
            }
            SalRequest::Read(Read::Delta { view, after_tick, reply_layout, ref spec }) => (
                K::Delta,
                view,
                WireMsg {
                    arg0: reply_layout,
                    arg1: after_tick,
                    blob: spec,
                    ..head
                },
            ),
            SalRequest::Read(Read::Pin { snapshot }) => (K::Pin, 0, WireMsg { arg0: snapshot, ..head }),
            SalRequest::Read(Read::Unpin { snapshot }) => (K::Unpin, 0, WireMsg { arg0: snapshot, ..head }),
            SalRequest::Read(Read::SnapshotPage { snapshot, tid, ref page }) => {
                (K::SnapshotPage, tid, WireMsg { arg0: snapshot, blob: page, ..head })
            }
            SalRequest::Read(Read::Backup { ref dir }) => (K::Backup, 0, WireMsg { blob: dir, ..head }),
        }
    }
}

impl<'a> SalRequest<'a> {
    /// The inverse of [`Self::encode`]. A read borrows what it reads of `blob`;
    /// the tid lists of `Backfill` and `Tick` are owned, since a drive iterates
    /// them while it reads later groups, a `Flush` among them.
    pub(super) fn decode(
        kind: SalMessageKind,
        tid: u64,
        hdr: &ControlHeader,
        blob: &'a [u8],
    ) -> Result<SalRequest<'a>, String> {
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
            SalMessageKind::Push => Apply::Push {
                tid,
                rule: match gnitz_wire::bool_word(hdr.arg0)? {
                    true => PkRule::Fresh,
                    false => PkRule::Enforce,
                },
            }
            .into(),
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
                Read::HasPk { tid, probe, keys: Cow::Borrowed(blob) }.into()
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
                spec: Cow::Borrowed(blob),
            }
            .into(),
            SalMessageKind::Delta => Read::Delta {
                view: tid,
                after_tick: hdr.arg1,
                reply_layout: hdr.arg0,
                spec: Cow::Borrowed(blob),
            }
            .into(),
            SalMessageKind::Pin => Read::Pin { snapshot: hdr.arg0 }.into(),
            SalMessageKind::Unpin => Read::Unpin { snapshot: hdr.arg0 }.into(),
            SalMessageKind::SnapshotPage => Read::SnapshotPage {
                snapshot: hdr.arg0,
                tid,
                page: Cow::Borrowed(blob),
            }
            .into(),
            SalMessageKind::Backup => Read::Backup { dir: Cow::Borrowed(blob) }.into(),
        })
    }
}

/// A read that owns its bytes, so it may outlive the SAL group it was decoded
/// from. Only [`Read::into_owned`] builds one: a SAL read is handed out as
/// `Read<'static>` while it borrows the group, so the lifetime alone cannot
/// say that it owns them.
#[derive(Debug)]
pub(crate) struct OwnedRead(Read<'static>);

impl std::ops::Deref for OwnedRead {
    type Target = Read<'static>;

    fn deref(&self) -> &Read<'static> {
        &self.0
    }
}

impl Read<'_> {
    /// The same read, owning its bytes.
    pub(crate) fn into_owned(self) -> OwnedRead {
        let own = |c: Cow<'_, [u8]>| Cow::Owned(c.into_owned());
        OwnedRead(match self {
            Read::HasPk { tid, probe, keys } => Read::HasPk { tid, probe, keys: own(keys) },
            Read::KeySpans { tid, cols, claim } => Read::KeySpans { tid, cols, claim },
            Read::ReleasePreflight => Read::ReleasePreflight,
            Read::ScanSpec { tid, reply_layout, spec } => Read::ScanSpec { tid, reply_layout, spec: own(spec) },
            Read::Delta { view, after_tick, reply_layout, spec } => Read::Delta {
                view,
                after_tick,
                reply_layout,
                spec: own(spec),
            },
            Read::Pin { snapshot } => Read::Pin { snapshot },
            Read::Unpin { snapshot } => Read::Unpin { snapshot },
            Read::SnapshotPage { snapshot, tid, page } => Read::SnapshotPage { snapshot, tid, page: own(page) },
            Read::Backup { dir } => Read::Backup { dir: own(dir) },
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
