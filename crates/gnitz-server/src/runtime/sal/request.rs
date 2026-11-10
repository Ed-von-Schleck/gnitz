//! What a SAL group asks of a worker, typed by who answers it: a [`Read`] by a
//! reply train, an [`Apply`] by an ACK, an [`Ingest`] by nobody. The master
//! names one over its bytes, a worker decodes one that borrows the SAL's, and
//! each `encode` and [`SalRequest::decode`] are the one mapping between its
//! fields and a group's header scalars and blob.

use std::borrow::Cow;
use std::num::NonZeroU64;

use super::format::{RequestHead, SalMessageKind};
use crate::catalog::SysFamily;
use gnitz_store::read::Probe;
use gnitz_store::relation::PkRule;
use gnitz_wire::{PkColList, PkListRole};

/// A request answered with a reply train; its failure is a fault frame to the
/// asker, so it may change only state the asker can retry or release.
///
/// A decoded read borrows its byte fields from the SAL group it came in;
/// [`Read::into_owned`] is the one kept past it.
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
    Backup { dir: Cow<'a, str> },
}

/// A request every worker runs and ACKs once done; a worker that cannot has
/// diverged from the log and fail-stops.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Apply<'a> {
    /// Base round of a checkpoint: flush base and system tables.
    Flush,
    /// Ephemeral round of a checkpoint: flush every view's traces and output
    /// stores, stamped with `generation`.
    FlushEph { generation: u64 },
    /// The fill of each of `views`, in order, from the relations it scans.
    Backfill { views: Cow<'a, [u64]> },
    /// One tick per tid, at consecutive rounds from `first_round`.
    Tick { first_round: u64, tids: Cow<'a, [u64]> },
}

/// The rows a group carries into a relation, answered by nobody: a worker
/// that cannot take them has diverged from the log and fail-stops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Ingest {
    /// Catalog rows of `family`.
    DdlSync { family: SysFamily },
    /// Rows of `tid`, a base table's held to `rule`.
    Push { tid: u64, rule: PkRule },
}

/// A decoded request: a read borrows its group's bytes, the others own what
/// they carry.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum SalRequest<'r> {
    Read(Read<'r>),
    Apply(Apply<'static>),
    Ingest(Ingest),
}

impl Apply<'_> {
    /// The scalars the apply's group header carries, and the blob each payload
    /// of the group does: the inverse of [`SalRequest::decode`].
    pub(super) fn encode(&self) -> (RequestHead, &[u8]) {
        use SalMessageKind as K;
        let head = RequestHead::new;
        let none: &[u8] = &[];
        match *self {
            Apply::Flush => (head(K::Flush, 0, 0, 0), none),
            Apply::FlushEph { generation } => (head(K::FlushEph, 0, generation, 0), none),
            Apply::Backfill { ref views } => (head(K::Backfill, 0, 0, 0), gnitz_wire::as_le_bytes(views)),
            Apply::Tick { first_round, ref tids } => (head(K::Tick, 0, first_round, 0), gnitz_wire::as_le_bytes(tids)),
        }
    }
}

impl Ingest {
    /// The scalars the group's header carries; it has no blob. The inverse of
    /// [`SalRequest::decode`].
    pub(super) fn encode(self) -> RequestHead {
        use SalMessageKind as K;
        match self {
            Ingest::DdlSync { family } => RequestHead::new(K::DdlSync, family.id(), 0, 0),
            Ingest::Push { tid, rule } => RequestHead::new(K::Push, tid, u64::from(rule == PkRule::Fresh), 0),
        }
    }
}

impl Read<'_> {
    /// The scalars the read's group header carries, and the blob each payload
    /// of the group does unless a worker is sent its own: the inverse of
    /// [`SalRequest::decode`].
    pub(super) fn encode(&self) -> (RequestHead, &[u8]) {
        use SalMessageKind as K;
        let head = RequestHead::new;
        let none: &[u8] = &[];
        match *self {
            Read::HasPk { tid, probe, ref keys } => (
                match probe {
                    Probe::Pk => head(K::HasPk, tid, 0, 0),
                    Probe::PkColumn(col) => head(K::HasPkColumn, tid, col.into(), 0),
                    Probe::Index(cols, cap) => head(K::HasPkIndex, tid, cap.get(), cols.pack()),
                },
                keys,
            ),
            Read::KeySpans { tid, cols, claim } => (head(K::KeySpans, tid, claim.into(), cols.pack()), none),
            Read::ReleasePreflight => (head(K::ReleasePreflight, 0, 0, 0), none),
            Read::ScanSpec { tid, reply_layout, ref spec } => (head(K::ScanSpec, tid, reply_layout, 0), spec),
            Read::Delta { view, after_tick, reply_layout, ref spec } => {
                (head(K::Delta, view, reply_layout, after_tick), spec)
            }
            Read::Pin { snapshot } => (head(K::Pin, 0, snapshot, 0), none),
            Read::Unpin { snapshot } => (head(K::Unpin, 0, snapshot, 0), none),
            Read::SnapshotPage { snapshot, tid, ref page } => (head(K::SnapshotPage, tid, snapshot, 0), page),
            Read::Backup { ref dir } => (head(K::Backup, 0, 0, 0), dir.as_bytes()),
        }
    }

    /// The name of the kind the read is logged under.
    pub(crate) fn name(&self) -> &'static str {
        self.encode().0.kind.variant_name()
    }
}

impl<'r> SalRequest<'r> {
    /// The inverse of [`Read::encode`], [`Apply::encode`] and [`Ingest::encode`].
    /// A read borrows what it reads of `blob`; the tid lists of `Backfill` and
    /// `Tick` are owned, since a drive iterates them while it reads later groups.
    pub(super) fn decode(head: &RequestHead, blob: &'r [u8]) -> Result<Self, String> {
        let RequestHead { kind, target_id: tid, arg0, arg1 } = *head;
        Ok(match kind {
            SalMessageKind::Flush => Apply::Flush.into(),
            SalMessageKind::FlushEph => Apply::FlushEph { generation: arg0 }.into(),
            SalMessageKind::DdlSync => Ingest::DdlSync {
                family: SysFamily::from_id(tid).ok_or_else(|| format!("{tid} is not a system family"))?,
            }
            .into(),
            SalMessageKind::Backfill => Apply::Backfill {
                views: gnitz_wire::le_scalars(blob)?.into(),
            }
            .into(),
            SalMessageKind::Push => Ingest::Push {
                tid,
                rule: match gnitz_wire::bool_word(arg0)? {
                    true => PkRule::Fresh,
                    false => PkRule::Enforce,
                },
            }
            .into(),
            SalMessageKind::Tick => Apply::Tick {
                first_round: arg0,
                tids: gnitz_wire::le_scalars(blob)?.into(),
            }
            .into(),
            SalMessageKind::HasPk | SalMessageKind::HasPkColumn | SalMessageKind::HasPkIndex => {
                let refused = || format!("no probe is carried as ({}, {:#x})", arg0, arg1);
                let probe = match (kind, arg0, arg1) {
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
                cols: PkColList::unpack_as(arg1, PkListRole::ColumnList)?,
                claim: gnitz_wire::bool_word(arg0)?,
            }
            .into(),
            SalMessageKind::ReleasePreflight => Read::ReleasePreflight.into(),
            SalMessageKind::ScanSpec => Read::ScanSpec {
                tid,
                reply_layout: arg0,
                spec: Cow::Borrowed(blob),
            }
            .into(),
            SalMessageKind::Delta => Read::Delta {
                view: tid,
                after_tick: arg1,
                reply_layout: arg0,
                spec: Cow::Borrowed(blob),
            }
            .into(),
            SalMessageKind::Pin => Read::Pin { snapshot: arg0 }.into(),
            SalMessageKind::Unpin => Read::Unpin { snapshot: arg0 }.into(),
            SalMessageKind::SnapshotPage => Read::SnapshotPage {
                snapshot: arg0,
                tid,
                page: Cow::Borrowed(blob),
            }
            .into(),
            SalMessageKind::Backup => Read::Backup {
                dir: Cow::Borrowed(std::str::from_utf8(blob).map_err(|_| "a backup directory that is not UTF-8")?),
            }
            .into(),
        })
    }
}

impl Read<'_> {
    /// The same read, owning its bytes.
    pub(crate) fn into_owned(self) -> Read<'static> {
        let own = |c: Cow<'_, [u8]>| Cow::Owned(c.into_owned());
        match self {
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
            Read::Backup { dir } => Read::Backup { dir: Cow::Owned(dir.into_owned()) },
        }
    }
}

impl<'r> From<Read<'r>> for SalRequest<'r> {
    fn from(read: Read<'r>) -> Self {
        SalRequest::Read(read)
    }
}

impl From<Apply<'static>> for SalRequest<'_> {
    fn from(apply: Apply<'static>) -> Self {
        SalRequest::Apply(apply)
    }
}

impl From<Ingest> for SalRequest<'_> {
    fn from(ingest: Ingest) -> Self {
        SalRequest::Ingest(ingest)
    }
}

#[cfg(test)]
#[path = "tests/request.rs"]
mod tests;
