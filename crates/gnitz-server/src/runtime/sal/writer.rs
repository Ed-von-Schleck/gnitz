//! The master's end of the log: laying groups out and publishing them, the
//! hold that serializes writers, the publication scope with its atomic zone,
//! LSNs, and when a checkpoint is due.

use std::cell::Cell;
use std::os::fd::AsRawFd;
use std::os::unix::fs::FileExt;
use std::sync::atomic::Ordering;

use super::durable::Durable;
use super::format::{
    anchor_word, dir_sizes, directory, epoch_word, group_header_size, group_total_size, next_epoch, payload_encode,
    payload_spans, GroupHead, Header, SalMessageKind, PAYLOAD_HEAD, PREFIX_BYTES, PRESENT,
};
use super::group::{Group, Routed};
use super::log::{SalLog, ANCHOR_BYTES, MIN_SAL_BYTES};
use super::request::{Apply, Read};
use super::tail::CommittedTail;
use crate::catalog::SysFamily;
use crate::runtime::park::{WorkerParks, WorkerSet};
use crate::runtime::reactor::{Reactor, ReadyAcked};
use crate::runtime::sync::{AsyncRwLock, WriteGuard};
use crate::runtime::w2m::RequestId;
use gnitz_foundation::fault::Seam;
use gnitz_store::relation::PkRule;
use gnitz_wire::{WireFault, WireStatus, MAX_WORKERS};
use gnitz_zset::repr::Batch;

/// `GNITZ_INJECT_SAL_ZONE_PANIC=<ddl|commit>`: crash the master between a zone's
/// groups publishing and its closing member. `commit` aborts a zone a push is
/// a member of, `ddl` one no push is a member of.
static ZONE_PANIC: Seam = Seam::new("GNITZ_INJECT_SAL_ZONE_PANIC");

/// The payload span from which a group's payloads are written with `pwrite`
/// rather than stored through the mapping.
const PAYLOAD_PWRITE_BYTES: usize = 4096;

/// Space only a checkpoint's two rounds — the base round's `Flush`, the
/// ephemeral round's `FlushEph` — and the `Shutdown` broadcast may spend, so
/// those always fit.
const CHECKPOINT_RESERVE: usize = 64 << 10;

const _: () = {
    let control_only = PREFIX_BYTES + group_header_size(1) + PAYLOAD_HEAD;
    assert!(
        CHECKPOINT_RESERVE >= 3 * control_only,
        "the checkpoint band must hold three control-only groups: one epoch can spend it on a FlushEph, a Flush and a Shutdown"
    );
};

/// The highest byte a group of `kind` may occupy: its end prefix stays free,
/// and so does the checkpoint band unless the group is one of the three that
/// must never be refused.
fn group_limit(kind: SalMessageKind, ring_len: usize) -> usize {
    let band = if matches!(
        kind,
        SalMessageKind::Flush | SalMessageKind::FlushEph | SalMessageKind::Shutdown
    ) {
        0
    } else {
        CHECKPOINT_RESERVE
    };
    ring_len - PREFIX_BYTES - band
}

const _: () = assert!(
    PREFIX_BYTES + CHECKPOINT_RESERVE < MIN_SAL_BYTES / 64,
    "the checkpoint reserve must leave ordinary groups nearly all of the smallest SAL"
);

/// The tag of a [`SalWriter`]'s lock.
enum SalLock {}

pub(crate) struct SalWriter {
    log: SalLog,
    /// The end of the published log. A scope's unpublished groups lie past it
    /// until [`SalScope::commit`].
    write_cursor: Cell<u64>,
    epoch: Cell<u32>,
    /// The log position just past the latest scope that published anything; see
    /// [`Self::watermark`].
    watermark: Cell<u64>,
    checkpoint_threshold: u64,
    /// The published end just behind this epoch's latest `FlushEph`, what its
    /// checkpoint left in the log; 0 while the epoch has none. Set where
    /// [`SalExcl::apply`] writes one, cleared by [`Self::rewind`].
    checkpoint_floor: Cell<u64>,
    /// A group a checkpoint may admit ([`Self::checkpoint_may_admit`]) was
    /// refused since the last rewind, so a checkpoint is warranted whatever the
    /// write cursor says. Cleared by [`Self::rewind`].
    refused_transient: Cell<bool>,
    /// The encode buffer of a group whose payloads are written with `pwrite`.
    scratch: Cell<Vec<u8>>,
    /// A zone member pushing rows was published in this epoch, which is since
    /// the last base round. Cleared by [`Self::rewind`].
    holds_pushes: Cell<bool>,
    /// Taken by every [`SalExcl`].
    excl: AsyncRwLock<SalLock>,
    /// Every worker's park; its length is the worker count groups are written at.
    parks: WorkerParks,
    /// How far the log is durable; see [`Self::durable_through`].
    durable: Durable,
}

impl SalWriter {
    /// The writer of the boot that recovered `tail`, rewound to its live epoch.
    pub(crate) fn boot(tail: CommittedTail, parks: WorkerParks, _: ReadyAcked) -> Self {
        Self::rewound(tail.log(), parks, tail.live_epoch())
    }

    /// A writer over `log` rewound to cursor 0 of `epoch`, its watermark below
    /// every zone LSN of `epoch` and above every earlier epoch's.
    fn rewound(log: SalLog, parks: WorkerParks, epoch: u32) -> Self {
        assert!(
            log.ring_len > PREFIX_BYTES + CHECKPOINT_RESERVE,
            "the ring must hold the checkpoint band"
        );
        let checkpoint_threshold =
            gnitz_foundation::env::env_num("GNITZ_CHECKPOINT_BYTES", (log.ring_len as u64 * 3) >> 2);
        let writer = SalWriter {
            log,
            write_cursor: Cell::default(),
            epoch: Cell::default(),
            watermark: Cell::new(epoch_word(epoch, 0)),
            checkpoint_threshold,
            checkpoint_floor: Cell::default(),
            refused_transient: Cell::default(),
            scratch: Cell::default(),
            holds_pushes: Cell::default(),
            excl: AsyncRwLock::default(),
            parks,
            durable: Durable::default(),
        };
        writer.rewind(epoch);
        writer
    }

    /// Sole write access, once no other task holds it.
    pub(crate) async fn lock(&self) -> SalExcl<'_> {
        SalExcl {
            writer: self,
            reached: WorkerSet::EMPTY,
            in_cut: false,
            _guard: self.excl.write().await,
        }
    }

    /// The worker count every group is written at.
    pub(crate) fn num_workers(&self) -> usize {
        self.parks.len()
    }

    /// The log bytes a push of `rows` under the schema record `record` occupies,
    /// wherever [`SalScope::push`] lays it out. Neither its target nor its rule
    /// sizes a group.
    pub(crate) fn push_footprint(&self, record: &[u8], rows: Routed<'_>) -> usize {
        let mut sizes = [0usize; MAX_WORKERS];
        let payloads = Group::push(0, PkRule::Enforce, record, rows).sizes(self.parks.len(), &mut sizes);
        let sizes = &sizes[..payloads.len()];
        group_total_size(group_header_size(sizes.len()), sizes.iter().copied())
    }

    /// Whether a checkpoint may make room for groups refused at `base`, `bytes`
    /// sizing them and `limit` being their kind's [`group_limit`]: `base` lies
    /// past `checkpoint_floor`, and they are within the limit, so something other than
    /// a checkpoint's own groups is ahead of them in the log. A checkpoint
    /// removes that and nothing else; what it leaves ahead in its place is its
    /// own groups, whose size this does not predict, so the answer is "may".
    /// Groups refused directly at the floor — offset 0 in an epoch without an
    /// ephemeral round — have nothing ahead of them a checkpoint would remove.
    /// `bytes` runs only when `base` leaves the question open.
    fn checkpoint_may_admit(&self, limit: usize, base: u64, bytes: impl FnOnce() -> usize) -> bool {
        base > self.checkpoint_floor.get() && bytes() <= limit
    }

    /// Lay one group out at `base`, which is at or past the published end, `fill`
    /// writing the bytes of each payload, named by its directory index, and return the group's
    /// end — the group is complete on the log but for its digest, and
    /// unpublished; [`Self::publish`] does both. Moves no writer position. A
    /// group that does not fit is refused with [`WireStatus::SalFull`] and leaves
    /// the log untouched; the refusal reaches clients verbatim, so `base` goes
    /// to the operator log instead. A zone member checksums its payload bytes.
    fn write_slots(
        &self,
        base: u64,
        head: GroupHead,
        sizes: &[usize],
        mut fill: impl FnMut(usize, &mut [u8]),
    ) -> Result<u64, WireFault> {
        let epoch = self.epoch.get();
        assert!(
            epoch >= 1,
            "SAL group epoch must be >= 1 — epoch 0 is indistinguishable from the empty prefix"
        );
        let width = self.parks.len();
        debug_assert_eq!(sizes.len(), head.payload_count());
        debug_assert_eq!(
            head.targets,
            head.targets.within(width),
            "a group addresses launched workers"
        );

        let hdr_size = group_header_size(sizes.len());
        let total = group_total_size(hdr_size, sizes.iter().copied());
        let cap = group_limit(head.request.kind, self.log.ring_len);
        if total > cap.saturating_sub(base as usize) {
            // On the writer, not on the scope: the group did not fit whether or
            // not a rolled-back transaction contained it.
            if self.checkpoint_may_admit(cap, base, || total) {
                self.refused_transient.set(true);
            }
            gnitz_debug!(
                "SAL group refused: cursor={} mmap={} epoch={} need={} cap={}",
                base,
                self.log.ring_len,
                epoch,
                total,
                cap
            );
            return Err(WireFault {
                status: WireStatus::SalFull,
                text: format!("SAL full: {:?} group did not fit", head.request.kind),
            });
        }

        let hdr_off = base as usize + PREFIX_BYTES;
        // A payload span from `PAYLOAD_PWRITE_BYTES` up is laid out in `scratch`
        // and written with `pwrite`, not stored through the mapping: no page
        // fault per page, and the page cache sizes its pages to the write.
        // `fill` writes every payload byte, and each payload's padding is
        // written as zeros, so the span is a function of the group alone,
        // whatever an earlier group left in `scratch` or at this offset.
        let span = total - PREFIX_BYTES - hdr_size;
        let mut scratch = (span >= PAYLOAD_PWRITE_BYTES).then(|| {
            let mut buf = self.scratch.take();
            if buf.len() < span {
                buf.resize(span, 0);
            }
            buf
        });
        // SAFETY: `base + total <= ring_len`, so the ring holds the group, and
        // the group lies past the published end.
        let hdr = unsafe { self.log.span_mut(hdr_off, hdr_size) };
        let payload: &mut [u8] = match &mut scratch {
            Some(buf) => &mut buf[..span],
            // SAFETY: as the header's; the payloads lie behind it.
            None => unsafe { self.log.span_mut(hdr_off + hdr_size, span) },
        };
        for (i, (off, sz)) in payload_spans(sizes.iter().copied()).enumerate() {
            Header::set_payload_size(hdr, i, sz);
            fill(i, &mut payload[off..off + sz]);
            payload[off + sz..off + sz.next_multiple_of(8)].fill(0);
        }
        let header = Header {
            head,
            base: base as u32,
            epoch,
            width: width as u8,
            payload_sum: if head.zoned() { gnitz_wire::checksum(payload) } else { 0 },
        };
        header.encode(hdr);
        if let Some(buf) = scratch {
            // Once it returns, the bytes are in the page cache the mapping
            // reads, ahead of the prefix store that publishes them.
            let at = (ANCHOR_BYTES + hdr_off + hdr_size) as u64;
            if let Err(e) = self.log.file.write_all_at(&buf[..span], at) {
                gnitz_fatal_abort!("SAL pwrite ({span} bytes at {at}) failed: {e}");
            }
            self.scratch.set(buf);
        }

        let end = base + total as u64;
        // SAFETY: `end <= cap <= ring_len - PREFIX_BYTES` — `group_limit` keeps
        // the end prefix free — `end` is the next group's base, and the word
        // there is past the published end, so no reader has reached it.
        unsafe { self.log.set_prefix_word(end, 0) };
        Ok(end)
    }

    /// Seal the header of the group laid out at `base` and store its prefix: the
    /// group is readable from here on.
    #[inline]
    fn publish(&self, base: u64) {
        // SAFETY: laid out by this writer and, its prefix word still zero,
        // reached by no reader.
        Header::seal(unsafe { self.log.header_mut(base) });
        let word = epoch_word(self.epoch.get(), PRESENT);
        // SAFETY: the base of a group this writer laid out, whose readers wait
        // at or before it for this store.
        unsafe { self.log.set_prefix_word(base, word) };
    }

    /// Publish every group laid out in `[from, end)` in layout order.
    fn publish_range(&self, from: u64, end: u64) {
        let mut base = from;
        while base < end {
            // SAFETY: as `publish`.
            let hdr = unsafe { self.log.header_mut(base) };
            let next = base + group_total_size(hdr.len(), dir_sizes(directory(hdr))) as u64;
            self.publish(base);
            base = next;
        }
    }

    /// Whether a checkpoint is warranted: the bytes published behind this
    /// epoch's checkpoint have crossed the configured threshold, or a group a
    /// checkpoint may admit ([`Self::checkpoint_may_admit`]) has been refused
    /// since the last rewind, on a log that may still sit below the threshold.
    pub(crate) fn needs_checkpoint(&self) -> bool {
        self.write_cursor.get() - self.checkpoint_floor.get() >= self.checkpoint_threshold
            || self.refused_transient.get()
    }

    /// Rewind to cursor 0 of `epoch`, which is anchored durably before any of
    /// its groups can exist: a base round's reclaim, and a boot's.
    fn rewind(&self, epoch: u32) {
        debug_assert!(epoch >= 1, "the first live SAL epoch is 1");
        self.write_cursor.set(0);
        self.checkpoint_floor.set(0);
        self.epoch.set(epoch);
        self.refused_transient.set(false);
        self.holds_pushes.set(false);
        // The anchor is synced before any group of `epoch` exists, so no epoch
        // is ever written twice. The ring's first prefix is zeroed only after
        // that sync: a power loss that keeps the old anchor keeps the old tail.
        self.log.anchor_atomic().store(anchor_word(epoch), Ordering::Relaxed);
        if let Err(e) = self.log.file.sync_data() {
            gnitz_fatal_abort!("SAL fdatasync (rewind to epoch {epoch}) failed: {e}");
        }
        self.durable.mark(self.watermark());
        // Every page of the ring is clean once synced: drop the cached ones no
        // process maps, so the new epoch's small groups do not land in the large
        // pages earlier `pwrite`s left behind, each of which a store dirties
        // and a sync writes whole.
        // SAFETY: an advisory call on an open descriptor.
        unsafe {
            libc::posix_fadvise(
                self.log.file.as_raw_fd(),
                ANCHOR_BYTES as libc::off_t,
                self.log.ring_len as libc::off_t,
                libc::POSIX_FADV_DONTNEED,
            )
        };
        // SAFETY: offset 0 is the ring's first group base, and no reader is past
        // it: one that stepped over the `Flush` waits there for `epoch`.
        unsafe { self.log.set_prefix_word(0, 0) };
    }

    /// Whether a zone member pushing rows was published since the last base
    /// round: the next one moves a base store.
    pub(crate) fn holds_pushes(&self) -> bool {
        self.holds_pushes.get()
    }

    /// The log position just past the last scope that published anything, as an
    /// LSN: every zone closed so far has an LSN at or below it, every later one
    /// an LSN above it. Moves only when a scope publishes something.
    pub(crate) fn watermark(&self) -> u64 {
        self.watermark.get()
    }

    /// Start making every group published at or below `lsn` durable, without
    /// waiting for it. Needs [`Self::run_syncs`] running.
    pub(crate) fn want_durable(&self, lsn: u64) {
        debug_assert!(lsn <= self.watermark(), "LSN {lsn} is not published");
        self.durable.want(lsn);
    }

    /// Resolves once every group published at or below `lsn` is durable: at
    /// once for `0`, which no zone carries. Needs [`Self::run_syncs`] running.
    pub(crate) async fn durable_through(&self, lsn: u64) {
        debug_assert!(lsn <= self.watermark(), "LSN {lsn} is not published");
        self.durable.through(lsn).await
    }

    /// Whether the fdatasync in flight makes every published group durable.
    pub(crate) fn syncing_all(&self) -> bool {
        self.durable.syncing() == self.watermark()
    }

    /// Run the fdatasyncs [`Self::durable_through`] waits for, each covering
    /// every group published before it is submitted. Publishing needs the
    /// writer lock, this does not. Never returns.
    pub(crate) async fn run_syncs(&self, reactor: &Reactor) {
        self.durable.run(reactor, self.log.file, || self.watermark()).await
    }
}

/// Sole write access. Dropping it wakes each worker a group written under it
/// reached that no earlier wake did. Held across no await except a base
/// round's ACKs.
pub(crate) struct SalExcl<'a> {
    writer: &'a SalWriter,
    /// The workers groups laid out since the last [`Self::wake`] reached.
    reached: WorkerSet,
    /// Whether the group last laid out under this hold was a read: the open
    /// [`Cut`](super::reader::Cut) the next read joins.
    in_cut: bool,
    _guard: WriteGuard<SalLock>,
}

impl<'a> SalExcl<'a> {
    /// Lay `g` out at `base`, unpublished, as a member of its scope's zone when
    /// `zoned`, and return its end. A read laid out directly behind another read
    /// of this hold joins its [`Cut`](super::reader::Cut); a refused group leaves
    /// the cut as it was. The one encode path.
    fn lay_out(&mut self, g: &Group, base: u64, zoned: bool) -> Result<u64, WireFault> {
        let nw = self.writer.parks.len();
        let set = g.to.within(nw);
        let mut sizes = [0usize; MAX_WORKERS];
        let payloads = g.sizes(nw, &mut sizes);
        let head = GroupHead::new(g.request, zoned, self.in_cut && g.reads, g.shared(), g.reply, set);
        let mut workers = payloads.iter();
        let end = self
            .writer
            .write_slots(base, head, &sizes[..payloads.len()], |_, slot| {
                let (blob, rows) = g.payload(workers.next().expect("one payload per size"));
                payload_encode(slot, blob, rows)
            })?;
        self.reached = self.reached.union(set);
        self.in_cut = g.reads;
        Ok(end)
    }

    /// Write and publish one group, outside every zone.
    fn write_group(&mut self, g: &Group) -> Result<(), WireFault> {
        let w = self.writer;
        let base = w.write_cursor.get();
        let end = self.lay_out(g, base, false)?;
        w.publish(base);
        w.write_cursor.set(end);
        Ok(())
    }

    /// Write `read` to `to`, answered on `reply`; with `blobs`, worker `w` is
    /// sent `blobs[w]` in place of the read's own blob. Consecutive reads written
    /// under one hold form one [`Cut`](super::reader::Cut): any other group
    /// between two of them ends it, a scope's included whether or not the scope
    /// committed.
    pub(crate) fn read(
        &mut self,
        read: Read<'_>,
        to: WorkerSet,
        blobs: Option<&[Vec<u8>]>,
        reply: RequestId,
    ) -> Result<(), WireFault> {
        self.write_group(&Group::read(&read, to, blobs, reply))
    }

    /// Write `apply` to every worker, acknowledged on `ack`, and wake them now:
    /// a caller may await the ACKs under this hold. A `Push` or `DdlSync` written
    /// here carries no rows. A `FlushEph` is the last group of its checkpoint:
    /// what follows it counts toward the next one.
    pub(crate) fn apply(&mut self, apply: Apply<'_>, ack: RequestId) -> Result<(), WireFault> {
        let ends_checkpoint = matches!(apply, Apply::FlushEph { .. });
        self.write_group(&Group::apply(&apply, ack))?;
        if ends_checkpoint {
            self.writer.checkpoint_floor.set(self.writer.write_cursor.get());
        }
        self.wake();
        Ok(())
    }

    /// Broadcast `Shutdown` and wake every worker.
    pub(crate) fn shutdown(&mut self) -> Result<(), WireFault> {
        self.write_group(&Group::shutdown())?;
        self.wake();
        Ok(())
    }

    /// Open a publication scope at the published end: groups laid out in it are
    /// invisible until [`SalScope::commit`], and a scope dropped uncommitted
    /// leaves the log as it found it. `&mut`, so no scope overlaps another or a
    /// rewind.
    pub(crate) fn begin(&mut self) -> SalScope<'_, 'a> {
        SalScope {
            staged: Staged {
                end: self.writer.write_cursor.get(),
                zone: None,
            },
            excl: self,
        }
    }

    /// Rewind into the next epoch — the base round's reclaim — and return
    /// that epoch.
    pub(crate) fn checkpoint_reset(&mut self) -> u32 {
        let epoch = next_epoch(self.writer.epoch.get());
        self.writer.rewind(epoch);
        epoch
    }

    /// Wake every worker a group written since the last wake reached.
    fn wake(&mut self) {
        self.writer
            .parks
            .wake(std::mem::replace(&mut self.reached, WorkerSet::EMPTY));
    }
}

impl Drop for SalExcl<'_> {
    fn drop(&mut self) {
        self.wake()
    }
}

/// A scope's open zone.
#[derive(Clone, Copy)]
struct OpenZone {
    /// The base of the latest zoned group.
    last: u64,
    /// A zoned group carries a push.
    pushes: bool,
}

/// What a scope has laid out and not published.
#[derive(Clone, Copy)]
struct Staged {
    /// The end of the scope's latest group: the published end while it has none.
    end: u64,
    zone: Option<OpenZone>,
}

/// One publication span on the SAL, holding at most one zone: its zoned
/// groups. Nothing is visible until [`Self::commit`], and the writer's
/// published end stands still until then.
pub(crate) struct SalScope<'s, 'w> {
    excl: &'s mut SalExcl<'w>,
    staged: Staged,
}

impl SalScope<'_, '_> {
    /// Lay `g` out inside the scope, unpublished. `zoned` puts it in the
    /// scope's atomic zone.
    fn write(&mut self, g: &Group, zoned: bool) -> Result<(), WireFault> {
        let base = self.staged.end;
        self.staged.end = self.excl.lay_out(g, base, zoned)?;
        if zoned {
            let pushes = g.request.kind == SalMessageKind::Push || self.staged.zone.is_some_and(|z| z.pushes);
            self.staged.zone = Some(OpenZone { last: base, pushes });
        }
        Ok(())
    }

    /// Lay out a catalog mutation of `family` as a member of the scope's zone:
    /// `batch`, sent whole to every worker, or to none for a family that reaches
    /// no worker.
    pub(crate) fn ddl_sync(&mut self, family: SysFamily, batch: &Batch) -> Result<(), WireFault> {
        self.write(&Group::ddl_sync(family, batch), true)
    }

    /// Lay out a push of `rows` into `tid`, held to `rule`, whose schema record
    /// is `record`, addressed to the workers `rows` holds rows for and answered
    /// by none. `recovered` makes it a member of the scope's zone: the rows a
    /// restart must recover.
    pub(crate) fn push(
        &mut self,
        tid: u64,
        rule: PkRule,
        record: &[u8],
        rows: Routed<'_>,
        recovered: bool,
    ) -> Result<(), WireFault> {
        self.write(&Group::push(tid, rule, record, rows), recovered)
    }

    /// Whether a checkpoint may make room for the pushes refused where the
    /// scope's next group is laid out, `bytes` sizing them: the question
    /// [`SalWriter::checkpoint_may_admit`] answers, asked of a unit of pushes
    /// that [`Self::all_or_nothing`] has just taken back, which starts there.
    pub(crate) fn checkpoint_may_admit_pushes(&self, bytes: impl FnOnce() -> usize) -> bool {
        let w = self.excl.writer;
        w.checkpoint_may_admit(
            group_limit(SalMessageKind::Push, w.log.ring_len),
            self.staged.end,
            bytes,
        )
    }

    /// Run `f`, and take back every group it laid out when it fails: nothing in
    /// a scope is published, so restoring what the scope has staged is the whole
    /// undo.
    pub(crate) fn all_or_nothing<T>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<T, WireFault>,
    ) -> Result<T, WireFault> {
        let saved = self.staged;
        let done = f(self);
        if done.is_err() {
            self.staged = saved;
        }
        done
    }

    /// Publish the span in layout order. Answers the zone's LSN — the log
    /// position just past the scope, in the writer's epoch — when a zone closed.
    /// The epoch is the high word, so LSNs follow SAL order across checkpoints
    /// and restarts: a zone's LSN is at or above the end of each of its members
    /// and below the end of every group written later.
    pub(crate) fn commit(self) -> Option<u64> {
        let w = self.excl.writer;
        let from = w.write_cursor.get();
        let Staged { end, zone } = self.staged;
        let lsn = epoch_word(w.epoch.get(), end);
        // A scope that published nothing moves nothing.
        if end > from {
            w.watermark.set(lsn);
        }
        let closed = match zone {
            None => {
                w.publish_range(from, end);
                None
            }
            Some(z) => {
                if z.pushes {
                    w.holds_pushes.set(true);
                }
                // SAFETY: as `publish`.
                Header::set_zone_end(unsafe { w.log.header_mut(z.last) });
                w.publish_range(from, z.last);
                if ZONE_PANIC.at(if z.pushes { "commit" } else { "ddl" }) {
                    std::process::abort();
                }
                w.publish_range(z.last, end);
                Some(lsn)
            }
        };
        w.write_cursor.set(end);
        closed
    }
}

/// The SAL fixture.
#[cfg(test)]
#[path = "tests/fixtures.rs"]
pub(crate) mod fixtures;

#[cfg(test)]
#[path = "tests/writer.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/writer.rs"]
mod bench;
