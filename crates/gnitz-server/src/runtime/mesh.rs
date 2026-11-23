//! The exchange mesh: workers trade a round's partitions directly through one
//! shared memfd mapping. A round is one or more parts, and each part is one
//! arrival per worker. Every worker runs the same parts in the same order, so a
//! part is complete once the cluster-wide `arrivals` count reaches
//! `(part + 1) * W`. Each worker has two outboxes, alternating by part, so it
//! rewrites one only after every peer has read it. A partition too large for
//! one outbox continues in the round's next part. Nothing here is durable.
//!
//! ```text
//! [0, 8)                         arrivals: AtomicU64 — parts published since boot
//! [HEADER_BYTES + (2w + p) * outbox_bytes, +outbox_bytes)   worker w's outbox p
//!   outbox: [Head, one LaneHead + nw Spans per lane, padded to BLOCKS_AT][blocks, 8-aligned]
//! ```
//!
//! A round carries one or more lanes, each one view's rows traded as a round of
//! its own would be, so several views' exchanges share one wait per part. A
//! block is one receiver's rows of a lane in a part as a WAL block of the lane's
//! schema, which every worker already holds from its own partition; an empty
//! [`Span`] sends none. Each [`LaneHead`] names its view, and is the divergence
//! check.

use std::borrow::Cow;
use std::fs::File;
use std::os::fd::{AsFd, AsRawFd};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::query::Lane;
use crate::runtime::park::{WorkerParks, WorkerSet};
use gnitz_foundation::posix_io;
use gnitz_wire::MAX_WORKERS;
use gnitz_zset::algebra::ScatterPlan;
use gnitz_zset::repr::{Batch, MemBatch, merge_consolidated};
use gnitz_zset::schema::SchemaDescriptor;
use smallvec::SmallVec;

/// The default and largest virtual size of one outbox.
const OUTBOX_BYTES: usize = 1 << 30;
const _: () = assert!(
    OUTBOX_BYTES <= u32::MAX as usize,
    "a WAL block stores its size as a u32"
);

const PAGE_BYTES: usize = 4096;

/// The page holding `arrivals`; the outboxes start past it.
const HEADER_BYTES: usize = PAGE_BYTES;

/// Where an outbox's blocks start: past its [`Head`].
const BLOCKS_AT: usize = PAGE_BYTES;
const _: () = assert!(
    size_of::<Head>() + size_of::<LaneHead>() + MAX_WORKERS * size_of::<Span>() <= BLOCKS_AT,
    "an outbox's first page holds its head and a lane's"
);

/// Resident bytes an outbox keeps past its latest part; a part that ends
/// further below the outbox's high-water mark returns the pages beyond it.
const RESIDENT_SLACK_BYTES: usize = 64 << 20;

/// The outbox size: [`OUTBOX_BYTES`], or `GNITZ_MESH_OUTBOX_PAGES` pages, from
/// one page of blocks past the head up to [`OUTBOX_BYTES`].
pub(crate) fn outbox_bytes() -> usize {
    let pages = gnitz_foundation::env::env_num_in(
        "GNITZ_MESH_OUTBOX_PAGES",
        OUTBOX_BYTES / PAGE_BYTES,
        (BLOCKS_AT + PAGE_BYTES) / PAGE_BYTES..=OUTBOX_BYTES / PAGE_BYTES,
    );
    pages * PAGE_BYTES
}

/// The mesh among `parks.len()` workers, over outboxes of `outbox_bytes`: one fresh
/// shared mapping, never unmapped, and every worker's view of it in rank order.
/// Zeroed. A memfd, so [`Mesh::trim`] can return pages by file offset.
pub(crate) fn create(outbox_bytes: usize, parks: WorkerParks) -> std::io::Result<Vec<Mesh>> {
    assert!(
        outbox_bytes.is_multiple_of(PAGE_BYTES) && outbox_bytes > BLOCKS_AT,
        "an outbox of {outbox_bytes} bytes is not whole pages past its head"
    );
    let nw = parks.len();
    let len = HEADER_BYTES + 2 * nw * outbox_bytes;
    // Leaked with the mapping.
    let file: &'static File = Box::leak(Box::new(posix_io::memfd(c"gnitz-mesh")?));
    file.set_len(len as u64)?;
    let base = posix_io::map_shared(Some(file.as_fd()), len)?;
    Ok((0..nw)
        .map(|rank| Mesh {
            base,
            file,
            rank,
            outbox_bytes,
            part: 0,
            in_round: false,
            parks,
            routed: Vec::new(),
            resident: [0; 2],
        })
        .collect())
}

/// Bytes `[at, at + len)` of an outbox; `len == 0` names no block.
#[derive(Clone, Copy)]
#[repr(C)]
struct Span {
    at: u32,
    len: u32,
}

/// An outbox's first bytes: what the publisher's part is. Its lanes' heads
/// follow it in the same page.
#[repr(C)]
struct Head {
    /// How many lanes the round carries; each worker's round carries the same.
    lanes: u64,
    /// The publisher's claim that its source is drained.
    drained: u64,
    /// The publisher has rows left for another part of this round.
    more: u64,
}

/// One lane's head, followed by one [`Span`] per worker: each receiver's block
/// of the lane in this part.
#[repr(C)]
struct LaneHead {
    view: u64,
    /// The lane's published partition was consolidated and it lands where it
    /// folds, so every block of it is merged on gather.
    consolidated: u64,
}

/// One worker's view of the mesh.
pub(crate) struct Mesh {
    base: *mut u8,
    /// The memfd behind `base`.
    file: &'static File,
    rank: usize,
    outbox_bytes: usize,
    /// Parts gathered since boot — identical on every worker; its parity picks
    /// the outbox.
    part: u64,
    /// A [`Round`] is open: [`Self::open`] returned it and no [`Self::step`] has
    /// closed it.
    in_round: bool,
    /// Every worker's park; a part's last arrival wakes the peers.
    parks: WorkerParks,
    /// Per-receiver row lists of the lane being sent, reused across lanes and
    /// rounds.
    routed: Vec<Vec<u32>>,
    /// How far into each of this worker's outboxes pages may be resident.
    resident: [usize; 2],
}

/// One exchange round, from [`Mesh::open`] to the [`Mesh::step`] that closes it:
/// one or more lanes, each traded as a round of its own would be, behind one
/// arrival per part.
pub(crate) struct Round<'b> {
    /// This worker's claim that its source is drained, sent with every part.
    drained: bool,
    lanes: Vec<LaneRound<'b>>,
    /// How far this worker's writing has come; `None` once every row it sends
    /// is written.
    at: Option<Cursor>,
}

/// One lane of an open [`Round`].
struct LaneRound<'b> {
    view: u64,
    schema: SchemaDescriptor,
    /// The published batch was consolidated and the lane lands where it folds.
    consolidated: bool,
    /// The batch the lane writes from and the plan routing it; `None`, dropping
    /// a given batch, once every row it sends is written.
    unsent: Option<(Cow<'b, Batch>, &'b ScatterPlan)>,
    /// This receiver's non-empty blocks of the lane from the round's earlier
    /// parts, copied out raw.
    saved: Vec<Vec<u8>>,
    /// Every sender of a block this receiver took in so far was consolidated,
    /// so the lane's blocks are merged on close.
    merge: bool,
}

/// How far a round's writing has come: in lane `lane`, at
/// `routed[list][offset..]`.
struct Cursor {
    lane: usize,
    /// How many lists the lane's plan routed into: one per receiver, or the
    /// one list every receiver is sent.
    lists: usize,
    list: usize,
    offset: usize,
}

impl Mesh {
    fn nw(&self) -> usize {
        self.parks.len()
    }

    fn arrivals(&self) -> &AtomicU64 {
        // SAFETY: the region's first 8 bytes, 8-aligned, live for the process.
        unsafe { AtomicU64::from_ptr(self.base.cast()) }
    }

    /// Where worker `w`'s outbox for the current part starts in the region.
    fn outbox_at(&self, w: usize) -> usize {
        assert!(w < self.nw(), "worker {w} of {}", self.nw());
        HEADER_BYTES + (2 * w + (self.part % 2) as usize) * self.outbox_bytes
    }

    /// Worker `w`'s outbox for the current part.
    fn outbox(&self, w: usize) -> *mut u8 {
        // SAFETY: `outbox_at` checked `w < nw`, so the outbox lies inside the region.
        unsafe { self.base.add(self.outbox_at(w)) }
    }

    /// The most lanes one round carries: as many heads as fit the page before
    /// the blocks.
    pub(crate) fn max_lanes(&self) -> usize {
        (BLOCKS_AT - size_of::<Head>()) / self.lane_bytes()
    }

    fn lane_bytes(&self) -> usize {
        size_of::<LaneHead>() + self.nw() * size_of::<Span>()
    }

    /// Open a round of `lanes`: send the nonzero-weight rows of each lane's
    /// batch split by its plan, all behind the same arrivals, as the round's
    /// first part. Every worker opens the same lanes in the same order, at most
    /// [`Self::max_lanes`]. `drained` is this worker's claim, which
    /// [`Self::step`] answers with every worker's. Fatal while a round is open,
    /// or on a row larger than an outbox.
    pub(crate) fn open<'b>(&mut self, lanes: impl ExactSizeIterator<Item = Lane<'b>>, drained: bool) -> Round<'b> {
        assert!(!self.in_round, "exchange: a round is already open");
        assert!(
            (1..=self.max_lanes()).contains(&lanes.len()),
            "exchange: a round of {} lanes",
            lanes.len()
        );
        self.in_round = true;
        let lanes = lanes
            .map(|Lane { view, batch, plan, fold }| LaneRound {
                view,
                schema: *batch.schema(),
                consolidated: fold && batch.is_consolidated(),
                unsent: Some((batch, plan)),
                saved: Vec::new(),
                merge: true,
            })
            .collect();
        let mut round = Round { drained, lanes, at: None };
        round.at = self.enter(&round, 0);
        self.send_part(&mut round);
        round
    }

    /// Route `round`'s lane `lane`, the next to write, into [`Self::routed`],
    /// and answer a cursor at its first list; `None` past the last lane.
    fn enter(&mut self, round: &Round<'_>, lane: usize) -> Option<Cursor> {
        let (batch, plan) = round
            .lanes
            .get(lane)?
            .unsent
            .as_ref()
            .expect("a lane is entered unsent");
        let nw = self.nw();
        let lists = plan.route(&batch.as_mem_batch(), &mut self.routed, nw).len();
        Some(Cursor { lane, lists, list: 0, offset: 0 })
    }

    /// Lane `lane`'s head, and the [`Span`]s after it, in worker `w`'s outbox
    /// for the current part.
    fn lane_at(&self, w: usize, lane: usize) -> (*mut LaneHead, *mut Span) {
        // SAFETY: `open` bounds the lanes to what fits the outbox's first page.
        unsafe {
            let head = self.outbox(w).add(size_of::<Head>() + lane * self.lane_bytes());
            (head.cast(), head.add(size_of::<LaneHead>()).cast())
        }
    }

    /// Send `round`'s next part: what is left of its rows, or nothing.
    fn send_part(&mut self, round: &mut Round<'_>) {
        let nw = self.nw();
        for (i, lane) in round.lanes.iter().enumerate() {
            let (head, spans) = self.lane_at(self.rank, i);
            // SAFETY: this worker's own outbox, which no peer reads until the
            // arrival that follows; a lane's head is 8-aligned.
            unsafe {
                head.write(LaneHead {
                    view: lane.view,
                    consolidated: lane.consolidated as u64,
                });
                std::slice::from_raw_parts_mut(spans, nw).fill(Span { at: 0, len: 0 });
            }
        }
        let outbox = self.outbox(self.rank);
        let mut end = BLOCKS_AT;
        // Each lane's rows from the cursor on, as one block per row list of the
        // lane, until the lanes end or the outbox fills.
        'lanes: while let Some(at) = &mut round.at {
            let lane = &mut round.lanes[at.lane];
            let (batch, _) = lane.unsent.as_ref().expect("the cursor's lane is unsent");
            // SAFETY: as above.
            let spans = unsafe { std::slice::from_raw_parts_mut(self.lane_at(self.rank, at.lane).1, nw) };
            while at.list < at.lists {
                let rest = &self.routed[at.list][at.offset..];
                if !rest.is_empty() {
                    // SAFETY: `[end, outbox_bytes)` lies inside this worker's own
                    // outbox, which no peer reads until the arrival that follows.
                    let out = unsafe { std::slice::from_raw_parts_mut(outbox.add(end), self.outbox_bytes - end) };
                    let Some((n, len)) = batch.as_mem_batch().encode_scattered_prefix(rest, out) else {
                        break 'lanes;
                    };
                    let span = Span { at: end as u32, len: len as u32 };
                    match at.lists {
                        1 => spans.fill(span),
                        _ => spans[at.list] = span,
                    }
                    end = (end + len).next_multiple_of(8);
                    if n < rest.len() {
                        at.offset += n;
                        break 'lanes;
                    }
                }
                at.list += 1;
                at.offset = 0;
            }
            lane.unsent = None;
            let next = at.lane + 1;
            round.at = self.enter(round, next);
        }
        // The cursor stays only where rows are left for another part.
        if let Some(at) = &round.at
            && end == BLOCKS_AT
        {
            gnitz_fatal_abort!(
                "exchange of view {}: a row does not fit the {}-byte outbox",
                round.lanes[at.lane].view,
                self.outbox_bytes
            );
        }
        // Publish the head and the blocks before `end` as this worker's part:
        // count the arrival, and wake every peer if it completed the part.
        // SAFETY: the outbox is page-aligned and its first page holds the head;
        // no peer reads it until the arrival below.
        unsafe {
            outbox.cast::<Head>().write(Head {
                lanes: round.lanes.len() as u64,
                drained: round.drained as u64,
                more: round.at.is_some() as u64,
            })
        }
        self.trim(end);
        let nw = nw as u64;
        // Release: a peer that sees the count sees the outbox writes above.
        if self.arrivals().fetch_add(1, Ordering::AcqRel) == (self.part + 1) * nw - 1 {
            self.parks.wake(WorkerSet::ALL.without(self.rank));
        }
    }

    /// Return the pages of this part's outbox past `end`, once they exceed
    /// [`RESIDENT_SLACK_BYTES`]. Every peer read what they held two parts ago.
    fn trim(&mut self, end: usize) {
        let p = (self.part % 2) as usize;
        let end = end.next_multiple_of(PAGE_BYTES);
        if self.resident[p] > end + RESIDENT_SLACK_BYTES {
            let at = self.outbox_at(self.rank) + end;
            // `[end, resident)` of this worker's own outbox, which every peer is done
            // with. Best-effort: a refusal only leaves the pages resident.
            let _ = posix_io::retry_eintr(|| unsafe {
                libc::fallocate(
                    self.file.as_raw_fd(),
                    libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                    at as libc::off_t,
                    (self.resident[p] - end) as libc::off_t,
                )
            });
            self.resident[p] = end;
        }
        self.resident[p] = self.resident[p].max(end);
    }

    /// A [`Round`] is open: this worker is inside an exchange wait.
    pub(crate) fn in_round(&self) -> bool {
        self.in_round
    }

    /// Every worker has published the current part.
    pub(crate) fn complete(&self) -> bool {
        self.arrivals().load(Ordering::Acquire) >= (self.part + 1) * self.nw() as u64
    }

    /// Take in the current part once it is complete. `None`: the part is
    /// incomplete, or `round`'s next part was sent. `Some`: the round is closed —
    /// pushed to `gathered`, per lane, the rows every peer sent this worker;
    /// answered, whether every worker's source was drained.
    pub(crate) fn step(&mut self, round: &mut Round<'_>, gathered: &mut Vec<Batch>) -> Option<bool> {
        assert!(self.in_round, "step without an open round");
        if !self.complete() {
            return None;
        }
        let (rank, nw) = (self.rank, self.nw());
        let (mut drained, mut more) = (true, false);
        for peer in 0..nw {
            // SAFETY: the peer wrote its head before the arrival `complete`
            // observed, and rewrites it only after this worker's next arrival.
            let head = unsafe { &*self.outbox(peer).cast::<Head>() };
            if head.lanes != round.lanes.len() as u64 {
                gnitz_fatal_abort!(
                    "exchange of view {}: worker {peer} published {} lanes, not {} — workers diverged",
                    round.lanes[0].view,
                    head.lanes,
                    round.lanes.len()
                );
            }
            drained &= head.drained != 0;
            more |= head.more != 0;
        }
        for (i, lane) in round.lanes.iter_mut().enumerate() {
            let view = lane.view;
            // The non-empty blocks peers sent this worker; they stay as read
            // until this worker's next arrival.
            let mut blocks: SmallVec<[&[u8]; MAX_WORKERS]> = SmallVec::new();
            for peer in 0..nw {
                let (head, spans) = self.lane_at(peer, i);
                // SAFETY: the peer wrote its lane heads before the arrival
                // `complete` observed, and rewrites them only after this
                // worker's next arrival; `head.lanes` bounds `i`.
                let (head, span) = unsafe { (&*head, *spans.add(rank)) };
                if head.view != view {
                    gnitz_fatal_abort!(
                        "exchange of view {view}: worker {peer} published view {} — workers diverged",
                        head.view
                    );
                }
                let (at, len) = (span.at as usize, span.len as usize);
                if len == 0 {
                    continue;
                }
                if at < BLOCKS_AT || at + len > self.outbox_bytes {
                    gnitz_fatal_abort!("exchange: worker {peer}'s head names bytes [{at}, +{len}) outside its outbox");
                }
                lane.merge &= head.consolidated != 0;
                // SAFETY: checked above to lie inside the peer's outbox, which it
                // rewrites only after this worker's next arrival.
                blocks.push(unsafe { std::slice::from_raw_parts(self.outbox(peer).add(at), len) });
            }
            if more {
                lane.saved.extend(blocks.into_iter().map(<[u8]>::to_vec));
                continue;
            }
            let schema = &lane.schema;
            let slices: SmallVec<[MemBatch; MAX_WORKERS]> = lane
                .saved
                .iter()
                .map(Vec::as_slice)
                .chain(blocks)
                .map(|block| {
                    MemBatch::of_wal_block(block, schema)
                        .unwrap_or_else(|e| gnitz_fatal_abort!("exchange of view {view}: a peer's block: {e}"))
                })
                .collect();
            // Z-set `+` over the lane's slices: merged where every sender's were
            // consolidated, else concatenated in order.
            gathered.push(match lane.merge {
                true => merge_consolidated(&slices, schema),
                false => Batch::concat(schema, slices.iter().copied()),
            });
        }
        self.part += 1;
        if more {
            self.send_part(round);
            return None;
        }
        self.in_round = false;
        Some(drained)
    }
}

#[cfg(test)]
#[path = "tests/mesh.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/mesh.rs"]
mod bench;
