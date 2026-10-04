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
//!   outbox: [Head, padded to BLOCKS_AT][blocks, 8-aligned]
//! ```
//!
//! A block is one receiver's rows of a part as a WAL block of the round's
//! schema, which every worker already holds from its own partition; an empty
//! [`Span`] sends none. The [`Head`] names the view, and is the divergence check.

use std::borrow::Cow;
use std::fs::File;
use std::os::fd::{AsRawFd, FromRawFd};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::runtime::park::WorkerParks;
use crate::runtime::sal::MAX_WORKERS;
use gnitz_foundation::posix_io;
use gnitz_zset::algebra::ScatterPlan;
use gnitz_zset::repr::{merge_consolidated, Batch, MemBatch};
use gnitz_zset::schema::SchemaDescriptor;

/// The default and largest virtual size of one outbox.
pub(crate) const OUTBOX_BYTES: usize = 1 << 30;
const _: () = assert!(
    OUTBOX_BYTES <= u32::MAX as usize,
    "a WAL block stores its size as a u32"
);

const PAGE_BYTES: usize = 4096;

/// The page holding `arrivals`; the outboxes start past it.
const HEADER_BYTES: usize = PAGE_BYTES;

/// Where an outbox's blocks start: past its [`Head`].
const BLOCKS_AT: usize = PAGE_BYTES;
const _: () = assert!(size_of::<Head>() <= BLOCKS_AT, "an outbox's head fits its first page");

/// Resident bytes an outbox keeps past its latest part; a part that ends
/// further below the outbox's high-water mark returns the pages beyond it.
const RESIDENT_SLACK_BYTES: usize = 64 << 20;

/// The outbox size: `GNITZ_MESH_OUTBOX_BYTES`, clamped, or [`OUTBOX_BYTES`].
pub(crate) fn outbox_bytes() -> usize {
    let asked = gnitz_foundation::env::env_num("GNITZ_MESH_OUTBOX_BYTES", OUTBOX_BYTES);
    let size = asked.clamp(2 * PAGE_BYTES, OUTBOX_BYTES).next_multiple_of(PAGE_BYTES);
    if size != asked {
        gnitz_info!(
            "GNITZ_MESH_OUTBOX_BYTES={asked} is outside [{}, {OUTBOX_BYTES}] or not page-sized; using {size}",
            2 * PAGE_BYTES
        );
    }
    size
}

/// The mesh among `parks.len()` workers, over outboxes of `outbox_bytes`: one fresh
/// shared mapping, never unmapped, and every worker's view of it in rank order.
/// Zeroed. A memfd, so [`Mesh::trim`] can return pages by file offset.
pub(crate) fn create(outbox_bytes: usize, parks: WorkerParks) -> std::io::Result<Vec<Mesh>> {
    let nw = parks.len();
    assert!(nw <= MAX_WORKERS, "{nw} workers");
    let len = HEADER_BYTES + 2 * nw * outbox_bytes;
    // SAFETY: the name is a NUL-terminated literal.
    let fd = unsafe { libc::memfd_create(c"gnitz-mesh".as_ptr(), libc::MFD_CLOEXEC) };
    if fd < 0 {
        return Err(std::io::Error::last_os_error());
    }
    // SAFETY: a descriptor nothing else owns. Leaked with the mapping.
    let file: &'static File = Box::leak(Box::new(unsafe { File::from_raw_fd(fd) }));
    file.set_len(len as u64)?;
    // SAFETY: a fresh mapping of the whole `len`-byte file.
    let base = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED,
            fd,
            0,
        )
    };
    if base == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error());
    }
    Ok((0..nw)
        .map(|rank| Mesh {
            base: base.cast(),
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
    at: u64,
    len: u64,
}

impl Span {
    const EMPTY: Span = Span { at: 0, len: 0 };
}

/// An outbox's first bytes: what the publisher's part is, and each receiver's
/// block of it.
#[repr(C)]
struct Head {
    view: u64,
    /// The publisher's claim that its source is drained.
    drained: u64,
    /// The published partition was consolidated and the round lands where it
    /// folds, so every block is merged on gather.
    consolidated: u64,
    /// The publisher has rows left for another part of this round.
    more: u64,
    blocks: [Span; MAX_WORKERS],
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
    /// Per-receiver row lists of the round being sent, reused across rounds.
    routed: Vec<Vec<u32>>,
    /// How far into each of this worker's outboxes pages may be resident.
    resident: [usize; 2],
}

/// One exchange round, from [`Mesh::open`] to the [`Mesh::step`] that closes it.
pub(crate) struct Round<'b> {
    view: u64,
    schema: SchemaDescriptor,
    /// This worker's claim that its source is drained, sent with every part.
    drained: bool,
    /// The published batch was consolidated and the round lands where it folds.
    consolidated: bool,
    /// How far this worker's writing has come, and the batch it writes from;
    /// `None`, dropping the batch, once every row it sends is written.
    unsent: Option<(Cursor, Cow<'b, Batch>)>,
    /// This receiver's non-empty blocks from the round's earlier parts, copied
    /// out raw.
    saved: Vec<Vec<u8>>,
    /// Every sender of a saved block was consolidated.
    saved_consolidated: bool,
}

/// How far a round's writing has come through its row lists: at
/// `routed[list][offset..]`.
#[derive(Clone, Copy)]
struct Cursor {
    /// How many lists the round's plan routed into: one per receiver, or the
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

    /// Worker `w`'s outbox for the current part.
    fn outbox(&self, w: usize) -> *mut u8 {
        assert!(w < self.nw(), "worker {w} of {}", self.nw());
        // SAFETY: `w < nw`, so the outbox lies inside the region.
        unsafe {
            self.base
                .add(HEADER_BYTES + (2 * w + self.parity()) * self.outbox_bytes)
        }
    }

    fn parity(&self) -> usize {
        (self.part % 2) as usize
    }

    /// Open a round of `view`: send the nonzero-weight rows of `batch` split by `plan` as its
    /// first part. `fold` is [`crate::query::DriveHost::exchange`]'s; `drained` is
    /// this worker's claim, which [`Self::step`] answers with every worker's.
    /// Fatal while a round is open, or on a row larger than an outbox.
    pub(crate) fn open<'b>(
        &mut self,
        view: u64,
        batch: Cow<'b, Batch>,
        plan: &ScatterPlan,
        fold: bool,
        drained: bool,
    ) -> Round<'b> {
        assert!(!self.in_round, "exchange of view {view}: a round is already open");
        self.in_round = true;
        let nw = self.nw();
        let lists = plan.route(&batch, &mut self.routed, nw).len();
        let mut round = Round {
            view,
            schema: *batch.schema(),
            drained,
            consolidated: fold && batch.is_consolidated(),
            unsent: Some((Cursor { lists, list: 0, offset: 0 }, batch)),
            saved: Vec::new(),
            saved_consolidated: true,
        };
        self.send_part(&mut round);
        round
    }

    /// Send `round`'s next part: what is left of its rows, or nothing.
    fn send_part(&mut self, round: &mut Round<'_>) {
        let mut head = Head {
            view: round.view,
            drained: round.drained as u64,
            consolidated: round.consolidated as u64,
            more: 0,
            blocks: [Span::EMPTY; MAX_WORKERS],
        };
        let mut end = BLOCKS_AT;
        if let Some((at, batch)) = &mut round.unsent {
            let more;
            (end, more) = self.write_rows(batch, at, &mut head.blocks);
            if more && end == BLOCKS_AT {
                gnitz_fatal_abort!(
                    "exchange of view {}: a row does not fit the {}-byte outbox",
                    head.view,
                    self.outbox_bytes
                );
            }
            head.more = more as u64;
            if !more {
                round.unsent = None;
            }
        }
        self.arrive(&head, end);
    }

    /// Write `batch`'s rows from `at` as one block per row list, naming each in
    /// `blocks`, until the lists end or the outbox fills. Returns where the
    /// blocks end, and whether rows are left.
    fn write_rows(&self, batch: &Batch, at: &mut Cursor, blocks: &mut [Span; MAX_WORKERS]) -> (usize, bool) {
        let nw = self.nw();
        let outbox = self.outbox(self.rank);
        let mut end = BLOCKS_AT;
        while at.list < at.lists {
            let rest = &self.routed[at.list][at.offset..];
            if !rest.is_empty() {
                // SAFETY: `[end, outbox_bytes)` lies inside this worker's own
                // outbox, which no peer reads until the arrival that follows.
                let out = unsafe { std::slice::from_raw_parts_mut(outbox.add(end), self.outbox_bytes - end) };
                let Some((n, len)) = batch.encode_scattered_prefix(rest, out) else {
                    return (end, true);
                };
                let span = Span { at: end as u64, len: len as u64 };
                match at.lists {
                    1 => blocks[..nw].fill(span),
                    _ => blocks[at.list] = span,
                }
                end = (end + len).next_multiple_of(8);
                if n < rest.len() {
                    at.offset += n;
                    return (end, true);
                }
            }
            at.list += 1;
            at.offset = 0;
        }
        (end, false)
    }

    /// Publish `head` and the blocks before `end` as this worker's part: count
    /// the arrival, and wake every peer if it completed the part.
    fn arrive(&mut self, head: &Head, end: usize) {
        // SAFETY: the outbox is page-aligned and its first page holds the head;
        // no peer reads it until the arrival below.
        unsafe { std::ptr::copy_nonoverlapping(head, self.outbox(self.rank).cast::<Head>(), 1) }
        self.trim(end);
        let nw = self.nw() as u64;
        // Release: a peer that sees the count sees the outbox writes above.
        if self.arrivals().fetch_add(1, Ordering::AcqRel) == (self.part + 1) * nw - 1 {
            for w in (0..self.nw()).filter(|&w| w != self.rank) {
                self.parks.wake(w);
            }
        }
    }

    /// Return the pages of this part's outbox past `end`, once they exceed
    /// [`RESIDENT_SLACK_BYTES`]. Every peer read what they held two parts ago.
    fn trim(&mut self, end: usize) {
        let p = self.parity();
        let end = end.next_multiple_of(PAGE_BYTES);
        if self.resident[p] > end + RESIDENT_SLACK_BYTES {
            let at = HEADER_BYTES + (2 * self.rank + p) * self.outbox_bytes + end;
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

    /// Every worker has published the current part.
    pub(crate) fn complete(&self) -> bool {
        self.arrivals().load(Ordering::Acquire) >= (self.part + 1) * self.nw() as u64
    }

    /// Take in the completed part. `None`: `round`'s next part was sent. `Some`:
    /// the round is closed — the rows every peer sent this worker, and whether
    /// every worker's source was drained.
    pub(crate) fn step(&mut self, round: &mut Round<'_>) -> Option<(Batch, bool)> {
        assert!(self.in_round, "step without an open round");
        assert!(self.complete(), "stepped before every worker published");
        let view = round.view;
        let (mut drained, mut more, mut consolidated) = (true, false, round.saved_consolidated);
        let mut blocks = Vec::with_capacity(self.nw());
        for peer in 0..self.nw() {
            // SAFETY: the peer wrote its head before the arrival `complete`
            // observed, and rewrites it only after this worker's next arrival.
            let head = unsafe { &*self.outbox(peer).cast::<Head>() };
            if head.view != view {
                gnitz_fatal_abort!(
                    "exchange of view {view}: worker {peer} published view {} — workers diverged",
                    head.view
                );
            }
            drained &= head.drained != 0;
            more |= head.more != 0;
            let block = self.block(peer, head.blocks[self.rank]);
            if !block.is_empty() {
                consolidated &= head.consolidated != 0;
                blocks.push(block);
            }
        }
        if more {
            round.saved.extend(blocks.into_iter().map(<[u8]>::to_vec));
            round.saved_consolidated = consolidated;
            self.part += 1;
            self.send_part(round);
            return None;
        }
        let schema = round.schema;
        let slices: Vec<MemBatch> = round
            .saved
            .iter()
            .map(Vec::as_slice)
            .chain(blocks)
            .map(|block| {
                MemBatch::of_wal_block(block, &schema)
                    .unwrap_or_else(|e| gnitz_fatal_abort!("exchange of view {view}: a peer's block: {e}"))
            })
            .collect();
        // Z-set `+` over the round's slices: merged where every sender's were
        // consolidated, else concatenated in order.
        let rows = match consolidated {
            true => merge_consolidated(&slices, &schema),
            false => Batch::concat(&schema, slices.iter().cloned()),
        };
        self.part += 1;
        self.in_round = false;
        Some((rows, drained))
    }

    /// The block `span` names in `peer`'s outbox for the current part.
    fn block(&self, peer: usize, span: Span) -> &[u8] {
        if span.len == 0 {
            return &[];
        }
        let inside = span.at >= BLOCKS_AT as u64
            && span
                .at
                .checked_add(span.len)
                .is_some_and(|e| e <= self.outbox_bytes as u64);
        if !inside {
            gnitz_fatal_abort!(
                "exchange: worker {peer}'s head names bytes [{}, +{}) outside its outbox",
                span.at,
                span.len
            );
        }
        // SAFETY: checked above to lie inside the peer's outbox, which it
        // rewrites only after this worker's next arrival.
        unsafe { std::slice::from_raw_parts(self.outbox(peer).add(span.at as usize), span.len as usize) }
    }
}

#[cfg(test)]
#[path = "tests/mesh_fixtures.rs"]
pub(crate) mod fixtures;

#[cfg(test)]
#[path = "tests/mesh.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/mesh.rs"]
mod bench;
