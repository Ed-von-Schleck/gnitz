//! Bounded external merge sort of fixed-stride OPK byte records.
//!
//! Callers that must stream a large record set **globally sorted** (e.g. the
//! CREATE UNIQUE INDEX pre-flight, whose master-side duplicate check needs
//! equal keys adjacent) cannot materialise it in RAM — a whole-partition
//! in-RAM sort OOM-kills the worker on a large table. [`SpillSort`] bounds
//! that memory: records accumulate in a flat byte buffer, and past a byte
//! budget the buffer is sorted and spilled as one run to a single anonymous
//! spill file; [`SpillSort::finish`] then k-way merges the runs via the shared
//! [`LoserTree`] kernel, lending the globally-sorted records one at a time.
//! Peak RAM is the budget plus the sort index and one reorder buffer —
//! independent of input size.
//!
//! Design choices that keep it simple and leak-free:
//! - **Flat `Vec<u8>` accumulation.** Records are fixed `stride` bytes, so a
//!   flat buffer of contiguous records with an index-permutation sort is both
//!   the honest memory accounting (the buffer length IS the budget check) and
//!   the natural layout to spill. Fatter per-record types (e.g. an 80-byte
//!   `PkBuf` for an 8-byte key) would let the accumulator hold ~10× more RAM
//!   than the budget names.
//! - **One `O_TMPFILE` spill file, `mmap`'d for the merge.** The anonymous
//!   inode is reclaimed by the kernel on close and on any process death — a
//!   `SIGKILL` or a `panic = "abort"` (the release profile) leaves nothing on
//!   disk, which a named temp file + RAII `unlinkat` cannot guarantee (Drop
//!   never runs on abort). One file holds all runs back-to-back; `mmap`-ing it
//!   once means the merge holds no fd and reads records straight from mapped
//!   memory, so run count never pressures the fd limit and no read syscall
//!   (hence no fallible read) happens mid-stream — every fallible spill write
//!   is front-loaded into `push` / `finish`.
//! - **`String` errors, not `StorageError`.** The consumers surface these
//!   messages verbatim to the client (a failed DDL), so they carry the path
//!   and OS error instead of collapsing into an opaque `Io` variant.

use std::os::fd::{AsRawFd, OwnedFd};

use super::repr::heap::{HeapNode, LoserTree};
use crate::foundation::posix_io;
use crate::foundation::posix_io::{Advice, Mmap};
use crate::schema::key::compare_pk_bytes;

/// Sort `idx` (rebuilt as `0..flat.len()/stride`) by the byte-lexicographic
/// order of the fixed-`stride` records of `flat` — the same `compare_pk_bytes`
/// order every merge path uses. Indirect (index-permutation) so the records
/// themselves never move.
///
/// Shared with `index_gather`, whose per-chunk PK scratch is the same
/// flat-record accumulator this module's header argues for.
pub(crate) fn sort_indices(flat: &[u8], stride: usize, idx: &mut Vec<u32>) {
    let n = flat.len() / stride;
    // u32 indices bound each run (and the in-RAM fast path) to 2^32 records —
    // the same per-source bound the LoserTree merge asserts.
    assert!(n <= u32::MAX as usize, "spill run exceeds u32 records");
    idx.clear();
    idx.extend(0..n as u32);
    idx.sort_unstable_by(|&a, &b| {
        let a = a as usize * stride;
        let b = b as usize * stride;
        compare_pk_bytes(&flat[a..a + stride], &flat[b..b + stride])
    });
}

/// Bounded external merge sort of fixed-width records (see the module doc).
///
/// Push records in any order; past the byte budget the buffer is sorted and
/// appended to the spill file as one sorted run. `finish` returns either the
/// in-RAM sorted records (nothing ever spilled — the fast path) or a streaming
/// k-way merge over the spilled runs. Every record is exactly `stride` bytes,
/// so runs are unframed fixed-stride records with no per-record length prefix.
pub struct SpillSort {
    stride: usize,
    budget: usize,
    dir: String,
    /// Accumulated records, `stride` bytes each; its length is the live byte
    /// count checked against `budget`.
    flat: Vec<u8>,
    /// Reused index permutation for the indirect sort of `flat`.
    idx: Vec<u32>,
    /// Reused reorder buffer: `flat`'s records copied out in sorted order,
    /// then written to the spill file in one call.
    scratch: Vec<u8>,
    /// The spill file, opened lazily on the first spill (the fast path never
    /// creates it). `O_TMPFILE`: dropping the fd reclaims the anonymous inode.
    spill: Option<OwnedFd>,
    /// Record count of each spilled run, in spill order. Empty ⇒ fast path.
    runs: Vec<usize>,
}

impl SpillSort {
    /// `dir` anchors the `O_TMPFILE` spill on a specific filesystem (the fast
    /// path never touches it). `stride` is the fixed record width; `budget`
    /// the in-RAM byte ceiling before a run spills.
    pub fn new(dir: &str, stride: usize, budget: usize) -> Self {
        debug_assert!(stride > 0);
        SpillSort {
            stride,
            budget,
            dir: dir.to_string(),
            flat: Vec::new(),
            idx: Vec::new(),
            scratch: Vec::new(),
            spill: None,
            runs: Vec::new(),
        }
    }

    /// Append one `stride`-byte record; spill a sorted run once the buffer
    /// reaches the byte budget. Duplicates are preserved at full multiplicity;
    /// copies split across runs are still merged adjacently by `finish`.
    pub fn push(&mut self, record: &[u8]) -> Result<(), String> {
        debug_assert_eq!(record.len(), self.stride);
        self.flat.extend_from_slice(record);
        if self.flat.len() >= self.budget {
            self.spill_run()?;
        }
        Ok(())
    }

    /// Lazily open the anonymous spill file in `dir`, returning its raw fd.
    fn ensure_spill_fd(&mut self) -> Result<i32, String> {
        if self.spill.is_none() {
            let fd = posix_io::open_tmpfile(&self.dir)
                .map_err(|e| format!("external sort: cannot create spill file in {}: {e}", self.dir))?;
            self.spill = Some(fd);
        }
        Ok(self.spill.as_ref().unwrap().as_raw_fd())
    }

    /// Sort the accumulated buffer and append it to the spill file as one run.
    /// No-op on an empty buffer, so a run in `runs` always has `>= 1` record.
    fn spill_run(&mut self) -> Result<(), String> {
        let n = self.flat.len() / self.stride;
        if n == 0 {
            return Ok(());
        }
        sort_indices(&self.flat, self.stride, &mut self.idx);
        self.scratch.clear();
        self.scratch.reserve(n * self.stride);
        for &i in &self.idx {
            let o = i as usize * self.stride;
            self.scratch.extend_from_slice(&self.flat[o..o + self.stride]);
        }
        let fd = self.ensure_spill_fd()?;
        // `write_all_fd` fully writes or returns an error, so a short/ENOSPC
        // write becomes an `Err` here and the sort aborts before the merge —
        // a truncated (misaligned) run can never reach it.
        posix_io::write_all_fd(fd, &self.scratch).map_err(|e| format!("external sort: spill write failed: {e}"))?;
        self.runs.push(n);
        self.flat.clear();
        Ok(())
    }

    /// Finish accumulation and yield the sorted-record producer. On the fast
    /// path (nothing spilled) it sorts the in-RAM buffer; otherwise it spills
    /// the final partial run, `mmap`s the spill file, and primes the k-way
    /// merge. The last fallible I/O happens here — the producer is infallible.
    pub fn finish(mut self) -> Result<KeyProducer, String> {
        if self.runs.is_empty() {
            let mut idx = Vec::new();
            sort_indices(&self.flat, self.stride, &mut idx);
            return Ok(KeyProducer::Fast(FastProducer {
                flat: self.flat,
                idx,
                stride: self.stride,
                pos: 0,
            }));
        }

        // Spill the final partial buffer so the merge is uniform over disk runs.
        self.spill_run()?;
        let stride = self.stride;
        let total: usize = self.runs.iter().sum();
        let fd = self.spill.as_ref().expect("spill fd after >= 1 run").as_raw_fd();
        let map = Mmap::from_fd(fd, total * stride, Advice::Sequential)
            .map_err(|e| format!("external sort: mmap spill file failed: {e}"))?;

        // Per-run geometry: byte offset of each run's first record and its
        // record count. Runs are non-empty, so every source primes at row 0.
        let mut run_starts = Vec::with_capacity(self.runs.len());
        let mut run_lens = Vec::with_capacity(self.runs.len());
        let mut off = 0usize;
        for &records in &self.runs {
            run_starts.push(off);
            run_lens.push(records as u32);
            off += records * stride;
        }
        let tree = {
            let bytes = map.as_slice();
            let less = record_less(bytes, &run_starts, stride);
            LoserTree::build(self.runs.len(), |_| Some(0u32), less)
        };
        // `self` (and its `spill` OwnedFd) drop on return, closing the fd; the
        // mapping keeps the inode alive until the producer drops.
        Ok(KeyProducer::Merge(MergeProducer {
            map,
            stride,
            run_starts,
            run_lens,
            tree,
            remaining: total,
        }))
    }
}

/// The record at `(source_idx, row)` of the mapped spill file.
#[inline]
fn record_at<'a>(bytes: &'a [u8], run_starts: &[usize], stride: usize, n: &HeapNode) -> &'a [u8] {
    let off = run_starts[n.source_idx as usize] + n.row as usize * stride;
    &bytes[off..off + stride]
}

/// The keyless `LoserTree` comparator: byte order of the records the nodes
/// point at, read straight from the mapped file.
fn record_less<'a>(
    bytes: &'a [u8],
    run_starts: &'a [usize],
    stride: usize,
) -> impl Fn(&HeapNode, &HeapNode) -> bool + 'a {
    move |a, b| {
        compare_pk_bytes(
            record_at(bytes, run_starts, stride, a),
            record_at(bytes, run_starts, stride, b),
        )
        .is_lt()
    }
}

/// Fast-path producer: the in-RAM flat buffer lent out in sorted-index order.
pub struct FastProducer {
    flat: Vec<u8>,
    idx: Vec<u32>,
    stride: usize,
    pos: usize,
}

impl FastProducer {
    fn next(&mut self) -> Option<&[u8]> {
        let &i = self.idx.get(self.pos)?;
        self.pos += 1;
        let o = i as usize * self.stride;
        Some(&self.flat[o..o + self.stride])
    }
}

/// Merge-path producer: a streaming k-way merge over the mapped spill runs.
pub struct MergeProducer {
    map: Mmap,
    stride: usize,
    run_starts: Vec<usize>,
    run_lens: Vec<u32>,
    tree: LoserTree,
    remaining: usize,
}

impl MergeProducer {
    fn next(&mut self) -> Option<&[u8]> {
        if self.tree.is_empty() {
            return None;
        }
        let (src, row) = {
            let n = self.tree.peek();
            (n.source_idx as usize, n.row)
        };
        let bytes = self.map.as_slice();
        let less = record_less(bytes, &self.run_starts, self.stride);
        self.tree
            .step_top((row + 1 < self.run_lens[src]).then_some(row + 1), &less);
        self.remaining -= 1;
        let off = self.run_starts[src] + row as usize * self.stride;
        Some(&bytes[off..off + self.stride])
    }
}

/// The globally-sorted record stream `finish` yields, lending one
/// `stride`-byte record per `next` call (no per-record copy or allocation).
/// Infallible: with an `mmap`'d merge there is no read syscall to fail
/// mid-stream (an I/O fault on a mapped page is a SIGBUS that kills the
/// process — never a silently truncated stream), and all spill writes already
/// completed in `push` / `finish`.
pub enum KeyProducer {
    Fast(FastProducer),
    Merge(MergeProducer),
}

impl KeyProducer {
    /// Lend the next sorted record, or `None` when drained.
    // Not `Iterator::next`: the record is lent out of `self`, so the returned
    // borrow outlives no second call — a shape `Iterator` cannot express.
    #[allow(clippy::should_implement_trait)]
    #[inline]
    pub fn next(&mut self) -> Option<&[u8]> {
        match self {
            KeyProducer::Fast(p) => p.next(),
            KeyProducer::Merge(p) => p.next(),
        }
    }

    /// Records not yet yielded. Exact — lets consumers size buffers and place
    /// end-of-stream markers without lookahead.
    #[inline]
    pub fn remaining(&self) -> usize {
        match self {
            KeyProducer::Fast(p) => p.idx.len() - p.pos,
            KeyProducer::Merge(p) => p.remaining,
        }
    }
}

#[cfg(test)]
#[path = "tests/spill.rs"]
mod tests;
