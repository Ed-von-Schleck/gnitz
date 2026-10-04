//! Bounded external sort of fixed-stride byte records: peak RAM is about one
//! run, whatever the input size. The spill file is `O_TMPFILE`, so no exit
//! leaves it on disk.
//!
//! A record occupies a slot of its stride rounded up to 8 bytes, zero past the
//! record, so the kernel compares whole words and the slot orders as the record.

use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::fs::OpenOptionsExt;

use super::mmap::Mmap;
use gnitz_wire::MAX_PK_BYTES;

use super::batch::Batch;
use super::loser_tree::{HeapNode, LoserTree};

/// Unsigned byte order of two records, compared a big-endian word at a time.
#[inline(always)]
fn cmp_records(a: &[u8], b: &[u8]) -> Ordering {
    for (x, y) in a.as_chunks::<8>().0.iter().zip(b.as_chunks::<8>().0) {
        let (x, y) = (u64::from_be_bytes(*x), u64::from_be_bytes(*y));
        if x != y {
            return x.cmp(&y);
        }
    }
    Ordering::Equal
}

fn sort_records(flat: &mut [u8], slot: usize) {
    macro_rules! by_width {
        ($($w:literal)*) => {
            match slot {
                $($w => flat.as_chunks_mut::<$w>().0.sort_unstable_by(|a, b| cmp_records(a, b)),)*
                _ => unreachable!("a slot is a multiple of 8 up to MAX_PK_BYTES"),
            }
        };
    }
    by_width!(8 16 24 32 40 48 56 64 72 80)
}
const _: () = assert!(MAX_PK_BYTES == 80); // the arm list above is total

pub struct SpillSort {
    stride: usize,
    /// `stride` rounded up to whole 8-byte words.
    slot: usize,
    run_len: usize,
    dir: String,
    flat: Vec<u8>,
    spill: Option<File>,
}

impl SpillSort {
    /// `dir` names the filesystem the spill file goes on; a run is `budget`
    /// bytes of slots, rounded up to whole slots.
    pub fn new(dir: &str, stride: usize, budget: usize) -> Self {
        assert!((1..=MAX_PK_BYTES).contains(&stride), "record stride {stride}");
        let slot = stride.next_multiple_of(8);
        SpillSort {
            stride,
            slot,
            // `HeapNode::row` indexes a run with a `u32`.
            run_len: budget.div_ceil(slot).clamp(1, u32::MAX as usize),
            dir: dir.to_string(),
            flat: Vec::new(),
            spill: None,
        }
    }

    /// Bytes a record occupies in [`Self::push`]'s input.
    pub fn slot(&self) -> usize {
        self.slot
    }

    /// Push a whole number of slot-wide records, each zero past its `stride` bytes.
    pub fn push(&mut self, records: &[u8]) -> Result<(), String> {
        debug_assert!(records.len().is_multiple_of(self.slot));
        self.flat.extend_from_slice(records);
        let run = self.run_len * self.slot;
        let mut at = 0;
        while self.flat.len() - at >= run {
            self.spill_run(at, at + run)?;
            at += run;
        }
        self.flat.drain(..at);
        Ok(())
    }

    /// Sort `flat[from..to]` and append it to the spill file as one run.
    fn spill_run(&mut self, from: usize, to: usize) -> Result<(), String> {
        sort_records(&mut self.flat[from..to], self.slot);
        let file = match &mut self.spill {
            Some(f) => f,
            None => self.spill.insert(
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .mode(0o600)
                    .custom_flags(libc::O_TMPFILE)
                    .open(&self.dir)
                    .map_err(|e| format!("external sort: cannot create spill file in {}: {e}", self.dir))?,
            ),
        };
        file.write_all(&self.flat[from..to])
            .map_err(|e| format!("external sort: spill write failed: {e}"))
    }

    pub fn finish(mut self) -> Result<KeyProducer, String> {
        let records = if self.spill.is_none() {
            sort_records(&mut self.flat, self.slot);
            Records::Ram(self.flat)
        } else {
            // The last run goes to disk too, so `flat` is freed before the merge.
            self.spill_run(0, self.flat.len())?;
            let file = self.spill.take().expect("spilled");
            // The mapping keeps the inode alive after `file` closes.
            let map = Mmap::from_file(&file).map_err(|e| format!("external sort: mmap spill file failed: {e}"))?;
            map.advise_sequential();
            Records::Mapped(map)
        };
        Ok(KeyProducer::new(records, self.stride, self.slot, self.run_len))
    }
}

/// The sorted runs, back to back.
enum Records {
    Ram(Vec<u8>),
    Mapped(Mmap),
}

impl Records {
    fn as_slice(&self) -> &[u8] {
        match self {
            Records::Ram(v) => v,
            Records::Mapped(m) => m.as_slice(),
        }
    }
}

/// The record index of `n`'s row: every run but the last holds `run_len` records.
fn record_index(n: &HeapNode, run_len: usize) -> usize {
    n.source_idx as usize * run_len + n.row as usize
}

fn less(bytes: &[u8], slot: usize, run_len: usize) -> impl Fn(&HeapNode, &HeapNode) -> bool + '_ {
    let rec = move |n: &HeapNode| &bytes[record_index(n, run_len) * slot..][..slot];
    move |a, b| cmp_records(rec(a), rec(b)).is_lt()
}

/// The records in sorted order, merged across runs.
pub struct KeyProducer {
    records: Records,
    stride: usize,
    slot: usize,
    run_len: usize,
    total: usize,
    remaining: usize,
    tree: LoserTree,
}

impl KeyProducer {
    fn new(records: Records, stride: usize, slot: usize, run_len: usize) -> Self {
        let total = records.as_slice().len() / slot;
        let tree = LoserTree::build(
            total.div_ceil(run_len),
            |_| Some(0),
            less(records.as_slice(), slot, run_len),
        );
        KeyProducer {
            records,
            stride,
            slot,
            run_len,
            total,
            remaining: total,
            tree,
        }
    }

    // Not `Iterator::next`: the record is lent out of `self`, so the returned
    // borrow outlives no second call — a shape `Iterator` cannot express.
    #[allow(clippy::should_implement_trait)]
    #[inline]
    pub fn next(&mut self) -> Option<&[u8]> {
        let top = self.tree.peek()?;
        let g = record_index(&top, self.run_len);
        let more = (top.row as usize + 1) < self.run_len && g + 1 < self.total;
        let bytes = self.records.as_slice();
        self.tree
            .step_top(more.then_some(top.row + 1), &less(bytes, self.slot, self.run_len));
        self.remaining -= 1;
        Some(&bytes[g * self.slot..][..self.stride])
    }

    #[inline]
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    /// Reset `chunk` to the next `max_rows` records, or as many as remain, each
    /// as a key-only row at weight 1.
    pub fn fill(&mut self, chunk: &mut Batch, max_rows: usize) {
        chunk.clear();
        for _ in 0..self.remaining.min(max_rows) {
            chunk.push_key_row(self.next().expect("`remaining` records are left"), 1);
        }
    }
}

#[cfg(test)]
#[path = "tests/spill.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/spill.rs"]
mod bench;
