//! Bounded external sort of fixed-stride byte records: peak RAM is about one
//! run, whatever the input size. The spill file is `O_TMPFILE`, so no exit
//! leaves it on disk.
//!
//! [`SpillSort::push`] stores a record in a slot of its stride rounded up to 8
//! bytes, zero past the record, so the kernel compares whole words and the slot
//! orders as the record.

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
    if slot == 8 {
        // One word: sorting by the word itself compiles tighter than `cmp_records`.
        return flat
            .as_chunks_mut::<8>()
            .0
            .sort_unstable_by_key(|r| u64::from_be_bytes(*r));
    }
    macro_rules! by_width {
        ($($w:literal)*) => {
            match slot {
                $($w => flat.as_chunks_mut::<$w>().0.sort_unstable_by(|a, b| cmp_records(a, b)),)*
                _ => unreachable!("a slot is a multiple of 8 up to MAX_PK_BYTES"),
            }
        };
    }
    by_width!(16 24 32 40 48 56 64 72 80)
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

    /// Push a whole number of `stride`-byte records, spilling each run as it fills.
    pub fn push(&mut self, mut records: &[u8]) -> Result<(), String> {
        let (stride, slot) = (self.stride, self.slot);
        debug_assert!(records.len().is_multiple_of(stride));
        while !records.is_empty() {
            let room = self.run_len - self.flat.len() / slot;
            let (now, rest) = records.split_at(room.min(records.len() / stride) * stride);
            records = rest;
            if slot == stride {
                self.flat.extend_from_slice(now);
            } else {
                let at = self.flat.len();
                self.flat.resize(at + now.len() / stride * slot, 0);
                for (d, s) in self.flat[at..].chunks_exact_mut(slot).zip(now.chunks_exact(stride)) {
                    d[..stride].copy_from_slice(s);
                }
            }
            if self.flat.len() == self.run_len * slot {
                self.spill_run()?;
            }
        }
        Ok(())
    }

    /// Sort `flat` and append it to the spill file as one run.
    fn spill_run(&mut self) -> Result<(), String> {
        sort_records(&mut self.flat, self.slot);
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
        file.write_all(&self.flat)
            .map_err(|e| format!("external sort: spill write failed: {e}"))?;
        self.flat.clear();
        Ok(())
    }

    pub fn finish(mut self) -> Result<KeyProducer, String> {
        let records = if self.spill.is_none() {
            sort_records(&mut self.flat, self.slot);
            Records::Ram(self.flat)
        } else {
            // The last run goes to disk too, so `flat` is freed before the merge.
            self.spill_run()?;
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

/// Record `index`'s tournament key: its leading 16 bytes, or its one word, the
/// order of every record that differs in them.
#[inline(always)]
fn record_key(bytes: &[u8], slot: usize, index: usize) -> u128 {
    let record = &bytes[index * slot..];
    match slot {
        8 => u128::from(u64::from_be_bytes(*record.first_chunk().unwrap())),
        _ => u128::from_be_bytes(*record.first_chunk().unwrap()),
    }
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
            |run| Some((0, record_key(records.as_slice(), slot, run * run_len))),
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

    /// The next record of the merge across runs.
    #[inline]
    fn next(&mut self) -> Option<&[u8]> {
        let top = self.tree.peek()?;
        let g = record_index(&top, self.run_len);
        let more = (top.row as usize + 1) < self.run_len && g + 1 < self.total;
        let bytes = self.records.as_slice();
        self.tree.step_top(
            more.then(|| (top.row + 1, record_key(bytes, self.slot, g + 1))),
            &less(bytes, self.slot, self.run_len),
        );
        self.remaining -= 1;
        Some(&bytes[g * self.slot..][..self.stride])
    }

    /// Reset `chunk` to the next `max_rows` records, or as many as remain, each
    /// as a key-only row at weight 1; empty once none remain.
    pub fn fill(&mut self, chunk: &mut Batch, max_rows: usize) {
        chunk.clear();
        let n = self.remaining.min(max_rows);
        let (stride, slot) = (self.stride, self.slot);
        chunk.append_session(n).write(n, |w| {
            let (keys, weights, nulls) = w.fixed_mut();
            if self.total <= self.run_len {
                // One run: the records are in order already, so no merge steps.
                let at = self.total - self.remaining;
                let records = &self.records.as_slice()[at * slot..][..n * slot];
                if slot == stride {
                    keys.copy_from_slice(records);
                } else {
                    for (d, s) in keys.chunks_exact_mut(stride).zip(records.chunks_exact(slot)) {
                        d.copy_from_slice(&s[..stride]);
                    }
                }
                self.remaining -= n;
            } else {
                for d in keys.chunks_exact_mut(stride) {
                    d.copy_from_slice(self.next().expect("`remaining` records are left"));
                }
            }
            for w in weights.as_chunks_mut::<8>().0 {
                *w = 1i64.to_le_bytes();
            }
            nulls.fill(0);
        });
    }
}

#[cfg(test)]
#[path = "tests/spill.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/spill.rs"]
mod bench;
