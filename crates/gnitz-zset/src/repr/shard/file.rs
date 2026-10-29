//! Shard image encoding and writing, reached through `Batch::write_as_shard`
//! and `GuardRun::write_as_shard`.

use std::borrow::Cow;
use std::fs::OpenOptions;
use std::os::unix::fs::{FileExt, OpenOptionsExt};

use super::codebook::Codebook;
use super::encoding::*;
use super::error::StorageError;
use super::layout::*;
use super::pk_filter;
use crate::repr::Batch;
use crate::schema::SchemaColumn;
use gnitz_wire::{
    read_u64_le, FixedInt, Heap, RowSource, FIXED_REGION_BYTES, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT,
};
use rustc_hash::{FxHashMap, FxHashSet};

/// Whether every `width`-byte element of `region` is the same. Every element
/// equals the next exactly when the region equals itself shifted by one
/// element: one `bcmp` for the whole region.
fn is_constant(region: &[u8], width: usize) -> bool {
    region[width..] == region[..region.len() - width]
}

/// A region of `width`-byte elements as it is written: its one element where
/// they are all the same, else what `pack` makes of it where that leaves the
/// region fewer aligned bytes, else the region itself. A win that vanishes after
/// alignment saves no disk and still costs a decode.
fn encode_region(
    src: &[u8],
    width: usize,
    pack: impl FnOnce() -> Option<(Encoding, Vec<u8>)>,
) -> (Encoding, Cow<'_, [u8]>) {
    if is_constant(src, width) {
        return (Encoding::Constant, Cow::Borrowed(&src[..width]));
    }
    match pack().filter(|(_, image)| aligned(image.len()) < aligned(src.len())) {
        Some((encoding, image)) => (encoding, Cow::Owned(image)),
        None => (Encoding::Raw, Cow::Borrowed(src)),
    }
}

/// The packed image of a weight or null region, whose words are `word`s.
fn pack_words(src: &[u8], word: FixedInt) -> Option<(Encoding, Vec<u8>)> {
    two_value_encode(src)
        .map(|image| (Encoding::TwoValue, image))
        .or_else(|| for_encode(src, word).map(|image| (Encoding::For, image)))
}

/// The packed image of `col`, payload column `pi` and no string's, under the
/// batch's null region `nulls`: its frame, unless its non-NULL cells alone or a
/// dictionary — the smaller of the two — take no more than half of what the
/// region would else, since both cost a decode a frame does not.
fn pack_fixed_column(col: &SchemaColumn, pi: usize, src: &[u8], nulls: &[u8]) -> Option<(Encoding, Vec<u8>)> {
    let n = nulls.len() / FIXED_REGION_BYTES;
    let width = col.size();
    let fi = FixedInt::from_type_code(col.type_code);
    let framed = fi
        .and_then(|fi| for_encode(src, fi))
        .filter(|image| aligned(image.len()) < aligned(src.len()));
    let limit = framed.as_ref().map_or(src.len(), Vec::len) / 2;
    let is_null = |row: usize| gnitz_wire::null_word_get(read_u64_le(nulls, row * FIXED_REGION_BYTES), pi);
    // A column under half NULL has more cells than half the region holds.
    let sparse = (col.nullable && (0..n).filter(|&r| is_null(r)).count() > n / 2)
        .then(|| sparse_encode(src, width, fi, is_null))
        .filter(|image| aligned(image.len()) < aligned(limit));
    let limit = sparse.as_ref().map_or(limit, Vec::len);
    let dict = (dict_image_len(n, 2) < limit && sample_repeats(n, |row| &src[row * width..][..width]))
        .then(|| dict_fixed_column(src, width, limit))
        .flatten();
    match (dict, sparse) {
        (Some(image), _) => Some((Encoding::Dict, image)),
        (None, Some(image)) => Some((Encoding::Sparse, image)),
        (None, None) => framed.map(|image| (Encoding::For, image)),
    }
}

/// A string's content as a map key, hashed with XXH3: the content is the
/// client's, and Fx collides on chosen input.
#[derive(PartialEq, Eq)]
struct Content<'a>(&'a [u8]);

impl std::hash::Hash for Content<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write_u64(gnitz_wire::checksum(self.0));
    }
}

/// Cells per run of a [`sample_repeats`] sample, and runs per sample.
const SAMPLE_RUN: usize = 64;
const SAMPLE_RUNS: usize = 64;

/// Whether a sample of a column's `n` rows holds one content twice. The sample
/// is runs of adjacent rows spread evenly over the column, so it sees a value
/// repeated across the column and one repeated only in a run of neighbours. A
/// NULL cell reads as zeroes or the empty string, so a column of mostly NULLs
/// repeats.
fn sample_repeats<'a>(n: usize, content: impl Fn(usize) -> &'a [u8]) -> bool {
    let stride = (n / SAMPLE_RUNS).max(SAMPLE_RUN);
    let mut seen = FxHashSet::default();
    (0..n)
        .step_by(stride)
        .flat_map(|run| run..(run + SAMPLE_RUN).min(n))
        .any(|row| !seen.insert(Content(content(row))))
}

/// The Dict image of a column of `width`-byte cells, or `None` unless it is
/// smaller than `limit` bytes — which the pass stops at, so a column of values
/// that seldom repeat costs the rows up to its first entry too many.
fn dict_fixed_column(src: &[u8], width: usize, limit: usize) -> Option<Vec<u8>> {
    let n = src.len() / width;
    let mut index: FxHashMap<Content<'_>, u32> = FxHashMap::default();
    let mut entries: Vec<[u8; 16]> = Vec::new();
    let mut ids = Vec::with_capacity(n);
    for cell in src.chunks_exact(width) {
        let next = entries.len();
        let id = *index.entry(Content(cell)).or_insert(next as u32);
        if id as usize == next {
            if next == DICT_MAX_ENTRIES || aligned(dict_image_len(n, next + 1)) >= aligned(limit) {
                return None;
            }
            let mut entry = [0u8; 16];
            entry[..width].copy_from_slice(cell);
            entries.push(entry);
        }
        ids.push(id);
    }
    Some(dict_encode(&entries, &ids))
}

/// A string column's image over `heap`, which gains the column's long values:
/// each distinct one once under a dictionary or a cell per row, every row's
/// under the lengths alone. The image is whichever of the three leaves the
/// smaller region and heap — a dictionary only while the values fit one — and
/// one cell for a column of one value. A column whose sample repeats no value
/// is priced without the pass that tells its values apart, and the pass stops
/// at the first value past which the lengths are the smaller.
fn pack_string_column<'a>(cells: &'a [[u8; 16]], src: Heap<'a>, heap: &mut HeapWriter<'_>) -> (Encoding, Vec<u8>) {
    if is_constant(cells.as_flattened(), 16) {
        return (Encoding::Constant, heap.place(&cells[0], src).to_vec());
    }
    let (at, raw) = (heap.mark(), aligned(cells.len() * 16));
    let (mut span, mut short, mut long) = ((usize::MAX, 0), 0, 0);
    for len in cells.iter().map(|cell| stored_value(cell, src).0) {
        span = (span.0.min(len), span.1.max(len));
        *if len > gnitz_wire::SHORT_STRING_THRESHOLD {
            &mut long
        } else {
            &mut short
        } += len;
    }
    let seq = aligned(seq_image_len(cells.len(), span, short)) + long;
    // The image that holds each of `entries` distinct values once, `shared`
    // heap bytes in all, if it is no larger than the lengths. Both only grow
    // over the pass, so a `None` stays one.
    let sharing = |entries: usize, shared: usize| {
        let dict = aligned(dict_image_len(cells.len(), entries));
        if entries <= DICT_MAX_ENTRIES && dict < raw && dict + shared <= seq {
            Some(Encoding::Dict)
        } else {
            (raw + shared <= seq).then_some(Encoding::Raw)
        }
    };
    if raw + long <= seq || sample_repeats(cells.len(), |row| stored_value(&cells[row], src).1) {
        let mut index: FxHashMap<(usize, Content<'_>), u32> = FxHashMap::default();
        let mut entries: Vec<[u8; 16]> = Vec::new();
        let mut ids: Vec<u32> = Vec::with_capacity(cells.len());
        for cell in cells {
            let next = entries.len() as u32;
            let (len, bytes) = stored_value(cell, src);
            let id = *index.entry((len, Content(bytes))).or_insert(next);
            if id == next {
                entries.push(heap.place(cell, src));
                if next > 0 && sharing(entries.len(), heap.plain_len() - at.1).is_none() {
                    break;
                }
            }
            ids.push(id);
        }
        if entries.len() == 1 {
            return (Encoding::Constant, entries[0].to_vec());
        }
        match sharing(entries.len(), heap.plain_len() - at.1) {
            Some(Encoding::Dict) => return (Encoding::Dict, dict_encode(&entries, &ids)),
            Some(_) => return (Encoding::Raw, ids.iter().flat_map(|&id| entries[id as usize]).collect()),
            None => heap.truncate(at),
        }
    }
    (Encoding::Seq, seq_encode(cells, span, src, heap))
}

/// Per-call policy for writing a shard.
#[derive(Clone, Copy, Default)]
pub struct ShardWriteOpts<'a> {
    /// Write no PK filter — named for what it turns off, so `Default` builds one.
    pub skip_pk_filter: bool,
    /// Code the heap under this codebook, unless that leaves it more than three
    /// quarters of its bytes.
    pub codebook: Option<&'a Codebook>,
}

impl Batch {
    /// Write this batch as a new, unsynced shard at `path`; `Err` if `path` exists.
    pub fn write_as_shard(&self, path: &str, opts: ShardWriteOpts<'_>) -> Result<(), StorageError> {
        self.write_shard_over(self, path, opts, false)
    }

    /// [`write_as_shard`](Self::write_as_shard) of this batch's rows as `rows`
    /// reads them: over the heap its blob holds. `skeleton` sets the header's
    /// skeleton flag.
    pub(crate) fn write_shard_over(
        &self,
        rows: &impl RowSource,
        path: &str,
        opts: ShardWriteOpts<'_>,
        skeleton: bool,
    ) -> Result<(), StorageError> {
        let schema = self.schema();
        let n = self.count;
        assert!(n > 0, "every writer skips an empty output");
        let src = rows.heap();
        if src.plain().is_some() {
            self.debug_verify_dead_heap();
        }
        self.debug_verify_consolidated(rows);
        let regions = self.wire_regions();
        let npc = schema.num_payload_cols();
        // A skeleton weight sums a per-key time prefix of a positive integral.
        debug_assert!(
            !skeleton || (npc == 0 && (0..n).all(|row| self.get_weight(row) > 0)),
            "a skeleton shard is the PK-only projection of its relation's schema at positive coarse weights",
        );
        // A store nothing point-probes writes no filter.
        let filter = (!opts.skip_pk_filter)
            .then(|| pk_filter::build(regions[REG_PK], schema.pk_stride()))
            .flatten();
        let nulls = regions[REG_NULL_BMP];
        let mut images: Vec<(Encoding, Cow<[u8]>)> = Vec::with_capacity(regions.len() + 1);
        images.push(encode_region(regions[REG_PK], schema.pk_stride(), || {
            key_encode(regions[REG_PK], schema).map(|image| (Encoding::Key, image))
        }));
        images.push(encode_region(regions[REG_WEIGHT], FIXED_REGION_BYTES, || {
            pack_words(regions[REG_WEIGHT], FixedInt::I64)
        }));
        images.push(encode_region(nulls, FIXED_REGION_BYTES, || {
            pack_words(nulls, FixedInt::U64)
        }));
        // The string columns share a heap that holds no dead byte, packed again
        // into a plain one where coding it saved too little.
        let pack_strings = |heap: &mut HeapWriter| -> Vec<Option<(Encoding, Vec<u8>)>> {
            schema
                .payload_columns()
                .map(|(pi, col)| {
                    let cells = regions[REG_PAYLOAD_START + pi].as_chunks::<16>().0;
                    col.type_code
                        .is_german_string()
                        .then(|| pack_string_column(cells, src, heap))
                })
                .collect()
        };
        let mut heap = opts.codebook.map_or_else(HeapWriter::plain, HeapWriter::coded);
        let mut strings = pack_strings(&mut heap);
        if heap.coding_lost() {
            heap = HeapWriter::plain();
            strings = pack_strings(&mut heap);
        }
        for ((pi, col), string) in schema.payload_columns().zip(strings) {
            let src = regions[REG_PAYLOAD_START + pi];
            images.push(match string {
                Some((encoding, image)) => (encoding, Cow::Owned(image)),
                None => encode_region(src, col.size(), || pack_fixed_column(col, pi, src, nulls)),
            });
        }
        let (encoding, heap) = heap.finish();
        images.push((encoding, Cow::Owned(heap)));
        images.push((Encoding::Raw, Cow::Borrowed(filter.as_deref().unwrap_or(&[]))));

        static PAD: [u8; ALIGNMENT] = [0; ALIGNMENT];
        let file = OpenOptions::new().write(true).create_new(true).mode(0o644).open(path)?;
        let mut header = vec![0u8; desc_len(npc)];
        let mut body = gnitz_wire::ChecksumStream::default();
        let mut end = header.len();
        for (i, (encoding, image)) in images.iter().enumerate() {
            for bytes in [&PAD[..aligned(end) - end], &image[..]] {
                body.update(bytes);
                file.write_all_at(bytes, end as u64)?;
                end += bytes.len();
            }
            DirEntry {
                size: image.len(),
                encoding: encoding.as_wire(),
            }
            .write(&mut header, i);
        }

        ShardHeader {
            row_count: n,
            retractions: self.retracted_rows().count(),
            file_npc: npc,
            skeleton,
            body_checksum: body.digest(),
        }
        .stamp(path, &mut header);
        file.write_all_at(&header, 0)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/file.rs"]
mod tests;
