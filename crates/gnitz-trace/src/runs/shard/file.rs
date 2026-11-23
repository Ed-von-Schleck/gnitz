//! Shard image encoding and writing, reached through `write_shard` and
//! `GuardRun::write_as_shard`.

use std::borrow::Cow;
use std::fs::OpenOptions;
use std::io::IoSlice;
use std::os::unix::fs::OpenOptionsExt;

use super::codebook::Codebook;
use super::encoding::*;
use super::error::StorageError;
use super::layout::*;
use super::pk_filter;
use gnitz_expr::Level;
use gnitz_wire::{FIXED_REGION_BYTES, FixedInt, Heap, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT, RowSource};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaColumn;
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
pub(super) fn pack_fixed_column(
    col: &SchemaColumn,
    pi: usize,
    src: &[u8],
    nulls: &[u8],
) -> Option<(Encoding, Vec<u8>)> {
    let n = nulls.len() / FIXED_REGION_BYTES;
    let width = col.size();
    let fi = FixedInt::from_type_code(col.type_code);
    let framed = fi
        .and_then(|fi| for_encode(src, fi))
        .filter(|image| aligned(image.len()) < aligned(src.len()));
    let limit = framed.as_ref().map_or(src.len(), Vec::len) / 2;
    let held = col.nullable.then(|| {
        let mut held = vec![0u64; n.div_ceil(64)];
        held_bits(Level::new(), nulls, pi, &mut held);
        held
    });
    let present = |held: &[u64]| held.iter().map(|w| w.count_ones() as usize).sum::<usize>();
    // A column under half NULL has more cells than half the region holds.
    let sparse = held
        .filter(|held| n - present(held) > n / 2)
        .map(|held| sparse_encode(src, width, fi, &held))
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
/// NULL string cell reads as the empty string, which counts as a repeat only
/// where such cells are most of the sample; a NULL fixed cell reads as zeroes.
/// Either way a column of mostly NULLs repeats.
fn sample_repeats<'a>(n: usize, content: impl Fn(usize) -> &'a [u8]) -> bool {
    let stride = (n / SAMPLE_RUNS).max(SAMPLE_RUN);
    let mut seen = FxHashSet::with_capacity_and_hasher(n.min(SAMPLE_RUNS * SAMPLE_RUN), Default::default());
    let (mut sampled, mut empty) = (0usize, 0usize);
    (0..n)
        .step_by(stride)
        .flat_map(|run| run..(run + SAMPLE_RUN).min(n))
        .any(|row| {
            sampled += 1;
            let c = content(row);
            empty += c.is_empty() as usize;
            !c.is_empty() && !seen.insert(Content(c))
        })
        || empty * 2 > sampled
}

/// The Dict image of a column of `width`-byte cells, or `None` unless it is
/// smaller than `limit` bytes — which the pass stops at, so a column of values
/// that seldom repeat costs the rows up to its first entry too many.
fn dict_fixed_column(src: &[u8], width: usize, limit: usize) -> Option<Vec<u8>> {
    let n = src.len() / width;
    let mut index: FxHashMap<Content<'_>, u32> =
        FxHashMap::with_capacity_and_hasher(n.min(DICT_MAX_ENTRIES + 1), Default::default());
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
/// at the first value past which the lengths are the smaller. The pass reaches
/// the heap only once a dictionary or a cell per row has won it.
fn pack_string_column<'a>(cells: &'a [[u8; 16]], src: Heap<'a>, heap: &mut HeapWriter<'_>) -> (Encoding, Vec<u8>) {
    if is_constant(cells.as_flattened(), 16) {
        return (Encoding::Constant, heap.place(&cells[0], src).to_vec());
    }
    let raw = aligned(cells.len() * 16);
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
        let mut index: FxHashMap<(usize, Content<'_>), u32> =
            FxHashMap::with_capacity_and_hasher(cells.len().min(DICT_MAX_ENTRIES + 1), Default::default());
        // Each distinct value's first cell, and the heap bytes their long values take.
        let mut firsts: Vec<&[u8; 16]> = Vec::new();
        let mut shared = 0;
        let mut ids: Vec<u32> = Vec::with_capacity(cells.len());
        for cell in cells {
            let next = firsts.len() as u32;
            let (len, bytes) = stored_value(cell, src);
            let id = *index.entry((len, Content(bytes))).or_insert(next);
            if id == next {
                firsts.push(cell);
                if len > gnitz_wire::SHORT_STRING_THRESHOLD {
                    shared += len;
                }
                if next > 0 && sharing(firsts.len(), shared).is_none() {
                    break;
                }
            }
            ids.push(id);
        }
        let shares = sharing(firsts.len(), shared);
        if firsts.len() == 1 || shares.is_some() {
            let entries: Vec<[u8; 16]> = firsts.iter().map(|cell| heap.place(cell, src)).collect();
            if entries.len() == 1 {
                return (Encoding::Constant, entries[0].to_vec());
            }
            return match shares {
                Some(Encoding::Dict) => (Encoding::Dict, dict_encode(&entries, &ids)),
                _ => (Encoding::Raw, ids.iter().flat_map(|&id| entries[id as usize]).collect()),
            };
        }
    }
    (Encoding::Seq, seq_encode(cells, span, src, heap))
}

/// Per-call policy for writing a shard.
#[derive(Clone, Copy, Default)]
pub struct ShardWriteOpts<'a> {
    /// Write no PK filter.
    pub skip_pk_filter: bool,
    /// Code the heap under this codebook where it [pays](Codebook::pays) and
    /// the coded heap does (see `HeapWriter::coding_lost`).
    pub codebook: Option<&'a Codebook>,
}

/// Write `batch` as a new, unsynced shard at `path`; `Err` if `path` exists.
pub fn write_shard(batch: &Batch, path: &str, opts: ShardWriteOpts<'_>) -> Result<(), StorageError> {
    write_shard_over(batch, batch, path, opts, false)
}

/// [`write_shard`] of `batch`'s rows as `rows` reads them: over the heap its
/// blob holds. `skeleton` sets the header's
/// skeleton flag.
pub(crate) fn write_shard_over(
    batch: &Batch,
    rows: &impl RowSource,
    path: &str,
    opts: ShardWriteOpts<'_>,
    skeleton: bool,
) -> Result<(), StorageError> {
    let schema = batch.schema();
    let n = batch.len();
    assert!(n > 0, "every writer skips an empty output");
    let src = rows.heap();
    if src.plain().is_some() {
        batch.debug_verify_dead_heap();
    }
    batch.debug_verify_consolidated(rows);
    let regions = batch.regions();
    let npc = schema.num_payload_cols();
    // A skeleton weight sums a per-key time prefix of a positive integral.
    debug_assert!(
        !skeleton || (npc == 0 && (0..n).all(|row| batch.get_weight(row) > 0)),
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
    let mut heap = (opts.codebook.filter(|b| b.pays())).map_or_else(HeapWriter::plain, HeapWriter::coded);
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

    // The file is laid out whole in memory: the header, then each image
    // behind the zero pad that aligns it. The body checksum covers the
    // same pads and images in the same order, so the header is stamped
    // before any byte is written and the file goes out in one vectored
    // write.
    static PAD: [u8; ALIGNMENT] = [0; ALIGNMENT];
    let mut header = vec![0u8; desc_len(npc)];
    let mut body = gnitz_wire::ChecksumStream::default();
    // The header's slot, filled once it is stamped.
    let mut slices: Vec<IoSlice> = Vec::with_capacity(1 + 2 * images.len());
    slices.push(IoSlice::new(&[]));
    let mut end = header.len();
    for (i, (encoding, image)) in images.iter().enumerate() {
        for bytes in [&PAD[..aligned(end) - end], &image[..]] {
            body.update(bytes);
            if !bytes.is_empty() {
                slices.push(IoSlice::new(bytes));
            }
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
        retractions: batch.retracted_rows().count(),
        file_npc: npc,
        skeleton,
        body_checksum: body.digest(),
    }
    .stamp(path, &mut header);
    slices[0] = IoSlice::new(&header);
    // A fresh file's cursor is at offset 0.
    let mut file = OpenOptions::new().write(true).create_new(true).mode(0o644).open(path)?;
    gnitz_foundation::posix_io::write_all_vectored(&mut file, &mut slices)?;
    Ok(())
}

#[cfg(test)]
#[path = "tests/file.rs"]
mod tests;
