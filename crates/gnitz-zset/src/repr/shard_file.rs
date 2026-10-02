//! Shard image encoding and writing, reached through `Batch::write_as_shard`.

use std::borrow::Cow;
use std::fs::OpenOptions;
use std::os::unix::fs::{FileExt, OpenOptionsExt};

use super::batch::{Batch, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::layout::*;
use super::shard_filter;
use super::string_heap::relocate_german_string_vec;
use crate::repr::error::StorageError;
use crate::schema::key::probe_key;
use crate::schema::SchemaDescriptor;
use gnitz_wire::{german_string_content, read_u64_le, write_u64_le, FixedInt};
use rustc_hash::{FxHashMap, FxHashSet};
use xorf::BinaryFuse8;

/// Whether every `width`-byte element of `region` is the same. Every element
/// equals the next exactly when the region equals itself shifted by one
/// element: one `bcmp` for the whole region.
fn is_constant(region: &[u8], width: usize) -> bool {
    region[width..] == region[..region.len() - width]
}

/// Fixed-width region `i`'s on-disk encoding and image. `pack_ints` admits FoR
/// on fixed-int payload regions, and on any payload region but a string's the
/// smaller of its non-NULL cells alone and a dictionary, either no larger than
/// half of what the region would else take: both cost a decode a frame does
/// not. `nulls` is the batch's null region.
fn encode_region<'a>(
    schema: &SchemaDescriptor,
    i: usize,
    src: &'a [u8],
    nulls: &[u8],
    pack_ints: bool,
) -> (Encoding, Cow<'a, [u8]>) {
    let n = nulls.len() / 8;
    let width = src.len() / n;
    if is_constant(src, width) {
        return (Encoding::Constant, Cow::Borrowed(&src[..width]));
    }
    let packed = if i == REG_WEIGHT || i == REG_NULL_BMP {
        let word = if i == REG_WEIGHT { FixedInt::I64 } else { FixedInt::U64 };
        two_value_encode(src)
            .map(|image| (Encoding::TwoValue, image))
            .or_else(|| for_encode(src, word).map(|image| (Encoding::For, image)))
    } else if pack_ints && i >= REG_PAYLOAD_START {
        let pi = i - REG_PAYLOAD_START;
        let col = &schema.columns[schema.payload_col_idx(pi)];
        let framed = col.fixed_int().and_then(|fi| for_encode(src, fi));
        let limit = framed.as_ref().map_or(src.len(), Vec::len) / 2;
        let is_null = |row: usize| read_u64_le(nulls, row * 8) >> pi & 1 != 0;
        // A column under half NULL has more cells than half the region holds.
        let sparse =
            (col.nullable && !col.type_code.is_german_string() && (0..n).filter(|&r| is_null(r)).count() > n / 2)
                .then(|| sparse_encode(src, width, col.fixed_int(), is_null))
                .filter(|image| region_start(image.len()) < region_start(limit));
        let limit = sparse.as_ref().map_or(limit, Vec::len);
        let dict = (!col.type_code.is_german_string()
            && dict_image_len(n, 2) < limit
            && sample_repeats(n, |row| &src[row * width..][..width]))
        .then(|| dict_fixed_column(src, width, limit))
        .flatten();
        match (dict, sparse) {
            (Some(image), _) => Some((Encoding::Dict, image)),
            (None, Some(image)) => Some((Encoding::Sparse, image)),
            (None, None) => framed.map(|image| (Encoding::For, image)),
        }
    } else {
        None
    };
    match packed {
        Some((encoding, image)) => (encoding, Cow::Owned(image)),
        None => (Encoding::Raw, Cow::Borrowed(src)),
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
            if next == DICT_MAX_ENTRIES || region_start(dict_image_len(n, next + 1)) >= region_start(limit) {
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
/// is priced without the pass that tells its values apart.
fn pack_string_column<'a>(cells: &'a [[u8; 16]], src_heap: &'a [u8], heap: &mut Vec<u8>) -> (Encoding, Vec<u8>) {
    let content = |cell: &'a [u8; 16]| german_string_content(cell, src_heap);
    let (at, raw) = (heap.len(), region_start(cells.len() * 16));
    let (mut span, mut short, mut long) = ((usize::MAX, 0), 0, 0);
    for len in cells.iter().map(|cell| content(cell).len()) {
        span = (span.0.min(len), span.1.max(len));
        *if len > gnitz_wire::SHORT_STRING_THRESHOLD {
            &mut long
        } else {
            &mut short
        } += len;
    }
    let seq = region_start(seq_image_len(cells.len(), span, short)) + long;
    if raw + long <= seq || sample_repeats(cells.len(), |row| content(&cells[row])) {
        let mut index: FxHashMap<Content<'_>, u32> = FxHashMap::default();
        let mut entries: Vec<[u8; 16]> = Vec::new();
        let ids: Vec<u32> = cells
            .iter()
            .map(|cell| {
                *index.entry(Content(content(cell))).or_insert_with(|| {
                    entries.push(relocate_german_string_vec(cell, src_heap, heap, None));
                    (entries.len() - 1) as u32
                })
            })
            .collect();
        if entries.len() == 1 {
            return (Encoding::Constant, entries[0].to_vec());
        }
        let shared = heap.len() - at;
        let dict = region_start(dict_image_len(ids.len(), entries.len()));
        if entries.len() <= DICT_MAX_ENTRIES && dict < raw && dict + shared <= seq {
            return (Encoding::Dict, dict_encode(&entries, &ids));
        }
        if raw + shared <= seq {
            return (Encoding::Raw, ids.iter().flat_map(|&id| entries[id as usize]).collect());
        }
        heap.truncate(at);
    }
    (
        Encoding::Seq,
        seq_encode(cells.len(), span, cells.iter().map(content), heap),
    )
}

/// The `(size, encoding)` projection of a region's [`DirEntry`], for the
/// shard-format assertions here and in the compaction / shard-reader test
/// modules. The entry shape itself lives in `layout`.
#[cfg(test)]
pub(crate) fn region_dir(image: &[u8], i: usize) -> (usize, Encoding) {
    let e = DirEntry::read(image, i);
    (e.size, Encoding::from_byte(e.encoding).unwrap())
}

fn build_shard_filter_from_pk_region(pk_bytes: &[u8], stride: usize) -> Option<BinaryFuse8> {
    // One hashed key per distinct PK. The PK region is sorted, so rows that
    // share a PK but differ in payload (valid under (PK, payload) element
    // identity) are adjacent — skipping chunks byte-equal to their predecessor
    // is an allocation-free O(n) pre-shrink that bounds `build`'s sort at the
    // number of *distinct* PKs in the region rather than its row count.
    // `probe_key` is the derivation the probe side must match exactly.
    let mut keys: Vec<u64> = Vec::with_capacity(pk_bytes.len() / stride);
    let mut prev: Option<&[u8]> = None;
    for chunk in pk_bytes.chunks_exact(stride) {
        if prev == Some(chunk) {
            continue;
        }
        prev = Some(chunk);
        keys.push(probe_key(chunk));
    }
    shard_filter::build(keys)
}

/// Per-call policy for [`Batch::write_as_shard`].
#[derive(Clone, Copy, Default)]
pub struct ShardWriteOpts {
    /// FoR on fixed-int payload regions, and a dictionary on any but a string's.
    pub pack_ints: bool,
    /// Stamp [`SHARD_FLAG_SKELETON`].
    pub skeleton: bool,
    /// Write no PK filter — named for what it turns off, so `Default` builds one.
    pub skip_pk_filter: bool,
}

impl Batch {
    /// Write this batch as a new, unsynced shard at `path`; `Err` if `path` exists.
    pub fn write_as_shard(&self, path: &str, opts: ShardWriteOpts) -> Result<(), StorageError> {
        let schema = self.schema();
        let n = self.count;
        assert!(n > 0, "every writer skips an empty output");
        self.debug_verify_dead_heap();
        let regions = self.wire_regions();
        let npc = schema.num_payload_cols();
        self.debug_verify_consolidated();
        debug_assert!(
            !opts.skeleton || npc == 0,
            "a skeleton shard must be written under the PK-only projection of its relation's schema",
        );
        // A store nothing point-probes writes no filter.
        let filter = (!opts.skip_pk_filter)
            .then(|| build_shard_filter_from_pk_region(regions[REG_PK], schema.pk_stride()))
            .flatten()
            .map(|f| shard_filter::serialize(&f));
        // Blob and filter are Raw; every other region is fixed-width.
        let fixed = &regions[..regions.len() - 1];
        let mut images: Vec<(Encoding, Cow<[u8]>)> = fixed
            .iter()
            .enumerate()
            .map(|(i, &src)| encode_region(schema, i, src, regions[REG_NULL_BMP], opts.pack_ints))
            .collect();
        // The string columns, over a heap that holds no dead byte.
        let (mb, mut heap) = (self.as_mem_batch(), Vec::new());
        for pi in gnitz_wire::BitIter(schema.string_payload_slots()) {
            let cells = mb.col_data(pi, 16).as_chunks::<16>().0;
            let (encoding, image) = pack_string_column(cells, mb.blob, &mut heap);
            images[REG_PAYLOAD_START + pi] = (encoding, Cow::Owned(image));
        }
        images.push((Encoding::Raw, Cow::Owned(heap)));
        images.push((Encoding::Raw, Cow::Borrowed(filter.as_deref().unwrap_or(&[]))));

        static PAD: [u8; ALIGNMENT] = [0; ALIGNMENT];
        let file = OpenOptions::new().write(true).create_new(true).mode(0o644).open(path)?;
        let mut header = vec![0u8; desc_len(npc)];
        let mut body = gnitz_wire::RowHasher::default();
        let mut end = header.len();
        for (i, (encoding, image)) in images.iter().enumerate() {
            for bytes in [&PAD[..region_start(end) - end], &image[..]] {
                body.update(bytes);
                file.write_all_at(bytes, end as u64)?;
                end += bytes.len();
            }
            DirEntry {
                size: image.len(),
                encoding: *encoding as u8,
            }
            .write(&mut header, i);
        }

        ShardHeader {
            row_count: n,
            retractions: self.retracted_rows().count(),
            file_npc: npc,
            skeleton: opts.skeleton,
            body_checksum: body.digest(),
        }
        .write(&mut header);
        let desc = desc_digest(path, &header);
        write_u64_le(&mut header, OFF_DESC_CHECKSUM, desc);
        file.write_all_at(&header, 0)?;
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/shard_file.rs"]
mod tests;
