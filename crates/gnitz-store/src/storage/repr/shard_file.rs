//! Shard image encoding and atomic writing, reached through `Batch::write_as_shard`.

use std::borrow::Cow;
use std::ffi::CStr;
use std::os::unix::fs::FileExt;

use super::super::error::StorageError;
use super::super::StagedFile;
use super::batch::{Batch, MAX_WIRE_REGIONS, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::layout::*;
use super::shard_filter;
use crate::schema::key::probe_key;
use crate::schema::SchemaDescriptor;
use gnitz_wire::{read_i64_le, read_signed_exact, read_unsigned_exact, write_u64_le, FixedInt};
use xorf::BinaryFuse8;

/// Region `i`'s on-disk encoding and image. `blob` is the blob region's index;
/// `pack_ints` admits FoR on fixed-int payload regions.
fn encode_region<'a>(
    schema: &SchemaDescriptor,
    i: usize,
    src: &'a [u8],
    n: usize,
    blob: usize,
    pack_ints: bool,
) -> (u8, Cow<'a, [u8]>) {
    if n == 0 || i == blob {
        return (ENCODING_RAW, Cow::Borrowed(src));
    }
    let width = src.len() / n;
    // Every element equals the next exactly when the region equals itself
    // shifted by one element: one `bcmp` for the whole region.
    if src[width..] == src[..src.len() - width] {
        return (ENCODING_CONSTANT, Cow::Borrowed(&src[..width]));
    }
    let packed = if i == REG_WEIGHT {
        two_value_image(src).map(|image| (ENCODING_TWO_VALUE, image))
    } else if pack_ints && i >= REG_PAYLOAD_START {
        let col = &schema.columns[schema.payload_col_idx(i - REG_PAYLOAD_START)];
        col.fixed_int()
            .and_then(|fi| for_image(src, fi))
            .map(|image| (ENCODING_FOR, image))
    } else {
        None
    };
    match packed {
        Some((encoding, image)) => (encoding, Cow::Owned(image)),
        None => (ENCODING_RAW, Cow::Borrowed(src)),
    }
}

/// A weight region's TwoValue image (`value_a` ‖ `value_b` ‖ bitvec, bit set ⇔
/// `value_b`), or `None` at a third distinct weight. Allocated at the first
/// second value.
fn two_value_image(src: &[u8]) -> Option<Vec<u8>> {
    let n = src.len() / 8;
    let first = read_i64_le(src, 0);
    // Every row before the first second value equals `first`, so its bit is
    // already 0 when the image is allocated.
    let mut second: Option<(i64, Vec<u8>)> = None;
    for i in 1..n {
        let v = read_i64_le(src, i * 8);
        if v == first {
            continue;
        }
        match &mut second {
            None => {
                let mut image = vec![0u8; two_value_image_len(n)];
                image[..8].copy_from_slice(&first.to_le_bytes());
                image[8..TWO_VALUE_HEADER].copy_from_slice(&v.to_le_bytes());
                two_value_set_bit(&mut image[TWO_VALUE_HEADER..], i);
                second = Some((v, image));
            }
            Some((b, image)) => {
                if v != *b {
                    return None;
                }
                two_value_set_bit(&mut image[TWO_VALUE_HEADER..], i);
            }
        }
    }
    second.map(|(_, image)| image)
}

// Frame-of-reference + byte-width truncation (`ENCODING_FOR`): re-keyed ex-PK
// columns and other narrow-range integers dominate compacted view payloads, so
// framing on the region min and truncating offsets to whole bytes shrinks them
// severalfold while keeping decode an add per row.

/// The FoR image of a fixed-int region, or `None` when it would not shrink the
/// 64-byte-aligned footprint. Dispatches once on the column type, so both scans
/// run at a constant cell width and signedness.
fn for_image(src: &[u8], fi: FixedInt) -> Option<Vec<u8>> {
    match fi {
        // A 1-byte cell has no narrower offset width.
        FixedInt::U8 | FixedInt::I8 => None,
        FixedInt::U16 => for_image_of::<2, false>(src),
        FixedInt::I16 => for_image_of::<2, true>(src),
        FixedInt::U32 => for_image_of::<4, false>(src),
        FixedInt::I32 => for_image_of::<4, true>(src),
        FixedInt::U64 => for_image_of::<8, false>(src),
        FixedInt::I64 => for_image_of::<8, true>(src),
    }
}

/// Frame on the typed minimum; each row's offset is written as a full 8-byte
/// store at its `bw`-strided position — the next row overwrites the excess and
/// the slack absorbs the last row's — then truncated.
fn for_image_of<const W: usize, const SIGNED: bool>(src: &[u8]) -> Option<Vec<u8>> {
    let cells = src.as_chunks::<W>().0;
    let widen = |cell: &[u8; W]| {
        if SIGNED {
            read_signed_exact(cell) as u64
        } else {
            read_unsigned_exact(cell)
        }
    };
    // XOR with the sign bit maps i64 order onto u64 order, so one unsigned
    // min/max serves both signednesses.
    let bias = if SIGNED { 1u64 << 63 } else { 0 };
    let (mut min, mut max) = (u64::MAX, 0u64);
    for cell in cells {
        let b = widen(cell) ^ bias;
        min = min.min(b);
        max = max.max(b);
    }
    let reference = min ^ bias;
    let max_offset = (max ^ bias).wrapping_sub(reference);
    let bw = ((u64::BITS - max_offset.leading_zeros()) as usize).div_ceil(8);
    let n = cells.len();
    // A raw-byte win that vanishes after alignment saves no disk and still costs
    // a decode. Implies `bw < W`.
    if bw == 0 || for_image_len(n, bw).next_multiple_of(ALIGNMENT) >= src.len().next_multiple_of(ALIGNMENT) {
        return None;
    }
    let mut image = vec![0u8; for_image_len(n, bw) + (8 - bw)];
    image[..FOR_HEADER].copy_from_slice(&reference.to_le_bytes());
    for (row, cell) in cells.iter().enumerate() {
        let at = FOR_HEADER + row * bw;
        image[at..at + 8].copy_from_slice(&widen(cell).wrapping_sub(reference).to_le_bytes());
    }
    image.truncate(for_image_len(n, bw));
    Some(image)
}

/// The `(size, encoding)` projection of a region's [`DirEntry`], for the
/// shard-format assertions here and in the compaction / shard-reader test
/// modules. The entry shape itself lives in `layout`.
#[cfg(test)]
pub(crate) fn region_dir(image: &[u8], i: usize) -> (usize, u8) {
    let e = DirEntry::read(image, i);
    (e.size, e.encoding)
}

fn build_shard_filter_from_pk_region(pk_bytes: &[u8], stride: usize) -> Option<BinaryFuse8> {
    // `chunks_exact(0)` panics, and a non-empty region does not exclude a zero
    // stride.
    if pk_bytes.is_empty() || stride == 0 {
        return None;
    }
    // One hashed key per distinct PK. The PK region is sorted, so rows that
    // share a PK but differ in payload (valid under (PK, payload) element
    // identity) are adjacent — skipping chunks byte-equal to their predecessor
    // is an allocation-free O(n) pre-shrink that bounds `build`'s sort at the
    // number of *distinct* PKs in the region rather than its row count.
    // `probe_key` owns the narrow/wide derivation the probe side must
    // match exactly.
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
pub(crate) struct ShardWriteOpts {
    /// FoR on fixed-int payload regions.
    pub pack_ints: bool,
    /// Stamp [`SHARD_FLAG_SKELETON`].
    pub skeleton: bool,
    /// Write no PK filter — named for what it turns off, so `Default` builds one.
    pub skip_pk_filter: bool,
}

impl ShardWriteOpts {
    /// The compaction write policy: FoR-packed integer payload regions. The
    /// differential-test oracles reuse it so they cannot drift from the
    /// production write; compaction itself overrides `skip_pk_filter` per call.
    pub(crate) const COMPACTION: Self = ShardWriteOpts {
        pack_ints: true,
        skeleton: false,
        skip_pk_filter: false,
    };
}

/// The single-payload-column test shard writer: build a `Batch` through the typed
/// row API and hand it to the production [`Batch::write_as_shard`], so region
/// *order* has a single owner. Rows are `(opk_bytes, weight, i64_payload)`.
/// Several I64 payload columns go through [`write_i64_shard`].
#[cfg(test)]
pub(in crate::storage) fn write_test_shard(
    path: &std::path::Path,
    schema: &SchemaDescriptor,
    rows: &[(Vec<u8>, i64, i64)],
    opts: ShardWriteOpts,
) -> std::ffi::CString {
    // One payload column is written, so a wider schema would leave later regions
    // short of `count` and produce a shard the reader cannot make sense of.
    debug_assert_eq!(
        schema.num_payload_cols(),
        1,
        "write_test_shard fills exactly one payload column"
    );
    let mut b = super::batch::Batch::with_capacity(schema, rows.len().max(1));
    for (pk, w, v) in rows {
        b.begin_row(pk, *w);
        b.extend_col(0, &v.to_le_bytes());
        b.commit_row(0);
    }
    let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
    b.write_as_shard(&cpath, opts).unwrap();
    cpath
}

/// [`write_test_shard`] at any number of I64 payload columns, with explicit null
/// words and a raw blob heap. Rows are `(opk_bytes, weight, null_word, payload)`.
#[cfg(test)]
pub(in crate::storage) fn write_i64_shard(
    path: &CStr,
    schema: &SchemaDescriptor,
    rows: &[(Vec<u8>, i64, u64, Vec<i64>)],
    blob: &[u8],
    opts: ShardWriteOpts,
) {
    let mut b = Batch::with_capacity(schema, rows.len().max(1));
    b.blob.extend_from_slice(blob);
    for (pk, w, null_word, cols) in rows {
        debug_assert_eq!(cols.len(), schema.num_payload_cols());
        b.begin_row(pk, *w);
        for (pi, v) in cols.iter().enumerate() {
            b.extend_col(pi, &v.to_le_bytes());
        }
        b.commit_row(*null_word);
    }
    b.write_as_shard(path, opts).unwrap();
}

impl Batch {
    /// Write this batch as a shard at `path`, staged as `<path>.tmp` and renamed
    /// into place. Unsynced: the caller registers the shard with the flush
    /// barrier, which fdatasyncs it and fsyncs its directory.
    pub(crate) fn write_as_shard(&self, path: &CStr, opts: ShardWriteOpts) -> Result<(), StorageError> {
        let schema = self.schema();
        let n = self.count;
        let mut regions: [&[u8]; MAX_WIRE_REGIONS] = [&[]; MAX_WIRE_REGIONS];
        let num_regions = self.fill_regions(&mut regions);
        let regions = &regions[..num_regions];
        // Shards are ghost-free by construction: flush persists the run set's
        // consolidated net-state run and compaction's merge drops net-zero groups.
        debug_assert!(
            !(0..n).any(|i| read_i64_le(regions[REG_WEIGHT], i * 8) == 0),
            "shard writer got weight-0 rows; every producer consolidates first",
        );
        debug_assert!(
            !opts.skeleton || schema.num_payload_cols() == 0,
            "a skeleton shard must be written under the PK-only projection of its relation's schema",
        );
        // A store nothing point-probes writes no filter.
        let filter = (!opts.skip_pk_filter)
            .then(|| build_shard_filter_from_pk_region(regions[REG_PK], schema.pk_stride()))
            .flatten()
            .map(|f| shard_filter::serialize(&f));

        let staged = StagedFile::create(path)?;
        let file = staged.file();
        let mut header = vec![0u8; desc_len(num_regions)];
        let mut end = header.len();
        for (i, &src) in regions.iter().enumerate() {
            let (encoding, image) = encode_region(schema, i, src, n, num_regions - 1, opts.pack_ints);
            let offset = end.next_multiple_of(ALIGNMENT);
            file.write_all_at(&image, offset as u64)?;
            DirEntry {
                offset,
                size: image.len(),
                checksum: gnitz_wire::checksum(&image),
                encoding,
            }
            .write(&mut header, i);
            end = offset + image.len();
        }
        let (filter_offset, filter_size, filter_checksum) = match &filter {
            Some(f) => {
                let offset = end.next_multiple_of(ALIGNMENT);
                file.write_all_at(f, offset as u64)?;
                end = offset + f.len();
                (offset, f.len(), gnitz_wire::checksum(f))
            }
            None => (0, 0, 0),
        };
        // An empty trailing region sits past the last byte written.
        file.set_len(end as u64)?;

        write_u64_le(&mut header, OFF_MAGIC, SHARD_MAGIC);
        write_u64_le(&mut header, OFF_VERSION, SHARD_VERSION);
        write_u64_le(&mut header, OFF_ROW_COUNT, n as u64);
        write_u64_le(
            &mut header,
            OFF_FILE_NPC,
            schema.num_payload_cols() as u64 | if opts.skeleton { SHARD_FLAG_SKELETON } else { 0 },
        );
        write_u64_le(&mut header, OFF_SHARD_FILTER_OFFSET, filter_offset as u64);
        write_u64_le(&mut header, OFF_SHARD_FILTER_SIZE, filter_size as u64);
        // Inside the digest below, so a forged filter cannot be re-stamped to match.
        write_u64_le(&mut header, OFF_SHARD_FILTER_CHECKSUM, filter_checksum);
        // Last, over every other field, seeded with the final name's basename.
        let desc = desc_digest(shard_basename(path.to_bytes()), &header, num_regions);
        write_u64_le(&mut header, OFF_DESC_CHECKSUM, desc);
        file.write_all_at(&header, 0)?;
        staged.commit()
    }
}

#[cfg(test)]
#[path = "tests/shard_file.rs"]
mod tests;
