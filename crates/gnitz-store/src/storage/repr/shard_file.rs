//! Shard file image building and atomic writing.
//!
//! Shared by the compaction path (`lsm::compact`) and the RAM-tier spill /
//! checkpoint path (`lsm::table::flush`).

use std::ffi::CStr;
use std::fs::File;
use std::os::unix::fs::FileExt;

use libc::c_int;

use super::super::error::StorageError;
use super::batch::{strides_from_schema, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::layout::*;
use super::shard_filter;
use crate::foundation::posix_io;
use crate::foundation::xxh;
use crate::schema::key::probe_key;
use crate::schema::SchemaDescriptor;
use gnitz_wire::{
    is_fixed_int, is_signed_int, read_i64_le, read_signed_exact, read_u64_le, read_unsigned_exact, write_u64_le,
};
use xorf::BinaryFuse8;

fn align64(val: usize) -> usize {
    (val + ALIGNMENT - 1) & !(ALIGNMENT - 1)
}

// ---------------------------------------------------------------------------
// Per-region encoding detection (used by the streaming shard writer)
// ---------------------------------------------------------------------------

enum RegionEncoding {
    Raw,
    /// All elements identical; the on-disk image is the first `width` bytes of
    /// the raw region.
    Constant {
        width: usize,
    },
    /// Exactly two distinct weight values. `buf` is the ready on-disk image
    /// (`value_a` LE ‖ `value_b` LE ‖ bitvec), built by
    /// `detect_weight_encoding` at detection time.
    TwoValue {
        buf: Vec<u8>,
    },
    /// Frame-of-reference + byte-width truncation of an integer payload region.
    /// `buf` is the ready on-disk image (8-byte reference, then each row's
    /// `widen(value) − reference` in its low `bw` bytes LE), built by
    /// `encode_for_region` at detection time.
    For {
        buf: Vec<u8>,
    },
}

impl RegionEncoding {
    /// The on-disk bytes for this region given its raw source bytes: `Raw`
    /// passes the source through, `Constant` its first element, and the
    /// prebuilt variants their image. One resolution serves both the checksum
    /// and the pwrite.
    fn encoded_bytes<'a>(&'a self, src: &'a [u8]) -> &'a [u8] {
        match self {
            RegionEncoding::Raw => src,
            RegionEncoding::Constant { width } => &src[..*width],
            RegionEncoding::TwoValue { buf } | RegionEncoding::For { buf } => buf,
        }
    }
}

/// Detect whether a fixed-width region is all-constant.
/// Returns `Constant` if every element is identical, else `Raw`.
fn detect_encoding(data: &[u8], element_width: usize) -> RegionEncoding {
    debug_assert!(!data.is_empty() && data.len().is_multiple_of(element_width));
    // Not attempted past 16 bytes: the only regions that wide are compound-PK
    // strides, where an all-equal region needs every PK column to repeat — rare
    // enough that the scan is not worth running on every shard write.
    if element_width > 16 {
        return RegionEncoding::Raw;
    }
    let first = &data[..element_width];
    let n = data.len() / element_width;
    for i in 1..n {
        let elem = &data[i * element_width..(i + 1) * element_width];
        if elem != first {
            return RegionEncoding::Raw;
        }
    }
    RegionEncoding::Constant { width: element_width }
}

/// Detect weight encoding: constant, two-value (bitvec), or raw.
fn detect_weight_encoding(data: &[u8]) -> RegionEncoding {
    debug_assert!(!data.is_empty() && data.len().is_multiple_of(8));
    let n = data.len() / 8;
    let first = read_i64_le(data, 0);
    // Single decode pass building the on-disk image directly: `value_a` LE ‖
    // `value_b` LE ‖ bitvec (bit i set ⇔ row i == value_b). The buffer is
    // allocated lazily at the first sighting of a second distinct value —
    // every earlier row equals `first`, so its bit is already 0 — keeping the
    // dominant all-equal (Constant) region allocation-free; a 3+-distinct
    // region still aborts to Raw at the third value (paying at most one wasted
    // allocation).
    let mut second: Option<(i64, Vec<u8>)> = None;
    for i in 1..n {
        let v = read_i64_le(data, i * 8);
        if v == first {
            continue;
        }
        match &mut second {
            None => {
                let mut buf = vec![0u8; two_value_image_len(n)];
                buf[..8].copy_from_slice(&first.to_le_bytes());
                buf[8..TWO_VALUE_HEADER].copy_from_slice(&v.to_le_bytes());
                two_value_set_bit(&mut buf[TWO_VALUE_HEADER..], i);
                second = Some((v, buf));
            }
            Some((b, buf)) => {
                if v != *b {
                    return RegionEncoding::Raw; // 3+ distinct values
                }
                two_value_set_bit(&mut buf[TWO_VALUE_HEADER..], i);
            }
        }
    }

    match second {
        None => RegionEncoding::Constant { width: 8 },
        Some((_, buf)) => RegionEncoding::TwoValue { buf },
    }
}

// ---------------------------------------------------------------------------
// Frame-of-reference (FoR) + byte-width truncation codec for integer payload
// regions (`ENCODING_FOR`, layout.rs): re-keyed ex-PK columns and other
// narrow-range integers dominate compacted view payloads, so framing on the
// region min and truncating offsets to whole bytes shrinks them severalfold
// while keeping decode a trivial add-per-row.
// ---------------------------------------------------------------------------

/// Widen one fixed-int cell to `u64`, matching the column's signedness:
/// sign-extend to `i64` for signed type codes, zero-extend for unsigned — the
/// same convention as `read_signed_exact` / `read_unsigned_exact`. Both spaces
/// bit-cast to `u64`, so a FoR `wrapping_sub` / `wrapping_add` pair round-trips
/// exactly. `cell` is one whole column cell (both callers walk
/// `chunks_exact(stride)`), so it carries its own width.
#[inline]
fn widen_cell(cell: &[u8], signed: bool) -> u64 {
    if signed {
        read_signed_exact(cell) as u64
    } else {
        read_unsigned_exact(cell)
    }
}

/// On-disk byte size of a FoR region: an 8-byte reference plus `n · bw` offset
/// bytes, tightly packed with no interior padding.
fn for_encoded_size(n: usize, bw: usize) -> usize {
    8 + n * bw
}

/// Scan a fixed-int region once for its typed `[min, max]` and decide whether
/// FoR beats Raw after 64-byte region alignment. Returns `(reference, bw)` on a
/// win — `reference` is `widen(min)` (the 8-byte frame) and `bw` the truncated
/// offset width in `1..stride` — or `None` to keep the region Raw. Allocates no
/// value buffer: the size test is closed-form from the one min/max scan.
fn for_params(data: &[u8], stride: usize, signed: bool, n: usize) -> Option<(u64, usize)> {
    debug_assert!(n > 0 && data.len() == n * stride);
    // Typed min/max via one biased u64 comparison: XORing the sign bit maps
    // sign-extended i64 order onto plain u64 order (the OPK sign-flip idiom),
    // so a single branch-free loop covers signed and unsigned columns.
    let bias = if signed { 1u64 << 63 } else { 0 };
    let mut mn = u64::MAX;
    let mut mx = 0u64;
    for cell in data.chunks_exact(stride) {
        let b = widen_cell(cell, signed) ^ bias;
        mn = mn.min(b);
        mx = mx.max(b);
    }
    // Frame on the typed min; the span is a plain wrapping subtraction of the
    // widened extremes (correct across zero for the correct extension).
    let reference = mn ^ bias;
    let max_offset = (mx ^ bias).wrapping_sub(reference);
    // All-equal (max_offset == 0) is claimed by Constant before FoR is tried;
    // an extreme span (MIN..MAX) yields bw >= stride and falls back to Raw.
    if max_offset == 0 {
        return None;
    }
    let bw = ((64 - max_offset.leading_zeros()) as usize).div_ceil(8);
    // Compare *aligned* footprints: a raw-byte win that vanishes after the
    // writer's 64-byte region padding buys zero disk / page-fault savings while
    // still costing decode CPU and a pinned decoded buffer. This inequality
    // implies `bw < stride` for `n > 0`; the explicit term is kept for clarity.
    if bw >= stride || align64(for_encoded_size(n, bw)) >= align64(n * stride) {
        return None;
    }
    Some((reference, bw))
}

/// Build the on-disk FoR image: 8-byte `reference` LE, then each row's
/// `widen(value) − reference` truncated to its low `bw` bytes LE. Each row is
/// emitted as one full 8-byte store at its `bw`-strided position — the next
/// row's store overwrites the excess bytes and the zeroed slack absorbs the
/// last row's — then the buffer is truncated to the exact encoded size.
fn build_for_buffer(data: &[u8], stride: usize, signed: bool, n: usize, reference: u64, bw: usize) -> Vec<u8> {
    let mut buf = vec![0u8; for_encoded_size(n, bw) + (8 - bw)];
    buf[..8].copy_from_slice(&reference.to_le_bytes());
    let mut pos = 8;
    for cell in data.chunks_exact(stride) {
        let offset = widen_cell(cell, signed).wrapping_sub(reference);
        buf[pos..pos + 8].copy_from_slice(&offset.to_le_bytes());
        pos += bw;
    }
    buf.truncate(for_encoded_size(n, bw));
    buf
}

/// Try to FoR-encode a fixed-int region: the on-disk image on a win, `None` to
/// keep it Raw. `for_params` makes the pack-or-not decision without allocating,
/// so the image is built only for winners. The chosen `bw` is recoverable from
/// the image as `(len - 8) / n` — the same derivation `decode_for_region` uses —
/// so it is not returned.
fn encode_for_region(data: &[u8], stride: usize, signed: bool, n: usize) -> Option<Vec<u8>> {
    let (reference, bw) = for_params(data, stride, signed, n)?;
    Some(build_for_buffer(data, stride, signed, n, reference, bw))
}

/// A FoR region decoded back to its raw little-endian image. Backed by
/// `Box<[u64]>` so the bytes are 8-aligned (payload accessors hand out
/// naturally-aligned pointers) and the address is stable for the owner's
/// lifetime.
pub(crate) struct DecodedRegion {
    words: Box<[u64]>,
    byte_len: usize,
}

impl DecodedRegion {
    fn zeroed(byte_len: usize) -> Self {
        Self {
            words: vec![0u64; byte_len.div_ceil(8)].into_boxed_slice(),
            byte_len,
        }
    }

    /// The decoded image as bytes — `byte_len` long, 8-aligned, stable address.
    pub(crate) fn as_bytes(&self) -> &[u8] {
        // SAFETY: `words` is 8-aligned and holds >= `byte_len` bytes.
        unsafe { std::slice::from_raw_parts(self.words.as_ptr() as *const u8, self.byte_len) }
    }

    fn as_bytes_mut(&mut self) -> &mut [u8] {
        // SAFETY: as in `as_bytes`.
        unsafe { std::slice::from_raw_parts_mut(self.words.as_mut_ptr() as *mut u8, self.byte_len) }
    }
}

/// Decode a FoR region image (`8 + n·bw` bytes) back to its `n · elem_width`
/// little-endian raw form: read the 8-byte `reference`, derive `bw` from the
/// image length, and for each row widen its `bw` bytes, `wrapping_add` the
/// reference, and store the low `elem_width` bytes. Pure byte arithmetic over
/// in-bounds slices — infallible once the reader's open-time checks hold.
pub(crate) fn decode_for_region(encoded: &[u8], n: usize, elem_width: usize) -> DecodedRegion {
    let reference = read_u64_le(encoded, 0);
    let bw = (encoded.len() - 8) / n;
    debug_assert!((1..8).contains(&bw), "open-time checks bound bw to 1..stride<=8");
    let mask = (1u64 << (8 * bw)) - 1;
    // One masked 8-byte load per row; the few tail rows whose full-word load
    // would overrun the image fall back to a byte gather.
    let offset_at = |i: usize| -> u64 {
        let base = 8 + i * bw;
        if base + 8 <= encoded.len() {
            read_u64_le(encoded, base) & mask
        } else {
            encoded[base..base + bw]
                .iter()
                .rev()
                .fold(0u64, |acc, &b| (acc << 8) | b as u64)
        }
    };
    let mut out = DecodedRegion::zeroed(n * elem_width);
    if elem_width == 8 {
        // The dominant I64/U64 shape: one whole-word store per row.
        for (i, w) in out.words.iter_mut().enumerate() {
            *w = offset_at(i).wrapping_add(reference).to_le();
        }
    } else {
        let bytes = out.as_bytes_mut();
        for i in 0..n {
            let v = offset_at(i).wrapping_add(reference);
            bytes[i * elem_width..(i + 1) * elem_width].copy_from_slice(&v.to_le_bytes()[..elem_width]);
        }
    }
    out
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

/// Per-call policy for the shard writers ([`write_shard_streaming`] /
/// `Batch::write_as_shard`).
///
/// `pack_ints` enables FoR (`ENCODING_FOR`) on eligible integer payload
/// regions — set only by compaction; L0 spill/checkpoint writers stay raw.
///
/// `skeleton` stamps [`SHARD_FLAG_SKELETON`] for a capacity-bounded view's
/// payload-free shard, whose `schema` must be the PK-only projection of the
/// relation's schema; set only by compaction's per-guard dehydration. It is a
/// separate field rather than an alternative to `pack_ints` because the two are
/// independent policies over one write. They merely never co-occur today, since
/// a skeleton's schema has no payload region to pack.
///
/// `skip_pk_filter` drops the shard PK filter, for a store nothing point-probes.
/// Named for what it turns *off* so the derived default keeps building one: a
/// missing filter changes no answer (`shard_filter_may_contain` admits
/// everything on a filterless shard), so the wrong default would be invisible to
/// every test.
///
/// Durability is not a per-write choice: the flush barrier fdatasyncs every
/// registered-unsynced shard in one batched io_uring submission, then fsyncs the
/// table directory the renames landed in.
#[derive(Clone, Copy, Default)]
pub(crate) struct ShardWriteOpts {
    pub pack_ints: bool,
    pub skeleton: bool,
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
    /// A dehydrated guard's write: payload-free, so nothing to pack.
    pub(crate) const SKELETON: Self = ShardWriteOpts {
        pack_ints: false,
        skeleton: true,
        skip_pk_filter: false,
    };
}

/// The single-payload-column test shard writer: build a `Batch` through the typed
/// row API and hand it to the production [`Batch::write_as_shard`], so region
/// *order* has a single owner. Rows are `(opk_bytes, weight, i64_payload)`.
/// A test needing several payload columns builds its own `Batch` instead.
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
    b.write_as_shard(&cpath, schema, opts).unwrap();
    cpath
}

/// Write the .tmp shard, close, and rename to `basename`. The sole shard writer.
/// Unsynced — the caller registers the result for the flush barrier's batched
/// fdatasync sweep (see [`ShardWriteOpts`]).
pub(crate) fn write_shard_streaming(
    dirfd: c_int,
    basename: &CStr,
    row_count: u32,
    regions: &[&[u8]],
    schema: &SchemaDescriptor,
    opts: ShardWriteOpts,
) -> Result<(), StorageError> {
    let tmp_name = write_shard_streaming_inner(dirfd, basename, row_count, regions, schema, opts)?;
    if let Err(e) = posix_io::renameat(dirfd, &tmp_name, dirfd, basename) {
        unsafe { libc::unlinkat(dirfd, tmp_name.as_ptr(), 0) };
        return Err(e.into());
    }
    // Both the file's contents and the renamed directory entry are made durable
    // by the flush barrier: it fdatasyncs every registered-unsynced file and then
    // fsyncs the table directory through a real `O_DIRECTORY` fd. This writer
    // could not fsync the directory anyway — `Batch::write_as_shard` addresses
    // the file by path from `AT_FDCWD`, which is not a directory fd.
    Ok(())
}

/// The caller's regions must match the layout `schema` implies: one fixed
/// region per role, each exactly one element per row, plus the variable-length
/// blob region. The reader re-derives that layout from the schema alone, and the
/// descriptive digest is computed from these same bytes — so a region that
/// disagrees would validate at every open and be mis-read forever. This is the
/// one writer↔reader disagreement no digest can catch, which is why it is
/// checked here rather than asserted.
fn check_region_shape(regions: &[&[u8]], n: usize, strides: &[u8], nr: usize) -> Result<(), StorageError> {
    if regions.len() != nr + 1 {
        return Err(StorageError::InvalidShard);
    }
    for (i, src) in regions[..nr].iter().enumerate() {
        if src.len() != n * strides[i] as usize {
            return Err(StorageError::InvalidShard);
        }
    }
    Ok(())
}

/// Open the .tmp shard, write header+regions+filter, and close it — unsynced,
/// since the flush barrier fdatasyncs every registered file later. Returns the
/// .tmp's name for the caller to rename. On error the .tmp is unlinked.
#[allow(clippy::needless_range_loop)]
fn write_shard_streaming_inner(
    dirfd: c_int,
    basename: &CStr,
    row_count: u32,
    regions: &[&[u8]],
    schema: &SchemaDescriptor,
    opts: ShardWriteOpts,
) -> Result<std::ffi::CString, StorageError> {
    let num_regions = regions.len();
    let n = row_count as usize;
    // Writer↔reader region-layout contract, shared with `MappedShard::open`:
    // `strides` holds each fixed-width region's per-element width, `nr` is the
    // trailing blob region's index.
    let (strides, nr) = strides_from_schema(schema);
    let nr = nr as usize;
    check_region_shape(regions, n, &strides, nr)?;
    // Shards are ghost-free by construction: flush persists the run set's
    // consolidated net-state run and compaction's merge drops net-zero groups.
    debug_assert!(
        !(0..n).any(|i| read_i64_le(regions[REG_WEIGHT], i * 8) == 0),
        "shard writer got weight-0 rows; every producer consolidates first",
    );

    // --- Phase 1: detect encodings and compute actual sizes ---
    let mut encodings: Vec<RegionEncoding> = Vec::with_capacity(num_regions);
    let mut actual_sizes: Vec<usize> = Vec::with_capacity(num_regions);

    for i in 0..num_regions {
        let src = regions[i];
        let orig_sz = src.len();
        // The blob region is variable-length (always Raw); a rowless shard has
        // nothing to encode. `check_region_shape` already tied every other
        // region's length to `n`.
        if n == 0 || i >= nr {
            encodings.push(RegionEncoding::Raw);
            actual_sizes.push(orig_sz);
            continue;
        }
        let width = strides[i] as usize;
        if orig_sz != n * width {
            return Err(StorageError::InvalidShard);
        }
        // FoR eligibility: only fixed-int payload regions, only when the caller
        // opted in (compaction outputs). `Some(signed)` when eligible.
        let for_signed = (opts.pack_ints && i >= REG_PAYLOAD_START)
            .then(|| schema.columns[schema.payload_col_idx(i - REG_PAYLOAD_START)].type_code)
            .filter(|&tc| is_fixed_int(tc))
            .map(is_signed_int);
        // `detect_encoding` returns Raw for any width > 16, so wide-PK /
        // wide-payload regions naturally stay Raw. Selection order per payload
        // region: Constant, FoR (eligible and a size win), else Raw.
        let enc = if i == REG_WEIGHT {
            detect_weight_encoding(src)
        } else {
            match (detect_encoding(src, width), for_signed) {
                (c @ RegionEncoding::Constant { .. }, _) => c,
                (_, Some(signed)) => match encode_for_region(src, width, signed, n) {
                    Some(buf) => RegionEncoding::For { buf },
                    None => RegionEncoding::Raw,
                },
                _ => RegionEncoding::Raw,
            }
        };
        actual_sizes.push(enc.encoded_bytes(src).len());
        encodings.push(enc);
    }

    // --- Phase 2: PK filter built from pk region (pk_stride bytes/row, zero-extended to u128) ---
    // A store nothing point-probes writes no filter: the build, the file bytes
    // and the open-time checksum are all work for a reader that does not exist.
    let pk_filter = if row_count > 0 && !opts.skip_pk_filter {
        build_shard_filter_from_pk_region(regions[REG_PK], schema.pk_stride())
    } else {
        None
    };

    // --- Phase 3: compute offsets ---
    let hdr_dir_size = desc_len(num_regions);
    let mut pos = align64(hdr_dir_size);
    let mut region_offsets = Vec::with_capacity(num_regions);
    for &actual_sz in actual_sizes.iter().take(num_regions) {
        region_offsets.push(pos);
        pos = align64(pos + actual_sz);
    }
    // num_regions >= 4 always (pk, weight, null, blob at minimum).
    let data_end = region_offsets[num_regions - 1] + actual_sizes[num_regions - 1];

    let filter_data = pk_filter.as_ref().map(shard_filter::serialize);
    let filter_offset = if filter_data.is_some() { align64(data_end) } else { 0 };
    let filter_size = filter_data.as_ref().map_or(0, |d| d.len());
    let total_size = if filter_data.is_some() {
        filter_offset + filter_size
    } else {
        data_end
    };

    // --- Phase 4: build header + directory buffer ---
    let mut hdr_buf = vec![0u8; hdr_dir_size];

    for i in 0..num_regions {
        let src = regions[i];
        // Empty regions short-circuit to checksum 0 — never encode an empty
        // region. Non-Raw encodings are only chosen for non-empty regions.
        let cs = if actual_sizes[i] > 0 && !src.is_empty() {
            xxh::checksum(encodings[i].encoded_bytes(src))
        } else {
            0
        };

        DirEntry {
            offset: region_offsets[i],
            size: actual_sizes[i],
            checksum: cs,
            encoding: match &encodings[i] {
                RegionEncoding::Raw => ENCODING_RAW,
                RegionEncoding::Constant { .. } => ENCODING_CONSTANT,
                RegionEncoding::TwoValue { .. } => ENCODING_TWO_VALUE,
                RegionEncoding::For { .. } => ENCODING_FOR,
            },
        }
        .write(&mut hdr_buf, i);
    }

    write_u64_le(&mut hdr_buf, OFF_MAGIC, SHARD_MAGIC);
    write_u64_le(&mut hdr_buf, OFF_VERSION, SHARD_VERSION);
    write_u64_le(&mut hdr_buf, OFF_ROW_COUNT, row_count as u64);
    // The file's own arity. `check_region_shape` above already tied
    // `regions.len()` to `strides_from_schema(schema)`, so the count stamped
    // here and the directory written below come from the same descriptor.
    let skeleton = opts.skeleton;
    debug_assert!(
        !skeleton || schema.num_payload_cols() == 0,
        "a skeleton shard must be written under the PK-only projection of its relation's schema",
    );
    write_u64_le(
        &mut hdr_buf,
        OFF_FILE_NPC,
        schema.num_payload_cols() as u64 | if skeleton { SHARD_FLAG_SKELETON } else { 0 },
    );
    write_u64_le(&mut hdr_buf, OFF_SHARD_FILTER_OFFSET, filter_offset as u64);
    write_u64_le(&mut hdr_buf, OFF_SHARD_FILTER_SIZE, filter_size as u64);
    // A filterless shard carries 0 here and `filter_offset == 0`, which is the
    // reader's own filterless arm. The field sits in the header, so the digest
    // below closes over it: a forged filter cannot be re-stamped to match.
    write_u64_le(
        &mut hdr_buf,
        OFF_SHARD_FILTER_CHECKSUM,
        filter_data.as_ref().map_or(0, |d| xxh::checksum(d)),
    );

    // Last, over every other field. `basename` is the *final* name — the `.tmp`
    // suffix below is applied afterwards and the rename restores it — so writer
    // and reader seed the digest identically.
    let desc = desc_digest(shard_basename(basename.to_bytes()), &hdr_buf, num_regions);
    write_u64_le(&mut hdr_buf, OFF_DESC_CHECKSUM, desc);

    let tmp_name = super::super::cstr_with_tmp_suffix(basename)?;

    // The `File` is the sole closer, so every error return below closes it —
    // `abort` only unlinks.
    let file = File::from(posix_io::openat_owned(
        dirfd,
        &tmp_name,
        libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
    )?);

    let abort = |e: std::io::Error| -> StorageError {
        unsafe { libc::unlinkat(dirfd, tmp_name.as_ptr(), 0) };
        e.into()
    };

    file.set_len(total_size as u64).map_err(abort)?;
    file.write_all_at(&hdr_buf, 0).map_err(abort)?;

    for i in 0..num_regions {
        let src = regions[i];
        // Same empty short-circuit as the checksum pass.
        if actual_sizes[i] > 0 && !src.is_empty() {
            file.write_all_at(encodings[i].encoded_bytes(src), region_offsets[i] as u64)
                .map_err(abort)?;
        }
    }

    if let Some(ref data) = filter_data {
        file.write_all_at(data, filter_offset as u64).map_err(abort)?;
    }

    drop(file); // Closed before the caller renames, matching the manifest path.
    Ok(tmp_name)
}

#[cfg(test)]
#[path = "tests/shard_file.rs"]
mod tests;
