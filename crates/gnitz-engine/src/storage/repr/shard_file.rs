//! Shard file image building and atomic writing.
//!
//! Shared by the compaction path (`compact.rs`) and the memtable flush path
//! (`memtable.rs`).

use std::ffi::CStr;
use std::os::fd::{AsRawFd, OwnedFd};

use libc::c_int;

use super::super::error::StorageError;
use super::batch::{strides_from_schema, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::layout::*;
use super::xor8;
use crate::foundation::codec::write_u64_le;
use crate::foundation::posix_io::{fdatasync_eintr, fsync_eintr};
use crate::foundation::xxh;
use crate::schema::key::pack_pk_be;
use crate::schema::{read_signed, read_unsigned, SchemaDescriptor, MAX_PK_BYTES};
use gnitz_wire::{is_fixed_int, is_signed_int};
use xorf::Xor8;

fn align64(val: usize) -> usize {
    (val + ALIGNMENT - 1) & !(ALIGNMENT - 1)
}

// ---------------------------------------------------------------------------
// PkUniqueChecker — shared detection primitive (used by flush and compaction)
// ---------------------------------------------------------------------------

/// Determines at write time whether an output shard qualifies as PkUnique.
///
/// Call `observe` once per output row in write order. A shard qualifies if it
/// contains no negative-weight rows and no duplicate adjacent PKs. The u128
/// prefix fast-path avoids the full byte compare for the overwhelmingly common
/// case of distinct adjacent PKs.
///
/// Placed in `shard_file.rs` so both the flush path (table.rs) and the
/// compaction path (compact.rs) can use it without a separate module.
pub struct PkUniqueChecker {
    last_prefix: u128,
    last_pk: [u8; MAX_PK_BYTES],
    has_last: bool,
    qualifies: bool,
}

impl PkUniqueChecker {
    pub fn new() -> Self {
        Self {
            last_prefix: 0,
            last_pk: [0; MAX_PK_BYTES],
            has_last: false,
            qualifies: true,
        }
    }

    /// Observe one output row. `prefix` must be `pack_pk_be(pk_bytes)` — the
    /// compaction emit loop already has it for guard routing, so it is taken
    /// rather than recomputed. `pk_bytes` is the OPK-encoded key slice (sorted;
    /// needed to confirm a prefix tie for wide keys). `weight` is the net row
    /// weight after consolidation.
    pub fn observe(&mut self, prefix: u128, pk_bytes: &[u8], weight: i64) {
        if !self.qualifies {
            return;
        }
        if weight < 0 {
            self.qualifies = false;
            return;
        }
        debug_assert_eq!(prefix, pack_pk_be(pk_bytes));
        let n = pk_bytes.len().min(MAX_PK_BYTES);
        // Is this row an adjacent duplicate of its predecessor? For narrow keys
        // (n ≤ 16) pack_pk_be is injective, so an equal prefix *is* an equal PK
        // and last_pk is never maintained; for wide keys the prefix is lossy, so
        // a prefix tie is only confirmed by the full byte compare. The has_last
        // guard keeps the first row — or a genuine all-zero key, which aliases
        // the zero-initialised last_prefix/last_pk — from reading as a duplicate.
        let is_duplicate =
            self.has_last && prefix == self.last_prefix && (n <= 16 || self.last_pk[..n] == pk_bytes[..n]);
        if is_duplicate {
            self.qualifies = false;
            return;
        }
        // Remember this row as the predecessor for the next observe. Narrow keys
        // skip the last_pk copy — for them the prefix alone is authoritative.
        self.last_prefix = prefix;
        if n > 16 {
            self.last_pk[..n].copy_from_slice(&pk_bytes[..n]);
        }
        self.has_last = true;
    }

    /// Returns `SHARD_FLAG_PK_UNIQUE` when the observed stream was unique,
    /// otherwise 0. Callers gate on `can_tag_pk_unique` before constructing a
    /// checker — a never-observed checker vacuously qualifies.
    pub fn flags(&self) -> u8 {
        use super::layout::SHARD_FLAG_PK_UNIQUE;
        if self.qualifies {
            SHARD_FLAG_PK_UNIQUE
        } else {
            0
        }
    }
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
    // The Constant payload buffer is 16 bytes; wider regions (compound
    // PK strides > 16) cannot use this encoding and stay Raw.
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
    let first = i64::from_le_bytes(data[..8].try_into().unwrap());
    // Single decode pass building the on-disk image directly: `value_a` LE ‖
    // `value_b` LE ‖ bitvec (bit i set ⇔ row i == value_b). The buffer is
    // allocated lazily at the first sighting of a second distinct value —
    // every earlier row equals `first`, so its bit is already 0 — keeping the
    // dominant all-equal (Constant) region allocation-free; a 3+-distinct
    // region still aborts to Raw at the third value (paying at most one wasted
    // allocation).
    let mut second: Option<(i64, Vec<u8>)> = None;
    for i in 1..n {
        let v = i64::from_le_bytes(data[i * 8..(i + 1) * 8].try_into().unwrap());
        if v == first {
            continue;
        }
        match &mut second {
            None => {
                let mut buf = vec![0u8; 16 + n.div_ceil(8)];
                buf[..8].copy_from_slice(&first.to_le_bytes());
                buf[8..16].copy_from_slice(&v.to_le_bytes());
                buf[16 + i / 8] |= 1 << (i % 8);
                second = Some((v, buf));
            }
            Some((b, buf)) => {
                if v != *b {
                    return RegionEncoding::Raw; // 3+ distinct values
                }
                buf[16 + i / 8] |= 1 << (i % 8);
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
/// same convention as `read_signed` / `read_unsigned`. Both spaces bit-cast to
/// `u64`, so a FoR `wrapping_sub` / `wrapping_add` pair round-trips exactly.
#[inline]
fn widen_cell(bytes: &[u8], stride: usize, signed: bool) -> u64 {
    if signed {
        read_signed(bytes, stride) as u64
    } else {
        read_unsigned(bytes, stride)
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
        let b = widen_cell(cell, stride, signed) ^ bias;
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
        let offset = widen_cell(cell, stride, signed).wrapping_sub(reference);
        buf[pos..pos + 8].copy_from_slice(&offset.to_le_bytes());
        pos += bw;
    }
    buf.truncate(for_encoded_size(n, bw));
    buf
}

/// Try to FoR-encode a fixed-int region: `(bw, image)` on a win, `None` to keep
/// it Raw. `for_params` makes the pack-or-not decision without allocating, so
/// the image is built only for winners.
fn encode_for_region(data: &[u8], stride: usize, signed: bool, n: usize) -> Option<(usize, Vec<u8>)> {
    let (reference, bw) = for_params(data, stride, signed, n)?;
    Some((bw, build_for_buffer(data, stride, signed, n, reference, bw)))
}

/// A FoR region decoded back to its raw little-endian image. Backed by
/// `Box<[u64]>` so the bytes are 8-aligned (payload accessors hand out
/// naturally-aligned pointers) and the address is stable for the owner's
/// lifetime.
#[derive(Clone)]
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
    let reference = u64::from_le_bytes(encoded[..8].try_into().unwrap());
    let bw = (encoded.len() - 8) / n;
    debug_assert!((1..8).contains(&bw), "open-time checks bound bw to 1..stride<=8");
    let mask = (1u64 << (8 * bw)) - 1;
    // One masked 8-byte load per row; the few tail rows whose full-word load
    // would overrun the image fall back to a byte gather.
    let offset_at = |i: usize| -> u64 {
        let base = 8 + i * bw;
        if base + 8 <= encoded.len() {
            u64::from_le_bytes(encoded[base..base + 8].try_into().unwrap()) & mask
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

/// Directory entry `(size, encoding_byte)` for region `i` of a shard image.
/// Shared by the shard-format tests here and in the compaction / shard-reader
/// test modules.
#[cfg(test)]
pub(crate) fn region_dir(image: &[u8], i: usize) -> (usize, u8) {
    use crate::foundation::codec::read_u64_le;
    let dir_off = read_u64_le(image, OFF_DIR_OFFSET) as usize;
    let d = dir_off + i * DIR_ENTRY_SIZE;
    (read_u64_le(image, d + 8) as usize, image[d + 24])
}

fn build_xor8_from_pk_region(pk_ptr: *const u8, pk_sz: usize, stride: usize) -> Option<Xor8> {
    if pk_ptr.is_null() || pk_sz == 0 || stride == 0 {
        return None;
    }
    let pk_bytes = unsafe { std::slice::from_raw_parts(pk_ptr, pk_sz) };
    // One hashed key per distinct PK. The PK region is sorted, so rows that
    // share a PK but differ in payload (valid under (PK, payload) element
    // identity) are adjacent — skipping chunks byte-equal to their predecessor
    // is an O(n) pre-shrink that keeps `build`'s sort at d distinct keys
    // instead of n rows on duplicate-PK trace shards (MIN/MAX AggValueIndex).
    // `xor8::fingerprint` owns the narrow/wide derivation the probe side must
    // match exactly.
    let mut keys: Vec<u64> = Vec::with_capacity(pk_sz / stride);
    let mut prev: Option<&[u8]> = None;
    for chunk in pk_bytes.chunks_exact(stride) {
        if prev == Some(chunk) {
            continue;
        }
        prev = Some(chunk);
        keys.push(xxh::hash_u128(xor8::fingerprint(chunk)));
    }
    xor8::build(keys)
}

/// Per-call policy for the shard writers ([`write_shard_streaming`] /
/// `Batch::write_as_shard`).
///
/// `durable` fdatasyncs the file and its directory around the finalizing
/// rename — spills and barrier folds pass `false` (the barrier's by-path sweep
/// fdatasyncs them), compaction outputs and WAL-block conversions pass `true`.
/// `flags` is the persisted `OFF_FLAGS` header byte (`SHARD_FLAG_PK_UNIQUE`).
/// `pack_ints` enables FoR (`ENCODING_FOR`) on eligible integer payload
/// regions — set only by compaction; L0 spill/checkpoint writers stay raw.
#[derive(Clone, Copy, Default)]
pub struct ShardWriteOpts {
    pub durable: bool,
    pub flags: u8,
    pub pack_ints: bool,
}

/// Write the .tmp shard, then fdatasync (if `opts.durable`), close, and rename
/// to `basename`. The sole shard writer.
pub fn write_shard_streaming(
    dirfd: c_int,
    basename: &CStr,
    row_count: u32,
    regions: &[(*const u8, usize)],
    schema: &SchemaDescriptor,
    opts: ShardWriteOpts,
) -> Result<(), StorageError> {
    let (fd, tmp_name) = write_shard_streaming_inner(dirfd, basename, row_count, regions, schema, opts)?;
    if opts.durable && fdatasync_eintr(fd.as_raw_fd()).is_err() {
        unsafe {
            libc::unlinkat(dirfd, tmp_name.as_ptr(), 0);
        }
        return Err(StorageError::Io);
    }
    drop(fd); // Close before the rename, matching the batched two-phase path.
    unsafe {
        if libc::renameat(dirfd, tmp_name.as_ptr(), dirfd, basename.as_ptr()) < 0 {
            libc::unlinkat(dirfd, tmp_name.as_ptr(), 0);
            return Err(StorageError::Io);
        }
        // Flush the directory inode so the renamed entry survives a power loss
        // (fdatasync on the file alone does not flush the parent directory).
        if opts.durable {
            // Ignore EINTR-retried fsync errors on directory fds (best-effort).
            let _ = fsync_eintr(dirfd);
        }
    }
    Ok(())
}

/// Open .tmp shard, write header+regions+xor8, leave fd open and unsynced.
/// Caller is responsible for fdatasync, close, and rename. On error the fd
/// is closed and the .tmp is unlinked. `opts.flags` is written to the
/// `OFF_FLAGS` byte in the header; `opts.durable` is the caller's concern.
#[allow(clippy::needless_range_loop)]
fn write_shard_streaming_inner(
    dirfd: c_int,
    basename: &CStr,
    row_count: u32,
    regions: &[(*const u8, usize)],
    schema: &SchemaDescriptor,
    opts: ShardWriteOpts,
) -> Result<(OwnedFd, std::ffi::CString), StorageError> {
    let num_regions = regions.len();
    let n = row_count as usize;
    // Writer↔reader region-layout contract, shared with `MappedShard::open`:
    // `strides` holds each fixed-width region's per-element width, `nr` is the
    // trailing blob region's index. Debug-only checks: production callers derive
    // `regions` and `schema` from the same batch.
    let (strides, nr) = strides_from_schema(schema);
    let nr = nr as usize;
    debug_assert_eq!(num_regions, nr + 1, "region count must be 3 + num_payload_cols + 1");

    // --- Phase 1: detect encodings and compute actual sizes ---
    let mut encodings: Vec<RegionEncoding> = Vec::with_capacity(num_regions);
    let mut actual_sizes: Vec<usize> = Vec::with_capacity(num_regions);

    for i in 0..num_regions {
        let (src_ptr, orig_sz) = regions[i];
        // The blob region is variable-length (always Raw); null/empty regions
        // never reach `from_raw_parts`.
        if n == 0 || orig_sz == 0 || src_ptr.is_null() || i >= nr {
            encodings.push(RegionEncoding::Raw);
            actual_sizes.push(orig_sz);
            continue;
        }
        // A wrong-stride schema would silently mis-chunk the directory.
        let width = strides[i] as usize;
        debug_assert_eq!(
            orig_sz,
            n * width,
            "region {i}: schema width {width} × {n} rows != region size {orig_sz}"
        );
        let src = unsafe { std::slice::from_raw_parts(src_ptr, orig_sz) };
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
                    Some((_, buf)) => RegionEncoding::For { buf },
                    None => RegionEncoding::Raw,
                },
                _ => RegionEncoding::Raw,
            }
        };
        let actual = match &enc {
            RegionEncoding::Raw => orig_sz,
            _ => enc.encoded_bytes(src).len(),
        };
        encodings.push(enc);
        actual_sizes.push(actual);
    }

    // --- Phase 2: XOR8 filter built from pk region (pk_stride bytes/row, zero-extended to u128) ---
    let xor8_filter = if row_count > 0 {
        let (pk_ptr, pk_sz) = regions[REG_PK];
        build_xor8_from_pk_region(pk_ptr, pk_sz, schema.pk_stride() as usize)
    } else {
        None
    };

    // --- Phase 3: compute offsets ---
    let dir_size = num_regions * DIR_ENTRY_SIZE;
    let dir_offset = HEADER_SIZE;
    let mut pos = align64(dir_offset + dir_size);
    let mut region_offsets = Vec::with_capacity(num_regions);
    for &actual_sz in actual_sizes.iter().take(num_regions) {
        region_offsets.push(pos);
        pos = align64(pos + actual_sz);
    }
    // num_regions >= 4 always (pk, weight, null, blob at minimum).
    let data_end = region_offsets[num_regions - 1] + actual_sizes[num_regions - 1];

    let xor8_data = xor8_filter.as_ref().map(xor8::serialize);
    let xor8_offset = if xor8_data.is_some() { align64(data_end) } else { 0 };
    let xor8_size = xor8_data.as_ref().map_or(0, |d| d.len());
    let total_size = if xor8_data.is_some() {
        xor8_offset + xor8_size
    } else {
        data_end
    };

    // --- Phase 4: build header + directory buffer ---
    let hdr_dir_size = HEADER_SIZE + dir_size;
    let mut hdr_buf = vec![0u8; hdr_dir_size];

    for i in 0..num_regions {
        let (src_ptr, orig_sz) = regions[i];
        // Empty/null regions short-circuit to checksum 0 — never form a slice
        // from a null or zero-length region. Non-Raw encodings are only chosen
        // for non-empty, non-null regions.
        let cs = if actual_sizes[i] > 0 && !src_ptr.is_null() {
            let src = unsafe { std::slice::from_raw_parts(src_ptr, orig_sz) };
            xxh::checksum(encodings[i].encoded_bytes(src))
        } else {
            0
        };

        let d = dir_offset + i * DIR_ENTRY_SIZE;
        write_u64_le(&mut hdr_buf, d, region_offsets[i] as u64);
        write_u64_le(&mut hdr_buf, d + 8, actual_sizes[i] as u64);
        write_u64_le(&mut hdr_buf, d + 16, cs);
        let encoding_byte = match &encodings[i] {
            RegionEncoding::Raw => ENCODING_RAW,
            RegionEncoding::Constant { .. } => ENCODING_CONSTANT,
            RegionEncoding::TwoValue { .. } => ENCODING_TWO_VALUE,
            RegionEncoding::For { .. } => ENCODING_FOR,
        };
        hdr_buf[d + 24] = encoding_byte;
    }

    write_u64_le(&mut hdr_buf, OFF_MAGIC, SHARD_MAGIC);
    write_u64_le(&mut hdr_buf, OFF_VERSION, SHARD_VERSION);
    write_u64_le(&mut hdr_buf, OFF_ROW_COUNT, row_count as u64);
    write_u64_le(&mut hdr_buf, OFF_DIR_OFFSET, dir_offset as u64);
    write_u64_le(&mut hdr_buf, OFF_XOR8_OFFSET, xor8_offset as u64);
    write_u64_le(&mut hdr_buf, OFF_XOR8_SIZE, xor8_size as u64);
    hdr_buf[OFF_FLAGS] = opts.flags;

    let tmp_name = super::super::cstr_with_tmp_suffix(basename)?;

    // The `OwnedFd` is the sole closer, so every error return below closes
    // it — `abort` only unlinks.
    let fd =
        crate::foundation::posix_io::openat_owned(dirfd, &tmp_name, libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC)
            .ok_or(StorageError::Io)?;

    unsafe {
        let abort = || -> StorageError {
            libc::unlinkat(dirfd, tmp_name.as_ptr(), 0);
            StorageError::Io
        };

        crate::foundation::posix_io::ftruncate(fd.as_raw_fd(), total_size as i64).map_err(|_| abort())?;

        crate::foundation::posix_io::pwrite_all_fd(fd.as_raw_fd(), &hdr_buf, 0).map_err(|_| abort())?;

        for i in 0..num_regions {
            let (src_ptr, orig_sz) = regions[i];
            let r_off = region_offsets[i] as libc::off_t;
            // Same empty/null short-circuit as the checksum pass.
            if actual_sizes[i] > 0 && !src_ptr.is_null() {
                let src = std::slice::from_raw_parts(src_ptr, orig_sz);
                crate::foundation::posix_io::pwrite_all_fd(fd.as_raw_fd(), encodings[i].encoded_bytes(src), r_off)
                    .map_err(|_| abort())?;
            }
        }

        if let Some(ref data) = xor8_data {
            crate::foundation::posix_io::pwrite_all_fd(fd.as_raw_fd(), data, xor8_offset as libc::off_t)
                .map_err(|_| abort())?;
        }

        Ok((fd, tmp_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::codec::read_u64_le;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor, MAX_COLUMNS};
    use crate::storage::lsm::shard_reader::MappedShard;

    fn make_schema_desc(num_cols: u32, pk_index: u32) -> SchemaDescriptor {
        let mut cols = [SchemaColumn::new(0, 0); MAX_COLUMNS];
        cols[0] = SchemaColumn::new(8, 0);
        if num_cols > 1 {
            cols[1] = SchemaColumn::new(9, 0);
        }
        SchemaDescriptor::new(&cols[..num_cols as usize], &[pk_index])
    }

    #[test]
    fn build_image_roundtrip() {
        let row_count = 3u32;
        let pks: Vec<u64> = vec![10, 20, 30];
        let weights: Vec<i64> = vec![1, 1, 1];
        let nulls: Vec<u64> = vec![0, 0, 0];
        let vals: Vec<i64> = vec![100, 200, 300];

        // PK region is OPK (big-endian) at rest; U64 OPK == BE.
        let pk_bytes: Vec<u8> = pks.iter().flat_map(|p| p.to_be_bytes()).collect();
        let weight_bytes: Vec<u8> = weights.iter().flat_map(|w| w.to_le_bytes()).collect();
        let null_bytes: Vec<u8> = nulls.iter().flat_map(|n| n.to_le_bytes()).collect();
        let val_bytes: Vec<u8> = vals.iter().flat_map(|v| v.to_le_bytes()).collect();
        let blob: Vec<u8> = vec![];

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (weight_bytes.as_ptr(), weight_bytes.len()),
            (null_bytes.as_ptr(), null_bytes.len()),
            (val_bytes.as_ptr(), val_bytes.len()),
            (blob.as_ptr(), blob.len()),
        ];

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("build_image.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        write_shard_streaming(
            libc::AT_FDCWD,
            &cpath,
            row_count,
            &regions,
            &make_schema_desc(2, 0),
            ShardWriteOpts::default(),
        )
        .unwrap();
        let image = std::fs::read(&path).unwrap();

        assert_eq!(read_u64_le(&image, OFF_MAGIC), SHARD_MAGIC);
        assert_eq!(read_u64_le(&image, OFF_VERSION), SHARD_VERSION);
        assert_eq!(read_u64_le(&image, OFF_ROW_COUNT), 3);
        assert!(read_u64_le(&image, OFF_XOR8_OFFSET) > 0);
        assert!(read_u64_le(&image, OFF_XOR8_SIZE) > 0);
    }

    #[test]
    fn empty_shard() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let regions: Vec<(*const u8, usize)> = vec![
            (std::ptr::null(), 0),
            (std::ptr::null(), 0),
            (std::ptr::null(), 0),
            (std::ptr::null(), 0),
        ];

        // All-PK single-column schema (num_payload_cols = 0) → 4 regions.
        let schema = make_schema_desc(1, 0);
        write_shard_streaming(libc::AT_FDCWD, &cpath, 0, &regions, &schema, ShardWriteOpts::default()).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 0);
        assert!(!shard.has_xor8());
    }

    /// write_shard_streaming roundtrip — PK + weight + null_bmp + i64 value regions.
    #[test]
    fn test_write_shard_streaming_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("streaming.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let row_count = 3u32;
        let pks: Vec<u64> = vec![10, 20, 30];
        let weights: Vec<i64> = vec![1, 1, 1];
        let nulls: Vec<u64> = vec![0, 0, 0];
        let vals: Vec<i64> = vec![100, 200, 300];
        // PK region is OPK (big-endian) at rest; U64 OPK == BE.
        let pk_bytes: Vec<u8> = pks.iter().flat_map(|p| p.to_be_bytes()).collect();
        let blob: Vec<u8> = vec![];

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (nulls.as_ptr() as *const u8, nulls.len() * 8),
            (vals.as_ptr() as *const u8, vals.len() * 8),
            (blob.as_ptr(), 0),
        ];

        let schema = make_schema_desc(2, 0);
        let opts = ShardWriteOpts {
            durable: true,
            ..Default::default()
        };
        write_shard_streaming(libc::AT_FDCWD, &cpath, row_count, &regions, &schema, opts).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 3);
        assert_eq!(shard.get_pk(0), 10);
        assert_eq!(shard.get_pk(1), 20);
        assert_eq!(shard.get_pk(2), 30);
        assert_eq!(shard.get_weight(0), 1);
        assert_eq!(shard.get_weight(1), 1);
        assert_eq!(shard.get_weight(2), 1);
        assert!(shard.has_xor8());
        assert!(shard.xor8_may_contain(10));
        assert!(shard.xor8_may_contain(20));
        assert!(shard.xor8_may_contain(30));
    }

    #[test]
    fn u64_pk_shard_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("u64pk.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let n = 5u32;
        let pks: Vec<u64> = vec![100, 200, 300, 400, 500];
        let weights: Vec<i64> = vec![1; n as usize];
        let nulls: Vec<u64> = vec![0; n as usize];
        let vals: Vec<i64> = vec![10, 20, 30, 40, 50];
        // PK region is OPK (big-endian) at rest; U64 OPK == BE.
        let pk_bytes: Vec<u8> = pks.iter().flat_map(|p| p.to_be_bytes()).collect();
        assert_eq!(pk_bytes.len(), n as usize * 8, "U64 PK region must be 8B/row");
        let blob: Vec<u8> = vec![];

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (nulls.as_ptr() as *const u8, nulls.len() * 8),
            (vals.as_ptr() as *const u8, vals.len() * 8),
            (blob.as_ptr(), 0),
        ];

        let schema = make_schema_desc(2, 0);
        write_shard_streaming(libc::AT_FDCWD, &cpath, n, &regions, &schema, ShardWriteOpts::default()).unwrap();
        let image = std::fs::read(&path).unwrap();
        assert_eq!(
            read_u64_le(&image, OFF_VERSION),
            SHARD_VERSION,
            "must write current shard version"
        );

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.pk_stride, 8, "pk_stride must be 8 for U64 schema");
        assert_eq!(shard.count, n as usize);
        for (i, &expected_pk) in pks.iter().enumerate() {
            assert_eq!(shard.get_pk(i), expected_pk as u128, "get_pk row {i}");
        }
        assert!(shard.has_xor8());
        for &pk in &pks {
            assert!(shard.xor8_may_contain(pk as u128), "XOR8 must contain PK {pk}");
        }
    }

    #[test]
    fn u64_pk_constant_shard() {
        // All rows share the same PK → Constant encoding for the PK region.
        let n = 6u32;
        let pk_val: u64 = 42;
        let pks: Vec<u64> = vec![pk_val; n as usize];
        let weights: Vec<i64> = vec![1; n as usize];
        let nulls: Vec<u64> = vec![0; n as usize];
        let vals: Vec<i64> = (1..=n as i64).collect();
        // PK region is OPK (big-endian) at rest; U64 OPK == BE.
        let pk_bytes: Vec<u8> = pks.iter().flat_map(|p| p.to_be_bytes()).collect();
        let blob: Vec<u8> = vec![];

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (nulls.as_ptr() as *const u8, nulls.len() * 8),
            (vals.as_ptr() as *const u8, vals.len() * 8),
            (blob.as_ptr(), 0),
        ];

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("u64_const.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        write_shard_streaming(
            libc::AT_FDCWD,
            &cpath,
            n,
            &regions,
            &make_schema_desc(2, 0),
            ShardWriteOpts::default(),
        )
        .unwrap();
        let image = std::fs::read(&path).unwrap();

        // Constant-encoded PK region → directory entry size == 8 (one elem).
        assert_eq!(
            region_dir(&image, 0),
            (8, ENCODING_CONSTANT),
            "PK region must be Constant-encoded, storing a single 8B value"
        );
    }

    /// Bug 3: streaming write with regions that trigger Constant encoding.
    #[test]
    fn test_write_shard_streaming_encodings() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("streaming_enc.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let row_count = 4u32;
        let pks: Vec<u64> = vec![1, 2, 3, 4];
        let weights: Vec<i64> = vec![1, 1, 1, 1]; // all-same → Constant encoding
        let nulls: Vec<u64> = vec![0, 0, 0, 0]; // all-zero → Constant encoding
        let vals: Vec<i64> = vec![42, 42, 42, 42]; // all-same → Constant encoding
                                                   // PK region is OPK (big-endian) at rest; U64 OPK == BE.
        let pk_bytes: Vec<u8> = pks.iter().flat_map(|p| p.to_be_bytes()).collect();
        let blob: Vec<u8> = vec![];

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (nulls.as_ptr() as *const u8, nulls.len() * 8),
            (vals.as_ptr() as *const u8, vals.len() * 8),
            (blob.as_ptr(), 0),
        ];

        let schema = make_schema_desc(2, 0);
        let opts = ShardWriteOpts {
            durable: true,
            ..Default::default()
        };
        write_shard_streaming(libc::AT_FDCWD, &cpath, row_count, &regions, &schema, opts).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 4);
        for i in 0..4 {
            assert_eq!(shard.get_pk(i), (i + 1) as u128);
            assert_eq!(shard.get_weight(i), 1);
        }
    }

    /// Pins every `(region role → encoding)` pair the writer can emit to today's
    /// behavior. A shard has one weight and one null_bmp region, so one shard can
    /// pin at most one weight and one null encoding; three shards choreograph all
    /// ten pairs — PK{Constant,Raw}, Weight{Constant,TwoValue,Raw},
    /// NullBmp{Constant,Raw}, Payload{Constant,Raw}, Blob{Raw}.
    #[test]
    fn encoding_selection_pins_all_roles() {
        let dir = tempfile::tempdir().unwrap();
        let write_and_read =
            |schema: &SchemaDescriptor, n: u32, regions: &[(*const u8, usize)], name: &str| -> Vec<u8> {
                let path = dir.path().join(name);
                let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
                write_shard_streaming(libc::AT_FDCWD, &cpath, n, regions, schema, ShardWriteOpts::default()).unwrap();
                std::fs::read(&path).unwrap()
            };

        // --- Shard A (Constant-heavy): constant PK, all-1 weight, all-0 nulls,
        // one constant + one varying payload column, non-empty blob. ---
        let schema_a = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0), // PK
                SchemaColumn::new(type_code::I64, 0), // constant payload
                SchemaColumn::new(type_code::I64, 0), // varying payload
            ],
            &[0],
        );
        let n_a = 4usize;
        let pk_a: Vec<u8> = vec![7u64; n_a].iter().flat_map(|p| p.to_be_bytes()).collect();
        let w_a: Vec<i64> = vec![1; n_a];
        let null_a: Vec<u64> = vec![0; n_a];
        let const_col: Vec<i64> = vec![42; n_a];
        let vary_col: Vec<i64> = (0..n_a as i64).collect();
        let blob_a: Vec<u8> = vec![0xAA, 0xBB, 0xCC];
        let regions_a: Vec<(*const u8, usize)> = vec![
            (pk_a.as_ptr(), pk_a.len()),
            (w_a.as_ptr() as *const u8, w_a.len() * 8),
            (null_a.as_ptr() as *const u8, null_a.len() * 8),
            (const_col.as_ptr() as *const u8, const_col.len() * 8),
            (vary_col.as_ptr() as *const u8, vary_col.len() * 8),
            (blob_a.as_ptr(), blob_a.len()),
        ];
        let img_a = write_and_read(&schema_a, n_a as u32, &regions_a, "pin_a.db");
        assert_eq!(region_dir(&img_a, 0), (8, ENCODING_CONSTANT), "A pk constant");
        assert_eq!(region_dir(&img_a, 1), (8, ENCODING_CONSTANT), "A weight constant");
        assert_eq!(region_dir(&img_a, 2), (8, ENCODING_CONSTANT), "A null constant");
        assert_eq!(region_dir(&img_a, 3), (8, ENCODING_CONSTANT), "A payload constant");
        assert_eq!(
            region_dir(&img_a, 4),
            (n_a * 8, ENCODING_RAW),
            "A payload varying → raw"
        );
        assert_eq!(region_dir(&img_a, 5), (blob_a.len(), ENCODING_RAW), "A blob raw");

        // --- Shard B (TwoValue weight, Raw nulls): distinct PKs, alternating
        // 1/-1 weights, a nullable column NULL on a subset so the null_bmp holds
        // ≥2 distinct null-words. ---
        let schema_b = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 1), // nullable
            ],
            &[0],
        );
        let n_b = 4usize;
        let pk_b: Vec<u8> = (1u64..=n_b as u64).flat_map(|p| p.to_be_bytes()).collect();
        let w_b: Vec<i64> = vec![1, -1, 1, -1];
        let null_b: Vec<u64> = vec![1, 0, 1, 0]; // col-0 null bit set on rows 0,2
        let pay_b: Vec<i64> = vec![10, 20, 30, 40];
        let blob_b: Vec<u8> = vec![];
        let regions_b: Vec<(*const u8, usize)> = vec![
            (pk_b.as_ptr(), pk_b.len()),
            (w_b.as_ptr() as *const u8, w_b.len() * 8),
            (null_b.as_ptr() as *const u8, null_b.len() * 8),
            (pay_b.as_ptr() as *const u8, pay_b.len() * 8),
            (blob_b.as_ptr(), 0),
        ];
        let img_b = write_and_read(&schema_b, n_b as u32, &regions_b, "pin_b.db");
        assert_eq!(region_dir(&img_b, 0), (n_b * 8, ENCODING_RAW), "B pk distinct → raw");
        assert_eq!(
            region_dir(&img_b, 1),
            (16 + n_b.div_ceil(8), ENCODING_TWO_VALUE),
            "B weight two-value"
        );
        assert_eq!(region_dir(&img_b, 2), (n_b * 8, ENCODING_RAW), "B null mixed → raw");

        // --- Shard C (Raw weight): ≥3 distinct weight values. ---
        let schema_c = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let n_c = 3usize;
        let pk_c: Vec<u8> = (1u64..=n_c as u64).flat_map(|p| p.to_be_bytes()).collect();
        let w_c: Vec<i64> = vec![1, -1, 2];
        let null_c: Vec<u64> = vec![0; n_c];
        let pay_c: Vec<i64> = vec![5, 6, 7];
        let blob_c: Vec<u8> = vec![];
        let regions_c: Vec<(*const u8, usize)> = vec![
            (pk_c.as_ptr(), pk_c.len()),
            (w_c.as_ptr() as *const u8, w_c.len() * 8),
            (null_c.as_ptr() as *const u8, null_c.len() * 8),
            (pay_c.as_ptr() as *const u8, pay_c.len() * 8),
            (blob_c.as_ptr(), 0),
        ];
        let img_c = write_and_read(&schema_c, n_c as u32, &regions_c, "pin_c.db");
        assert_eq!(
            region_dir(&img_c, 1),
            (n_c * 8, ENCODING_RAW),
            "C weight ≥3 distinct → raw"
        );
    }

    #[test]
    fn build_xor8_wide_compound_region() {
        let stride = 24usize;
        let rows: Vec<Vec<u8>> = (0u8..5)
            .map(|r| (0..stride).map(|b| r.wrapping_add(b as u8)).collect())
            .collect();
        let pk_bytes: Vec<u8> = rows.iter().flatten().copied().collect();
        let f = build_xor8_from_pk_region(pk_bytes.as_ptr(), pk_bytes.len(), stride)
            .expect("wide compound region must build a filter");
        for row in &rows {
            assert!(
                xor8::may_contain(&f, crate::foundation::xxh::checksum(row) as u128),
                "no false negative for wide-region row"
            );
        }
    }

    #[test]
    fn build_xor8_narrow_compound_region() {
        let stride = 12usize;
        let rows: Vec<Vec<u8>> = (0u8..5)
            .map(|r| (0..stride).map(|b| r.wrapping_mul(7).wrapping_add(b as u8)).collect())
            .collect();
        let pk_bytes: Vec<u8> = rows.iter().flatten().copied().collect();
        let f = build_xor8_from_pk_region(pk_bytes.as_ptr(), pk_bytes.len(), stride)
            .expect("narrow compound region must build a filter");
        for row in &rows {
            // Builder fingerprint for stride <= 16 is widen_pk_be(OPK bytes);
            // the probe must derive the same value.
            assert!(
                xor8::may_contain(&f, gnitz_wire::widen_pk_be(row, stride)),
                "no false negative for narrow-compound row"
            );
        }
    }

    #[test]
    fn build_xor8_single_pk_regression() {
        let pks64: Vec<u64> = vec![10, 20, 30, 40];
        // OPK at rest: U64 region is big-endian.
        let b64: Vec<u8> = pks64.iter().flat_map(|p| p.to_be_bytes()).collect();
        let f64 = build_xor8_from_pk_region(b64.as_ptr(), b64.len(), 8).expect("8-byte region must build a filter");
        for p in &pks64 {
            assert!(xor8::may_contain(&f64, *p as u128));
        }

        let pks128: Vec<u128> = vec![1, 1 << 64, u128::MAX, 12345];
        // OPK at rest: U128 region is big-endian.
        let b128: Vec<u8> = pks128.iter().flat_map(|p| p.to_be_bytes()).collect();
        let f128 =
            build_xor8_from_pk_region(b128.as_ptr(), b128.len(), 16).expect("16-byte region must build a filter");
        for p in &pks128 {
            assert!(xor8::may_contain(&f128, *p));
        }
    }

    const FLAG: u8 = SHARD_FLAG_PK_UNIQUE;

    /// `flags()` after observing the given OPK byte rows in order.
    fn tag(rows: &[(&[u8], i64)]) -> u8 {
        let mut c = PkUniqueChecker::new();
        for &(pk, w) in rows {
            c.observe(pack_pk_be(pk), pk, w);
        }
        c.flags()
    }

    #[test]
    fn pk_unique_checker_narrow_adjacent_duplicate() {
        let k5 = 5u64.to_be_bytes();
        let k6 = 6u64.to_be_bytes();
        // Regression: a narrow (≤16B) duplicate appearing exactly twice was
        // missed. pack_pk_be is injective for ≤16B, so the prefix tie alone
        // proves the duplicate — last_pk is not maintained for narrow keys, and
        // the old fall-through byte compare ran against a stale/zero buffer.
        assert_eq!(tag(&[(&k5, 1), (&k5, 1)]), 0, "adjacent narrow dup must disqualify");
        assert_eq!(tag(&[(&k5, 1), (&k6, 1)]), FLAG, "distinct narrow keys qualify");
        assert_eq!(tag(&[(&k5, 1)]), FLAG, "single key qualifies");
        // Three consecutive copies were caught even before the fix; assert the
        // post-fix behavior is still correct.
        assert_eq!(tag(&[(&k5, 1), (&k5, 1), (&k5, 1)]), 0);
    }

    #[test]
    fn pk_unique_checker_all_zero_key() {
        // The all-zero key aliases the initial last_prefix/last_pk state. A lone
        // zero key (or zero followed by a distinct key) must still qualify; only
        // an actual adjacent zero duplicate disqualifies. The audit's proposed
        // short-circuit dropped the has_last guard and regressed this case.
        let z = 0u64.to_be_bytes();
        let k1 = 1u64.to_be_bytes();
        assert_eq!(tag(&[(&z, 1)]), FLAG, "lone zero key qualifies");
        assert_eq!(tag(&[(&z, 1), (&k1, 1)]), FLAG, "zero then distinct qualifies");
        assert_eq!(tag(&[(&z, 1), (&z, 1)]), 0, "adjacent zero dup disqualifies");
    }

    #[test]
    fn pk_unique_checker_wide_prefix_twins() {
        // Wide (>16B) keys sharing a 16-byte prefix: the prefix tie is not
        // authoritative, so the full byte compare on last_pk decides.
        let mut a = [0u8; 24];
        let mut b = [0u8; 24];
        a[..16].copy_from_slice(&[7u8; 16]);
        b[..16].copy_from_slice(&[7u8; 16]);
        a[16] = 1;
        b[16] = 2;
        assert_eq!(
            tag(&[(&a, 1), (&b, 1)]),
            FLAG,
            "wide prefix-twins, distinct tails qualify"
        );
        assert_eq!(tag(&[(&a, 1), (&a, 1)]), 0, "identical wide keys disqualify");
        // Interleaving a distinct-prefix row must not lose the wide last_pk.
        let c = [9u8; 24];
        assert_eq!(
            tag(&[(&a, 1), (&c, 1), (&a, 1)]),
            FLAG,
            "non-adjacent wide repeats qualify"
        );
    }

    #[test]
    fn pk_unique_checker_negative_weight() {
        let k1 = 1u64.to_be_bytes();
        let k2 = 2u64.to_be_bytes();
        // A negative weight disqualifies regardless of PK distinctness.
        assert_eq!(tag(&[(&k1, 1), (&k2, -1)]), 0, "negative weight disqualifies");
    }
}

#[cfg(test)]
mod for_codec_tests {
    use super::{align64, decode_for_region, encode_for_region, for_encoded_size, for_params};
    use crate::test_rng::Rng;

    /// Pack signed/unsigned values into a raw `stride`-byte-per-cell LE region,
    /// exactly as the payload region would appear on the batch.
    fn pack(vals: &[i128], stride: usize) -> Vec<u8> {
        let mut b = Vec::with_capacity(vals.len() * stride);
        for &v in vals {
            let le = (v as i64 as u64).to_le_bytes();
            b.extend_from_slice(&le[..stride]);
        }
        b
    }

    /// Encode → assert size → decode → assert byte-exact reproduction of the
    /// input region. Returns the chosen `bw` (None ⇒ region stays Raw).
    fn roundtrip(vals: &[i128], stride: usize, signed: bool) -> Option<usize> {
        let raw = pack(vals, stride);
        let n = vals.len();
        let (bw, image) = encode_for_region(&raw, stride, signed, n)?;
        assert_eq!(image.len(), for_encoded_size(n, bw), "emitted size == for_encoded_size");
        assert!(bw >= 1 && bw < stride, "bw {bw} in [1, {stride})");
        let decoded = decode_for_region(&image, n, stride);
        let bytes = decoded.as_bytes();
        assert_eq!(bytes.as_ptr() as usize % 8, 0, "decoded buffer 8-aligned");
        assert_eq!(bytes, &raw[..], "byte-exact roundtrip (bw={bw}, stride={stride})");
        Some(bw)
    }

    #[test]
    fn stride1_never_packs() {
        // U8/I8 (stride 1) can never pack: bw >= 1 == stride.
        assert_eq!(roundtrip(&[0, 1, 2, 3, 200], 1, false), None);
        assert_eq!(roundtrip(&[-5, 0, 5, 100], 1, true), None);
    }

    #[test]
    fn small_unsigned_bw1_bw2() {
        // Small values → bw 1.
        let small: Vec<i128> = (0..300).map(|i| (i % 200) as i128).collect();
        assert_eq!(roundtrip(&small, 4, false), Some(1));
        assert_eq!(roundtrip(&small, 8, false), Some(1));
        // Span needing 2 bytes.
        let mid: Vec<i128> = (0..300).map(|i| ((i * 211) % 60000) as i128).collect();
        assert_eq!(roundtrip(&mid, 4, false), Some(2));
    }

    #[test]
    fn high_floor_unsigned_drops_bytes() {
        // A tight range far from zero frames on its min and drops the high bytes.
        let hi: Vec<i128> = (0..256).map(|i| 1_000_000 + (i % 50) as i128).collect();
        assert_eq!(roundtrip(&hi, 4, false), Some(1));
        let hi64: Vec<i128> = (0..256).map(|i| 5_000_000_000i128 + (i % 40) as i128).collect();
        // Span < 256 → bw 1 even though the values need 5 bytes raw.
        assert_eq!(roundtrip(&hi64, 8, false), Some(1));
    }

    #[test]
    fn signed_negative_min_frames_on_min() {
        // Range spanning zero frames on the signed min, not on 0.
        let s: Vec<i128> = (0..256).map(|i| -100 + (i % 150) as i128).collect();
        assert_eq!(roundtrip(&s, 4, true), Some(1));
        assert_eq!(roundtrip(&s, 8, true), Some(1));
        // Larger signed span, still one frame.
        let s2: Vec<i128> = (0..500).map(|i| -30_000 + (i % 60000) as i128).collect();
        assert_eq!(roundtrip(&s2, 4, true), Some(2));
    }

    #[test]
    fn all_equal_declines_for() {
        // Constant is claimed before FoR; for_params/encode return None.
        let eq = vec![42i128; 100];
        assert_eq!(for_params(&pack(&eq, 4), 4, false, 100), None);
        assert_eq!(encode_for_region(&pack(&eq, 4), 4, false, 100), None);
    }

    #[test]
    fn extremes_fall_back_to_raw() {
        // MIN/MAX span needs the full stride → bw >= stride → Raw.
        let ext_u: Vec<i128> = vec![0, u32::MAX as i128];
        assert_eq!(roundtrip(&ext_u, 4, false), None);
        let ext_s: Vec<i128> = vec![i32::MIN as i128, i32::MAX as i128];
        assert_eq!(roundtrip(&ext_s, 4, true), None);
        let ext_u64: Vec<i128> = vec![0, u64::MAX as i128];
        assert_eq!(roundtrip(&ext_u64, 8, false), None);
        let ext_i64: Vec<i128> = vec![i64::MIN as i128, i64::MAX as i128];
        assert_eq!(roundtrip(&ext_i64, 8, true), None);
    }

    #[test]
    fn null_zero_cells_roundtrip_bit_exact() {
        // NULL cells carry a zeroed value that joins the region's [min, max].
        // Packing frames on 0 and the zeros must round-trip bit-exact.
        let mut v: Vec<i128> = Vec::new();
        for i in 0..200 {
            v.push(if i % 3 == 0 { 0 } else { 1_000_000 + (i % 500) as i128 });
        }
        // min 0, max ~1_000_500 → 3-byte span.
        assert_eq!(roundtrip(&v, 4, false), Some(3));
        // Signed variant: NULL zeros among negative values → min is negative.
        let mut vs: Vec<i128> = Vec::new();
        for i in 0..200 {
            vs.push(if i % 4 == 0 { 0 } else { -500_000 - (i % 300) as i128 });
        }
        assert_eq!(roundtrip(&vs, 4, true), Some(3));
    }

    #[test]
    fn seeded_random_every_eligible_type() {
        let mut rng = Rng::new(0x9E3779B97F4A7C15);
        // (stride, signed, value mask) for every eligible fixed-int type code.
        let cases: &[(usize, bool)] = &[(2, false), (2, true), (4, false), (4, true), (8, false), (8, true)];
        for &(stride, signed) in cases {
            for &n in &[1usize, 1023, 1024, 10000] {
                // Draw a random tight base and a random small span so most
                // regions pack; a few will naturally decline.
                let base = rng.next_u64();
                let span = 1 + (rng.next_u64() % 4000);
                let vals: Vec<i128> = (0..n)
                    .map(|_| {
                        let off = (rng.next_u64() % span) as i128;
                        if signed {
                            // Center the window around a signed base.
                            (base as i64).wrapping_add(off as i64) as i128
                        } else {
                            // Mask into the type width to stay in range.
                            let masked = if stride == 2 {
                                (base as u16 as u64).wrapping_add(off as u64) as u16 as u64
                            } else if stride == 4 {
                                (base as u32 as u64).wrapping_add(off as u64) as u32 as u64
                            } else {
                                base.wrapping_add(off as u64)
                            };
                            masked as i128
                        }
                    })
                    .collect();
                // Whatever the verdict, the roundtrip helper asserts byte-exact
                // reproduction whenever it does pack; a None means Raw, also fine.
                roundtrip(&vals, stride, signed);
            }
        }
    }

    #[test]
    fn aligned_footprint_tie_declines() {
        // 10 U32 rows: raw 40 B, packed 8 + 10·2 = 28 B — both align64 to 64.
        // No block dropped → decline (stay Raw) despite a raw-byte win.
        let vals: Vec<i128> = (0..10).map(|i| (i * 1000) as i128).collect();
        assert_eq!(align64(40), align64(28));
        assert_eq!(roundtrip(&vals, 4, false), None, "aligned footprints tie → Raw");
        // 100 U32 rows with the same per-row span pack (align64 drops blocks).
        let many: Vec<i128> = (0..100).map(|i| (i % 60000) as i128).collect();
        assert!(roundtrip(&many, 4, false).is_some(), "100 rows pack");
    }

    /// FoR decode-throughput + compression-ratio microbench. Run in release:
    /// `cargo test -p gnitz-engine --release for_decode_bench --
    ///   --ignored --nocapture --test-threads=1`
    #[test]
    #[ignore]
    fn for_decode_bench() {
        use std::hint::black_box;
        use std::time::Instant;

        // A representative re-keyed ex-PK column: 1M I64 rows over a narrow
        // range far from zero (the `_int_`/`_hist_`/`_reduce_in_` shape).
        let n = 1_000_000usize;
        let vals: Vec<i128> = (0..n).map(|i| 3_000_000_000i128 + (i % 4000) as i128).collect();
        let raw = pack(&vals, 8);
        let (bw, image) = encode_for_region(&raw, 8, true, n).expect("region must pack");

        let raw_bytes = align64(n * 8);
        let packed_bytes = align64(for_encoded_size(n, bw));
        println!(
            "FoR ratio: {n} I64 rows, bw={bw}, raw(aligned)={raw_bytes}B packed(aligned)={packed_bytes}B \
             ({:.2}x)",
            raw_bytes as f64 / packed_bytes as f64
        );

        let iters = 200;
        let start = Instant::now();
        for _ in 0..iters {
            let decoded = decode_for_region(black_box(&image), n, 8);
            black_box(&decoded);
        }
        let elapsed = start.elapsed();
        let vals_per_s = (n as f64 * iters as f64) / elapsed.as_secs_f64();
        println!(
            "FoR decode: {:.1} M values/s ({:.2} ms per {n}-row region)",
            vals_per_s / 1e6,
            elapsed.as_secs_f64() * 1e3 / iters as f64
        );
    }
}
