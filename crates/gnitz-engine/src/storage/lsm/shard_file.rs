//! Shard file image building and atomic writing.
//!
//! Shared by the compaction path (`compact.rs`) and the memtable flush path
//! (`memtable.rs`).

use std::ffi::CStr;
#[cfg(test)]
use std::ptr;

use libc::c_int;

use super::layout::*;
use crate::foundation::codec::write_u64_le;
use super::error::StorageError;
use super::xor8;
use xorf::Xor8;
use crate::foundation::xxh;
use super::merge::pack_pk_be;
use crate::schema::MAX_PK_BYTES;
use crate::foundation::posix_io::{fdatasync_eintr, fsync_eintr};

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

    /// Observe one output row. `pk_bytes` is the OPK-encoded key slice (sorted).
    /// `weight` is the net row weight after consolidation.
    pub fn observe(&mut self, pk_bytes: &[u8], weight: i64) {
        if !self.qualifies { return; }
        if weight < 0 {
            self.qualifies = false;
            return;
        }
        let prefix = pack_pk_be(pk_bytes);
        let n = pk_bytes.len().min(MAX_PK_BYTES);
        // Is this row an adjacent duplicate of its predecessor? For narrow keys
        // (n ≤ 16) pack_pk_be is injective, so an equal prefix *is* an equal PK
        // and last_pk is never maintained; for wide keys the prefix is lossy, so
        // a prefix tie is only confirmed by the full byte compare. The has_last
        // guard keeps the first row — or a genuine all-zero key, which aliases
        // the zero-initialised last_prefix/last_pk — from reading as a duplicate.
        let is_duplicate = self.has_last
            && prefix == self.last_prefix
            && (n <= 16 || self.last_pk[..n] == pk_bytes[..n]);
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

    /// Returns `SHARD_FLAG_PK_UNIQUE` when `enabled` and the observed stream
    /// was unique, otherwise 0. Convenience for flush and compaction callers.
    pub fn flags_if(&self, enabled: bool) -> u8 {
        use super::layout::SHARD_FLAG_PK_UNIQUE;
        if enabled && self.qualifies { SHARD_FLAG_PK_UNIQUE } else { 0 }
    }
}

// ---------------------------------------------------------------------------
// Per-region encoding detection (internal to build_shard_image)
// ---------------------------------------------------------------------------

#[allow(dead_code)]
enum RegionEncoding {
    Raw,
    Constant { value: [u8; 16] },
    TwoValue { value_a: i64, value_b: i64, bitvec: Vec<u8> },
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
    let mut value = [0u8; 16];
    value[..element_width].copy_from_slice(first);
    RegionEncoding::Constant { value }
}

/// Detect weight encoding: constant, two-value (bitvec), or raw.
fn detect_weight_encoding(data: &[u8]) -> RegionEncoding {
    debug_assert!(!data.is_empty() && data.len().is_multiple_of(8));
    let n = data.len() / 8;
    let first = i64::from_le_bytes(data[..8].try_into().unwrap());
    let mut second: Option<i64> = None;

    // Pass 1: determine how many distinct values exist without allocating.
    for i in 1..n {
        let v = i64::from_le_bytes(data[i * 8..(i + 1) * 8].try_into().unwrap());
        if v == first {
            // matches first value
        } else if let Some(b) = second {
            if v != b {
                return RegionEncoding::Raw; // 3+ distinct values
            }
        } else {
            second = Some(v);
        }
    }

    match second {
        None => {
            let mut value = [0u8; 16];
            value[..8].copy_from_slice(&first.to_le_bytes());
            RegionEncoding::Constant { value }
        }
        Some(second_val) => {
            // Pass 2: exactly 2 distinct values — build the bitvec now.
            let bitvec_len = n.div_ceil(8);
            let mut bitvec = vec![0u8; bitvec_len];
            for i in 1..n {
                let v = i64::from_le_bytes(data[i * 8..(i + 1) * 8].try_into().unwrap());
                if v != first {
                    bitvec[i / 8] |= 1 << (i % 8);
                }
            }
            RegionEncoding::TwoValue {
                value_a: first,
                value_b: second_val,
                bitvec,
            }
        }
    }
}

fn build_xor8_from_pk_region(pk_ptr: *const u8, pk_sz: usize, n: usize) -> Option<Xor8> {
    if pk_ptr.is_null() || n == 0 || pk_sz == 0 {
        return None;
    }
    let stride = pk_sz / n;
    let pk_bytes = unsafe { std::slice::from_raw_parts(pk_ptr, pk_sz) };
    // One fingerprint per row's OPK region (`stride` bytes); `xor8::fingerprint`
    // owns the narrow/wide derivation that the probe side must match exactly.
    let mut pks: Vec<u128> = pk_bytes.chunks_exact(stride).map(xor8::fingerprint).collect();
    // The XOR filter's hypergraph peeling fails (hang/panic) on duplicate
    // fingerprints. The PK region is sorted, so rows that share a PK but
    // differ in payload (valid under (PK, payload) element identity) produce
    // adjacent identical fingerprints; dedup collapses them in O(n).
    pks.dedup();
    xor8::build(&pks)
}

/// Build a complete shard file image from pre-built region buffers.
///
/// `regions` is an array of (pointer, size) pairs in the canonical order:
///   pk (pk_stride bytes/row), weight, null_bitmap, [non-PK columns...], blob_arena.
///
/// The XOR8 filter is built internally from region[0] (pk) when `row_count > 0`.
///
/// Returns the complete file image ready for writing.
#[cfg(test)]
pub(crate) fn build_shard_image(
    table_id: u32,
    row_count: u32,
    regions: &[(*const u8, usize)],
) -> Vec<u8> {
    let num_regions = regions.len();
    let n = row_count as usize;
    let last_region = if num_regions > 0 { num_regions - 1 } else { 0 };

    // --- Phase 1: detect encodings and compute actual sizes ---

    let mut encodings: Vec<RegionEncoding> = Vec::with_capacity(num_regions);
    let mut actual_sizes: Vec<usize> = Vec::with_capacity(num_regions);

    for (i, &(src_ptr, orig_sz)) in regions.iter().enumerate().take(num_regions) {
        if n == 0 || i == last_region || orig_sz == 0 || src_ptr.is_null() {
            // Empty region, blob arena (last), or zero rows -> Raw passthrough
            encodings.push(RegionEncoding::Raw);
            actual_sizes.push(orig_sz);
        } else {
            let elem_width = orig_sz / n;
            let src = unsafe { std::slice::from_raw_parts(src_ptr, orig_sz) };
            let enc = if i == 1 {
                // weight region
                detect_weight_encoding(src)
            } else {
                detect_encoding(src, elem_width)
            };
            let actual = match &enc {
                RegionEncoding::Raw => orig_sz,
                RegionEncoding::Constant { .. } => elem_width,
                RegionEncoding::TwoValue { bitvec, .. } => 16 + bitvec.len(),
            };
            encodings.push(enc);
            actual_sizes.push(actual);
        }
    }

    // --- XOR8 filter built from pk region (pk_stride bytes/row, zero-extended to u128) ---

    let xor8_filter = if row_count > 0 && num_regions >= 1 {
        let (pk_ptr, pk_sz) = regions[0];
        build_xor8_from_pk_region(pk_ptr, pk_sz, n)
    } else {
        None
    };

    // --- Phase 2: compute offsets, allocate, and write ---

    let dir_size = num_regions * DIR_ENTRY_SIZE;
    let dir_offset = HEADER_SIZE;

    let mut pos = align64(dir_offset + dir_size);
    let mut region_offsets = Vec::with_capacity(num_regions);
    for &actual_sz in actual_sizes.iter().take(num_regions) {
        region_offsets.push(pos);
        pos = align64(pos + actual_sz);
    }
    let data_end = if num_regions == 0 {
        HEADER_SIZE
    } else {
        region_offsets[num_regions - 1] + actual_sizes[num_regions - 1]
    };

    let xor8_data = xor8_filter.as_ref().map(xor8::serialize);
    let xor8_offset = if xor8_data.is_some() {
        align64(data_end)
    } else {
        0
    };
    let xor8_size = xor8_data.as_ref().map_or(0, |d| d.len());
    let total_size = if xor8_data.is_some() {
        xor8_offset + xor8_size
    } else {
        data_end
    };

    let mut image = vec![0u8; total_size];

    // Write region data
    for i in 0..num_regions {
        let r_off = region_offsets[i];
        let (src_ptr, orig_sz) = regions[i];

        match &encodings[i] {
            RegionEncoding::Raw => {
                if orig_sz > 0 && !src_ptr.is_null() {
                    unsafe {
                        ptr::copy_nonoverlapping(src_ptr, image[r_off..].as_mut_ptr(), orig_sz);
                    }
                }
            }
            RegionEncoding::Constant { .. } => {
                // Copy first elem_width bytes from source
                let elem_width = actual_sizes[i];
                unsafe {
                    ptr::copy_nonoverlapping(src_ptr, image[r_off..].as_mut_ptr(), elem_width);
                }
            }
            RegionEncoding::TwoValue { value_a, value_b, bitvec } => {
                image[r_off..r_off + 8].copy_from_slice(&value_a.to_le_bytes());
                image[r_off + 8..r_off + 16].copy_from_slice(&value_b.to_le_bytes());
                image[r_off + 16..r_off + 16 + bitvec.len()].copy_from_slice(bitvec);
            }
        }

        // Checksum over what was actually written
        let a_sz = actual_sizes[i];
        let cs = if a_sz > 0 {
            xxh::checksum(&image[r_off..r_off + a_sz])
        } else {
            0
        };

        // Write 32-byte directory entry
        let d = dir_offset + i * DIR_ENTRY_SIZE;
        write_u64_le(&mut image, d, r_off as u64);
        write_u64_le(&mut image, d + 8, a_sz as u64);
        write_u64_le(&mut image, d + 16, cs);
        let encoding_byte = match &encodings[i] {
            RegionEncoding::Raw => ENCODING_RAW,
            RegionEncoding::Constant { .. } => ENCODING_CONSTANT,
            RegionEncoding::TwoValue { .. } => ENCODING_TWO_VALUE,
        };
        image[d + 24] = encoding_byte;
        // bytes [d+25..d+32] are already zero (reserved)
    }

    // Header
    write_u64_le(&mut image, OFF_MAGIC, SHARD_MAGIC);
    write_u64_le(&mut image, OFF_VERSION, SHARD_VERSION);
    write_u64_le(&mut image, OFF_ROW_COUNT, row_count as u64);
    write_u64_le(&mut image, OFF_DIR_OFFSET, dir_offset as u64);
    write_u64_le(&mut image, OFF_TABLE_ID, table_id as u64);
    write_u64_le(&mut image, OFF_XOR8_OFFSET, xor8_offset as u64);
    write_u64_le(&mut image, OFF_XOR8_SIZE, xor8_size as u64);

    // XOR8 filter data
    if let Some(ref data) = xor8_data {
        image[xor8_offset..xor8_offset + data.len()].copy_from_slice(data);
    }

    image
}

/// pwrite loop that handles short writes and EINTR.
unsafe fn pwrite_all(fd: c_int, buf: &[u8], mut offset: libc::off_t) -> Result<(), StorageError> {
    let mut remaining = buf.len();
    let mut p = buf.as_ptr();
    while remaining > 0 {
        let written = libc::pwrite(fd, p as *const libc::c_void, remaining, offset);
        if written < 0 {
            let e = *libc::__errno_location();
            if e == libc::EINTR {
                continue;
            }
            return Err(StorageError::Io);
        }
        if written == 0 {
            return Err(StorageError::Io);
        }
        remaining -= written as usize;
        p = p.add(written as usize);
        offset += written as libc::off_t;
    }
    Ok(())
}

/// Open .tmp shard file (basename + ".tmp") and write the full shard image to
/// it, but do NOT fdatasync, close, or rename. Returns the open fd plus the
/// .tmp basename so the caller can batch the fdatasync and perform the rename
/// later. On any internal error the fd is closed and the .tmp is unlinked.
pub struct PreparedShard {
    pub fd: c_int,
    pub tmp_name: std::ffi::CString,
}

#[allow(clippy::needless_range_loop)]
pub fn write_shard_streaming_prepare(
    dirfd: c_int,
    basename: &CStr,
    table_id: u32,
    row_count: u32,
    regions: &[(*const u8, usize)],
    flags: u8,
) -> Result<PreparedShard, StorageError> {
    let (fd, tmp_name) = write_shard_streaming_inner(dirfd, basename, table_id, row_count, regions, flags)?;
    Ok(PreparedShard { fd, tmp_name })
}

/// Write the .tmp shard, then fdatasync (if durable), close, and rename to
/// `basename`. Used by compaction outputs and non-durable / single-table flush
/// paths where batching across partitions is not needed.
pub fn write_shard_streaming(
    dirfd: c_int,
    basename: &CStr,
    table_id: u32,
    row_count: u32,
    regions: &[(*const u8, usize)],
    durable: bool,
    flags: u8,
) -> Result<(), StorageError> {
    let prepared = write_shard_streaming_prepare(dirfd, basename, table_id, row_count, regions, flags)?;
    if durable && fdatasync_eintr(prepared.fd).is_err() {
        unsafe {
            libc::close(prepared.fd);
            libc::unlinkat(dirfd, prepared.tmp_name.as_ptr(), 0);
        }
        return Err(StorageError::Io);
    }
    unsafe { libc::close(prepared.fd); }
    unsafe {
        if libc::renameat(dirfd, prepared.tmp_name.as_ptr(), dirfd, basename.as_ptr()) < 0 {
            libc::unlinkat(dirfd, prepared.tmp_name.as_ptr(), 0);
            return Err(StorageError::Io);
        }
        // Flush the directory inode so the renamed entry survives a power loss
        // (fdatasync on the file alone does not flush the parent directory).
        if durable {
            // Ignore EINTR-retried fsync errors on directory fds (best-effort).
            let _ = fsync_eintr(dirfd);
        }
    }
    Ok(())
}

/// Open .tmp shard, write header+regions+xor8, leave fd open and unsynced.
/// Caller is responsible for fdatasync, close, and rename. On error the fd
/// is closed and the .tmp is unlinked. `flags` is written to the `OFF_FLAGS`
/// byte in the header (use `SHARD_FLAG_PK_UNIQUE` as appropriate).
#[allow(clippy::needless_range_loop)]
fn write_shard_streaming_inner(
    dirfd: c_int,
    basename: &CStr,
    table_id: u32,
    row_count: u32,
    regions: &[(*const u8, usize)],
    flags: u8,
) -> Result<(c_int, std::ffi::CString), StorageError> {
    let num_regions = regions.len();
    let n = row_count as usize;
    let last_region = if num_regions > 0 { num_regions - 1 } else { 0 };

    // --- Phase 1: detect encodings and compute actual sizes ---
    let mut encodings: Vec<RegionEncoding> = Vec::with_capacity(num_regions);
    let mut actual_sizes: Vec<usize> = Vec::with_capacity(num_regions);

    for i in 0..num_regions {
        let (src_ptr, orig_sz) = regions[i];
        if n == 0 || i == last_region || orig_sz == 0 || src_ptr.is_null() {
            encodings.push(RegionEncoding::Raw);
            actual_sizes.push(orig_sz);
        } else {
            let elem_width = orig_sz / n;
            let src = unsafe { std::slice::from_raw_parts(src_ptr, orig_sz) };
            let enc = if i == 1 {
                detect_weight_encoding(src)
            } else {
                detect_encoding(src, elem_width)
            };
            let actual = match &enc {
                RegionEncoding::Raw => orig_sz,
                RegionEncoding::Constant { .. } => elem_width,
                RegionEncoding::TwoValue { bitvec, .. } => 16 + bitvec.len(),
            };
            encodings.push(enc);
            actual_sizes.push(actual);
        }
    }

    // --- Phase 2: XOR8 filter built from pk region (pk_stride bytes/row, zero-extended to u128) ---
    let xor8_filter = if row_count > 0 && num_regions >= 1 {
        let (pk_ptr, pk_sz) = regions[0];
        build_xor8_from_pk_region(pk_ptr, pk_sz, n)
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
    let data_end = if num_regions == 0 {
        HEADER_SIZE
    } else {
        region_offsets[num_regions - 1] + actual_sizes[num_regions - 1]
    };

    let xor8_data = xor8_filter.as_ref().map(xor8::serialize);
    let xor8_offset = if xor8_data.is_some() { align64(data_end) } else { 0 };
    let xor8_size = xor8_data.as_ref().map_or(0, |d| d.len());
    let total_size = if xor8_data.is_some() {
        xor8_offset + xor8_size
    } else {
        data_end
    };

    // --- Phase 4: build header + directory buffer ---
    // For TwoValue regions, precompute the on-disk bytes now so they can be
    // used for both the checksum and the later write (avoids building them
    // twice).  Slot is None for Raw / Constant regions.
    let mut two_value_bufs: Vec<Option<Vec<u8>>> = (0..num_regions).map(|_| None).collect();

    let hdr_dir_size = HEADER_SIZE + dir_size;
    let mut hdr_buf = vec![0u8; hdr_dir_size];

    for i in 0..num_regions {
        let (src_ptr, orig_sz) = regions[i];
        let cs = match &encodings[i] {
            RegionEncoding::Raw => {
                if actual_sizes[i] > 0 && !src_ptr.is_null() {
                    xxh::checksum(unsafe { std::slice::from_raw_parts(src_ptr, orig_sz) })
                } else {
                    0
                }
            }
            RegionEncoding::Constant { .. } => {
                let elem_width = actual_sizes[i];
                xxh::checksum(unsafe { std::slice::from_raw_parts(src_ptr, elem_width) })
            }
            RegionEncoding::TwoValue { value_a, value_b, bitvec } => {
                let mut buf = Vec::with_capacity(16 + bitvec.len());
                buf.extend_from_slice(&value_a.to_le_bytes());
                buf.extend_from_slice(&value_b.to_le_bytes());
                buf.extend_from_slice(bitvec);
                let cs = xxh::checksum(&buf);
                two_value_bufs[i] = Some(buf);
                cs
            }
        };

        let d = dir_offset + i * DIR_ENTRY_SIZE;
        write_u64_le(&mut hdr_buf, d, region_offsets[i] as u64);
        write_u64_le(&mut hdr_buf, d + 8, actual_sizes[i] as u64);
        write_u64_le(&mut hdr_buf, d + 16, cs);
        let encoding_byte = match &encodings[i] {
            RegionEncoding::Raw => ENCODING_RAW,
            RegionEncoding::Constant { .. } => ENCODING_CONSTANT,
            RegionEncoding::TwoValue { .. } => ENCODING_TWO_VALUE,
        };
        hdr_buf[d + 24] = encoding_byte;
    }

    write_u64_le(&mut hdr_buf, OFF_MAGIC, SHARD_MAGIC);
    write_u64_le(&mut hdr_buf, OFF_VERSION, SHARD_VERSION);
    write_u64_le(&mut hdr_buf, OFF_ROW_COUNT, row_count as u64);
    write_u64_le(&mut hdr_buf, OFF_DIR_OFFSET, dir_offset as u64);
    write_u64_le(&mut hdr_buf, OFF_TABLE_ID, table_id as u64);
    write_u64_le(&mut hdr_buf, OFF_XOR8_OFFSET, xor8_offset as u64);
    write_u64_le(&mut hdr_buf, OFF_XOR8_SIZE, xor8_size as u64);
    hdr_buf[OFF_FLAGS] = flags;

    let tmp_name = super::cstr_with_tmp_suffix(basename)?;

    unsafe {
        let fd = libc::openat(
            dirfd,
            tmp_name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
            0o644,
        );
        if fd < 0 {
            return Err(StorageError::Io);
        }

        let abort = |fd: c_int, tmp_ptr: *const libc::c_char| -> StorageError {
            libc::close(fd);
            libc::unlinkat(dirfd, tmp_ptr, 0);
            StorageError::Io
        };

        if libc::ftruncate(fd, total_size as libc::off_t) < 0 {
            return Err(abort(fd, tmp_name.as_ptr()));
        }

        pwrite_all(fd, &hdr_buf, 0).map_err(|_| abort(fd, tmp_name.as_ptr()))?;

        for i in 0..num_regions {
            let (src_ptr, orig_sz) = regions[i];
            let r_off = region_offsets[i] as libc::off_t;

            match &encodings[i] {
                RegionEncoding::Raw => {
                    if actual_sizes[i] > 0 && !src_ptr.is_null() {
                        let src = std::slice::from_raw_parts(src_ptr, orig_sz);
                        pwrite_all(fd, src, r_off).map_err(|_| abort(fd, tmp_name.as_ptr()))?;
                    }
                }
                RegionEncoding::Constant { .. } => {
                    let elem_width = actual_sizes[i];
                    let src = std::slice::from_raw_parts(src_ptr, elem_width);
                    pwrite_all(fd, src, r_off).map_err(|_| abort(fd, tmp_name.as_ptr()))?;
                }
                RegionEncoding::TwoValue { .. } => {
                    let buf = two_value_bufs[i].as_ref().expect("TwoValue buf precomputed");
                    pwrite_all(fd, buf, r_off).map_err(|_| abort(fd, tmp_name.as_ptr()))?;
                }
            }
        }

        if let Some(ref data) = xor8_data {
            pwrite_all(fd, data, xor8_offset as libc::off_t)
                .map_err(|_| abort(fd, tmp_name.as_ptr()))?;
        }

        Ok((fd, tmp_name))
    }
}

/// Write `image` atomically using openat/fdatasync/renameat.
///
/// `dirfd`: directory fd for openat/renameat.  Use `libc::AT_FDCWD` (-100)
/// for absolute paths.
#[cfg(test)]
pub(crate) fn write_shard_at(dirfd: c_int, basename: &CStr, image: &[u8], durable: bool) -> Result<(), StorageError> {
    let tmp_name = super::cstr_with_tmp_suffix(basename)?;

    unsafe {
        let fd = libc::openat(
            dirfd,
            tmp_name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
            0o644,
        );
        if fd < 0 {
            return Err(StorageError::Io);
        }

        let abort = |fd: c_int, tmp_ptr: *const libc::c_char| -> StorageError {
            libc::close(fd);
            libc::unlinkat(dirfd, tmp_ptr, 0);
            StorageError::Io
        };

        if crate::foundation::posix_io::write_all_fd(fd, image) < 0 {
            return Err(abort(fd, tmp_name.as_ptr()));
        }

        if durable && fdatasync_eintr(fd).is_err() {
            return Err(abort(fd, tmp_name.as_ptr()));
        }

        libc::close(fd);

        if libc::renameat(dirfd, tmp_name.as_ptr(), dirfd, basename.as_ptr()) < 0 {
            libc::unlinkat(dirfd, tmp_name.as_ptr(), 0);
            return Err(StorageError::Io);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::codec::read_u64_le;
    use super::super::shard_reader::MappedShard;
    use crate::schema::{SchemaColumn, SchemaDescriptor, MAX_COLUMNS};

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

        let image = build_shard_image(42, row_count, &regions);

        assert_eq!(
            read_u64_le(&image, OFF_MAGIC),
            SHARD_MAGIC
        );
        assert_eq!(
            read_u64_le(&image, OFF_VERSION),
            SHARD_VERSION
        );
        assert_eq!(read_u64_le(&image, OFF_ROW_COUNT), 3);
        assert_eq!(read_u64_le(&image, OFF_TABLE_ID), 42);
        assert!(read_u64_le(&image, OFF_XOR8_OFFSET) > 0);
        assert!(read_u64_le(&image, OFF_XOR8_SIZE) > 0);
    }

    #[test]
    fn write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let shard_path = dir.path().join("test.db");
        let path_cstr =
            std::ffi::CString::new(shard_path.to_str().unwrap()).unwrap();

        let row_count = 2u32;
        let pks: Vec<u64> = vec![1, 2];
        let weights: Vec<i64> = vec![1, 1];
        let nulls: Vec<u64> = vec![0, 0];
        let vals: Vec<i64> = vec![42, 99];
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

        let image = build_shard_image(7, row_count, &regions);
        write_shard_at(libc::AT_FDCWD, &path_cstr, &image, true).unwrap();
        assert!(shard_path.exists());

        let schema = make_schema_desc(2, 0);
        let shard = MappedShard::open(&path_cstr, &schema, true).unwrap();
        assert_eq!(shard.count, 2);
        assert_eq!(shard.get_pk(0), 1);
        assert_eq!(shard.get_pk(1), 2);
        assert_eq!(shard.get_weight(0), 1);
        assert_eq!(shard.get_weight(1), 1);
        assert!(shard.has_xor8());
        assert!(shard.xor8_may_contain(1));
        assert!(shard.xor8_may_contain(2));
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

        let image = build_shard_image(1, 0, &regions);
        write_shard_at(libc::AT_FDCWD, &cpath, &image, false).unwrap();

        let schema = make_schema_desc(1, 0);
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 0);
        assert!(!shard.has_xor8());
    }

    /// Bug 3: write_shard_streaming roundtrip — multiple column types including STRING.
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

        write_shard_streaming(
            libc::AT_FDCWD, &cpath, 42, row_count, &regions, true, 0,
        ).unwrap();

        let schema = make_schema_desc(2, 0);
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

        let image = build_shard_image(99, n, &regions);
        assert_eq!(
            read_u64_le(&image, OFF_VERSION),
            SHARD_VERSION,
            "must write V6"
        );
        write_shard_at(libc::AT_FDCWD, &cpath, &image, false).unwrap();

        let schema = make_schema_desc(2, 0);
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

        let image = build_shard_image(1, n, &regions);

        // PK region should be Constant-encoded → directory entry size == 8 (one elem).
        let dir_off = read_u64_le(&image, OFF_DIR_OFFSET) as usize;
        let pk_region_sz = read_u64_le(&image, dir_off + 8) as usize;
        assert_eq!(pk_region_sz, 8, "Constant PK region must store a single 8B value");

        // Verify encoding byte is ENCODING_CONSTANT.
        assert_eq!(image[dir_off + 24], ENCODING_CONSTANT, "PK region must use Constant encoding");
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
        let nulls: Vec<u64> = vec![0, 0, 0, 0];   // all-zero → Constant encoding
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

        write_shard_streaming(
            libc::AT_FDCWD, &cpath, 7, row_count, &regions, true, 0,
        ).unwrap();

        let schema = make_schema_desc(2, 0);
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 4);
        for i in 0..4 {
            assert_eq!(shard.get_pk(i), (i + 1) as u128);
            assert_eq!(shard.get_weight(i), 1);
        }
    }

    #[test]
    fn build_xor8_wide_compound_region() {
        let stride = 24usize;
        let rows: Vec<Vec<u8>> = (0u8..5)
            .map(|r| (0..stride).map(|b| r.wrapping_add(b as u8)).collect())
            .collect();
        let pk_bytes: Vec<u8> = rows.iter().flatten().copied().collect();
        let f =
            build_xor8_from_pk_region(pk_bytes.as_ptr(), pk_bytes.len(), rows.len())
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
        let f =
            build_xor8_from_pk_region(pk_bytes.as_ptr(), pk_bytes.len(), rows.len())
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
        let f64 = build_xor8_from_pk_region(b64.as_ptr(), b64.len(), pks64.len())
            .expect("8-byte region must build a filter");
        for p in &pks64 {
            assert!(xor8::may_contain(&f64, *p as u128));
        }

        let pks128: Vec<u128> = vec![1, 1 << 64, u128::MAX, 12345];
        // OPK at rest: U128 region is big-endian.
        let b128: Vec<u8> = pks128.iter().flat_map(|p| p.to_be_bytes()).collect();
        let f128 = build_xor8_from_pk_region(b128.as_ptr(), b128.len(), pks128.len())
            .expect("16-byte region must build a filter");
        for p in &pks128 {
            assert!(xor8::may_contain(&f128, *p));
        }
    }

    const FLAG: u8 = SHARD_FLAG_PK_UNIQUE;

    /// `flags_if(true)` after observing the given OPK byte rows in order.
    fn tag(rows: &[(&[u8], i64)]) -> u8 {
        let mut c = PkUniqueChecker::new();
        for &(pk, w) in rows {
            c.observe(pk, w);
        }
        c.flags_if(true)
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
        assert_eq!(tag(&[(&a, 1), (&b, 1)]), FLAG, "wide prefix-twins, distinct tails qualify");
        assert_eq!(tag(&[(&a, 1), (&a, 1)]), 0, "identical wide keys disqualify");
        // Interleaving a distinct-prefix row must not lose the wide last_pk.
        let c = [9u8; 24];
        assert_eq!(tag(&[(&a, 1), (&c, 1), (&a, 1)]), FLAG, "non-adjacent wide repeats qualify");
    }

    #[test]
    fn pk_unique_checker_weight_and_disabled() {
        let k1 = 1u64.to_be_bytes();
        let k2 = 2u64.to_be_bytes();
        // A negative weight disqualifies regardless of PK distinctness.
        assert_eq!(tag(&[(&k1, 1), (&k2, -1)]), 0, "negative weight disqualifies");
        // flags_if(false) never tags, even for a clean unique stream.
        let mut c = PkUniqueChecker::new();
        c.observe(&k1, 1);
        assert_eq!(c.flags_if(false), 0, "disabled tagging never sets the flag");
    }
}
