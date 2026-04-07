//! Shard file image building and atomic writing.
//!
//! Shared by the compaction path (`compact.rs`) and the memtable flush path
//! (`memtable.rs`).

use std::ffi::CStr;
use std::ptr;

use libc::c_int;

use crate::layout::*;
use crate::util::write_u64_le;
use super::xor8;
use crate::xxh;

fn align64(val: usize) -> usize {
    (val + ALIGNMENT - 1) & !(ALIGNMENT - 1)
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
    debug_assert!(!data.is_empty() && data.len() % element_width == 0);
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
    debug_assert!(!data.is_empty() && data.len() % 8 == 0);
    let n = data.len() / 8;
    let first = i64::from_le_bytes(data[..8].try_into().unwrap());
    let mut second: Option<i64> = None;

    // We'll build the bitvec speculatively; discard if 3+ distinct.
    let bitvec_len = (n + 7) / 8;
    let mut bitvec = vec![0u8; bitvec_len];

    for i in 1..n {
        let v = i64::from_le_bytes(data[i * 8..(i + 1) * 8].try_into().unwrap());
        if v == first {
            // bit stays 0
        } else if let Some(b) = second {
            if v == b {
                bitvec[i / 8] |= 1 << (i % 8);
            } else {
                return RegionEncoding::Raw; // 3+ distinct values
            }
        } else {
            second = Some(v);
            bitvec[i / 8] |= 1 << (i % 8);
        }
    }

    match second {
        None => {
            let mut value = [0u8; 16];
            value[..8].copy_from_slice(&first.to_le_bytes());
            RegionEncoding::Constant { value }
        }
        Some(b) => RegionEncoding::TwoValue {
            value_a: first,
            value_b: b,
            bitvec,
        },
    }
}

/// Build a complete shard file image from pre-built region buffers.
///
/// `regions` is an array of (pointer, size) pairs in the canonical order:
///   pk_lo, pk_hi, weight, null_bitmap, [non-PK columns...], blob_arena.
///
/// The XOR8 filter is built internally from the first two regions (pk_lo,
/// pk_hi) when `row_count > 0`.
///
/// Returns the complete file image ready for writing.
pub fn build_shard_image(
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

    for i in 0..num_regions {
        let (src_ptr, orig_sz) = regions[i];
        if n == 0 || i == last_region || orig_sz == 0 || src_ptr.is_null() {
            // Empty region, blob arena (last), or zero rows -> Raw passthrough
            encodings.push(RegionEncoding::Raw);
            actual_sizes.push(orig_sz);
        } else {
            let elem_width = orig_sz / n;
            let src = unsafe { std::slice::from_raw_parts(src_ptr, orig_sz) };
            let enc = if i == 2 {
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

    // --- XOR8 filter (from raw caller pointers, unchanged) ---

    let xor8_filter = if row_count > 0 && num_regions >= 2 {
        let (pk_lo_ptr, pk_lo_sz) = regions[0];
        let (pk_hi_ptr, pk_hi_sz) = regions[1];
        if !pk_lo_ptr.is_null()
            && !pk_hi_ptr.is_null()
            && pk_lo_sz >= n * 8
            && pk_hi_sz >= n * 8
        {
            let lo = unsafe { std::slice::from_raw_parts(pk_lo_ptr as *const u64, n) };
            let hi = unsafe { std::slice::from_raw_parts(pk_hi_ptr as *const u64, n) };
            xor8::build(lo, hi)
        } else {
            None
        }
    } else {
        None
    };

    // --- Phase 2: compute offsets, allocate, and write ---

    let dir_size = num_regions * DIR_ENTRY_SIZE;
    let dir_offset = HEADER_SIZE;

    let mut pos = align64(dir_offset + dir_size);
    let mut region_offsets = Vec::with_capacity(num_regions);
    for i in 0..num_regions {
        region_offsets.push(pos);
        pos = align64(pos + actual_sizes[i]);
    }
    let data_end = if num_regions == 0 {
        HEADER_SIZE
    } else {
        region_offsets[num_regions - 1] + actual_sizes[num_regions - 1]
    };

    let xor8_data = xor8_filter.as_ref().map(|f| xor8::serialize(f));
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

/// pwrite loop that handles short writes.
unsafe fn pwrite_all(fd: c_int, buf: &[u8], mut offset: libc::off_t) -> i32 {
    let mut remaining = buf.len();
    let mut p = buf.as_ptr();
    while remaining > 0 {
        let written = libc::pwrite(fd, p as *const libc::c_void, remaining, offset);
        if written <= 0 {
            return -3;
        }
        remaining -= written as usize;
        p = p.add(written as usize);
        offset += written as libc::off_t;
    }
    0
}

/// Write a shard directly to disk without building an intermediate Vec<u8>.
///
/// Same output format as `build_shard_image` + `write_shard_at`, but peak memory
/// is only the header+directory buffer instead of the entire shard image.
pub fn write_shard_streaming(
    dirfd: c_int,
    basename: &CStr,
    table_id: u32,
    row_count: u32,
    regions: &[(*const u8, usize)],
    durable: bool,
) -> i32 {
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
            let enc = if i == 2 {
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

    // --- Phase 2: XOR8 filter ---
    let xor8_filter = if row_count > 0 && num_regions >= 2 {
        let (pk_lo_ptr, pk_lo_sz) = regions[0];
        let (pk_hi_ptr, pk_hi_sz) = regions[1];
        if !pk_lo_ptr.is_null()
            && !pk_hi_ptr.is_null()
            && pk_lo_sz >= n * 8
            && pk_hi_sz >= n * 8
        {
            let lo = unsafe { std::slice::from_raw_parts(pk_lo_ptr as *const u64, n) };
            let hi = unsafe { std::slice::from_raw_parts(pk_hi_ptr as *const u64, n) };
            xor8::build(lo, hi)
        } else {
            None
        }
    } else {
        None
    };

    // --- Phase 3: compute offsets ---
    let dir_size = num_regions * DIR_ENTRY_SIZE;
    let dir_offset = HEADER_SIZE;
    let mut pos = align64(dir_offset + dir_size);
    let mut region_offsets = Vec::with_capacity(num_regions);
    for i in 0..num_regions {
        region_offsets.push(pos);
        pos = align64(pos + actual_sizes[i]);
    }
    let data_end = if num_regions == 0 {
        HEADER_SIZE
    } else {
        region_offsets[num_regions - 1] + actual_sizes[num_regions - 1]
    };

    let xor8_data = xor8_filter.as_ref().map(|f| xor8::serialize(f));
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
                let mut tmp = Vec::with_capacity(16 + bitvec.len());
                tmp.extend_from_slice(&value_a.to_le_bytes());
                tmp.extend_from_slice(&value_b.to_le_bytes());
                tmp.extend_from_slice(bitvec);
                xxh::checksum(&tmp)
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

    // XOR8 filter data
    if let Some(ref data) = xor8_data {
        // Nothing to do here; written to file in phase 8
        let _ = data;
    }

    // --- Phase 5-9: open file, pwrite regions, sync, rename ---
    let base_bytes = basename.to_bytes();
    let mut tmp_name_buf = Vec::with_capacity(base_bytes.len() + 5);
    tmp_name_buf.extend_from_slice(base_bytes);
    tmp_name_buf.extend_from_slice(b".tmp\0");
    let tmp_name = tmp_name_buf.as_ptr() as *const libc::c_char;

    unsafe {
        let fd = libc::openat(
            dirfd,
            tmp_name,
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
            0o644,
        );
        if fd < 0 {
            return -3;
        }

        if libc::ftruncate(fd, total_size as libc::off_t) < 0 {
            libc::close(fd);
            libc::unlinkat(dirfd, tmp_name, 0);
            return -3;
        }

        // Phase 6: write header + directory
        if pwrite_all(fd, &hdr_buf, 0) < 0 {
            libc::close(fd);
            libc::unlinkat(dirfd, tmp_name, 0);
            return -3;
        }

        // Phase 7: write each region
        for i in 0..num_regions {
            let (src_ptr, orig_sz) = regions[i];
            let r_off = region_offsets[i] as libc::off_t;

            match &encodings[i] {
                RegionEncoding::Raw => {
                    if actual_sizes[i] > 0 && !src_ptr.is_null() {
                        let src = std::slice::from_raw_parts(src_ptr, orig_sz);
                        if pwrite_all(fd, src, r_off) < 0 {
                            libc::close(fd);
                            libc::unlinkat(dirfd, tmp_name, 0);
                            return -3;
                        }
                    }
                }
                RegionEncoding::Constant { .. } => {
                    let elem_width = actual_sizes[i];
                    let src = std::slice::from_raw_parts(src_ptr, elem_width);
                    if pwrite_all(fd, src, r_off) < 0 {
                        libc::close(fd);
                        libc::unlinkat(dirfd, tmp_name, 0);
                        return -3;
                    }
                }
                RegionEncoding::TwoValue { value_a, value_b, bitvec } => {
                    let mut tmp = Vec::with_capacity(16 + bitvec.len());
                    tmp.extend_from_slice(&value_a.to_le_bytes());
                    tmp.extend_from_slice(&value_b.to_le_bytes());
                    tmp.extend_from_slice(bitvec);
                    if pwrite_all(fd, &tmp, r_off) < 0 {
                        libc::close(fd);
                        libc::unlinkat(dirfd, tmp_name, 0);
                        return -3;
                    }
                }
            }
        }

        // Phase 8: write XOR8 filter
        if let Some(ref data) = xor8_data {
            if pwrite_all(fd, data, xor8_offset as libc::off_t) < 0 {
                libc::close(fd);
                libc::unlinkat(dirfd, tmp_name, 0);
                return -3;
            }
        }

        // Phase 9: sync and rename
        if durable {
            if libc::fdatasync(fd) < 0 {
                libc::close(fd);
                libc::unlinkat(dirfd, tmp_name, 0);
                return -3;
            }
        }

        libc::close(fd);

        if libc::renameat(dirfd, tmp_name, dirfd, basename.as_ptr()) < 0 {
            libc::unlinkat(dirfd, tmp_name, 0);
            return -3;
        }
    }

    0
}

/// Write `image` atomically using openat/fdatasync/renameat.
///
/// `dirfd`: directory fd for openat/renameat.  Use `libc::AT_FDCWD` (-100)
/// for absolute paths.
///
/// Returns 0 on success, -3 on I/O error.
pub fn write_shard_at(dirfd: c_int, basename: &CStr, image: &[u8], durable: bool) -> i32 {
    let base_bytes = basename.to_bytes();
    let mut tmp_buf = Vec::with_capacity(base_bytes.len() + 5);
    tmp_buf.extend_from_slice(base_bytes);
    tmp_buf.extend_from_slice(b".tmp\0");
    let tmp_name = tmp_buf.as_ptr() as *const libc::c_char;

    unsafe {
        let fd = libc::openat(
            dirfd,
            tmp_name,
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
            0o644,
        );
        if fd < 0 {
            return -3;
        }

        let rc = crate::util::write_all_fd(fd, image);
        if rc < 0 {
            libc::close(fd);
            libc::unlinkat(dirfd, tmp_name, 0);
            return -3;
        }

        if durable {
            if libc::fdatasync(fd) < 0 {
                libc::close(fd);
                libc::unlinkat(dirfd, tmp_name, 0);
                return -3;
            }
        }

        libc::close(fd);

        if libc::renameat(dirfd, tmp_name, dirfd, basename.as_ptr()) < 0 {
            libc::unlinkat(dirfd, tmp_name, 0);
            return -3;
        }
    }

    0
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::shard_reader::MappedShard;
    use crate::schema::{SchemaColumn, SchemaDescriptor};

    fn make_schema_desc(num_cols: u32, pk_index: u32) -> SchemaDescriptor {
        let mut sd = SchemaDescriptor {
            num_columns: num_cols,
            pk_index,
            columns: [SchemaColumn {
                type_code: 0,
                size: 0,
                nullable: 0,
                _pad: 0,
            }; 64],
        };
        sd.columns[0] = SchemaColumn {
            type_code: 8,
            size: 8,
            nullable: 0,
            _pad: 0,
        };
        if num_cols > 1 {
            sd.columns[1] = SchemaColumn {
                type_code: 9,
                size: 8,
                nullable: 0,
                _pad: 0,
            };
        }
        sd
    }

    #[test]
    fn build_image_roundtrip() {
        let row_count = 3u32;
        let pk_lo: Vec<u64> = vec![10, 20, 30];
        let pk_hi: Vec<u64> = vec![0, 0, 0];
        let weights: Vec<i64> = vec![1, 1, 1];
        let nulls: Vec<u64> = vec![0, 0, 0];
        let vals: Vec<i64> = vec![100, 200, 300];

        let pk_lo_bytes = unsafe {
            std::slice::from_raw_parts(pk_lo.as_ptr() as *const u8, pk_lo.len() * 8)
        };
        let pk_hi_bytes = unsafe {
            std::slice::from_raw_parts(pk_hi.as_ptr() as *const u8, pk_hi.len() * 8)
        };
        let weight_bytes = unsafe {
            std::slice::from_raw_parts(weights.as_ptr() as *const u8, weights.len() * 8)
        };
        let null_bytes = unsafe {
            std::slice::from_raw_parts(nulls.as_ptr() as *const u8, nulls.len() * 8)
        };
        let val_bytes = unsafe {
            std::slice::from_raw_parts(vals.as_ptr() as *const u8, vals.len() * 8)
        };
        let blob: Vec<u8> = vec![];

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_lo_bytes.as_ptr(), pk_lo_bytes.len()),
            (pk_hi_bytes.as_ptr(), pk_hi_bytes.len()),
            (weight_bytes.as_ptr(), weight_bytes.len()),
            (null_bytes.as_ptr(), null_bytes.len()),
            (val_bytes.as_ptr(), val_bytes.len()),
            (blob.as_ptr(), blob.len()),
        ];

        let image = build_shard_image(42, row_count, &regions);

        assert_eq!(
            crate::util::read_u64_le(&image, OFF_MAGIC),
            SHARD_MAGIC
        );
        assert_eq!(
            crate::util::read_u64_le(&image, OFF_VERSION),
            SHARD_VERSION
        );
        assert_eq!(crate::util::read_u64_le(&image, OFF_ROW_COUNT), 3);
        assert_eq!(crate::util::read_u64_le(&image, OFF_TABLE_ID), 42);
        assert!(crate::util::read_u64_le(&image, OFF_XOR8_OFFSET) > 0);
        assert!(crate::util::read_u64_le(&image, OFF_XOR8_SIZE) > 0);
    }

    #[test]
    fn write_and_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let shard_path = dir.path().join("test.db");
        let path_cstr =
            std::ffi::CString::new(shard_path.to_str().unwrap()).unwrap();

        let row_count = 2u32;
        let pk_lo: Vec<u64> = vec![1, 2];
        let pk_hi: Vec<u64> = vec![0, 0];
        let weights: Vec<i64> = vec![1, 1];
        let nulls: Vec<u64> = vec![0, 0];
        let vals: Vec<i64> = vec![42, 99];
        let blob: Vec<u8> = vec![];

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_lo.as_ptr() as *const u8, pk_lo.len() * 8),
            (pk_hi.as_ptr() as *const u8, pk_hi.len() * 8),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (nulls.as_ptr() as *const u8, nulls.len() * 8),
            (vals.as_ptr() as *const u8, vals.len() * 8),
            (blob.as_ptr(), 0),
        ];

        let image = build_shard_image(7, row_count, &regions);
        let rc = write_shard_at(libc::AT_FDCWD, &path_cstr, &image, true);
        assert_eq!(rc, 0);
        assert!(shard_path.exists());

        let schema = make_schema_desc(2, 0);
        let shard = MappedShard::open(&path_cstr, &schema, true).unwrap();
        assert_eq!(shard.count, 2);
        assert_eq!(shard.get_pk_lo(0), 1);
        assert_eq!(shard.get_pk_lo(1), 2);
        assert_eq!(shard.get_weight(0), 1);
        assert_eq!(shard.get_weight(1), 1);
        assert!(shard.has_xor8());
        assert!(shard.xor8_may_contain(1, 0));
        assert!(shard.xor8_may_contain(2, 0));
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
            (std::ptr::null(), 0),
        ];

        let image = build_shard_image(1, 0, &regions);
        let rc = write_shard_at(libc::AT_FDCWD, &cpath, &image, false);
        assert_eq!(rc, 0);

        let schema = make_schema_desc(1, 0);
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 0);
        assert!(!shard.has_xor8());
    }

    /// Bug 3: write_shard_streaming roundtrip — multiple column types including STRING.
    #[test]
    fn test_write_shard_streaming_roundtrip() {
        use crate::schema::type_code;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("streaming.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let row_count = 3u32;
        let pk_lo: Vec<u64> = vec![10, 20, 30];
        let pk_hi: Vec<u64> = vec![0, 0, 0];
        let weights: Vec<i64> = vec![1, 1, 1];
        let nulls: Vec<u64> = vec![0, 0, 0];
        let vals: Vec<i64> = vec![100, 200, 300];
        let blob: Vec<u8> = vec![];

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_lo.as_ptr() as *const u8, pk_lo.len() * 8),
            (pk_hi.as_ptr() as *const u8, pk_hi.len() * 8),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (nulls.as_ptr() as *const u8, nulls.len() * 8),
            (vals.as_ptr() as *const u8, vals.len() * 8),
            (blob.as_ptr(), 0),
        ];

        let rc = write_shard_streaming(
            libc::AT_FDCWD, &cpath, 42, row_count, &regions, true,
        );
        assert_eq!(rc, 0);

        let schema = make_schema_desc(2, 0);
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 3);
        assert_eq!(shard.get_pk_lo(0), 10);
        assert_eq!(shard.get_pk_lo(1), 20);
        assert_eq!(shard.get_pk_lo(2), 30);
        assert_eq!(shard.get_weight(0), 1);
        assert_eq!(shard.get_weight(1), 1);
        assert_eq!(shard.get_weight(2), 1);
        assert!(shard.has_xor8());
        assert!(shard.xor8_may_contain(10, 0));
        assert!(shard.xor8_may_contain(20, 0));
        assert!(shard.xor8_may_contain(30, 0));
    }

    /// Bug 3: streaming write with regions that trigger Constant encoding.
    #[test]
    fn test_write_shard_streaming_encodings() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("streaming_enc.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let row_count = 4u32;
        let pk_lo: Vec<u64> = vec![1, 2, 3, 4];
        let pk_hi: Vec<u64> = vec![0, 0, 0, 0]; // all-zero → Constant encoding
        let weights: Vec<i64> = vec![1, 1, 1, 1]; // all-same → Constant encoding
        let nulls: Vec<u64> = vec![0, 0, 0, 0];   // all-zero → Constant encoding
        let vals: Vec<i64> = vec![42, 42, 42, 42]; // all-same → Constant encoding
        let blob: Vec<u8> = vec![];

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_lo.as_ptr() as *const u8, pk_lo.len() * 8),
            (pk_hi.as_ptr() as *const u8, pk_hi.len() * 8),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (nulls.as_ptr() as *const u8, nulls.len() * 8),
            (vals.as_ptr() as *const u8, vals.len() * 8),
            (blob.as_ptr(), 0),
        ];

        let rc = write_shard_streaming(
            libc::AT_FDCWD, &cpath, 7, row_count, &regions, true,
        );
        assert_eq!(rc, 0);

        let schema = make_schema_desc(2, 0);
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 4);
        for i in 0..4 {
            assert_eq!(shard.get_pk_lo(i), (i + 1) as u64);
            assert_eq!(shard.get_weight(i), 1);
        }
    }
}
