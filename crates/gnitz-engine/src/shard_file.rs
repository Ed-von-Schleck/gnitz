//! Shard file image building and atomic writing.
//!
//! Shared by both the compaction path (`compact.rs`) and the RPython flush
//! path (via `gnitz_write_shard` FFI).

use std::ffi::CStr;
use std::ptr;

use libc::c_int;

use crate::layout::*;
use crate::util::write_u64_le;
use crate::xor8;
use crate::xxh;

fn align64(val: usize) -> usize {
    (val + ALIGNMENT - 1) & !(ALIGNMENT - 1)
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
    let dir_size = num_regions * DIR_ENTRY_SIZE;
    let dir_offset = HEADER_SIZE;

    let mut pos = align64(dir_offset + dir_size);
    let mut region_offsets = Vec::with_capacity(num_regions);
    for &(_ptr, sz) in regions {
        region_offsets.push(pos);
        pos = align64(pos + sz);
    }
    let data_end = if num_regions == 0 {
        HEADER_SIZE
    } else {
        region_offsets[num_regions - 1] + regions[num_regions - 1].1
    };

    let xor8_filter = if row_count > 0 && num_regions >= 2 {
        let (pk_lo_ptr, pk_lo_sz) = regions[0];
        let (pk_hi_ptr, pk_hi_sz) = regions[1];
        let n = row_count as usize;
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

    write_u64_le(&mut image, OFF_MAGIC, SHARD_MAGIC);
    write_u64_le(&mut image, OFF_VERSION, SHARD_VERSION);
    write_u64_le(&mut image, OFF_ROW_COUNT, row_count as u64);
    write_u64_le(&mut image, OFF_DIR_OFFSET, dir_offset as u64);
    write_u64_le(&mut image, OFF_TABLE_ID, table_id as u64);
    write_u64_le(&mut image, OFF_XOR8_OFFSET, xor8_offset as u64);
    write_u64_le(&mut image, OFF_XOR8_SIZE, xor8_size as u64);

    for i in 0..num_regions {
        let r_off = region_offsets[i];
        let (src_ptr, r_sz) = regions[i];

        if r_sz > 0 && !src_ptr.is_null() {
            unsafe {
                ptr::copy_nonoverlapping(src_ptr, image[r_off..].as_mut_ptr(), r_sz);
            }
        }

        let cs = if r_sz > 0 {
            xxh::checksum(&image[r_off..r_off + r_sz])
        } else {
            0
        };

        let d = dir_offset + i * DIR_ENTRY_SIZE;
        write_u64_le(&mut image, d, r_off as u64);
        write_u64_le(&mut image, d + 8, r_sz as u64);
        write_u64_le(&mut image, d + 16, cs);
    }

    if let Some(ref data) = xor8_data {
        image[xor8_offset..xor8_offset + data.len()].copy_from_slice(data);
    }

    image
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
    use crate::shard_reader::MappedShard;
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
}
