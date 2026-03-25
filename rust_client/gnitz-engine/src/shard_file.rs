//! Shared shard file serialization and atomic write.
//!
//! Both the compaction path (`compact.rs`) and the RPython flush path
//! (via FFI) call `write_shard` to produce identical shard files.

use std::ffi::CStr;

use crate::layout::*;
use crate::util::write_u64_le;
use crate::{xor8, xxh};

// Error codes returned to callers (including FFI).
pub const ERR_BAD_ARGS: i32 = -1;
pub const ERR_OPENAT: i32 = -2;
pub const ERR_WRITE: i32 = -3;
pub const ERR_FDATASYNC: i32 = -4;
pub const ERR_CLOSE: i32 = -5;
pub const ERR_RENAMEAT: i32 = -6;
pub const ERR_XOR8: i32 = -7;

#[inline]
pub fn align64(val: usize) -> usize {
    (val + ALIGNMENT - 1) & !(ALIGNMENT - 1)
}

/// Write a complete shard file atomically via openat/fdatasync/renameat.
///
/// - `dirfd`:      directory fd, or `libc::AT_FDCWD` (-100) for absolute/CWD paths
/// - `filename`:   basename (relative to dirfd) or absolute path
/// - `table_id`:   table identifier embedded in the shard header
/// - `row_count`:  number of rows (0 is valid — produces an empty shard)
/// - `regions`:    ordered slice of column data regions
/// - `pk_lo`, `pk_hi`: PK arrays for xor8 construction (each `row_count` elements)
/// - `durable`:    if false, skip fdatasync
pub fn write_shard(
    dirfd: i32,
    filename: &CStr,
    table_id: u32,
    row_count: usize,
    regions: &[&[u8]],
    pk_lo: &[u64],
    pk_hi: &[u64],
    durable: bool,
) -> Result<(), i32> {
    let xor8_data = if row_count > 0 {
        let filter = xor8::build(pk_lo, pk_hi).ok_or(ERR_XOR8)?;
        Some(xor8::serialize(&filter))
    } else {
        None
    };

    let num_regions = regions.len();
    let dir_offset = HEADER_SIZE;
    let dir_size = num_regions * DIR_ENTRY_SIZE;

    let mut pos = align64(dir_offset + dir_size);
    let mut region_offsets = Vec::with_capacity(num_regions);
    for r in regions {
        region_offsets.push(pos);
        pos = align64(pos + r.len());
    }
    let data_end = if num_regions == 0 {
        HEADER_SIZE
    } else {
        region_offsets[num_regions - 1] + regions[num_regions - 1].len()
    };

    let xor8_offset = xor8_data.as_ref().map_or(0, |_| align64(data_end));
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
        let r_sz = regions[i].len();
        image[r_off..r_off + r_sz].copy_from_slice(regions[i]);
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

    atomic_write(dirfd, filename, &image, durable)
}

fn atomic_write(
    dirfd: i32,
    filename: &CStr,
    data: &[u8],
    durable: bool,
) -> Result<(), i32> {
    let name_bytes = filename.to_bytes();
    let mut tmp_path: Vec<u8> = Vec::with_capacity(name_bytes.len() + 5);
    tmp_path.extend_from_slice(name_bytes);
    tmp_path.extend_from_slice(b".tmp\0");

    let fd = unsafe {
        libc::openat(
            dirfd,
            tmp_path.as_ptr() as *const libc::c_char,
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
            0o644,
        )
    };
    if fd < 0 {
        return Err(ERR_OPENAT);
    }

    let mut written: usize = 0;
    while written < data.len() {
        let n = unsafe {
            libc::write(
                fd,
                data[written..].as_ptr() as *const libc::c_void,
                data.len() - written,
            )
        };
        if n < 0 {
            let e = unsafe { *libc::__errno_location() };
            if e == libc::EINTR { continue; }
            unsafe { libc::close(fd); }
            return Err(ERR_WRITE);
        }
        if n == 0 {
            unsafe { libc::close(fd); }
            return Err(ERR_WRITE);
        }
        written += n as usize;
    }

    if durable {
        if unsafe { libc::fdatasync(fd) } < 0 {
            unsafe { libc::close(fd); }
            return Err(ERR_FDATASYNC);
        }
    }

    if unsafe { libc::close(fd) } < 0 {
        return Err(ERR_CLOSE);
    }

    let rc = unsafe {
        libc::renameat(
            dirfd,
            tmp_path.as_ptr() as *const libc::c_char,
            dirfd,
            filename.as_ptr(),
        )
    };
    if rc < 0 {
        return Err(ERR_RENAMEAT);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shard_reader::MappedShard;
    use crate::compact::SchemaDescriptor;

    fn make_schema_1col() -> SchemaDescriptor {
        let mut sd = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [crate::compact::SchemaColumn {
                type_code: 8, // U64
                size: 8,
                nullable: 0,
                _pad: 0,
            }; 64],
        };
        sd.columns[1] = crate::compact::SchemaColumn {
            type_code: 9, // I64
            size: 8,
            nullable: 0,
            _pad: 0,
        };
        sd
    }

    #[test]
    fn write_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let pk_lo: Vec<u64> = vec![10, 20, 30];
        let pk_hi: Vec<u64> = vec![0, 0, 0];
        let weights: Vec<i64> = vec![1, 1, 1];
        let nulls: Vec<u64> = vec![0, 0, 0];
        let col1: Vec<i64> = vec![100, 200, 300];

        let regions: Vec<&[u8]> = vec![
            as_bytes(&pk_lo), as_bytes(&pk_hi),
            as_bytes(&weights), as_bytes(&nulls),
            as_bytes(&col1), &[],
        ];

        let result = write_shard(
            libc::AT_FDCWD, &cpath, 42, 3,
            &regions, &pk_lo, &pk_hi, false,
        );
        assert!(result.is_ok(), "write_shard failed: {:?}", result);

        // Read back via MappedShard
        let schema = make_schema_1col();
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 3);
        assert_eq!(shard.get_pk_lo(0), 10);
        assert_eq!(shard.get_pk_lo(1), 20);
        assert_eq!(shard.get_pk_lo(2), 30);
        assert_eq!(shard.get_weight(0), 1);
        assert!(shard.has_xor8());
        assert!(shard.xor8_may_contain(10, 0));
        assert!(shard.xor8_may_contain(20, 0));
        assert!(shard.xor8_may_contain(30, 0));
    }

    #[test]
    fn write_empty_shard() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let regions: Vec<&[u8]> = vec![&[], &[], &[], &[], &[], &[]];
        let result = write_shard(
            libc::AT_FDCWD, &cpath, 1, 0,
            &regions, &[], &[], false,
        );
        assert!(result.is_ok());

        let schema = make_schema_1col();
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 0);
        assert!(!shard.has_xor8());
    }

    #[test]
    fn write_with_dirfd() {
        let dir = tempfile::tempdir().unwrap();
        let dirfd = unsafe {
            let dp = std::ffi::CString::new(dir.path().to_str().unwrap()).unwrap();
            libc::open(dp.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY)
        };
        assert!(dirfd >= 0);

        let basename = std::ffi::CString::new("dirfd_test.db").unwrap();
        let count = 2usize;
        let pk_lo: Vec<u64> = vec![1, 2];
        let pk_hi: Vec<u64> = vec![0, 0];
        let weights: Vec<i64> = vec![1, 1];
        let nulls: Vec<u64> = vec![0, 0];
        let col1: Vec<i64> = vec![10, 20];

        let regions: Vec<&[u8]> = vec![
            as_bytes(&pk_lo), as_bytes(&pk_hi),
            as_bytes(&weights), as_bytes(&nulls),
            as_bytes(&col1), &[],
        ];

        let result = write_shard(
            dirfd, &basename, 5, count,
            &regions, &pk_lo, &pk_hi, false,
        );
        assert!(result.is_ok());
        unsafe { libc::close(dirfd); }

        // Verify via absolute path
        let full_path = dir.path().join("dirfd_test.db");
        let cpath = std::ffi::CString::new(full_path.to_str().unwrap()).unwrap();
        let schema = make_schema_1col();
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 2);
        assert_eq!(shard.get_pk_lo(0), 1);
        assert_eq!(shard.get_pk_lo(1), 2);
    }

    #[test]
    fn non_durable_skips_fsync() {
        // Just verify it doesn't error — we can't easily verify fdatasync was skipped.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nondurable.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let pk_lo: Vec<u64> = vec![1];
        let pk_hi: Vec<u64> = vec![0];
        let w: Vec<i64> = vec![1];
        let n: Vec<u64> = vec![0];
        let c: Vec<i64> = vec![99];

        let regions: Vec<&[u8]> = vec![
            as_bytes(&pk_lo), as_bytes(&pk_hi),
            as_bytes(&w), as_bytes(&n),
            as_bytes(&c), &[],
        ];

        assert!(write_shard(
            libc::AT_FDCWD, &cpath, 1, 1,
            &regions, &pk_lo, &pk_hi, false,
        ).is_ok());

        assert!(write_shard(
            libc::AT_FDCWD, &cpath, 1, 1,
            &regions, &pk_lo, &pk_hi, true,
        ).is_ok());
    }

    #[test]
    fn openat_bad_dirfd_returns_error() {
        let cpath = std::ffi::CString::new("nope.db").unwrap();
        let result = write_shard(
            9999, &cpath, 1, 0,
            &[&[], &[], &[], &[], &[], &[]], &[], &[], false,
        );
        assert_eq!(result, Err(ERR_OPENAT));
    }

    fn as_bytes<T>(v: &[T]) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(v.as_ptr() as *const u8, v.len() * std::mem::size_of::<T>())
        }
    }
}
