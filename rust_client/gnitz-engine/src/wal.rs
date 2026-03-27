use std::ffi::CStr;
use std::ptr;

use libc::c_int;

use crate::util::{read_u32_le, read_u64_le, write_u32_le, write_u64_le};
use crate::xxh;

//  WAL block header layout (48 bytes):
//  [0,8)   LSN        u64
//  [8,12)  TID        u32
//  [12,16) COUNT      u32
//  [16,20) SIZE       u32
//  [20,24) VERSION    u32
//  [24,32) CHECKSUM   u64
//  [32,36) NUM_REGIONS u32
//  [36,40) RESERVED   u32
//  [40,48) BLOB_SIZE  u64

pub const HEADER_SIZE: usize = 48;
pub const FORMAT_VERSION: u32 = 2;

const OFF_LSN: usize = 0;
const OFF_TID: usize = 8;
const OFF_COUNT: usize = 12;
const OFF_SIZE: usize = 16;
const OFF_VERSION: usize = 20;
const OFF_CHECKSUM: usize = 24;
const OFF_NUM_REGIONS: usize = 32;
const OFF_BLOB_SIZE: usize = 40;

fn align8(val: usize) -> usize {
    (val + 7) & !7
}

/// Compute the total byte size of a WAL block with the given regions.
fn block_size(num_regions: usize, region_sizes: &[u32]) -> usize {
    let mut pos = HEADER_SIZE + num_regions * 8;
    for i in 0..num_regions {
        pos = align8(pos);
        pos += region_sizes[i] as usize;
    }
    pos
}

/// Encode a WAL block from region data into `out_buf` starting at `out_offset`.
///
/// Returns the new offset (= out_offset + total_block_size), or -1 on error
/// (buffer too small).
///
/// The block layout is:
///   [48B header][directory: num_regions * 8B][data regions, 8B-aligned]
///
/// Directory entries store offsets relative to block start (not buffer start).
pub fn encode(
    out_buf: &mut [u8],
    out_offset: usize,
    lsn: u64,
    table_id: u32,
    entry_count: u32,
    region_ptrs: &[*const u8],
    region_sizes: &[u32],
    blob_size: u64,
) -> i64 {
    let num_regions = region_ptrs.len().min(region_sizes.len());
    let dir_size = num_regions * 8;
    let total_size = block_size(num_regions, region_sizes);

    if out_offset + total_size > out_buf.len() {
        return -1;
    }

    let block = &mut out_buf[out_offset..out_offset + total_size];
    block[..HEADER_SIZE].fill(0);

    let mut pos = HEADER_SIZE + dir_size;
    for i in 0..num_regions {
        pos = align8(pos);
        let sz = region_sizes[i] as usize;
        if sz > 0 && !region_ptrs[i].is_null() {
            unsafe {
                ptr::copy_nonoverlapping(region_ptrs[i], block[pos..].as_mut_ptr(), sz);
            }
        }
        let dir_off = HEADER_SIZE + i * 8;
        write_u32_le(block, dir_off, pos as u32);
        write_u32_le(block, dir_off + 4, sz as u32);
        pos += sz;
    }

    write_u64_le(block, OFF_LSN, lsn);
    write_u32_le(block, OFF_TID, table_id);
    write_u32_le(block, OFF_COUNT, entry_count);
    write_u32_le(block, OFF_SIZE, total_size as u32);
    write_u32_le(block, OFF_VERSION, FORMAT_VERSION);
    write_u32_le(block, OFF_NUM_REGIONS, num_regions as u32);
    write_u64_le(block, OFF_BLOB_SIZE, blob_size);

    if total_size > HEADER_SIZE {
        let cs = xxh::checksum(&block[HEADER_SIZE..total_size]);
        write_u64_le(block, OFF_CHECKSUM, cs);
    }

    (out_offset + total_size) as i64
}

/// Error codes for validate_and_parse.
pub const WAL_OK: i32 = 0;
pub const WAL_ERR_VERSION: i32 = -1;
pub const WAL_ERR_CHECKSUM: i32 = -2;
pub const WAL_ERR_TRUNCATED: i32 = -3;

/// Validate a WAL block and extract header fields + directory entries.
///
/// On success (returns 0): header fields and directory arrays are populated.
/// On error: returns a negative error code.
///
/// `out_region_offsets` and `out_region_sizes` must have at least `max_regions`
/// entries. Only `min(num_regions, max_regions)` entries are written.
pub fn validate_and_parse(
    block: &[u8],
    out_lsn: &mut u64,
    out_tid: &mut u32,
    out_count: &mut u32,
    out_num_regions: &mut u32,
    out_blob_size: &mut u64,
    out_region_offsets: &mut [u32],
    out_region_sizes: &mut [u32],
    max_regions: u32,
) -> i32 {
    if block.len() < HEADER_SIZE {
        return WAL_ERR_TRUNCATED;
    }

    let version = read_u32_le(block, OFF_VERSION);
    if version != FORMAT_VERSION {
        return WAL_ERR_VERSION;
    }

    let total_size = read_u32_le(block, OFF_SIZE) as usize;
    if total_size > block.len() || total_size < HEADER_SIZE {
        return WAL_ERR_TRUNCATED;
    }

    let expected_cs = read_u64_le(block, OFF_CHECKSUM);
    if total_size > HEADER_SIZE {
        let actual_cs = xxh::checksum(&block[HEADER_SIZE..total_size]);
        if actual_cs != expected_cs {
            return WAL_ERR_CHECKSUM;
        }
    }

    *out_lsn = read_u64_le(block, OFF_LSN);
    *out_tid = read_u32_le(block, OFF_TID);
    *out_count = read_u32_le(block, OFF_COUNT);
    let num_regions = read_u32_le(block, OFF_NUM_REGIONS);
    *out_num_regions = num_regions;
    *out_blob_size = read_u64_le(block, OFF_BLOB_SIZE);

    let n = (num_regions as usize).min(max_regions as usize);
    for i in 0..n {
        let dir_off = HEADER_SIZE + i * 8;
        if dir_off + 8 > total_size {
            break;
        }
        out_region_offsets[i] = read_u32_le(block, dir_off);
        out_region_sizes[i] = read_u32_le(block, dir_off + 4);
    }

    WAL_OK
}

#[derive(Debug)]
pub struct WalWriter {
    fd: c_int,
    buf: Vec<u8>,
}

impl WalWriter {
    pub fn open(path: &CStr) -> Result<Self, i32> {
        unsafe {
            let fd = libc::open(
                path.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_APPEND,
                0o644 as libc::mode_t,
            );
            if fd < 0 {
                return Err(-3);
            }
            if libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) < 0 {
                let e = *libc::__errno_location();
                libc::close(fd);
                return Err(if e == libc::EWOULDBLOCK { -2 } else { -3 });
            }
            Ok(WalWriter { fd, buf: Vec::new() })
        }
    }

    pub fn append_batch(
        &mut self,
        lsn: u64,
        table_id: u32,
        count: u32,
        region_ptrs: &[*const u8],
        region_sizes: &[u32],
        blob_size: u64,
    ) -> i32 {
        if count == 0 {
            return 0;
        }
        let num_regions = region_ptrs.len().min(region_sizes.len());
        let needed = block_size(num_regions, region_sizes);

        if self.buf.len() < needed {
            self.buf.resize(needed, 0);
        }

        let written = encode(
            &mut self.buf, 0, lsn, table_id, count,
            region_ptrs, region_sizes, blob_size,
        );
        if written < 0 {
            return -3;
        }
        let written = written as usize;

        unsafe {
            let rc = crate::util::write_all_fd(self.fd, &self.buf[..written]);
            if rc < 0 {
                return rc;
            }
            if libc::fdatasync(self.fd) < 0 {
                return -3;
            }
        }
        0
    }

    pub fn truncate(&mut self) -> i32 {
        unsafe {
            if libc::ftruncate(self.fd, 0) < 0 {
                return -3;
            }
            if libc::lseek(self.fd, 0, libc::SEEK_SET) < 0 {
                return -3;
            }
        }
        0
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        unsafe {
            let _ = libc::flock(self.fd, libc::LOCK_UN);
            libc::close(self.fd);
        }
    }
}

pub struct WalReader {
    fd: c_int,
    ptr: *mut u8,
    file_size: usize,
    offset: usize,
}

impl WalReader {
    pub fn open(path: &CStr) -> Result<Self, i32> {
        unsafe {
            let fd = libc::open(path.as_ptr(), libc::O_RDONLY, 0);
            if fd < 0 {
                return Err(-3);
            }
            let mut st: libc::stat = std::mem::zeroed();
            if libc::fstat(fd, &mut st) < 0 {
                libc::close(fd);
                return Err(-3);
            }
            let file_size = st.st_size as usize;
            let ptr = if file_size > 0 {
                let p = libc::mmap(
                    ptr::null_mut(),
                    file_size,
                    libc::PROT_READ,
                    libc::MAP_SHARED,
                    fd,
                    0,
                );
                if p == libc::MAP_FAILED {
                    libc::close(fd);
                    return Err(-3);
                }
                p as *mut u8
            } else {
                ptr::null_mut()
            };
            Ok(WalReader { fd, ptr, file_size, offset: 0 })
        }
    }

    pub fn base_ptr(&self) -> *const u8 {
        self.ptr as *const u8
    }

    pub fn file_size(&self) -> usize {
        self.file_size
    }

    /// Read the next WAL block. Returns:
    ///   0 = success (output params populated)
    ///   1 = EOF
    ///  -4 = corrupt/truncated block
    pub fn read_next_block(
        &mut self,
        out_lsn: &mut u64,
        out_tid: &mut u32,
        out_count: &mut u32,
        out_num_regions: &mut u32,
        out_blob_size: &mut u64,
        out_offsets: &mut [u32],
        out_sizes: &mut [u32],
        max_regions: u32,
    ) -> i32 {
        if self.offset >= self.file_size {
            return 1; // EOF
        }
        if self.offset + HEADER_SIZE > self.file_size {
            return -4; // truncated header
        }

        let block_start = self.offset;
        let block = unsafe {
            std::slice::from_raw_parts(self.ptr.add(block_start), self.file_size - block_start)
        };

        let total_size = read_u32_le(block, OFF_SIZE) as usize;
        if total_size < HEADER_SIZE {
            return -4;
        }
        if block_start + total_size > self.file_size {
            return -4;
        }

        let rc = validate_and_parse(
            &block[..total_size],
            out_lsn, out_tid, out_count, out_num_regions, out_blob_size,
            out_offsets, out_sizes, max_regions,
        );
        if rc != WAL_OK {
            return -4; // map all validation errors to -4
        }

        let n = (*out_num_regions as usize).min(max_regions as usize);
        for i in 0..n {
            out_offsets[i] += block_start as u32;
        }

        self.offset += total_size;
        0
    }
}

impl Drop for WalReader {
    fn drop(&mut self) {
        unsafe {
            if !self.ptr.is_null() {
                libc::munmap(self.ptr as *mut libc::c_void, self.file_size);
            }
            libc::close(self.fd);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_regions() -> (Vec<Vec<u8>>, Vec<*const u8>, Vec<u32>) {
        // 3 regions: pk_lo (16B), pk_hi (16B), weight (16B) — simulating 2 rows
        let r0 = vec![1u8, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0];
        let r1 = vec![0u8; 16];
        let r2 = vec![1u8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0];
        let ptrs = vec![r0.as_ptr(), r1.as_ptr(), r2.as_ptr()];
        let sizes = vec![16u32, 16, 16];
        (vec![r0, r1, r2], ptrs, sizes)
    }

    #[test]
    fn encode_decode_roundtrip() {
        let (_regions, ptrs, sizes) = make_test_regions();
        let mut buf = vec![0u8; 4096];

        let new_offset = encode(&mut buf, 0, 42, 7, 2, &ptrs, &sizes, 0);
        assert!(new_offset > 0);
        let block_len = new_offset as usize;

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let mut offsets = [0u32; 16];
        let mut rsizes = [0u32; 16];

        let rc = validate_and_parse(
            &buf[..block_len],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut offsets, &mut rsizes, 16,
        );
        assert_eq!(rc, WAL_OK);
        assert_eq!(lsn, 42);
        assert_eq!(tid, 7);
        assert_eq!(count, 2);
        assert_eq!(num_regions, 3);
        assert_eq!(blob_size, 0);

        // Verify region data
        for i in 0..3 {
            let off = offsets[i] as usize;
            let sz = rsizes[i] as usize;
            assert_eq!(sz, 16);
            assert_eq!(&buf[off..off + sz], &_regions[i][..]);
        }
    }

    #[test]
    fn encode_append_semantics() {
        let (_regions, ptrs, sizes) = make_test_regions();
        let mut buf = vec![0u8; 8192];

        // Encode two blocks sequentially (like IPC does)
        let off1 = encode(&mut buf, 0, 1, 10, 2, &ptrs, &sizes, 0);
        assert!(off1 > 0);
        let off2 = encode(&mut buf, off1 as usize, 2, 20, 2, &ptrs, &sizes, 0);
        assert!(off2 > off1);

        // Decode both blocks
        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let mut offsets = [0u32; 16];
        let mut rsizes = [0u32; 16];

        let rc1 = validate_and_parse(
            &buf[..off1 as usize],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut offsets, &mut rsizes, 16,
        );
        assert_eq!(rc1, WAL_OK);
        assert_eq!(lsn, 1);
        assert_eq!(tid, 10);

        let rc2 = validate_and_parse(
            &buf[off1 as usize..off2 as usize],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut offsets, &mut rsizes, 16,
        );
        assert_eq!(rc2, WAL_OK);
        assert_eq!(lsn, 2);
        assert_eq!(tid, 20);
    }

    #[test]
    fn alignment() {
        // Region of 5 bytes should still result in 8-byte aligned next region
        let r0 = vec![1u8, 2, 3, 4, 5];
        let r1 = vec![6u8, 7, 8, 9];
        let ptrs = vec![r0.as_ptr(), r1.as_ptr()];
        let sizes = vec![5u32, 4];
        let mut buf = vec![0u8; 4096];

        let new_offset = encode(&mut buf, 0, 0, 0, 2, &ptrs, &sizes, 0);
        assert!(new_offset > 0);

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let mut offsets = [0u32; 16];
        let mut rsizes = [0u32; 16];

        let rc = validate_and_parse(
            &buf[..new_offset as usize],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut offsets, &mut rsizes, 16,
        );
        assert_eq!(rc, WAL_OK);

        // Second region offset must be 8-byte aligned
        assert_eq!(offsets[1] % 8, 0, "region 1 not 8-byte aligned: {}", offsets[1]);
    }

    #[test]
    fn bad_version() {
        let mut buf = vec![0u8; HEADER_SIZE];
        write_u32_le(&mut buf, OFF_VERSION, 99);
        write_u32_le(&mut buf, OFF_SIZE, HEADER_SIZE as u32);

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;

        let rc = validate_and_parse(
            &buf, &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut [], &mut [], 0,
        );
        assert_eq!(rc, WAL_ERR_VERSION);
    }

    #[test]
    fn bad_checksum() {
        let (_regions, ptrs, sizes) = make_test_regions();
        let mut buf = vec![0u8; 4096];
        let new_offset = encode(&mut buf, 0, 1, 1, 2, &ptrs, &sizes, 0);

        // Corrupt one byte in the body
        buf[HEADER_SIZE + 1] ^= 0xFF;

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;

        let rc = validate_and_parse(
            &buf[..new_offset as usize],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut [], &mut [], 0,
        );
        assert_eq!(rc, WAL_ERR_CHECKSUM);
    }

    #[test]
    fn truncated_block() {
        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;

        // Too short for header
        let rc = validate_and_parse(
            &[0u8; 10],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut [], &mut [], 0,
        );
        assert_eq!(rc, WAL_ERR_TRUNCATED);
    }

    #[test]
    fn empty_regions() {
        let mut buf = vec![0u8; 4096];
        let new_offset = encode(&mut buf, 0, 1, 1, 0, &[], &[], 0);
        assert!(new_offset > 0);
        assert_eq!(new_offset as usize, HEADER_SIZE); // just a header, no directory, no data

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;

        let rc = validate_and_parse(
            &buf[..new_offset as usize],
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut [], &mut [], 0,
        );
        assert_eq!(rc, WAL_OK);
        assert_eq!(num_regions, 0);
    }

    #[test]
    fn buffer_too_small() {
        let r0 = vec![0u8; 100];
        let ptrs = vec![r0.as_ptr()];
        let sizes = vec![100u32];
        let mut buf = vec![0u8; 64]; // too small

        let rc = encode(&mut buf, 0, 0, 0, 0, &ptrs, &sizes, 0);
        assert_eq!(rc, -1);
    }

    // --- WalWriter / WalReader lifecycle tests ---

    #[test]
    fn writer_reader_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("test.wal");
        let cpath = std::ffi::CString::new(wal_path.to_str().unwrap()).unwrap();

        // Write two blocks
        let r0 = vec![10u64, 20]; // pk_lo
        let r1 = vec![0u64, 0];   // pk_hi
        let r2 = vec![1i64, 1];   // weight
        let ptrs: Vec<*const u8> = vec![
            r0.as_ptr() as *const u8,
            r1.as_ptr() as *const u8,
            r2.as_ptr() as *const u8,
        ];
        let sizes: Vec<u32> = vec![16, 16, 16];

        {
            let mut w = WalWriter::open(&cpath).unwrap();
            assert_eq!(w.append_batch(1, 42, 2, &ptrs, &sizes, 0), 0);
            assert_eq!(w.append_batch(2, 42, 2, &ptrs, &sizes, 0), 0);
            // drop closes and unlocks
        }

        // Read back
        let mut reader = WalReader::open(&cpath).unwrap();
        assert!(!reader.base_ptr().is_null());
        assert!(reader.file_size() > 0);

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let mut offsets = [0u32; 16];
        let mut rsizes = [0u32; 16];

        // Block 1
        let rc = reader.read_next_block(
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut offsets, &mut rsizes, 16,
        );
        assert_eq!(rc, 0);
        assert_eq!(lsn, 1);
        assert_eq!(tid, 42);
        assert_eq!(count, 2);
        assert_eq!(num_regions, 3);

        // Block 2
        let rc = reader.read_next_block(
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut offsets, &mut rsizes, 16,
        );
        assert_eq!(rc, 0);
        assert_eq!(lsn, 2);

        // EOF
        let rc = reader.read_next_block(
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut offsets, &mut rsizes, 16,
        );
        assert_eq!(rc, 1);
        // drop closes mmap + fd
    }

    #[test]
    fn writer_lock_contention() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("locked.wal");
        let cpath = std::ffi::CString::new(wal_path.to_str().unwrap()).unwrap();

        let _w1 = WalWriter::open(&cpath).unwrap();
        // Second open should fail with lock contention
        let err = WalWriter::open(&cpath).unwrap_err();
        assert_eq!(err, -2);
    }

    #[test]
    fn writer_truncate() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("trunc.wal");
        let cpath = std::ffi::CString::new(wal_path.to_str().unwrap()).unwrap();

        let r0 = vec![1u64];
        let ptrs: Vec<*const u8> = vec![r0.as_ptr() as *const u8];
        let sizes: Vec<u32> = vec![8];

        {
            let mut w = WalWriter::open(&cpath).unwrap();
            assert_eq!(w.append_batch(1, 1, 1, &ptrs, &sizes, 0), 0);
            assert_eq!(w.truncate(), 0);
        }

        // File should be empty after truncate
        let reader = WalReader::open(&cpath).unwrap();
        assert_eq!(reader.file_size(), 0);
    }

    #[test]
    fn reader_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("empty.wal");
        // Create empty file
        std::fs::File::create(&wal_path).unwrap();
        let cpath = std::ffi::CString::new(wal_path.to_str().unwrap()).unwrap();

        let mut reader = WalReader::open(&cpath).unwrap();
        assert!(reader.base_ptr().is_null());
        assert_eq!(reader.file_size(), 0);

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let rc = reader.read_next_block(
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut [], &mut [], 0,
        );
        assert_eq!(rc, 1); // EOF
    }

    #[test]
    fn reader_absolute_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let wal_path = dir.path().join("absoff.wal");
        let cpath = std::ffi::CString::new(wal_path.to_str().unwrap()).unwrap();

        let r0 = vec![42u64];
        let ptrs: Vec<*const u8> = vec![r0.as_ptr() as *const u8];
        let sizes: Vec<u32> = vec![8];

        {
            let mut w = WalWriter::open(&cpath).unwrap();
            assert_eq!(w.append_batch(1, 7, 1, &ptrs, &sizes, 0), 0);
        }

        let mut reader = WalReader::open(&cpath).unwrap();
        let base = reader.base_ptr();

        let mut lsn = 0u64;
        let mut tid = 0u32;
        let mut count = 0u32;
        let mut num_regions = 0u32;
        let mut blob_size = 0u64;
        let mut offsets = [0u32; 4];
        let mut rsizes = [0u32; 4];

        let rc = reader.read_next_block(
            &mut lsn, &mut tid, &mut count, &mut num_regions, &mut blob_size,
            &mut offsets, &mut rsizes, 4,
        );
        assert_eq!(rc, 0);
        assert_eq!(num_regions, 1);
        assert_eq!(rsizes[0], 8);

        // Verify absolute offset: reading u64 at base+offset should give 42
        let val = unsafe {
            let p = base.add(offsets[0] as usize) as *const u64;
            *p
        };
        assert_eq!(val, 42);
    }
}
