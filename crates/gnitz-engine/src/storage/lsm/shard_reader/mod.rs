//! Memory-mapped columnar shard reader.
//!
//! Used by compaction (`compact`) and query-time reads (`read_cursor`).
//! Split into the cold open/validation path ([`open`]) and the hot per-row
//! accessors ([`access`]); the type definitions and the `mmap` RAII handle
//! live here, so both sub-modules read the (otherwise private) fields directly.

use std::ffi::CStr;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::ptr;

use xorf::Xor8;

use super::error::StorageError;
use super::layout::*;
#[cfg(test)]
use crate::foundation::codec::{read_i64_le, read_u64_le};
use crate::foundation::posix_io;

mod access;
mod open;

/// RAII handle for an mmap'd file region.
///
/// Owning the mapping in a single `Drop`-bearing struct means the unmap path
/// lives in exactly one place — neither `MappedShard::open`'s error returns
/// nor `MappedShard::Drop` need to repeat the `munmap` call.
struct Mmap {
    ptr: *mut u8,
    len: usize,
}

impl Mmap {
    /// Open `path` read-only, mmap the whole file, and apply huge-page +
    /// sequential madvise hints.  Returns `Truncated` for a file shorter than
    /// `HEADER_SIZE` (the minimum a shard header occupies).
    fn open(path: &CStr) -> Result<Self, StorageError> {
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY) };
        if fd < 0 {
            return Err(StorageError::Io);
        }
        // SAFETY: fresh descriptor from `open`; the `OwnedFd` is the sole
        // closer, so every return below closes it (the mapping outlives the fd).
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd.as_raw_fd(), &mut st) } < 0 {
            return Err(StorageError::Io);
        }
        let len = st.st_size as usize;
        if len < HEADER_SIZE {
            return Err(StorageError::Truncated);
        }
        let raw = unsafe {
            libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(StorageError::Io);
        }
        let ptr = raw as *mut u8;
        posix_io::madvise_hugepage(ptr, len);
        posix_io::madvise_sequential(ptr, len);
        Ok(Mmap { ptr, len })
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.ptr as *mut libc::c_void, self.len);
        }
    }
}

// ---------------------------------------------------------------------------
// ScalarRegion / WeightRegion — self-describing region accessors
// ---------------------------------------------------------------------------

/// A pk / null-bitmap / payload-column region. These never carry the
/// `TwoValue` encoding (only the weight region does), so the variant is
/// unrepresentable here rather than rejected-then-asserted at every accessor.
#[derive(Clone)]
pub(crate) enum ScalarRegion {
    Raw {
        offset: usize,
        size: usize,
    },
    /// All elements identical. `value` holds the element bytes (for fast
    /// accessor reads via `from_le_bytes`). `offset` points into the mmap
    /// so that `col_ptr_by_logical` can return a naturally-aligned pointer.
    Constant {
        value: [u8; 16],
        offset: usize,
    },
}

/// The weight region — the only region that may use the two-value encoding.
#[derive(Clone)]
pub(crate) enum WeightRegion {
    Raw {
        offset: usize,
        size: usize,
    },
    /// All weights identical; `value` holds the i64 bytes.
    Constant {
        value: [u8; 16],
    },
    /// Exactly two distinct weights, selected per row by `bitvec_off`.
    TwoValue {
        value_a: i64,
        value_b: i64,
        bitvec_off: usize,
    },
}

pub struct MappedShard {
    /// Owning RAII handle for the mmap.  Dropped last, so `as_slice()` /
    /// raw pointers derived from it remain valid for the entire lifetime
    /// of the `MappedShard`.
    mmap: Mmap,
    pub(crate) count: usize,
    pub(crate) pk: ScalarRegion,
    pub(crate) weight: WeightRegion,
    pub(crate) null_bmp: ScalarRegion,
    /// Non-PK column regions indexed by payload position.
    pub(crate) col_regions: Vec<ScalarRegion>,
    pub(crate) blob_off: usize,
    pub(crate) blob_len: usize,
    /// Column index mapping: logical col_idx -> payload col index.
    /// PK column maps to usize::MAX (sentinel).
    col_to_payload: Vec<usize>,
    /// XOR8 membership filter (loaded from embedded header data).
    xor8_filter: Option<Xor8>,
    /// True if weight region is Raw (may contain zero-weight ghosts).
    /// False for Constant/TwoValue (non-zero weights guaranteed).
    pub(crate) has_ghosts: bool,
    /// Physical byte width of each PK value on disk (8 for U64, 16 for U128/String).
    pub(crate) pk_stride: u8,
    /// True when `SHARD_FLAG_PK_UNIQUE` is set: this shard contains at most one
    /// positive-weight row per PK. When all cursor sources carry this flag, the
    /// payload comparator can be skipped on a cross-source PK tie.
    pub(crate) is_pk_unique: bool,
}

// MappedShard does not implement Drop — the owned `mmap: Mmap` field handles
// `munmap` automatically when the shard is dropped.

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::posix_io::raise_fd_limit_for_tests;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};

    /// Build a shard via write_shard_streaming (uses encoding detection).
    fn build_test_shard(dir: &std::path::Path, rows: &[(u64, i64)]) -> String {
        let path = dir.join("test.db");
        let count = rows.len() as u32;

        // PK region holds OPK (order-preserving big-endian) bytes at rest.
        let pk_bytes: Vec<u8> = rows.iter().flat_map(|&(pk, _)| pk.to_be_bytes()).collect();
        let weights: Vec<i64> = rows.iter().map(|_| 1i64).collect();
        let null_bm: Vec<u64> = rows.iter().map(|_| 0u64).collect();
        let col1_data: Vec<i64> = rows.iter().map(|&(_, v)| v).collect();
        let blob: Vec<u8> = Vec::new();

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (null_bm.as_ptr() as *const u8, null_bm.len() * 8),
            (col1_data.as_ptr() as *const u8, col1_data.len() * 8),
            (blob.as_ptr(), blob.len()),
        ];

        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        super::super::shard_file::write_shard_streaming(libc::AT_FDCWD, &cpath, 0, count, &regions, false, 0).unwrap();
        path.to_str().unwrap().to_string()
    }

    /// Build a shard with custom weights via write_shard_streaming.
    fn build_test_shard_weights(dir: &std::path::Path, name: &str, pks: &[u64], wts: &[i64], vals: &[i64]) -> String {
        let path = dir.join(name);
        let count = pks.len() as u32;
        // PK region holds OPK (order-preserving big-endian) bytes at rest.
        let pk_bytes: Vec<u8> = pks.iter().flat_map(|&p| p.to_be_bytes()).collect();
        let null_bm: Vec<u64> = vec![0; pks.len()];
        let blob: Vec<u8> = Vec::new();

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (wts.as_ptr() as *const u8, wts.len() * 8),
            (null_bm.as_ptr() as *const u8, null_bm.len() * 8),
            (vals.as_ptr() as *const u8, vals.len() * 8),
            (blob.as_ptr(), blob.len()),
        ];

        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        super::super::shard_file::write_shard_streaming(libc::AT_FDCWD, &cpath, 0, count, &regions, false, 0).unwrap();
        path.to_str().unwrap().to_string()
    }

    fn test_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    #[test]
    fn open_and_read() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64 * 100)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        assert_eq!(shard.count, 10);
        assert_eq!(shard.get_pk(0), 1);
        assert_eq!(shard.get_pk(9), 10);
        assert_eq!(shard.get_weight(0), 1);
        assert!(!shard.has_ghosts);
    }

    #[test]
    fn binary_search() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=100).map(|i| (i * 2, i as i64)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();

        assert_eq!(shard.find_row_index(10), Some(4));
        assert_eq!(shard.find_row_index(200), Some(99));

        assert_eq!(shard.find_row_index(3), None);
        assert_eq!(shard.find_row_index(0), None);
        assert_eq!(shard.find_row_index(201), None);

        assert_eq!(shard.find_lower_bound(3), 1);
        assert_eq!(shard.find_lower_bound(1), 0);
        assert_eq!(shard.find_lower_bound(201), 100);
    }

    #[test]
    fn col_ptr_by_logical() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![(1u64, 42i64), (2, 84)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();

        let ptr = shard.col_ptr_by_logical(0, 0, 8);
        assert!(!ptr.is_null());
        // PK column holds OPK (big-endian) bytes at rest.
        let pk_be = unsafe { std::slice::from_raw_parts(ptr, 8) };
        assert_eq!(u64::from_be_bytes(pk_be.try_into().unwrap()), 1);

        let ptr = shard.col_ptr_by_logical(0, 1, 8);
        assert!(!ptr.is_null());
        let val = unsafe { *(ptr as *const i64) };
        assert_eq!(val, 42);
    }

    #[test]
    fn checksum_validation() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows = vec![(1u64, 10i64)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true);
        assert!(shard.is_ok());

        let path_str = cpath.to_str().unwrap();
        let mut data = std::fs::read(path_str).unwrap();
        let dir_off = read_u64_le(&data, OFF_DIR_OFFSET) as usize;
        let pk_lo_off = read_u64_le(&data, dir_off) as usize;
        data[pk_lo_off] ^= 0xFF;
        std::fs::write(path_str, &data).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true);
        assert_eq!(shard.err(), Some(StorageError::ChecksumMismatch));
    }

    #[test]
    fn empty_shard() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = vec![];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        assert_eq!(shard.count, 0);
        assert_eq!(shard.find_row_index(1), None);
        assert_eq!(shard.find_lower_bound(1), 0);
    }

    // --- v4 encoding tests ---

    #[test]
    fn constant_weight_roundtrip() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 100;
        let pks: Vec<u64> = (1..=n).collect();
        let wts: Vec<i64> = vec![1; n as usize];
        let vals: Vec<i64> = (1..=n).map(|i| i as i64 * 10).collect();
        let path = build_test_shard_weights(dir.path(), "const_w.db", &pks, &wts, &vals);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path.clone()).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, n as usize);
        assert!(!shard.has_ghosts);
        for i in 0..n as usize {
            assert_eq!(shard.get_weight(i), 1);
        }

        // Verify v4 shard is smaller than a naive raw shard would be
        let file_len = std::fs::metadata(&path).unwrap().len() as usize;
        // Raw weight region alone would be n*8 = 800 bytes
        // Constant region is just 8 bytes
        assert!(file_len < HEADER_SIZE + n as usize * 8 * 6);
    }

    #[test]
    fn two_value_weight_roundtrip() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 64usize;
        let pks: Vec<u64> = (1..=n as u64).collect();
        let wts: Vec<i64> = (0..n).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect();
        let vals: Vec<i64> = (0..n).map(|i| i as i64 * 10).collect();
        let path = build_test_shard_weights(dir.path(), "twoval_w.db", &pks, &wts, &vals);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, n);
        // TwoValue weight -> has_ghosts = false (neither value is 0)
        assert!(!shard.has_ghosts);
        for i in 0..n {
            let expected = if i % 2 == 0 { 1i64 } else { -1i64 };
            assert_eq!(shard.get_weight(i), expected, "row {i}");
        }
    }

    #[test]
    fn three_value_weight_raw() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let pks: Vec<u64> = vec![1, 2, 3];
        let wts: Vec<i64> = vec![1, -1, 2];
        let vals: Vec<i64> = vec![10, 20, 30];
        let path = build_test_shard_weights(dir.path(), "raw_w.db", &pks, &wts, &vals);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert!(!shard.has_ghosts); // Raw encoding but no zero weights → no ghosts
        assert_eq!(shard.get_weight(0), 1);
        assert_eq!(shard.get_weight(1), -1);
        assert_eq!(shard.get_weight(2), 2);
    }

    #[test]
    fn constant_pk_roundtrip() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 128;
        let rows: Vec<(u64, i64)> = (1..=n).map(|i| (i, i as i64)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        for i in 0..n as usize {
            assert_eq!(shard.get_pk(i), (i + 1) as u128);
        }
        // Binary search still works
        assert_eq!(shard.find_row_index(64), Some(63));
        assert_eq!(shard.find_row_index(1), Some(0));
        assert_eq!(shard.find_row_index(128), Some(127));
        assert_eq!(shard.find_lower_bound(65), 64);
    }

    #[test]
    fn constant_null_bmp() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        for i in 0..10 {
            assert_eq!(shard.get_null_word(i), 0);
        }
    }

    #[test]
    fn constant_payload_column() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 10;
        let pks: Vec<u64> = (1..=n).collect();
        let wts: Vec<i64> = vec![1; n as usize];
        let vals: Vec<i64> = vec![42; n as usize]; // all same
        let path = build_test_shard_weights(dir.path(), "const_col.db", &pks, &wts, &vals);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        for i in 0..n as usize {
            let data = shard.get_col_ptr(i, 0, 8);
            let v = i64::from_le_bytes(data.try_into().unwrap());
            assert_eq!(v, 42);
            // col_ptr_by_logical for constant payload column
            let ptr = shard.col_ptr_by_logical(i, 1, 8);
            assert!(!ptr.is_null());
            let v2 = unsafe { *(ptr as *const i64) };
            assert_eq!(v2, 42);
        }
    }

    #[test]
    fn unknown_encoding_rejected() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = vec![(1, 10)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();

        // Corrupt: set encoding byte of first region to 0x10
        let mut data = std::fs::read(&path).unwrap();
        let dir_off = read_u64_le(&data, OFF_DIR_OFFSET) as usize;
        data[dir_off + 24] = 0x10; // unknown encoding
        std::fs::write(&path, &data).unwrap();

        let cpath = std::ffi::CString::new(path).unwrap();
        assert_eq!(
            MappedShard::open(&cpath, &schema, false).err(),
            Some(StorageError::InvalidShard)
        );
    }

    #[test]
    fn nonzero_reserved_rejected() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = vec![(1, 10)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();

        // Corrupt: set a reserved byte to non-zero
        let mut data = std::fs::read(&path).unwrap();
        let dir_off = read_u64_le(&data, OFF_DIR_OFFSET) as usize;
        data[dir_off + 25] = 0xFF; // reserved byte
        std::fs::write(&path, &data).unwrap();

        let cpath = std::ffi::CString::new(path).unwrap();
        assert_eq!(
            MappedShard::open(&cpath, &schema, false).err(),
            Some(StorageError::InvalidShard)
        );
    }

    #[test]
    fn two_value_truncated_bitvec_rejected() {
        // Build a shard with TwoValue weight encoding, then corrupt it so the
        // region size is shorter than the required bitvec (< 16 + ceil(n/8)).
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 64usize;
        let pks: Vec<u64> = (1..=n as u64).collect();
        let wts: Vec<i64> = (0..n).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect();
        let vals: Vec<i64> = (0..n).map(|i| i as i64).collect();
        let path = build_test_shard_weights(dir.path(), "twoval_trunc.db", &pks, &wts, &vals);
        let schema = test_schema();

        // The weight region must be TwoValue.  Shrink its size field in the
        // directory entry so bitvec_len < ceil(n/8) = 8 bytes.  Weight
        // region is entry index 1 in the directory.
        let mut data = std::fs::read(&path).unwrap();
        let dir_off = read_u64_le(&data, OFF_DIR_OFFSET) as usize;
        // Entry index 1 = weight region.
        let weight_entry_off = dir_off + DIR_ENTRY_SIZE;
        let orig_size = read_u64_le(&data, weight_entry_off + 8);
        // Only write the two values (16 bytes), drop the bitvec.
        let truncated_size = 16u64;
        assert!(
            orig_size > truncated_size,
            "weight region must be larger than 16 bytes for this test"
        );
        // Patch the size field.  Checksum validation is disabled below so the
        // stale checksum doesn't mask the InvalidShard we're expecting.
        crate::foundation::codec::write_u64_le(&mut data, weight_entry_off + 8, truncated_size);
        std::fs::write(&path, &data).unwrap();

        let cpath = std::ffi::CString::new(path).unwrap();
        // Must be rejected — the bitvec is too short for 64 rows.
        assert_eq!(
            MappedShard::open(&cpath, &schema, false).err(),
            Some(StorageError::InvalidShard),
            "truncated TwoValue bitvec must be rejected at open time"
        );
    }

    #[test]
    fn two_value_pk_region_rejected() {
        // TwoValue is valid only for the weight region. Forge it onto the pk
        // region (dir entry 0) and confirm open rejects the shard — `ScalarRegion`
        // has no TwoValue variant, so `build_scalar_region` returns InvalidShard.
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 8usize;
        let pks: Vec<u64> = (1..=n as u64).collect();
        let wts: Vec<i64> = vec![1; n];
        let vals: Vec<i64> = (0..n).map(|i| i as i64).collect();
        let path = build_test_shard_weights(dir.path(), "twoval_pk.db", &pks, &wts, &vals);
        let schema = test_schema();

        // Patch the pk directory entry's encoding byte (entry 0, offset +24) to
        // ENCODING_TWO_VALUE. Checksums off so the stale checksum doesn't mask it.
        let mut data = std::fs::read(&path).unwrap();
        let dir_off = read_u64_le(&data, OFF_DIR_OFFSET) as usize;
        data[dir_off + 24] = ENCODING_TWO_VALUE;
        std::fs::write(&path, &data).unwrap();

        let cpath = std::ffi::CString::new(path).unwrap();
        assert_eq!(
            MappedShard::open(&cpath, &schema, false).err(),
            Some(StorageError::InvalidShard),
            "TwoValue on the pk region must be rejected at open time"
        );
    }

    #[test]
    fn single_row_shard() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = vec![(42, 999)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.count, 1);
        assert!(!shard.has_ghosts);
        assert_eq!(shard.get_pk(0), 42);
        assert_eq!(shard.get_weight(0), 1);
        assert_eq!(shard.get_null_word(0), 0);
        assert_eq!(shard.find_row_index(42), Some(0));
        assert_eq!(shard.find_row_index(1), None);
    }

    #[test]
    fn to_owned_batch_roundtrip() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64 * 100)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        let batch = shard.to_owned_batch(&schema);

        assert_eq!(batch.count, 10);
        assert!(batch.is_sorted());
        assert!(batch.is_consolidated());
        for i in 0..10 {
            assert_eq!(batch.get_pk(i), (i + 1) as u128);
            let w = read_i64_le(batch.weight_data(), i * 8);
            assert_eq!(w, 1);
            let v = read_i64_le(batch.col_data(0), i * 8);
            assert_eq!(v, (i as i64 + 1) * 100);
        }
    }

    #[test]
    fn to_owned_batch_constant_regions() {
        // All weights = 1 (Constant), all null = 0 (Constant), all vals = 42 (Constant)
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 20;
        let pks: Vec<u64> = (1..=n).collect();
        let wts: Vec<i64> = vec![1; n as usize];
        let vals: Vec<i64> = vec![42; n as usize]; // constant payload
        let path = build_test_shard_weights(dir.path(), "const_batch.db", &pks, &wts, &vals);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        let batch = shard.to_owned_batch(&schema);

        assert_eq!(batch.count, n as usize);
        for i in 0..n as usize {
            assert_eq!(batch.get_pk(i), (i + 1) as u128);
            assert_eq!(read_i64_le(batch.weight_data(), i * 8), 1);
            assert_eq!(read_i64_le(batch.col_data(0), i * 8), 42);
        }
    }

    #[test]
    fn to_owned_batch_two_value_weight() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let n = 16usize;
        let pks: Vec<u64> = (1..=n as u64).collect();
        let wts: Vec<i64> = (0..n).map(|i| if i % 2 == 0 { 1 } else { -1 }).collect();
        let vals: Vec<i64> = (0..n).map(|i| i as i64 * 10).collect();
        let path = build_test_shard_weights(dir.path(), "twoval_batch.db", &pks, &wts, &vals);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        let batch = shard.to_owned_batch(&schema);

        assert_eq!(batch.count, n);
        for i in 0..n {
            let expected_w = if i % 2 == 0 { 1i64 } else { -1i64 };
            assert_eq!(read_i64_le(batch.weight_data(), i * 8), expected_w, "row {i}");
            assert_eq!(read_i64_le(batch.col_data(0), i * 8), i as i64 * 10);
        }
    }

    #[test]
    fn slice_to_owned_batch_with_offset() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=10).map(|i| (i, i as i64 * 100)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();

        // Slice rows 3..7 (0-indexed: PKs 4,5,6,7)
        let batch = shard.slice_to_owned_batch(3, 4, &schema);
        assert_eq!(batch.count, 4);
        assert_eq!(batch.get_pk(0), 4);
        assert_eq!(batch.get_pk(3), 7);
        assert_eq!(read_i64_le(batch.col_data(0), 0), 400);
        assert_eq!(read_i64_le(batch.col_data(0), 3 * 8), 700);
    }

    #[test]
    fn slice_to_owned_batch_empty() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=5).map(|i| (i, i as i64)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        let batch = shard.slice_to_owned_batch(0, 0, &schema);
        assert_eq!(batch.count, 0);
    }

    #[test]
    fn u64_pk_open_and_read() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = vec![(10, 100), (20, 200), (30, 300)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert_eq!(shard.pk_stride, 8, "pk_stride must be 8 for U64 schema");
        assert_eq!(shard.count, 3);
        assert_eq!(shard.get_pk(0), 10u128);
        assert_eq!(shard.get_pk(1), 20u128);
        assert_eq!(shard.get_pk(2), 30u128);
        assert_eq!(shard.find_lower_bound(15), 1);
        assert_eq!(shard.find_lower_bound(10), 0);
        assert_eq!(shard.find_lower_bound(31), 3);
    }

    #[test]
    fn u64_pk_slice_to_owned_batch() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1u64..=8).map(|i| (i * 10, i as i64 * 100)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();

        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        let batch = shard.slice_to_owned_batch(0, 8, &schema);

        assert_eq!(batch.count, 8);
        // pk_data() length = count * pk_stride = 8 * 8
        assert_eq!(batch.pk_data().len(), 8 * 8, "pk region must be 8B/row for U64 schema");
        for i in 0..8usize {
            assert_eq!(batch.get_pk(i), (i as u128 + 1) * 10, "PK row {i}");
        }
    }

    fn u128_pk_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U128, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn build_test_shard_u128(dir: &std::path::Path, name: &str, pks: &[u128], vals: &[i64]) -> String {
        let path = dir.join(name);
        let count = pks.len() as u32;
        // PK region holds OPK (order-preserving big-endian) bytes at rest.
        let pk_bytes: Vec<u8> = pks.iter().flat_map(|&p| p.to_be_bytes()).collect();
        let weights: Vec<i64> = vec![1; pks.len()];
        let null_bm: Vec<u64> = vec![0; pks.len()];
        let blob: Vec<u8> = Vec::new();

        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (null_bm.as_ptr() as *const u8, null_bm.len() * 8),
            (vals.as_ptr() as *const u8, vals.len() * 8),
            (blob.as_ptr(), blob.len()),
        ];

        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        super::super::shard_file::write_shard_streaming(libc::AT_FDCWD, &cpath, 0, count, &regions, false, 0).unwrap();
        path.to_str().unwrap().to_string()
    }

    #[test]
    fn shard_reader_get_pk_bytes_raw_region_u64() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = (1..=8).map(|i| (i * 3, i as i64)).collect();
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();
        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        assert!(matches!(shard.pk, ScalarRegion::Raw { .. }), "expected Raw PK region");
        assert_eq!(shard.pk_stride, 8);
        for i in 0..rows.len() {
            let bytes = shard.get_pk_bytes(i);
            assert_eq!(bytes.len(), 8, "row {i} stride");
            let pk_u128 = shard.get_pk(i);
            // PK region is OPK (big-endian); the low 8 bytes of the BE u128.
            assert_eq!(bytes, &pk_u128.to_be_bytes()[8..], "row {i} opk bytes");
        }
    }

    #[test]
    fn shard_reader_get_pk_bytes_raw_region_u128() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let pks: Vec<u128> = vec![1, (u64::MAX as u128) + 1, (u64::MAX as u128) * 2 + 3, u128::MAX];
        let vals: Vec<i64> = (0..pks.len() as i64).collect();
        let path = build_test_shard_u128(dir.path(), "u128_raw.db", &pks, &vals);
        let schema = u128_pk_schema();
        let cpath = std::ffi::CString::new(path).unwrap();
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert!(matches!(shard.pk, ScalarRegion::Raw { .. }), "expected Raw PK region");
        assert_eq!(shard.pk_stride, 16);
        for (i, &pk) in pks.iter().enumerate() {
            let bytes = shard.get_pk_bytes(i);
            assert_eq!(bytes.len(), 16, "row {i} stride");
            // PK region is OPK (order-preserving big-endian) at rest.
            assert_eq!(bytes, &pk.to_be_bytes(), "row {i} opk bytes");
            assert_eq!(shard.get_pk(i), pk, "row {i} u128");
        }
    }

    #[test]
    fn shard_reader_get_pk_bytes_constant_region() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        // All PKs identical -> encoding detection picks Constant.
        let pk_value: u128 = 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210u128;
        let n = 32;
        let pks: Vec<u128> = vec![pk_value; n];
        let vals: Vec<i64> = (0..n as i64).collect();
        let path = build_test_shard_u128(dir.path(), "u128_const.db", &pks, &vals);
        let schema = u128_pk_schema();
        let cpath = std::ffi::CString::new(path).unwrap();
        let shard = MappedShard::open(&cpath, &schema, true).unwrap();
        assert!(
            matches!(shard.pk, ScalarRegion::Constant { .. }),
            "expected Constant PK region"
        );
        assert_eq!(shard.pk_stride, 16);
        // PK region is OPK (order-preserving big-endian) at rest.
        let expected = pk_value.to_be_bytes();
        for i in 0..n {
            assert_eq!(shard.get_pk_bytes(i), &expected, "row {i} bytes");
            assert_eq!(shard.get_pk(i), pk_value, "row {i} u128");
        }
    }

    #[test]
    fn find_lower_bound_bytes_narrow_matches_find_lower_bound() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let rows: Vec<(u64, i64)> = vec![(10, 100), (20, 200), (30, 300), (40, 400)];
        let path = build_test_shard(dir.path(), &rows);
        let schema = test_schema();
        let cpath = std::ffi::CString::new(path).unwrap();
        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        for probe in [0u64, 5, 10, 15, 20, 25, 30, 40, 41, u64::MAX] {
            let by_u128 = shard.find_lower_bound(probe as u128);
            // OPK bytes for a U64 PK are big-endian; raw memcmp binary search.
            let by_bytes = shard.find_lower_bound_bytes(&probe.to_be_bytes());
            assert_eq!(by_u128, by_bytes, "probe={probe}");
        }
    }

    #[test]
    fn find_lower_bound_bytes_wide_pk_distinct() {
        // Wide PK (3xU64 all-PK, stride 24). Distinct PKs keep the shard PK
        // region as Raw (ScalarRegion::Constant's get_pk_bytes returns
        // &value[..stride] from a 16-byte buffer and would panic for stride
        // 24 — see §6 caveat).
        //
        // Schema is all-PK (num_payload = 0) so MappedShard::open's payload
        // validation loop is empty. The shard image must still carry the
        // (3 + (num_cols - 1) + 1) = 6 regions open() reads from the
        // directory; the trailing slots beyond pk/weight/null/blob are
        // never accessed via the byte path.
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0, 1, 2],
        );
        assert_eq!(schema.pk_stride(), 24);

        // OPK for a 3xU64 compound PK: each column big-endian, concatenated in
        // pk-list order. memcmp of the bytes then equals (col0, col1, col2)
        // lexicographic order.
        let opk3 = |a: u64, b: u64, c: u64| -> [u8; 24] {
            let mut out = [0u8; 24];
            out[0..8].copy_from_slice(&a.to_be_bytes());
            out[8..16].copy_from_slice(&b.to_be_bytes());
            out[16..24].copy_from_slice(&c.to_be_bytes());
            out
        };

        // Five rows, sorted in compare_pk_bytes order (col0, then col1, col2).
        let pks: [[u8; 24]; 5] = [
            opk3(0, 0, 0),
            opk3(1, 0, 0),
            opk3(1, 5, 0),
            opk3(1, 5, 9),
            opk3(2, 0, 0),
        ];
        let count = pks.len() as u32;
        let pk_bytes: Vec<u8> = pks.iter().flat_map(|r| r.iter().copied()).collect();
        let weights: Vec<i64> = vec![1; count as usize];
        let null_bm: Vec<u64> = vec![0; count as usize];
        let empty: Vec<u8> = Vec::new();

        // 6 regions: pk, weight, null_bmp, two "phantom non-pk" slots
        // (num_non_pk = num_cols - 1 = 2), blob.
        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (null_bm.as_ptr() as *const u8, null_bm.len() * 8),
            (empty.as_ptr(), 0),
            (empty.as_ptr(), 0),
            (empty.as_ptr(), 0),
        ];
        let path = dir.path().join("wide_pk.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        super::super::shard_file::write_shard_streaming(libc::AT_FDCWD, &cpath, 0, count, &regions, false, 0).unwrap();
        let shard = MappedShard::open(&cpath, &schema, false).unwrap();
        assert_eq!(shard.pk_stride, 24);
        assert!(
            matches!(shard.pk, ScalarRegion::Raw { .. }),
            "distinct PKs must keep PK region Raw"
        );

        // Probe keys covering before, between, and after each row.
        let probes: [[u8; 24]; 5] = [
            opk3(0, 0, 0),
            opk3(0, 0, 1),
            opk3(1, 5, 9),
            opk3(1, 5, 10),
            opk3(3, 0, 0),
        ];
        for key in &probes {
            let expected = (0..shard.count)
                .find(|&i| {
                    super::super::columnar::compare_pk_bytes(shard.get_pk_bytes(i), key) != std::cmp::Ordering::Less
                })
                .unwrap_or(shard.count);
            let got = shard.find_lower_bound_bytes(key);
            assert_eq!(got, expected, "probe={key:?}");
        }
    }

    /// Defense-in-depth: a wide PK region (stride > 16) declaring
    /// ENCODING_CONSTANT must be rejected at open time. A Constant region holds
    /// only a `[u8; 16]` value, so `get_pk_bytes` would slice `&value[..24]`
    /// out of bounds. The writer never emits this, so we forge it by patching
    /// the PK directory entry's encoding byte on an otherwise-valid wide shard.
    #[test]
    fn wide_pk_constant_encoding_rejected() {
        raise_fd_limit_for_tests();
        let dir = tempfile::tempdir().unwrap();
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0, 1, 2],
        );
        assert_eq!(schema.pk_stride(), 24);

        // Two distinct wide PKs keep the PK region Raw.
        // Grouped by 8-byte u64 column boundaries (3xU64 compound PK).
        #[rustfmt::skip]
        let pks: [[u8; 24]; 2] = [
            [1,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
            [2,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0],
        ];
        let count = pks.len() as u32;
        let pk_bytes: Vec<u8> = pks.iter().flat_map(|r| r.iter().copied()).collect();
        let weights: Vec<i64> = vec![1; count as usize];
        let null_bm: Vec<u64> = vec![0; count as usize];
        let empty: Vec<u8> = Vec::new();
        let regions: Vec<(*const u8, usize)> = vec![
            (pk_bytes.as_ptr(), pk_bytes.len()),
            (weights.as_ptr() as *const u8, weights.len() * 8),
            (null_bm.as_ptr() as *const u8, null_bm.len() * 8),
            (empty.as_ptr(), 0),
            (empty.as_ptr(), 0),
            (empty.as_ptr(), 0),
        ];
        let path = dir.path().join("wide_const.db");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();
        super::super::shard_file::write_shard_streaming(libc::AT_FDCWD, &cpath, 0, count, &regions, false, 0).unwrap();

        // Sanity: unpatched shard opens fine with a Raw PK region.
        assert!(MappedShard::open(&cpath, &schema, false).is_ok());

        // Patch the PK directory entry (index 0) encoding byte to CONSTANT.
        let mut data = std::fs::read(&path).unwrap();
        let dir_off = read_u64_le(&data, OFF_DIR_OFFSET) as usize;
        data[dir_off + 24] = ENCODING_CONSTANT;
        std::fs::write(&path, &data).unwrap();

        assert_eq!(
            MappedShard::open(&cpath, &schema, false).err(),
            Some(StorageError::InvalidShard),
            "wide PK region declaring ENCODING_CONSTANT must be rejected",
        );
    }
}
