use crate::schema::MAX_PK_BYTES;
use crate::util::{read_u64_le, write_u64_le};
use super::error::StorageError;

// ---------------------------------------------------------------------------
// Manifest file format (V4)
// ---------------------------------------------------------------------------
//
// Header (64 bytes):
//   [0,8)   Magic   0x4D414E49464E5447
//   [8,16)  Version u64 (4)
//   [16,24) Count   u64
//   [24,32) Global max LSN u64
//   [32,64) Reserved
//
// Entry (338 bytes each):
//   [0,8)     table_id   u64
//   [8,89)    pk_min     PkBuf  (1 len byte + 80 raw bytes)
//   [89,170)  pk_max     PkBuf  (1 len byte + 80 raw bytes)
//   [170,178) min_lsn    u64
//   [178,186) max_lsn    u64
//   [186,314) filename   128 bytes (null-terminated)
//   [314,322) level      u64
//   [322,338) guard_key  u128 LE
//
// `pk_min`/`pk_max` carry a width-tagged byte buffer so a shard whose
// PK region is wider than 16 bytes (a compound PK) records its true
// min/max key. `guard_key` stays u128 — the compaction-routing path is
// single-PK only and out of scope here.

const MAGIC: u64 = 0x4D414E49464E5447;
const VERSION_V4: u64 = 4;
const HEADER_SIZE: usize = 64;
const ENTRY_SIZE_V4: usize = 338;

// Field offsets within an entry, kept in sync with the doc-comment above.
const OFF_TABLE_ID: usize = 0;
const OFF_PK_MIN: usize = 8;
const OFF_PK_MAX: usize = 89;
const OFF_MIN_LSN: usize = 170;
const OFF_MAX_LSN: usize = 178;
const OFF_FILENAME: usize = 186;
const OFF_LEVEL: usize = 314;
const OFF_GUARD_KEY: usize = 322;

// Build break if the documented field widths stop summing to the entry
// size — an offset/size edit that desyncs serialize/parse fails the
// build rather than corrupting the manifest. Literal widths only: the
// in-memory struct is not #[repr(C)], so size_of is unrelated to the
// 338-byte on-disk image.
const _: () = assert!(
    ENTRY_SIZE_V4 == 8 + (1 + 80) + (1 + 80) + 8 + 8 + 128 + 8 + 16,
    "V4 entry field widths do not sum to ENTRY_SIZE_V4",
);
// The 81-byte PkBuf field windows are fixed. A wider MAX_PK_BYTES would
// silently grow PkBuf::bytes while the windows stay fixed, so serialize's
// copy_from_slice of the full bytes array would panic at runtime. Force
// a deliberate build break (→ V5 format decision) instead.
const _: () = assert!(
    MAX_PK_BYTES == 80,
    "V4 manifest layout assumes an 80-byte PK capacity",
);

/// Width-tagged PK byte buffer. Plain value type — no generics, no
/// trait bounds. `len` mirrors the owning table's `pk_stride`, so a
/// manifest round-trip preserves the exact key width. Only
/// `bytes[..len]` is meaningful; the tail is always zero by
/// construction, which lets the single-PK fast path widen `bytes[..len]`
/// to a `u128` with no ambiguity.
#[derive(Clone, Copy)]
pub struct PkBuf {
    pub bytes: [u8; MAX_PK_BYTES],
    pub len: u8,
}

impl PkBuf {
    /// All-zero `bytes`, `len = stride`. The zero-row / placeholder /
    /// empty-shard form.
    pub fn empty(stride: u8) -> Self {
        debug_assert!(stride as usize <= MAX_PK_BYTES);
        PkBuf { bytes: [0u8; MAX_PK_BYTES], len: stride }
    }

    /// `len = slice.len()`, `bytes[..len]` copied from `slice`, tail
    /// zero. The only row constructor: `MappedShard::get_pk_bytes(row)`
    /// returns exactly `pk_stride` bytes, and manifest `parse` passes
    /// its on-disk `len`/payload slice.
    pub fn from_bytes(slice: &[u8]) -> Self {
        debug_assert!(slice.len() <= MAX_PK_BYTES);
        let mut bytes = [0u8; MAX_PK_BYTES];
        bytes[..slice.len()].copy_from_slice(slice);
        PkBuf { bytes, len: slice.len() as u8 }
    }

    /// `&self.bytes[..len]` — the compound / generic-comparison path
    /// uses this so the `len`-window logic lives in one place.
    #[inline]
    pub fn pk_bytes(&self) -> &[u8] {
        &self.bytes[..self.len as usize]
    }

    /// Widens a single-PK `PkBuf` to its u128 fast-path key. Mirrors
    /// the `match stride { 8 | 16 => …, _ => panic }` dispatch of
    /// `MappedShard::get_pk` / `Batch::get_pk`: panicking (not silently
    /// zero-extending) on an unexpected width is what catches a
    /// misrouted compound key reaching this path.
    #[inline]
    pub fn as_u128_single_pk(&self) -> u128 {
        match self.len {
            8 => u64::from_le_bytes(self.bytes[..8].try_into().unwrap()) as u128,
            16 => u128::from_le_bytes(self.bytes[..16].try_into().unwrap()),
            other => panic!("as_u128_single_pk: unexpected pk width {other}"),
        }
    }
}

/// On-disk manifest entry. Matches the V4 binary format exactly.
#[derive(Clone, Copy)]
pub struct ManifestEntryRaw {
    pub table_id: u64,
    pub pk_min: PkBuf,
    pub pk_max: PkBuf,
    pub min_lsn: u64,
    pub max_lsn: u64,
    pub filename: [u8; 128],
    pub level: u64,
    pub guard_key: u128,
}

impl Default for ManifestEntryRaw {
    fn default() -> Self {
        Self {
            table_id:  0,
            pk_min:    PkBuf::empty(0),
            pk_max:    PkBuf::empty(0),
            min_lsn:   0,
            max_lsn:   0,
            filename:  [0; 128],
            level:     0,
            guard_key: 0,
        }
    }
}

impl ManifestEntryRaw {
    pub fn zeroed() -> Self {
        Self::default()
    }

    pub fn filename_str(&self) -> &str {
        crate::util::cstr_from_buf(&self.filename)
    }
}

/// Serialize manifest entries into `out_buf`.
/// Returns bytes written, or `BufferTooSmall` if `out_buf` cannot fit.
pub fn serialize(
    out_buf: &mut [u8],
    entries: &[ManifestEntryRaw],
    global_max_lsn: u64,
) -> Result<usize, StorageError> {
    let count = entries.len();
    let total = HEADER_SIZE + count * ENTRY_SIZE_V4;
    if out_buf.len() < total {
        return Err(StorageError::BufferTooSmall);
    }

    // Zero header
    out_buf[..HEADER_SIZE].fill(0);

    // Write header
    write_u64_le(out_buf, 0, MAGIC);
    write_u64_le(out_buf, 8, VERSION_V4);
    write_u64_le(out_buf, 16, count as u64);
    write_u64_le(out_buf, 24, global_max_lsn);

    // Write entries field-by-field (symmetric with parse; immune to padding changes).
    // Each PkBuf is written as `len: u8` then the full 80-byte `bytes`
    // array verbatim (not bytes[..len]) — the tail is always zero by
    // construction, so the on-disk image is deterministic regardless of
    // the zero-init of the output buffer.
    for (i, e) in entries.iter().enumerate().take(count) {
        let off = HEADER_SIZE + i * ENTRY_SIZE_V4;
        write_u64_le(out_buf, off + OFF_TABLE_ID, e.table_id);
        out_buf[off + OFF_PK_MIN] = e.pk_min.len;
        out_buf[off + OFF_PK_MIN + 1..off + OFF_PK_MIN + 1 + MAX_PK_BYTES]
            .copy_from_slice(&e.pk_min.bytes);
        out_buf[off + OFF_PK_MAX] = e.pk_max.len;
        out_buf[off + OFF_PK_MAX + 1..off + OFF_PK_MAX + 1 + MAX_PK_BYTES]
            .copy_from_slice(&e.pk_max.bytes);
        write_u64_le(out_buf, off + OFF_MIN_LSN, e.min_lsn);
        write_u64_le(out_buf, off + OFF_MAX_LSN, e.max_lsn);
        out_buf[off + OFF_FILENAME..off + OFF_FILENAME + 128]
            .copy_from_slice(&e.filename);
        write_u64_le(out_buf, off + OFF_LEVEL, e.level);
        out_buf[off + OFF_GUARD_KEY..off + OFF_GUARD_KEY + 16]
            .copy_from_slice(&e.guard_key.to_le_bytes());
    }

    Ok(total)
}

/// Parse a manifest buffer. Returns entry count on success.
///
/// V4 only. There is no on-disk data to migrate in dev and the test
/// suite is green at session start, so a non-V4 version is a hard
/// `InvalidVersion` — no per-version field gating, no shims.
pub fn parse(
    buf: &[u8],
    out_entries: &mut [ManifestEntryRaw],
    max_entries: u32,
    out_global_max_lsn: &mut u64,
) -> Result<usize, StorageError> {
    if buf.len() < HEADER_SIZE {
        return Err(StorageError::Truncated);
    }

    let magic = read_u64_le(buf, 0);
    if magic != MAGIC {
        return Err(StorageError::InvalidMagic);
    }

    let version = read_u64_le(buf, 8);
    let count = read_u64_le(buf, 16) as usize;
    *out_global_max_lsn = read_u64_le(buf, 24);

    if version != VERSION_V4 {
        return Err(StorageError::InvalidVersion);
    }

    let expected_data = HEADER_SIZE + count * ENTRY_SIZE_V4;
    if buf.len() < expected_data {
        return Err(StorageError::Truncated);
    }

    let n = count.min(max_entries as usize);
    for (i, out_entry) in out_entries.iter_mut().enumerate().take(n) {
        let off = HEADER_SIZE + i * ENTRY_SIZE_V4;
        let mut entry = ManifestEntryRaw::zeroed();

        entry.table_id = read_u64_le(buf, off + OFF_TABLE_ID);
        entry.pk_min = parse_pkbuf(buf, off + OFF_PK_MIN)?;
        entry.pk_max = parse_pkbuf(buf, off + OFF_PK_MAX)?;
        entry.min_lsn = read_u64_le(buf, off + OFF_MIN_LSN);
        entry.max_lsn = read_u64_le(buf, off + OFF_MAX_LSN);
        entry.filename
            .copy_from_slice(&buf[off + OFF_FILENAME..off + OFF_FILENAME + 128]);
        entry.level = read_u64_le(buf, off + OFF_LEVEL);
        entry.guard_key =
            u128::from_le_bytes(buf[off + OFF_GUARD_KEY..off + OFF_GUARD_KEY + 16].try_into().unwrap());

        *out_entry = entry;
    }

    Ok(n)
}

/// Read a `[len u8][80 bytes]` PkBuf field at `field_off`.
///
/// `PkBuf::from_bytes` only `debug_assert!`s `len <= MAX_PK_BYTES`; in a
/// release build a corrupt manifest with `len > 80` would index past
/// the 80-byte field window. Reject it as a soft parse error, matching
/// the existing `Truncated`/`InvalidMagic` posture — a corrupt manifest
/// is not a panic.
#[inline]
fn parse_pkbuf(buf: &[u8], field_off: usize) -> Result<PkBuf, StorageError> {
    let len = buf[field_off] as usize;
    if len > MAX_PK_BYTES {
        return Err(StorageError::Truncated);
    }
    Ok(PkBuf::from_bytes(&buf[field_off + 1..field_off + 1 + len]))
}

/// Returns the buffer size needed to serialize `count` entries.
pub const fn serialized_size(count: usize) -> usize {
    HEADER_SIZE + count * ENTRY_SIZE_V4
}

// ---------------------------------------------------------------------------
// File I/O (read + atomic write)
// ---------------------------------------------------------------------------

/// Read a manifest file from disk, parse it, write entries into `out_entries`.
///
/// Returns entry count on success.
pub fn read_file(
    path: &std::ffi::CStr,
    out_entries: &mut [ManifestEntryRaw],
    max_entries: u32,
    out_global_max_lsn: &mut u64,
) -> Result<usize, StorageError> {
    unsafe {
        let fd = libc::open(path.as_ptr(), libc::O_RDONLY, 0);
        if fd < 0 {
            return Err(StorageError::Io);
        }

        let mut st: libc::stat = std::mem::zeroed();
        if libc::fstat(fd, &mut st) < 0 {
            libc::close(fd);
            return Err(StorageError::Io);
        }
        let file_size = st.st_size as usize;

        if file_size < HEADER_SIZE {
            libc::close(fd);
            return Err(StorageError::Truncated);
        }

        let mut buf = vec![0u8; file_size];
        let bytes_read = crate::util::read_all_fd(fd, &mut buf);
        libc::close(fd);

        if bytes_read < 0 || (bytes_read as usize) < file_size {
            return Err(StorageError::Io);
        }

        parse(&buf, out_entries, max_entries, out_global_max_lsn)
    }
}

/// Open .tmp manifest at "<path>.tmp", serialize entries into it, and return
/// the open fd plus owned path buffers. Does NOT fdatasync, close, or rename.
/// On any internal write error closes the fd and unlinks the .tmp.
pub struct PreparedManifest {
    pub fd: Option<libc::c_int>,
    pub tmp_path: std::ffi::CString,
    pub final_path: std::ffi::CString,
}

impl Drop for PreparedManifest {
    fn drop(&mut self) {
        if let Some(fd) = self.fd.take() {
            unsafe { libc::close(fd); }
        }
    }
}

pub fn prepare_file(
    path: &std::ffi::CStr,
    entries: &[ManifestEntryRaw],
    global_max_lsn: u64,
) -> Result<PreparedManifest, StorageError> {
    let count = entries.len();
    let total = serialized_size(count);

    let mut buf = vec![0u8; total];
    let written = serialize(&mut buf, entries, global_max_lsn)?;

    let tmp_path = super::cstr_with_tmp_suffix(path)?;
    let final_path = std::ffi::CString::new(path.to_bytes()).map_err(|_| StorageError::InvalidPath)?;

    unsafe {
        let fd = libc::open(
            tmp_path.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_TRUNC,
            0o644 as libc::mode_t,
        );
        if fd < 0 {
            return Err(StorageError::Io);
        }

        let rc = crate::util::write_all_fd(fd, &buf[..written]);
        if rc < 0 {
            libc::close(fd);
            libc::unlink(tmp_path.as_ptr());
            return Err(StorageError::Io);
        }

        Ok(PreparedManifest { fd: Some(fd), tmp_path, final_path })
    }
}

/// Serialize entries and write atomically (prepare, fdatasync, close, rename).
pub fn write_file(
    path: &std::ffi::CStr,
    entries: &[ManifestEntryRaw],
    global_max_lsn: u64,
) -> Result<(), StorageError> {
    let mut prepared = prepare_file(path, entries, global_max_lsn)?;
    let fd = prepared.fd.take().expect("prepare_file always returns fd");
    unsafe {
        if libc::fdatasync(fd) < 0 {
            libc::close(fd);
            libc::unlink(prepared.tmp_path.as_ptr());
            return Err(StorageError::Io);
        }
        libc::close(fd);
        if libc::rename(prepared.tmp_path.as_ptr(), prepared.final_path.as_ptr()) < 0 {
            libc::unlink(prepared.tmp_path.as_ptr());
            return Err(StorageError::Io);
        }
    }
    Ok(())
}

/// Read just the entry count from a manifest file header.
///
/// Returns `Ok(None)` when the file cannot be opened (does not exist yet —
/// load_manifest treats this as the empty manifest), `Ok(Some(n))` when the
/// header parses, or `Err(_)` on any other failure.
pub fn entry_count(path: &std::ffi::CStr) -> Result<Option<usize>, StorageError> {
    unsafe {
        let fd = libc::open(path.as_ptr(), libc::O_RDONLY, 0);
        if fd < 0 {
            return Ok(None);
        }
        let mut hdr = [0u8; HEADER_SIZE];
        let n = crate::util::read_all_fd(fd, &mut hdr);
        libc::close(fd);
        if n < HEADER_SIZE as i64 {
            return Err(StorageError::Truncated);
        }
        let magic = read_u64_le(&hdr, 0);
        if magic != MAGIC {
            return Err(StorageError::InvalidMagic);
        }
        Ok(Some(read_u64_le(&hdr, 16) as usize))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn pkbuf(slice: &[u8]) -> PkBuf {
        PkBuf::from_bytes(slice)
    }

    fn assert_pkbuf_eq(got: &PkBuf, want: &PkBuf) {
        assert_eq!(got.len, want.len, "PkBuf len mismatch");
        assert_eq!(got.pk_bytes(), want.pk_bytes(), "PkBuf bytes mismatch");
        assert!(
            got.bytes[got.len as usize..].iter().all(|&b| b == 0),
            "PkBuf tail must be zero",
        );
    }

    fn make_entry(id: u64, name: &str) -> ManifestEntryRaw {
        let mut e = ManifestEntryRaw::zeroed();
        e.table_id = id;
        e.pk_min = pkbuf(&(id * 10).to_le_bytes());
        e.pk_max = pkbuf(&(id * 10 + 99).to_le_bytes());
        e.min_lsn = 1;
        e.max_lsn = 100;
        e.level = 1;
        e.guard_key = 42;
        let bytes = name.as_bytes();
        let len = bytes.len().min(127);
        e.filename[..len].copy_from_slice(&bytes[..len]);
        e
    }

    #[test]
    fn roundtrip() {
        // Cover PkBuf widths 8, 16, 24, and 80.
        let widths = [8usize, 16, 24, 80];
        let entries: Vec<ManifestEntryRaw> = widths
            .iter()
            .enumerate()
            .map(|(i, &w)| {
                let mut e = make_entry(i as u64 + 1, &format!("shard_{i}.db"));
                let mut min_b = vec![0u8; w];
                let mut max_b = vec![0u8; w];
                for (j, b) in min_b.iter_mut().enumerate() {
                    *b = (j as u8).wrapping_add(1);
                }
                for (j, b) in max_b.iter_mut().enumerate() {
                    *b = (j as u8).wrapping_add(0x80);
                }
                e.pk_min = pkbuf(&min_b);
                e.pk_max = pkbuf(&max_b);
                e
            })
            .collect();
        let n = entries.len();
        let mut buf = vec![0u8; serialized_size(n)];

        let written = serialize(&mut buf, &entries, 999).unwrap();
        assert_eq!(written, buf.len());

        let mut out = vec![ManifestEntryRaw::zeroed(); n];
        let mut lsn = 0u64;
        let count = parse(&buf, &mut out, n as u32, &mut lsn).unwrap();
        assert_eq!(count, n);
        assert_eq!(lsn, 999);

        for i in 0..n {
            assert_eq!(out[i].table_id, entries[i].table_id);
            assert_pkbuf_eq(&out[i].pk_min, &entries[i].pk_min);
            assert_pkbuf_eq(&out[i].pk_max, &entries[i].pk_max);
            assert_eq!(out[i].pk_min.len as usize, widths[i]);
            assert_eq!(out[i].min_lsn, entries[i].min_lsn);
            assert_eq!(out[i].max_lsn, entries[i].max_lsn);
            assert_eq!(out[i].level, entries[i].level);
            assert_eq!(out[i].guard_key, entries[i].guard_key);
            assert_eq!(out[i].filename, entries[i].filename);
        }
    }

    #[test]
    fn v3_rejected() {
        // A V3 header must be rejected outright — no legacy reader.
        let mut buf = vec![0u8; HEADER_SIZE];
        write_u64_le(&mut buf, 0, MAGIC);
        write_u64_le(&mut buf, 8, 3); // V3
        write_u64_le(&mut buf, 16, 0);
        write_u64_le(&mut buf, 24, 0);

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        let mut lsn = 0u64;
        assert_eq!(
            parse(&buf, &mut out, 1, &mut lsn),
            Err(StorageError::InvalidVersion),
        );
    }

    #[test]
    fn corrupt_pkbuf_len_rejected() {
        let entries = vec![make_entry(1, "shard_1.db")];
        let mut buf = vec![0u8; serialized_size(1)];
        serialize(&mut buf, &entries, 0).unwrap();

        // Overwrite the pk_min len byte with 255 (> MAX_PK_BYTES).
        buf[HEADER_SIZE + OFF_PK_MIN] = 255;

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        let mut lsn = 0u64;
        assert_eq!(
            parse(&buf, &mut out, 1, &mut lsn),
            Err(StorageError::Truncated),
        );
    }

    #[test]
    fn pkbuf_width_independent_of_schema_stride() {
        // A PkBuf with len=16 must round-trip as len=16 even though some
        // table's pk_stride is 8 — PkBuf.len, not the live schema,
        // defines the stored key width (no silent truncation).
        let mut e = make_entry(1, "wide.db");
        e.pk_min = pkbuf(&[0xAB; 16]);
        e.pk_max = pkbuf(&[0xCD; 16]);

        let mut buf = vec![0u8; serialized_size(1)];
        serialize(&mut buf, &[e], 0).unwrap();

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        let mut lsn = 0u64;
        parse(&buf, &mut out, 1, &mut lsn).unwrap();

        assert_eq!(out[0].pk_min.len, 16);
        assert_eq!(out[0].pk_min.pk_bytes(), &[0xAB; 16]);
        assert_eq!(out[0].pk_max.len, 16);
        assert_eq!(out[0].pk_max.pk_bytes(), &[0xCD; 16]);
    }

    #[test]
    fn bad_magic() {
        let mut buf = vec![0u8; HEADER_SIZE];
        write_u64_le(&mut buf, 0, 0xDEADBEEF);

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        let mut lsn = 0u64;
        assert_eq!(parse(&buf, &mut out, 1, &mut lsn), Err(StorageError::InvalidMagic));
    }

    #[test]
    fn truncated() {
        assert_eq!(
            parse(&[0u8; 10], &mut [], 0, &mut 0u64),
            Err(StorageError::Truncated),
        );
    }

    #[test]
    fn buffer_too_small() {
        let entries = vec![make_entry(1, "test.db")];
        let mut buf = vec![0u8; 64]; // too small for header + entry
        assert_eq!(serialize(&mut buf, &entries, 0), Err(StorageError::BufferTooSmall));
    }

    #[test]
    fn empty_manifest() {
        let mut buf = vec![0u8; HEADER_SIZE];
        let written = serialize(&mut buf, &[], 42).unwrap();
        assert_eq!(written, HEADER_SIZE);

        let mut lsn = 0u64;
        let count = parse(&buf, &mut [], 0, &mut lsn).unwrap();
        assert_eq!(count, 0);
        assert_eq!(lsn, 42);
    }

    #[test]
    fn filename_null_terminated() {
        let e = make_entry(1, "hello.db");
        let mut buf = vec![0u8; serialized_size(1)];
        serialize(&mut buf, &[e], 0).unwrap();

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        let mut lsn = 0u64;
        parse(&buf, &mut out, 1, &mut lsn).unwrap();

        // Extract filename
        let end = out[0].filename.iter().position(|&b| b == 0).unwrap_or(128);
        let name = std::str::from_utf8(&out[0].filename[..end]).unwrap();
        assert_eq!(name, "hello.db");
    }

    // --- File I/O tests ---

    #[test]
    fn write_read_file_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let entries = vec![
            make_entry(1, "shard_1.db"),
            make_entry(2, "shard_2.db"),
            make_entry(3, "shard_3.db"),
        ];

        write_file(&cpath, &entries, 99).unwrap();
        assert!(path.exists());

        let mut out = vec![ManifestEntryRaw::zeroed(); 8];
        let mut lsn = 0u64;
        let count = read_file(&cpath, &mut out, 8, &mut lsn).unwrap();
        assert_eq!(count, 3);
        assert_eq!(lsn, 99);
        assert_eq!(out[0].table_id, 1);
        assert_eq!(out[1].table_id, 2);
        assert_eq!(out[2].table_id, 3);
        assert_eq!(out[0].level, 1);
        assert_eq!(out[0].guard_key, 42);
    }

    #[test]
    fn read_file_nonexistent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("DOES_NOT_EXIST");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        let mut lsn = 0u64;
        let rc = read_file(&cpath, &mut out, 1, &mut lsn);
        assert_eq!(rc, Err(StorageError::Io));
    }

    #[test]
    fn write_file_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("MANIFEST_EMPTY");
        let cpath = std::ffi::CString::new(path.to_str().unwrap()).unwrap();

        write_file(&cpath, &[], 42).unwrap();

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        let mut lsn = 0u64;
        let count = read_file(&cpath, &mut out, 1, &mut lsn).unwrap();
        assert_eq!(count, 0);
        assert_eq!(lsn, 42);
    }
}
