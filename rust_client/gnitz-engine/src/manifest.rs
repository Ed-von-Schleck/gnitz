use std::mem;

// ---------------------------------------------------------------------------
// Manifest file format
// ---------------------------------------------------------------------------
//
// Header (64 bytes):
//   [0,8)   Magic   0x4D414E49464E5447
//   [8,16)  Version u64 (2 = legacy, 3 = current)
//   [16,24) Count   u64
//   [24,32) Global max LSN u64
//   [32,64) Reserved
//
// Entries (208 bytes each for V3, 184 for V2):
//   [0,8)     table_id
//   [8,24)    pk_min (lo, hi)
//   [24,40)   pk_max (lo, hi)
//   [40,56)   lsn range (min, max)
//   [56,184)  filename (128 bytes, null-terminated)
//   [184,192) level       (V3 only, default 0)
//   [192,200) guard_key_lo (V3 only, default 0)
//   [200,208) guard_key_hi (V3 only, default 0)

const MAGIC: u64 = 0x4D414E49464E5447;
const VERSION_V3: u64 = 3;
const VERSION_V2: u64 = 2;
const HEADER_SIZE: usize = 64;
const ENTRY_SIZE_V3: usize = 208;
const ENTRY_SIZE_V2: usize = 184;

/// On-disk manifest entry. Matches the V3 binary format exactly.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ManifestEntryRaw {
    pub table_id: u64,
    pub pk_min_lo: u64,
    pub pk_min_hi: u64,
    pub pk_max_lo: u64,
    pub pk_max_hi: u64,
    pub min_lsn: u64,
    pub max_lsn: u64,
    pub filename: [u8; 128],
    pub level: u64,
    pub guard_key_lo: u64,
    pub guard_key_hi: u64,
}

const _: () = assert!(mem::size_of::<ManifestEntryRaw>() == ENTRY_SIZE_V3);

impl ManifestEntryRaw {
    pub fn zeroed() -> Self {
        unsafe { mem::zeroed() }
    }
}

pub const MANIFEST_OK: i32 = 0;
pub const MANIFEST_ERR_MAGIC: i32 = -1;
pub const MANIFEST_ERR_TRUNCATED: i32 = -2;

fn read_u64_le(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

fn write_u64_le(buf: &mut [u8], off: usize, val: u64) {
    buf[off..off + 8].copy_from_slice(&val.to_le_bytes());
}

/// Serialize manifest entries into `out_buf`.
/// Returns bytes written, or -1 if buffer too small.
pub fn serialize(
    out_buf: &mut [u8],
    entries: &[ManifestEntryRaw],
    global_max_lsn: u64,
) -> i64 {
    let count = entries.len();
    let total = HEADER_SIZE + count * ENTRY_SIZE_V3;
    if out_buf.len() < total {
        return -1;
    }

    // Zero header
    out_buf[..HEADER_SIZE].fill(0);

    // Write header
    write_u64_le(out_buf, 0, MAGIC);
    write_u64_le(out_buf, 8, VERSION_V3);
    write_u64_le(out_buf, 16, count as u64);
    write_u64_le(out_buf, 24, global_max_lsn);

    // Write entries as raw bytes
    for i in 0..count {
        let off = HEADER_SIZE + i * ENTRY_SIZE_V3;
        let entry_bytes = unsafe {
            std::slice::from_raw_parts(
                &entries[i] as *const ManifestEntryRaw as *const u8,
                ENTRY_SIZE_V3,
            )
        };
        out_buf[off..off + ENTRY_SIZE_V3].copy_from_slice(entry_bytes);
    }

    total as i64
}

/// Parse a manifest buffer. Returns entry count on success, negative on error.
/// Supports V2 (184-byte entries, level/guard default to 0) and V3 (208 bytes).
pub fn parse(
    buf: &[u8],
    out_entries: &mut [ManifestEntryRaw],
    max_entries: u32,
    out_global_max_lsn: &mut u64,
) -> i32 {
    if buf.len() < HEADER_SIZE {
        return MANIFEST_ERR_TRUNCATED;
    }

    let magic = read_u64_le(buf, 0);
    if magic != MAGIC {
        return MANIFEST_ERR_MAGIC;
    }

    let version = read_u64_le(buf, 8);
    let count = read_u64_le(buf, 16) as usize;
    *out_global_max_lsn = read_u64_le(buf, 24);

    let entry_size = if version >= VERSION_V3 {
        ENTRY_SIZE_V3
    } else if version >= VERSION_V2 {
        ENTRY_SIZE_V2
    } else {
        return MANIFEST_ERR_MAGIC; // unsupported version
    };

    let expected_data = HEADER_SIZE + count * entry_size;
    if buf.len() < expected_data {
        return MANIFEST_ERR_TRUNCATED;
    }

    let n = count.min(max_entries as usize);
    for i in 0..n {
        let off = HEADER_SIZE + i * entry_size;
        let mut entry = ManifestEntryRaw::zeroed();

        // Common fields (both V2 and V3)
        entry.table_id = read_u64_le(buf, off);
        entry.pk_min_lo = read_u64_le(buf, off + 8);
        entry.pk_min_hi = read_u64_le(buf, off + 16);
        entry.pk_max_lo = read_u64_le(buf, off + 24);
        entry.pk_max_hi = read_u64_le(buf, off + 32);
        entry.min_lsn = read_u64_le(buf, off + 40);
        entry.max_lsn = read_u64_le(buf, off + 48);

        // Filename: 128 bytes at offset 56
        let fn_end = (off + 56 + 128).min(off + entry_size);
        let fn_len = fn_end - (off + 56);
        entry.filename[..fn_len].copy_from_slice(&buf[off + 56..off + 56 + fn_len]);

        // V3 fields
        if version >= VERSION_V3 {
            entry.level = read_u64_le(buf, off + 184);
            entry.guard_key_lo = read_u64_le(buf, off + 192);
            entry.guard_key_hi = read_u64_le(buf, off + 200);
        }
        // V2: level, guard_key_lo/hi already zeroed

        out_entries[i] = entry;
    }

    n as i32
}

/// Returns the buffer size needed to serialize `count` entries.
pub fn serialized_size(count: usize) -> usize {
    HEADER_SIZE + count * ENTRY_SIZE_V3
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(id: u64, name: &str) -> ManifestEntryRaw {
        let mut e = ManifestEntryRaw::zeroed();
        e.table_id = id;
        e.pk_min_lo = id * 10;
        e.pk_min_hi = 0;
        e.pk_max_lo = id * 10 + 99;
        e.pk_max_hi = 0;
        e.min_lsn = 1;
        e.max_lsn = 100;
        e.level = 1;
        e.guard_key_lo = 42;
        let bytes = name.as_bytes();
        let len = bytes.len().min(127);
        e.filename[..len].copy_from_slice(&bytes[..len]);
        e
    }

    #[test]
    fn roundtrip() {
        let entries = vec![
            make_entry(1, "shard_1.db"),
            make_entry(2, "shard_2.db"),
            make_entry(3, "path/to/shard_3.db"),
        ];
        let mut buf = vec![0u8; serialized_size(3)];

        let written = serialize(&mut buf, &entries, 999);
        assert_eq!(written as usize, buf.len());

        let mut out = vec![ManifestEntryRaw::zeroed(); 3];
        let mut lsn = 0u64;
        let count = parse(&buf, &mut out, 3, &mut lsn);
        assert_eq!(count, 3);
        assert_eq!(lsn, 999);

        for i in 0..3 {
            assert_eq!(out[i].table_id, entries[i].table_id);
            assert_eq!(out[i].pk_min_lo, entries[i].pk_min_lo);
            assert_eq!(out[i].pk_max_lo, entries[i].pk_max_lo);
            assert_eq!(out[i].min_lsn, entries[i].min_lsn);
            assert_eq!(out[i].max_lsn, entries[i].max_lsn);
            assert_eq!(out[i].level, entries[i].level);
            assert_eq!(out[i].guard_key_lo, entries[i].guard_key_lo);
            assert_eq!(out[i].filename, entries[i].filename);
        }
    }

    #[test]
    fn v2_compat() {
        // Build a V2 buffer manually (184-byte entries, no level/guard)
        let entry = make_entry(7, "legacy.db");
        let mut buf = vec![0u8; HEADER_SIZE + ENTRY_SIZE_V2];

        // Header
        write_u64_le(&mut buf, 0, MAGIC);
        write_u64_le(&mut buf, 8, VERSION_V2);
        write_u64_le(&mut buf, 16, 1); // count
        write_u64_le(&mut buf, 24, 50); // lsn

        // V2 entry (184 bytes — same as V3 but without level/guard)
        let off = HEADER_SIZE;
        write_u64_le(&mut buf, off, entry.table_id);
        write_u64_le(&mut buf, off + 8, entry.pk_min_lo);
        write_u64_le(&mut buf, off + 16, entry.pk_min_hi);
        write_u64_le(&mut buf, off + 24, entry.pk_max_lo);
        write_u64_le(&mut buf, off + 32, entry.pk_max_hi);
        write_u64_le(&mut buf, off + 40, entry.min_lsn);
        write_u64_le(&mut buf, off + 48, entry.max_lsn);
        buf[off + 56..off + 56 + 9].copy_from_slice(b"legacy.db");

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        let mut lsn = 0u64;
        let count = parse(&buf, &mut out, 1, &mut lsn);
        assert_eq!(count, 1);
        assert_eq!(lsn, 50);
        assert_eq!(out[0].table_id, 7);
        assert_eq!(out[0].level, 0); // default for V2
        assert_eq!(out[0].guard_key_lo, 0); // default for V2
    }

    #[test]
    fn bad_magic() {
        let mut buf = vec![0u8; HEADER_SIZE];
        write_u64_le(&mut buf, 0, 0xDEADBEEF);

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        let mut lsn = 0u64;
        assert_eq!(parse(&buf, &mut out, 1, &mut lsn), MANIFEST_ERR_MAGIC);
    }

    #[test]
    fn truncated() {
        assert_eq!(
            parse(&[0u8; 10], &mut [], 0, &mut 0u64),
            MANIFEST_ERR_TRUNCATED
        );
    }

    #[test]
    fn buffer_too_small() {
        let entries = vec![make_entry(1, "test.db")];
        let mut buf = vec![0u8; 64]; // too small for header + entry
        assert_eq!(serialize(&mut buf, &entries, 0), -1);
    }

    #[test]
    fn empty_manifest() {
        let mut buf = vec![0u8; HEADER_SIZE];
        let written = serialize(&mut buf, &[], 42);
        assert_eq!(written, HEADER_SIZE as i64);

        let mut lsn = 0u64;
        let count = parse(&buf, &mut [], 0, &mut lsn);
        assert_eq!(count, 0);
        assert_eq!(lsn, 42);
    }

    #[test]
    fn filename_null_terminated() {
        let e = make_entry(1, "hello.db");
        let mut buf = vec![0u8; serialized_size(1)];
        serialize(&mut buf, &[e], 0);

        let mut out = vec![ManifestEntryRaw::zeroed(); 1];
        let mut lsn = 0u64;
        parse(&buf, &mut out, 1, &mut lsn);

        // Extract filename
        let end = out[0].filename.iter().position(|&b| b == 0).unwrap_or(128);
        let name = std::str::from_utf8(&out[0].filename[..end]).unwrap();
        assert_eq!(name, "hello.db");
    }
}
