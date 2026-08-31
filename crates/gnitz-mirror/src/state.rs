//! `<base_dir>/mirror_state` — the durable record of what this store holds:
//! which views, under which schema, at which feed position, for which checkpoint
//! generation.
//!
//! A file, not a relation: a relation carrying this would drag a whole catalog
//! into a client — nine system families, the hooks, the DDL spine — to hold a
//! set whose size is the number of views one host mirrors.
//!
//! **The header carries the generation the records were written for, and a
//! reopen acts on them only at that generation.** Written after the copies are
//! durable at `g`, carrying the `g` the flush reported: any crash inside that
//! sequence leaves the two unequal and every view bootstraps. Write order alone
//! would miss the quiet failure — a file still naming `T₁` beside copies that
//! absorbed through `T₂` re-applies `(T₁, T₂]` onto rows that hold it, doubling
//! every weight while the row set stays identical.

use std::collections::HashMap;
use std::io::Write;

use gnitz_core::DeltaCursor;

/// `<base_dir>/mirror_state` — the file this module owns.
const STATE_FILENAME: &str = "mirror_state";
/// Magic + format word, so a file from a different layout is discarded rather
/// than misread.
const STATE_MAGIC: u32 = 0x474D_5331; // "GMS1"
/// Fixed header width, and the fixed prefix of each variable-length record.
const HEADER_LEN: usize = 32;
const RECORD_PREFIX_LEN: usize = 36;

/// Record flag: a feed position is recorded beside this registration.
///
/// A registration and a cursor are separate facts. A relation can be registered
/// with no cursor — never polled, or reset by an `Invalidate::Copy` whose
/// bootstrap then failed — and reading that back as a cursor at round 0 would
/// let the next poll answer off a copy the gate should have refused.
const REC_FLAG_HAS_CURSOR: u32 = 1;

/// What one mirrored relation's registration fixed, kept verbatim so a
/// checkpoint can rewrite the file for a relation this session never
/// re-mirrored — for which no client `Schema` is in hand.
#[derive(Clone)]
pub(crate) struct MirrorRecord {
    pub(crate) schema_name: String,
    pub(crate) name: String,
    /// The wire schema block, exactly as `gnitz-core`'s codec produced it. Whole
    /// rather than digested, because `Mirror::open` opens every store before any
    /// `mirror_view` supplies a schema and `Table::new` needs a descriptor. The
    /// shard header's `OFF_DESC_CHECKSUM` is no substitute: it is seeded from the
    /// file's own basename and header, never from a caller's schema.
    pub(crate) block: Vec<u8>,
}

/// What one open reads back: the header's fence plus one entry per relation.
pub(crate) struct MirrorState {
    pub(crate) generation: u64,
    pub(crate) records: HashMap<u64, MirrorRecord>,
    pub(crate) cursors: HashMap<u64, DeltaCursor>,
}

/// Read the state file, or `None`.
///
/// `None` on any of: an absent file, a short or corrupt one, a bad magic, a
/// record whose declared lengths would run past the end, trailing bytes after
/// the last record, or a topology word this binary did not launch under. The
/// caller then resumes nothing and reclaims the whole copies tree —
/// conservative in the one direction that is never wrong.
pub(crate) fn read_state(base_dir: &str, topology_word: u64) -> Option<MirrorState> {
    let bytes = std::fs::read(path(base_dir)).ok()?;
    if bytes.len() < HEADER_LEN {
        return None;
    }
    let u32_at = |o: usize| u32::from_le_bytes(bytes[o..o + 4].try_into().unwrap());
    let u64_at = |o: usize| u64::from_le_bytes(bytes[o..o + 8].try_into().unwrap());
    if u32_at(0) != STATE_MAGIC || u64_at(16) != topology_word {
        return None;
    }
    let generation = u64_at(8);
    let count = u64_at(24);

    let mut records = HashMap::new();
    let mut cursors = HashMap::new();
    // The count bounds the walk, since the records are variable-length. Every
    // field is read off an unaligned `&[u8]`; nothing maps or casts the buffer.
    let mut off = HEADER_LEN;
    for _ in 0..count {
        if bytes.len() - off < RECORD_PREFIX_LEN {
            return None;
        }
        let r = &bytes[off..];
        let g64 = |o: usize| u64::from_le_bytes(r[o..o + 8].try_into().unwrap());
        let table_id = g64(0);
        let cursor = DeltaCursor {
            tag: g64(8),
            tick: g64(16),
        };
        let flags = u32::from_le_bytes(r[24..28].try_into().unwrap());
        let block_len = u32::from_le_bytes(r[28..32].try_into().unwrap()) as usize;
        let schema_len = u16::from_le_bytes(r[32..34].try_into().unwrap()) as usize;
        let name_len = u16::from_le_bytes(r[34..36].try_into().unwrap()) as usize;
        let body = RECORD_PREFIX_LEN + block_len + schema_len + name_len;
        if bytes.len() - off < body {
            return None;
        }
        let block = r[RECORD_PREFIX_LEN..RECORD_PREFIX_LEN + block_len].to_vec();
        let (sa, sb) = (
            RECORD_PREFIX_LEN + block_len,
            RECORD_PREFIX_LEN + block_len + schema_len,
        );
        let (Ok(schema_name), Ok(name)) = (
            std::str::from_utf8(&r[sa..sb]),
            std::str::from_utf8(&r[sb..sb + name_len]),
        ) else {
            return None;
        };
        records.insert(
            table_id,
            MirrorRecord {
                schema_name: schema_name.to_string(),
                name: name.to_string(),
                block,
            },
        );
        if flags & REC_FLAG_HAS_CURSOR != 0 {
            cursors.insert(table_id, cursor);
        }
        off += body;
    }
    if off != bytes.len() {
        return None;
    }
    Some(MirrorState {
        generation,
        records,
        cursors,
    })
}

/// Replace the state file atomically: write a temp, `fdatasync` it, rename it
/// into place, then fsync the directory so the rename itself is durable.
pub(crate) fn write_state(
    base_dir: &str,
    generation: u64,
    topology_word: u64,
    records: &HashMap<u64, MirrorRecord>,
    cursors: &HashMap<u64, DeltaCursor>,
) -> Result<(), String> {
    let mut buf = Vec::with_capacity(HEADER_LEN + records.len() * 128);
    buf.extend_from_slice(&STATE_MAGIC.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // pad to the 8-byte generation
    buf.extend_from_slice(&generation.to_le_bytes());
    buf.extend_from_slice(&topology_word.to_le_bytes());
    buf.extend_from_slice(&(records.len() as u64).to_le_bytes());
    for (&table_id, rec) in records {
        let cursor = cursors.get(&table_id).copied();
        let flags = if cursor.is_some() { REC_FLAG_HAS_CURSOR } else { 0 };
        let cursor = cursor.unwrap_or(DeltaCursor { tag: 0, tick: 0 });
        buf.extend_from_slice(&table_id.to_le_bytes());
        buf.extend_from_slice(&cursor.tag.to_le_bytes());
        buf.extend_from_slice(&cursor.tick.to_le_bytes());
        buf.extend_from_slice(&flags.to_le_bytes());
        buf.extend_from_slice(&(rec.block.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(rec.schema_name.len() as u16).to_le_bytes());
        buf.extend_from_slice(&(rec.name.len() as u16).to_le_bytes());
        buf.extend_from_slice(&rec.block);
        buf.extend_from_slice(rec.schema_name.as_bytes());
        buf.extend_from_slice(rec.name.as_bytes());
    }

    let final_path = path(base_dir);
    let tmp_path = format!("{final_path}.tmp");
    let io = |what: &str, e: std::io::Error| format!("mirror state file: {what}: {e}");
    {
        let mut f = std::fs::File::create(&tmp_path).map_err(|e| io("create", e))?;
        f.write_all(&buf).map_err(|e| io("write", e))?;
        f.sync_data().map_err(|e| io("fdatasync", e))?;
    }
    std::fs::rename(&tmp_path, &final_path).map_err(|e| io("rename", e))?;
    // The rename is a directory entry, i.e. metadata, so it needs a full `fsync`
    // of the directory rather than the `sync_data` above.
    gnitz_store::storage::fsync_dir(base_dir).map_err(|e| format!("mirror state file: directory fsync: {e}"))
}

fn path(base_dir: &str) -> String {
    format!("{base_dir}/{STATE_FILENAME}")
}

#[cfg(test)]
#[path = "tests/state.rs"]
mod tests;
