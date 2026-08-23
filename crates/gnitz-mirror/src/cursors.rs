//! The durable record of where each mirrored view's feed got to.
//!
//! It is a file the crate writes, not a relation in the mirror's own catalog. A
//! locally-registered relation needs a locally-allocated `table_id`, and there
//! is no id space to allocate one from that the server cannot also mint: both
//! allocators count upward from `FIRST_USER_TABLE_ID`, and the register hook
//! only ever raises the local counter *past* an id it adopts. So a mirror that
//! registered a cursor relation first would take that id, and the first view a
//! user asks it to mirror may well be the server's first user relation. That is
//! what a two-line test setup produces, not a corner.
//!
//! **The header carries the checkpoint generation the records were written
//! for, and a reopen acts on them only when that is the generation the engine
//! came back at.** The file is one of the records the generation fence covers
//! (`bump_checkpoint_generation`): written after the copies are durable at `g`,
//! and carrying the `g` the flush reported, so equality with the resumed
//! generation *is* "these copies resumed from the checkpoint these positions
//! were written for". Any crash inside the sequence leaves the two unequal and
//! every view bootstraps.
//!
//! Write order alone would not do it, and the failure it misses is the quiet
//! one: a file still naming `T₁` beside copies that already absorbed through
//! `T₂` makes the next poll re-apply `(T₁, T₂]` onto rows that hold it, doubling
//! every weight in the interval while the row set stays identical.

use std::collections::HashMap;
use std::io::Write;

use gnitz_core::DeltaCursor;

/// `<base_dir>/mirror_cursors` — the file this module owns.
const CURSOR_FILENAME: &str = "mirror_cursors";
/// Magic + format word, so a file from a different layout is discarded rather
/// than misread.
const CURSOR_MAGIC: u32 = 0x474D_4331; // "GMC1"

/// Read the cursor file and return `view id → feed position` **only if** the
/// records describe the state this open actually resumed: `resume_generation`
/// must equal the header's generation, and the recorded topology must match this
/// binary's.
///
/// Everything else — an absent file, a short or corrupt one, a generation or
/// topology mismatch — yields an empty map, which makes every view bootstrap.
/// Conservative in the one direction that is never wrong.
pub(crate) fn read_cursors(
    base_dir: &str,
    resume_generation: u64,
    topology_matches: bool,
) -> HashMap<u64, DeltaCursor> {
    if !topology_matches {
        return HashMap::new();
    }
    let Ok(bytes) = std::fs::read(path(base_dir)) else {
        return HashMap::new();
    };
    if bytes.len() < 16 || (bytes.len() - 16) % 24 != 0 {
        return HashMap::new();
    }
    if u32::from_le_bytes(bytes[0..4].try_into().unwrap()) != CURSOR_MAGIC {
        return HashMap::new();
    }
    if u64::from_le_bytes(bytes[8..16].try_into().unwrap()) != resume_generation {
        return HashMap::new();
    }
    bytes[16..]
        .chunks_exact(24)
        .map(|r| {
            (
                u64::from_le_bytes(r[0..8].try_into().unwrap()),
                DeltaCursor {
                    tag: u64::from_le_bytes(r[8..16].try_into().unwrap()),
                    tick: u64::from_le_bytes(r[16..24].try_into().unwrap()),
                },
            )
        })
        .collect()
}

/// Replace the cursor file atomically: write a temp, `fdatasync` it, rename it
/// into place, then fsync the directory so the rename itself is durable.
pub(crate) fn write_cursors(base_dir: &str, generation: u64, cursors: &[(u64, DeltaCursor)]) -> Result<(), String> {
    let mut buf = Vec::with_capacity(16 + cursors.len() * 24);
    buf.extend_from_slice(&CURSOR_MAGIC.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes()); // pad to the 8-byte generation
    buf.extend_from_slice(&generation.to_le_bytes());
    for (view_id, cursor) in cursors {
        buf.extend_from_slice(&view_id.to_le_bytes());
        buf.extend_from_slice(&cursor.tag.to_le_bytes());
        buf.extend_from_slice(&cursor.tick.to_le_bytes());
    }

    let final_path = path(base_dir);
    let tmp_path = format!("{final_path}.tmp");
    let io = |what: &str, e: std::io::Error| format!("mirror cursor file: {what}: {e}");
    {
        let mut f = std::fs::File::create(&tmp_path).map_err(|e| io("create", e))?;
        f.write_all(&buf).map_err(|e| io("write", e))?;
        f.sync_data().map_err(|e| io("fdatasync", e))?;
    }
    std::fs::rename(&tmp_path, &final_path).map_err(|e| io("rename", e))?;
    // The rename is a directory entry, i.e. metadata, so it needs a full `fsync`
    // of the directory rather than the `sync_data` above.
    std::fs::File::open(base_dir)
        .and_then(|d| d.sync_all())
        .map_err(|e| io("directory fsync", e))
}

fn path(base_dir: &str) -> String {
    format!("{base_dir}/{CURSOR_FILENAME}")
}
