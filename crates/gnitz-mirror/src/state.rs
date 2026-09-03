//! `<base_dir>/mirror_state` — the durable record of what this store holds:
//! which views, under which schema, at which feed position, for which checkpoint
//! generation.
//!
//! A file holding one Z-set batch in the engine's wire codec, which checks the
//! version, every region extent and an XXH3-64 over the body on the way back
//! in. Only the generation word ahead of it carries no redundancy, and a
//! flipped generation can never equal the manifests'.
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

use gnitz_core::{DeltaCursor, MirrorError};
use gnitz_store::schema::{SchemaColumn, SchemaDescriptor};
use gnitz_store::storage::{Batch, BatchBuilder};
use gnitz_wire::{null_word_get, read_u64_le, type_code};

/// `<base_dir>/mirror_state` — the file this module owns.
const STATE_FILENAME: &str = "mirror_state";

/// One row per mirrored relation, keyed by id; a null cursor is a registration
/// with no feed position.
const STATE_SCHEMA: SchemaDescriptor = SchemaDescriptor::new(
    &[
        SchemaColumn::new(type_code::U64, 0),
        SchemaColumn::new(type_code::U64, 1),
        SchemaColumn::new(type_code::U64, 1),
        SchemaColumn::new(type_code::STRING, 0),
        SchemaColumn::new(type_code::STRING, 0),
        SchemaColumn::new(type_code::BLOB, 0),
    ],
    &[0],
);
/// The payload slots of [`STATE_SCHEMA`], in the order a row is written.
const TAG: usize = 0;
const TICK: usize = 1;
const SCHEMA_NAME: usize = 2;
const NAME: usize = 3;
const BLOCK: usize = 4;
/// `generation u64 LE ‖ checksummed wire block of STATE_SCHEMA rows`.
const HEADER_LEN: usize = 8;

/// What one mirrored relation's registration fixed, kept so a checkpoint can
/// rewrite the file for a relation this session never re-mirrored — for which
/// no client `Schema` is in hand.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MirrorRecord {
    pub(crate) schema_name: String,
    pub(crate) name: String,
    /// The wire schema block as `gnitz-core`'s codec produced it. Whole, because
    /// `Mirror::open` opens every store before any `mirror_view` supplies a
    /// schema.
    pub(crate) block: Vec<u8>,
    /// Where the copy's feed got to; `None` says the copy is not valid to read.
    pub(crate) cursor: Option<DeltaCursor>,
}

/// What one open reads back: the header's fence plus one entry per relation.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct PersistedState {
    pub(crate) generation: u64,
    pub(crate) records: HashMap<u64, MirrorRecord>,
}

/// Read the state file, or `None`.
///
/// `None` on any of: an absent file, a short one, a block the codec refuses —
/// a version, extent or checksum mismatch — or bytes past the block's end. The
/// caller then resumes nothing and reclaims the whole copies tree —
/// conservative in the one direction that is never wrong.
pub(crate) fn read_state(base_dir: &str) -> Option<PersistedState> {
    let bytes = std::fs::read(path(base_dir)).ok()?;
    if bytes.len() < HEADER_LEN {
        return None;
    }
    let (batch, used) = Batch::decode_from_wal_block(&bytes[HEADER_LEN..], &STATE_SCHEMA, true).ok()?;
    if HEADER_LEN + used != bytes.len() {
        return None;
    }
    let mut records = HashMap::with_capacity(batch.len());
    for row in 0..batch.len() {
        let cursor = (!null_word_get(batch.get_null_word(row), TAG)).then(|| DeltaCursor {
            tag: batch.read_payload_u64(row, TAG),
            tick: batch.read_payload_u64(row, TICK),
        });
        records.insert(
            batch.get_pk(row) as u64,
            MirrorRecord {
                schema_name: batch.read_payload_string(row, SCHEMA_NAME),
                name: batch.read_payload_string(row, NAME),
                block: batch.read_payload_bytes(row, BLOCK).to_vec(),
                cursor,
            },
        );
    }
    Some(PersistedState {
        generation: read_u64_le(&bytes, 0),
        records,
    })
}

/// Replace the state file atomically: write a temp, `fdatasync` it, rename it
/// into place, then fsync the directory so the rename itself is durable.
pub(crate) fn write_state(
    base_dir: &str,
    generation: u64,
    records: &HashMap<u64, MirrorRecord>,
) -> Result<(), MirrorError> {
    let mut bb = BatchBuilder::new(STATE_SCHEMA);
    for (&tid, r) in records {
        bb.begin_row(tid as u128, 1);
        match r.cursor {
            Some(c) => {
                bb.put_u64(c.tag);
                bb.put_u64(c.tick);
            }
            None => {
                bb.put_null();
                bb.put_null();
            }
        }
        bb.put_string(&r.schema_name);
        bb.put_string(&r.name);
        bb.put_blob(&r.block);
        bb.end_row();
    }
    let mut buf = generation.to_le_bytes().to_vec();
    buf.extend_from_slice(&bb.finish().encode_to_wire_vec(0, true));

    let final_path = path(base_dir);
    let tmp_path = format!("{final_path}.tmp");
    let io = |what: &str, e: std::io::Error| MirrorError::Engine(format!("mirror state file: {what}: {e}"));
    {
        let mut f = std::fs::File::create(&tmp_path).map_err(|e| io("create", e))?;
        f.write_all(&buf).map_err(|e| io("write", e))?;
        f.sync_data().map_err(|e| io("fdatasync", e))?;
    }
    std::fs::rename(&tmp_path, &final_path).map_err(|e| io("rename", e))?;
    // The rename is a directory entry, i.e. metadata, so it needs a full `fsync`
    // of the directory rather than the `sync_data` above.
    gnitz_store::storage::fsync_dir(base_dir)
        .map_err(|e| MirrorError::Engine(format!("mirror state file: directory fsync: {e}")))
}

fn path(base_dir: &str) -> String {
    format!("{base_dir}/{STATE_FILENAME}")
}

#[cfg(test)]
#[path = "tests/state.rs"]
mod tests;
