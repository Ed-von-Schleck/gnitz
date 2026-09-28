//! A copy's registration and feed position, published in the manifest beside
//! the copy's rows.

use std::num::NonZeroU64;

use gnitz_core::{DeltaCursor, MirrorError};
use gnitz_store::schema::SchemaDescriptor;
use gnitz_wire::{decode_all, Reader, Writer};

/// What one copy's registration fixed, and where its feed got to.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MirrorRecord {
    pub(crate) schema_name: String,
    pub(crate) name: String,
    /// The wire schema block `gnitz-core`'s codec produced; the copy's layout.
    pub(crate) block: Vec<u8>,
    /// Where the copy's feed got to; `None` says the copy is not valid to read.
    pub(crate) cursor: Option<DeltaCursor>,
}

impl MirrorRecord {
    /// The bytes a copy's manifest carries for it.
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.bool(self.cursor.is_some());
        if let Some(c) = self.cursor {
            w.u64(c.tag).u64(c.tick.get());
        }
        w.bytes32(self.schema_name.as_bytes())
            .bytes32(self.name.as_bytes())
            .bytes32(&self.block);
        w.into_vec()
    }

    /// `None` for bytes [`Self::encode`] did not produce, or whose block
    /// describes no layout.
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        let rec = decode_all(bytes, "mirror record", |r| {
            let cursor = match r.bool()? {
                false => None,
                true => {
                    let tag = r.u64()?;
                    let tick = NonZeroU64::new(r.u64()?).ok_or_else(|| "a cursor at round 0".to_string())?;
                    Some(DeltaCursor { tag, tick })
                }
            };
            let text = |r: &mut Reader| String::from_utf8(r.bytes32()?.to_vec()).map_err(|e| e.to_string());
            Ok(MirrorRecord {
                cursor,
                schema_name: text(r)?,
                name: text(r)?,
                block: r.bytes32()?.to_vec(),
            })
        })
        .ok()?;
        descriptor_of_block(&rec.block).ok()?;
        Some(rec)
    }
}

/// The engine descriptor a wire schema record denotes.
pub(crate) fn descriptor_of_block(block: &[u8]) -> Result<SchemaDescriptor, MirrorError> {
    gnitz_store::schema::decode_schema_block(block).map_err(|e| MirrorError::Engine(format!("schema record: {e}")))
}

#[cfg(test)]
#[path = "tests/record.rs"]
mod tests;
