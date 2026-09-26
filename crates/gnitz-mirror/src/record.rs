//! A copy's registration and feed position, published in the manifest beside
//! the copy's rows.

use gnitz_core::DeltaCursor;
use gnitz_wire::{Reader, Writer};

/// What one copy's registration fixed, and where its feed got to.
#[derive(Clone, Debug, PartialEq, Eq)]
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
        let mut w = Writer::with_capacity(32 + self.schema_name.len() + self.name.len() + self.block.len());
        match self.cursor {
            Some(c) => w.u32(1).u64(c.tag).u64(c.tick),
            None => w.u32(0),
        };
        w.bytes32(self.schema_name.as_bytes())
            .bytes32(self.name.as_bytes())
            .bytes32(&self.block);
        w.into_vec()
    }

    /// `None` for bytes [`Self::encode`] did not produce, or whose block
    /// describes no layout.
    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        let mut r = Reader::new(bytes, "mirror record");
        let cursor = match r.u32().ok()? {
            0 => None,
            1 => Some(DeltaCursor { tag: r.u64().ok()?, tick: r.u64().ok()? }),
            _ => return None,
        };
        let text = |r: &mut Reader| r.bytes32().ok().and_then(|b| String::from_utf8(b.to_vec()).ok());
        let rec = MirrorRecord {
            cursor,
            schema_name: text(&mut r)?,
            name: text(&mut r)?,
            block: r.bytes32().ok()?.to_vec(),
        };
        r.expect_consumed().ok()?;
        crate::register::descriptor_of_block(&rec.block).ok()?;
        Some(rec)
    }
}

#[cfg(test)]
#[path = "tests/record.rs"]
mod tests;
