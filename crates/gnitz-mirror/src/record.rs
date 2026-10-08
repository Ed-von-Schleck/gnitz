//! A copy's registration and feed position, published in the manifest beside
//! the copy's rows.

use gnitz_core::{DeltaCursor, MirrorError, RelDescriptor, RelName};
use gnitz_wire::{decode_all, PkColList, Reader, Writer};
use gnitz_zset::schema::SchemaDescriptor;

/// What one copy's registration fixed, and where its feed got to.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MirrorRecord {
    pub(crate) name: RelName,
    /// The view's schema record; the copy's layout.
    pub(crate) block: Vec<u8>,
    /// Whether two of the view's rows may share a PK.
    pub(crate) pk_repeats: bool,
    /// The column list of each index the copy holds: [`index_lists`]' order.
    pub(crate) indexes: Vec<PkColList>,
    /// Where the copy's feed got to; `None` says the copy is not valid to read.
    pub(crate) cursor: Option<DeltaCursor>,
}

impl MirrorRecord {
    /// The bytes a copy's manifest carries for it.
    pub(crate) fn encode(&self) -> Vec<u8> {
        // Round 0 is no cursor here as on the wire, where it asks for a bootstrap.
        let (tag, tick) = self.cursor.map_or((0, 0), DeltaCursor::pair);
        let mut w = Writer::new();
        w.u64(tag)
            .u64(tick)
            .bytes32(self.name.schema().as_bytes())
            .bytes32(self.name.name().as_bytes())
            .bytes32(&self.block)
            .bool(self.pk_repeats)
            .u32(self.indexes.len() as u32);
        for cols in &self.indexes {
            w.u64(cols.pack());
        }
        w.into_vec()
    }

    /// The record and the layout its block describes; `None` for bytes
    /// [`Self::encode`] did not produce, or whose block describes no layout.
    pub(crate) fn decode(bytes: &[u8]) -> Option<(Self, SchemaDescriptor)> {
        let rec = decode_all(bytes, "mirror record", |r| {
            let (tag, tick) = (r.u64()?, r.u64()?);
            let cursor = DeltaCursor::from_pair(tag, tick);
            fn text<'a>(r: &mut Reader<'a>) -> Result<&'a str, String> {
                std::str::from_utf8(r.bytes32()?).map_err(|e| e.to_string())
            }
            let name = RelName::new(text(r)?, text(r)?)?;
            let block = r.bytes32()?.to_vec();
            let pk_repeats = r.bool()?;
            let indexes = (0..r.u32()?)
                .map(|_| PkColList::unpack(r.u64()?).map_err(|rule| format!("index columns: {rule:?}")))
                .collect::<Result<Vec<_>, String>>()?;
            // Canonical, so a record holds no index twice.
            if !indexes.windows(2).all(|w| w[0].pack() < w[1].pack()) {
                return Err("index column lists out of order".into());
            }
            Ok(MirrorRecord { cursor, name, block, pk_repeats, indexes })
        })
        .ok()?;
        let schema = descriptor_of_block(&rec.block).ok()?;
        Some((rec, schema))
    }
}

/// The column list of each index `desc` names, ascending by packing, none twice.
pub(crate) fn index_lists(desc: &RelDescriptor) -> Vec<PkColList> {
    let mut lists: Vec<PkColList> = desc.indexes.iter().map(|ix| ix.cols).collect();
    lists.sort_unstable_by_key(|cols| cols.pack());
    lists.dedup();
    lists
}

/// The engine descriptor a wire schema record denotes.
pub(crate) fn descriptor_of_block(block: &[u8]) -> Result<SchemaDescriptor, MirrorError> {
    gnitz_zset::schema::decode_schema_block(block).map_err(|e| MirrorError::Engine(format!("schema record: {e}")))
}

#[cfg(test)]
#[path = "tests/record.rs"]
mod tests;
