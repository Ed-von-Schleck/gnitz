//! A copy's registration and feed position, published in the manifest beside
//! the copy's rows.

use gnitz_core::{DeltaCursor, RelDescriptor, RelName};
use gnitz_wire::{MAX_LIST_LEN, PkColList, Writer, decode_all};
use gnitz_zset::schema::{SchemaDescriptor, decode_schema_record};

/// What one copy's registration fixed, and where its feed got to.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MirrorRecord {
    pub(crate) name: RelName,
    /// The view's schema record; the copy's layout.
    pub(crate) schema: Vec<u8>,
    /// Whether two of the view's rows may share a PK.
    pub(crate) pk_repeats: bool,
    /// The column list of each index the copy holds: [`index_lists`]' order.
    pub(crate) indexes: Vec<PkColList>,
    pub(crate) feed: Feed,
}

/// Where a copy's feed got to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Feed {
    /// The copy holds no value: it is not valid to read.
    Empty,
    /// The copy is being filled with its view's whole value, and is not valid
    /// to read until it is sealed. Persisted as [`Feed::Empty`].
    Filling,
    /// The copy is the view's value at this round.
    At(DeltaCursor),
}

impl Feed {
    /// The round the copy answers reads at, if it answers any.
    pub(crate) fn cursor(self) -> Option<DeltaCursor> {
        match self {
            Feed::At(cursor) => Some(cursor),
            Feed::Empty | Feed::Filling => None,
        }
    }
}

impl MirrorRecord {
    /// The bytes a copy's manifest carries for it.
    pub(crate) fn encode(&self) -> Vec<u8> {
        // Round 0 is no cursor here as on the wire, where it asks for a bootstrap.
        let mut w = Writer::new();
        DeltaCursor::write_opt(&mut w, self.feed.cursor());
        w.bytes32(self.name.schema().as_bytes())
            .bytes32(self.name.name().as_bytes())
            .bytes32(&self.schema)
            .bool(self.pk_repeats)
            .list(&self.indexes);
        w.into_vec()
    }

    /// The record and the layout its schema record describes; `None` for bytes
    /// [`Self::encode`] did not produce, or whose schema record describes no
    /// layout.
    pub(crate) fn decode(bytes: &[u8]) -> Option<(Self, SchemaDescriptor)> {
        let rec = decode_all(bytes, "mirror record", |r| {
            let feed = DeltaCursor::read_opt(r)?.map_or(Feed::Empty, Feed::At);
            let name = RelName::new(r.str32()?, r.str32()?)?;
            let schema = r.bytes32()?.to_vec();
            let pk_repeats = r.bool()?;
            let indexes: Vec<PkColList> = r.list("index list", MAX_LIST_LEN)?;
            // Canonical, so a record holds no index twice.
            if !indexes.is_sorted_by(|a, b| a < b) {
                return Err("index column lists out of order".into());
            }
            Ok(MirrorRecord { feed, name, schema, pk_repeats, indexes })
        })
        .ok()?;
        let layout = decode_schema_record(&rec.schema).ok()?;
        Some((rec, layout))
    }
}

/// The column list of each index `desc` names, ascending, none twice.
pub(crate) fn index_lists(desc: &RelDescriptor) -> Vec<PkColList> {
    let mut lists: Vec<PkColList> = desc.indexes.iter().map(|ix| ix.cols).collect();
    lists.sort_unstable();
    lists.dedup();
    lists
}

#[cfg(test)]
#[path = "tests/record.rs"]
mod tests;
