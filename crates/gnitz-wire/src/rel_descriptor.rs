//! The relation-descriptor blob a RESOLVE reply carries beside the schema block.

use crate::codec::{decode_all, Writer};
use crate::{PkColList, PkListRole, ViewProps};

wire_enum! {
    /// What a relation is, as the client sees it.
    #[derive(Default)]
    pub enum RelClass: u8 {
        #[default]
        Table = 0,
        /// A storeless, append-only ingestion point: it holds no rows to read.
        Stream = 1,
        View = 2,
        /// A view created `WITH (capacity = …)`.
        BoundedView = 3,
        /// A view created `WITH (delta = …)`.
        FedView = 4,
    }
}

impl RelClass {
    /// What to call this relation in a message to the user.
    pub fn noun(self) -> &'static str {
        match self {
            RelClass::Table => "table",
            RelClass::Stream => "stream",
            RelClass::View | RelClass::BoundedView | RelClass::FedView => "view",
        }
    }

    pub fn is_view(self) -> bool {
        matches!(self, RelClass::View | RelClass::BoundedView | RelClass::FedView)
    }
}

impl From<ViewProps> for RelClass {
    fn from(props: ViewProps) -> RelClass {
        match props {
            ViewProps::Plain => RelClass::View,
            ViewProps::Bounded { .. } => RelClass::BoundedView,
            ViewProps::Fed { .. } => RelClass::FedView,
        }
    }
}

/// One secondary index: its full declared column list and whether it is unique.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct RelIndex {
    pub cols: PkColList,
    pub is_unique: bool,
}

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct RelDescriptorBlob {
    pub class: RelClass,
    /// Whether two rows may share a PK, or one may stand at weight above 1.
    pub pk_repeats: bool,
    /// [`crate::TableProps::serial`].
    pub serial: bool,
    pub indexes: Vec<RelIndex>,
}

impl RelDescriptorBlob {
    pub fn encode(&self) -> Vec<u8> {
        let index_count = u16::try_from(self.indexes.len()).expect("a relation's index count fits a u16");
        let mut w = Writer::new();
        w.put(&self.class)
            .bool(self.pk_repeats)
            .bool(self.serial)
            .u16(index_count);
        for ix in &self.indexes {
            w.u64(ix.cols.pack()).bool(ix.is_unique);
        }
        w.into_vec()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        decode_all(buf, "rel descriptor", |r| {
            let class = r.get()?;
            let pk_repeats = r.bool()?;
            let serial = r.bool()?;
            let indexes = (0..r.u16()?)
                .map(|_| {
                    let cols = PkColList::unpack(r.u64()?)
                        .map_err(|rule| format!("index {}", rule.for_role(PkListRole::ColumnList)))?;
                    let is_unique = r.bool()?;
                    Ok(RelIndex { cols, is_unique })
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(RelDescriptorBlob { class, pk_repeats, serial, indexes })
        })
    }
}

#[cfg(test)]
#[path = "tests/rel_descriptor.rs"]
mod tests;
