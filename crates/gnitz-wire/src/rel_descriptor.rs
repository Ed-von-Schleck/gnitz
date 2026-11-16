//! The relation-descriptor blob a RESOLVE reply carries: the relation's class,
//! keys and indexes, and its schema record.

use crate::codec::{MAX_LIST_LEN, Reader, Wire, Writer, decode_all};
use crate::{PkColList, SysFamily, ViewBudget};

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
        /// A system family: read, and scanned by a view, and written by a DDL alone.
        Catalog = 5,
    }
}

impl RelClass {
    /// What to call this relation in a message to the user.
    pub fn noun(self) -> &'static str {
        match self {
            RelClass::Table => "table",
            RelClass::Stream => "stream",
            RelClass::Catalog => "system table",
            RelClass::View | RelClass::BoundedView | RelClass::FedView => "view",
        }
    }

    /// [`Self::noun`], naming a view's budget too.
    pub fn describe(self) -> &'static str {
        match self {
            RelClass::BoundedView => "capacity-bounded view",
            RelClass::FedView => "view with a delta feed",
            RelClass::Table | RelClass::Stream | RelClass::View | RelClass::Catalog => self.noun(),
        }
    }

    pub fn is_view(self) -> bool {
        matches!(self, RelClass::View | RelClass::BoundedView | RelClass::FedView)
    }

    /// The system family this relation's row lives in.
    pub fn family(self) -> SysFamily {
        if self.is_view() {
            SysFamily::View
        } else {
            SysFamily::Table
        }
    }
}

impl From<ViewBudget> for RelClass {
    fn from(props: ViewBudget) -> RelClass {
        match props {
            ViewBudget::Plain => RelClass::View,
            ViewBudget::Bounded { .. } => RelClass::BoundedView,
            ViewBudget::Fed { .. } => RelClass::FedView,
        }
    }
}

/// One secondary index: its full declared column list and whether it is unique.
#[derive(Copy, Clone, PartialEq, Eq, Debug)]
pub struct RelIndex {
    pub cols: PkColList,
    pub is_unique: bool,
}

impl Wire for RelIndex {
    fn write(&self, w: &mut Writer) {
        w.put(&self.cols).bool(self.is_unique);
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        Ok(RelIndex { cols: r.get()?, is_unique: r.bool()? })
    }
}

#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct RelDescriptorBlob {
    pub class: RelClass,
    /// Whether two rows may share a PK, or one may stand at weight above 1.
    pub pk_repeats: bool,
    /// [`crate::TableProps::serial`].
    pub serial: bool,
    pub indexes: Vec<RelIndex>,
    /// The relation's [`crate::schema_record`], names included.
    pub schema: Box<[u8]>,
}

impl RelDescriptorBlob {
    pub fn encode(&self) -> Vec<u8> {
        let mut w = Writer::new();
        w.put(&self.class)
            .bool(self.pk_repeats)
            .bool(self.serial)
            .list(&self.indexes)
            .bytes32(&self.schema);
        w.into_vec()
    }

    pub fn decode(buf: &[u8]) -> Result<Self, String> {
        decode_all(buf, "rel descriptor", |r| {
            let class = r.get()?;
            let pk_repeats = r.bool()?;
            let serial = r.bool()?;
            let indexes = r.list("index list", MAX_LIST_LEN)?;
            let schema = r.bytes32()?.into();
            Ok(RelDescriptorBlob {
                class,
                pk_repeats,
                serial,
                indexes,
                schema,
            })
        })
    }
}

#[cfg(test)]
#[path = "tests/rel_descriptor.rs"]
mod tests;
