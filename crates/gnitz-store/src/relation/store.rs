//! What this process holds of one relation's rows ([`Rows`]). Every row verb is
//! the `Table`'s own, reached through `Relation::table`.

use super::feed::Feed;
use crate::storage::Table;
use gnitz_zset::repr::{Batch, StorageError};
use gnitz_zset::schema::SchemaDescriptor;

/// This process's rows of one relation.
// One per registered relation, so a stream's pending batch is held inline: boxing
// it would cost an allocation per seal cycle.
#[allow(clippy::large_enum_variant)]
pub(super) enum Rows {
    /// This process's store of a table's or view's rows, and the delta feed it
    /// serves for a fed view.
    Held { table: Box<Table>, feed: Option<Feed> },
    /// A user relation on the master, which opens no user store: its schema
    /// stays readable.
    Absent(Box<SchemaDescriptor>),
    /// A stream, on every process: no rows, only its pushes since the last seal,
    /// held as they arrived, since nothing reads them before it.
    Stream {
        schema: Box<SchemaDescriptor>,
        unsealed: Option<Batch>,
    },
}

impl Rows {
    /// The schema these rows are read in.
    pub(super) fn schema(&self) -> &SchemaDescriptor {
        match self {
            Rows::Held { table, .. } => table.schema(),
            Rows::Absent(schema) | Rows::Stream { schema, .. } => schema,
        }
    }

    /// Publish a new schema for a base table's rows, down into a held `Table`,
    /// which rebinds its shards to it if it changed.
    pub(super) fn swap_schema(&mut self, new: SchemaDescriptor) -> Result<(), StorageError> {
        match self {
            Rows::Held { table, .. } => table.swap_schema(new),
            Rows::Absent(schema) => {
                **schema = new;
                Ok(())
            }
            Rows::Stream { .. } => unreachable!("only a base table's columns change"),
        }
    }

    /// The `Table`, if this process holds one.
    pub(super) fn table(&self) -> Option<&Table> {
        match self {
            Rows::Held { table, .. } => Some(table),
            Rows::Absent(_) | Rows::Stream { .. } => None,
        }
    }

    /// [`Self::table`] as `&mut`.
    pub(super) fn table_mut(&mut self) -> Option<&mut Table> {
        match self {
            Rows::Held { table, .. } => Some(table),
            Rows::Absent(_) | Rows::Stream { .. } => None,
        }
    }

    /// The delta feed this process serves, if any.
    pub(super) fn feed(&self) -> Option<&Feed> {
        match self {
            Rows::Held { feed, .. } => feed.as_ref(),
            Rows::Absent(_) | Rows::Stream { .. } => None,
        }
    }
}

#[cold]
#[track_caller]
pub(super) fn not_held() -> ! {
    panic!("relation store is not held by this process")
}
