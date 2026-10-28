//! What this process holds of one relation ([`Rows`]) or of one of its indexes
//! ([`Store`]). Every row verb is the `Table`'s own, reached through `held`.

use super::feed::Feed;
use crate::storage::Table;
use gnitz_zset::repr::{Batch, StorageError};
use gnitz_zset::schema::SchemaDescriptor;

/// This process's rows of one relation.
// One per registered relation, so a stream's pending batch is held inline: boxing
// it would cost an allocation per seal cycle.
#[allow(clippy::large_enum_variant)]
pub(super) enum Rows {
    /// A table's or view's store, and the delta feed this process serves for a
    /// fed view.
    Stored { store: Store, feed: Option<Feed> },
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
            Rows::Stored { store, .. } => store.schema(),
            Rows::Stream { schema, .. } => schema,
        }
    }

    /// Publish a new schema for these rows, down into a held `Table`, which
    /// rebinds its shards to it if it changed.
    pub(super) fn swap_schema(&mut self, new: SchemaDescriptor) -> Result<(), StorageError> {
        match self {
            Rows::Stored { store, .. } => store.swap_schema(new),
            Rows::Stream { schema, .. } => {
                **schema = new;
                Ok(())
            }
        }
    }

    /// The `Table`, if this process holds one.
    pub(super) fn table(&self) -> Option<&Table> {
        match self {
            Rows::Stored { store, .. } => store.table(),
            Rows::Stream { .. } => None,
        }
    }

    /// [`Self::table`] as `&mut`.
    pub(super) fn table_mut(&mut self) -> Option<&mut Table> {
        match self {
            Rows::Stored { store, .. } => store.table_mut(),
            Rows::Stream { .. } => None,
        }
    }

    /// The `Table` this process holds; panics when it holds none.
    #[track_caller]
    pub(super) fn held(&self) -> &Table {
        match self.table() {
            Some(t) => t,
            None => not_held(),
        }
    }

    /// [`Self::held`] as `&mut`.
    #[track_caller]
    pub(super) fn held_mut(&mut self) -> &mut Table {
        match self.table_mut() {
            Some(t) => t,
            None => not_held(),
        }
    }

    /// The delta feed this process serves, if any.
    pub(super) fn feed(&self) -> Option<&Feed> {
        match self {
            Rows::Stored { feed, .. } => feed.as_ref(),
            Rows::Stream { .. } => None,
        }
    }
}

/// This process's store of one relation's rows or of one secondary index.
pub(super) enum Store {
    Held(Box<Table>),
    /// No store in this process (a user relation's, on the master); the schema
    /// stays readable.
    Absent(Box<SchemaDescriptor>),
}

impl Store {
    /// The schema this store's rows are read in.
    pub(super) fn schema(&self) -> &SchemaDescriptor {
        match self {
            Store::Held(t) => t.schema(),
            Store::Absent(s) => s,
        }
    }

    /// Publish a new schema for this store, down into a held `Table`, which
    /// rebinds its shards to it if it changed.
    fn swap_schema(&mut self, schema: SchemaDescriptor) -> Result<(), StorageError> {
        match self {
            Store::Held(t) => t.swap_schema(schema),
            Store::Absent(s) => {
                **s = schema;
                Ok(())
            }
        }
    }

    /// The `Table`, if this process holds one.
    pub(super) fn table(&self) -> Option<&Table> {
        match self {
            Store::Held(t) => Some(t),
            Store::Absent(_) => None,
        }
    }

    /// [`Self::table`] as `&mut`.
    pub(super) fn table_mut(&mut self) -> Option<&mut Table> {
        match self {
            Store::Held(t) => Some(t),
            Store::Absent(_) => None,
        }
    }

    /// The `Table` this process holds; panics on [`Store::Absent`].
    #[track_caller]
    pub(super) fn held(&self) -> &Table {
        match self.table() {
            Some(t) => t,
            None => not_held(),
        }
    }

    /// [`Self::held`] as `&mut`.
    #[track_caller]
    pub(super) fn held_mut(&mut self) -> &mut Table {
        match self.table_mut() {
            Some(t) => t,
            None => not_held(),
        }
    }
}

#[cold]
#[track_caller]
fn not_held() -> ! {
    panic!("relation store is not held by this process")
}
