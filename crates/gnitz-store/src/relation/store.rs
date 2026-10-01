//! `Store` — this process's store of one relation or index, or its absence.
//! Every row verb is the `Table`'s own, reached through [`Store::held`].

use crate::storage::Table;
use gnitz_zset::repr::StorageError;
use gnitz_zset::schema::SchemaDescriptor;

/// This process's store of one relation or index.
pub(crate) enum Store {
    Held(Box<Table>),
    /// No store in this process; the schema stays readable.
    Absent(Box<SchemaDescriptor>),
}

impl Store {
    /// The schema this store's rows are read in.
    pub(crate) fn schema(&self) -> &SchemaDescriptor {
        match self {
            Store::Held(t) => t.schema(),
            Store::Absent(s) => s,
        }
    }

    /// Publish a new schema for this store, down into a held `Table`, which
    /// rebinds its shards if the region count grew.
    pub(crate) fn swap_schema(&mut self, schema: SchemaDescriptor) -> Result<(), StorageError> {
        match self {
            Store::Held(t) => t.swap_schema(schema),
            Store::Absent(s) => {
                **s = schema;
                Ok(())
            }
        }
    }

    /// The `Table` this process holds; panics on [`Store::Absent`].
    #[track_caller]
    pub(crate) fn held(&self) -> &Table {
        match self {
            Store::Held(t) => t,
            Store::Absent(_) => not_held(),
        }
    }

    /// [`Self::held`] as `&mut`.
    #[track_caller]
    pub(crate) fn held_mut(&mut self) -> &mut Table {
        match self {
            Store::Held(t) => t,
            Store::Absent(_) => not_held(),
        }
    }

    /// The `Table`, if this process holds one.
    pub(in crate::relation) fn table_mut(&mut self) -> Option<&mut Table> {
        match self {
            Store::Held(t) => Some(t),
            Store::Absent(_) => None,
        }
    }
}

#[cold]
#[track_caller]
fn not_held() -> ! {
    panic!("relation store is not held by this process")
}
