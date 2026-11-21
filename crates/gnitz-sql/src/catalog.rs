//! The relations a statement's plan reads, as every planner probes them.

use crate::error::{GnitzSqlError, missing_relation};
use gnitz_core::{RelDescriptor, RelName};
use std::sync::Arc;

/// The relations a statement's plan reads.
pub(crate) trait Catalog {
    /// The session's schema.
    fn schema_name(&self) -> &str;

    /// The relation `name` resolves to, `None` for a free name.
    fn probe(&self, name: &RelName) -> Option<Arc<RelDescriptor>>;

    /// The relation `name` a statement reads: [`Self::probe`], with absence an error.
    fn probe_relation(&self, name: &RelName) -> Result<Arc<RelDescriptor>, GnitzSqlError> {
        self.probe(name).ok_or_else(|| missing_relation(name))
    }
}
