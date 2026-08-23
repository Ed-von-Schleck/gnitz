//! The read seam an ad-hoc SELECT is planned against.
//!
//! [`GnitzClient`] implements it by delegating to what it already does, so the
//! planner reaching a server is the default and needs nothing new. What the
//! trait buys is the other implementor: a local copy of a relation that answers
//! the same calls without a socket.
//!
//! **It is declared here, and not in `gnitz-sql`, so an implementor need not
//! link the planner.** Every type in its signatures is already `gnitz-core`'s
//! own or a `gnitz-wire` re-export, so this adds no dependency in either
//! direction and `gnitz-core` stays free of any engine.
//!
//! **It is a trait rather than a hook inside [`GnitzClient`] because
//! [`GnitzClient`] is `Send` and must stay so.** `gnitz-py` runs the planner
//! inside `Python::detach`, whose closure is `Send`-bound, and an implementor
//! backed by an engine is `!Send`. A boxed field would make *every* client
//! `!Send`, including the remote-only ones; behind a `dyn` the caller holds,
//! the `!Send` half never touches [`GnitzClient`].

use std::sync::Arc;

use crate::client::{IndexMeta, RelKind};
use crate::error::ClientError;
use crate::protocol::{Schema, ZSetBatch};

/// What an ad-hoc SELECT needs of the relation it reads: one relation, resolved
/// by name and then read whole or through a bound.
///
/// Writes are deliberately absent. An implementor answers reads only; a host
/// that also writes keeps a [`GnitzClient`] for that.
pub trait ReadTarget {
    /// Open a statement scope. Both resolving methods below populate and read it
    /// — [`Self::resolve_relation`] by name, [`Self::table_indexes`] by id — so
    /// dropping the bracket costs a delegated read a second RESOLVE.
    fn begin_statement(&mut self);

    /// Close the statement scope, discarding whatever it cached.
    fn end_statement(&mut self);

    /// The relation's schema and its resolved `(id, class, delta)`.
    fn resolve_relation(&mut self, schema_name: &str, name: &str) -> Result<(Arc<Schema>, RelKind), ClientError>;

    /// Every row of the relation. The path a bare `SELECT *` with no WHERE,
    /// ORDER BY, LIMIT or OFFSET over a relation with no hidden payload column
    /// takes — it builds no `ReadSpec` at all, so this is not redundant with
    /// [`Self::scan_spec`].
    ///
    /// The schema is `None` when the reply carried none and the caller falls
    /// back to the one it resolved. A served LSN is deliberately absent: it is a
    /// server-side counter, and a copy's freshness is a feed round instead.
    fn scan(&mut self, table_id: u64) -> Result<(Option<Arc<Schema>>, Option<ZSetBatch>), ClientError>;

    /// A parameterized bounded read: `spec` is the encoded `ReadSpec` and
    /// `reply_schema` the shape the planner recorded for the result. The SQL
    /// layer applies the ORDER BY / LIMIT window and the aggregate fold to what
    /// comes back.
    fn scan_spec(
        &mut self,
        table_id: u64,
        spec: &[u8],
        reply_schema: &Schema,
    ) -> Result<Option<ZSetBatch>, ClientError>;

    /// The relation's secondary indexes, for the planner's index probe.
    fn table_indexes(&mut self, table_id: u64) -> Result<Arc<Vec<IndexMeta>>, ClientError>;
}
