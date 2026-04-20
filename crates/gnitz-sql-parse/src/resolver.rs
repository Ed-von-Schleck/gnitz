//! Abstracted catalog access used by the binder during view compile.
//!
//! The client-side path implements this in `gnitz-sql` backed by a
//! `GnitzClient`. The server-side path (`gnitz-engine::sqlc`)
//! implements it over `DesiredState` + `AllocatedIds` + the live
//! catalog's existing name→id maps — no IPC round-trip.

use gnitz_protocol::Schema;

/// Abstracted catalog resolution. Returns (table_id, schema) for an
/// unqualified or qualified name, and optionally the best secondary
/// index over a given column. Methods return `None` on miss — callers
/// surface the user-visible error.
pub trait CatalogResolver {
    /// Resolve an unqualified table-or-view name in the given default
    /// schema. Returns (object_id, declared schema).
    fn resolve_table_or_view(
        &self,
        schema: &str,
        name:   &str,
    ) -> Option<(u64, Schema)>;

    /// Look up the best secondary index (index_id, is_unique) covering
    /// `col_idx` on `table_id`, if any. Implementations may return
    /// `None` when no index exists.
    fn find_index_for_column(
        &self,
        table_id: u64,
        col_idx:  usize,
    ) -> Option<(u64, bool)>;
}
