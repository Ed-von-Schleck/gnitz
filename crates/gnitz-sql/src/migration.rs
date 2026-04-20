//! Client-side re-export of the pure SQL→DesiredState parser.
//!
//! The parser itself lives in `gnitz-sql-parse` (no client dep) so
//! `gnitz-engine` can link it without pulling in `GnitzClient`. This
//! module preserves the existing path (`gnitz_sql::migration::...`)
//! for any client-side consumer.

pub use gnitz_sql_parse::migration::{
    canonicalize, decanonicalize, compute_migration_hash, diff_by_name,
    topo_sort_diff, validate_drop_closure,
    parse_desired_state,
    ColumnDef, DesiredState, Diff, IndexDef, TableDef, ViewDef, ViewDep,
};
