use super::*;

/// The name an FK auto-index is created under. **Injective**, which the minting
/// site's `index_by_name` skip needs to be exact — an interpolated name is not,
/// and a collision there silently skips creating the second FK's index. The
/// leading `_` keeps it unspellable at every user surface.
pub(in crate::catalog) fn make_fk_index_name(table_id: i64, col_idx: usize) -> String {
    format!("_fk_{table_id}_{col_idx}")
}

// ---------------------------------------------------------------------------
// On-disk directory naming conventions
//
// The ones only the catalog names — schema roots, the system-catalog tree, an
// index's own directory, the pre-flight root. A *relation* directory is
// `relation::dirs`', because a mirror names one too. Below a relation directory
// the names are storage's `ChildAddr` grammar.
//
// Every builder here sits beside its parser, so the creation hooks and the
// boot-time orphan sweep (`gc_orphan_directories`) cannot disagree on a shape.
// ---------------------------------------------------------------------------

/// `<base_dir>/<schema_name>` — a schema's directory. Name-based (no id): a
/// DROP+CREATE of the same schema name reuses the path, which is why recreation
/// must cancel a pending deletion of it.
pub(in crate::catalog) fn schema_dir(base_dir: &str, schema_name: &str) -> String {
    format!("{base_dir}/{schema_name}")
}

/// `<base_dir>/_system_catalog` — the system catalog root. Not a user schema
/// directory, so the boot orphan sweep (which scans only registered schema
/// names) never reaches it.
pub(in crate::catalog) fn sys_catalog_dir(base_dir: &str) -> String {
    const SYS_CATALOG_DIRNAME: &str = "_system_catalog";
    format!("{base_dir}/{SYS_CATALOG_DIRNAME}")
}

/// `<base_dir>/_system_catalog/<family_name>` — one system family's store
/// directory.
pub(in crate::catalog) fn sys_family_dir(base_dir: &str, family_name: &str) -> String {
    format!("{}/{family_name}", sys_catalog_dir(base_dir))
}

/// `<base_dir>/<schema_name>/_preflight_<vid>` — the throwaway root the master's
/// CREATE VIEW pre-flight compiles into. Not the view's own directory:
/// `child_scratch_dir` stamps the rank into each scratch child's name and the
/// master is rank 0, so compiling in place would write worker 0's real paths.
/// Sits under a schema dir and ends in `_<digits>`, which is what lets the boot
/// orphan sweep reclaim one left by a crash mid-compile; a root elsewhere would
/// leak a directory per crash.
pub(in crate::catalog) fn preflight_dir(base_dir: &str, schema_name: &str, vid: i64) -> String {
    format!("{base_dir}/{schema_name}/_preflight_{vid}")
}

// ---------------------------------------------------------------------------
// Copy/retract helpers
// ---------------------------------------------------------------------------

/// Emit a weight=−1 batch of every live row of `table` in the OPK key range
/// `[start, end)` (`end` `None` = unbounded above). The band-bounded half of the
/// catalog's retraction pair — one owner's column records, or one view's circuit
/// rows; [`retract_pk_list`] is the key-list half. Both take
/// their bounds through `schema::key`, so no call site re-derives the OPK layout.
/// The output batch is in `rel`'s own schema — a caller cannot hand it one the
/// rows it copies are not laid out in.
pub(in crate::catalog) fn retract_key_range(rel: &Relation, start: &[u8], end: &[u8]) -> Batch {
    let (mut cursor, _) = rel.range_cursor(start, Some(end));
    // Sized off the positioned walk's own upper bound, so the appends never
    // re-grow (each growth re-copies every live byte).
    let mut batch = Batch::with_capacity(&rel.schema(), cursor.estimated_length());
    cursor.for_each_positive(|c| c.copy_current_row_into(&mut batch, -1));
    batch
}

/// The OPK image of a **single-column** native system-table PK: a U64 id. A
/// two-column key — COL_TAB's `(owner_id, col_idx)`, the circuit family's
/// `(view_id, node_id)` — has its own [`pair_opk`].
pub(in crate::catalog) fn sys_opk(schema: &SchemaDescriptor, pk: u128) -> gnitz_store::schema::key::PkBuf {
    gnitz_store::schema::key::opk_key_cols(schema, &[pk])
}

/// The OPK image of a pair-keyed system row's compound PK.
pub(in crate::catalog) fn pair_opk(
    schema: &SchemaDescriptor,
    leading: i64,
    trailing: u64,
) -> gnitz_store::schema::key::PkBuf {
    gnitz_store::schema::key::opk_key_cols(schema, &[leading as u64 as u128, trailing as u128])
}

/// Emit a weight=−1 batch of the live rows of `table` at `ids`; an id with no
/// live row contributes nothing, so a one-element list is the "retract this row,
/// or return an empty batch" case. Takes `ids` by value and sorts and dedups
/// them, because the whole list rides one forward-only cursor: strictly ascending
/// is what lets an id with no live row be settled without repositioning.
pub(in crate::catalog) fn retract_pk_list(rel: &Relation, mut ids: Vec<u128>) -> Batch {
    let schema = &rel.schema();
    ids.sort_unstable();
    ids.dedup();
    let mut batch = Batch::with_capacity(schema, ids.len());
    let (Some(&first), Some(&last)) = (ids.first(), ids.last()) else {
        return batch;
    };
    let (lo, hi) = (sys_opk(schema, first), sys_opk(schema, last));
    let mut cursor = rel.cursor_in_range(lo.pk_bytes(), Some(hi.pk_bytes()));
    for pk in ids {
        let key = sys_opk(schema, pk);
        // The weight gate rejects a tombstone an uncompacted source still holds.
        if cursor.seek_pk_group_ascending(key.pk_bytes()) && cursor.current_weight > 0 {
            cursor.copy_current_row_into(&mut batch, -1);
        }
    }
    batch
}
