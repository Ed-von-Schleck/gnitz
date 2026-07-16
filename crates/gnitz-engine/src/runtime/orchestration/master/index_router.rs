//! Master-side unique-index routing cache: `PartitionRouter` maps
//! (table_id, col_idx, key) → worker, recorded on commit and read to unicast
//! integer unique-index seeks instead of broadcasting. `extract_col_key`
//! (batch-row side) and `index_route_key` (native-seek side) are the two halves
//! of one key image and MUST byte-agree — both yield the same
//! `widen_pk_be(encode_pk_column(native))` OPK image via the shared `schema`
//! route-key encoders, so equal logical values co-route. STRING/BLOB never reach
//! here: `index_key_type` rejects them as secondary-index columns at circuit
//! registration, so no string-keyed circuit exists to record or seek. Keeping the
//! pair and its pinning tests in one module is what stops them drifting apart.

use rustc_hash::FxHashMap;

use crate::schema::{ColumnLocator, SchemaDescriptor};
use crate::storage::{Batch, MemBatch};

/// Index-routing-cache key for the resolved column `loc` at `row`: a PK column
/// widens its OPK bytes; an integer payload column OPK-encodes then widens
/// (sign-flipping signed columns) — the shared `route_key` image, so equal
/// logical values route as `partition_for_pk_bytes` does and `index_route_key`
/// rebuilds the same key from a native seek value. A NULL (nullable integer
/// column) shares key 0.
fn extract_col_key(loc: ColumnLocator, mb: &MemBatch<'_>, row: usize) -> u128 {
    debug_assert!(
        !gnitz_wire::is_german_string(loc.type_code()),
        "extract_col_key: STRING/BLOB can't be a secondary-index column (index_key_type rejects them)"
    );
    // NULL routes to key 0. PK columns are never null, so this only fires for a
    // nullable payload column.
    if loc.is_null(mb, row) {
        return 0u128;
    }
    loc.route_key(mb, row)
}

// ---------------------------------------------------------------------------
// Partition routing cache
// ---------------------------------------------------------------------------

/// Master-side routing cache: maps (table_id, col_idx, key) → worker.
///
/// Populated by `record_routing_from_source` after repartitioning single-column
/// unique-indexed columns. Lets the master unicast index seeks instead of
/// broadcasting on cache hit.
pub(super) struct PartitionRouter {
    index_routing: FxHashMap<(u32, u32, u128), u32>,
}

impl PartitionRouter {
    pub(super) fn new() -> Self {
        PartitionRouter {
            index_routing: FxHashMap::default(),
        }
    }

    /// Returns the worker for a given index key, or -1 on cache miss.
    pub(super) fn worker_for_index_key(&self, tid: u32, col_idx: u32, key: u128) -> i32 {
        match self.index_routing.get(&(tid, col_idx, key)) {
            Some(&w) => w as i32,
            None => -1,
        }
    }

    /// Iterate `indices` into `source`, recording (non-negative weight) or
    /// retracting (negative weight) each row's index key → worker mapping. Used
    /// by the scatter-to-wire path, which never materialises per-worker Batch
    /// objects.
    pub(super) fn record_routing_from_source(
        &mut self,
        source: &Batch,
        indices: &[u32],
        schema: &SchemaDescriptor,
        tid: u32,
        col_idx: u32,
        worker: u32,
    ) {
        let mb = source.as_mem_batch();
        // `locate` is loop-invariant (it depends only on `schema`/`col_idx`), so
        // resolve the column once rather than per row.
        let loc = schema.locate(col_idx as usize);
        for &idx in indices {
            let row = idx as usize;
            let weight = source.get_weight(row);
            let key = extract_col_key(loc, &mb, row);
            let map_key = (tid, col_idx, key);
            if weight < 0 {
                self.index_routing.remove(&map_key);
            } else {
                self.index_routing.insert(map_key, worker);
            }
        }
    }
}

/// Encode a native index-seek key into the routing-cache image `extract_col_key`
/// stores. Delegates to the shared `schema::payload_route_key` encoder — the same
/// one `extract_col_key`'s `route_key` bottoms out in — so the seek and record
/// halves agree by construction. Returns `None` for STRING/BLOB: they can't be
/// secondary-index columns (`index_key_type` rejects them at registration), so no
/// such seek ever probes the cache.
pub(super) fn index_route_key(schema: &SchemaDescriptor, col_idx: u32, native: u128) -> Option<u128> {
    use crate::schema::type_code;
    let col = schema.columns[col_idx as usize];
    match col.type_code {
        type_code::STRING | type_code::BLOB => None,
        _ => Some(crate::schema::payload_route_key(
            &native.to_le_bytes(),
            0,
            col.size() as usize,
            col.type_code,
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{type_code, SchemaColumn};
    use crate::test_support::{make_batch, make_schema_u64_i64};

    #[test]
    fn partition_router_basic() {
        let schema = make_schema_u64_i64();
        let mut router = PartitionRouter::new();

        let b = make_batch(&schema, &[(42, 1, 0)]);
        router.record_routing_from_source(&b, &[0], &schema, 1, 0, 3);
        assert_eq!(router.worker_for_index_key(1, 0, 42), 3);
        assert_eq!(router.worker_for_index_key(1, 0, 99), -1);

        // Retract: negative weight removes the entry.
        let b2 = make_batch(&schema, &[(42, -1, 0)]);
        router.record_routing_from_source(&b2, &[0], &schema, 1, 0, 3);
        assert_eq!(router.worker_for_index_key(1, 0, 42), -1);
    }

    #[test]
    fn index_route_key_hits_signed_routing_cache() {
        // The cache stores `extract_col_key` (OPK-widened) keys; a seek arrives with
        // the native value. For a signed column the two differ, so a raw native
        // query misses and `index_route_key` must transform it to hit.
        let schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::I64, 0)], &[0]);
        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&(-7i64).to_le_bytes(), type_code::I64, &mut opk);

        // Single I64 PK column, no payload: one row is PK bytes + weight + null word.
        let mut batch = Batch::with_schema(schema, 1);
        batch.extend_pk_bytes(&opk);
        batch.extend_weight(&1i64.to_le_bytes());
        batch.extend_null_bmp(&0u64.to_le_bytes());
        batch.count += 1;

        let mut router = PartitionRouter::new();
        router.record_routing_from_source(&batch, &[0], &schema, 1, 0, 2);

        let native = (-7i64) as u64 as u128;
        // Raw native query misses: native != OPK-widened for a signed column.
        assert_eq!(router.worker_for_index_key(1, 0, native), -1);
        // Transformed query hits.
        let rk = index_route_key(&schema, 0, native).expect("integer column has a route key");
        assert_eq!(router.worker_for_index_key(1, 0, rk), 2);
    }
}
