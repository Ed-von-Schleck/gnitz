//! An index fill over a base table on disk, in instructions per row.
//!
//! `cd crates && cargo test -p gnitz-store --release index_fill_bench -- --ignored --nocapture --test-threads=1`

use crate::test_support::{wide_on_disk, TID};
use gnitz_wire::PkColList;

use super::*;

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn index_fill_bench() {
    let counter = gnitz_foundation::perf::Counter::instructions();
    for shards in [1, 3] {
        for (what, cols) in [("I64 column", &[1u32][..]), ("two I64 columns", &[1, 3])] {
            let mut r = wide_on_disk(RelationKind::BaseTable, 3 << 17, shards);
            let rows = r.relation(TID).unwrap().table().estimated_rows();
            let claim = (
                IndexClaim::Index { id: TID + 1, unique: false },
                PkColList::from_slice(cols),
            );
            let (filled, instructions) = counter.measure(|| r.add_indexes(TID, &[claim]));
            filled.unwrap();
            println!(
                "index_fill_bench index on {what:<16} {shards} shard(s) {rows:>7} rows {:>7.1} instr/row",
                instructions as f64 / rows as f64
            );
        }
    }
}
