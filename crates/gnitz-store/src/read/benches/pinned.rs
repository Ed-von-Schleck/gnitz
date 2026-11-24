//! A walk of a pinned store, page by page, in instructions per row and per page.
//!
//! `cd crates && cargo test -p gnitz-store --release page_bench -- --ignored --nocapture --test-threads=1`

use std::hint::black_box;

use crate::relation::RelationKind;
use crate::test_support::{TID, make_batch, make_schema_u64_i64, relation_fixture, table_shards};
use gnitz_wire::PkBuf;
use gnitz_wire::snapshot::{MAX_PAGE_ROWS, PageRequest};

/// Rows the store holds.
const ROWS: u64 = 1 << 20;
/// Shards the store holds, each a checkpoint of PK-interleaved rows, so every
/// page merges them all.
const SHARDS: u64 = 4;

#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn page_bench() {
    let schema = make_schema_u64_i64();
    let mut r = relation_fixture(RelationKind::BaseTable, schema, &[], []);
    for round in 0..SHARDS {
        let rows: Vec<_> = (round..ROWS)
            .step_by(SHARDS as usize)
            .map(|k| (k, 1, k as i64))
            .collect();
        r.ingest(TID, make_batch(&schema, &rows)).unwrap();
        r.checkpoint_base().unwrap();
    }
    assert!(
        table_shards(r.relation(TID).unwrap().table()).len() > 1,
        "premise: a merge of shards"
    );
    let pin = r.pin();
    let counter = gnitz_foundation::perf::Counter::instructions();
    for limit in [1024, 65_536, MAX_PAGE_ROWS] {
        // Every page of the store, each after the last one's greatest key.
        let walk = || {
            let (mut pages, mut rows, mut after) = (0u64, 0u64, None::<PkBuf>);
            loop {
                let req = PageRequest {
                    layout: schema.layout_digest(),
                    limit,
                    after: vec![after],
                };
                let page = pin.page(TID, &req).unwrap();
                (pages, rows) = (pages + 1, rows + page.len() as u64);
                if (page.len() as u64) < limit {
                    return (pages, rows);
                }
                after = Some(PkBuf::from_bytes(page.get_pk_bytes(page.len() - 1)));
            }
        };
        // Untimed: the batch pool is warm.
        black_box(walk());
        let ((pages, rows), instructions) = counter.measure(walk);
        assert_eq!(rows, ROWS, "limit {limit}: rows");
        println!(
            "page_bench limit {limit:>8} {pages:>5} pages {:>9.1} instr/row {:>12.0} instr/page",
            instructions as f64 / rows as f64,
            instructions as f64 / pages as f64
        );
    }
}
