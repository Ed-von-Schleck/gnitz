//! The shard shape a relayout writes: what its output carries, and where in the
//! level structure it registers.

use super::*;
use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
use crate::storage::BatchBuilder;

/// `(id U64 PK | x I64)`, keyed.
fn schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(type_code::U64, 0),
            SchemaColumn::new(type_code::I64, 0),
        ],
        &[0],
    )
}

/// A `w{k}of{of}` child of `rel`, opened as the durable ingest path opens one.
fn open_child(rel: &str, k: u32, of: u32, tid: u32) -> Table {
    Table::new(
        &ChildAddr::Worker { rank: k, of }.dir(rel),
        schema(),
        tid,
        RecoverySource::SalReplay,
        StoreBudgets::default(),
    )
    .unwrap()
}

/// Write the complete `of`-worker set of `rel` holding `rows`, routed the way
/// the store routes them, and publish every child — including the empty ones,
/// which is what makes the set complete.
fn seed_set(rel: &str, of: u32, tid: u32, rows: &[(u128, i64)]) {
    let s = schema();
    for k in 0..of {
        let mut t = open_child(rel, k, of, tid);
        let mine: Vec<&(u128, i64)> = rows
            .iter()
            .filter(|&&(pk, _)| {
                let opk = crate::schema::key::opk_key(&s, &pk.to_le_bytes());
                s.worker_for_pk(opk.pk_bytes(), of as usize) == k as usize
            })
            .collect();
        if !mine.is_empty() {
            let mut bb = BatchBuilder::new(s);
            for &&(pk, x) in &mine {
                bb.begin_row(pk, 1);
                bb.put_int(x as u128);
                bb.end_row();
            }
            t.ingest_owned_batch(bb.finish()).unwrap();
        }
        t.flush().unwrap();
    }
}

/// The relayout is a second shard writer, and it writes *base* shards — the one
/// kind that is point-probed, so every shard it writes must carry a PK filter.
#[test]
fn a_relayout_writes_probed_base_shards() {
    let dir = tempfile::tempdir().unwrap();
    let rel = dir.path().join("rel");
    let rel = rel.to_str().unwrap();
    let tid = 9100u32;

    let rows: Vec<(u128, i64)> = (0..40u128).map(|id| (id, id as i64)).collect();
    seed_set(rel, 1, tid, &rows);
    repartition_relation(rel, &schema(), tid, 2).unwrap();

    let census: Vec<(usize, usize)> = (0..2).map(|k| open_child(rel, k, 2, tid).pk_filter_census()).collect();
    assert!(census.iter().any(|&(n, _)| n > 0), "the relayout wrote shards");
    assert!(
        census.iter().all(|&(n, filtered)| filtered == n),
        "a relayout writes probed base shards",
    );
}

/// A relayout's output is already what a guard-partitioned level requires —
/// globally ascending, non-overlapping, consolidated, one guard key per shard —
/// so it registers at the terminal level. At L0 it would instead sit as a run
/// nothing compacts until the next spill, whose single fold would then mint an
/// `l0_run_bytes` the size of the whole child: a running max that never decays,
/// leaving the store one guard for the rest of its life.
#[test]
fn a_relayout_registers_its_shards_at_the_terminal_level() {
    let dir = tempfile::tempdir().unwrap();
    let rel = dir.path().join("rel");
    let rel = rel.to_str().unwrap();
    let tid = 9101u32;

    let rows: Vec<(u128, i64)> = (0..400u128).map(|id| (id, id as i64)).collect();
    seed_set(rel, 1, tid, &rows);
    repartition_relation(rel, &schema(), tid, 2).unwrap();

    for k in 0..2 {
        let t = open_child(rel, k, 2, tid);
        let (shards, _) = t.pk_filter_census();
        assert!(shards > 0, "child {k} holds no shard");
        // Levels are 0-based in memory, so the relayout's output is the last:
        // nothing in L0 or L1, one terminal guard per shard.
        assert_eq!(t.level_shape(), (0, [0, shards]), "child {k}: terminal placement");
    }
}
