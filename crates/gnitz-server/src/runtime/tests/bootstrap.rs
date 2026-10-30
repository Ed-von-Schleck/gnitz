use super::*;
use crate::catalog::SysFamily;
use crate::runtime::master::route::with_routed;
use crate::runtime::sal::fixtures::{holders_of, test_log};
use crate::runtime::sal::SalGroup;
use crate::test_support::{
    batch_rows, circuit_batch, col_tab_batch, identity_circuit, make_batch, net_weight, register_identity_view,
    sys_batch, table_tab_batch, u64_i64_cols, view_tab_row,
};
use gnitz_wire::{TableDistribution, TableProps};
use gnitz_zset::algebra::Placement;
use gnitz_zset::repr::Batch;

const SAL_SIZE: usize = 1 << 20;

fn col_tab(owner: u64, weight: i64) -> Batch {
    col_tab_batch(owner, &u64_i64_cols(), weight)
}

fn replicated() -> TableProps {
    TableProps {
        distribution: TableDistribution::Replicated,
        ..Default::default()
    }
}

/// Boot the database at `dir` as `slot`'s worker over `log`'s committed tail:
/// the master's recovery and then the worker's, with no fork between them.
fn boot(dir: &str, slot: Slot, log: &SalWriter) -> CatalogEngine {
    let tail = CommittedTail::read(log.log()).unwrap();
    let opened = CatalogEngine::open_master(dir, slot.of, Default::default()).unwrap();
    let mut engine = master_pre_fork_recovery(opened, tail).unwrap();
    worker_boot_recovery(&mut engine, tail, slot).unwrap();
    engine
}

/// Write `rows` to `log` as the master's push of them to `tid`.
fn push(log: &SalWriter, engine: &CatalogEngine, tid: u64, rows: &Batch) {
    let (rel, record) = engine.wire_target(tid).unwrap();
    with_routed(rows, rel.placement(), log.num_workers(), |data| {
        log.commit_zone(&[SalGroup::push(tid, record, data, holders_of(&data))]);
    });
}

/// The rows `tid`'s store holds, with their weights.
fn held(engine: &CatalogEngine, tid: u64) -> Vec<(u64, i64, i64)> {
    batch_rows(&engine.registry.relation(tid).unwrap().full_scan())
}

// -- System tail ----------------------------------------------------------------

/// A view's VIEW_TAB and COL_TAB rows flushed, its circuit only in the SAL.
#[test]
fn a_circuit_behind_its_flushed_view_registers_as_a_clean_boot_does() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let r = engine
        .create_table_with("public.r", &u64_i64_cols(), &[0], replicated())
        .unwrap();
    let v = engine.allocate_ids(1).unwrap();
    engine.registry.ingest(SysFamily::Column.id(), col_tab(v, 1)).unwrap();
    engine
        .registry
        .ingest(SysFamily::View.id(), sys_batch([(view_tab_row(v, "v"), 1)]))
        .unwrap();
    engine.close();

    let log = test_log(SAL_SIZE, 1, 1);
    let circuit = circuit_batch(v, &identity_circuit(r, gnitz_wire::ReadBound::None));
    log.ddl_zone(&[(SysFamily::Circuit, &circuit)]);
    let engine = boot(dir, Slot::new(0, 1), &log);

    assert_eq!(engine.dag.sources_of(v).collect::<Vec<_>>(), [r]);
    assert_eq!(engine.registry.relation(v).unwrap().placement(), Placement::Replicated);
    assert_eq!(net_weight(&engine, SysFamily::View.id()), 1);
}

/// A table's create flushed without the id counter, its drop only in the SAL.
/// A second boot over the same tail — a crash between the boot flush and the
/// SAL reset — finds the drop already flushed and stages it no second time.
#[test]
fn a_tail_drop_nets_out_once_and_its_id_is_not_reissued() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let slot = Slot::new(0, 1);
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    // Above the id counter this session flushes.
    let t: u64 = 10_000;
    let table_tab = |weight| table_tab_batch(&[(t, "t", weight)]);
    engine.registry.ingest(SysFamily::Column.id(), col_tab(t, 1)).unwrap();
    engine.registry.ingest(SysFamily::Table.id(), table_tab(1)).unwrap();
    engine.close();

    let log = test_log(SAL_SIZE, 1, 1);
    log.ddl_zone(&[(SysFamily::Table, &table_tab(-1)), (SysFamily::Column, &col_tab(t, -1))]);
    let nets = |engine: &CatalogEngine| [SysFamily::Table, SysFamily::Column].map(|f| net_weight(engine, f.id()));
    let mut engine = boot(dir, slot, &log);
    assert!(engine.registry.relation(t).is_none(), "the drop nets the table out");
    assert_eq!(engine.allocate_ids(1).unwrap(), t + 1);
    let flushed = nets(&engine);
    drop(engine);

    let mut engine = boot(dir, slot, &log);
    assert_eq!(nets(&engine), flushed);
    assert!(engine.allocate_ids(1).unwrap() > t);
}

// -- Push tail ------------------------------------------------------------------

/// A tail written at another worker count than the one launched: a replicated
/// push holds its rows once however many workers it was written for, and a
/// keyed push written at one worker is re-cut to this rank's share. One written
/// at the launched count applies this rank's payload alone, and over a store
/// that already holds it changes nothing.
#[test]
fn a_tail_written_at_another_width_replays_for_the_launched_one() {
    const ROWS: u64 = 64;
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let slot = Slot::new(0, 2);
    let mut engine = CatalogEngine::open(dir, slot.of).unwrap();
    let r = engine
        .create_table_with("public.r", &u64_i64_cols(), &[0], replicated())
        .unwrap();
    let k = engine.create_table("public.k", &u64_i64_cols(), &[0]).unwrap();
    let keyed = engine.registry.relation(k).unwrap().placement();
    let all: Vec<_> = (0..ROWS).map(|pk| (pk, 1, pk as i64)).collect();
    let own: Vec<_> = all
        .iter()
        .copied()
        .filter(|(pk, ..)| keyed.owner(&pk.to_be_bytes(), slot.of as usize) == Some(slot.rank as usize))
        .collect();
    assert!(!own.is_empty() && own.len() < all.len(), "the key space is split");
    let rows = make_batch(&engine.registry.relation(k).unwrap().schema(), &all);
    let tails: Vec<_> = [(r, 4, &all), (k, 1, &own), (k, 2, &own)]
        .into_iter()
        .map(|(tid, written_at, expected)| {
            let log = test_log(SAL_SIZE, written_at, 1);
            push(&log, &engine, tid, &rows);
            (tid, written_at, log, expected)
        })
        .collect();
    engine.close();

    for (tid, written_at, log, expected) in &tails {
        let engine = boot(dir, slot, log);
        assert_eq!(&held(&engine, *tid), *expected, "table {tid} written at {written_at}");
    }
}

/// A replayed push lands above its base's cut, past the boot flush, only for a
/// tick to take: under a view the boot keeps, and not under one that awaits its
/// rebuild, whose backfill reads the base whole.
#[test]
fn a_tail_lands_above_the_cut_only_under_a_kept_view() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let slot = Slot::new(0, 1);
    let mut engine = CatalogEngine::open(dir, slot.of).unwrap();
    let kept = engine.create_table("public.kept", &u64_i64_cols(), &[0]).unwrap();
    let rejected = engine.create_table("public.rejected", &u64_i64_cols(), &[0]).unwrap();
    register_identity_view(&mut engine, kept, "v_kept", &u64_i64_cols());
    engine.record_topology(slot.of).unwrap();
    let g = engine.advance_durable_generation().unwrap();
    engine.flush_ephemeral_round(g).unwrap();
    // Created after the checkpoint, so the boot rebuilds it.
    register_identity_view(&mut engine, rejected, "v_rejected", &u64_i64_cols());
    let rows = [(1, 1, 10), (2, 1, 20)];
    let log = test_log(SAL_SIZE, slot.of as usize, 1);
    for tid in [kept, rejected] {
        push(
            &log,
            &engine,
            tid,
            &make_batch(&engine.registry.relation(tid).unwrap().schema(), &rows),
        );
    }
    engine.close();

    let mut engine = boot(dir, slot, &log);
    for tid in [kept, rejected] {
        assert_eq!(held(&engine, tid), rows, "table {tid}");
    }
    let above = engine
        .registry
        .seal(kept)
        .unwrap()
        .expect("the kept view's tick takes the tail");
    assert_eq!(above.len(), 2);
    assert!(engine.registry.seal(rejected).unwrap().is_none());
}

// -- The descriptor limit ---------------------------------------------------

#[test]
fn the_fd_limit_is_raised_to_the_requested_soft_ceiling() {
    use crate::runtime::test_support::{assert_child_exited_ok, fork_child};

    let soft = || {
        let mut rl: libc::rlimit = unsafe { std::mem::zeroed() };
        unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut rl) };
        rl
    };
    // A child, since the soft limit is process-wide.
    let pid = unsafe {
        fork_child(|| {
            let mut rl = soft();
            rl.rlim_cur = 64;
            assert_eq!(libc::setrlimit(libc::RLIMIT_NOFILE, &rl), 0);
            raise_fd_limit(32);
            assert_eq!(soft().rlim_cur, 64);
            raise_fd_limit(128);
            assert_eq!(soft().rlim_cur, 128.min(rl.rlim_max));
            raise_fd_limit(u64::MAX);
            assert_eq!(soft().rlim_cur, rl.rlim_max);
        })
    };
    unsafe { assert_child_exited_ok(pid) };
}
