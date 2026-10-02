use super::*;
use crate::catalog::{CatalogColumn, SysFamily};
use crate::runtime::sal::fixtures::TestLog;
use crate::test_support::{
    circuit_batch, col_def, col_tab_batch, identity_circuit, push_view_tab_row, sum_weights, table_tab_batch,
};
use gnitz_wire::TypeCode;
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::Placement;

/// Which of a group's written slots this rank replays. A wrong range silently
/// loses or doubles ACKed rows, so every case is pinned.
#[test]
fn replay_slots_covers_every_width_and_placement() {
    /// `written` slots on disk, this process being rank `rank` of `of`.
    fn check(written: u32, rank: u32, of: u32, replicated: bool, want: Range<u32>, want_reslice: bool) {
        let (range, reslice) = replay_slots(written, Slot::new(rank, of), replicated);
        assert_eq!(
            (range, reslice),
            (want, want_reslice),
            "written={written} rank={rank} of={of} replicated={replicated}"
        );
    }

    // Written at the launched width: this rank's own slot, whether it holds its
    // share of a partitioned group or a whole replicated copy.
    check(4, 2, 4, false, 2..3, false);
    check(4, 2, 4, true, 2..3, false);
    check(1, 0, 1, false, 0..1, false);

    // Another width, replicated: every slot is the same whole copy, so exactly
    // one is read and never re-cut.
    check(4, 1, 2, true, 0..1, false);
    check(2, 1, 4, true, 0..1, false);

    // Another width, partitioned: no slot holds this rank's rows, so every
    // written slot is walked and re-cut.
    check(4, 1, 2, false, 0..4, true);
    check(2, 3, 4, false, 0..2, true);
}

// -- Boot staging -----------------------------------------------------------

const SAL_SIZE: usize = 1 << 20;

fn cols() -> Vec<CatalogColumn> {
    vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)]
}

fn col_tab(owner: u64, weight: i64) -> Batch {
    col_tab_batch(owner, &cols(), weight)
}

fn table_tab(tid: u64, weight: i64) -> Batch {
    table_tab_batch(&[(tid, "t", weight)])
}

/// Boot the catalog at `dir` over `log`'s committed tail.
fn recover(log: &TestLog, dir: &str) -> CatalogEngine {
    let mut opened = CatalogEngine::open_master(dir, 1).unwrap();
    stage_system_tail(CommittedTail::read(log.log()).unwrap(), &mut opened).unwrap();
    opened.replay().unwrap()
}

/// The net weight `family`'s store holds.
fn net(engine: &CatalogEngine, family: SysFamily) -> i64 {
    sum_weights(engine.registry.relation(family.id()).unwrap().cursor())
}

/// A view's VIEW_TAB and COL_TAB rows flushed, its circuit only in the SAL.
#[test]
fn a_circuit_behind_its_flushed_view_registers_as_a_clean_boot_does() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    let replicated = gnitz_wire::TableProps {
        distribution: gnitz_wire::TableDistribution::Replicated,
        ..Default::default()
    };
    let r = engine.create_table_with("public.r", &cols(), &[0], replicated).unwrap();
    let v = r + 1;
    let mut view_tab = BatchBuilder::new(SysFamily::View.schema());
    push_view_tab_row(&mut view_tab, 1, v, "v", 0, 0, 0);
    engine.registry.ingest(SysFamily::Column.id(), col_tab(v, 1)).unwrap();
    engine.registry.ingest(SysFamily::View.id(), view_tab.finish()).unwrap();
    engine.close();

    let log = TestLog::new(SAL_SIZE, 1, 1);
    let circuit = circuit_batch(v, &identity_circuit(r, gnitz_wire::ReadBound::None));
    log.ddl_zone(&[(SysFamily::Circuit, &circuit)]);
    let engine = recover(&log, dir);

    assert_eq!(engine.dag.sources_of(v), &[r][..]);
    assert_eq!(engine.registry.relation(v).unwrap().placement(), Placement::Replicated);
    assert_eq!(net(&engine, SysFamily::View), 1);
}

/// A table's create flushed without the id counter, its drop only in the SAL.
/// A second boot over the same tail — a crash between the boot flush and the
/// SAL reset — finds the drop already flushed and stages it no second time.
#[test]
fn a_tail_drop_nets_out_once_and_its_id_is_not_reissued() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_str().unwrap();
    let mut engine = CatalogEngine::open(dir, 1).unwrap();
    // Above the id counter this session flushes.
    let t: u64 = 10_000;
    engine.registry.ingest(SysFamily::Column.id(), col_tab(t, 1)).unwrap();
    engine.registry.ingest(SysFamily::Table.id(), table_tab(t, 1)).unwrap();
    engine.close();

    let log = TestLog::new(SAL_SIZE, 1, 1);
    log.ddl_zone(&[
        (SysFamily::Table, &table_tab(t, -1)),
        (SysFamily::Column, &col_tab(t, -1)),
    ]);
    let mut engine = recover(&log, dir);
    assert!(engine.registry.relation(t).is_none(), "the drop nets the table out");
    assert_eq!(engine.allocate_ids(1).unwrap(), t + 1);
    let flushed = [net(&engine, SysFamily::Table), net(&engine, SysFamily::Column)];
    engine.close();

    let engine = recover(&log, dir);
    assert_eq!(
        [net(&engine, SysFamily::Table), net(&engine, SysFamily::Column)],
        flushed
    );
}
