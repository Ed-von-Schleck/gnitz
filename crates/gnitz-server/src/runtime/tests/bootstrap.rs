use super::*;
use crate::catalog::{CatalogColumn, SysFamily};
use crate::runtime::master::scatter::with_routed;
use crate::runtime::sal::fixtures::TestLog;
use crate::runtime::sal::{DirectGroup, GroupTargets};
use crate::runtime::wire::WireSchema;
use crate::test_support::{
    circuit_batch, col_def, col_tab_batch, identity_circuit, make_batch, push_view_tab_row, sum_weights,
    table_tab_batch,
};
use gnitz_wire::TypeCode;
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::Placement;

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

// -- Tail replay --------------------------------------------------------------

/// The PKs `tid`'s store holds, and the net weight over them.
fn held(engine: &CatalogEngine, tid: u64) -> (Vec<u64>, i64) {
    let relation = engine.registry.relation(tid).unwrap();
    let mut cursor = relation.cursor();
    let mut pks = Vec::new();
    while let Some(chunk) = cursor.drain_chunk(1024) {
        pks.extend((0..chunk.len()).map(|row| u64::from_be_bytes(chunk.get_pk_bytes(row).try_into().unwrap())));
    }
    (pks, sum_weights(relation.cursor()))
}

/// A tail written at another worker count than the one launched. A replicated
/// push holds its rows once however many workers it was written for, and a
/// keyed push written at one worker is re-cut to this rank's share.
#[test]
fn a_tail_written_at_another_width_replays_for_the_launched_one() {
    const ROWS: u64 = 64;
    let tmp = tempfile::tempdir().unwrap();
    let slot = Slot::new(0, 2);
    let mut engine = CatalogEngine::open(tmp.path().to_str().unwrap(), slot.of).unwrap();
    let replicated = gnitz_wire::TableProps {
        distribution: gnitz_wire::TableDistribution::Replicated,
        ..Default::default()
    };
    let r = engine.create_table_with("public.r", &cols(), &[0], replicated).unwrap();
    let k = engine.create_table("public.k", &cols(), &[0]).unwrap();
    let keyed = engine.registry.relation(k).unwrap().placement();
    let rows: Vec<_> = (0..ROWS).map(|pk| (pk, 1, pk as i64)).collect();
    let rows = make_batch(&engine.registry.relation(k).unwrap().schema(), &rows);

    for (tid, written_at) in [(r, 4), (k, 1)] {
        let log = TestLog::new(SAL_SIZE, written_at, 1);
        let placement = engine.registry.relation(tid).unwrap().placement();
        let relation = WireSchema::from_catalog(&engine, tid);
        with_routed(&rows, placement, written_at, |data| {
            let targets = GroupTargets {
                set: data.holders(),
                ..GroupTargets::UNADDRESSED
            };
            log.commit_zone(&[DirectGroup::push(&relation, data, targets)]);
        });
        log.synced_through(log.cursor());

        let tail = CommittedTail::read(log.log()).unwrap();
        let group = tail.groups().next().expect("the push is committed");
        assert_eq!(
            (group.width() as usize, group.payloads().count()),
            (written_at, 1),
            "one payload, written for {written_at} worker(s)"
        );
        recover_from_sal(tail, slot, &[], &mut engine).unwrap();
    }

    assert_eq!(held(&engine, r), ((0..ROWS).collect(), ROWS as i64), "replicated");
    let own: Vec<u64> = (0..ROWS)
        .filter(|pk| keyed.owner(&pk.to_be_bytes(), slot.of as usize) == Some(slot.rank as usize))
        .collect();
    assert!(!own.is_empty() && own.len() < ROWS as usize, "the key space is split");
    let net = own.len() as i64;
    assert_eq!(held(&engine, k), (own, net), "keyed: this rank's share");
    engine.close();
}
