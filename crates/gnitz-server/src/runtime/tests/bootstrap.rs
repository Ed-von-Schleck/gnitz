use super::*;
use gnitz_store::storage::Batch;

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

    // A group with nothing written reads nothing on either arm: `of >= 1`, so it
    // can never match the launched width.
    check(0, 0, 1, false, 0..0, true);
    check(0, 0, 1, true, 0..1, false);
}

// -- Boot staging -----------------------------------------------------------

mod staging {
    use super::*;
    use crate::catalog::{SysFamily, PUBLIC_SCHEMA_ID};
    use crate::runtime::sal::fixtures::TestLog;
    use crate::test_support::{
        col_def, col_tab_batch, identity_circuit, push_table_tab_row, push_view_tab_row, scratch_dir,
    };
    use gnitz_store::schema::Placement;
    use gnitz_store::storage::BatchBuilder;
    use gnitz_wire::sys_rows::write_circuit_rows;
    use gnitz_wire::TypeCode;

    const SAL_SIZE: usize = 1 << 20;
    /// Above every family's replay floor after a one-DDL session.
    const ZONE_LSN: u64 = 1_000;

    fn cols() -> Vec<crate::catalog::ColumnDef> {
        vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)]
    }

    fn col_tab(owner: u64, weight: i64) -> Batch {
        col_tab_batch(owner, &cols(), weight)
    }

    fn table_tab(tid: u64, name: &str, props: gnitz_wire::TableProps, weight: i64) -> Batch {
        let mut bb = BatchBuilder::new(*SysFamily::Table.schema());
        let pk = gnitz_wire::pack_pk_cols(&[0]);
        push_table_tab_row(&mut bb, tid, PUBLIC_SCHEMA_ID, name, pk, props.pack(), weight);
        bb.finish()
    }

    fn group(family: SysFamily, batch: &Batch) -> (u64, gnitz_store::schema::SchemaDescriptor, &Batch) {
        (family.id(), *family.schema(), batch)
    }

    /// Stage `log`'s committed tail into `opened` and replay it.
    fn recover(log: &TestLog, mut opened: UnreplayedCatalog) -> CatalogEngine {
        stage_system_tail(CommittedTail::read(log.log()).unwrap(), &mut opened).unwrap();
        opened.replay().unwrap()
    }

    /// A view's VIEW_TAB row flushed, its circuit only in the SAL.
    #[test]
    fn a_circuit_behind_its_flushed_view_registers_as_a_clean_boot_does() {
        let dir = scratch_dir("bootstrap", "circuit_behind_view");
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        let r = engine.allocate_ids(1).unwrap();
        engine.write_column_records(r, &cols()).unwrap();
        let replicated = gnitz_wire::TableProps {
            distribution: gnitz_wire::TableDistribution::Replicated,
            ..Default::default()
        };
        engine
            .submit(SysFamily::Table, table_tab(r, "r", replicated, 1))
            .unwrap();
        engine.close();

        let v = r + 1;
        let mut view_tab = BatchBuilder::new(*SysFamily::View.schema());
        push_view_tab_row(&mut view_tab, 1, v, "v", 0, 0, 0);
        let view_tab = view_tab.finish();
        let mut circuit = BatchBuilder::new(*SysFamily::CircuitNodes.schema());
        write_circuit_rows(&mut circuit, v, &identity_circuit(r, gnitz_wire::ReadBound::None));
        let circuit = circuit.finish();

        let mut opened = CatalogEngine::open_master(&dir, 1).unwrap();
        // The part of the bundle whose families published before the crash.
        opened.stage(SysFamily::Column.id(), ZONE_LSN, col_tab(v, 1)).unwrap();
        opened
            .stage(SysFamily::View.id(), ZONE_LSN, Batch::clone(&view_tab))
            .unwrap();
        let log = TestLog::new(SAL_SIZE, 1, 1);
        let columns = col_tab(v, 1);
        log.ddl_zone(
            ZONE_LSN,
            &[
                group(SysFamily::Column, &columns),
                group(SysFamily::CircuitNodes, &circuit),
                group(SysFamily::View, &view_tab),
            ],
        );
        let engine = recover(&log, opened);

        assert_eq!(engine.dag.sources_of(v), &[r][..]);
        assert_eq!(
            engine.registry.relation(v).unwrap().schema().placement(),
            Placement::Replicated
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A table's create flushed without the id counter, its drop only in the SAL.
    #[test]
    fn an_id_dropped_in_the_tail_is_not_reissued() {
        let dir = scratch_dir("bootstrap", "tail_id_not_reissued");
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        // Above the id counter this session flushes.
        let t: u64 = 10_000;
        engine.registry.ingest(SysFamily::Column.id(), col_tab(t, 1)).unwrap();
        let created = table_tab(t, "t", Default::default(), 1);
        engine.registry.ingest(SysFamily::Table.id(), created).unwrap();
        engine.close();

        let (columns, table) = (col_tab(t, -1), table_tab(t, "t", Default::default(), -1));
        let log = TestLog::new(SAL_SIZE, 1, 1);
        log.ddl_zone(
            ZONE_LSN,
            &[group(SysFamily::Table, &table), group(SysFamily::Column, &columns)],
        );
        let mut engine = recover(&log, CatalogEngine::open_master(&dir, 1).unwrap());

        assert!(engine.registry.relation(t).is_none(), "the drop nets the table out");
        assert!(engine.allocate_ids(1).unwrap() > t);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// The AF_UNIX bind replaces only a stale socket: a regular file at the path
/// and a socket a live server answers on each refuse the boot, untouched.
#[test]
fn bind_unix_socket_replaces_only_a_stale_socket() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.sock");
    let path_str = path.to_str().unwrap();

    std::fs::write(&path, b"data").unwrap();
    let e = bind_unix_socket(path_str).expect_err("a regular file refuses");
    assert!(e.contains("is not a socket"), "{e}");
    assert_eq!(std::fs::read(&path).unwrap(), b"data", "and survives");
    std::fs::remove_file(&path).unwrap();

    let live = bind_unix_socket(path_str).expect("a free path binds");
    let e = bind_unix_socket(path_str).expect_err("a live socket refuses");
    assert!(e.contains("running server"), "{e}");

    drop(live);
    bind_unix_socket(path_str).expect("a stale socket is replaced");
}
