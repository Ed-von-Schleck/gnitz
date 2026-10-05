//! What the catalog refuses of a hand-built `DDL_TXN` bundle: shapes the client
//! never builds, which a wire peer skipping its canonicalization can still send.

use super::*;

fn id_v() -> Vec<CatalogColumn> {
    vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)]
}

/// A name whose bytes are not UTF-8 is refused as such in every family that
/// stores one, not read as some other name.
#[test]
fn a_name_that_is_not_utf8_is_refused() {
    let dir = temp_dir("bundle_non_utf8_names");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let id = engine.allocate_ids(1).unwrap();
    for family in [SysFamily::Schema, SysFamily::Table, SysFamily::View, SysFamily::Index] {
        let schema = family.schema();
        let mut bb = BatchBuilder::new(schema);
        bb.begin_row(id as u128, 1);
        for (_, col) in schema.payload_columns() {
            match col.type_code {
                TypeCode::U64 => bb.put_u64(0),
                _ => bb.put_blob(b"\xff\xfe"),
            }
        }
        bb.end_row();
        let err = engine.precheck_family(family, &bb.finish()).unwrap_err();
        assert!(err.contains("name is not UTF-8"), "{}: {err}", family.name());
    }
    engine.close();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Nothing synthesizes a schema name, so it takes the full identifier rule,
/// leading `_` included, and one batch may not claim it twice: the name would
/// resolve to the second row, leaving the first id live and unreachable. Nor may
/// it claim a name a live schema holds.
#[test]
fn a_schema_row_takes_the_full_identifier_rule_once_per_name() {
    let dir = temp_dir("bundle_schema_names");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let (a, b) = (engine.allocate_ids(1).unwrap(), engine.allocate_ids(1).unwrap());

    for (name, why) in [
        ("a/b", "invalid characters"),
        ("with space", "invalid characters"),
        ("", "cannot be empty"),
        ("_x", "cannot start with '_'"),
        ("MixedCase", "not canonical"),
    ] {
        let err = engine
            .precheck_family(SysFamily::Schema, &schema_tab_batch(&[(a, name, 1)]))
            .unwrap_err();
        assert!(err.contains(why), "{name:?}: {err}");
    }
    for (rows, taken) in [
        (&[(a, "twice", 1), (b, "twice", 1)][..], "twice"),
        (&[(a, "public", 1)], "public"),
    ] {
        let err = engine
            .precheck_family(SysFamily::Schema, &schema_tab_batch(rows))
            .unwrap_err();
        assert!(err.contains(&format!("Schema already exists: {taken}")), "{err}");
    }
    engine
        .precheck_family(SysFamily::Schema, &schema_tab_batch(&[(a, "twice", 1)]))
        .expect("one row under the name is legal");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// Two IDX_TAB rows under one name both miss the live indexes; applying both
/// would leave two indexes under one name.
#[test]
fn an_index_row_takes_a_canonical_name_once_per_batch() {
    let cols = vec![
        col_def("id", TypeCode::U64),
        col_def("b", TypeCode::I64),
        col_def("c", TypeCode::I64),
    ];
    let (mut engine, tid, dir) = table_fixture("bundle_index_names", &cols);
    let (a, b) = (engine.allocate_ids(1).unwrap(), engine.allocate_ids(1).unwrap());

    let mut twice = idx_tab_batch(a, tid, &[1], "ix", false, 1);
    twice.append_batch(&idx_tab_batch(b, tid, &[2], "ix", false, 1));
    let err = engine.precheck_family(SysFamily::Index, &twice).unwrap_err();
    assert!(err.contains("Index already exists: ix"), "{err}");

    let mixed = idx_tab_batch(a, tid, &[1], "MixedCase", false, 1);
    let err = engine.precheck_family(SysFamily::Index, &mixed).unwrap_err();
    assert!(err.contains("not canonical"), "{err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// An IDX_TAB `+1` whose column list names a column twice is refused: its store
/// directory would carry a name the directory grammar does not parse.
#[test]
fn an_index_row_repeating_a_column_is_refused() {
    let (mut engine, tid, dir) = table_fixture("bundle_index_repeated_col", &id_v());
    let index_id = engine.allocate_ids(1).unwrap();

    let mut bb = BatchBuilder::new(SysFamily::Index.schema());
    let sink: &mut dyn gnitz_wire::sys_rows::SysRowSink = &mut bb;
    sink.begin_row(&[index_id as u128], 1);
    sink.put_u64(tid);
    sink.put_u64(PK_LIST_PACKED_FLAG | 2 | 1 << 4 | 1 << 11);
    sink.put_string("ix");
    sink.put_u64(0);
    sink.end_row();
    let err = engine.precheck_family(SysFamily::Index, &bb.finish()).unwrap_err();
    assert!(err.contains("names column 1 twice"), "{err}");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A VIEW_TAB `+1`'s `owner_view_id` must name a real view other than itself —
/// the drop cascade keys on it — and its name must be canonical.
#[test]
fn a_view_row_names_a_real_owner_and_a_canonical_name() {
    let dir = temp_dir("bundle_view_rows");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let vid = engine.allocate_ids(1).unwrap();
    engine.write_column_records(vid, &id_v()).unwrap();
    let view_row = |name: &str, owner_view_id: u64| {
        let mut bb = BatchBuilder::new(SysFamily::View.schema());
        push_view_tab_row(&mut bb, 1, vid, name, 0, 0, owner_view_id);
        bb.finish()
    };

    for (name, owner, why) in [
        ("seg", 999_999, "which no relation holds"),
        ("seg", vid, "declares itself its own owner"),
        ("MixedCase", 0, "not canonical"),
    ] {
        let err = engine
            .precheck_family(SysFamily::View, &view_row(name, owner))
            .unwrap_err();
        assert!(err.contains(why), "{name}/{owner}: {err}");
    }
    engine
        .precheck_family(SysFamily::View, &view_row("v", 0))
        .expect("a canonical user view is legal");

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A column record on an owner nothing registers is unretractable: the only
/// COL_TAB retractor is the owner's own drop cascade.
#[test]
fn a_column_block_needs_an_owner_its_bundle_creates_or_the_catalog_holds() {
    let dir = temp_dir("bundle_column_owner");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let tid = engine.allocate_ids(1).unwrap();
    let mut families: [Option<Batch>; SysFamily::COUNT] = std::array::from_fn(|_| None);
    families[SysFamily::Column.index()] = Some(col_tab_batch(tid, &id_v(), 1));

    let err = engine.precheck_bundle(&families).unwrap_err();
    assert!(
        err.contains(&format!("owner {tid}, which this transaction does not create")),
        "{err}"
    );
    families[SysFamily::Table.index()] = Some(table_tab_batch(&[(tid, "t", 1)]));
    engine.precheck_bundle(&families).unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A circuit `+1` under a view its bundle does not create would inject nodes
/// into a running view's circuit, or pin its source's drop forever. A rewrite
/// pair on VIEW_TAB creates nothing.
#[test]
fn a_circuit_block_names_only_views_its_bundle_creates() {
    let dir = temp_dir("bundle_circuit_owner");
    let engine = CatalogEngine::open(&dir, 1).unwrap();
    let mut families: [Option<Batch>; SysFamily::COUNT] = std::array::from_fn(|_| None);
    let mut circuits = crate::test_support::circuit_batch(30, &negate_chain(16, 2));
    circuits.append_batch(&crate::test_support::circuit_batch(20, &negate_chain(16, 2)));
    families[SysFamily::Circuit.index()] = Some(circuits);
    let mut views = |rows: &[(i64, u64)]| {
        let mut bb = BatchBuilder::new(SysFamily::View.schema());
        for &(weight, vid) in rows {
            push_view_tab_row(&mut bb, weight, vid, "v", 0, 0, 0);
        }
        families[SysFamily::View.index()] = Some(bb.finish());
        engine.precheck_bundle(&families)
    };

    for (rows, uncreated) in [(&[][..], 30), (&[(1, 30)], 20), (&[(1, 30), (-1, 20), (1, 20)], 20)] {
        let err = views(rows).unwrap_err();
        assert!(
            err.contains(&format!("view {uncreated}, which this transaction does not create")),
            "{rows:?}: {err}"
        );
    }
    // Descending, so the rule cannot lean on the block's row order.
    views(&[(1, 30), (1, 20)]).unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

// ── `apply_bundle` ──────────────────────────────────────────────────────────

/// A creating bundle registers its table whatever order its blocks arrive in:
/// the column records are applied before the row whose hook reads them.
#[test]
fn a_creating_bundle_registers_its_table() {
    let dir = temp_dir("bundle_apply_create");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let tid = engine.allocate_ids(1).unwrap();

    engine
        .apply_bundle(bundle([
            (SysFamily::Table, table_tab_batch(&[(tid, "t", 1)])),
            (SysFamily::Column, col_tab_batch(tid, &id_v(), 1)),
        ]))
        .unwrap();
    assert_eq!(engine.get_by_name("public", "t"), Some(tid));
    assert!(engine.registry.has_id(tid));

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// A bundle that only drops retires a dependent first: the view before the table
/// it reads, the schema after its last member.
#[test]
fn an_all_drop_bundle_drops_a_schema_with_its_members() {
    let dir = temp_dir("bundle_apply_drop_schema");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    engine.create_schema("s").unwrap();
    let sid = engine.schema_id("s").unwrap();
    let tid = engine.create_table("s.t", &id_v(), &[0]).unwrap();
    let vid = register_identity_view(&mut engine, tid, "v", &id_v());

    let drop = |family, id| (family, engine.retract_under(family, &[id]));
    let drops = bundle([
        drop(SysFamily::Schema, sid),
        drop(SysFamily::Table, tid),
        drop(SysFamily::View, vid),
    ]);
    engine.apply_bundle(drops).unwrap();
    assert!(engine.schema_id("s").is_none());
    assert!(!engine.registry.has_id(tid) && !engine.registry.has_id(vid));
    assert!(engine.dag.dependents_of(tid).is_empty());

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}

/// Three bundles refused after rows of theirs applied — a view whose circuit does
/// not compile, a view whose column records carry an FK, a view over a table its
/// own bundle drops — each compensate to the catalog they found.
#[test]
fn a_refused_view_bundle_compensates_to_the_prior_catalog() {
    let dir = temp_dir("bundle_apply_refused_view");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let base = engine.create_table("public.base", &id_v(), &[0]).unwrap();
    let before = sys_row_counts(&engine);
    let view_bundle = |vid: u64, circuit: gnitz_wire::Circuit, cols: &[CatalogColumn]| {
        [
            (SysFamily::Circuit, crate::test_support::circuit_batch(vid, &circuit)),
            (SysFamily::Column, col_tab_batch(vid, cols, 1)),
            (SysFamily::View, build_view_tab_row(vid, "v")),
        ]
    };
    let identity = || crate::test_support::identity_circuit(base, gnitz_wire::ReadBound::None);
    // A filter whose predicate blob the expression decoder refuses.
    let mut uncompilable = gnitz_wire::Circuit::default();
    let scan = uncompilable.input_delta(base, gnitz_wire::ReadBound::None);
    let filter = uncompilable.filter(scan, vec![0xFF]);
    uncompilable.sink(filter);
    let fk_cols = [col_def("id", TypeCode::U64), fk_def("v", TypeCode::U64, base, 0)];

    type Case<'a> = (
        &'a str,
        Box<dyn Fn(&CatalogEngine, u64) -> Vec<(SysFamily, Batch)> + 'a>,
    );
    let cases: [Case; 3] = [
        (
            "expr blob",
            Box::new(|_, vid| view_bundle(vid, uncompilable.clone(), &id_v()).into()),
        ),
        (
            "may not carry a FOREIGN KEY; only a base table's may",
            Box::new(|_, vid| view_bundle(vid, identity(), &fk_cols).into()),
        ),
        (
            &format!("relation {base}"),
            Box::new(|engine, vid| {
                let mut blocks: Vec<_> = view_bundle(vid, identity(), &id_v()).into();
                blocks.push((SysFamily::Table, engine.retract_under(SysFamily::Table, &[base])));
                blocks
            }),
        ),
    ];
    for (want, blocks) in cases {
        let _ = engine.drain_pending_broadcasts();
        let vid = engine.allocate_ids(1).unwrap();
        let err = engine
            .apply_bundle(bundle(blocks(&engine, vid)))
            .expect_err("the bundle is refused");
        assert!(err.contains(want), "{want}: {err}");
        engine.compensate_stage_a().unwrap();

        assert_eq!(sys_row_counts(&engine), before, "{want}");
        assert!(engine.registry.has_id(base) && !engine.registry.has_id(vid), "{want}");
        assert_eq!(engine.get_by_name("public", "base"), Some(base), "{want}");
        assert!(engine.dag.dependents_of(base).is_empty(), "{want}");
    }

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
