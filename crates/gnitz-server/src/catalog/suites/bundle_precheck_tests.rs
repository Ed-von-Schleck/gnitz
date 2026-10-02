//! What the catalog refuses of a hand-built `DDL_TXN` bundle: shapes the client
//! never builds, which a wire peer skipping its canonicalization can still send.

use super::*;
use gnitz_wire::IndexProps;

fn id_v() -> Vec<CatalogColumn> {
    vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)]
}

/// Nothing synthesizes a schema name, so it takes the full identifier rule,
/// leading `_` included, and one batch may not claim it twice: `schema_by_name`
/// would keep the second row, leaving the first id live and unreachable. Nor may
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

/// Two IDX_TAB rows under one name both pass the persisted `index_by_name`
/// check; applying both would leave one index live and unreachable by name.
#[test]
fn an_index_row_takes_a_canonical_name_once_per_batch() {
    let cols = vec![
        col_def("id", TypeCode::U64),
        col_def("b", TypeCode::I64),
        col_def("c", TypeCode::I64),
    ];
    let (mut engine, tid, dir) = table_fixture("bundle_index_names", &cols);
    let (a, b) = (engine.allocate_ids(1).unwrap(), engine.allocate_ids(1).unwrap());

    let mut twice = idx_tab_batch(a, tid, &[1], "ix", IndexProps::default(), 1);
    twice.append_batch(&idx_tab_batch(b, tid, &[2], "ix", IndexProps::default(), 1));
    let err = engine.precheck_family(SysFamily::Index, &twice).unwrap_err();
    assert!(err.contains("Index already exists: ix"), "{err}");

    let mixed = idx_tab_batch(a, tid, &[1], "MixedCase", IndexProps::default(), 1);
    let err = engine.precheck_family(SysFamily::Index, &mixed).unwrap_err();
    assert!(err.contains("not canonical"), "{err}");

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
