//! What the catalog refuses of a hand-built `DDL_TXN` bundle: shapes the client
//! never builds, which a wire peer skipping its canonicalization can still send.

use super::*;
use gnitz_wire::sys_rows::{write_idx_tab_row, IdxTabRow};
use gnitz_wire::IndexProps;

fn id_v() -> Vec<CatalogColumn> {
    vec![col_def("id", TypeCode::U64), col_def("v", TypeCode::I64)]
}

/// A schema name is the one catalog name the engine interpolates into a
/// filesystem path, so it takes the full identifier rule, leading `_` included,
/// and one batch may not claim it twice: `schema_by_name` would keep the second
/// row, leaving the first id live and unreachable.
#[test]
fn a_schema_row_takes_the_full_identifier_rule_once_per_name() {
    let dir = temp_dir("bundle_schema_names");
    let mut engine = CatalogEngine::open(&dir, 1).unwrap();
    let (a, b) = (engine.allocate_ids(1).unwrap(), engine.allocate_ids(1).unwrap());

    for name in ["..", "../escape", "a/b", "with space", "", "_x", "MixedCase"] {
        assert!(
            engine
                .precheck_family(SysFamily::Schema, &schema_tab_batch(&[(a, name, 1)]))
                .is_err(),
            "{name:?} must be refused"
        );
    }
    let err = engine
        .precheck_family(
            SysFamily::Schema,
            &schema_tab_batch(&[(a, "twice", 1), (b, "twice", 1)]),
        )
        .unwrap_err();
    assert!(err.contains("Schema already exists: twice"), "{err}");
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

    let mut bb = BatchBuilder::new(*SysFamily::Index.schema());
    for (index_id, col) in [(a, 1), (b, 2)] {
        let row = IdxTabRow {
            index_id,
            owner_id: tid,
            source_col_idx: pack_pk_cols(&[col]),
            name: "ix",
            flags: IndexProps::default().pack(),
        };
        write_idx_tab_row(&mut bb, &row, 1);
    }
    let err = engine.precheck_family(SysFamily::Index, &bb.finish()).unwrap_err();
    assert!(err.contains("Index already exists: ix"), "{err}");

    let mixed = idx_tab_batch(a, tid, pack_pk_cols(&[1]), "MixedCase", IndexProps::default(), 1);
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
        let mut bb = BatchBuilder::new(*SysFamily::View.schema());
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

    let err = engine.precheck_bundle(&families, &[]).unwrap_err();
    assert!(
        err.contains(&format!("owner {tid}, which this transaction does not create")),
        "{err}"
    );
    families[SysFamily::Table.index()] = Some(build_table_tab_row(tid, pack_pk_cols(&[0]), "t"));
    engine.precheck_bundle(&families, &[]).unwrap();

    engine.close();
    let _ = fs::remove_dir_all(&dir);
}
