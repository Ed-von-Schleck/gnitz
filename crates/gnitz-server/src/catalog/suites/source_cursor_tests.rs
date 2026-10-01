//! `open_source_cursor` — the source a view's backfill drives: the bound its
//! circuit recorded, over the indexes the catalog holds now.

use super::*;
use gnitz_wire::{key_image, Cut, KeyRange, PkColList};
use gnitz_zset::repr::SourceCursor;

/// A `(id U64 PK | val I64)` base of 200 rows at `val = id * 10`, indexed on
/// `val`, and an identity view over it carrying `bound`. Returns
/// `(engine, base tid, view id)`.
fn fixture(name: &str, bound: Option<KeyRange>) -> (CatalogEngine, u64, u64) {
    let cols = vec![col_def("id", TypeCode::U64), col_def("val", TypeCode::I64)];
    let (mut engine, tid) = ingest_fixture(name, &cols, 200, |bb, id| bb.put_u64(id * 10));
    engine.create_index("public.t", &["val"], false).unwrap();

    let vid = engine.allocate_ids(1).unwrap();
    let bound = bound.map_or(gnitz_wire::ReadBound::None, gnitz_wire::ReadBound::Range);
    write_identity_circuit(&mut engine, vid, tid, bound);
    engine.write_column_records(vid, &cols).unwrap();
    let batch = build_view_tab_row(vid, "v_base");
    engine.ingest_to_family(gnitz_wire::VIEW_TAB, &batch).unwrap();
    (engine, tid, vid)
}

/// A view's backfill opens the bound its circuit recorded — a walk of the index
/// while the catalog holds one, a full scan once it is dropped — and an unbounded
/// view's opens a full scan.
#[test]
fn a_backfill_opens_the_bound_its_view_recorded() {
    let img = |v: i64| key_image(TypeCode::I64, v as u64 as u128);
    let bound = KeyRange::new(
        PkColList::from_slice(&[1]),
        &[],
        Cut::before(img(500)),
        Cut::before(img(600)),
    );
    let (mut engine, tid, vid) = fixture("srccur_bounded", Some(bound));
    let mut cur = engine.open_source_cursor(vid, tid).unwrap();
    assert!(matches!(cur, SourceCursor::Bounded(_)));
    let mut pks = Vec::new();
    while let Some(b) = cur.drain_chunk(64) {
        pks.extend((0..b.len()).map(|i| (b.get_pk(i), b.get_weight(i))));
    }
    assert_eq!(pks, (50..60).map(|id| (id, 1)).collect::<Vec<_>>());

    engine.drop_index("public__t__idx_val").unwrap();
    assert!(matches!(
        engine.open_source_cursor(vid, tid).unwrap(),
        SourceCursor::Full(_)
    ));
    engine.close();

    let (mut engine, tid, vid) = fixture("srccur_unbounded", None);
    assert!(matches!(
        engine.open_source_cursor(vid, tid).unwrap(),
        SourceCursor::Full(_)
    ));
    engine.close();
}
