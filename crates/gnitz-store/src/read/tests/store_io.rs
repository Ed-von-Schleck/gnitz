use super::*;
use crate::relation::{IndexClaim, RelationSpec, StoreConfig};
use crate::schema::{Slot, TypeCode};
use crate::storage::BatchBuilder;
use crate::test_support::{make_batch_raw, make_schema_u64_i64, opk_pk, payload0_i64, pk_only_schema};
use gnitz_wire::{key_image, Cut, PkColList};

/// The base relation every test below reads.
const TID: u64 = gnitz_wire::FIRST_USER_TABLE_ID;

const NBASE: u64 = 200;

/// A `(id U64 PK | val I64)` base of `NBASE` rows at `val = val_of(id)`, indexed
/// on `val`.
fn fixture(name: &str, val_of: impl Fn(u64) -> i64) -> RelationRegistry {
    let schema = make_schema_u64_i64();
    let mut registry = RelationRegistry::new(
        &crate::test_support::scratch_dir("store_io", name),
        Slot::SOLO,
        StoreConfig::default(),
    );
    let kind = RelationKind::BaseTable;
    registry.register(RelationSpec { id: TID, kind, schema }).unwrap();
    registry
        .add_index(TID, IndexClaim::Index { id: TID + 1, unique: false }, &[1])
        .unwrap();
    let rows: Vec<_> = (0..NBASE).map(|id| (id, 1, val_of(id))).collect();
    registry.ingest(TID, make_batch_raw(&schema, &rows)).unwrap();
    registry
}

/// `v`'s key image in the I64 `val` column.
fn img(v: i64) -> u128 {
    key_image(TypeCode::I64, v as u64 as u128)
}

/// The bound `val ∈ [lo, hi)`.
fn val_range(lo: i64, hi: i64) -> ReadBound {
    let r = KeyRange::new(
        PkColList::from_slice(&[1]),
        &[],
        Cut::before(img(lo)),
        Cut::before(img(hi)),
    );
    ReadBound::Range(r)
}

/// Drain a cursor to `(pk, weight)` pairs, `chunk` rows at a time so a chunk
/// boundary lands mid-range, checking each chunk is non-empty and PK-ascending.
fn drain_all(cur: &mut SourceCursor, chunk: usize) -> Vec<(u128, i64)> {
    let mut out = Vec::new();
    while let Some(b) = cur.drain_chunk(chunk) {
        assert!(!b.is_empty(), "drain_chunk yielded an empty chunk");
        for i in 0..b.len() {
            assert!(
                i == 0 || b.get_pk(i - 1) < b.get_pk(i),
                "chunk {chunk}: rows out of PK order"
            );
            out.push((b.get_pk(i), b.get_weight(i)));
        }
    }
    out.sort_unstable();
    out
}

fn ids(ids: impl IntoIterator<Item = u128>) -> Vec<(u128, i64)> {
    ids.into_iter().map(|id| (id, 1)).collect()
}

/// A range within the selectivity gate walks the index and yields exactly its rows
/// at every chunk size.
#[test]
fn a_selective_range_walks_the_index_at_every_chunk_size() {
    let r = fixture("walk_chunks", |id| id as i64 * 10);
    for chunk in [1, 3, 7, 64, usize::MAX] {
        let (mut cur, unapplied) = r.open_bound(TID, val_range(500, 600)).unwrap();
        assert!(matches!(cur, SourceCursor::Bounded(_)), "chunk {chunk}");
        assert_eq!(unapplied, ReadBound::None, "chunk {chunk}");
        assert_eq!(drain_all(&mut cur, chunk), ids(50..60), "chunk {chunk}");
    }
}

/// With `val` anti-correlated to the PK, a range at val's minimum makes the first
/// chunk end on the base's last PK group — exhausting the base cursor — and every
/// later chunk's first probe sorts below it: a backward re-seek from an exhausted
/// cursor.
#[test]
fn a_walk_reseeks_backward_from_an_exhausted_base_cursor() {
    let r = fixture("walk_backward", |id| (NBASE - id) as i64 * 10);
    let (mut cur, _) = r.open_bound(TID, val_range(10, 110)).unwrap();
    assert!(matches!(cur, SourceCursor::Bounded(_)));
    assert_eq!(drain_all(&mut cur, 3), ids(190..200));
}

/// The walk sees the index the base's writes left: a retracted row is gone, a row
/// updated into the range appears, one updated within it appears once, and one
/// updated out of it is gone.
#[test]
fn a_walk_follows_the_bases_updates() {
    let mut r = fixture("walk_updates", |id| id as i64 * 10);
    let updates = [
        (55, -1, 550),
        (10, -1, 100),
        (10, 1, 555),
        (51, -1, 510),
        (51, 1, 530),
        (52, -1, 520),
        (52, 1, 5200),
    ];
    r.ingest(TID, make_batch_raw(&make_schema_u64_i64(), &updates)).unwrap();
    let (mut cur, _) = r.open_bound(TID, val_range(500, 560)).unwrap();
    assert!(matches!(cur, SourceCursor::Bounded(_)));
    assert_eq!(drain_all(&mut cur, 4), ids([10, 50, 51, 53, 54]));
}

/// A range walks the index only while its entries are at most 1/16 of the base's
/// rows — 12 of 200 do, 13 do not; past the gate, and with no index at all, the
/// source is a full scan and the range comes back unapplied, for the caller's filter.
#[test]
fn a_range_walks_the_index_only_within_the_selectivity_gate() {
    let mut r = fixture("walk_gate", |id| id as i64 * 10);
    let point = ReadBound::Range(KeyRange::point(PkColList::from_slice(&[1]), &[], img(730)));
    let inverted = ReadBound::Range(KeyRange::new(
        PkColList::from_slice(&[1]),
        &[],
        Cut::after(img(900)),
        Cut::before(img(300)),
    ));
    for (bound, want) in [
        (val_range(500, 620), ids(50..62)),
        (point.clone(), ids([73])),
        (inverted, vec![]),
    ] {
        let (mut cur, unapplied) = r.open_bound(TID, bound.clone()).unwrap();
        assert!(matches!(cur, SourceCursor::Bounded(_)), "{bound:?}");
        assert_eq!(unapplied, ReadBound::None, "{bound:?}");
        assert_eq!(drain_all(&mut cur, 64), want, "{bound:?}");
    }

    let scanned = |r: &RelationRegistry, bound: ReadBound| {
        let (mut cur, unapplied) = r.open_bound(TID, bound.clone()).unwrap();
        assert!(matches!(cur, SourceCursor::Full(_)), "{bound:?}");
        assert_eq!(unapplied, bound);
        assert_eq!(drain_all(&mut cur, 64), ids(0..NBASE as u128), "{bound:?}");
    };
    scanned(&r, val_range(500, 630));
    scanned(&r, val_range(0, i64::MAX));
    r.release_index(TID, TID + 1);
    scanned(&r, point);
}

/// A range over a PK prefix walks the table's own store even when an index names the
/// same columns: `INDEX(a)` under `PRIMARY KEY (a, b)` is never walked for `a`.
#[test]
fn a_pk_prefix_range_walks_the_store_over_a_matching_index() {
    let schema = pk_only_schema(&[TypeCode::U64, TypeCode::U64]);
    let mut r = RelationRegistry::new(
        &crate::test_support::scratch_dir("store_io", "pk_prefix"),
        Slot::SOLO,
        StoreConfig::default(),
    );
    let kind = RelationKind::BaseTable;
    r.register(RelationSpec { id: TID, kind, schema }).unwrap();
    r.add_index(TID, IndexClaim::Index { id: TID + 1, unique: false }, &[0])
        .unwrap();
    let mut bb = BatchBuilder::new(schema);
    for a in 0..8u128 {
        for b in 0..3u128 {
            bb.begin_row_opk(&[a, b], 1);
            bb.end_row();
        }
    }
    r.ingest(TID, bb.finish()).unwrap();

    let range = KeyRange::point(PkColList::from_slice(&[0]), &[], 5);
    let (mut cur, unapplied) = r.open_bound(TID, ReadBound::Range(range)).unwrap();
    assert!(matches!(cur, SourceCursor::Full(_)), "a PK walk, not the index");
    assert_eq!(unapplied, ReadBound::None, "a PK walk applies the whole range");
    assert_eq!(drain_all(&mut cur, 64), ids((0..3).map(|b| 5 << 64 | b)));
}

/// A `PkSet` at a stride other than the relation's cannot name its keys, and an
/// unregistered id names no relation: both are refused.
#[test]
fn open_bound_refuses_a_foreign_key_stride_and_an_unknown_relation() {
    let r = fixture("refusals", |id| id as i64);
    let wide = ReadBound::PkSet(PkKeys::from_keys(16, [&[0u8; 16][..]]));
    assert!(r.open_bound(TID, wide).is_err());
    assert!(r.open_bound(999_999, ReadBound::None).is_err());
}

/// The FK parent probe answers each named key's live row at weight 1, projected to
/// the referenced column, and nothing for a key with no row.
#[test]
fn gather_bytes_projects_each_live_parent_to_its_referenced_column() {
    let mut r = fixture("gather", |id| id as i64 * 10);
    let schema = make_schema_u64_i64();
    r.ingest(TID, make_batch_raw(&schema, &[(5, -1, 50)])).unwrap();
    let keys: Vec<_> = [2, 5, 7, 300].map(|k| opk_pk(&schema, &[k])).to_vec();
    let got = r
        .gather_bytes(TID, PkKeys::from_keys(8, keys.iter().map(Vec::as_slice)), 1)
        .unwrap();
    let rows: Vec<_> = (0..got.len())
        .map(|i| (got.get_pk(i), got.get_weight(i), payload0_i64(&got, i)))
        .collect();
    assert_eq!(rows, [(2, 1, 20), (7, 1, 70)]);
}
