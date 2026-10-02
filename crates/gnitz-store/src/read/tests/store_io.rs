use super::*;
use crate::test_support::{
    make_batch_raw, make_schema_u64_i64, opk_pk, payload0_i64, pk_only_schema, relation_fixture, RelationFixture, TID,
};
use gnitz_wire::TypeCode;
use gnitz_wire::{key_image, Cut, PkColList};
use gnitz_zset::repr::BatchBuilder;

const NBASE: u64 = 200;

/// A `(id U64 PK | val I64)` base of `NBASE` rows at `val = val_of(id)`, indexed
/// on `val`.
fn fixture(val_of: impl Fn(u64) -> i64) -> RelationFixture {
    let schema = make_schema_u64_i64();
    let rows: Vec<_> = (0..NBASE).map(|id| (id, 1, val_of(id))).collect();
    let rows = make_batch_raw(&schema, &rows);
    relation_fixture(RelationKind::BaseTable, schema, &[1], rows)
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
    let r = fixture(|id| id as i64 * 10);
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
    let r = fixture(|id| (NBASE - id) as i64 * 10);
    let (mut cur, _) = r.open_bound(TID, val_range(10, 110)).unwrap();
    assert!(matches!(cur, SourceCursor::Bounded(_)));
    assert_eq!(drain_all(&mut cur, 3), ids(190..200));
}

/// The walk sees the index the base's writes left: a retracted row is gone, a row
/// updated into the range appears, one updated within it appears once, and one
/// updated out of it is gone.
#[test]
fn a_walk_follows_the_bases_updates() {
    let mut r = fixture(|id| id as i64 * 10);
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
    let mut r = fixture(|id| id as i64 * 10);
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
    let mut bb = BatchBuilder::new(&schema);
    for a in 0..8u128 {
        for b in 0..3u128 {
            bb.begin_row_opk(&[a, b], 1);
            bb.end_row();
        }
    }
    let r = relation_fixture(RelationKind::BaseTable, schema, &[0], bb.finish());

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
    let r = fixture(|id| id as i64);
    let wide = ReadBound::PkSet(PkKeys::from_keys(16, [&[0u8; 16][..]]));
    assert!(r.open_bound(TID, wide).is_err());
    assert!(r.open_bound(999_999, ReadBound::None).is_err());
}

/// Probe keys over the table's own PK store, one row per id.
fn pk_probe(ids: &[u128]) -> Batch {
    let schema = pk_only_schema(&[TypeCode::U64]);
    let mut keys = Batch::empty_with_schema(&schema);
    for &id in ids {
        keys.push_key_row(&opk_pk(&schema, &[id]), 1);
    }
    keys
}

/// The rows of a probe reply as `(key bytes, weight)`.
fn reply_rows(reply: &Batch) -> Vec<(Vec<u8>, i64)> {
    (0..reply.len())
        .map(|i| (reply.get_pk_bytes(i).to_vec(), reply.get_weight(i)))
        .collect()
}

/// A column probe answers each named key's live row at weight 1 with that
/// column, and nothing for a key with no row — one retracted, one never held.
#[test]
fn a_column_probe_answers_each_live_key_with_its_column() {
    let mut r = fixture(|id| id as i64 * 10);
    let schema = make_schema_u64_i64();
    r.ingest(TID, make_batch_raw(&schema, &[(5, -1, 50)])).unwrap();
    let got = r.probe(TID, Probe::PkColumn(1), &pk_probe(&[2, 5, 7, 300])).unwrap();
    let rows: Vec<_> = (0..got.len())
        .map(|i| (got.get_pk(i), got.get_weight(i), payload0_i64(&got, i)))
        .collect();
    assert_eq!(rows, [(2, 1, 20), (7, 1, 70)]);
}

/// A PK probe echoes each key a live row carries, and nothing at all for an id
/// this process has not registered.
#[test]
fn a_pk_probe_echoes_the_keys_a_live_row_carries() {
    let mut r = fixture(|id| id as i64 * 10);
    let schema = make_schema_u64_i64();
    r.ingest(TID, make_batch_raw(&schema, &[(5, -1, 50)])).unwrap();
    let keys = pk_probe(&[2, 5, 7, 300]);
    let held = r.probe(TID, Probe::Pk, &keys).unwrap();
    assert_eq!(reply_rows(&held), reply_rows(&pk_probe(&[2, 7])));
    assert!(r.probe(999_999, Probe::Pk, &keys).unwrap().is_empty());
}

/// The index of [`fixture`], with what builds a stored entry and a probe key
/// of it.
struct ValIndex {
    spec: gnitz_zset::schema::KeySpec,
    schema: SchemaDescriptor,
}

impl ValIndex {
    fn of(r: &RelationFixture) -> Self {
        let ix = r.relation(TID).unwrap().index_on(&[1]).unwrap();
        ValIndex { spec: ix.key_spec(), schema: ix.schema() }
    }

    /// The entry row `id` stores under `val`.
    fn entry(&self, id: u64, val: i64) -> Vec<u8> {
        let rows = make_batch_raw(&make_schema_u64_i64(), &[(id, 1, val)]);
        let entries = gnitz_zset::algebra::index_entries(&rows, &self.spec, &self.schema);
        entries.get_pk_bytes(0).to_vec()
    }

    /// The probe keys of `vals`, each span over a holder no row has.
    fn keys(&self, vals: &[i64]) -> Batch {
        let mut keys = Batch::empty_with_schema(&self.schema);
        for &val in vals {
            let mut key = self.entry(0, val);
            key[self.spec.key_size()..].fill(0xAB);
            keys.push_key_row(&key, 1);
        }
        keys
    }
}

fn cap(n: u64) -> std::num::NonZeroU64 {
    std::num::NonZeroU64::new(n).unwrap()
}

/// An index probe answers a held span with its stored entries, up to the cap,
/// and a span no row holds with nothing.
#[test]
fn an_index_probe_answers_a_held_span_with_its_holders() {
    // Ids 2k and 2k + 1 share `val = 10k`.
    let r = fixture(|id| (id / 2) as i64 * 10);
    let cols = PkColList::from_slice(&[1]);
    let ix = ValIndex::of(&r);
    let keys = ix.keys(&[20, 25]);
    let probe = |n| reply_rows(&r.probe(TID, Probe::Index(cols, cap(n)), &keys).unwrap());
    let (first, second) = (ix.entry(4, 20), ix.entry(5, 20));

    assert_eq!(probe(1), [(first.clone(), 1)]);
    assert_eq!(probe(2), [(first.clone(), 1), (second.clone(), 1)]);
    assert_eq!(probe(8), [(first, 1), (second, 1)]);
}

/// An index probe's one cursor answers each key as a seek of its own does: a
/// run of misses ahead of a hit, a hit ahead of its neighbour, and a miss past
/// the last entry. Keys that do not strictly ascend are refused.
#[test]
fn an_index_probe_seeks_ascending_keys_with_one_cursor() {
    // Ids 2k and 2k + 1 share `val = 10k`.
    let r = fixture(|id| (id / 2) as i64 * 10);
    let cols = PkColList::from_slice(&[1]);
    let ix = ValIndex::of(&r);
    let vals = [-5, 1, 2, 3, 10, 20, 21, 30, 5000];
    for n in [1, 2, 8] {
        let probe = |vals: &[i64]| r.probe(TID, Probe::Index(cols, cap(n)), &ix.keys(vals));
        let alone: Vec<_> = vals.iter().flat_map(|&v| reply_rows(&probe(&[v]).unwrap())).collect();
        assert_eq!(reply_rows(&probe(&vals).unwrap()), alone, "cap {n}");
        assert_eq!(alone.len(), 3 * (n as usize).min(2), "cap {n}: three held values");
    }
    for vals in [&[10, 10][..], &[20, 10]] {
        let got = r.probe(TID, Probe::Index(cols, cap(1)), &ix.keys(vals));
        assert!(got.is_err(), "{vals:?}");
    }
}

/// A probe is refused when no index is on its column list, and when its keys are
/// not as wide as the store it reads.
#[test]
fn a_probe_refuses_a_missing_index_and_a_foreign_key_stride() {
    let r = fixture(|id| id as i64);
    let index = |cols: &[u32]| Probe::Index(PkColList::from_slice(cols), cap(1));
    let pk_keys = pk_probe(&[2]);
    for (id, probe) in [(TID, index(&[0, 1])), (999_999, index(&[1])), (TID, index(&[1]))] {
        assert!(r.probe(id, probe, &pk_keys).is_err(), "{id} {probe:?}");
    }
    let wide = Batch::empty_with_schema(&r.relation(TID).unwrap().index_on(&[1]).unwrap().schema());
    for probe in [Probe::Pk, Probe::PkColumn(1)] {
        assert!(r.probe(TID, probe, &wide).is_err(), "{probe:?}");
    }
}
