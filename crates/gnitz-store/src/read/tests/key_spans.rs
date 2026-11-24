use super::*;
use crate::relation::RelationKind;
use crate::test_support::{
    RelationFixture, TID, chunked, id_rows, leading_pk_fixture, opk_pk, pk_only_schema, relation_fixture_with,
    u64_pk_schema,
};
use gnitz_wire::TypeCode;
use gnitz_zset::schema::SchemaColumn;

const ROWS: u64 = 50;

/// Row `id`'s `val`: `-(id / 2)`, so spans repeat and descend as ids ascend; NULL
/// for every seventh row.
fn val(id: u64) -> Option<i64> {
    (!id.is_multiple_of(7)).then_some(-((id / 2) as i64))
}

/// `(id U64 PK | val I64 NULL)` holding ids `0..ROWS`, every third then retracted,
/// read `chunk_rows` rows a chunk. Indexed on `val` when `indexed`.
fn fixture(indexed: bool, chunk_rows: usize) -> RelationFixture {
    let schema = u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, true)]);
    let rows = |w: i64, ids: &mut dyn Iterator<Item = u64>| {
        id_rows(&schema, ids.map(|id| (id, w)), |bb, id| {
            bb.put_opt_int(val(id).map(|v| v as u128));
        })
    };
    let indexed: &[&[u32]] = if indexed { &[&[1]] } else { &[] };
    let rounds = [rows(1, &mut (0..ROWS)), rows(-1, &mut (0..ROWS).step_by(3))];
    relation_fixture_with(chunked(chunk_rows), RelationKind::BaseTable, schema, indexed, rounds)
}

/// Every span of `spans` in order, and the row count of each chunk.
fn drain(mut spans: KeySpans) -> (Vec<Vec<u8>>, Vec<usize>) {
    let (mut keys, mut chunks) = (Vec::new(), Vec::new());
    while !spans.chunk().is_empty() {
        let chunk = spans.chunk();
        keys.extend((0..chunk.len()).map(|i| chunk.get_pk_bytes(i).to_vec()));
        chunks.push(chunk.len());
        spans.advance();
    }
    spans.advance();
    assert!(spans.chunk().is_empty(), "nothing follows the first empty chunk");
    (keys, chunks)
}

/// The spans of a relation are the index's entries as one sorted list, across
/// chunk boundaries, and neither a NULL in the column nor a retracted row yields
/// one. A column list the relation has no index on, or a relation it does not
/// hold, is refused before anything is read.
#[test]
fn the_index_yields_every_span_in_order() {
    let span_schema = pk_only_schema(&[TypeCode::I64]);
    let mut want: Vec<Vec<u8>> = (0..ROWS)
        .filter(|id| !id.is_multiple_of(3))
        .filter_map(val)
        .map(|v| opk_pk(&span_schema, &[v as u128]))
        .collect();
    want.sort();

    for chunk_rows in [1, 4, 1024] {
        let (got, chunks) = drain(fixture(true, chunk_rows).key_spans(TID, &[1]).unwrap());
        assert_eq!(got, want, "chunk_rows={chunk_rows}");
        assert!(chunks.iter().all(|&n| n <= chunk_rows), "{chunks:?} over {chunk_rows}");
    }

    let r = fixture(true, 1024);
    let unindexed = fixture(false, 1024);
    for (got, why) in [
        (r.key_spans(TID, &[0]), "has no index"),
        (r.key_spans(TID + 100, &[1]), "not registered"),
        (unindexed.key_spans(TID, &[1]), "has no index"),
    ] {
        let err = got.map(drop).unwrap_err();
        assert!(err.contains(why), "{err}");
    }
}

/// Where the columns lead the PK, the spans are read off the owner's rows: one
/// per live row, its PK's leading columns, in order.
#[test]
fn an_index_leading_the_pk_spans_the_owners_rows() {
    let r = leading_pk_fixture();
    let rel = r.relation(TID).unwrap();
    let rows = rel.full_scan();
    for cols in [&[0u32][..], &[0, 1]] {
        let k = rel.index_on(cols).unwrap().key_spec().key_size();
        let want: Vec<Vec<u8>> = (0..rows.len()).map(|i| rows.get_pk_bytes(i)[..k].to_vec()).collect();
        assert_eq!(drain(r.key_spans(TID, cols).unwrap()).0, want, "{cols:?}");
    }
}
