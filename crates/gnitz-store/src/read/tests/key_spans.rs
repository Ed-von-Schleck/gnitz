use super::*;
use crate::relation::RelationKind;
use crate::test_support::{relation_fixture, RelationFixture, TID};
use gnitz_expr::SchemaFacts;
use gnitz_wire::TypeCode;
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

const ROWS: u64 = 50;

/// `(id U64 PK | val I64 NULL)` holding ids `0..ROWS` at `val = -(id / 2)`, so
/// spans repeat and descend as ids ascend; every seventh row's `val` is NULL.
/// Indexed on `val` when `indexed`.
fn fixture(indexed: bool) -> RelationFixture {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let mut bb = BatchBuilder::new(&schema);
    for id in 0..ROWS {
        bb.begin_row(id as u128, 1);
        match id % 7 {
            0 => bb.put_null(),
            _ => bb.put_int((-((id / 2) as i64)) as u128),
        }
        bb.end_row();
    }
    let indexed: &[u32] = if indexed { &[1] } else { &[] };
    relation_fixture(RelationKind::BaseTable, schema, indexed, bb.finish())
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

/// The spans of a relation are one sorted list whether an index on the columns
/// holds them or a sort of the rows makes them — across chunk boundaries, and
/// with a spill budget of a few records — and a NULL in the column yields none.
#[test]
fn the_index_and_the_sort_yield_the_same_sorted_spans() {
    let span_schema = crate::test_support::pk_only_schema(&[TypeCode::I64]);
    let mut want: Vec<Vec<u8>> = (0..ROWS)
        .filter(|id| id % 7 != 0)
        .map(|id| {
            let val = -((id / 2) as i64);
            span_schema.opk_key(&(val as u128).to_le_bytes()).pk_bytes().to_vec()
        })
        .collect();
    want.sort();

    for chunk_rows in [1, 4, 1024] {
        for indexed in [true, false] {
            let mut r = fixture(indexed);
            r.config.scan_chunk_rows = chunk_rows;
            // Three 8-byte records a run.
            r.config.key_spans_spill_bytes = 24;
            assert_eq!(r.relation(TID).unwrap().index_on(&[1]).is_some(), indexed);

            let (got, chunks) = drain(r.key_spans(TID, &[1]).unwrap());
            assert_eq!(got, want, "indexed={indexed}, chunk_rows={chunk_rows}");
            assert!(
                chunks.iter().all(|&n| n <= chunk_rows),
                "indexed={indexed}: {chunks:?} over {chunk_rows}"
            );
            assert_eq!(chunks.len(), want.len().div_ceil(chunk_rows), "indexed={indexed}");
        }
    }
}

/// A column list the relation does not have is refused before anything is read.
#[test]
fn spans_of_a_column_the_relation_lacks_are_refused() {
    let r = fixture(false);
    assert!(r.key_spans(TID, &[9]).is_err());
    assert!(r.key_spans(TID + 100, &[1]).is_err());
}
