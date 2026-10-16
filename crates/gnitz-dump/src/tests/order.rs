use super::*;
use gnitz_core::BatchAppender;
use gnitz_wire::{ColumnDef, TypeCode};

/// `(id PK, parent NULL)` rows `(id, parent)`.
fn tree(rows: &[(u64, Option<u64>)]) -> (ZSetBatch, Schema) {
    let schema = Schema::from_parts(
        vec![
            ColumnDef::new("id", TypeCode::U64, false),
            ColumnDef::new("parent", TypeCode::U64, true),
        ],
        &[0],
    )
    .unwrap();
    let mut batch = ZSetBatch::new(&schema);
    let mut app = BatchAppender::new(&mut batch);
    for &(id, parent) in rows {
        let r = app.add_row(id as u128, 1);
        match parent {
            Some(p) => r.u64_val(p),
            None => r.null(),
        };
    }
    (batch, schema)
}

fn ids(batch: &ZSetBatch, order: &[u32]) -> Vec<u64> {
    order.iter().map(|&r| batch.pks.get(r as usize) as u64).collect()
}

#[test]
fn every_row_comes_after_its_parent() {
    // Children listed ahead of their parents.
    let (batch, schema) = tree(&[
        (4, Some(3)),
        (3, Some(1)),
        (2, Some(1)),
        (1, None),
        (5, Some(5)),
        (6, Some(99)),
    ]);
    let got = ids(&batch, &order(&batch, &schema, &[(1, 0)]));
    assert_eq!(got, [1, 5, 6, 3, 2, 4]);
}

#[test]
fn a_cycle_comes_last() {
    let (batch, schema) = tree(&[(1, Some(2)), (2, Some(1)), (3, None), (4, Some(1))]);
    let got = ids(&batch, &order(&batch, &schema, &[(1, 0)]));
    assert_eq!(got, [3, 1, 2, 4]);
}
