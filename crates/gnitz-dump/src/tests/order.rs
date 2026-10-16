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
    let (order, acyclic) = order(&batch, &schema, &[1]);
    assert_eq!(ids(&batch, &order), [1, 5, 6, 3, 2, 4]);
    assert_eq!(acyclic, 6);
}

#[test]
fn a_cycle_comes_last() {
    let (batch, schema) = tree(&[(1, Some(2)), (2, Some(1)), (3, None), (4, Some(1))]);
    let (order, acyclic) = order(&batch, &schema, &[1]);
    assert_eq!(ids(&batch, &order), [3, 1, 2, 4]);
    assert_eq!(acyclic, 1);
}

#[test]
fn a_row_waiting_on_a_cycle_comes_at_or_after_its_position() {
    // 1 and 2 reference each other, 3 is a child of 2, and 4 stands apart.
    let (batch, schema) = tree(&[(3, Some(2)), (1, Some(2)), (4, None), (2, Some(1))]);
    let (order, acyclic) = order(&batch, &schema, &[1]);
    assert_eq!(acyclic, 1);
    assert_eq!(ids(&batch, &order[..acyclic]), [4]);
    let mut waiting = ids(&batch, &order[acyclic..]);
    waiting.sort_unstable();
    assert_eq!(waiting, [1, 2, 3]);
}

#[test]
fn a_table_with_no_reference_keeps_its_order() {
    let (batch, schema) = tree(&[(2, Some(1)), (1, None)]);
    assert_eq!(order(&batch, &schema, &[]), (vec![0, 1], 2));
}
