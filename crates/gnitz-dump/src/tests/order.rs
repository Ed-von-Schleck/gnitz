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

/// Each row after its parent, and the rows on a reference cycle, and every row
/// waiting on one, after the rest, in no order of their own.
#[test]
fn every_row_comes_after_its_parent() {
    /// Rows `(id, parent)`, the referencing columns, the rows in order up to
    /// the first waiting on a cycle, and the rest.
    type Case = (
        &'static [(u64, Option<u64>)],
        &'static [usize],
        &'static [u64],
        &'static [u64],
    );
    let cases: [Case; 4] = [
        // Children listed ahead of their parents, a row referencing itself and
        // one referencing no row of the table.
        (
            &[
                (4, Some(3)),
                (3, Some(1)),
                (2, Some(1)),
                (1, None),
                (5, Some(5)),
                (6, Some(99)),
            ],
            &[1],
            &[1, 5, 6, 3, 2, 4],
            &[],
        ),
        (
            &[(1, Some(2)), (2, Some(1)), (3, None), (4, Some(1))],
            &[1],
            &[3],
            &[1, 2, 4],
        ),
        // 1 and 2 reference each other, 3 is a child of 2, and 4 stands apart.
        (
            &[(3, Some(2)), (1, Some(2)), (4, None), (2, Some(1))],
            &[1],
            &[4],
            &[1, 2, 3],
        ),
        // A table with no reference keeps its order.
        (&[(2, Some(1)), (1, None)], &[], &[2, 1], &[]),
    ];
    for (rows, refs, head, tail) in cases {
        let (batch, schema) = tree(rows);
        let (order, acyclic) = order(&batch, &schema, refs);
        let mut ids: Vec<u64> = order.iter().map(|&r| batch.pks.get(r as usize) as u64).collect();
        ids[acyclic..].sort_unstable();
        assert_eq!((&ids[..acyclic], &ids[acyclic..]), (head, tail), "{rows:?}");
    }
}
