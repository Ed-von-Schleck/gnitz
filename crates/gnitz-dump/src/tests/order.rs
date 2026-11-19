use super::*;
use gnitz_wire::{ColumnDef, TypeCode};

/// `(id PK, p0 NULL, p1 NULL, …)` rows, one `p` column per parent cell given.
fn tree(rows: &[(u64, &[Option<u64>])]) -> (ZSetBatch, Schema) {
    let mut cols = vec![ColumnDef::new("id", TypeCode::U64, false)];
    for i in 0..rows[0].1.len() {
        cols.push(ColumnDef::new(format!("p{i}"), TypeCode::U64, true));
    }
    let schema = Schema::from_parts(cols, &[0]).unwrap();
    let mut batch = ZSetBatch::new(&schema);
    let mut app = batch.builder();
    for &(id, parents) in rows {
        let r = app.begin_row(id as u128, 1);
        for parent in parents {
            match parent {
                Some(p) => r.put_int((*p) as u128),
                None => r.put_null(),
            };
        }
    }
    (batch, schema)
}

/// Every row after its parents, the rows that reference one another round a
/// cycle together with no push ending among them, and every other row free to
/// end a push.
#[test]
fn every_row_comes_after_its_parents() {
    /// Rows `(id, parents)`, and the sets of ids that reference one another round a cycle.
    type Case = (&'static [(u64, &'static [Option<u64>])], &'static [&'static [u64]]);
    let cases: [Case; 5] = [
        // Children listed ahead of their parents, a row referencing itself and
        // one referencing no row of the table.
        (
            &[
                (4, &[Some(3)]),
                (3, &[Some(1)]),
                (2, &[Some(1)]),
                (1, &[None]),
                (5, &[Some(5)]),
                (6, &[Some(99)]),
            ],
            &[],
        ),
        (
            &[(1, &[Some(2)]), (2, &[Some(1)]), (3, &[None]), (4, &[Some(1)])],
            &[&[1, 2]],
        ),
        // A chain below a cycle is no part of it.
        (
            &[
                (3, &[Some(2)]),
                (5, &[Some(3)]),
                (1, &[Some(2)]),
                (4, &[None]),
                (2, &[Some(1)]),
            ],
            &[&[1, 2]],
        ),
        // Couples, each its own cycle.
        (
            &[
                (1, &[Some(2)]),
                (2, &[Some(1)]),
                (3, &[Some(4)]),
                (4, &[Some(3)]),
                (5, &[Some(6)]),
                (6, &[Some(5)]),
            ],
            &[&[1, 2], &[3, 4], &[5, 6]],
        ),
        // Two references each: cycles {1, 2}, {4, 5} and {7, 8}, with 6 a child
        // of 2 that {7, 8} references; 9 names 1 twice, 10 is a child of 4 and
        // 1, and 13 a child of 10.
        (
            &[
                (13, &[Some(10), None]),
                (10, &[Some(4), Some(1)]),
                (9, &[Some(1), Some(1)]),
                (8, &[Some(7), None]),
                (7, &[Some(6), Some(8)]),
                (6, &[Some(2), None]),
                (5, &[Some(4), None]),
                (4, &[Some(5), None]),
                (2, &[Some(1), None]),
                (1, &[Some(2), None]),
                (11, &[None, None]),
                (12, &[Some(11), Some(11)]),
            ],
            &[&[1, 2], &[4, 5], &[7, 8]],
        ),
    ];
    for (rows, cycles) in cases {
        let (batch, schema) = tree(rows);
        let refs: Vec<usize> = (1..=rows[0].1.len()).collect();
        let order = order(&batch, &schema, &refs);
        assert_eq!(order.len(), rows.len(), "{rows:?}");
        assert!(order.last().unwrap().1, "the last row may end a push: {order:?}");
        // The rows between one push end and the next.
        let sets: Vec<Vec<u64>> = order
            .split_inclusive(|&(_, may_end)| may_end)
            .map(|s| s.iter().map(|&(r, _)| batch.pk(r as usize) as u64).collect())
            .collect();
        let set_of: HashMap<u64, usize> = sets
            .iter()
            .enumerate()
            .flat_map(|(i, s)| s.iter().map(move |&id| (id, i)))
            .collect();
        let mut together: Vec<Vec<u64>> = sets.into_iter().filter(|s| s.len() > 1).collect();
        together.iter_mut().for_each(|s| s.sort_unstable());
        together.sort();
        assert_eq!(together, cycles, "{rows:?}");
        for &(id, parents) in rows {
            for p in parents.iter().flatten().filter_map(|p| set_of.get(p)) {
                assert!(*p <= set_of[&id], "{id} ahead of a parent: {order:?}");
            }
        }
    }
}

/// With no referencing column, the rows in their own order, each free to end a push.
#[test]
fn rows_with_no_reference_keep_their_order() {
    let (batch, schema) = tree(&[(2, &[]), (1, &[]), (3, &[])]);
    assert_eq!(order(&batch, &schema, &[]), [(0, true), (1, true), (2, true)]);
}
