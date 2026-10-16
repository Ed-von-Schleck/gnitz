//! The order a table's rows are restored in when the table references itself:
//! a push is checked against what is committed and what it carries itself, and
//! a large table takes more than one push, so every row's parents go in an
//! earlier push than it — or in the same one.

use std::collections::HashMap;

use gnitz_core::{Schema, ZSetBatch};

/// Rows of `batch`, each after every row it references through one of `edges`
/// — `(referencing column, referenced column)` pairs, both of `schema`. Rows on
/// a reference cycle come last.
pub(crate) fn order(batch: &ZSetBatch, schema: &Schema, edges: &[(usize, usize)]) -> Vec<u32> {
    let n = batch.len();
    let layout = schema.layout();
    // A cell as the key the engine checks a reference by, which compares equal
    // across a key and a payload column of one type.
    let cell = |ci: usize, row: usize| -> Option<u128> {
        let loc = layout.locate(ci);
        (!loc.is_null(batch, row)).then(|| loc.opk_image(batch, row))
    };
    // Per referenced column, which row holds each value: the column is unique.
    let mut holders: HashMap<usize, HashMap<u128, u32>> = HashMap::new();
    for &(_, parent_col) in edges {
        holders.entry(parent_col).or_insert_with(|| {
            (0..n)
                .filter_map(|row| cell(parent_col, row).map(|k| (k, row as u32)))
                .collect()
        });
    }
    let mut waiting = vec![0usize; n];
    let mut children: Vec<Vec<u32>> = vec![Vec::new(); n];
    for (row, waits) in waiting.iter_mut().enumerate() {
        for &(child_col, parent_col) in edges {
            let parent = cell(child_col, row).and_then(|k| holders[&parent_col].get(&k).copied());
            // A row referencing itself, or no row of this table, waits on nothing here.
            if let Some(p) = parent.filter(|&p| p as usize != row) {
                *waits += 1;
                children[p as usize].push(row as u32);
            }
        }
    }
    let mut out: Vec<u32> = (0..n as u32).filter(|&r| waiting[r as usize] == 0).collect();
    let mut next = 0;
    while let Some(&r) = out.get(next) {
        next += 1;
        for &c in &children[r as usize] {
            waiting[c as usize] -= 1;
            if waiting[c as usize] == 0 {
                out.push(c);
            }
        }
    }
    out.extend((0..n as u32).filter(|&r| waiting[r as usize] > 0));
    out
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;
