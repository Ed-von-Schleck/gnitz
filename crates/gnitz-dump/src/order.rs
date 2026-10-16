//! The order a table's rows are restored in when the table references itself
//! and takes more than one push: a push is checked against what is committed
//! and what it carries itself, so every row's parents go in an earlier push
//! than it — or in the same one.

use std::collections::HashMap;

use gnitz_core::{Schema, ZSetBatch};

/// Rows of `batch`, each after every row it references through one of `refs` —
/// columns of `schema` that reference its lone key column — and the position
/// from which the rest wait on a reference cycle: those rows, and every row that
/// waits on one, come last.
pub(crate) fn order(batch: &ZSetBatch, schema: &Schema, refs: &[usize]) -> (Vec<u32>, usize) {
    let n = batch.len();
    if refs.is_empty() {
        return ((0..n as u32).collect(), n);
    }
    let layout = schema.layout();
    let ref_locs: Vec<_> = refs.iter().map(|&c| layout.locate(c)).collect();
    let key = layout.locate(schema.pk_cols()[0] as usize);
    let holder: HashMap<u128, u32> = (0..n).map(|r| (key.opk_image(batch, r), r as u32)).collect();
    let mut waiting = vec![0usize; n];
    let mut children: Vec<Vec<u32>> = vec![Vec::new(); n];
    for (row, waits) in waiting.iter_mut().enumerate() {
        for loc in &ref_locs {
            // The cell as the key the engine checks a reference by, which compares
            // equal across a key and a payload column of one type.
            let parent = (!loc.is_null(batch, row))
                .then(|| loc.opk_image(batch, row))
                .and_then(|k| holder.get(&k).copied());
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
    let acyclic = out.len();
    out.extend((0..n as u32).filter(|&r| waiting[r as usize] > 0));
    (out, acyclic)
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;
