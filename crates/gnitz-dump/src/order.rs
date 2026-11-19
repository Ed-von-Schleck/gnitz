//! The order the rows of a row group too large for one push are restored in —
//! for a table that references itself, the whole table: a push is checked
//! against what is committed and what it carries itself, so every row's parents
//! go in an earlier push than it — or in the same one.

use std::collections::HashMap;

use gnitz_core::{Schema, ZSetBatch};

/// Rows of `batch`, each after every row it references through one of `refs` —
/// columns of `schema` that reference its lone key column — except where rows
/// reference one another round a cycle: those come together and go in one
/// push. Each row comes with whether a push may end after it, which is false
/// inside such a set. With no `refs`, the rows in their own order.
pub(crate) fn order(batch: &ZSetBatch, schema: &Schema, refs: &[usize]) -> Vec<(u32, bool)> {
    let n = batch.len();
    if refs.is_empty() {
        return (0..n as u32).map(|r| (r, true)).collect();
    }
    let layout = schema.layout();
    let batch = &batch.view();
    let ref_locs: Vec<_> = refs.iter().map(|&c| layout.locate(c)).collect();
    let k = ref_locs.len();
    // Row r's parents are parents[r * k..(r + 1) * k]. A NULL cell, or one naming
    // no row of this table, names r itself: a self-loop leaves its component as it is.
    let parents: Vec<u32> = {
        let key = layout.locate(schema.pk_cols()[0] as usize);
        let holder: HashMap<u128, u32> = (0..n).map(|r| (key.opk_image(batch, r), r as u32)).collect();
        let mut parents = Vec::with_capacity(n * k);
        for row in 0..n {
            for loc in &ref_locs {
                // The cell as the key the engine checks a reference by, which compares
                // equal across a key and a payload column of one type.
                let parent = (!loc.is_null(batch, row))
                    .then(|| loc.opk_image(batch, row))
                    .and_then(|key| holder.get(&key).copied());
                parents.push(parent.unwrap_or(row as u32));
            }
        }
        parents
    };

    // Tarjan's strongly connected components over the edges row → parent, with
    // an explicit stack, since a chain of references is as deep as the table.
    // A component is complete only once every component it references is, so
    // the order they complete in puts parents first.
    const UNSEEN: u32 = u32::MAX;
    let (mut index, mut low) = (vec![UNSEEN; n], vec![0u32; n]);
    let mut on_stack = vec![false; n];
    let (mut stack, mut dfs, mut out) = (Vec::new(), Vec::new(), Vec::with_capacity(n));
    let mut seen = 0u32;
    for root in 0..n {
        if index[root] != UNSEEN {
            continue;
        }
        // A frame is a row and its next edge.
        dfs.push((root, root * k));
        while let Some(&mut (v, ref mut edge)) = dfs.last_mut() {
            if index[v] == UNSEEN {
                (index[v], low[v]) = (seen, seen);
                seen += 1;
                stack.push(v as u32);
                on_stack[v] = true;
            }
            if *edge < (v + 1) * k {
                let p = parents[*edge] as usize;
                *edge += 1;
                if index[p] == UNSEEN {
                    dfs.push((p, p * k));
                } else if on_stack[p] {
                    low[v] = low[v].min(index[p]);
                }
                continue;
            }
            dfs.pop();
            if let Some(&(u, _)) = dfs.last() {
                low[u] = low[u].min(low[v]);
            }
            if low[v] == index[v] {
                // v's component is the stack down to v; a push may end after its last row.
                while let Some(r) = stack.pop() {
                    on_stack[r as usize] = false;
                    out.push((r, r as usize == v));
                    if r as usize == v {
                        break;
                    }
                }
            }
        }
    }
    out
}

#[cfg(test)]
#[path = "tests/order.rs"]
mod tests;
