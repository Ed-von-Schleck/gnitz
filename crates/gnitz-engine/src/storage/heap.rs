//! Generic min-heap for N-way merge operations.
//!
//! Replaces the duplicated TournamentTree (compact.rs, merge.rs) and
//! CursorTree (read_cursor.rs) with a single `MergeHeap`.

use std::cmp::Ordering;

pub struct HeapNode {
    pub key: u128,
    pub idx: usize,
}

pub struct MergeHeap {
    pub heap: Vec<HeapNode>,
    pub pos_map: Vec<i32>,
    pub min_indices: Vec<usize>,
}

#[inline]
fn swap_entries(heap: &mut [HeapNode], pos_map: &mut [i32], a: usize, b: usize) {
    let ai = heap[a].idx;
    let bi = heap[b].idx;
    heap.swap(a, b);
    pos_map[ai] = b as i32;
    pos_map[bi] = a as i32;
}

impl MergeHeap {
    /// Build a heap from `n` entries.  `key_fn(i)` returns `Some(pk)` for
    /// valid entries and `None` for invalid/exhausted ones.
    ///
    /// `less(a, b)` compares two `HeapNode` references and returns true if a < b.
    pub fn build(
        n: usize,
        key_fn: impl Fn(usize) -> Option<u128>,
        less: impl Fn(&HeapNode, &HeapNode) -> bool,
    ) -> Self {
        let mut heap = Vec::with_capacity(n);
        let mut pos_map = vec![-1i32; n];

        for i in 0..n {
            if let Some(k) = key_fn(i) {
                let idx = heap.len();
                pos_map[i] = idx as i32;
                heap.push(HeapNode { key: k, idx: i });
            }
        }

        let mut h = MergeHeap {
            heap,
            pos_map,
            min_indices: Vec::with_capacity(8),
        };
        let size = h.heap.len();
        for i in (0..size / 2).rev() {
            Self::sift_down_static(&mut h.heap, &mut h.pos_map, i, &less);
        }
        h
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    #[inline]
    pub fn min_idx(&self) -> usize {
        self.heap[0].idx
    }

    /// Sift down operating on heap/pos_map directly — avoids &mut self borrow conflicts.
    #[inline]
    pub fn sift_down_static(
        heap: &mut [HeapNode],
        pos_map: &mut [i32],
        mut idx: usize,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        let len = heap.len();
        loop {
            let left = 2 * idx + 1;
            if left >= len {
                break;
            }
            let right = left + 1;
            let child =
                if right < len && less(&heap[right], &heap[left]) { right } else { left };
            if !less(&heap[child], &heap[idx]) {
                break;
            }
            swap_entries(heap, pos_map, idx, child);
            idx = child;
        }
    }

    /// Sift up operating on heap/pos_map directly.
    #[inline]
    pub fn sift_up_static(
        heap: &mut [HeapNode],
        pos_map: &mut [i32],
        mut idx: usize,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if less(&heap[idx], &heap[parent]) {
                swap_entries(heap, pos_map, idx, parent);
                idx = parent;
            } else {
                break;
            }
        }
    }

    /// Advance a cursor/entry.  If `new_key` is `None` the entry is
    /// exhausted and removed from the heap; otherwise its key is updated
    /// and sifted down.
    ///
    /// Only sift-down is needed on the `Some` path: all callers are merge
    /// cursors advancing through sorted input, so the new key is always
    /// >= the old key.  Do NOT add sift-up here.
    pub fn advance(
        &mut self,
        entry_idx: usize,
        new_key: Option<u128>,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        let heap_idx = self.pos_map[entry_idx];
        if heap_idx < 0 {
            return;
        }
        let heap_idx = heap_idx as usize;

        match new_key {
            None => {
                // Removal: move last element into the vacated slot.  The
                // replacement has no ordering relationship to its new
                // neighbors, so both sift-down and sift-up are needed.
                self.pos_map[entry_idx] = -1;
                let last = self.heap.len() - 1;
                if heap_idx != last {
                    let last_entry = self.heap[last].idx;
                    self.heap[heap_idx] = HeapNode {
                        key: self.heap[last].key,
                        idx: last_entry,
                    };
                    self.pos_map[last_entry] = heap_idx as i32;
                    self.heap.pop();
                    if !self.heap.is_empty() && heap_idx < self.heap.len() {
                        Self::sift_down_static(&mut self.heap, &mut self.pos_map, heap_idx, less);
                        Self::sift_up_static(&mut self.heap, &mut self.pos_map, heap_idx, less);
                    }
                } else {
                    self.heap.pop();
                }
            }
            Some(k) => {
                self.heap[heap_idx].key = k;
                Self::sift_down_static(&mut self.heap, &mut self.pos_map, heap_idx, less);
            }
        }
    }

    /// Collect all entries equal to the root into `min_indices`.
    /// `eq_root(node_at_idx, root_node)` compares a heap node to the root
    /// and returns `Ordering::Equal` if the entry should be included.
    pub fn collect_min_indices(
        &mut self,
        eq_root: &impl Fn(&HeapNode, &HeapNode) -> Ordering,
    ) -> usize {
        self.min_indices.clear();
        if self.heap.is_empty() {
            return 0;
        }
        let heap = &self.heap;
        let mut stack = [0usize; 64];
        let mut sp = 1;
        stack[0] = 0;
        while sp > 0 {
            sp -= 1;
            let idx = stack[sp];
            if idx == 0 || eq_root(&heap[idx], &heap[0]) == Ordering::Equal {
                self.min_indices.push(heap[idx].idx);
                let right = 2 * idx + 2;
                if right < heap.len() {
                    debug_assert!(sp < stack.len(), "collect_min_indices stack overflow");
                    stack[sp] = right;
                    sp += 1;
                }
                let left = 2 * idx + 1;
                if left < heap.len() {
                    debug_assert!(sp < stack.len(), "collect_min_indices stack overflow");
                    stack[sp] = left;
                    sp += 1;
                }
            }
        }
        self.min_indices.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    fn less(a: &HeapNode, b: &HeapNode) -> bool { a.key < b.key }

    fn eq_root(a: &HeapNode, root: &HeapNode) -> Ordering { a.key.cmp(&root.key) }

    fn build(keys: &[Option<u128>]) -> MergeHeap {
        MergeHeap::build(keys.len(), |i| keys[i], less)
    }

    /// After every mutation, verify that pos_map[entry] == the heap position
    /// holding that entry, and heap[pos_map[entry]].idx == entry.
    fn assert_pos_map_consistent(h: &MergeHeap, n: usize) {
        for (hp, node) in h.heap.iter().enumerate() {
            assert_eq!(h.pos_map[node.idx], hp as i32, "heap[{hp}].idx={} but pos_map[{}]={}", node.idx, node.idx, h.pos_map[node.idx]);
        }
        for i in 0..n {
            let hp = h.pos_map[i];
            if hp >= 0 {
                assert_eq!(h.heap[hp as usize].idx, i, "pos_map[{i}]={hp} but heap[{hp}].idx={}", h.heap[hp as usize].idx);
            }
        }
    }

    // --- build ---

    #[test]
    fn build_empty() {
        let h = build(&[]);
        assert!(h.is_empty());
    }

    #[test]
    fn build_single() {
        let h = build(&[Some(42)]);
        assert!(!h.is_empty());
        assert_eq!(h.heap.len(), 1);
        assert_eq!(h.heap[0].key, 42);
        assert_eq!(h.min_idx(), 0);
        assert_eq!(h.pos_map[0], 0);
    }

    #[test]
    fn build_all_exhausted() {
        let h = build(&[None, None, None]);
        assert!(h.is_empty());
        // All pos_map entries should be -1.
        assert!(h.pos_map.iter().all(|&p| p == -1));
    }

    #[test]
    fn build_some_exhausted() {
        // Entries: 0→30, 1→None, 2→10.  Only 0 and 2 are valid.
        let h = build(&[Some(30), None, Some(10)]);
        assert_eq!(h.heap.len(), 2);
        assert_eq!(h.heap[0].key, 10);  // min at root
        assert_eq!(h.min_idx(), 2);     // entry 2 holds key=10
        assert_eq!(h.pos_map[1], -1);   // exhausted entry
        assert_pos_map_consistent(&h, 3);
    }

    #[test]
    fn build_min_at_root() {
        // 5 entries in reverse order: keys 50,40,30,20,10
        let h = build(&[Some(50), Some(40), Some(30), Some(20), Some(10)]);
        assert_eq!(h.heap[0].key, 10);
        assert_eq!(h.min_idx(), 4);
        assert_pos_map_consistent(&h, 5);
    }

    // --- advance(Some) ---

    #[test]
    fn advance_some_sinks_below_sibling() {
        // Heap built from keys 10, 20, 30; entry 0 at root (key=10).
        // Advance entry 0 to key=25 — it should sink below 20 but stay above 30.
        let mut h = build(&[Some(10), Some(20), Some(30)]);
        assert_eq!(h.min_idx(), 0);

        h.advance(0, Some(25), &less);

        assert_eq!(h.heap[0].key, 20);
        assert_eq!(h.min_idx(), 1);
        assert_pos_map_consistent(&h, 3);
    }

    #[test]
    fn advance_some_to_max_sinks_to_leaf() {
        let mut h = build(&[Some(1), Some(2), Some(3)]);
        h.advance(0, Some(1000), &less);
        assert_eq!(h.heap[0].key, 2);
        assert_eq!(h.min_idx(), 1);
        assert_pos_map_consistent(&h, 3);
    }

    #[test]
    fn advance_some_already_min_stays_root() {
        // Advance root to a key still smaller than all others.
        let mut h = build(&[Some(1), Some(5), Some(10)]);
        h.advance(0, Some(3), &less);
        assert_eq!(h.heap[0].key, 3);
        assert_eq!(h.min_idx(), 0);
        assert_pos_map_consistent(&h, 3);
    }

    // --- advance(None) ---

    #[test]
    fn advance_none_removes_root() {
        let mut h = build(&[Some(10), Some(20), Some(30)]);
        h.advance(0, None, &less);

        assert_eq!(h.heap.len(), 2);
        assert_eq!(h.pos_map[0], -1);
        assert_eq!(h.heap[0].key, 20);
        assert_pos_map_consistent(&h, 3);
    }

    #[test]
    fn advance_none_removes_non_root() {
        // Entry 2 (key=30) is a leaf; removing it should not disturb the root.
        let mut h = build(&[Some(10), Some(20), Some(30)]);
        h.advance(2, None, &less);

        assert_eq!(h.heap.len(), 2);
        assert_eq!(h.pos_map[2], -1);
        assert_eq!(h.heap[0].key, 10);
        assert_eq!(h.min_idx(), 0);
        assert_pos_map_consistent(&h, 3);
    }

    #[test]
    fn advance_none_single_entry() {
        let mut h = build(&[Some(42)]);
        h.advance(0, None, &less);
        assert!(h.is_empty());
    }

    #[test]
    fn advance_none_drains_all() {
        // Drain a 4-entry heap one-by-one; heap invariant must hold at each step.
        let mut h = build(&[Some(40), Some(10), Some(30), Some(20)]);
        let mut n = 4;
        let mut prev_min = 0u128;
        while !h.is_empty() {
            let min_key = h.heap[0].key;
            assert!(min_key >= prev_min, "heap order violated: {min_key} < {prev_min}");
            prev_min = min_key;
            let entry = h.min_idx();
            h.advance(entry, None, &less);
            n -= 1;
            assert_eq!(h.heap.len(), n);
            // We can't call assert_pos_map_consistent with the original n=4 here
            // because some entries' pos_map slots have been freed, so just check heap consistency.
            for (hp, node) in h.heap.iter().enumerate() {
                assert_eq!(h.pos_map[node.idx], hp as i32);
            }
        }
    }

    // --- collect_min_indices ---

    #[test]
    fn collect_min_indices_empty() {
        let mut h = build(&[]);
        assert_eq!(h.collect_min_indices(&eq_root), 0);
        assert!(h.min_indices.is_empty());
    }

    #[test]
    fn collect_min_indices_all_distinct() {
        let mut h = build(&[Some(10), Some(20), Some(30)]);
        assert_eq!(h.collect_min_indices(&eq_root), 1);
        assert_eq!(h.min_indices, vec![0]); // only entry 0 (key=10)
    }

    #[test]
    fn collect_min_indices_two_tied() {
        // Entries 0 and 1 both have key=10; entry 2 has key=20.
        let mut h = build(&[Some(10), Some(10), Some(20)]);
        let n = h.collect_min_indices(&eq_root);
        assert_eq!(n, 2);
        let mut got = h.min_indices.clone();
        got.sort();
        assert_eq!(got, vec![0, 1]);
    }

    #[test]
    fn collect_min_indices_all_equal() {
        let mut h = build(&[Some(5), Some(5), Some(5), Some(5)]);
        let n = h.collect_min_indices(&eq_root);
        assert_eq!(n, 4);
        let mut got = h.min_indices.clone();
        got.sort();
        assert_eq!(got, vec![0, 1, 2, 3]);
    }
}
