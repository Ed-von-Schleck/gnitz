//! Generic min-heap for N-way merge operations.
//!
//! Two operations on the root drive every caller:
//! - `replace_top_key(new_key, less)` — overwrite `heap[0].key` and sift
//!   down. The fast path used by every emit; `new_key >= old_key` always
//!   (sources are sorted, advance produces ≥ keys), so sift-up is unreachable.
//! - `pop_top(less)` — remove `heap[0]`. swap_remove + sift down.
//!
//! `sift_down_hole` is the half-swap form: extract the moving node into a
//! local, shift the smaller child up into the hole one level at a time
//! (one write per level), then place the extracted node at the final hole
//! position. Vs full-swap sift down (three writes per level) this is the
//! constant-factor win that motivates the custom heap; std `BinaryHeap`
//! also uses the hole pattern but exposes neither replace-top nor a
//! min-heap variant without `Reverse` wrappers that obstruct inlining.
//!
//! No `pos_map`. No caller advances a non-root entry: ReadCursor folds
//! tied rows by repeatedly popping/replacing the root, and merge_batches
//! and compact peel the root one at a time. The slot-tracking that an
//! entry-indexed advance would need is therefore pure overhead and is gone.

#[derive(Clone, Copy)]
pub struct HeapNode {
    pub key: u128,
    pub source_idx: usize,
}

pub struct MergeHeap {
    pub heap: Vec<HeapNode>,
}

impl MergeHeap {
    /// Build a heap from `n` entries. `key_fn(i)` returns `Some(pk)` for
    /// valid entries and `None` for invalid/exhausted ones.
    pub fn build(
        n: usize,
        key_fn: impl Fn(usize) -> Option<u128>,
        less: impl Fn(&HeapNode, &HeapNode) -> bool,
    ) -> Self {
        let mut heap = Vec::with_capacity(n);
        for i in 0..n {
            if let Some(k) = key_fn(i) {
                heap.push(HeapNode { key: k, source_idx: i });
            }
        }
        let size = heap.len();
        for i in (0..size / 2).rev() {
            sift_down_hole(&mut heap, i, &less);
        }
        MergeHeap { heap }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    #[inline]
    pub fn peek(&self) -> &HeapNode {
        &self.heap[0]
    }

    /// Overwrite `heap[0].key` and sift the root down.
    /// Caller must guarantee `new_key >= old_key`.
    #[inline]
    pub fn replace_top_key(
        &mut self,
        new_key: u128,
        less: impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        debug_assert!(new_key >= self.heap[0].key, "replace_top_key: new < old");
        self.heap[0].key = new_key;
        sift_down_hole(&mut self.heap, 0, &less);
    }

    /// Remove `heap[0]`. After return, the new root (if any) is the next-min.
    #[inline]
    pub fn pop_top(
        &mut self,
        less: impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        debug_assert!(!self.heap.is_empty());
        let last = self.heap.len() - 1;
        if last == 0 {
            self.heap.pop();
            return;
        }
        self.heap[0] = self.heap[last];
        self.heap.pop();
        sift_down_hole(&mut self.heap, 0, &less);
    }
}

/// Hole-pattern sift down. Extract `heap[hole]` into `saved`, shift the
/// smaller child up one level at a time (one write per level), then place
/// `saved` at the final hole position.
#[inline]
fn sift_down_hole(
    heap: &mut [HeapNode],
    mut hole: usize,
    less: &impl Fn(&HeapNode, &HeapNode) -> bool,
) {
    let len = heap.len();
    debug_assert!(hole < len);
    let saved = heap[hole];
    loop {
        let left = 2 * hole + 1;
        if left >= len {
            break;
        }
        let right = left + 1;
        let child = if right < len && less(&heap[right], &heap[left]) {
            right
        } else {
            left
        };
        if !less(&heap[child], &saved) {
            break;
        }
        heap[hole] = heap[child];
        hole = child;
    }
    heap[hole] = saved;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn less(a: &HeapNode, b: &HeapNode) -> bool {
        a.key < b.key
    }

    fn build(keys: &[Option<u128>]) -> MergeHeap {
        MergeHeap::build(keys.len(), |i| keys[i], less)
    }

    /// Verify the binary-heap invariant: every parent is `<=` its children
    /// under `less`. (Equality is allowed.)
    fn assert_heap_invariant(h: &MergeHeap) {
        for i in 1..h.heap.len() {
            let parent = (i - 1) / 2;
            assert!(
                !less(&h.heap[i], &h.heap[parent]),
                "heap invariant violated at index {i}: child key {} < parent key {} (parent idx {parent})",
                h.heap[i].key,
                h.heap[parent].key,
            );
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
        assert_eq!(h.peek().key, 42);
        assert_eq!(h.peek().source_idx, 0);
    }

    #[test]
    fn build_all_exhausted() {
        let h = build(&[None, None, None]);
        assert!(h.is_empty());
    }

    #[test]
    fn build_some_exhausted() {
        // Entries: 0→30, 1→None, 2→10.  Only 0 and 2 are valid.
        let h = build(&[Some(30), None, Some(10)]);
        assert_eq!(h.heap.len(), 2);
        assert_eq!(h.peek().key, 10);
        assert_eq!(h.peek().source_idx, 2);
        assert_heap_invariant(&h);
    }

    #[test]
    fn build_min_at_root() {
        let h = build(&[Some(50), Some(40), Some(30), Some(20), Some(10)]);
        assert_eq!(h.peek().key, 10);
        assert_eq!(h.peek().source_idx, 4);
        assert_heap_invariant(&h);
    }

    // --- replace_top_key ---

    #[test]
    fn replace_top_sinks_below_sibling() {
        // Heap built from keys 10, 20, 30; entry 0 at root (key=10).
        // Replace root key with 25 — should sink below 20 but stay above 30.
        let mut h = build(&[Some(10), Some(20), Some(30)]);
        assert_eq!(h.peek().source_idx, 0);

        h.replace_top_key(25, less);

        assert_eq!(h.peek().key, 20);
        assert_eq!(h.peek().source_idx, 1);
        assert_heap_invariant(&h);
        // The advanced source's source_idx is preserved — it's now somewhere
        // below the root, but still source_idx=0.
        assert!(h.heap.iter().any(|n| n.source_idx == 0 && n.key == 25));
    }

    #[test]
    fn replace_top_to_max_sinks_to_leaf() {
        let mut h = build(&[Some(1), Some(2), Some(3)]);
        h.replace_top_key(1000, less);
        assert_eq!(h.peek().key, 2);
        assert_eq!(h.peek().source_idx, 1);
        assert_heap_invariant(&h);
    }

    #[test]
    fn replace_top_already_min_stays_root() {
        let mut h = build(&[Some(1), Some(5), Some(10)]);
        h.replace_top_key(3, less);
        assert_eq!(h.peek().key, 3);
        assert_eq!(h.peek().source_idx, 0);
        assert_heap_invariant(&h);
    }

    // --- pop_top ---

    #[test]
    fn pop_top_removes_root() {
        let mut h = build(&[Some(10), Some(20), Some(30)]);
        h.pop_top(less);

        assert_eq!(h.heap.len(), 2);
        assert_eq!(h.peek().key, 20);
        assert_heap_invariant(&h);
    }

    #[test]
    fn pop_top_single_entry_empties() {
        let mut h = build(&[Some(42)]);
        h.pop_top(less);
        assert!(h.is_empty());
    }

    #[test]
    fn pop_top_drains_in_sorted_order() {
        let mut h = build(&[Some(40), Some(10), Some(30), Some(20)]);
        let mut prev_min = 0u128;
        let mut n = 4;
        while !h.is_empty() {
            let k = h.peek().key;
            assert!(k >= prev_min, "drain order violated: {k} < {prev_min}");
            prev_min = k;
            h.pop_top(less);
            n -= 1;
            assert_eq!(h.heap.len(), n);
            assert_heap_invariant(&h);
        }
    }

    // --- mixed ops: replace, pop, replace ---

    #[test]
    fn mixed_ops_preserve_invariant() {
        let mut h = build(&[Some(5), Some(15), Some(25), Some(35), Some(45)]);
        // simulate an N-way merge over 5 sorted streams
        // step 1: root=5 (src 0), advance to 50
        h.replace_top_key(50, less);
        assert_heap_invariant(&h);
        assert_eq!(h.peek().key, 15);
        // step 2: root=15 (src 1), advance to 16
        h.replace_top_key(16, less);
        assert_heap_invariant(&h);
        assert_eq!(h.peek().key, 16);
        // step 3: root=16, exhaust
        h.pop_top(less);
        assert_heap_invariant(&h);
        assert_eq!(h.peek().key, 25);
        // step 4: drain
        let mut prev = 0u128;
        while !h.is_empty() {
            let k = h.peek().key;
            assert!(k >= prev);
            prev = k;
            h.pop_top(less);
            assert_heap_invariant(&h);
        }
    }

    /// Build a heap from a sequence of keys, then drain it, simulating
    /// a concurrent k-way merge by repeatedly popping the root. Verify
    /// the emitted key sequence is non-decreasing (the heap-min property).
    #[test]
    fn drain_emits_sorted() {
        let keys = [97u128, 12, 53, 88, 1, 44, 73, 25, 60, 32, 18, 91];
        let mut h = MergeHeap::build(keys.len(), |i| Some(keys[i]), less);

        let mut emitted = Vec::with_capacity(keys.len());
        while !h.is_empty() {
            emitted.push(h.peek().key);
            h.pop_top(less);
        }
        let mut sorted = keys.to_vec();
        sorted.sort();
        assert_eq!(emitted, sorted);
    }

    /// Simulate the read-cursor pattern: many sources, each contributing a
    /// sorted run; emit by repeatedly replacing root with the next key from
    /// the winning source and popping when a source exhausts. Output must
    /// equal the merged-and-sorted concatenation.
    #[test]
    fn k_way_merge_against_reference() {
        let runs: Vec<Vec<u128>> = vec![
            vec![1, 4, 7, 10, 13],
            vec![2, 5, 8, 11, 14],
            vec![3, 6, 9, 12, 15],
            vec![],                // exhausted source
            vec![0, 100, 200],
        ];
        let mut positions = vec![0usize; runs.len()];
        let mut h = MergeHeap::build(
            runs.len(),
            |i| runs[i].first().copied(),
            less,
        );

        let mut emitted = Vec::new();
        while !h.is_empty() {
            let src = h.peek().source_idx;
            let key = h.peek().key;
            emitted.push(key);
            positions[src] += 1;
            if positions[src] < runs[src].len() {
                h.replace_top_key(runs[src][positions[src]], less);
            } else {
                h.pop_top(less);
            }
        }

        let mut expected: Vec<u128> = runs.iter().flatten().copied().collect();
        expected.sort();
        assert_eq!(emitted, expected);
    }
}
