//! Generic min-heap for N-way merge operations.
//!
//! Replaces the duplicated TournamentTree (compact.rs, merge.rs) and
//! CursorTree (read_cursor.rs) with a single `MergeHeap`.

use std::cmp::Ordering;

// ---------------------------------------------------------------------------
// HeapNode + MergeHeap
// ---------------------------------------------------------------------------

pub struct HeapNode {
    pub key: u128,
    pub idx: usize,
}

pub struct MergeHeap {
    pub heap: Vec<HeapNode>,
    pub pos_map: Vec<i32>,
    pub min_indices: Vec<usize>,
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
            min_indices: Vec::with_capacity(n),
        };
        let size = h.heap.len();
        if size > 1 {
            for i in (0..size / 2).rev() {
                Self::sift_down_static(&mut h.heap, &mut h.pos_map, i, &less);
            }
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

    #[inline]
    pub fn min_key(&self) -> u128 {
        if self.heap.is_empty() {
            u128::MAX
        } else {
            self.heap[0].key
        }
    }

    /// Sift down operating on heap/pos_map directly — avoids &mut self borrow conflicts.
    pub fn sift_down_static(
        heap: &mut Vec<HeapNode>,
        pos_map: &mut Vec<i32>,
        mut idx: usize,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        loop {
            let mut smallest = idx;
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            if left < heap.len() && less(&heap[left], &heap[smallest]) {
                smallest = left;
            }
            if right < heap.len() && less(&heap[right], &heap[smallest]) {
                smallest = right;
            }
            if smallest != idx {
                let ci = heap[idx].idx;
                let cs = heap[smallest].idx;
                heap.swap(idx, smallest);
                pos_map[ci] = smallest as i32;
                pos_map[cs] = idx as i32;
                idx = smallest;
            } else {
                break;
            }
        }
    }

    /// Sift up operating on heap/pos_map directly.
    pub fn sift_up_static(
        heap: &mut Vec<HeapNode>,
        pos_map: &mut Vec<i32>,
        mut idx: usize,
        less: &impl Fn(&HeapNode, &HeapNode) -> bool,
    ) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if less(&heap[idx], &heap[parent]) {
                let ci = heap[idx].idx;
                let cp = heap[parent].idx;
                heap.swap(idx, parent);
                pos_map[ci] = parent as i32;
                pos_map[cp] = idx as i32;
                idx = parent;
            } else {
                break;
            }
        }
    }

    /// Advance a cursor/entry.  If `new_key` is `None` the entry is
    /// exhausted and removed from the heap; otherwise its key is updated
    /// and the heap re-established.
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
                // Remove from heap
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
        Self::collect_equal_static(&mut self.min_indices, &self.heap, 0, eq_root);
        self.min_indices.len()
    }

    fn collect_equal_static(
        min_indices: &mut Vec<usize>,
        heap: &[HeapNode],
        idx: usize,
        eq_root: &impl Fn(&HeapNode, &HeapNode) -> Ordering,
    ) {
        if idx >= heap.len() {
            return;
        }
        if idx == 0 || eq_root(&heap[idx], &heap[0]) == Ordering::Equal {
            min_indices.push(heap[idx].idx);
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            if left < heap.len() {
                Self::collect_equal_static(min_indices, heap, left, eq_root);
            }
            if right < heap.len() {
                Self::collect_equal_static(min_indices, heap, right, eq_root);
            }
        }
        // If > root, prune entire subtree
    }
}
