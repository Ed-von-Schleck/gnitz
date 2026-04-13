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
                    stack[sp] = right;
                    sp += 1;
                }
                let left = 2 * idx + 1;
                if left < heap.len() {
                    stack[sp] = left;
                    sp += 1;
                }
            }
        }
        self.min_indices.len()
    }
}
