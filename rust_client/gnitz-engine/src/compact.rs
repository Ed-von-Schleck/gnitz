//! Self-contained shard compaction: N-way merge of sorted shard files.
//!
//! Replaces the RPython compactor.py + tournament_tree.py + shard_table.py (read) +
//! writer_table.py (write) + comparator.py for the compaction path.

use std::collections::HashMap;
use std::ffi::CStr;
use std::fs;
use std::os::unix::io::AsRawFd;
use std::ptr;

use crate::xxh;
use crate::xor8;

// ---------------------------------------------------------------------------
// Constants (from layout.py)
// ---------------------------------------------------------------------------

const SHARD_MAGIC: u64 = 0x31305F5A54494E47;
const SHARD_VERSION: u64 = 3;
const HEADER_SIZE: usize = 64;
const DIR_ENTRY_SIZE: usize = 24;
const ALIGNMENT: usize = 64;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 8;
const OFF_ROW_COUNT: usize = 16;
const OFF_DIR_OFFSET: usize = 24;
const OFF_TABLE_ID: usize = 32;
const OFF_XOR8_OFFSET: usize = 40;
const OFF_XOR8_SIZE: usize = 48;

// Type codes (from core/types.py)
const TYPE_U8: u8 = 1;
const TYPE_I8: u8 = 2;
const TYPE_U16: u8 = 3;
const TYPE_I16: u8 = 4;
const TYPE_U32: u8 = 5;
const TYPE_I32: u8 = 6;
const TYPE_F32: u8 = 7;
const TYPE_U64: u8 = 8;
const TYPE_I64: u8 = 9;
const TYPE_F64: u8 = 10;
const TYPE_STRING: u8 = 11;
const TYPE_U128: u8 = 12;

const SHORT_STRING_THRESHOLD: usize = 12;

// ---------------------------------------------------------------------------
// Schema descriptor (passed from RPython via FFI)
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct SchemaColumn {
    pub type_code: u8,
    pub size: u8,
    pub nullable: u8,
    pub _pad: u8,
}

#[repr(C)]
pub struct SchemaDescriptor {
    pub num_columns: u32,
    pub pk_index: u32,
    pub columns: [SchemaColumn; 64],
}

// ---------------------------------------------------------------------------
// Guard output result (returned from merge_and_route)
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct GuardResult {
    pub guard_key_lo: u64,
    pub guard_key_hi: u64,
    pub filename: [u8; 256], // null-terminated
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn align64(val: usize) -> usize {
    (val + ALIGNMENT - 1) & !(ALIGNMENT - 1)
}

fn read_u64_le(buf: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

fn read_i64_le(buf: &[u8], off: usize) -> i64 {
    i64::from_le_bytes(buf[off..off + 8].try_into().unwrap())
}

fn read_u32_le(buf: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(buf[off..off + 4].try_into().unwrap())
}

fn write_u64_le(buf: &mut [u8], off: usize, val: u64) {
    buf[off..off + 8].copy_from_slice(&val.to_le_bytes());
}

fn write_u32_le(buf: &mut [u8], off: usize, val: u32) {
    buf[off..off + 4].copy_from_slice(&val.to_le_bytes());
}

// ---------------------------------------------------------------------------
// Shard reader (mmap-based)
// ---------------------------------------------------------------------------

struct MappedShard {
    mmap_ptr: *mut u8,
    mmap_len: usize,
    data: &'static [u8], // slice over mmap — valid for struct lifetime
    count: usize,
    // Region slices (offsets into data)
    pk_lo_off: usize,
    pk_lo_len: usize,
    pk_hi_off: usize,
    pk_hi_len: usize,
    weight_off: usize,
    weight_len: usize,
    null_off: usize,
    null_len: usize,
    // Non-PK column regions: (offset, size) indexed by payload position
    col_regions: Vec<(usize, usize)>,
    // Blob heap
    blob_off: usize,
    blob_len: usize,
}

impl MappedShard {
    fn open(path: &CStr, schema: &SchemaDescriptor) -> Result<Self, i32> {
        let fd = unsafe { libc::open(path.as_ptr(), libc::O_RDONLY) };
        if fd < 0 {
            return Err(-1);
        }
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(fd, &mut st) } < 0 {
            unsafe { libc::close(fd); }
            return Err(-1);
        }
        let file_size = st.st_size as usize;
        if file_size < HEADER_SIZE {
            unsafe { libc::close(fd); }
            return Err(-2);
        }

        let mmap_ptr = unsafe {
            libc::mmap(
                ptr::null_mut(),
                file_size,
                libc::PROT_READ,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        unsafe { libc::close(fd); }
        if mmap_ptr == libc::MAP_FAILED {
            return Err(-1);
        }
        let mmap_ptr = mmap_ptr as *mut u8;
        let data = unsafe { std::slice::from_raw_parts(mmap_ptr, file_size) };

        // Validate header
        if read_u64_le(data, OFF_MAGIC) != SHARD_MAGIC {
            unsafe { libc::munmap(mmap_ptr as *mut libc::c_void, file_size); }
            return Err(-2);
        }
        if read_u64_le(data, OFF_VERSION) != SHARD_VERSION {
            unsafe { libc::munmap(mmap_ptr as *mut libc::c_void, file_size); }
            return Err(-2);
        }

        let count = read_u64_le(data, OFF_ROW_COUNT) as usize;
        let dir_off = read_u64_le(data, OFF_DIR_OFFSET) as usize;

        // Parse region directory
        let num_cols = schema.num_columns as usize;
        let num_non_pk = num_cols - 1; // PK column excluded from payload regions
        let num_regions = 4 + num_non_pk + 1; // pk_lo, pk_hi, weight, null, cols, blob

        let mut regions: Vec<(usize, usize)> = Vec::with_capacity(num_regions);
        for i in 0..num_regions {
            let entry_off = dir_off + i * DIR_ENTRY_SIZE;
            if entry_off + DIR_ENTRY_SIZE > file_size {
                unsafe { libc::munmap(mmap_ptr as *mut libc::c_void, file_size); }
                return Err(-2);
            }
            let r_off = read_u64_le(data, entry_off) as usize;
            let r_sz = read_u64_le(data, entry_off + 8) as usize;
            // Bounds check
            if r_off + r_sz > file_size {
                unsafe { libc::munmap(mmap_ptr as *mut libc::c_void, file_size); }
                return Err(-2);
            }
            regions.push((r_off, r_sz));
        }

        // Map non-PK column regions (skip PK column)
        let mut col_regions = Vec::with_capacity(num_non_pk);
        let mut reg_idx = 4; // after pk_lo, pk_hi, weight, null
        for ci in 0..num_cols {
            if ci == schema.pk_index as usize {
                continue;
            }
            col_regions.push(regions[reg_idx]);
            reg_idx += 1;
        }
        let blob_region = regions[reg_idx];

        Ok(MappedShard {
            mmap_ptr,
            mmap_len: file_size,
            data,
            count,
            pk_lo_off: regions[0].0,
            pk_lo_len: regions[0].1,
            pk_hi_off: regions[1].0,
            pk_hi_len: regions[1].1,
            weight_off: regions[2].0,
            weight_len: regions[2].1,
            null_off: regions[3].0,
            null_len: regions[3].1,
            col_regions,
            blob_off: blob_region.0,
            blob_len: blob_region.1,
        })
    }

    fn get_pk(&self, row: usize) -> u128 {
        let lo = read_u64_le(self.data, self.pk_lo_off + row * 8);
        let hi = read_u64_le(self.data, self.pk_hi_off + row * 8);
        ((hi as u128) << 64) | (lo as u128)
    }

    fn get_pk_lo(&self, row: usize) -> u64 {
        read_u64_le(self.data, self.pk_lo_off + row * 8)
    }

    fn get_pk_hi(&self, row: usize) -> u64 {
        read_u64_le(self.data, self.pk_hi_off + row * 8)
    }

    fn get_weight(&self, row: usize) -> i64 {
        read_i64_le(self.data, self.weight_off + row * 8)
    }

    fn get_null_word(&self, row: usize) -> u64 {
        read_u64_le(self.data, self.null_off + row * 8)
    }

    fn get_col_ptr(&self, row: usize, payload_col_idx: usize, col_size: usize) -> &[u8] {
        let (off, _) = self.col_regions[payload_col_idx];
        let start = off + row * col_size;
        &self.data[start..start + col_size]
    }

    fn blob_slice(&self) -> &[u8] {
        &self.data[self.blob_off..self.blob_off + self.blob_len]
    }
}

impl Drop for MappedShard {
    fn drop(&mut self) {
        if !self.mmap_ptr.is_null() {
            unsafe {
                libc::munmap(self.mmap_ptr as *mut libc::c_void, self.mmap_len);
            }
            self.mmap_ptr = ptr::null_mut();
        }
    }
}

// ---------------------------------------------------------------------------
// Shard cursor (position + ghost skip)
// ---------------------------------------------------------------------------

struct ShardCursor {
    shard_idx: usize,
    position: usize,
    count: usize,
}

impl ShardCursor {
    fn new(shard_idx: usize, shard: &MappedShard) -> Self {
        let mut c = ShardCursor {
            shard_idx,
            position: 0,
            count: shard.count,
        };
        c.skip_ghosts(shard);
        c
    }

    fn is_valid(&self) -> bool {
        self.position < self.count
    }

    fn advance(&mut self, shard: &MappedShard) {
        if self.is_valid() {
            self.position += 1;
            self.skip_ghosts(shard);
        }
    }

    fn skip_ghosts(&mut self, shard: &MappedShard) {
        while self.position < self.count {
            if shard.get_weight(self.position) != 0 {
                return;
            }
            self.position += 1;
        }
    }

    fn peek_key(&self, shard: &MappedShard) -> u128 {
        if self.is_valid() {
            shard.get_pk(self.position)
        } else {
            u128::MAX
        }
    }

    fn weight(&self, shard: &MappedShard) -> i64 {
        if self.is_valid() {
            shard.get_weight(self.position)
        } else {
            0
        }
    }
}

// ---------------------------------------------------------------------------
// Row comparison
// ---------------------------------------------------------------------------

fn compare_rows(
    schema: &SchemaDescriptor,
    shards: &[MappedShard],
    shard_a: usize,
    row_a: usize,
    shard_b: usize,
    row_b: usize,
) -> std::cmp::Ordering {
    let sa = &shards[shard_a];
    let sb = &shards[shard_b];
    let pk_index = schema.pk_index as usize;
    let mut payload_col_a: usize = 0;
    let mut payload_col_b: usize = 0;

    for ci in 0..schema.num_columns as usize {
        if ci == pk_index {
            continue;
        }

        // Null comparison: null < non-null, null == null
        let null_a = is_null(sa, row_a, ci, pk_index);
        let null_b = is_null(sb, row_b, ci, pk_index);
        if null_a && null_b {
            payload_col_a += 1;
            payload_col_b += 1;
            continue;
        }
        if null_a {
            return std::cmp::Ordering::Less;
        }
        if null_b {
            return std::cmp::Ordering::Greater;
        }

        let col = &schema.columns[ci];
        let col_size = col.size as usize;

        let ord = match col.type_code {
            TYPE_STRING => {
                let ptr_a = sa.get_col_ptr(row_a, payload_col_a, 16);
                let ptr_b = sb.get_col_ptr(row_b, payload_col_b, 16);
                compare_german_strings(ptr_a, sa.blob_slice(), ptr_b, sb.blob_slice())
            }
            TYPE_U128 => {
                let ba = sa.get_col_ptr(row_a, payload_col_a, 16);
                let bb = sb.get_col_ptr(row_b, payload_col_b, 16);
                let va = ((read_u64_le(ba, 8) as u128) << 64) | (read_u64_le(ba, 0) as u128);
                let vb = ((read_u64_le(bb, 8) as u128) << 64) | (read_u64_le(bb, 0) as u128);
                va.cmp(&vb)
            }
            TYPE_F64 => {
                let ba = sa.get_col_ptr(row_a, payload_col_a, 8);
                let bb = sb.get_col_ptr(row_b, payload_col_b, 8);
                let va = f64::from_bits(read_u64_le(ba, 0));
                let vb = f64::from_bits(read_u64_le(bb, 0));
                va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
            }
            TYPE_F32 => {
                let ba = sa.get_col_ptr(row_a, payload_col_a, 4);
                let bb = sb.get_col_ptr(row_b, payload_col_b, 4);
                let va = f32::from_bits(read_u32_le(ba, 0));
                let vb = f32::from_bits(read_u32_le(bb, 0));
                va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
            }
            _ => {
                // Signed integer comparison for all int types
                let va = read_signed(sa.get_col_ptr(row_a, payload_col_a, col_size), col_size);
                let vb = read_signed(sb.get_col_ptr(row_b, payload_col_b, col_size), col_size);
                va.cmp(&vb)
            }
        };

        payload_col_a += 1;
        payload_col_b += 1;

        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }

    std::cmp::Ordering::Equal
}

fn is_null(shard: &MappedShard, row: usize, col_idx: usize, pk_index: usize) -> bool {
    let null_word = shard.get_null_word(row);
    let payload_idx = if col_idx < pk_index { col_idx } else { col_idx - 1 };
    (null_word >> payload_idx) & 1 != 0
}

fn read_signed(bytes: &[u8], size: usize) -> i64 {
    match size {
        1 => bytes[0] as i8 as i64,
        2 => i16::from_le_bytes(bytes[..2].try_into().unwrap()) as i64,
        4 => i32::from_le_bytes(bytes[..4].try_into().unwrap()) as i64,
        8 => i64::from_le_bytes(bytes[..8].try_into().unwrap()),
        _ => 0,
    }
}

fn compare_german_strings(a: &[u8], blob_a: &[u8], b: &[u8], blob_b: &[u8]) -> std::cmp::Ordering {
    let len_a = read_u32_le(a, 0) as usize;
    let len_b = read_u32_le(b, 0) as usize;
    let min_len = len_a.min(len_b);

    // Compare prefix bytes (bytes 4..8 of the 16-byte struct)
    let pref_a = &a[4..8];
    let pref_b = &b[4..8];
    let limit = min_len.min(4);
    for i in 0..limit {
        if pref_a[i] != pref_b[i] {
            return pref_a[i].cmp(&pref_b[i]);
        }
    }

    if min_len <= 4 {
        return len_a.cmp(&len_b);
    }

    // Compare remaining bytes (after first 4)
    let data_a = string_data_ptr(a, blob_a, len_a);
    let data_b = string_data_ptr(b, blob_b, len_b);

    for i in 4..min_len {
        let ca = data_a(i);
        let cb = data_b(i);
        if ca != cb {
            return ca.cmp(&cb);
        }
    }
    len_a.cmp(&len_b)
}

/// Returns a closure that reads byte `i` of the string.
/// Short strings (≤12 bytes): bytes 4..12 are in the struct at offset 8.
/// Long strings (>12 bytes): bytes are in the blob heap at the stored offset.
fn string_data_ptr<'a>(
    struct_bytes: &'a [u8],
    blob: &'a [u8],
    length: usize,
) -> Box<dyn Fn(usize) -> u8 + 'a> {
    if length <= SHORT_STRING_THRESHOLD {
        // Inline: bytes 4+ are at struct offset 8, starting from byte 4 of the string
        Box::new(move |i: usize| struct_bytes[8 + (i - 4)])
    } else {
        // Heap: offset stored as u64 at struct offset 8
        let heap_offset = read_u64_le(struct_bytes, 8) as usize;
        Box::new(move |i: usize| blob[heap_offset + i])
    }
}

// ---------------------------------------------------------------------------
// Tournament tree (min-heap with u128 keys)
// ---------------------------------------------------------------------------

struct HeapNode {
    key: u128,
    cursor_idx: usize,
}

struct TournamentTree {
    heap: Vec<HeapNode>,
    pos_map: Vec<i32>,       // cursor_idx → heap position (-1 if not in heap)
    min_indices: Vec<usize>,  // reusable buffer for collect_min results
}

impl TournamentTree {
    fn build(cursors: &[ShardCursor], shards: &[MappedShard]) -> Self {
        let n = cursors.len();
        let mut heap = Vec::with_capacity(n);
        let mut pos_map = vec![-1i32; n];

        for i in 0..n {
            if cursors[i].is_valid() {
                let idx = heap.len();
                pos_map[i] = idx as i32;
                heap.push(HeapNode {
                    key: cursors[i].peek_key(&shards[cursors[i].shard_idx]),
                    cursor_idx: i,
                });
            }
        }

        let mut tree = TournamentTree {
            heap,
            pos_map,
            min_indices: Vec::with_capacity(n),
        };
        // Build heap (bottom-up)
        let size = tree.heap.len();
        if size > 1 {
            for i in (0..size / 2).rev() {
                tree.sift_down(i, shards, cursors);
            }
        }
        tree
    }

    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    fn min_key(&self) -> u128 {
        if self.heap.is_empty() { u128::MAX } else { self.heap[0].key }
    }

    fn collect_min_indices(
        &mut self,
        shards: &[MappedShard],
        cursors: &[ShardCursor],
        schema: &SchemaDescriptor,
    ) -> usize {
        self.min_indices.clear();
        if self.heap.is_empty() {
            return 0;
        }
        self.collect_equal(0, shards, cursors, schema);
        self.min_indices.len()
    }

    fn collect_equal(
        &mut self,
        idx: usize,
        shards: &[MappedShard],
        cursors: &[ShardCursor],
        schema: &SchemaDescriptor,
    ) {
        if idx >= self.heap.len() {
            return;
        }
        // Compare this node to root (node 0)
        if idx == 0 || self.compare_to_root(idx, shards, cursors, schema) == std::cmp::Ordering::Equal {
            self.min_indices.push(self.heap[idx].cursor_idx);
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            // Must call with concrete indices to avoid borrow issues
            if left < self.heap.len() {
                self.collect_equal(left, shards, cursors, schema);
            }
            if right < self.heap.len() {
                self.collect_equal(right, shards, cursors, schema);
            }
        }
        // If > root, prune entire subtree
    }

    fn compare_to_root(
        &self,
        idx: usize,
        shards: &[MappedShard],
        cursors: &[ShardCursor],
        schema: &SchemaDescriptor,
    ) -> std::cmp::Ordering {
        let a = &self.heap[idx];
        let b = &self.heap[0];
        // Primary key comparison
        let key_ord = a.key.cmp(&b.key);
        if key_ord != std::cmp::Ordering::Equal {
            return key_ord;
        }
        // Payload comparison
        let ca = &cursors[a.cursor_idx];
        let cb = &cursors[b.cursor_idx];
        compare_rows(
            schema, shards,
            ca.shard_idx, ca.position,
            cb.shard_idx, cb.position,
        )
    }

    fn advance_cursor(
        &mut self,
        cursor_idx: usize,
        cursors: &mut [ShardCursor],
        shards: &[MappedShard],
        schema: &SchemaDescriptor,
    ) {
        let heap_idx = self.pos_map[cursor_idx];
        if heap_idx < 0 {
            return;
        }
        let heap_idx = heap_idx as usize;

        let shard = &shards[cursors[cursor_idx].shard_idx];
        cursors[cursor_idx].advance(shard);

        if !cursors[cursor_idx].is_valid() {
            // Remove from heap
            self.pos_map[cursor_idx] = -1;
            let last = self.heap.len() - 1;
            if heap_idx != last {
                let last_cursor = self.heap[last].cursor_idx;
                self.heap[heap_idx] = HeapNode {
                    key: self.heap[last].key,
                    cursor_idx: last_cursor,
                };
                self.pos_map[last_cursor] = heap_idx as i32;
                self.heap.pop();
                if !self.heap.is_empty() && heap_idx < self.heap.len() {
                    self.sift_down(heap_idx, shards, &*cursors);
                    self.sift_up(heap_idx, shards, &*cursors);
                }
            } else {
                self.heap.pop();
            }
        } else {
            // Update key and sift down (new key > old key, can only move down)
            let shard = &shards[cursors[cursor_idx].shard_idx];
            self.heap[heap_idx].key = cursors[cursor_idx].peek_key(shard);
            self.sift_down(heap_idx, shards, &*cursors);
        }
    }

    fn compare_nodes(
        &self,
        i: usize,
        j: usize,
        shards: &[MappedShard],
        cursors: &[ShardCursor],
    ) -> std::cmp::Ordering {
        let a = &self.heap[i];
        let b = &self.heap[j];
        let key_ord = a.key.cmp(&b.key);
        if key_ord != std::cmp::Ordering::Equal {
            return key_ord;
        }
        // For sift operations, we only compare by key — not payload.
        // Payload comparison is only needed for collect_min_indices.
        // This matches the RPython behavior where _compare_nodes does
        // compare payloads, but for correctness of the heap property
        // only key ordering matters.
        std::cmp::Ordering::Equal
    }

    fn sift_down(&mut self, mut idx: usize, shards: &[MappedShard], cursors: &[ShardCursor]) {
        loop {
            let mut smallest = idx;
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            if left < self.heap.len() && self.compare_nodes(left, smallest, shards, cursors) == std::cmp::Ordering::Less {
                smallest = left;
            }
            if right < self.heap.len() && self.compare_nodes(right, smallest, shards, cursors) == std::cmp::Ordering::Less {
                smallest = right;
            }
            if smallest != idx {
                // Swap
                let ci = self.heap[idx].cursor_idx;
                let cs = self.heap[smallest].cursor_idx;
                self.heap.swap(idx, smallest);
                self.pos_map[ci] = smallest as i32;
                self.pos_map[cs] = idx as i32;
                idx = smallest;
            } else {
                break;
            }
        }
    }

    fn sift_up(&mut self, mut idx: usize, shards: &[MappedShard], cursors: &[ShardCursor]) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if self.compare_nodes(idx, parent, shards, cursors) == std::cmp::Ordering::Less {
                let ci = self.heap[idx].cursor_idx;
                let cp = self.heap[parent].cursor_idx;
                self.heap.swap(idx, parent);
                self.pos_map[ci] = parent as i32;
                self.pos_map[cp] = idx as i32;
                idx = parent;
            } else {
                break;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Shard writer
// ---------------------------------------------------------------------------

struct ShardWriter {
    pk_lo: Vec<u8>,
    pk_hi: Vec<u8>,
    weight: Vec<u8>,
    null_bitmap: Vec<u8>,
    col_bufs: Vec<Vec<u8>>,
    blob_heap: Vec<u8>,
    blob_cache: HashMap<(u64, usize), usize>,
    count: usize,
}

impl ShardWriter {
    fn new(schema: &SchemaDescriptor) -> Self {
        let mut col_bufs = Vec::with_capacity(schema.num_columns as usize);
        for ci in 0..schema.num_columns as usize {
            if ci == schema.pk_index as usize {
                col_bufs.push(Vec::new()); // placeholder, never written
            } else {
                col_bufs.push(Vec::with_capacity(1024 * schema.columns[ci].size as usize));
            }
        }
        ShardWriter {
            pk_lo: Vec::with_capacity(8 * 1024),
            pk_hi: Vec::with_capacity(8 * 1024),
            weight: Vec::with_capacity(8 * 1024),
            null_bitmap: Vec::with_capacity(8 * 1024),
            col_bufs,
            blob_heap: Vec::with_capacity(4096),
            blob_cache: HashMap::new(),
            count: 0,
        }
    }

    fn add_row(
        &mut self,
        key: u128,
        weight: i64,
        shard: &MappedShard,
        row: usize,
        schema: &SchemaDescriptor,
    ) {
        if weight == 0 {
            return;
        }
        self.count += 1;
        let key_lo = key as u64;
        let key_hi = (key >> 64) as u64;
        self.pk_lo.extend_from_slice(&key_lo.to_le_bytes());
        self.pk_hi.extend_from_slice(&key_hi.to_le_bytes());
        self.weight.extend_from_slice(&weight.to_le_bytes());

        let pk_index = schema.pk_index as usize;
        let mut null_word: u64 = 0;
        let mut payload_idx: usize = 0;

        for ci in 0..schema.num_columns as usize {
            if ci == pk_index {
                continue;
            }
            let is_null = is_null(shard, row, ci, pk_index);
            if is_null {
                let null_bit_pos = if ci < pk_index { ci } else { ci - 1 };
                null_word |= 1u64 << null_bit_pos;
            }
            self.write_column(ci, payload_idx, is_null, shard, row, schema);
            payload_idx += 1;
        }
        self.null_bitmap.extend_from_slice(&null_word.to_le_bytes());
    }

    fn write_column(
        &mut self,
        col_idx: usize,
        payload_col_idx: usize,
        is_null: bool,
        shard: &MappedShard,
        row: usize,
        schema: &SchemaDescriptor,
    ) {
        let col = &schema.columns[col_idx];
        let col_size = col.size as usize;
        let buf = &mut self.col_bufs[col_idx];

        if is_null {
            buf.extend(std::iter::repeat(0u8).take(col_size));
            return;
        }

        let src = shard.get_col_ptr(row, payload_col_idx, col_size);

        match col.type_code {
            TYPE_STRING => {
                self.write_string_column(col_idx, src, shard.blob_slice());
            }
            _ => {
                // All fixed-width types: direct copy
                buf.extend_from_slice(src);
            }
        }
    }

    fn write_string_column(&mut self, col_idx: usize, src_struct: &[u8], src_blob: &[u8]) {
        let length = read_u32_le(src_struct, 0) as usize;

        // Build the 16-byte German string struct
        let mut dest = [0u8; 16];
        // Length
        dest[0..4].copy_from_slice(&(length as u32).to_le_bytes());
        // Prefix
        dest[4..8].copy_from_slice(&src_struct[4..8]);

        if length <= SHORT_STRING_THRESHOLD {
            // Inline: copy suffix from struct bytes 8..16
            let suffix_len = if length > 4 { length - 4 } else { 0 };
            dest[8..8 + suffix_len].copy_from_slice(&src_struct[8..8 + suffix_len]);
        } else {
            // Heap: read old offset, get data, dedup into our blob heap
            let old_offset = read_u64_le(src_struct, 8) as usize;
            let src_data = &src_blob[old_offset..old_offset + length];
            let new_offset = self.get_or_append_blob(src_data);
            dest[8..16].copy_from_slice(&(new_offset as u64).to_le_bytes());
        }

        self.col_bufs[col_idx].extend_from_slice(&dest);
    }

    fn get_or_append_blob(&mut self, data: &[u8]) -> usize {
        if data.is_empty() {
            return 0;
        }
        let h = xxh::checksum(data);
        let cache_key = (h, data.len());
        if let Some(&existing_offset) = self.blob_cache.get(&cache_key) {
            // Collision check
            let existing = &self.blob_heap[existing_offset..existing_offset + data.len()];
            if existing == data {
                return existing_offset;
            }
        }
        let new_offset = self.blob_heap.len();
        self.blob_heap.extend_from_slice(data);
        self.blob_cache.insert(cache_key, new_offset);
        new_offset
    }

    fn finalize(
        &self,
        path: &CStr,
        table_id: u32,
        schema: &SchemaDescriptor,
    ) -> Result<(), i32> {
        // Build region list: pk_lo, pk_hi, weight, null, non-PK cols, blob
        let pk_index = schema.pk_index as usize;
        let mut regions: Vec<&[u8]> = Vec::new();
        regions.push(&self.pk_lo);
        regions.push(&self.pk_hi);
        regions.push(&self.weight);
        regions.push(&self.null_bitmap);
        for ci in 0..schema.num_columns as usize {
            if ci == pk_index {
                continue;
            }
            regions.push(&self.col_bufs[ci]);
        }
        regions.push(&self.blob_heap);

        let num_regions = regions.len();
        let dir_size = num_regions * DIR_ENTRY_SIZE;
        let dir_offset = HEADER_SIZE;

        // Compute total file size
        let mut pos = align64(dir_offset + dir_size);
        let mut region_offsets = Vec::with_capacity(num_regions);
        for r in &regions {
            region_offsets.push(pos);
            pos = align64(pos + r.len());
        }
        let data_end = if regions.is_empty() { HEADER_SIZE } else {
            region_offsets[num_regions - 1] + regions[num_regions - 1].len()
        };

        // Build XOR8 filter
        let xor8_filter = if self.count > 0 {
            xor8::build(
                unsafe { std::slice::from_raw_parts(self.pk_lo.as_ptr() as *const u64, self.count) },
                unsafe { std::slice::from_raw_parts(self.pk_hi.as_ptr() as *const u64, self.count) },
            )
        } else {
            None
        };

        let xor8_data = xor8_filter.as_ref().map(|f| xor8::serialize(f));
        let xor8_offset = if xor8_data.is_some() { align64(data_end) } else { 0 };
        let xor8_size = xor8_data.as_ref().map_or(0, |d| d.len());
        let total_size = if xor8_data.is_some() {
            xor8_offset + xor8_size
        } else {
            data_end
        };

        // Build file image
        let mut image = vec![0u8; total_size];

        // Header
        write_u64_le(&mut image, OFF_MAGIC, SHARD_MAGIC);
        write_u64_le(&mut image, OFF_VERSION, SHARD_VERSION);
        write_u64_le(&mut image, OFF_ROW_COUNT, self.count as u64);
        write_u64_le(&mut image, OFF_DIR_OFFSET, dir_offset as u64);
        write_u64_le(&mut image, OFF_TABLE_ID, table_id as u64);
        write_u64_le(&mut image, OFF_XOR8_OFFSET, xor8_offset as u64);
        write_u64_le(&mut image, OFF_XOR8_SIZE, xor8_size as u64);

        // Directory + regions
        for i in 0..num_regions {
            let r_off = region_offsets[i];
            let r_sz = regions[i].len();
            // Copy region data
            image[r_off..r_off + r_sz].copy_from_slice(regions[i]);
            // Compute checksum
            let cs = if r_sz > 0 { xxh::checksum(&image[r_off..r_off + r_sz]) } else { 0 };
            // Write directory entry
            let d = dir_offset + i * DIR_ENTRY_SIZE;
            write_u64_le(&mut image, d, r_off as u64);
            write_u64_le(&mut image, d + 8, r_sz as u64);
            write_u64_le(&mut image, d + 16, cs);
        }

        // XOR8 filter
        if let Some(ref data) = xor8_data {
            image[xor8_offset..xor8_offset + data.len()].copy_from_slice(data);
        }

        // Atomic write: .tmp → fsync → rename
        let path_str = path.to_str().unwrap_or("");
        let tmp_path = format!("{}.tmp", path_str);

        let file = fs::File::create(&tmp_path).map_err(|_| -3)?;
        use std::io::Write;
        (&file).write_all(&image).map_err(|_| -3)?;
        file.sync_all().map_err(|_| -3)?;
        drop(file);

        fs::rename(&tmp_path, path_str).map_err(|_| -3)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Guard key routing (binary search)
// ---------------------------------------------------------------------------

fn find_guard_for_key(guard_keys: &[(u64, u64)], key: u128) -> usize {
    let n = guard_keys.len();
    let mut lo = 0usize;
    let mut hi = n - 1;
    while lo < hi {
        let mid = (lo + hi + 1) / 2;
        let (gk_lo, gk_hi) = guard_keys[mid];
        let gk = ((gk_hi as u128) << 64) | (gk_lo as u128);
        if gk <= key {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    lo
}

// ---------------------------------------------------------------------------
// Entry points
// ---------------------------------------------------------------------------

pub fn compact_shards(
    input_files: &[&CStr],
    output_file: &CStr,
    schema: &SchemaDescriptor,
    table_id: u32,
) -> i32 {
    // Open all input shards
    let mut shards: Vec<MappedShard> = Vec::with_capacity(input_files.len());
    for f in input_files {
        match MappedShard::open(f, schema) {
            Ok(s) => shards.push(s),
            Err(e) => return e,
        }
    }

    // Create cursors
    let mut cursors: Vec<ShardCursor> = Vec::with_capacity(shards.len());
    for i in 0..shards.len() {
        cursors.push(ShardCursor::new(i, &shards[i]));
    }

    // Build tournament tree
    let mut tree = TournamentTree::build(&cursors, &shards);

    // Create writer
    let mut writer = ShardWriter::new(schema);

    // Merge loop
    while !tree.is_empty() {
        let min_key = tree.min_key();
        let num_min = tree.collect_min_indices(&shards, &cursors, schema);

        let mut net_weight: i64 = 0;
        for i in 0..num_min {
            let ci = tree.min_indices[i];
            net_weight += cursors[ci].weight(&shards[cursors[ci].shard_idx]);
        }

        if net_weight != 0 {
            let exemplar = tree.min_indices[0];
            let shard_idx = cursors[exemplar].shard_idx;
            let row = cursors[exemplar].position;
            writer.add_row(min_key, net_weight, &shards[shard_idx], row, schema);
        }

        // Advance all cursors at min — collect indices first to avoid borrow issues
        let indices: Vec<usize> = tree.min_indices[..num_min].to_vec();
        for ci in indices {
            tree.advance_cursor(ci, &mut cursors, &shards, schema);
        }
    }

    // Finalize output
    match writer.finalize(output_file, table_id, schema) {
        Ok(()) => 0,
        Err(e) => e,
    }
}

pub fn merge_and_route(
    input_files: &[&CStr],
    output_dir: &CStr,
    guard_keys: &[(u64, u64)],
    schema: &SchemaDescriptor,
    table_id: u32,
    level_num: u32,
    lsn_tag: u64,
    out_results: &mut [GuardResult],
) -> i32 {
    let num_guards = guard_keys.len();

    // Open all input shards
    let mut shards: Vec<MappedShard> = Vec::with_capacity(input_files.len());
    for f in input_files {
        match MappedShard::open(f, schema) {
            Ok(s) => shards.push(s),
            Err(e) => return e,
        }
    }

    // Create cursors
    let mut cursors: Vec<ShardCursor> = Vec::with_capacity(shards.len());
    for i in 0..shards.len() {
        cursors.push(ShardCursor::new(i, &shards[i]));
    }

    // Build tournament tree
    let mut tree = TournamentTree::build(&cursors, &shards);

    // Create writers (one per guard)
    let mut writers: Vec<ShardWriter> = Vec::with_capacity(num_guards);
    let out_dir_str = output_dir.to_str().unwrap_or("");
    let mut out_filenames: Vec<String> = Vec::with_capacity(num_guards);
    for i in 0..num_guards {
        writers.push(ShardWriter::new(schema));
        out_filenames.push(format!(
            "{}/shard_{}_{}_{}_G{}.db",
            out_dir_str,
            table_id,
            lsn_tag,
            format!("L{}", level_num),
            i
        ));
    }

    // Merge loop with routing
    while !tree.is_empty() {
        let min_key = tree.min_key();
        let num_min = tree.collect_min_indices(&shards, &cursors, schema);

        let mut net_weight: i64 = 0;
        for i in 0..num_min {
            let ci = tree.min_indices[i];
            net_weight += cursors[ci].weight(&shards[cursors[ci].shard_idx]);
        }

        if net_weight != 0 {
            let guard_idx = find_guard_for_key(guard_keys, min_key);
            let exemplar = tree.min_indices[0];
            let shard_idx = cursors[exemplar].shard_idx;
            let row = cursors[exemplar].position;
            writers[guard_idx].add_row(min_key, net_weight, &shards[shard_idx], row, schema);
        }

        let indices: Vec<usize> = tree.min_indices[..num_min].to_vec();
        for ci in indices {
            tree.advance_cursor(ci, &mut cursors, &shards, schema);
        }
    }

    // Finalize non-empty writers, build results
    let mut result_count: i32 = 0;
    for i in 0..num_guards {
        if writers[i].count > 0 {
            let cpath = std::ffi::CString::new(out_filenames[i].as_str()).unwrap();
            match writers[i].finalize(&cpath, table_id, schema) {
                Ok(()) => {
                    // Check file exists
                    if std::path::Path::new(&out_filenames[i]).exists() {
                        let ri = result_count as usize;
                        if ri < out_results.len() {
                            out_results[ri].guard_key_lo = guard_keys[i].0;
                            out_results[ri].guard_key_hi = guard_keys[i].1;
                            let name_bytes = out_filenames[i].as_bytes();
                            let len = name_bytes.len().min(255);
                            out_results[ri].filename[..len].copy_from_slice(&name_bytes[..len]);
                            out_results[ri].filename[len] = 0;
                        }
                        result_count += 1;
                    }
                }
                Err(e) => return e,
            }
        }
    }

    result_count
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Helper: build a minimal shard file in memory and write to disk
    fn write_test_shard(path: &str, pks: &[u64], weights: &[i64], schema: &SchemaDescriptor) {
        let count = pks.len();
        let pk_index = schema.pk_index as usize;
        let num_cols = schema.num_columns as usize;

        let mut writer = ShardWriter::new(schema);

        // For simplicity, create a mock MappedShard-like approach:
        // We'll build the shard using the writer directly
        for i in 0..count {
            let key = pks[i] as u128; // lo only, hi = 0
            writer.count += 1;
            writer.pk_lo.extend_from_slice(&pks[i].to_le_bytes());
            writer.pk_hi.extend_from_slice(&0u64.to_le_bytes());
            writer.weight.extend_from_slice(&weights[i].to_le_bytes());

            // Write non-PK columns with dummy data
            let mut null_word: u64 = 0;
            for ci in 0..num_cols {
                if ci == pk_index {
                    continue;
                }
                let col_size = schema.columns[ci].size as usize;
                // Write the PK value as column data (for testing)
                let mut val_bytes = vec![0u8; col_size];
                let pk_bytes = pks[i].to_le_bytes();
                let copy_len = col_size.min(8);
                val_bytes[..copy_len].copy_from_slice(&pk_bytes[..copy_len]);
                writer.col_bufs[ci].extend_from_slice(&val_bytes);
            }
            writer.null_bitmap.extend_from_slice(&null_word.to_le_bytes());
        }

        let cpath = std::ffi::CString::new(path).unwrap();
        writer.finalize(&cpath, 0, schema).unwrap();
    }

    fn make_test_schema() -> SchemaDescriptor {
        let mut s = SchemaDescriptor {
            num_columns: 2,
            pk_index: 0,
            columns: [SchemaColumn { type_code: 0, size: 0, nullable: 0, _pad: 0 }; 64],
        };
        s.columns[0] = SchemaColumn { type_code: TYPE_U64, size: 8, nullable: 0, _pad: 0 };
        s.columns[1] = SchemaColumn { type_code: TYPE_I64, size: 8, nullable: 0, _pad: 0 };
        s
    }

    #[test]
    fn test_compact_basic() {
        let dir = std::env::temp_dir().join("gnitz_compact_test_basic");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        // Shard 1: keys 1, 3, 5
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[1, 3, 5], &[1, 1, 1], &schema);

        // Shard 2: keys 2, 4, 6
        let s2 = dir.join("s2.db");
        write_test_shard(s2.to_str().unwrap(), &[2, 4, 6], &[1, 1, 1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        // Read back merged shard
        let merged = MappedShard::open(&cout, &schema).unwrap();
        assert_eq!(merged.count, 6);

        // Verify sorted order
        let mut prev = 0u128;
        for i in 0..merged.count {
            let pk = merged.get_pk(i);
            assert!(pk > prev, "not sorted at row {}: {} <= {}", i, pk, prev);
            prev = pk;
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_weight_elimination() {
        let dir = std::env::temp_dir().join("gnitz_compact_test_weight");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();

        // Shard 1: insert keys 1, 2, 3
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[1, 2, 3], &[1, 1, 1], &schema);

        // Shard 2: delete key 2 (weight = -1)
        let s2 = dir.join("s2.db");
        write_test_shard(s2.to_str().unwrap(), &[2], &[-1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cs2 = std::ffi::CString::new(s2.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str(), cs2.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        // Key 2 should be eliminated (net weight = 0)
        let merged = MappedShard::open(&cout, &schema).unwrap();
        assert_eq!(merged.count, 2);
        assert_eq!(merged.get_pk(0), 1);
        assert_eq!(merged.get_pk(1), 3);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_single_shard() {
        let dir = std::env::temp_dir().join("gnitz_compact_test_single");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[10, 20, 30], &[1, 1, 1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        let merged = MappedShard::open(&cout, &schema).unwrap();
        assert_eq!(merged.count, 3);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_compact_ghost_rows() {
        let dir = std::env::temp_dir().join("gnitz_compact_test_ghost");
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();

        let schema = make_test_schema();
        // Shard with ghost rows (weight=0)
        let s1 = dir.join("s1.db");
        write_test_shard(s1.to_str().unwrap(), &[1, 2, 3, 4, 5], &[1, 0, 1, 0, 1], &schema);

        let output = dir.join("merged.db");
        let cs1 = std::ffi::CString::new(s1.to_str().unwrap()).unwrap();
        let cout = std::ffi::CString::new(output.to_str().unwrap()).unwrap();

        let inputs = [cs1.as_c_str()];
        let rc = compact_shards(&inputs, &cout, &schema, 0);
        assert_eq!(rc, 0);

        let merged = MappedShard::open(&cout, &schema).unwrap();
        assert_eq!(merged.count, 3); // only keys 1, 3, 5

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_string_comparison() {
        // Short strings
        let mut a = [0u8; 16];
        let mut b = [0u8; 16];
        // len=3, prefix="abc"
        a[0..4].copy_from_slice(&3u32.to_le_bytes());
        a[4] = b'a'; a[5] = b'b'; a[6] = b'c';
        b[0..4].copy_from_slice(&3u32.to_le_bytes());
        b[4] = b'a'; b[5] = b'b'; b[6] = b'd';
        assert_eq!(compare_german_strings(&a, &[], &b, &[]), std::cmp::Ordering::Less);

        // Equal strings
        b[6] = b'c';
        assert_eq!(compare_german_strings(&a, &[], &b, &[]), std::cmp::Ordering::Equal);

        // Different length, same prefix
        b[0..4].copy_from_slice(&4u32.to_le_bytes());
        b[7] = b'z';
        assert_eq!(compare_german_strings(&a, &[], &b, &[]), std::cmp::Ordering::Less);
    }

    #[test]
    fn test_guard_routing() {
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 50), 0);
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 100), 1);
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 150), 1);
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 200), 2);
        assert_eq!(find_guard_for_key(&[(0, 0), (100, 0), (200, 0)], 999), 2);
    }
}
