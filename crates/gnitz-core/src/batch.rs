//! The client batch: a Z-set in the canonical region order, its builders, and
//! its WAL-block encode and decode.
//!
//! A batch's fixed regions lie in one arena packed as a WAL block packs them, so a reply
//! block is a batch as it arrived: a lone block of a frame the transport handed over whole
//! keeps that frame as its buffer. The arena has no alignment; cells are read and written as
//! little-endian bytes.

use std::borrow::Cow;
use std::ops::Range;
use std::sync::Arc;

use gnitz_expr::SchemaDescriptor;
use gnitz_wire::wal::WAL_HEADER_SIZE;
use gnitz_wire::{FIXED_REGION_BYTES, MAX_PK_COLUMNS, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use gnitz_wire::{PkKeys, Regions, TypeCode, read_i64_le, read_u64_le, write_u64_le};

use crate::{ProtocolError, Schema};

/// A batch's PK region: `stride` bytes per row of **order-preserving key** (OPK,
/// §4) — the bytes the wire carries and the engine stores, so a client batch's
/// region and a `BatchBuilder`'s are byte-identical.
///
/// Private fields: routing hashes these bytes, so an un-encoded write would
/// mis-partition rather than error. Every append encodes or copies OPK.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PkColumn {
    /// `types[..n]` are the key columns' types in PK-list order; the rest is `U8`.
    types: [TypeCode; MAX_PK_COLUMNS],
    n: u8,
    /// The sum of the key columns' widths.
    stride: u8,
    buf: Vec<u8>,
}

impl PkColumn {
    /// Empty `PkColumn` holding `schema`'s key column types.
    pub fn empty_for_schema(schema: &Schema) -> Self {
        let layout = schema.layout();
        let mut types = [TypeCode::U8; MAX_PK_COLUMNS];
        for (t, c) in types.iter_mut().zip(layout.pk_columns()) {
            *t = c.type_code;
        }
        PkColumn {
            types,
            n: layout.pk_cols().len() as u8,
            stride: layout.pk_stride() as u8,
            buf: vec![],
        }
    }

    /// A column of `schema`'s single-column keys from their native values. The
    /// counterpart of [`Self::get`].
    pub fn from_natives(schema: &Schema, vals: impl IntoIterator<Item = u128>) -> Self {
        let mut c = Self::empty_for_schema(schema);
        assert_eq!(c.n, 1, "PkColumn::from_natives: a compound key takes push_natives");
        let (width, bias) = (c.stride(), gnitz_wire::opk_bias(c.types[0]));
        for v in vals {
            gnitz_wire::push_opk(&mut c.buf, width, v ^ bias);
        }
        c
    }

    /// A column of `schema`'s keys from each key column's native little-endian cells, in
    /// PK-list order: [`Self::push_natives`] over every row at once.
    pub fn from_native_columns(schema: &Schema, cols: &[Vec<u8>]) -> Self {
        let mut c = Self::empty_for_schema(schema);
        let types = c.types;
        let keys = &types[..c.n as usize];
        // Hard, not debug-only: a missing or short column would leave key bytes unencoded.
        assert_eq!(
            cols.len(),
            keys.len(),
            "from_native_columns: one cell column per key column"
        );
        let n = cols[0].len() / keys[0].wire_stride();
        let stride = c.stride();
        c.buf = vec![0; n * stride];
        let mut off = 0;
        for (tc, cells) in keys.iter().zip(cols) {
            let w = tc.wire_stride();
            assert_eq!(
                cells.len(),
                n * w,
                "from_native_columns: key columns of different lengths"
            );
            gnitz_wire::encode_pk_cells(cells, *tc, &mut c.buf, stride, off);
            off += w;
        }
        c
    }

    /// Bytes per row.
    #[inline]
    pub fn stride(&self) -> usize {
        self.stride as usize
    }

    /// The key columns' types in PK-list order.
    #[inline]
    fn key_types(&self) -> &[TypeCode] {
        &self.types[..self.n as usize]
    }

    /// These are keys of `layout`: its key column types, in PK-list order.
    fn has_keys_of(&self, layout: &SchemaDescriptor) -> bool {
        self.key_types()
            .iter()
            .copied()
            .eq(layout.pk_columns().map(|c| c.type_code))
    }

    /// The whole PK region: `len()` rows of `stride` OPK bytes.
    #[inline]
    pub fn region(&self) -> &[u8] {
        &self.buf
    }

    /// These keys as a read's key set.
    pub fn keys(&self) -> PkKeys {
        PkKeys::from_keys(self.stride(), self.buf.chunks_exact(self.stride()))
    }

    pub fn len(&self) -> usize {
        self.buf.len() / self.stride()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Row `i`'s native value. Defined only for a single-column key; a compound
    /// one is read as bytes.
    pub fn get(&self, i: usize) -> u128 {
        assert_eq!(self.n, 1, "PkColumn::get: a compound key has no scalar form");
        gnitz_wire::decode_opk_u128(self.get_bytes(i), self.types[0])
    }

    /// Borrow row `i`'s `stride` OPK bytes.
    #[inline(always)]
    pub fn get_bytes(&self, i: usize) -> &[u8] {
        let (s, buf): (usize, &[u8]) = (self.stride as usize, &self.buf);
        &buf[i * s..i * s + s]
    }

    /// Room for `n` more rows.
    pub fn reserve(&mut self, n: usize) {
        self.buf.reserve(n * self.stride());
    }

    /// Append one row from the PK columns' native values in PK-list order.
    pub fn push_natives(&mut self, natives: &[u128]) {
        check_key_arity(natives, self.n as usize);
        for (&tc, &v) in self.types.iter().zip(natives) {
            gnitz_wire::push_opk(&mut self.buf, tc.wire_stride(), gnitz_wire::key_image(tc, v));
        }
    }
}

/// Hard, not debug-only: a short list would encode a short key, and the next column's
/// bytes would land in its place.
fn check_key_arity(natives: &[u128], n: usize) {
    assert_eq!(natives.len(), n, "push_natives: one native value per key column");
}

/// `a` and `b` have the same key column types, in PK-list order.
fn same_key_types(a: &SchemaDescriptor, b: &SchemaDescriptor) -> bool {
    (a.pk_columns().map(|c| c.type_code)).eq(b.pk_columns().map(|c| c.type_code))
}

/// Write the OPK bytes of a key given as its columns' native values, in PK-list order, into
/// `dst`, one key wide.
fn store_key_natives(layout: &SchemaDescriptor, dst: &mut [u8], natives: &[u128]) {
    check_key_arity(natives, layout.pk_cols().len());
    let mut at = 0;
    for (c, &v) in layout.pk_columns().zip(natives) {
        let w = c.size();
        gnitz_wire::store_opk(&mut dst[at..at + w], gnitz_wire::key_image(c.type_code, v));
        at += w;
    }
}

/// A batch in the canonical region order: every payload slot is its own wire region,
/// and a STRING/BLOB slot holds 16-byte German-string cells against [`Self::heap`].
pub struct ZSetBatch {
    layout: Arc<SchemaDescriptor>,
    /// The arena at `base`: region `r` starts `region_start(r, cap)` bytes past it.
    buf: Vec<u8>,
    base: usize,
    /// The rows each region of the arena has room for.
    cap: usize,
    rows: usize,
    heap: Heap,
}

/// Where a batch's heap lies.
enum Heap {
    /// As it arrived: these bytes of the batch's buffer, behind the arena.
    Received(Range<usize>),
    /// A buffer of its own, which writes grow.
    Owned(Vec<u8>),
}

/// The fewest rows an arena grows to when rows are appended one at a time.
const MIN_GROWTH_ROWS: usize = 4;

impl ZSetBatch {
    pub fn new(schema: &Schema) -> Self {
        Self::of_layout(schema.shared_layout())
    }

    fn of_layout(layout: &Arc<SchemaDescriptor>) -> Self {
        ZSetBatch {
            layout: Arc::clone(layout),
            buf: Vec::new(),
            base: 0,
            cap: 0,
            rows: 0,
            heap: Heap::Owned(Vec::new()),
        }
    }

    /// An empty batch whose arena has room for `n` rows.
    pub fn with_capacity(schema: &Schema, n: usize) -> Self {
        let mut b = Self::new(schema);
        b.reserve(n);
        b
    }

    /// `n` rows of zero bytes: weight 0, no NULL, every cell zero.
    pub fn zeroed(schema: &Schema, n: usize) -> Self {
        let mut b = Self::with_capacity(schema, n);
        b.rows = n;
        b
    }

    /// The `-1` retraction batch for `pks`: the server's unique-PK rule
    /// retracts by PK alone, so the payload columns are inert filler.
    pub fn retraction(schema: &Schema, pks: &PkColumn) -> Self {
        assert!(pks.has_keys_of(schema.layout()), "retraction: key layout mismatch");
        Self::retraction_of(schema.shared_layout(), pks.region())
    }

    /// The `-1` retraction batch of `schema` for this batch's keys, as [`Self::retraction`].
    pub fn key_retraction(&self, schema: &Schema) -> Self {
        assert!(
            same_key_types(&self.layout, schema.layout()),
            "key_retraction: key layout mismatch"
        );
        Self::retraction_of(schema.shared_layout(), self.pk_region())
    }

    fn retraction_of(layout: &Arc<SchemaDescriptor>, keys: &[u8]) -> Self {
        let mut b = Self::of_layout(layout);
        b.append_keys(keys, -1);
        b
    }

    /// Append one row per key of `keys`, OPK bytes of this batch's keys back to back, each
    /// at `weight`, every payload cell zero and no NULL.
    pub(crate) fn append_keys(&mut self, keys: &[u8], weight: i64) {
        let stride = self.pk_stride();
        assert!(keys.len().is_multiple_of(stride), "append_keys: a partial key");
        let (at, n) = (self.rows, keys.len() / stride);
        self.reserve(n);
        self.rows += n;
        for r in 0..self.layout.num_fixed_regions() {
            let w = self.layout.region_stride(r);
            let s = self.region_at(r) + at * w;
            let dst = &mut self.buf[s..s + n * w];
            match r {
                REG_PK => dst.copy_from_slice(keys),
                REG_WEIGHT => dst.as_chunks_mut::<FIXED_REGION_BYTES>().0.fill(weight.to_le_bytes()),
                _ => dst.fill(0),
            }
        }
    }

    // ── Where the regions lie ───────────────────────────────────────────────

    /// The byte of `buf` region `r` starts at.
    #[inline(always)]
    fn region_at(&self, r: usize) -> usize {
        self.base + self.layout.region_start(r, self.cap)
    }

    /// Region `r`'s cells of every row.
    #[inline(always)]
    fn region(&self, r: usize) -> &[u8] {
        let s = self.region_at(r);
        &self.buf[s..s + self.rows * self.layout.region_stride(r)]
    }

    #[inline(always)]
    fn region_mut(&mut self, r: usize) -> &mut [u8] {
        let (s, len) = (self.region_at(r), self.rows * self.layout.region_stride(r));
        &mut self.buf[s..s + len]
    }

    /// The bytes of row `row`'s cell in region `r`; `row` may be the open row past the last,
    /// within the arena.
    #[inline(always)]
    fn cell_range(&self, r: usize, row: usize) -> Range<usize> {
        debug_assert!(row < self.cap, "row {row} past the arena's {} rows", self.cap);
        let w = self.layout.region_stride(r);
        let s = self.region_at(r) + row * w;
        s..s + w
    }

    /// The type of payload slot `pi`.
    #[inline]
    fn payload_tc(&self, pi: usize) -> TypeCode {
        self.layout.columns()[self.layout.payload_col_idx(pi)].type_code
    }

    /// Move every region into a new arena of `cap` rows at base 0.
    fn grow_to(&mut self, cap: usize) {
        debug_assert!(cap >= self.rows);
        let mut buf = vec![0u8; cap * self.layout.row_width()];
        if self.rows > 0 {
            for r in 0..self.layout.num_fixed_regions() {
                let len = self.rows * self.layout.region_stride(r);
                let (from, to) = (self.region_at(r), self.layout.region_start(r, cap));
                buf[to..to + len].copy_from_slice(&self.buf[from..from + len]);
            }
        }
        self.heap = Heap::Owned(self.take_heap());
        self.buf = buf;
        self.base = 0;
        self.cap = cap;
    }

    /// Room for `n` more rows in the arena: exact when it is empty, at least doubling after.
    pub fn reserve(&mut self, n: usize) {
        let need = self.rows + n;
        if need > self.cap {
            let want = if self.cap == 0 { need } else { need.max(self.cap * 2) };
            self.grow_to(self.layout.arena_rows(want));
        }
    }

    /// Room for one more row, for a builder that appends rows one at a time.
    fn reserve_one(&mut self) {
        if self.rows == self.cap {
            self.grow_to(self.layout.arena_rows((self.cap * 2).max(MIN_GROWTH_ROWS)));
        }
    }

    // ── Reads ───────────────────────────────────────────────────────────────

    pub fn len(&self) -> usize {
        self.rows
    }

    pub fn is_empty(&self) -> bool {
        self.rows == 0
    }

    /// Row `row`'s weight.
    #[inline]
    pub fn weight(&self, row: usize) -> i64 {
        assert!(row < self.rows, "weight: row {row} of {}", self.rows);
        read_i64_le(&self.buf, self.region_at(REG_WEIGHT) + row * FIXED_REGION_BYTES)
    }

    /// Every row's weight, in row order.
    pub fn weights(&self) -> impl ExactSizeIterator<Item = i64> + '_ {
        let words = self.region(REG_WEIGHT).as_chunks::<FIXED_REGION_BYTES>().0;
        words.iter().map(|w| i64::from_le_bytes(*w))
    }

    /// Row `row`'s null word: bit `pi` set where payload slot `pi` is NULL.
    #[inline]
    pub fn null_word(&self, row: usize) -> u64 {
        assert!(row < self.rows, "null_word: row {row} of {}", self.rows);
        read_u64_le(&self.buf, self.region_at(REG_NULL_BMP) + row * FIXED_REGION_BYTES)
    }

    /// Every row's null word, in row order.
    pub fn null_words(&self) -> impl ExactSizeIterator<Item = u64> + '_ {
        let words = self.region(REG_NULL_BMP).as_chunks::<FIXED_REGION_BYTES>().0;
        words.iter().map(|w| u64::from_le_bytes(*w))
    }

    /// The whole PK region: `len()` rows of `pk_stride()` OPK bytes.
    #[inline]
    pub fn pk_region(&self) -> &[u8] {
        self.region(REG_PK)
    }

    /// Bytes of one row's key.
    #[inline]
    pub fn pk_stride(&self) -> usize {
        self.layout.pk_stride()
    }

    /// Row `row`'s key, its OPK bytes.
    #[inline(always)]
    pub fn pk_bytes(&self, row: usize) -> &[u8] {
        assert!(row < self.rows, "pk_bytes: row {row} of {}", self.rows);
        &self.buf[self.cell_range(REG_PK, row)]
    }

    /// Row `row`'s key as its native value. Defined only for a single-column key; a compound
    /// one is read as bytes.
    pub fn pk(&self, row: usize) -> u128 {
        let ci = (self.layout.lone_pk_col()).expect("ZSetBatch::pk: a compound key has no scalar form");
        gnitz_wire::decode_opk_u128(self.pk_bytes(row), self.layout.columns()[ci].type_code)
    }

    /// The keys as a read's key set.
    pub fn pk_keys(&self) -> PkKeys {
        PkKeys::from_keys(self.pk_stride(), self.pk_region().chunks_exact(self.pk_stride()))
    }

    /// Payload slots.
    pub fn payload_count(&self) -> usize {
        self.layout.num_payload_cols()
    }

    /// Payload slot `pi`'s cells of every row, its type's wire stride each.
    #[inline]
    pub fn col(&self, pi: usize) -> &[u8] {
        self.region(REG_PAYLOAD_START + pi)
    }

    /// The heap the German cells point into.
    #[inline(always)]
    pub fn heap(&self) -> &[u8] {
        match &self.heap {
            Heap::Received(at) => &self.buf[at.clone()],
            Heap::Owned(heap) => heap,
        }
    }

    /// Indices of the live rows — those with positive weight. A `ZSetBatch`
    /// row with weight ≤ 0 is a retraction/ghost, not a present element, so a
    /// catalog scan over a batch iterates only its live rows.
    pub(crate) fn live_rows(&self) -> impl Iterator<Item = usize> {
        self.weights().enumerate().filter(|&(_, w)| w > 0).map(|(i, _)| i)
    }

    // ── Writes ──────────────────────────────────────────────────────────────

    pub fn set_weight(&mut self, row: usize, w: i64) {
        assert!(row < self.rows, "set_weight: row {row} of {}", self.rows);
        let at = self.region_at(REG_WEIGHT) + row * FIXED_REGION_BYTES;
        write_u64_le(&mut self.buf, at, w as u64);
    }

    /// Every row at weight `w`.
    pub fn fill_weights(&mut self, w: i64) {
        for c in self.region_mut(REG_WEIGHT).as_chunks_mut::<FIXED_REGION_BYTES>().0 {
            *c = w.to_le_bytes();
        }
    }

    pub fn set_null_word(&mut self, row: usize, w: u64) {
        assert!(row < self.rows, "set_null_word: row {row} of {}", self.rows);
        let at = self.region_at(REG_NULL_BMP) + row * FIXED_REGION_BYTES;
        write_u64_le(&mut self.buf, at, w);
    }

    /// `f` of each row's index and null word, to rewrite the word in place.
    pub fn update_null_words(&mut self, mut f: impl FnMut(usize, &mut u64)) {
        for (row, c) in self
            .region_mut(REG_NULL_BMP)
            .as_chunks_mut::<FIXED_REGION_BYTES>()
            .0
            .iter_mut()
            .enumerate()
        {
            let mut w = u64::from_le_bytes(*c);
            f(row, &mut w);
            *c = w.to_le_bytes();
        }
    }

    /// Payload slot `pi`'s cells of every row, to write in place.
    #[inline]
    pub fn col_mut(&mut self, pi: usize) -> &mut [u8] {
        self.region_mut(REG_PAYLOAD_START + pi)
    }

    /// Payload slot `pi`'s cells and the heap, to write German cells into.
    pub fn col_and_heap_mut(&mut self, pi: usize) -> (&mut [u8], &mut Vec<u8>) {
        let (s, len) = (
            self.region_at(REG_PAYLOAD_START + pi),
            self.rows * self.layout.region_stride(REG_PAYLOAD_START + pi),
        );
        let (buf, heap) = self.buf_and_heap();
        (&mut buf[s..s + len], heap)
    }

    /// The heap, to append to: a received heap is first copied out of the buffer it
    /// arrived in.
    fn heap_mut(&mut self) -> &mut Vec<u8> {
        self.buf_and_heap().1
    }

    /// The buffer and the heap, the heap made [`Heap::Owned`].
    fn buf_and_heap(&mut self) -> (&mut Vec<u8>, &mut Vec<u8>) {
        if let Heap::Received(at) = &self.heap {
            self.heap = Heap::Owned(self.buf[at.clone()].to_vec());
        }
        match &mut self.heap {
            Heap::Owned(heap) => (&mut self.buf, heap),
            Heap::Received(_) => unreachable!("the heap was just copied out"),
        }
    }

    /// Replace the heap with `heap`, which every German cell now points into.
    pub fn set_heap(&mut self, heap: Vec<u8>) {
        self.heap = Heap::Owned(heap);
    }

    /// The heap, taken out of the batch; a received one is copied.
    pub fn take_heap(&mut self) -> Vec<u8> {
        match std::mem::replace(&mut self.heap, Heap::Owned(Vec::new())) {
            Heap::Received(at) => self.buf[at].to_vec(),
            Heap::Owned(heap) => heap,
        }
    }

    /// Overwrite every row's key with `keys`, one key per row.
    pub fn set_keys(&mut self, keys: &PkColumn) {
        assert!(keys.has_keys_of(&self.layout), "set_keys: key layout mismatch");
        self.region_mut(REG_PK).copy_from_slice(keys.region());
    }

    /// Overwrite every row's key and weight with `src`'s, row for row: both hold as many
    /// rows, under one key layout.
    pub fn copy_keys_and_weights(&mut self, src: &ZSetBatch) {
        assert!(
            self.rows == src.rows && same_key_types(&self.layout, &src.layout),
            "copy_keys_and_weights: rows or key layout mismatch"
        );
        for r in [REG_PK, REG_WEIGHT] {
            let (at, len) = (self.region_at(r), self.rows * self.layout.region_stride(r));
            self.buf[at..at + len].copy_from_slice(src.region(r));
        }
    }

    /// Overwrite every row's null word with `src`'s, row for row: both hold as many rows.
    pub fn copy_null_words(&mut self, src: &ZSetBatch) {
        self.region_mut(REG_NULL_BMP).copy_from_slice(src.region(REG_NULL_BMP));
    }

    /// Overwrite the STRING cell at `(row, pi)`. The replaced cell's spill, if any,
    /// stays in the heap unreferenced.
    pub(crate) fn set_string_cell(&mut self, row: usize, pi: usize, v: &str) {
        let cell = gnitz_wire::encode_german_string(v.as_bytes(), self.heap_mut());
        self.col_mut(pi)[row * 16..(row + 1) * 16].copy_from_slice(&cell);
    }

    /// Overwrite the 8-byte fixed-width cell at `(row, pi)` with `v`, little-endian.
    pub(crate) fn set_u64_cell(&mut self, row: usize, pi: usize, v: u64) {
        write_u64_le(self.col_mut(pi), row * 8, v);
    }

    /// Open one row past the last, every cell of it zero, to write through the returned
    /// writer; the row is the batch's once [`RowWriter::commit`] closes it.
    pub fn open_row(&mut self) -> RowWriter<'_> {
        self.reserve_one();
        RowWriter { batch: self }
    }

    /// Zero every cell of rows `from..to` of the arena, rows past the last: the arena's rows
    /// past the last are kept zero, so an opened row starts zero with no write.
    fn zero_rows_past_last(&mut self, from: usize, to: usize) {
        debug_assert!(self.rows <= from && to <= self.cap);
        for r in 0..self.layout.num_fixed_regions() {
            let (at, w) = (self.region_at(r), self.layout.region_stride(r));
            self.buf[at + from * w..at + to * w].fill(0);
        }
    }

    // ── Rows from other batches ─────────────────────────────────────────────

    /// Append row `i` of `src` to `self` at `weight`, verbatim — so a `-1`
    /// reproduces the stored row rather than relying on a decoder and an encoder
    /// agreeing. `src` and `self` must share a layout. A German cell is relocated
    /// onto this batch's heap: its heap offset is relative to `src`'s.
    pub fn copy_row_at(&mut self, src: &ZSetBatch, i: usize, weight: i64) {
        debug_assert!(self.same_layout(src), "copy_row_at: layout mismatch");
        assert!(i < src.rows, "copy_row_at: row {i} of {}", src.rows);
        self.reserve_one();
        let row = self.rows;
        let strings = self.string_regions();
        for r in 0..self.layout.num_fixed_regions() {
            let (to, from) = (self.cell_range(r, row), src.cell_range(r, i));
            if strings >> r & 1 != 0 {
                let cell = src.buf[from].first_chunk().expect("a German cell");
                let (buf, heap) = self.buf_and_heap();
                buf[to].copy_from_slice(&gnitz_wire::relocate_german_string(cell, src.heap(), heap));
            } else {
                self.buf[to].copy_from_slice(&src.buf[from]);
            }
        }
        self.rows += 1;
        self.set_weight(row, weight);
    }

    /// Bit `r` set for every fixed region `r` of German cells.
    #[inline]
    fn string_regions(&self) -> u128 {
        u128::from(self.layout.string_payload_slots()) << REG_PAYLOAD_START
    }

    /// Overwrite every cell of payload slot `dst_pi` with `src`'s slot `src_pi`, row for row;
    /// both hold as many rows. A German cell is relocated onto this batch's heap.
    pub fn copy_column_from(&mut self, dst_pi: usize, src: &ZSetBatch, src_pi: usize) {
        debug_assert_eq!(
            self.payload_tc(dst_pi),
            src.payload_tc(src_pi),
            "copy_column_from: slot type mismatch"
        );
        assert_eq!(self.rows, src.rows, "copy_column_from: row count mismatch");
        let from = src.col(src_pi);
        if self.payload_tc(dst_pi).is_german_string() {
            let (dst, heap) = self.col_and_heap_mut(dst_pi);
            for (d, s) in dst.as_chunks_mut::<16>().0.iter_mut().zip(from.as_chunks::<16>().0) {
                *d = gnitz_wire::relocate_german_string(s, src.heap(), heap);
            }
        } else {
            self.col_mut(dst_pi).copy_from_slice(from);
        }
    }

    /// Same key column types and the same payload type list.
    fn same_layout(&self, other: &ZSetBatch) -> bool {
        Arc::ptr_eq(&self.layout, &other.layout) || self.layout.same_region_types(&other.layout)
    }

    /// Append all rows of `other`, its German cells rebased onto this heap.
    pub(crate) fn extend_from(&mut self, other: &ZSetBatch) {
        assert!(self.same_layout(other), "extend_from: layout mismatch");
        self.append_region_list(&other.wire_regions());
    }

    /// `extend_from`, taking `other` whole when `self` is empty.
    pub fn extend_from_owned(&mut self, other: ZSetBatch) {
        if self.is_empty() {
            assert!(self.same_layout(&other), "extend_from_owned: layout mismatch");
            *self = other;
        } else {
            self.extend_from(&other);
        }
    }

    /// The batch's current extent, for [`Self::truncate_to`].
    pub fn mark(&self) -> RowMark {
        RowMark { rows: self.rows, heap: self.heap().len() }
    }

    /// Drop every row and heap byte appended since `mark`. Valid while no cell of a row
    /// before the mark points into the heap past it.
    pub fn truncate_to(&mut self, mark: RowMark) {
        assert!(
            mark.rows <= self.rows && mark.heap <= self.heap().len(),
            "truncate_to: a mark past the batch"
        );
        let rows = std::mem::replace(&mut self.rows, mark.rows);
        self.zero_rows_past_last(mark.rows, rows);
        match &mut self.heap {
            Heap::Received(at) => at.end = at.start + mark.heap,
            Heap::Owned(heap) => heap.truncate(mark.heap),
        }
    }

    /// Keep only the rows in `ranges` (ascending, disjoint, `end` exclusive), in order, moving each
    /// region's survivors down in place. The heap is untouched, so German cells stay valid.
    pub fn retain_ranges(&mut self, ranges: &[(usize, usize)]) {
        if ranges == [(0, self.len())] {
            return;
        }
        assert!(
            ranges.last().is_none_or(|&(_, e)| e <= self.rows),
            "retain_ranges: a range past the last row"
        );
        for r in 0..self.layout.num_fixed_regions() {
            let (at, w) = (self.region_at(r), self.layout.region_stride(r));
            let mut dst = 0;
            for &(s, e) in ranges {
                self.buf.copy_within(at + s * w..at + e * w, at + dst * w);
                dst += e - s;
            }
        }
        let rows = std::mem::replace(&mut self.rows, ranges.iter().map(|&(s, e)| e - s).sum());
        self.zero_rows_past_last(self.rows, rows);
    }

    /// The rows `rows` names, in order, each at most once. A gather naming every row in place is
    /// the batch itself. When every row survives they are permuted in place and the heap stays
    /// where it is, else only survivors' strings are copied.
    pub fn gather(mut self, rows: &[u32]) -> ZSetBatch {
        if rows.len() == self.len() && rows.iter().enumerate().all(|(i, &r)| r as usize == i) {
            return self;
        }
        if rows.len() < self.len() {
            return self.gathered(rows);
        }
        let mut scratch = Vec::new();
        for r in 0..self.layout.num_fixed_regions() {
            let region = self.region(r);
            scratch.resize(region.len(), 0);
            gather_cells(region, self.layout.region_stride(r), rows, &mut scratch);
            self.region_mut(r).copy_from_slice(&scratch);
        }
        self
    }

    /// The rows `rows` names, in order, each at most once, onto a fresh heap holding only
    /// their strings; `self` is left as it was.
    pub fn gathered(&self, rows: &[u32]) -> ZSetBatch {
        let mut out = Self::of_layout(&self.layout);
        out.append_rows(self, rows, true);
        out
    }

    /// Append the rows of `src` that `rows` names, in order, each at most once, at their
    /// weights. With `relocate`, German cells are moved onto this batch's heap; without,
    /// they are copied as they are, for a heap the caller supplies. `src` and `self` must
    /// share a layout.
    pub(crate) fn append_rows(&mut self, src: &ZSetBatch, rows: &[u32], relocate: bool) {
        debug_assert!(self.same_layout(src), "append_rows: layout mismatch");
        debug_assert!(
            rows.len() < 2 || {
                let mut seen = vec![false; src.len()];
                rows.iter().all(|&r| !std::mem::replace(&mut seen[r as usize], true))
            },
            "gather: a row named twice"
        );
        self.reserve(rows.len());
        let at = self.rows;
        self.rows += rows.len();
        let strings = if relocate { self.string_regions() } else { 0 };
        for r in 0..self.layout.num_fixed_regions() {
            let w = self.layout.region_stride(r);
            let s = self.region_at(r) + at * w;
            let dst = s..s + rows.len() * w;
            let from = src.region(r);
            if strings >> r & 1 != 0 {
                let cells = from.as_chunks::<16>().0;
                let (buf, heap) = self.buf_and_heap();
                for (d, &row) in buf[dst].as_chunks_mut::<16>().0.iter_mut().zip(rows) {
                    *d = gnitz_wire::relocate_german_string(&cells[row as usize], src.heap(), heap);
                }
            } else {
                gather_cells(from, w, rows, &mut self.buf[dst]);
            }
        }
    }

    /// Append rows given as `r`, a canonical region list in this batch's layout, verbatim:
    /// each region concatenates and the list's heap lands on this heap's tail, so every
    /// German cell has its heap offset shifted by the heap's old length.
    fn append_region_list(&mut self, r: &[&[u8]]) {
        let n = r[REG_WEIGHT].len() / FIXED_REGION_BYTES;
        self.reserve(n);
        let at = self.rows;
        self.rows += n;
        let heap = r[self.layout.num_fixed_regions()];
        let shift = self.heap().len();
        if !heap.is_empty() {
            self.heap_mut().extend_from_slice(heap);
        }
        let strings = self.string_regions();
        for (ri, region) in r[..self.layout.num_fixed_regions()].iter().enumerate() {
            let w = self.layout.region_stride(ri);
            let s = self.region_at(ri) + at * w;
            let dst = &mut self.buf[s..s + n * w];
            dst.copy_from_slice(region);
            if strings >> ri & 1 != 0 {
                gnitz_wire::shift_german_string_heaps(dst, shift);
            }
        }
    }

    // ── Shape checks ────────────────────────────────────────────────────────

    /// The batch's layout is `schema`'s: key column types, payload slot count,
    /// and each slot's type. O(columns): no row is read, and every region is as long as the
    /// rows by construction.
    pub(crate) fn layout_matches(&self, schema: &Schema) -> Result<(), String> {
        if Arc::ptr_eq(&self.layout, schema.shared_layout()) {
            return Ok(());
        }
        let want = schema.layout().pk_columns().map(|c| c.type_code);
        let got = self.layout.pk_columns().map(|c| c.type_code);
        if !got.clone().eq(want.clone()) {
            let (want, got): (Vec<TypeCode>, Vec<TypeCode>) = (want.collect(), got.collect());
            return Err(format!("mismatched key column types: expected {want:?}, got {got:?}"));
        }
        if self.payload_count() != schema.layout().num_payload_cols() {
            return Err(format!(
                "payload slot count {} != schema payload column count {}",
                self.payload_count(),
                schema.layout().num_payload_cols()
            ));
        }
        for (pi, _, def) in schema.payload_columns() {
            let tc = self.payload_tc(pi);
            if tc != def.ty.tc {
                return Err(format!(
                    "payload slot {pi} ('{}'): type {tc:?} != schema type {:?}",
                    def.name, def.ty.tc
                ));
            }
        }
        Ok(())
    }

    /// [`Self::layout_matches`], and every NULL is a zeroed cell of a nullable
    /// column — what the server certifies of each row it is sent, here with
    /// the row and column named.
    pub fn validate(&self, schema: &Schema) -> Result<(), String> {
        self.layout_matches(schema)?;
        check_not_null(self.region(REG_NULL_BMP), schema)?;
        let col = |pi: usize| (self.col(pi), self.payload_tc(pi).wire_stride());
        match gnitz_wire::first_valued_null(schema.layout().nullable_payload_slots(), self.region(REG_NULL_BMP), col) {
            None => Ok(()),
            Some((row, slot)) => Err(format!(
                "row {row} holds a value under NULL in column '{}'",
                schema.columns()[schema.layout().payload_col_idx(slot)].name
            )),
        }
    }
}

/// A copy holds its rows in an arena of exactly them, its heap behind it.
impl Clone for ZSetBatch {
    fn clone(&self) -> Self {
        Self::from_region_lists(&self.layout, &[self.wire_regions()])
    }
}

/// Equal layouts' types, rows and heap bytes; where either batch's buffer lies does not
/// enter it.
impl PartialEq for ZSetBatch {
    fn eq(&self, o: &Self) -> bool {
        self.same_layout(o) && self.wire_regions() == o.wire_regions()
    }
}

impl Eq for ZSetBatch {}

impl std::fmt::Debug for ZSetBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let cols: Vec<&[u8]> = (0..self.payload_count()).map(|pi| self.col(pi)).collect();
        f.debug_struct("ZSetBatch")
            .field("layout", &self.layout)
            .field("pks", &self.pk_region())
            .field("weights", &self.weights().collect::<Vec<_>>())
            .field("nulls", &self.null_words().collect::<Vec<_>>())
            .field("payload", &cols)
            .field("heap", &self.heap())
            .finish()
    }
}

/// Append the `stride`-wide cells of `src` that `rows` names, in order, onto `dst`, as long
/// as they are.
fn gather_cells(src: &[u8], stride: usize, rows: &[u32], dst: &mut [u8]) {
    fn fixed<const N: usize>(src: &[u8], rows: &[u32], dst: &mut [u8]) {
        let cells = src.as_chunks::<N>().0;
        for (d, &r) in dst.as_chunks_mut::<N>().0.iter_mut().zip(rows) {
            *d = cells[r as usize];
        }
    }
    match stride {
        1 => fixed::<1>(src, rows, dst),
        2 => fixed::<2>(src, rows, dst),
        4 => fixed::<4>(src, rows, dst),
        8 => fixed::<8>(src, rows, dst),
        16 => fixed::<16>(src, rows, dst),
        // A compound key's stride.
        _ => {
            for (d, &r) in dst.chunks_exact_mut(stride).zip(rows) {
                d.copy_from_slice(&src[r as usize * stride..][..stride]);
            }
        }
    }
}

/// No row sets a null bit `schema` does not admit: one on a NOT NULL payload
/// column, whose declaration every reader past the decoder trusts, or one past
/// the last payload column.
fn check_not_null(null_bmp: &[u8], schema: &Schema) -> Result<(), String> {
    match gnitz_wire::first_not_null_violation(schema.layout().nullable_payload_slots(), null_bmp) {
        None => Ok(()),
        Some((row, slot)) if slot >= schema.layout().num_payload_cols() => {
            Err(format!("row {row} sets null bit {slot}, past the last payload column"))
        }
        Some((row, slot)) => Err(format!(
            "row {row} sets a null bit on NOT NULL column '{}'",
            schema.columns()[schema.layout().payload_col_idx(slot)].name
        )),
    }
}

/// A [`ZSetBatch`]'s extent at one moment, taken by [`ZSetBatch::mark`].
#[derive(Clone, Copy)]
pub struct RowMark {
    rows: usize,
    heap: usize,
}

/// One row being written past a batch's last, from [`ZSetBatch::open_row`]: every cell
/// starts zero, and the row joins the batch at [`Self::commit`].
#[must_use = "a row joins the batch only at commit"]
pub struct RowWriter<'a> {
    batch: &'a mut ZSetBatch,
}

impl RowWriter<'_> {
    /// Where the open row's cell in region `r` lies in the buffer.
    #[inline(always)]
    fn at(&self, r: usize) -> Range<usize> {
        self.batch.cell_range(r, self.batch.rows)
    }

    /// The row's key, from the key columns' native values in PK-list order.
    pub fn key_natives(&mut self, natives: &[u128]) -> &mut Self {
        let at = self.at(REG_PK);
        store_key_natives(&self.batch.layout, &mut self.batch.buf[at], natives);
        self
    }

    pub fn weight(&mut self, w: i64) -> &mut Self {
        let at = self.at(REG_WEIGHT);
        self.batch.buf[at].copy_from_slice(&w.to_le_bytes());
        self
    }

    pub fn null_word(&mut self, w: u64) -> &mut Self {
        let at = self.at(REG_NULL_BMP);
        self.batch.buf[at].copy_from_slice(&w.to_le_bytes());
        self
    }

    /// Payload slot `pi`'s cell, its type's wire stride wide.
    pub fn cell(&mut self, pi: usize) -> &mut [u8] {
        let at = self.at(REG_PAYLOAD_START + pi);
        &mut self.batch.buf[at]
    }

    /// Payload slot `pi`'s cell and the heap, to write a German cell.
    pub fn cell_and_heap(&mut self, pi: usize) -> (&mut [u8], &mut Vec<u8>) {
        let at = self.at(REG_PAYLOAD_START + pi);
        let (buf, heap) = self.batch.buf_and_heap();
        (&mut buf[at], heap)
    }

    /// Close the row: it is the batch's last.
    pub fn commit(self) {
        self.batch.rows += 1;
        // The row is the batch's, so `Drop` must not clear it.
        std::mem::forget(self);
    }
}

/// A row dropped before its commit is cleared, so the arena past the last row stays zero.
impl Drop for RowWriter<'_> {
    fn drop(&mut self) {
        let row = self.batch.rows;
        self.batch.zero_rows_past_last(row, row + 1);
    }
}

/// Appends rows to a [`ZSetBatch`] one payload slot at a time. A fixed-width value is
/// checked against its slot's width and refused by a STRING/BLOB slot, `int_val` also
/// against the integer type's range, and `str_val`/`bytes_val` against the exact type.
///
/// Columns are appended in payload-slot order, so callers supply only payload
/// values; the cursor is the payload slot, and equals the payload count while no row
/// is open. A row joins the batch when its last value lands; starting or closing a row
/// before then panics, and an appender is not dropped with a row open: the open row's
/// cells lie past the batch's last row, which [`ZSetBatch::open_row`] takes to be zero.
pub struct BatchAppender<'a> {
    batch: &'a mut ZSetBatch,
    cursor: usize,
}

/// The client half of the shared catalog row codecs: the sink
/// a `gnitz_wire` system row writes itself into.
impl gnitz_wire::SysRowSink for BatchAppender<'_> {
    fn begin_row(&mut self, pk: &[u128], weight: i64) {
        self.add_row_natives(pk, weight);
    }
    fn put_u64(&mut self, v: u64) {
        self.int_val(v);
    }
    fn put_string(&mut self, s: &str) {
        self.str_val(s);
    }
    fn put_bytes(&mut self, b: &[u8]) {
        self.bytes_val(b);
    }
    fn end_row(&mut self) {
        self.check_row_full();
    }
}

impl<'a> BatchAppender<'a> {
    pub fn new(batch: &'a mut ZSetBatch) -> Self {
        let cursor = batch.payload_count();
        BatchAppender { batch, cursor }
    }

    /// The open row, if any, got one value per payload column.
    fn check_row_full(&self) {
        assert_eq!(
            self.cursor,
            self.batch.payload_count(),
            "BatchAppender: row got {} of {} payload columns",
            self.cursor,
            self.batch.payload_count(),
        );
    }

    /// Start a new row with the given single-column primary key and weight.
    pub fn add_row(&mut self, pk: u128, weight: i64) -> &mut Self {
        self.add_row_natives(&[pk], weight)
    }

    /// [`Self::add_row`] for a **compound** PK: `natives` are the PK columns'
    /// native values in PK-list order.
    pub fn add_row_natives(&mut self, natives: &[u128], weight: i64) -> &mut Self {
        self.check_row_full();
        let b = &mut *self.batch;
        let row = b.rows;
        b.reserve_one();
        let at = b.cell_range(REG_PK, row);
        store_key_natives(&b.layout, &mut b.buf[at], natives);
        let at = b.cell_range(REG_WEIGHT, row);
        b.buf[at].copy_from_slice(&weight.to_le_bytes());
        self.cursor = 0;
        self.close_if_full();
        self
    }

    /// The open row joins the batch once its last payload slot is written.
    fn close_if_full(&mut self) {
        if self.cursor == self.batch.payload_count() {
            self.batch.rows += 1;
        }
    }

    /// Where the open row's cell in the next payload slot lies in the buffer, after
    /// `check` of that slot's type.
    fn next_cell(&mut self, check: impl FnOnce(usize, TypeCode)) -> Range<usize> {
        let pi = self.col_index();
        check(pi, self.batch.payload_tc(pi));
        self.cursor += 1;
        self.batch.cell_range(REG_PAYLOAD_START + pi, self.batch.rows)
    }

    /// Write one fixed-width cell, exactly the column's stride wide, into the next column.
    fn fixed_val(&mut self, bytes: &[u8]) -> &mut Self {
        let at = self.next_cell(|pi, tc| {
            assert!(
                !tc.is_german_string(),
                "BatchAppender: a fixed-width value cannot be written to the {tc:?} column at payload slot {pi}",
            );
            assert_eq!(
                bytes.len(),
                tc.wire_stride(),
                "BatchAppender: {tc:?} column at payload slot {pi} takes {} bytes",
                tc.wire_stride(),
            );
        });
        self.batch.buf[at].copy_from_slice(bytes);
        self.close_if_full();
        self
    }

    /// Append an integer to the next ≤8-byte integer column, at that column's
    /// own width and signedness; `v` must lie in the column type's range.
    pub fn int_val(&mut self, v: impl Into<i128>) -> &mut Self {
        let v = v.into();
        let tc = self.batch.payload_tc(self.col_index());
        let fi = gnitz_wire::FixedInt::from_type_code(tc)
            .unwrap_or_else(|| panic!("BatchAppender: {tc:?} is not an integer column"));
        let (lo, hi) = fi.range();
        assert!((lo..=hi).contains(&v), "BatchAppender: {v} is out of range for {tc:?}");
        self.fixed_val(&fi.pack(v).to_le_bytes()[..fi.width()])
    }

    /// Append an f64 value to the next column.
    pub fn f64_val(&mut self, v: f64) -> &mut Self {
        self.fixed_val(&v.to_le_bytes())
    }

    /// Append an f32 value to the next column.
    pub fn f32_val(&mut self, v: f32) -> &mut Self {
        self.fixed_val(&v.to_le_bytes())
    }

    /// Append a u128 value to the next column: its 16 native LE bytes, which are
    /// the column's wire region (U128/UUID/I128).
    pub fn u128_val(&mut self, v: u128) -> &mut Self {
        self.fixed_val(&v.to_le_bytes())
    }

    /// Append a string value to the next STRING column.
    pub fn str_val(&mut self, s: &str) -> &mut Self {
        self.german_val(s.as_bytes(), TypeCode::String)
    }

    /// Append a raw byte slice to the next BLOB column.
    pub fn bytes_val(&mut self, b: &[u8]) -> &mut Self {
        self.german_val(b, TypeCode::Blob)
    }

    /// Append one German-string cell of type `want`, spilling into the batch's
    /// heap.
    fn german_val(&mut self, b: &[u8], want: TypeCode) -> &mut Self {
        let at = self.next_cell(|pi, tc| {
            assert!(
                tc == want,
                "BatchAppender: a {want:?} value cannot be written to the {tc:?} column at payload slot {pi}",
            );
        });
        let (buf, heap) = self.batch.buf_and_heap();
        buf[at].copy_from_slice(&gnitz_wire::encode_german_string(b, heap));
        self.close_if_full();
        self
    }

    /// Append a SQL NULL to the next column, whatever its type: a zeroed cell under its bitmap bit.
    pub fn null(&mut self) -> &mut Self {
        let pi = self.col_index();
        // The open row's cell starts zero.
        self.next_cell(|_, _| {});
        let b = &mut *self.batch;
        let at = b.cell_range(REG_NULL_BMP, b.rows);
        let mut word = read_u64_le(&b.buf, at.start);
        gnitz_wire::null_word_set(&mut word, pi, true);
        b.buf[at].copy_from_slice(&word.to_le_bytes());
        self.close_if_full();
        self
    }

    /// The payload slot the next value goes to.
    fn col_index(&self) -> usize {
        assert!(
            self.cursor < self.batch.payload_count(),
            "BatchAppender: no payload slot open (cursor {} of {} columns): a value before add_row or past the row's last column",
            self.cursor,
            self.batch.payload_count(),
        );
        self.cursor
    }
}

/// Every method is `#[inline(always)]`; see [`gnitz_wire::RowSource`] for why
/// the plain hint is not enough.
impl gnitz_wire::RowSource for ZSetBatch {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        self.pk_bytes(row)
    }

    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        self.null_word(row)
    }

    #[inline(always)]
    fn get_col_ptr(&self, row: usize, pi: usize, sz: usize) -> &[u8] {
        assert!(row < self.rows, "get_col_ptr: row {row} of {}", self.rows);
        debug_assert_eq!(sz, self.layout.region_stride(REG_PAYLOAD_START + pi));
        let s = self.region_at(REG_PAYLOAD_START + pi) + row * sz;
        &self.buf[s..s + sz]
    }

    #[inline(always)]
    fn heap(&self) -> gnitz_wire::Heap<'_> {
        gnitz_wire::Heap::Plain(ZSetBatch::heap(self))
    }

    #[inline(always)]
    fn row_count(&self) -> usize {
        self.rows
    }
}

impl gnitz_expr::BatchView for ZSetBatch {
    #[inline(always)]
    fn col_data(&self, pi: usize, col_size: usize) -> &[u8] {
        debug_assert_eq!(
            col_size,
            self.layout.region_stride(REG_PAYLOAD_START + pi),
            "col_data({pi}, {col_size}) width mismatch"
        );
        self.col(pi)
    }

    #[inline(always)]
    fn null_bmp(&self) -> &[u8] {
        self.region(REG_NULL_BMP)
    }

    #[inline(always)]
    fn pk_region(&self) -> (&[u8], usize) {
        (self.region(REG_PK), self.layout.pk_stride())
    }
}

impl gnitz_expr::MapTarget for ZSetBatch {
    #[inline(always)]
    fn null_bmp_mut(&mut self) -> &mut [u8] {
        self.region_mut(REG_NULL_BMP)
    }

    #[inline(always)]
    fn slot_mut(&mut self, pi: usize) -> (&mut [u8], &mut [u8], &mut Vec<u8>) {
        let r = REG_PAYLOAD_START + pi;
        let (col, nulls) = (self.region_at(r), self.region_at(REG_NULL_BMP));
        let (col_len, null_len) = (self.rows * self.layout.region_stride(r), self.rows * FIXED_REGION_BYTES);
        let (buf, heap) = self.buf_and_heap();
        // The null region precedes every payload region.
        let (lo, hi) = buf.split_at_mut(col);
        (&mut hi[..col_len], &mut lo[nulls..nulls + null_len], heap)
    }
}

// ── WAL-block encode and decode ────────────────────────────────────────────

impl ZSetBatch {
    /// This batch as its canonical region list.
    pub(crate) fn wire_regions(&self) -> Regions<'_> {
        let mut out = Regions::new();
        for r in 0..self.layout.num_fixed_regions() {
            out.push(self.region(r));
        }
        out.push(self.heap());
        out
    }

    /// A batch of `schema` holding the rows of a canonical region list this process built
    /// or its own store produced. Its German cells are not checked, as no other process
    /// wrote them; its region lengths and NOT NULL are checked.
    pub fn from_own_regions(schema: &Schema, regions: &[&[u8]]) -> Result<Self, ProtocolError> {
        check_region_lengths(schema.layout(), regions).map_err(ProtocolError::DecodeError)?;
        check_not_null(regions[REG_NULL_BMP], schema).map_err(ProtocolError::DecodeError)?;
        Ok(Self::from_region_lists(
            schema.shared_layout(),
            &[Regions::from_slice(regions)],
        ))
    }

    /// The rows of the canonical region lists `lists`, each of `layout` and of lengths
    /// checked, in order, in an arena of exactly their rows with their heaps concatenated
    /// behind it: every German cell has its offset shifted by the heaps before its own.
    fn from_region_lists(layout: &Arc<SchemaDescriptor>, lists: &[Regions<'_>]) -> Self {
        let n = layout.num_fixed_regions();
        let rows: usize = lists.iter().map(|r| r[REG_WEIGHT].len() / FIXED_REGION_BYTES).sum();
        let heap: usize = lists.iter().map(|r| r[n].len()).sum();
        let fixed = rows * layout.row_width();
        // Every byte is written below: the arena holds exactly `rows` rows, so its regions
        // tile `..fixed`, and the heaps tile the rest.
        let mut buf: Vec<u8> = Vec::with_capacity(fixed + heap);
        let dst = &mut buf.spare_capacity_mut()[..fixed + heap];
        let strings = u128::from(layout.string_payload_slots()) << REG_PAYLOAD_START;
        let (mut at, mut heap_at) = (0, 0);
        for list in lists {
            let list_rows = list[REG_WEIGHT].len() / FIXED_REGION_BYTES;
            for (r, region) in list[..n].iter().enumerate() {
                let cells = dst[layout.region_start(r, rows) + at * layout.region_stride(r)..][..region.len()]
                    .write_copy_of_slice(region);
                if strings >> r & 1 != 0 {
                    gnitz_wire::shift_german_string_heaps(cells, heap_at);
                }
            }
            dst[fixed + heap_at..][..list[n].len()].write_copy_of_slice(list[n]);
            at += list_rows;
            heap_at += list[n].len();
        }
        // SAFETY: the loop wrote each of the `fixed + heap` bytes, as above.
        unsafe { buf.set_len(fixed + heap) };
        Self::received(layout, buf, 0, rows, heap)
    }

    /// The batch whose `rows` rows `buf` holds as one block's fixed regions at `base`, its
    /// heap of `heap` bytes right behind them.
    fn received(layout: &Arc<SchemaDescriptor>, buf: Vec<u8>, base: usize, rows: usize, heap: usize) -> Self {
        let heap_at = base + rows * layout.row_width();
        ZSetBatch {
            layout: Arc::clone(layout),
            buf,
            base,
            cap: rows,
            rows,
            heap: Heap::Received(heap_at..heap_at + heap),
        }
    }
}

/// `r` is a canonical region list in `layout`, each fixed region as long as the weight
/// region's row count implies.
fn check_region_lengths(layout: &SchemaDescriptor, r: &[&[u8]]) -> Result<(), String> {
    let want = gnitz_wire::num_regions(layout.num_payload_cols());
    if r.len() != want {
        return Err(format!("{} regions != expected {want}", r.len()));
    }
    let weights = r[REG_WEIGHT].len();
    if !weights.is_multiple_of(FIXED_REGION_BYTES) {
        return Err(format!("weight region length {weights} is not whole rows"));
    }
    let n = weights / FIXED_REGION_BYTES;
    let (pk, want) = (r[REG_PK].len(), n * layout.pk_stride());
    if pk != want {
        return Err(format!("pk region length {pk} != expected {want}"));
    }
    let (nulls, want) = (r[REG_NULL_BMP].len(), n * FIXED_REGION_BYTES);
    if nulls != want {
        return Err(format!("nulls length {nulls} != expected {want}"));
    }
    for (pi, region) in r[REG_PAYLOAD_START..layout.num_fixed_regions()].iter().enumerate() {
        let (got, want) = (region.len(), n * layout.region_stride(REG_PAYLOAD_START + pi));
        if got != want {
            return Err(format!("payload slot {pi}: length {got} != expected {want}"));
        }
    }
    Ok(())
}

/// The header and regions of the WAL block `data` under `schema`; its rows'
/// contents are not checked.
pub(crate) fn parse_wal_block<'a>(
    data: &'a [u8],
    schema: &Schema,
) -> Result<gnitz_wire::wal::ParsedBlock<'a>, ProtocolError> {
    gnitz_wire::wal::parse_block(data, schema.layout().row_width())
        .map_err(|e| ProtocolError::DecodeError(format!("WAL {e}")))
}

/// The WAL block `data` under `schema` as its canonical region list, with every row it holds
/// checked as a block that crossed a trust boundary must be: no null bit `schema` refuses,
/// and every German cell canonical against the block's own heap.
fn checked_wal_block<'a>(data: &'a [u8], schema: &Schema) -> Result<Regions<'a>, ProtocolError> {
    let layout = schema.layout();
    let parsed = parse_wal_block(data, schema)?;
    // The block's size was checked against its rows, so each region is as long as they are.
    let mut regions: Regions<'a> = (0..layout.num_fixed_regions())
        .map(|r| &parsed.fixed[layout.region_start(r, parsed.rows)..][..parsed.rows * layout.region_stride(r)])
        .collect();
    regions.push(parsed.heap);
    check_not_null(regions[REG_NULL_BMP], schema).map_err(ProtocolError::DecodeError)?;
    let bad = gnitz_wire::BitIter(layout.string_payload_slots())
        .find(|&pi| !gnitz_wire::german_string_region_ok(regions[REG_PAYLOAD_START + pi], parsed.heap));
    if let Some(pi) = bad {
        return Err(ProtocolError::DecodeError(format!(
            "column {}: German string cell is not in canonical form",
            layout.payload_col_idx(pi)
        )));
    }
    Ok(regions)
}

/// One WAL block of a reply and the frame it lies in at `block`: owned where the transport
/// handed the frame over whole, borrowed from its read window otherwise.
#[derive(Debug)]
pub(crate) struct FrameBlock<'a> {
    pub(crate) frame: Cow<'a, [u8]>,
    pub(crate) block: Range<usize>,
}

impl FrameBlock<'_> {
    /// The block's bytes.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.frame[self.block.clone()]
    }
}

/// Decode a reply's WAL blocks under `schema` into one batch, in order: the blocks `held`
/// back from earlier frames, then `last`. A lone block whose frame is owned becomes the batch
/// with no copy; a lone borrowed one is copied once, whole; several are each checked, then
/// concatenated into one arena sized for all of them.
pub(crate) fn decode_wal_blocks(
    mut held: Vec<FrameBlock<'static>>,
    last: Option<FrameBlock<'_>>,
    schema: &Schema,
) -> Result<ZSetBatch, ProtocolError> {
    let lone = match (held.len(), last) {
        (0, None) => return Ok(ZSetBatch::new(schema)),
        (0, Some(last)) => last,
        (1, None) => held.pop().expect("one held block"),
        (_, last) => {
            let lists = (held.iter().chain(last.as_ref()))
                .map(|b| checked_wal_block(b.bytes(), schema))
                .collect::<Result<Vec<_>, _>>()?;
            return Ok(ZSetBatch::from_region_lists(schema.shared_layout(), &lists));
        }
    };
    let (rows, heap) = {
        let regions = checked_wal_block(lone.bytes(), schema)?;
        (
            regions[REG_WEIGHT].len() / FIXED_REGION_BYTES,
            regions[regions.len() - 1].len(),
        )
    };
    let fixed_at = lone.block.start + WAL_HEADER_SIZE;
    Ok(match lone.frame {
        Cow::Owned(frame) => ZSetBatch::received(schema.shared_layout(), frame, fixed_at, rows, heap),
        Cow::Borrowed(frame) => ZSetBatch::received(
            schema.shared_layout(),
            frame[fixed_at..lone.block.end].to_vec(),
            0,
            rows,
            heap,
        ),
    })
}

#[cfg(test)]
#[path = "tests/batch.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/batch.rs"]
mod bench;
