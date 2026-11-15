//! The operator key composers: [`FoldCols`], the XXH3-128 row digest — a
//! packed group key's fold slot, a fold-keyed `_group_pk`, a hash-row map's PK —
//! and [`KeyPacker`], which packs a join key, a group key or an index span into a
//! contiguous OPK region. The reindex Map keys rows through it and the exchange
//! scatter routes by the bytes it packs, so the reindexed trace side and the
//! scattered delta co-partition byte-for-byte.

use std::cell::Cell;
use std::mem::MaybeUninit;

use gnitz_expr::{BatchView, ColumnLocator};
use gnitz_wire::{zip_cells, RowSource, TypeCode, MAX_PK_BYTES, MAX_PK_COLUMNS};

use crate::repr::{range_rows, with_scratch};
use crate::schema::{DerivedSchema, SchemaColumn, SchemaDescriptor};

/// Filler for a fixed locator array's unused slots; nothing reads it.
const PADDING_LOCATOR: ColumnLocator = ColumnLocator::Pk {
    byte_off: 0,
    size: 0,
    type_code: TypeCode::U8,
};

// ---------------------------------------------------------------------------
// Key columns
// ---------------------------------------------------------------------------

/// Column `c` of `schema` as a key column, `what` naming the key in a refusal.
/// A float has none: `+0.0` and `-0.0` differ byte-wise but compare equal.
pub(crate) fn locate_key_col(
    schema: &SchemaDescriptor,
    c: u32,
    what: &str,
) -> Result<(SchemaColumn, ColumnLocator), String> {
    let (col, loc) = schema.wire_col(format_args!("{what}: column"), c)?;
    if loc.type_code().is_float() {
        return Err(format!(
            "{what}: column {c} is a float, which has no order-preserving key image"
        ));
    }
    Ok((col, loc))
}

/// Where `locs` lie in the PK region, as `(at, n)`, when they are consecutive PK
/// columns.
pub(crate) fn pk_run(locs: &[ColumnLocator]) -> Option<(usize, usize)> {
    let &ColumnLocator::Pk { byte_off: at, .. } = locs.first()? else {
        return None;
    };
    let mut end = at as usize;
    locs.iter()
        .all(|l| match *l {
            ColumnLocator::Pk { byte_off, size, .. } if byte_off as usize == end => {
                end += size as usize;
                true
            }
            _ => false,
        })
        .then_some((at as usize, end - at as usize))
}

// ---------------------------------------------------------------------------
// Row digest — the hashed key a group key's fold slot, a fold-keyed `_group_pk`
// and a hash-row map's PK hold.
// ---------------------------------------------------------------------------

thread_local! {
    /// The key bytes of a fold past `FOLD_INLINE_COLS` columns, which the stack
    /// buffers cannot hold.
    static FOLD_SCRATCH: Cell<Vec<u8>> = const { Cell::new(Vec::new()) };
}

/// Columns a fold assembles on the stack; a wider fold takes the scratch buffer.
const FOLD_INLINE_COLS: usize = MAX_PK_COLUMNS;

/// The most key bytes one row of a stack-assembled fold holds.
const FOLD_INLINE_BYTES: usize = 8 + FOLD_BYTES * FOLD_INLINE_COLS;

/// The most rows one [`FoldCols::keys_chunk`] call digests.
pub(crate) const FOLD_CHUNK: usize = 256;

/// Fewest rows at which [`FoldCols::keys_chunk`] assembling them a column at a
/// time costs less than digesting each with [`FoldCols::key_row`].
pub(crate) const FOLD_CHUNK_MIN_ROWS: usize = 4;

/// The columns one 128-bit row digest folds. A row's key bytes are its null word
/// under the null bits of the fold's payload columns, then a 16-byte slot per
/// column: an integer's image at its own type, a string's content hash. A NULL
/// cell is zeroed, so its slot is one value, told from a zero by the null word.
#[derive(Clone)]
pub(crate) struct FoldCols {
    locs: Vec<ColumnLocator>,
    /// The null bits of the payload columns among `locs`.
    mask: u64,
}

impl FoldCols {
    pub(crate) fn new(locs: Vec<ColumnLocator>) -> Self {
        let mask = locs.iter().fold(0, |m, l| m | l.null_bit());
        FoldCols { locs, mask }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.locs.is_empty()
    }

    /// The bytes of one row's key.
    #[inline(always)]
    fn stride(&self) -> usize {
        8 + FOLD_BYTES * self.locs.len()
    }

    /// The 128-bit XXH3 digest of these columns' key bytes over `row`.
    #[inline(always)]
    pub(crate) fn key_row<R: RowSource>(&self, src: &R, row: usize) -> u128 {
        if self.locs.len() > FOLD_INLINE_COLS {
            return self.key_row_scratch(src, row);
        }
        // Every 8-byte read XXH3 makes lies inside one store.
        let mut buf = [0u8; FOLD_INLINE_BYTES];
        self.put_row(&mut buf[..self.stride()], src, row)
    }

    /// [`Self::key_row`] through the thread-local scratch, out of line so the
    /// stack arm stays small enough to inline into its callers.
    #[inline(never)]
    fn key_row_scratch<R: RowSource>(&self, src: &R, row: usize) -> u128 {
        with_scratch(&FOLD_SCRATCH, |buf| {
            buf.resize(self.stride(), 0);
            self.put_row(buf, src, row)
        })
    }

    /// `row`'s key bytes into `key`, all [`Self::stride`] of them, and their digest.
    #[inline(always)]
    fn put_row<R: RowSource>(&self, key: &mut [u8], src: &R, row: usize) -> u128 {
        let (word, slots) = key.split_at_mut(8);
        word.copy_from_slice(&(src.get_null_word(row) & self.mask).to_le_bytes());
        for (&loc, slot) in self.locs.iter().zip(slots.as_chunks_mut::<FOLD_BYTES>().0) {
            *slot = key_value(loc, src, row).to_be_bytes();
        }
        gnitz_wire::checksum_128(key)
    }

    /// Call `f` with each chunk of the rows of `runs`, in order: its first row and
    /// its rows' [`Self::key_row`], staged on the stack.
    #[inline(always)]
    pub(crate) fn for_each_chunk<B: BatchView>(
        &self,
        src: &B,
        runs: &[(usize, usize)],
        mut f: impl FnMut(usize, &[u128]),
    ) {
        let mut buf = [MaybeUninit::<u128>::uninit(); FOLD_CHUNK];
        for &(s, e) in runs {
            for start in (s..e).step_by(FOLD_CHUNK) {
                let keys = filled(&mut buf[..(e - start).min(FOLD_CHUNK)], 0);
                self.keys_chunk(src, start, keys);
                f(start, keys);
            }
        }
    }

    /// [`Self::key_row`] of rows `start..start + out.len()` of `src` into `out`,
    /// at most [`FOLD_CHUNK`] of them.
    #[inline]
    pub(crate) fn keys_chunk<B: BatchView>(&self, src: &B, start: usize, out: &mut [u128]) {
        assert!(out.len() <= FOLD_CHUNK, "a fold chunk is at most {FOLD_CHUNK} rows");
        if out.len() < FOLD_CHUNK_MIN_ROWS {
            for (key, row) in out.iter_mut().zip(start..) {
                *key = self.key_row(src, row);
            }
        } else {
            self.keys_by_column(src, start, out);
        }
    }

    /// [`Self::keys_chunk`] with the rows' key bytes assembled a column at a time,
    /// then each row digested. Out of line, so a chunk too short for it keeps a
    /// small frame.
    #[inline(never)]
    fn keys_by_column<B: BatchView>(&self, src: &B, start: usize, out: &mut [u128]) {
        let n = out.len() * self.stride();
        if self.locs.len() <= FOLD_INLINE_COLS {
            let mut buf = [MaybeUninit::<u8>::uninit(); FOLD_CHUNK * FOLD_INLINE_BYTES];
            self.assemble(src, start, filled(&mut buf[..n], 0), out);
        } else {
            with_scratch(&FOLD_SCRATCH, |buf| {
                buf.resize(n, 0);
                self.assemble(src, start, buf, out);
            })
        }
    }

    /// [`Self::keys_by_column`]'s assembly: each row's key bytes into its
    /// [`Self::stride`] bytes of `buf`, then its digest into `out`.
    #[inline(always)]
    fn assemble<B: BatchView>(&self, src: &B, start: usize, buf: &mut [u8], out: &mut [u128]) {
        let (stride, runs) = (self.stride(), [(start, start + out.len())]);
        for (i, &loc) in self.locs.iter().enumerate() {
            let slot = (FOLD_BYTES, gnitz_wire::opk_bias(loc.type_code()));
            pack_column(loc, slot, buf, stride, 8 + FOLD_BYTES * i, src, &runs);
        }
        let nulls = &src.null_bmp().as_chunks::<8>().0[start..start + out.len()];
        for ((key, out), word) in buf.chunks_exact_mut(stride).zip(out).zip(nulls) {
            key[..8].copy_from_slice(&(u64::from_le_bytes(*word) & self.mask).to_le_bytes());
            *out = gnitz_wire::checksum_128(key);
        }
    }
}

/// `s` with every element set to `v`.
#[inline(always)]
pub(super) fn filled<T: Copy>(s: &mut [MaybeUninit<T>], v: T) -> &mut [T] {
    s.fill(MaybeUninit::new(v));
    // SAFETY: the fill initialized every element of `s`.
    unsafe { s.assume_init_mut() }
}

// ---------------------------------------------------------------------------
// Packed group key
// ---------------------------------------------------------------------------

/// The two slots a packed group key carries besides its columns: a leading
/// presence bitmap and a trailing overflow fold.
const BITMAP_COL: SchemaColumn = SchemaColumn::new(TypeCode::U8, false);
const FOLD_COL: SchemaColumn = SchemaColumn::new(TypeCode::U128, false);
const BITMAP_BYTES: usize = BITMAP_COL.size();
const FOLD_BYTES: usize = FOLD_COL.size();
// `pack_into` writes the bitmap as a `u8` and the fold as a `u128`.
const _: () = assert!(BITMAP_BYTES == size_of::<u8>() && FOLD_BYTES == size_of::<u128>());
// The bitmap has a bit for every column packed behind it.
const _: () = assert!(MAX_PK_COLUMNS - 1 <= 8 * BITMAP_BYTES);
// Any join key fits the PK region, so `new` checks no width.
const _: () = assert!(MAX_PK_COLUMNS * TypeCode::U128.wire_stride() <= MAX_PK_BYTES);

// ---------------------------------------------------------------------------
// KeyPacker — the synthetic-key composer
// ---------------------------------------------------------------------------

/// One key column: where its source value lives, and the key column it packs
/// into.
#[derive(Clone, Copy)]
struct KeyCol {
    loc: ColumnLocator,
    out: SchemaColumn,
}

/// `row`'s key value of `loc`: a string's content hash, else the column's image
/// at its own type.
#[inline(always)]
fn key_value<R: RowSource>(loc: ColumnLocator, batch: &R, row: usize) -> u128 {
    match loc {
        ColumnLocator::Payload { slot, type_code, .. } if type_code.is_german_string() => {
            gnitz_wire::checksum_128(&gnitz_wire::payload_bytes(batch, row, slot as usize))
        }
        _ => loc.opk_image(batch, row),
    }
}

/// The key value of `loc` in each row of `runs` — ascending `[start, end)` — in
/// order, as the slot at `off` of each of `dst`'s keys, `stride` apart: `dw`
/// bytes of a string's content hash, else of the column's value plus `bias`,
/// big-endian.
#[inline(always)]
fn pack_column<B: BatchView>(
    loc: ColumnLocator,
    (dw, bias): (usize, u128),
    dst: &mut [u8],
    stride: usize,
    off: usize,
    batch: &B,
    runs: &[(usize, usize)],
) {
    let sw = loc.size();
    let col = IntCol { dst, runs, stride, off, bias };
    let signed = loc.type_code().is_signed_int();
    match loc {
        ColumnLocator::Payload { slot, type_code, .. } if type_code.is_german_string() => {
            for_rows(col.dst, stride, runs, |key, row| {
                let h = gnitz_wire::checksum_128(&gnitz_wire::payload_bytes(batch, row, slot as usize));
                key[off..off + dw].copy_from_slice(&h.to_be_bytes());
            });
        }
        ColumnLocator::Pk { byte_off, .. } => {
            let (pk, pk_stride) = batch.pk_region();
            col.dispatch::<true>(sw, dw, signed, pk, pk_stride, byte_off as usize);
        }
        ColumnLocator::Payload { slot, .. } => {
            col.dispatch::<false>(sw, dw, signed, batch.col_data(slot as usize, sw), sw, 0);
        }
    }
}

/// Packs a join key, a group key or an index span into a contiguous OPK region.
#[derive(Clone)]
pub(crate) struct KeyPacker {
    /// The packed columns in key order, between the bitmap and the fold: the
    /// first `n_cols`. Inline, so a span's packer cloned per push allocates nothing.
    cols: [KeyCol; MAX_PK_COLUMNS],
    n_cols: u8,
    pub(crate) out_stride: usize,
    /// Group key only: a leading `U8` slot, bit *i* set iff packed column *i* is
    /// NULL.
    has_bitmap: bool,
    /// Group key only: the columns past the packed ones, hashed into a trailing
    /// 16-byte slot.
    fold: FoldCols,
}

impl KeyPacker {
    /// The packer for a join key: each slot's source column at its slot type,
    /// tightly packed. A key list off the wire is refused, never panicked on.
    pub(crate) fn new(schema: &SchemaDescriptor, key: &[gnitz_wire::ReindexSlot]) -> Result<Self, String> {
        if key.len() > MAX_PK_COLUMNS {
            return Err(format!(
                "reindex key: {} columns exceeds the {MAX_PK_COLUMNS}-column PK limit",
                key.len()
            ));
        }
        let mut cols = Vec::with_capacity(key.len());
        for &(c, t) in key {
            let (_, loc) = locate_key_col(schema, c, "reindex key")?;
            let src = loc.type_code();
            if !src.packs_at(t) {
                return Err(format!("reindex key: column {c} of type {src} does not pack at {t}"));
            }
            cols.push((loc, t));
        }
        Ok(Self::finish(cols, false, FoldCols::new(Vec::new())))
    }

    /// The packer of a group key whose own images do not fit 16 bytes: the
    /// fold of `locs`, one or more columns, alone.
    pub(crate) fn fold_only(locs: Vec<ColumnLocator>) -> Self {
        debug_assert!(!locs.is_empty(), "a fold of no columns has no slot");
        Self::finish([], false, FoldCols::new(locs))
    }

    /// The packer of a span over `locs`, each column at its own type: no bitmap,
    /// no fold.
    pub(crate) fn of_span(locs: impl IntoIterator<Item = ColumnLocator>) -> Self {
        let cols = locs.into_iter().map(|loc| (loc, loc.type_code()));
        Self::finish(cols, false, FoldCols::new(Vec::new()))
    }

    /// The packer over `packed` — each column and its slot type — its stride the
    /// sum of [`Self::key_columns`].
    fn finish(packed: impl IntoIterator<Item = (ColumnLocator, TypeCode)>, has_bitmap: bool, fold: FoldCols) -> Self {
        let mut cols = [KeyCol { loc: PADDING_LOCATOR, out: BITMAP_COL }; MAX_PK_COLUMNS];
        let mut n_cols = 0;
        for (loc, tc) in packed {
            cols[n_cols] = KeyCol { loc, out: SchemaColumn::new(tc, false) };
            n_cols += 1;
        }
        let mut packer = KeyPacker {
            cols,
            n_cols: n_cols as u8,
            out_stride: 0,
            has_bitmap,
            fold,
        };
        packer.out_stride = packer.key_columns().map(|c| c.size()).sum();
        packer
    }

    /// The packed columns, in key order.
    #[inline(always)]
    fn cols(&self) -> &[KeyCol] {
        &self.cols[..self.n_cols as usize]
    }

    /// The output PK columns this packer's bytes fill, in key order.
    pub(super) fn key_columns(&self) -> impl Iterator<Item = SchemaColumn> + '_ {
        let bitmap = self.has_bitmap.then_some(BITMAP_COL);
        let fold = (!self.fold.is_empty()).then_some(FOLD_COL);
        bitmap.into_iter().chain(self.cols().iter().map(|c| c.out)).chain(fold)
    }

    /// The columns, when the packed key is their own OPK images laid end to end.
    pub(crate) fn identity_columns(&self) -> Option<Vec<ColumnLocator>> {
        let identity = |c: &KeyCol| {
            let src = c.loc.type_code();
            !src.is_german_string()
                && c.out.size() == c.loc.size()
                && gnitz_wire::opk_bias(src) == gnitz_wire::opk_bias(c.out.type_code)
        };
        let cols = self.cols();
        (!self.has_bitmap && self.fold.is_empty() && cols.iter().all(identity))
            .then(|| cols.iter().map(|c| c.loc).collect())
    }

    /// `row`'s packed key of at most 16 bytes, widened as `widen_pk_be` does.
    /// Always inlined: it runs once per row inside a caller's row loop.
    #[inline(always)]
    pub(crate) fn narrow_image<R: RowSource>(&self, batch: &R, row: usize) -> u128 {
        debug_assert!(self.out_stride <= 16, "a narrow key is at most 16 bytes");
        let mut be = [0u8; 16];
        self.pack_into(&mut be[16 - self.out_stride..], batch, row);
        u128::from_be_bytes(be)
    }

    /// The packed key of each row of `runs` — ascending `[start, end)` — widened as
    /// [`Self::narrow_image`] widens it, into `out[row]`; a row outside them keeps
    /// what it held.
    pub(crate) fn identities_over<B: BatchView>(&self, batch: &B, runs: &[(usize, usize)], out: &mut [u128]) {
        for &(s, e) in runs {
            self.identities(batch, s, &mut out[s..e]);
        }
    }

    /// The packed key of rows `start..start + out.len()`, widened as
    /// [`Self::narrow_image`] widens it, into `out`. A key of the fold alone is its
    /// digest, taken a chunk of rows at a time; any other packs into the front of
    /// each row's identity, read big-endian.
    #[inline]
    pub(crate) fn identities<B: BatchView>(&self, batch: &B, start: usize, out: &mut [u128]) {
        let w = self.out_stride;
        debug_assert!((1..=16).contains(&w), "a narrow key is 1 to 16 bytes");
        if self.n_cols == 0 && !self.has_bitmap {
            for (start, ids) in (start..).step_by(FOLD_CHUNK).zip(out.chunks_mut(FOLD_CHUNK)) {
                self.fold.keys_chunk(batch, start, ids);
            }
            return;
        }
        let runs = [(start, start + out.len())];
        self.pack_rows(gnitz_wire::as_le_bytes_mut(out), 16, batch, &runs);
        // A constant shift: a `u128` shifted by a runtime amount costs several times more.
        macro_rules! widen {
            ($($n:literal)*) => {
                match w {
                    $($n => out.iter_mut().for_each(|id| *id = u128::from_be(*id) >> (128 - 8 * $n)),)*
                    _ => unreachable!("a narrow key is 1 to 16 bytes"),
                }
            };
        }
        widen!(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16);
    }

    /// The reindex Map's output schema: [`Self::key_columns`], then
    /// `payload_cols` of `in_schema`.
    pub(crate) fn output_schema(
        &self,
        in_schema: &SchemaDescriptor,
        payload_cols: &[u32],
    ) -> Result<SchemaDescriptor, String> {
        let mut b = DerivedSchema::new();
        self.key_columns().for_each(|c| b.push_pk(c));
        for &c in payload_cols {
            b.push(in_schema.wire_col("reindex map: payload column", c)?.0);
        }
        b.finish().map_err(|e| format!("reindex map: output {e}"))
    }

    /// Build the packer for a **group** key over `group_cols`, and the PK region
    /// of an index keyed by it: [`Self::key_columns`], then `suffix`.
    ///
    /// Leading columns pack while the PK budget lasts; the rest fold into one
    /// hash slot, so no group set is too wide.
    pub(crate) fn new_group_key(
        schema: &SchemaDescriptor,
        group_cols: &[u32],
        suffix: &[SchemaColumn],
    ) -> Result<(Self, DerivedSchema), String> {
        let max_cols = MAX_PK_COLUMNS - suffix.len();
        let max_bytes = MAX_PK_BYTES - suffix.iter().map(|c| c.size()).sum::<usize>();
        assert!(
            max_cols >= 2 && max_bytes >= BITMAP_BYTES + FOLD_BYTES,
            "a group-key suffix must leave room for a bitmap byte and a fold slot",
        );
        let group: Vec<(SchemaColumn, ColumnLocator)> = group_cols
            .iter()
            .map(|&c| locate_key_col(schema, c, "group key"))
            .collect::<Result<_, String>>()?;
        let has_bitmap = group.iter().any(|(col, _)| col.nullable);

        // The bitmap occupies one leading slot, so both budgets start spent by it.
        let lead = usize::from(has_bitmap);
        let mut packed = Vec::with_capacity(max_cols);
        let mut stride = lead * BITMAP_BYTES;
        for (i, &(col, loc)) in group.iter().enumerate() {
            let out_tc = col.type_code.reindex_output_type();
            let w = out_tc.wire_stride();
            // Reserve the fold slot the columns behind this one would need.
            let tail_cols = usize::from(i + 1 < group.len());
            if lead + packed.len() + 1 + tail_cols > max_cols || stride + w + tail_cols * FOLD_BYTES > max_bytes {
                break;
            }
            packed.push((loc, out_tc));
            stride += w;
        }
        let fold = FoldCols::new(group[packed.len()..].iter().map(|&(_, loc)| loc).collect());
        let packer = Self::finish(packed, has_bitmap, fold);
        let mut b = DerivedSchema::new();
        packer
            .key_columns()
            .chain(suffix.iter().copied())
            .for_each(|c| b.push_pk(c));
        Ok((packer, b))
    }

    /// [`Self::pack_into`] over the leading `out_stride` bytes of `buf`,
    /// returning them.
    #[inline]
    pub(crate) fn pack_prefix<'a, R: RowSource>(&self, buf: &'a mut [u8], batch: &R, row: usize) -> &'a [u8] {
        let n = self.out_stride;
        self.pack_into(&mut buf[..n], batch, row);
        &buf[..n]
    }

    /// Pack `row`'s key into `dst`, all `out_stride` bytes of it.
    #[inline(always)]
    fn pack_into<R: RowSource>(&self, dst: &mut [u8], batch: &R, row: usize) {
        let null_word = batch.get_null_word(row);
        let mut off = usize::from(self.has_bitmap) * BITMAP_BYTES;
        let mut null_bits = 0u8;
        for (i, col) in self.cols().iter().enumerate() {
            let w = col.out.size();
            let slot = &mut dst[off..off + w];
            off += w;
            if self.has_bitmap && col.loc.is_null_word(null_word) {
                null_bits |= 1 << i;
                slot.fill(0);
            } else {
                let image = promote_image(key_value(col.loc, batch, row), col.loc.type_code(), col.out.type_code);
                gnitz_wire::store_opk(slot, image, false);
            }
        }
        if self.has_bitmap {
            dst[0] = null_bits;
        }
        if !self.fold.is_empty() {
            let h = self.fold.key_row(batch, row);
            dst[off..off + FOLD_BYTES].copy_from_slice(&h.to_be_bytes());
        }
    }

    /// Whether every slot packs without hashing, so packing all of a batch's rows costs
    /// about what packing the rows read costs.
    pub(crate) fn packs_without_hashing(&self) -> bool {
        self.fold.is_empty() && self.cols().iter().all(|c| !c.loc.type_code().is_german_string())
    }

    /// [`Self::pack_into`] for the rows of `runs` — ascending `[start, end)` — in order,
    /// into the fronts of `dst`'s slots `stride` apart.
    pub(crate) fn pack_rows<B: BatchView>(&self, dst: &mut [u8], stride: usize, batch: &B, runs: &[(usize, usize)]) {
        let dst = &mut dst[..range_rows(runs) * stride];
        let mut off = usize::from(self.has_bitmap) * BITMAP_BYTES;
        if self.has_bitmap {
            dst.chunks_exact_mut(stride).for_each(|key| key[0] = 0);
        }
        for (i, col) in self.cols().iter().enumerate() {
            let dw = col.out.size();
            pack_column(
                col.loc,
                (dw, gnitz_wire::opk_bias(col.out.type_code)),
                dst,
                stride,
                off,
                batch,
                runs,
            );
            if let (true, Some(slot)) = (self.has_bitmap, col.loc.payload_slot()) {
                // A NULL packs as a zeroed slot under its bitmap bit.
                let nulls = batch.null_bmp().as_chunks::<8>().0;
                for_rows(dst, stride, runs, |key, row| {
                    if gnitz_wire::null_word_get(u64::from_le_bytes(nulls[row]), slot) {
                        key[0] |= 1 << i;
                        key[off..off + dw].fill(0);
                    }
                });
            }
            off += dw;
        }
        if !self.fold.is_empty() {
            self.pack_fold_slots(dst, stride, off, batch, runs);
        }
    }

    /// [`Self::pack_rows`]' fold slot, at `off` of each key: the rows of `runs`
    /// digested a chunk at a time. Out of line, so a packer without one compiles
    /// as it does without it.
    #[inline(never)]
    fn pack_fold_slots<B: BatchView>(
        &self,
        dst: &mut [u8],
        stride: usize,
        off: usize,
        batch: &B,
        runs: &[(usize, usize)],
    ) {
        let mut keys = dst.chunks_exact_mut(stride);
        self.fold.for_each_chunk(batch, runs, |_, digests| {
            for (h, key) in digests.iter().zip(keys.by_ref()) {
                key[off..off + FOLD_BYTES].copy_from_slice(&h.to_be_bytes());
            }
        });
    }
}

/// Call `f` on each row of `runs` — ascending `[start, end)` — in order, with that row's
/// slot of `dst`, the slots `stride` apart.
#[inline(always)]
fn for_rows(dst: &mut [u8], stride: usize, runs: &[(usize, usize)], mut f: impl FnMut(&mut [u8], usize)) {
    let mut keys = dst.chunks_exact_mut(stride);
    for &(s, e) in runs {
        // The rows lead the zip: it stops on them without taking a key it leaves unwritten.
        for (row, key) in (s..e).zip(keys.by_ref()) {
            f(key, row);
        }
    }
}

/// One integer key column's slot, at `off` of each of `dst`'s keys: one key per
/// row of `runs`, in order.
struct IntCol<'a> {
    dst: &'a mut [u8],
    runs: &'a [(usize, usize)],
    stride: usize,
    off: usize,
    /// The slot type's OPK bias: a slot holds `value + bias`, big-endian.
    bias: u128,
}

impl IntCol<'_> {
    /// [`Self::pack`] monomorphized for a `sw`-byte source cell and a `dw`-byte
    /// slot, so every shift and width in the row loop is a constant.
    fn dispatch<const PK: bool>(
        self,
        sw: usize,
        dw: usize,
        signed: bool,
        src: &[u8],
        src_stride: usize,
        src_off: usize,
    ) {
        macro_rules! arms {
            ($(($s:literal, $d:literal)),*) => {
                match (sw, dw, signed) {
                    $(
                        ($s, $d, false) => self.pack::<$s, $d, PK, false>(src, src_stride, src_off),
                        ($s, $d, true) => self.pack::<$s, $d, PK, true>(src, src_stride, src_off),
                    )*
                    _ => unreachable!("a key slot is at least as wide as its 1/2/4/8/16-byte source"),
                }
            };
        }
        arms!(
            (1, 1),
            (1, 2),
            (1, 4),
            (1, 8),
            (1, 16),
            (2, 2),
            (2, 4),
            (2, 8),
            (2, 16),
            (4, 4),
            (4, 8),
            (4, 16),
            (8, 8),
            (8, 16),
            (16, 16)
        )
    }

    /// Each row's `SW`-byte cell at `src_off` of its `src_stride` bytes of `src` —
    /// a PK column's OPK image when `PK`, else a native value — as its slot's `DW`
    /// bytes.
    #[inline(always)]
    fn pack<const SW: usize, const DW: usize, const PK: bool, const SIGNED: bool>(
        self,
        src: &[u8],
        src_stride: usize,
        src_off: usize,
    ) {
        let IntCol { mut dst, runs, stride, off, bias } = self;
        let encode = |cell: &[u8; SW], key: &mut [u8]| {
            let mut b = [0u8; 16];
            let raw = match PK {
                true => {
                    b[16 - SW..].copy_from_slice(cell);
                    u128::from_be_bytes(b) ^ ((SIGNED as u128) << (8 * SW - 1))
                }
                false => {
                    b[..SW].copy_from_slice(cell);
                    u128::from_le_bytes(b)
                }
            };
            let shift = if SIGNED { 128 - 8 * SW as u32 } else { 0 };
            let v = (((raw << shift) as i128 >> shift) as u128).wrapping_add(bias);
            let slot: &mut [u8; DW] = (&mut key[off..off + DW]).try_into().unwrap();
            *slot = v.to_be_bytes()[16 - DW..].try_into().unwrap();
        };
        for &(s, e) in runs {
            let (keys, rest) = std::mem::take(&mut dst).split_at_mut((e - s) * stride);
            dst = rest;
            let src = &src[s * src_stride..e * src_stride];
            zip_cells::<SW, _>(src, src_stride, src_off, keys.chunks_exact_mut(stride), encode);
        }
    }
}

/// The image at type `out` of the value whose image at type `src` is `image`; `out`
/// holds every `src` value.
#[inline(always)]
fn promote_image(image: u128, src: TypeCode, out: TypeCode) -> u128 {
    if src == out {
        return image;
    }
    let v = image
        .wrapping_sub(gnitz_wire::opk_bias(src))
        .wrapping_add(gnitz_wire::opk_bias(out));
    debug_assert!(
        out.wire_stride() == 16 || v >> (out.wire_stride() * 8) == 0,
        "promote_image: slot narrower than the value"
    );
    v
}

#[cfg(test)]
#[path = "tests/key_pack.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/key_pack.rs"]
mod bench;
