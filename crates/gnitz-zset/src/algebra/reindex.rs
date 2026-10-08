//! The operator key composers: the hashed key bytes of a string column's content
//! and a group key's fold slot, and [`ReindexPacker`], which packs a reindex
//! column list into the contiguous OPK region a `_join_pk` or a group key holds.
//! The reindex Map keys rows through it and the exchange scatter routes by the
//! bytes it packs, so the reindexed trace side and the scattered delta
//! co-partition byte-for-byte. [`index_entries`] is a secondary index's
//! projection of its owner's rows, and [`append_spans`] their spans alone.

use std::cell::Cell;

use gnitz_expr::BatchView;
use gnitz_wire::PkBuf;
use gnitz_wire::RowSource;

use crate::repr::{copy_runs, range_rows, runs_where, write_to_batch, Batch, MemBatch};

use crate::schema::{
    ColumnLocator, DerivedSchema, KeySpec, SchemaColumn, SchemaDescriptor, TypeCode, MAX_PK_BYTES, MAX_PK_COLUMNS,
};

// ---------------------------------------------------------------------------
// Row-content key material — the hashed key bytes, for the slots no scalar OPK
// encode can produce: a string column's content and the group key's fold slot.
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

/// Append one column's key bytes to `buf`: a null marker, then a German string's
/// content behind its 4-byte LE length — which keeps "ab"+"c" from aliasing
/// "a"+"bc" — or the value's `opk_image`, so a payload FK keys like the same
/// value stored as a PK column.
#[inline]
fn push_col_key<R: RowSource>(buf: &mut Vec<u8>, src: &R, row: usize, null_word: u64, loc: ColumnLocator) {
    if loc.is_null_word(null_word) {
        buf.push(0);
        return;
    }
    buf.push(1);
    match loc {
        ColumnLocator::Payload { slot, type_code, .. } if type_code.is_german_string() => {
            let content = gnitz_wire::payload_bytes(src, row, slot as usize);
            buf.extend_from_slice(&(content.len() as u32).to_le_bytes());
            buf.extend_from_slice(content);
        }
        _ => buf.extend_from_slice(&loc.opk_image(src, row).to_le_bytes()),
    }
}

thread_local! {
    /// `key_row`'s buffer for a fold its stack buffer cannot hold.
    static FOLD_SCRATCH: Cell<Vec<u8>> = const { Cell::new(Vec::new()) };
}

/// Columns a fold assembles on the stack; a wider fold takes the scratch buffer.
const FOLD_INLINE_COLS: usize = MAX_PK_COLUMNS;

/// The columns one 128-bit row digest folds, and which of its two byte layouts
/// the digest is taken over. Over no columns it is the digest of no bytes.
pub(crate) struct FoldCols {
    locs: Vec<ColumnLocator>,
    /// One to `FOLD_INLINE_COLS` columns, each fixed-width: the key bytes are
    /// assembled on the stack.
    inline: bool,
}

impl FoldCols {
    pub(crate) fn new(locs: Vec<ColumnLocator>) -> Self {
        let inline =
            (1..=FOLD_INLINE_COLS).contains(&locs.len()) && !locs.iter().any(|l| l.type_code().is_german_string());
        FoldCols { locs, inline }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.locs.is_empty()
    }

    /// The 128-bit XXH3 digest of these columns' key bytes over `row`.
    #[inline]
    pub(crate) fn key_row<R: RowSource>(&self, src: &R, row: usize, null_word: u64) -> u128 {
        if self.inline {
            // One word of presence bits, then each present column's image: every
            // 8-byte read XXH3 makes lies inside one store.
            let mut buf = [0u8; 8 + 16 * FOLD_INLINE_COLS];
            let (mut n, mut present) = (8usize, 0u64);
            for (i, &loc) in self.locs.iter().enumerate() {
                if !loc.is_null_word(null_word) {
                    present |= 1 << i;
                    buf[n..n + 16].copy_from_slice(&loc.opk_image(src, row).to_le_bytes());
                    n += 16;
                }
            }
            buf[..8].copy_from_slice(&present.to_le_bytes());
            return gnitz_wire::checksum_128(&buf[..n]);
        }
        self.key_row_scratch(src, row, null_word)
    }

    /// [`Self::key_row`] through the thread-local scratch, out of line so the
    /// stack arm stays small enough to inline into its callers.
    #[inline(never)]
    fn key_row_scratch<R: RowSource>(&self, src: &R, row: usize, null_word: u64) -> u128 {
        FOLD_SCRATCH.with(|cell| {
            let mut buf = cell.take();
            buf.clear();
            for &loc in &self.locs {
                push_col_key(&mut buf, src, row, null_word, loc);
            }
            let key = gnitz_wire::checksum_128(&buf);
            cell.set(buf);
            key
        })
    }
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
// ReindexPacker — the synthetic-key composer
// ---------------------------------------------------------------------------

/// One key column: where its source value lives, and the key column it packs
/// into.
#[derive(Clone, Copy)]
struct KeyCol {
    loc: ColumnLocator,
    out: SchemaColumn,
}

impl KeyCol {
    /// Padding for the fixed array's unused slots; nothing reads past `n`.
    const EMPTY: KeyCol = KeyCol {
        loc: ColumnLocator::Pk {
            byte_off: 0,
            size: 0,
            type_code: TypeCode::U8,
        },
        out: SchemaColumn::new(TypeCode::U8, false),
    };
}

/// Packs a reindex column list into a contiguous OPK PK region.
pub(crate) struct ReindexPacker {
    /// `cols[..n]` are the packed columns, between the bitmap and the fold.
    cols: [KeyCol; MAX_PK_COLUMNS],
    n: u8,
    pub(crate) out_stride: usize,
    /// Group key only: a leading `U8` slot, bit *i* set iff packed column *i* is
    /// NULL.
    has_bitmap: bool,
    /// Group key only: the columns past the packed ones, hashed into a trailing
    /// 16-byte slot.
    fold: FoldCols,
}

impl ReindexPacker {
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

    /// The packer of `span` alone, each column at its own type: no bitmap, no fold.
    pub(crate) fn of_span(span: &KeySpec) -> Self {
        let cols = span.locators().iter().map(|&loc| (loc, loc.type_code()));
        Self::finish(cols, false, FoldCols::new(Vec::new()))
    }

    /// The packer over `packed` — each column and its slot type — its stride the
    /// sum of [`Self::key_columns`].
    fn finish(packed: impl IntoIterator<Item = (ColumnLocator, TypeCode)>, has_bitmap: bool, fold: FoldCols) -> Self {
        let mut packer = ReindexPacker {
            cols: [KeyCol::EMPTY; MAX_PK_COLUMNS],
            n: 0,
            out_stride: 0,
            has_bitmap,
            fold,
        };
        for (loc, tc) in packed {
            packer.cols[packer.n as usize] = KeyCol { loc, out: SchemaColumn::new(tc, false) };
            packer.n += 1;
        }
        packer.out_stride = packer.key_columns().map(|c| c.size()).sum();
        packer
    }

    /// The packed columns, in key order.
    fn columns(&self) -> &[KeyCol] {
        &self.cols[..self.n as usize]
    }

    /// The output PK columns this packer's bytes fill, in key order.
    fn key_columns(&self) -> impl Iterator<Item = SchemaColumn> + '_ {
        let bitmap = self.has_bitmap.then_some(BITMAP_COL);
        let fold = (!self.fold.is_empty()).then_some(FOLD_COL);
        bitmap
            .into_iter()
            .chain(self.columns().iter().map(|c| c.out))
            .chain(fold)
    }

    /// The columns, when the packed key is their own OPK images laid end to end.
    pub(crate) fn identity_columns(&self) -> Option<Vec<ColumnLocator>> {
        let identity = |c: &KeyCol| {
            let src = c.loc.type_code();
            !src.is_german_string()
                && c.out.size() == c.loc.size()
                && gnitz_wire::opk_bias(src) == gnitz_wire::opk_bias(c.out.type_code)
        };
        let cols = self.columns();
        (!self.has_bitmap && self.fold.is_empty() && cols.iter().all(identity))
            .then(|| cols.iter().map(|c| c.loc).collect())
    }

    /// Every column is packed: none is folded into the trailing hash slot.
    pub(crate) fn packs_whole(&self) -> bool {
        self.fold.is_empty()
    }

    /// Where the key lies in the input's own PK region, as `(at, n)`, when it is
    /// consecutive PK columns' own images.
    pub(crate) fn pk_range(&self) -> Option<(usize, usize)> {
        let locs = self.identity_columns()?;
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

    /// `row`'s packed key of at most 16 bytes, widened as `widen_pk_be` does.
    #[inline]
    pub(crate) fn narrow_image<R: RowSource>(&self, batch: &R, row: usize) -> u128 {
        let mut be = [0u8; 16];
        self.pack_into(&mut be[16 - self.out_stride..], batch, row);
        u128::from_be_bytes(be)
    }

    /// Every row's packed key, `out_stride` bytes each.
    pub(crate) fn keys<B: BatchView>(&self, batch: &B) -> Vec<u8> {
        let n = batch.row_count();
        let mut keys = vec![0u8; n * self.out_stride];
        self.pack_rows(&mut keys, self.out_stride, batch, &[(0, n)]);
        keys
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

    /// The least and the greatest [`Self::pack_prefix`] among `rows`; `None` for
    /// no row.
    pub(crate) fn prefix_span<R: RowSource>(
        &self,
        batch: &R,
        rows: impl Iterator<Item = usize>,
    ) -> Option<(PkBuf, PkBuf)> {
        let mut key = [0u8; MAX_PK_BYTES];
        rows.map(|row| PkBuf::from_bytes(self.pack_prefix(&mut key, batch, row)))
            .fold(None, |span, prefix| match span {
                None => Some((prefix, prefix)),
                Some((lo, hi)) => Some((lo.min(prefix), hi.max(prefix))),
            })
    }

    /// Pack `row`'s key into `dst`, all `out_stride` bytes of it.
    #[inline(always)]
    fn pack_into<R: RowSource>(&self, dst: &mut [u8], batch: &R, row: usize) {
        let null_word = batch.get_null_word(row);
        let mut off = usize::from(self.has_bitmap) * BITMAP_BYTES;
        let mut null_bits = 0u8;
        for (i, &KeyCol { loc, out }) in self.columns().iter().enumerate() {
            let w = out.size();
            let cell = &mut dst[off..off + w];
            off += w;
            match loc {
                _ if self.has_bitmap && loc.is_null_word(null_word) => {
                    null_bits |= 1 << i;
                    cell.fill(0);
                }
                ColumnLocator::Payload { slot, type_code, .. } if type_code.is_german_string() => {
                    let h = gnitz_wire::checksum_128(gnitz_wire::payload_bytes(batch, row, slot as usize));
                    cell.copy_from_slice(&h.to_be_bytes());
                }
                _ => {
                    let image = promote_image(loc.opk_image(batch, row), loc.type_code(), out.type_code);
                    gnitz_wire::store_opk(cell, image, false)
                }
            }
        }
        if self.has_bitmap {
            dst[0] = null_bits;
        }
        if !self.fold.is_empty() {
            let h = self.fold.key_row(batch, row, null_word);
            dst[off..off + FOLD_BYTES].copy_from_slice(&h.to_be_bytes());
        }
    }

    /// Every row of `batch` with a `width`-byte buffer, its packed key at the front.
    pub(crate) fn for_each_key<B: BatchView>(&self, batch: &B, width: usize, mut f: impl FnMut(usize, &mut [u8])) {
        let rows = batch.row_count();
        let mut bufs = vec![0u8; rows.min(KEY_CHUNK) * width];
        for start in (0..rows).step_by(KEY_CHUNK) {
            let n = (rows - start).min(KEY_CHUNK);
            self.pack_rows(&mut bufs, width, batch, &[(start, start + n)]);
            for (i, buf) in bufs.chunks_exact_mut(width).take(n).enumerate() {
                f(start + i, buf);
            }
        }
    }

    /// Whether [`Self::pack_rows`] packs a column at a time.
    fn packs_by_column(&self) -> bool {
        self.fold.is_empty() && self.columns().iter().all(|c| !c.loc.type_code().is_german_string())
    }

    /// [`Self::pack_into`] for the rows of `runs` — ascending `[start, end)` — in order,
    /// into the fronts of `dst`'s slots `stride` apart.
    pub(crate) fn pack_rows<B: BatchView>(&self, dst: &mut [u8], stride: usize, batch: &B, runs: &[(usize, usize)]) {
        let dst = &mut dst[..range_rows(runs) * stride];
        if !self.packs_by_column() {
            let mut keys = dst.chunks_exact_mut(stride);
            for &(s, e) in runs {
                // The rows lead the zip: it stops on them without taking a key it leaves unwritten.
                for (row, key) in (s..e).zip(keys.by_ref()) {
                    self.pack_into(&mut key[..self.out_stride], batch, row);
                }
            }
            return;
        }
        let mut off = usize::from(self.has_bitmap) * BITMAP_BYTES;
        if self.has_bitmap {
            dst.chunks_exact_mut(stride).for_each(|key| key[0] = 0);
        }
        for (i, &KeyCol { loc, out }) in self.columns().iter().enumerate() {
            let (sw, dw) = (loc.size(), out.size());
            let col = IntCol {
                dst: &mut *dst,
                runs,
                stride,
                off,
                bias: gnitz_wire::opk_bias(out.type_code),
            };
            let signed = loc.type_code().is_signed_int();
            match loc {
                ColumnLocator::Pk { byte_off, .. } => {
                    let (pk, pk_stride) = batch.pk_region();
                    col.dispatch::<true>(sw, dw, signed, pk, pk_stride, byte_off as usize);
                }
                ColumnLocator::Payload { slot, .. } => {
                    col.dispatch::<false>(sw, dw, signed, batch.col_data(slot as usize, sw), sw, 0);
                    if self.has_bitmap {
                        // A NULL packs as a zeroed slot under its bitmap bit.
                        let nulls = batch.null_bmp().as_chunks::<8>().0;
                        let mut rest = &mut *dst;
                        for &(s, e) in runs {
                            let (keys, tail) = rest.split_at_mut((e - s) * stride);
                            rest = tail;
                            for (key, word) in keys.chunks_exact_mut(stride).zip(&nulls[s..e]) {
                                if gnitz_wire::null_word_get(u64::from_le_bytes(*word), slot as usize) {
                                    key[0] |= 1 << i;
                                    key[off..off + dw].fill(0);
                                }
                            }
                        }
                    }
                }
            }
            off += dw;
        }
    }
}

/// Rows [`ReindexPacker::for_each_key`] packs per [`ReindexPacker::pack_rows`] call.
const KEY_CHUNK: usize = 256;

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
            let keys = keys.chunks_exact_mut(stride);
            match src_stride == SW {
                true => src.as_chunks::<SW>().0.iter().zip(keys).for_each(|(c, k)| encode(c, k)),
                false => src
                    .chunks_exact(src_stride)
                    .zip(keys)
                    .for_each(|(row, k)| encode(row[src_off..src_off + SW].try_into().unwrap(), k)),
            }
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

// ---------------------------------------------------------------------------
// Secondary-index entries
// ---------------------------------------------------------------------------

/// The row runs of `mb` that `spec` indexes — no NULL in an indexed column — and whose
/// weight `keep` admits.
fn indexed_runs(mb: &MemBatch<'_>, spec: &KeySpec, keep: impl Fn(i64) -> bool) -> Vec<(usize, usize)> {
    let indexed_slots = spec.locators().iter().fold(0, |slots, loc| slots | loc.null_bit());
    let weights = mb.weight().as_chunks::<8>().0;
    let nulls = mb.null_bmp().as_chunks::<8>().0;
    runs_where(weights.len(), |row| {
        keep(i64::from_le_bytes(weights[row])) && u64::from_le_bytes(nulls[row]) & indexed_slots == 0
    })
}

/// Append to `out` one zeroed `slot`-byte record per row [`indexed_runs`] yields, in row
/// order, the row's `spec` span at its front.
pub fn append_spans(out: &mut Vec<u8>, slot: usize, mb: &MemBatch<'_>, spec: &KeySpec, keep: impl Fn(i64) -> bool) {
    let runs = indexed_runs(mb, spec, keep);
    let at = out.len();
    out.resize(at + range_rows(&runs) * slot, 0);
    ReindexPacker::of_span(spec).pack_rows(&mut out[at..], slot, mb, &runs);
}

/// Each nonzero-weight row of `source` that `spec` indexes, as its index entry
/// `[span ‖ source PK]` at that row's weight, in source order. A row NULL in an
/// indexed column has no entry.
pub fn index_entries(source: &Batch, spec: &KeySpec, idx_schema: &SchemaDescriptor) -> Batch {
    /// Copy each `runs` row's `W`-byte PK behind its entry's span.
    #[inline(always)]
    fn suffix<const W: usize>(
        entries: &mut [u8],
        idx_stride: usize,
        key_size: usize,
        pks: &[u8],
        runs: &[(usize, usize)],
    ) {
        let mut entries = entries.chunks_exact_mut(idx_stride);
        for &(s, e) in runs {
            // The PKs lead the zip: it stops on them without taking an entry it leaves unwritten.
            for (pk, entry) in pks[s * W..e * W].as_chunks::<W>().0.iter().zip(entries.by_ref()) {
                let d: &mut [u8; W] = (&mut entry[key_size..]).try_into().unwrap();
                *d = *pk;
            }
        }
    }

    let (idx_stride, key_size) = (idx_schema.pk_stride(), spec.key_size());
    let src_stride = source.schema().pk_stride();
    assert_eq!(idx_stride, key_size + src_stride, "index schema of another spec");
    let mb = source.as_mem_batch();
    let runs = indexed_runs(&mb, spec, |w| w != 0);
    let rows = range_rows(&runs);
    write_to_batch(idx_schema, rows, 0, |w| {
        let (entries, weights, nulls) = w.fixed_mut();
        ReindexPacker::of_span(spec).pack_rows(entries, idx_stride, &mb, &runs);
        let pks = source.pk_data();
        match src_stride {
            4 => suffix::<4>(entries, idx_stride, key_size, pks, &runs),
            8 => suffix::<8>(entries, idx_stride, key_size, pks, &runs),
            16 => suffix::<16>(entries, idx_stride, key_size, pks, &runs),
            24 => suffix::<24>(entries, idx_stride, key_size, pks, &runs),
            32 => suffix::<32>(entries, idx_stride, key_size, pks, &runs),
            _ => {
                let mut entries = entries.chunks_exact_mut(idx_stride);
                for &(s, e) in &runs {
                    let pks = pks[s * src_stride..e * src_stride].chunks_exact(src_stride);
                    for (pk, entry) in pks.zip(entries.by_ref()) {
                        entry[key_size..].copy_from_slice(pk);
                    }
                }
            }
        }
        copy_runs::<8>(source.weight_data(), weights, runs.iter().copied(), 8);
        // An index entry is all key: it has no payload column to be NULL.
        nulls.fill(0);
    })
}

#[cfg(test)]
#[path = "tests/reindex.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/reindex.rs"]
mod bench;
