//! The operator key composers: the hashed key bytes of a string column's content
//! and a group key's fold slot, and [`ReindexPacker`], which packs a reindex
//! column list into the contiguous OPK region a `_join_pk` or a group key holds.
//! The reindex Map keys rows through it and the exchange scatter routes by the
//! bytes it packs, so the reindexed trace side and the scattered delta
//! co-partition byte-for-byte. [`index_entries`] is a secondary index's
//! projection of its owner's rows.

use std::cell::Cell;

use gnitz_expr::{BatchView, RowSource};

use crate::repr::{range_rows, runs_where, Batch};

use crate::schema::{
    key::KeyCol, oob_col, ColumnLocator, DerivedSchema, KeySpec, SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode,
    MAX_PK_BYTES, MAX_PK_COLUMNS,
};

// ---------------------------------------------------------------------------
// Row-content key material — the hashed key bytes, for the slots no scalar OPK
// encode can produce: a string column's content and the group key's fold slot.
// ---------------------------------------------------------------------------

/// Column `c` of `schema` as a key column, `what` naming the key in a refusal.
/// A float has none: `+0.0` and `-0.0` differ byte-wise but compare equal.
pub(crate) fn locate_key_col(schema: &SchemaDescriptor, c: u32, what: &str) -> Result<ColumnLocator, String> {
    let loc = schema
        .try_locate(c as usize)
        .ok_or_else(|| oob_col(&format!("{what}: column"), c, schema))?;
    if loc.type_code().is_float() {
        return Err(format!(
            "{what}: column {c} is a float, which has no order-preserving key image"
        ));
    }
    Ok(loc)
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
            let content = gnitz_expr::payload_bytes(src, row, slot as usize);
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

/// The columns one 128-bit row digest folds, and whether their key bytes fit
/// the stack buffer.
pub(crate) struct FoldCols {
    locs: Vec<ColumnLocator>,
    /// Every column is fixed-width, and there are at most `FOLD_INLINE_COLS`.
    inline: bool,
}

impl FoldCols {
    pub(crate) fn new(locs: Vec<ColumnLocator>) -> Self {
        let inline = locs.len() <= FOLD_INLINE_COLS && !locs.iter().any(|l| l.type_code().is_german_string());
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
            // `push_col_key`'s bytes, written to the stack.
            let mut buf = [0u8; 17 * FOLD_INLINE_COLS];
            let mut n = 0usize;
            for &loc in &self.locs {
                if loc.is_null_word(null_word) {
                    buf[n] = 0;
                    n += 1;
                    continue;
                }
                buf[n] = 1;
                buf[n + 1..n + 17].copy_from_slice(&loc.opk_image(src, row).to_le_bytes());
                n += 17;
            }
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
const BITMAP_BYTES: usize = BITMAP_COL.size() as usize;
const FOLD_BYTES: usize = FOLD_COL.size() as usize;
// `pack_into` writes the bitmap as a `u8` and the fold as a `u128`.
const _: () = assert!(BITMAP_BYTES == size_of::<u8>() && FOLD_BYTES == size_of::<u128>());
// The bitmap has a bit for every column packed behind it.
const _: () = assert!(MAX_PK_COLUMNS - 1 <= 8 * BITMAP_BYTES);
// Any join key fits the PK region, so `new` checks no width.
const _: () = assert!(MAX_PK_COLUMNS * TypeCode::U128.wire_stride() <= MAX_PK_BYTES);

// ---------------------------------------------------------------------------
// ReindexPacker — the synthetic-key composer
// ---------------------------------------------------------------------------

/// Packs a reindex column list into a contiguous OPK PK region.
pub(crate) struct ReindexPacker {
    /// The packed columns, between the bitmap and the fold.
    span: KeySpec,
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
            let loc = locate_key_col(schema, c, "reindex key")?;
            let src = loc.type_code();
            if !src.packs_at(t) {
                return Err(format!("reindex key: column {c} of type {src} does not pack at {t}"));
            }
            cols.push((loc, t));
        }
        Ok(Self::of_span(KeySpec::of(cols)))
    }

    /// The packer of `span` alone: no bitmap, no fold.
    pub(crate) fn of_span(span: KeySpec) -> Self {
        Self::finish(span, false, FoldCols::new(Vec::new()))
    }

    /// The packer over `span`, its stride the sum of [`Self::key_columns`].
    fn finish(span: KeySpec, has_bitmap: bool, fold: FoldCols) -> Self {
        let mut packer = ReindexPacker { span, out_stride: 0, has_bitmap, fold };
        packer.out_stride = packer.key_columns().map(|c| c.size() as usize).sum();
        packer
    }

    /// The output PK columns this packer's bytes fill, in key order.
    fn key_columns(&self) -> impl Iterator<Item = SchemaColumn> + '_ {
        let bitmap = self.has_bitmap.then_some(BITMAP_COL);
        let fold = (!self.fold.is_empty()).then_some(FOLD_COL);
        bitmap
            .into_iter()
            .chain(self.span.columns().iter().map(|c| c.out))
            .chain(fold)
    }

    /// The columns, when the packed key is their own OPK images laid end to end.
    pub(crate) fn identity_columns(&self) -> Option<Vec<ColumnLocator>> {
        let identity = |c: &KeyCol| {
            let (src, w) = (c.loc.type_code(), c.loc.size());
            !src.is_german_string()
                && c.out.size() as usize == w
                && gnitz_wire::opk_bias(src, w) == gnitz_wire::opk_bias(c.out.type_code, w)
        };
        let cols = self.span.columns();
        (!self.has_bitmap && self.fold.is_empty() && cols.iter().all(identity))
            .then(|| cols.iter().map(|c| c.loc).collect())
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
            let col = in_schema
                .column(c as usize)
                .ok_or_else(|| oob_col("reindex map: payload column", c, in_schema))?;
            b.push(col);
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
        let max_bytes = MAX_PK_BYTES - suffix.iter().map(|c| c.size() as usize).sum::<usize>();
        assert!(
            max_cols >= 2 && max_bytes >= BITMAP_BYTES + FOLD_BYTES,
            "a group-key suffix must leave room for a bitmap byte and a fold slot",
        );
        let group: Vec<(SchemaColumn, ColumnLocator)> = group_cols
            .iter()
            .map(|&c| {
                let loc = locate_key_col(schema, c, "group key")?;
                Ok((schema.columns[c as usize], loc))
            })
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
        let packer = Self::finish(KeySpec::of(packed), has_bitmap, fold);
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
        for (i, &KeyCol { loc, out }) in self.span.columns().iter().enumerate() {
            let w = out.size() as usize;
            let cell = &mut dst[off..off + w];
            off += w;
            match loc {
                _ if self.has_bitmap && loc.is_null_word(null_word) => {
                    null_bits |= 1 << i;
                    cell.fill(0);
                }
                ColumnLocator::Payload { slot, type_code, .. } if type_code.is_german_string() => {
                    let h = gnitz_wire::checksum_128(gnitz_expr::payload_bytes(batch, row, slot as usize));
                    cell.copy_from_slice(&h.to_be_bytes());
                }
                _ => loc.encode_opk_promoted(batch, row, out.type_code, cell),
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
            self.pack_rows(&mut bufs, width, batch, start, n);
            for (i, buf) in bufs.chunks_exact_mut(width).take(n).enumerate() {
                f(start + i, buf);
            }
        }
    }

    /// Whether [`Self::pack_rows`] packs a column at a time.
    fn packs_by_column(&self) -> bool {
        self.fold.is_empty()
            && self
                .span
                .columns()
                .iter()
                .all(|c| !c.loc.type_code().is_german_string())
    }

    /// [`Self::pack_into`] for `n` rows from `start`, into the fronts of `dst`'s
    /// `n` slots `stride` apart.
    pub(crate) fn pack_rows<B: BatchView>(&self, dst: &mut [u8], stride: usize, batch: &B, start: usize, n: usize) {
        let dst = &mut dst[..n * stride];
        if !self.packs_by_column() {
            for (i, key) in dst.chunks_exact_mut(stride).enumerate() {
                self.pack_into(&mut key[..self.out_stride], batch, start + i);
            }
            return;
        }
        let mut off = usize::from(self.has_bitmap) * BITMAP_BYTES;
        if self.has_bitmap {
            dst.chunks_exact_mut(stride).for_each(|key| key[0] = 0);
        }
        for (i, &KeyCol { loc, out }) in self.span.columns().iter().enumerate() {
            let (sw, dw) = (loc.size(), out.size() as usize);
            let col = IntCol {
                dst: &mut *dst,
                stride,
                off,
                bias: gnitz_wire::opk_bias(out.type_code, dw),
            };
            let signed = loc.type_code().is_signed_int();
            match loc {
                ColumnLocator::Pk { byte_off, .. } => {
                    let (pk, pk_stride) = batch.pk_region();
                    let src = &pk[start * pk_stride..(start + n) * pk_stride];
                    col.dispatch::<true>(sw, dw, signed, src, pk_stride, byte_off as usize);
                }
                ColumnLocator::Payload { slot, .. } => {
                    let src = &batch.col_data(slot as usize, sw)[start * sw..(start + n) * sw];
                    col.dispatch::<false>(sw, dw, signed, src, sw, 0);
                    if self.has_bitmap {
                        // A NULL packs as a zeroed slot under its bitmap bit.
                        let nulls = batch.null_bmp()[start * 8..(start + n) * 8].as_chunks::<8>().0;
                        for (key, word) in dst.chunks_exact_mut(stride).zip(nulls) {
                            if gnitz_wire::null_word_get(u64::from_le_bytes(*word), slot as usize) {
                                key[0] |= 1 << i;
                                key[off..off + dw].fill(0);
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

/// One integer key column's slot, at `off` of each of `dst`'s keys.
struct IntCol<'a> {
    dst: &'a mut [u8],
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

    /// Each source row's `SW`-byte cell at `src_off` of its `src_stride` bytes of
    /// `src` — a PK column's OPK image when `PK`, else a native value — as its
    /// slot's `DW` bytes.
    #[inline(always)]
    fn pack<const SW: usize, const DW: usize, const PK: bool, const SIGNED: bool>(
        self,
        src: &[u8],
        src_stride: usize,
        src_off: usize,
    ) {
        let IntCol { dst, stride, off, bias } = self;
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
        let keys = dst.chunks_exact_mut(stride);
        match src_stride == SW {
            true => src.as_chunks::<SW>().0.iter().zip(keys).for_each(|(c, k)| encode(c, k)),
            false => src
                .chunks_exact(src_stride)
                .zip(keys)
                .for_each(|(row, k)| encode(row[src_off..src_off + SW].try_into().unwrap(), k)),
        }
    }
}

// ---------------------------------------------------------------------------
// Secondary-index entries
// ---------------------------------------------------------------------------

/// Each nonzero-weight row of `source` that `spec` indexes, as its index entry
/// `[span ‖ source PK]` at that row's weight, in source order. A row NULL in an
/// indexed column has no entry.
pub fn index_entries(source: &Batch, spec: &KeySpec, idx_schema: &SchemaDescriptor) -> Batch {
    let (idx_stride, key_size) = (idx_schema.pk_stride(), spec.key_size());
    let src_stride = source.schema().pk_stride();
    assert_eq!(idx_stride, key_size + src_stride, "index schema of another spec");
    let mb = source.as_mem_batch();
    let indexed_slots = spec.columns().iter().fold(0u64, |slots, c| match c.loc {
        ColumnLocator::Payload { slot, .. } => slots | 1u64 << slot,
        ColumnLocator::Pk { .. } => slots,
    });
    let weights = source.weight_data().as_chunks::<8>().0;
    let nulls = source.null_bmp_data().as_chunks::<8>().0;
    let runs = runs_where(source.len(), |row| {
        weights[row] != [0; 8] && u64::from_le_bytes(nulls[row]) & indexed_slots == 0
    });
    let rows = range_rows(&runs);
    if rows == 0 {
        return Batch::empty_with_schema(idx_schema);
    }
    let packer = ReindexPacker::of_span(*spec);
    let mut out = Batch::with_capacity(idx_schema, rows);
    out.grow_rows(rows);
    let mut at = 0;
    for &(s, e) in &runs {
        let end = at + (e - s);
        let entries = &mut out.pk_data_mut()[at * idx_stride..end * idx_stride];
        packer.pack_rows(entries, idx_stride, &mb, s, e - s);
        let pks = source.pk_data()[s * src_stride..e * src_stride].chunks_exact(src_stride);
        for (entry, pk) in entries.chunks_exact_mut(idx_stride).zip(pks) {
            entry[key_size..].copy_from_slice(pk);
        }
        out.weight_data_mut()[at * 8..end * 8].copy_from_slice(&source.weight_data()[s * 8..e * 8]);
        at = end;
    }
    // An index entry is all key: it has no payload column to be NULL.
    out.null_bmp_data_mut().fill(0);
    out
}

#[cfg(test)]
#[path = "tests/reindex.rs"]
mod tests;
