//! The operator key composers: the hashed key bytes of a string column's content
//! and a group key's fold slot, and [`ReindexPacker`], which packs a reindex
//! column list into the contiguous OPK region a `_join_pk` or a group key holds.
//! The reindex Map and the exchange scatter both key rows through it, so the
//! reindexed trace side and the scattered delta co-partition byte-for-byte.

use std::cell::Cell;

use gnitz_expr::{BatchView, RowSource};

use crate::schema::key::EMPTY_LOC;
use crate::schema::{
    oob_col, ColumnLocator, DerivedSchema, SchemaColumn, SchemaDescriptor, SchemaFacts, TypeCode, MAX_PK_BYTES,
    MAX_PK_COLUMNS,
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

/// Columns a fold assembles on the stack: the PK arity, which every real group
/// set fits.
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
// ReindexPacker — the synthetic-key composer
// ---------------------------------------------------------------------------

/// Synthetic-PK / routing key for a German-string column's content. Both the
/// reindex Map (setting a row's `_join_pk`) and the exchange scatter (routing the
/// raw delta) reach it through the packer's `String` arm, so a string join key
/// scatters to the worker that owns its own `_join_pk` partition. Empty content —
/// including a NULL string, a zeroed German-string struct — hashes to 0.
#[inline]
pub(crate) fn german_string_promote_key(struct_bytes: &[u8], blob: &[u8]) -> u128 {
    let content = gnitz_wire::german_string_content(struct_bytes, blob);
    if content.is_empty() {
        return 0; // NULL / empty-string sentinel
    }
    // A true 128-bit content hash. A 64-bit hash widened to 128 bits would carry
    // only 2^64 of entropy — a ~2^32-row birthday bound past which two distinct
    // strings collide to one `_join_pk` and the join's OPK byte-compare silently
    // equijoins them.
    gnitz_wire::checksum_128(content)
}

// ---------------------------------------------------------------------------
// Packed group key
// ---------------------------------------------------------------------------

/// The two slots a packed group key carries besides its columns: a leading
/// presence bitmap and a trailing overflow fold. Every width below is read back
/// off these, so the schema, the stride and the pack offsets cannot disagree.
const BITMAP_COL: SchemaColumn = SchemaColumn::new(TypeCode::U8, false);
const FOLD_COL: SchemaColumn = SchemaColumn::new(TypeCode::U128, false);
const BITMAP_BYTES: usize = BITMAP_COL.size() as usize;
const FOLD_BYTES: usize = FOLD_COL.size() as usize;
// `pack_into` writes the bitmap as one bare byte and the fold as a `u128`'s
// big-endian image, which are those columns' OPK images only at these widths.
const _: () = assert!(BITMAP_BYTES == 1 && FOLD_BYTES == 16);

/// How one source column's bytes become key bytes. The group key's bitmap and
/// fold are facts of the whole [`ReindexPacker`], not columns, and live there.
#[derive(Clone, Copy)]
enum PromoteKind {
    /// Any scalar source column, at either width and either sign: the locator
    /// says which region holds the bytes, and the encode differs only by that.
    /// Never a float — both constructors reject one, because a reindex key is
    /// compared as raw OPK bytes and no IEEE-754 image survives that.
    Col(ColumnLocator),
    /// STRING/BLOB payload: sign-agnostic XXH3 content-hash key. The only source
    /// that is not a scalar cell the OPK encoders can consume.
    String(ColumnLocator),
}

/// Per-column classifier for "read a source column, project it to OPK PK
/// bytes". Runs once per column at construction; the resulting `PromoteKind` is
/// stored on the `ColPromoter`, and the per-row work is the read + OPK encode
/// `ReindexPacker::pack_into` performs.
fn classify_promote(loc: ColumnLocator) -> PromoteKind {
    match loc {
        // BLOB shares the 16-byte German-string struct layout with STRING, so it
        // takes the same hash path rather than a raw cell encode. Neither can be
        // a PK column, so only the payload arm needs the test.
        ColumnLocator::Payload { type_code, .. } if type_code.is_german_string() => PromoteKind::String(loc),
        _ => PromoteKind::Col(loc),
    }
}

/// One key column of a `ReindexPacker`: the output PK column it packs into —
/// resolved once at construction, and its `size()` is the slot width, so the
/// packed bytes and [`ReindexPacker::output_schema`] read one value rather than
/// two — plus the `PromoteKind` that says where the source bytes come from.
#[derive(Clone, Copy)]
struct ColPromoter {
    out_col: SchemaColumn,
    /// Group key only: the source column is nullable, so a NULL packs a zeroed
    /// slot and sets its bit in the presence bitmap.
    nullable: bool,
    kind: PromoteKind,
}

impl ColPromoter {
    /// Unused slots of the fixed `cols` array, matching what `IndexKeySpec::new`
    /// fills its own with: the schema layer's designated padding column and a
    /// zeroed PK locator.
    const PLACEHOLDER: ColPromoter = ColPromoter {
        out_col: SchemaColumn::EMPTY,
        nullable: false,
        kind: PromoteKind::Col(EMPTY_LOC),
    };

    /// A slot packing into a `out_tc` output PK column. The one place the output
    /// column is spelled, because it is never nullable while `nullable` — the
    /// *source* column's — routinely is.
    pub(crate) fn new(out_tc: TypeCode, nullable: bool, kind: PromoteKind) -> Self {
        ColPromoter {
            out_col: SchemaColumn::new(out_tc, false),
            nullable,
            kind,
        }
    }
}

/// Packs a reindex column list into a contiguous OPK PK region. The same packer
/// drives both the reindex map (which writes the synthetic `_join_pk` at
/// emission — `ops::MapPlan`'s `PkSource::Pack`) and the exchange scatter
/// (which routes the raw delta by the same key), so the reindexed trace side and
/// the delta scatter side co-partition byte-for-byte at every key arity and
/// width.
pub(crate) struct ReindexPacker {
    cols: [ColPromoter; MAX_PK_COLUMNS], // first `num_cols` valid
    num_cols: usize,
    pub(crate) out_stride: usize,
    /// Group-key only: a leading `U8` slot, bit *i* set iff packed column *i* is
    /// NULL. Without it a NULL group and a `0` group collide on one output PK.
    has_bitmap: bool,
    /// Group columns past the packed prefix, hashed into a trailing 16-byte
    /// fold slot. Empty for a join key and for a group key with no fold.
    fold: FoldCols,
}

impl ReindexPacker {
    /// Build per-column promoters from the reindex column list (key order),
    /// tightly packed with no inter-column padding. The **only** derivation of
    /// that layout: [`Self::output_schema`] reads these same promoters, so the
    /// two cannot disagree per slot while still agreeing on the total stride —
    /// which would silently stop equal keys co-partitioning.
    ///
    /// The whole trust boundary for a key list off the wire: a forged circuit is
    /// rejected, never panicked on.
    ///
    /// A carried target's domain is `join_key_common_type`'s codomain — the
    /// *key* domain, whose collapse to U128 is what a `_join_pk` slot does to a
    /// UUID pair, not the value domain `TypeCode::int_domain_fits` answers.
    pub(crate) fn new(schema: &SchemaDescriptor, key: &[gnitz_wire::ReindexSlot]) -> Result<Self, String> {
        if key.len() > MAX_PK_COLUMNS {
            return Err(format!(
                "reindex key: {} columns exceeds the {MAX_PK_COLUMNS}-column PK limit",
                key.len()
            ));
        }
        let mut cols = [ColPromoter::PLACEHOLDER; MAX_PK_COLUMNS];
        let mut stride = 0usize;
        for (i, &(c, carried)) in key.iter().enumerate() {
            let loc = locate_key_col(schema, c, "reindex key")?;
            if carried.is_some_and(|t| loc.type_code().join_key_common_type(t) != Some(t)) {
                return Err(format!(
                    "reindex key: column {c} does not promote to the carried target"
                ));
            }
            let kind = classify_promote(loc);
            // Carried promotion target (`None` = self-derive); the slot type and
            // width follow `resolve_reindex_type` so the scatter packer and the
            // trace-side reindex Map derive identical widths.
            let cp = ColPromoter::new(gnitz_wire::resolve_reindex_type(loc.type_code(), carried), false, kind);
            // A payload slot right-aligns its source, so one narrower than the
            // source would truncate it. `resolve_reindex_type` never derives that;
            // debug-only because it is a type-system invariant, not input.
            debug_assert!(
                cp.out_col.size() as usize >= loc.type_code().wire_stride()
                    || !matches!(kind, PromoteKind::Col(ColumnLocator::Payload { .. }))
            );
            stride += cp.out_col.size() as usize;
            cols[i] = cp;
        }
        if stride > MAX_PK_BYTES {
            return Err(format!(
                "reindex key: {stride} PK bytes exceeds the {MAX_PK_BYTES}-byte limit"
            ));
        }
        Ok(ReindexPacker {
            cols,
            num_cols: key.len(),
            out_stride: stride,
            has_bitmap: false,
            fold: FoldCols::new(Vec::new()),
        })
    }

    /// The output PK columns this packer's bytes fill, in key order. The
    /// promoters are the only derivation of that layout, so a schema built from
    /// these describes what `pack_into` writes by construction.
    pub(crate) fn key_columns(&self) -> impl Iterator<Item = SchemaColumn> + '_ {
        let bitmap = self.has_bitmap.then_some(BITMAP_COL);
        let fold = (!self.fold.is_empty()).then_some(FOLD_COL);
        bitmap
            .into_iter()
            .chain(self.cols[..self.num_cols].iter().map(|cp| cp.out_col))
            .chain(fold)
    }

    /// The reindex Map's output schema: the packer's own [`Self::key_columns`]
    /// — so this schema's stride and `out_stride` are the same sum — then
    /// `in_schema.columns[payload_cols[i]]`. `payload_cols` is what the reindex
    /// program copies, so a join side skipping a dead column stops persisting it.
    ///
    /// The PK-side bounds are `new`'s; `payload_cols` is bounded here.
    pub(crate) fn output_schema(
        &self,
        in_schema: &SchemaDescriptor,
        payload_cols: &[u32],
    ) -> Result<SchemaDescriptor, String> {
        let over = |e| format!("reindex map: output {e}");
        let mut b = DerivedSchema::new();
        for c in self.key_columns() {
            b.push_pk(c).map_err(over)?;
        }
        for &c in payload_cols {
            let col = in_schema
                .column(c as usize)
                .ok_or_else(|| oob_col("reindex map: payload column", c, in_schema))?;
            b.push(col).map_err(over)?;
        }
        Ok(b.finish())
    }

    /// Build the packer for a **group** key over `group_cols`, and the PK region
    /// of an index keyed by it: [`Self::key_columns`], then `suffix`.
    ///
    /// Greedy: pack leading columns while the budget still leaves room for the
    /// fold slot the rest would need. Unlike a join key this is total over
    /// *arity* — the overflow folds into one hash slot — so no group set is
    /// refused for being wide; it refuses what [`locate_key_col`] refuses.
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
        assert!(max_cols <= 9, "the one bitmap byte addresses at most 8 packed columns");
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
        let mut cols = [ColPromoter::PLACEHOLDER; MAX_PK_COLUMNS];
        let mut stride = lead * BITMAP_BYTES;
        let mut n_packed = 0usize;
        for (i, &(col, loc)) in group.iter().enumerate() {
            // A group column takes the same slot a join key's would; the float
            // arm the two policies would differ on returned above.
            let out_tc = col.type_code.reindex_output_type();
            let w = out_tc.wire_stride();
            // Room this column needs, plus the fold slot the columns behind it
            // would still require. Reserving it here is what keeps the greedy
            // walk from packing a column it would have to give back.
            let tail_cols = usize::from(i + 1 < group.len());
            if lead + n_packed + 1 + tail_cols > max_cols || stride + w + tail_cols * FOLD_BYTES > max_bytes {
                break;
            }
            cols[n_packed] = ColPromoter::new(out_tc, col.nullable, classify_promote(loc));
            stride += w;
            n_packed += 1;
        }
        let fold = FoldCols::new(group[n_packed..].iter().map(|&(_, loc)| loc).collect());
        stride += if fold.is_empty() { 0 } else { FOLD_BYTES };

        let packer = ReindexPacker {
            cols,
            num_cols: n_packed,
            out_stride: stride,
            has_bitmap,
            fold,
        };
        let mut b = DerivedSchema::new();
        for c in packer.key_columns().chain(suffix.iter().copied()) {
            b.push_pk(c)
                .expect("a group key packed inside the suffix's budget, plus the suffix, is non-null PK-eligible");
        }
        Ok((packer, b))
    }

    /// [`Self::pack_into`] over the leading `out_stride` bytes of `buf`,
    /// returning them — the prefix a group-keyed secondary index seeks by, so the
    /// key's width is read off the packer rather than re-sliced per index.
    #[inline]
    pub(crate) fn pack_prefix<'a, R: RowSource>(&self, buf: &'a mut [u8], batch: &R, row: usize) -> &'a [u8] {
        let n = self.out_stride;
        self.pack_into(&mut buf[..n], batch, row);
        &buf[..n]
    }

    /// Pack the full reindex key (`out_stride` OPK bytes) for `row` into `dst`.
    ///
    /// One pass over the source columns, between the two key-level slots a
    /// group key carries: the leading presence bitmap, whose bits are the NULL
    /// tests the packed slots already perform, and the trailing fold.
    #[inline]
    pub(crate) fn pack_into<R: RowSource>(&self, dst: &mut [u8], batch: &R, row: usize) {
        let null_word = batch.get_null_word(row);
        let mut off = usize::from(self.has_bitmap) * BITMAP_BYTES;
        let mut null_bits = 0u8;
        for (i, cp) in self.cols[..self.num_cols].iter().enumerate() {
            let w = cp.out_col.size() as usize;
            let slot = &mut dst[off..off + w];
            off += w;
            match cp.kind {
                // A NULL packed column: zeroed slot, and its bit in the bitmap.
                PromoteKind::Col(loc) | PromoteKind::String(loc) if cp.nullable && loc.is_null_word(null_word) => {
                    null_bits |= 1 << i;
                    slot.fill(0);
                }
                PromoteKind::Col(loc) => loc.encode_opk_promoted(batch, row, cp.out_col.type_code, slot),
                PromoteKind::String(loc) => {
                    let h = german_string_promote_key(loc.bytes(batch, row), batch.blob());
                    slot.copy_from_slice(&h.to_be_bytes());
                }
            }
        }
        // Both unconditional per row for the keys that have them, so every slot
        // of `dst` is fully overwritten — which is what lets a caller reuse one
        // destination across rows with no inter-row clear.
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

    /// Whether [`Self::pack_rows`] packs a column at a time: every column a plain
    /// integer, and no bitmap or fold.
    fn packs_by_column(&self) -> bool {
        !self.has_bitmap
            && self.fold.is_empty()
            && self.cols[..self.num_cols]
                .iter()
                .all(|cp| !cp.nullable && matches!(cp.kind, PromoteKind::Col(_)))
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
        let cols = &self.cols[..self.num_cols];
        let mut off = 0;
        for cp in cols {
            let PromoteKind::Col(loc) = cp.kind else {
                unreachable!("a by-column key packs integer columns only")
            };
            let (sw, dw) = (loc.size(), cp.out_col.size() as usize);
            let col = IntCol {
                dst: &mut *dst,
                stride,
                off,
                bias: gnitz_wire::opk_bias(cp.out_col.type_code, dw),
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

#[cfg(test)]
#[path = "tests/reindex.rs"]
mod tests;
