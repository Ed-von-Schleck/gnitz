//! The client batch: a Z-set in the canonical region order, its builders, and
//! its WAL-block encode and decode.

use gnitz_wire::{as_le_bytes, PkKeys, Regions, TypeCode};
use gnitz_wire::{FIXED_REGION_BYTES, MAX_PK_COLUMNS, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};

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
        let (width, signed) = (c.stride(), c.types[0].is_signed_int());
        for v in vals {
            gnitz_wire::push_opk(&mut c.buf, width, v, signed);
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
            gnitz_wire::encode_pk_cells(cells, w, tc.is_signed_int(), &mut c.buf, stride, off);
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
        // Hard, not debug-only: a short list would encode a short row and shift
        // every later one.
        assert_eq!(
            natives.len(),
            self.n as usize,
            "push_natives: one native value per key column"
        );
        for (&tc, &v) in self.types.iter().zip(natives) {
            gnitz_wire::push_opk(&mut self.buf, tc.wire_stride(), v, tc.is_signed_int());
        }
    }
}

/// One payload region: its wire type and its cells, `tc.wire_stride()` bytes
/// each (16-byte German cells for STRING/BLOB, into the batch's `blob`). The
/// type is fixed at construction, as a `PkColumn`'s stride is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadColumn {
    tc: TypeCode,
    pub bytes: Vec<u8>,
}

impl PayloadColumn {
    #[inline]
    pub(crate) fn tc(&self) -> TypeCode {
        self.tc
    }

    #[inline]
    pub(crate) fn stride(&self) -> usize {
        self.tc.wire_stride()
    }

    /// Append one zero cell: a NULL's filler, or a DROP COLUMN tombstone's value.
    pub fn push_zero(&mut self) {
        gnitz_wire::push_zero_cell(&mut self.bytes, self.tc)
    }
}

/// A batch in the canonical region order: every payload slot is its own wire region,
/// and a STRING/BLOB slot holds 16-byte German-string cells against
/// [`Self::blob`]. Nothing here is materialized — this is the form the wire
/// carries and the shared evaluator reads.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZSetBatch {
    pub pks: PkColumn,
    pub weights: Vec<i64>,
    pub nulls: Vec<u64>,
    /// One region per payload slot, in slot order: slot `pi` is null-bitmap bit
    /// `pi` and wire region `REG_PAYLOAD_START + pi`.
    pub payload: Vec<PayloadColumn>,
    /// The arena the German cells in `payload` point into.
    pub blob: Vec<u8>,
}

impl ZSetBatch {
    pub fn new(schema: &Schema) -> Self {
        ZSetBatch {
            pks: PkColumn::empty_for_schema(schema),
            weights: vec![],
            nulls: vec![],
            payload: Self::filler_columns(schema, 0),
            blob: vec![],
        }
    }

    /// The `-1` retraction batch for `pks`: the server's unique-PK rule
    /// retracts by PK alone, so the payload columns are inert filler.
    pub fn retraction(schema: &Schema, pks: PkColumn) -> Self {
        let count = pks.len();
        ZSetBatch {
            pks,
            weights: vec![-1; count],
            nulls: vec![0; count],
            payload: Self::filler_columns(schema, count),
            blob: vec![],
        }
    }

    /// One region per payload slot, zero-filled for `count` rows.
    pub fn filler_columns(schema: &Schema, count: usize) -> Vec<PayloadColumn> {
        schema
            .payload_columns()
            .map(|(_, _, c)| PayloadColumn {
                tc: c.ty.tc,
                bytes: vec![0; count * c.ty.tc.wire_stride()],
            })
            .collect()
    }

    /// Append the cell at row `i` of `src.payload[src_pi]` onto
    /// `self.payload[dst_pi]`. A German cell is relocated onto this batch's
    /// arena — its heap offset is relative to `src`'s and means nothing here.
    pub fn push_cell_from(&mut self, dst_pi: usize, src: &ZSetBatch, src_pi: usize, i: usize) {
        let col = &src.payload[src_pi];
        let tc = col.tc();
        debug_assert_eq!(self.payload[dst_pi].tc(), tc, "push_cell_from: slot type mismatch");
        if tc.is_german_string() {
            let cell = &col.bytes.as_chunks::<16>().0[i];
            let moved = gnitz_wire::relocate_german_string(cell, &src.blob, &mut self.blob);
            self.payload[dst_pi].bytes.extend_from_slice(&moved);
        } else {
            let w = tc.wire_stride();
            self.payload[dst_pi]
                .bytes
                .extend_from_slice(&col.bytes[i * w..(i + 1) * w]);
        }
    }

    /// Append row `i` of `src` to `self` at `weight`, verbatim — so a `-1`
    /// reproduces the stored row rather than relying on a decoder and an encoder
    /// agreeing. `src` and `self` must share a layout.
    pub fn copy_row_at(&mut self, src: &ZSetBatch, i: usize, weight: i64) {
        debug_assert!(self.same_layout(src), "copy_row_at: layout mismatch");
        self.pks.buf.extend_from_slice(src.pks.get_bytes(i));
        self.weights.push(weight);
        self.nulls.push(src.nulls[i]);
        for pi in 0..self.payload.len() {
            self.push_cell_from(pi, src, pi, i);
        }
    }

    /// Same key column types and the same payload type list.
    fn same_layout(&self, other: &ZSetBatch) -> bool {
        self.pks.key_types() == other.pks.key_types()
            && self.payload.len() == other.payload.len()
            && self.payload.iter().zip(&other.payload).all(|(a, b)| a.tc() == b.tc())
    }

    /// Overwrite the STRING cell at `(row, pi)`. The replaced cell's spill, if any,
    /// stays in the arena unreferenced.
    pub(crate) fn set_string_cell(&mut self, row: usize, pi: usize, v: &str) {
        let cell = gnitz_wire::encode_german_string(v.as_bytes(), &mut self.blob);
        self.payload[pi].bytes[row * 16..(row + 1) * 16].copy_from_slice(&cell);
    }

    /// Overwrite the 8-byte fixed-width cell at `(row, pi)` with `v`, little-endian.
    pub(crate) fn set_u64_cell(&mut self, row: usize, pi: usize, v: u64) {
        gnitz_wire::write_u64_le(&mut self.payload[pi].bytes, row * 8, v);
    }

    /// An empty batch with every growth stream sized for `n` rows: the PK buffer,
    /// the weights, the null words and each payload column.
    pub fn with_capacity(schema: &Schema, n: usize) -> Self {
        let mut b = Self::new(schema);
        b.reserve(n);
        b
    }

    /// Room for `n` more rows in every growth stream. Additive, as
    /// [`Vec::reserve`] is, so repeated appends to one batch compose.
    pub fn reserve(&mut self, n: usize) {
        self.pks.reserve(n);
        self.weights.reserve(n);
        self.nulls.reserve(n);
        for col in &mut self.payload {
            col.bytes.reserve(n * col.stride());
        }
    }

    pub fn len(&self) -> usize {
        self.weights.len()
    }

    pub fn is_empty(&self) -> bool {
        self.weights.is_empty()
    }

    /// Indices of the live rows — those with positive weight. A `ZSetBatch`
    /// row with weight ≤ 0 is a retraction/ghost, not a present element, so a
    /// catalog scan over a batch iterates only its live rows.
    pub(crate) fn live_rows(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).filter(move |&i| self.weights[i] > 0)
    }

    /// Append all rows of `other`, its German cells rebased onto this arena.
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
        RowMark {
            rows: self.weights.len(),
            blob: self.blob.len(),
        }
    }

    /// Drop every row and arena byte appended since `mark`. Valid while no cell of a row
    /// before the mark points into the arena past it.
    pub fn truncate_to(&mut self, mark: RowMark) {
        self.pks.buf.truncate(mark.rows * self.pks.stride());
        self.weights.truncate(mark.rows);
        self.nulls.truncate(mark.rows);
        self.blob.truncate(mark.blob);
        for col in &mut self.payload {
            col.bytes.truncate(mark.rows * col.stride());
        }
    }

    /// Keep only the rows in `ranges` (ascending, disjoint, `end` exclusive), in order, moving each
    /// region's survivors down in place. The arena is untouched, so German cells stay valid.
    pub fn retain_ranges(&mut self, ranges: &[(usize, usize)]) {
        if ranges == [(0, self.len())] {
            return;
        }
        let ps = self.pks.stride();
        let mut dst = 0;
        for &(s, e) in ranges {
            self.pks.buf.copy_within(s * ps..e * ps, dst * ps);
            self.weights.copy_within(s..e, dst);
            self.nulls.copy_within(s..e, dst);
            for col in &mut self.payload {
                let w = col.stride();
                col.bytes.copy_within(s * w..e * w, dst * w);
            }
            dst += e - s;
        }
        self.truncate_to(RowMark { rows: dst, blob: self.blob.len() });
    }

    /// `r` is a canonical region list in this batch's layout, each fixed region
    /// as long as the weight region's row count implies.
    fn check_region_lengths(&self, r: &[&[u8]]) -> Result<(), String> {
        let want = gnitz_wire::num_regions(self.payload.len());
        if r.len() != want {
            return Err(format!("{} regions != expected {want}", r.len()));
        }
        let weights = r[REG_WEIGHT].len();
        if !weights.is_multiple_of(FIXED_REGION_BYTES) {
            return Err(format!("weight region length {weights} is not whole rows"));
        }
        let n = weights / FIXED_REGION_BYTES;
        let (pk, want) = (r[REG_PK].len(), n * self.pks.stride());
        if pk != want {
            return Err(format!("pk region length {pk} != expected {want}"));
        }
        let (nulls, want) = (r[REG_NULL_BMP].len(), n * FIXED_REGION_BYTES);
        if nulls != want {
            return Err(format!("nulls length {nulls} != expected {want}"));
        }
        for (pi, (col, region)) in self.payload.iter().zip(&r[REG_PAYLOAD_START..]).enumerate() {
            let (got, want) = (region.len(), n * col.stride());
            if got != want {
                return Err(format!("payload slot {pi}: length {got} != expected {want}"));
            }
        }
        Ok(())
    }

    /// The batch's layout is `schema`'s: key column types, payload slot count,
    /// and each slot's type.
    pub(crate) fn layout_matches(&self, schema: &Schema) -> Result<(), String> {
        let want = schema.layout().pk_columns().map(|c| c.type_code);
        let got = self.pks.key_types();
        if !got.iter().copied().eq(want.clone()) {
            let want: Vec<TypeCode> = want.collect();
            return Err(format!("mismatched key column types: expected {want:?}, got {got:?}"));
        }
        if self.payload.len() != schema.layout().num_payload_cols() {
            return Err(format!(
                "payload slot count {} != schema payload column count {}",
                self.payload.len(),
                schema.layout().num_payload_cols()
            ));
        }
        for ((pi, _, def), col) in schema.payload_columns().zip(&self.payload) {
            if col.tc() != def.ty.tc {
                return Err(format!(
                    "payload slot {pi} ('{}'): type {:?} != schema type {:?}",
                    def.name,
                    col.tc(),
                    def.ty.tc
                ));
            }
        }
        Ok(())
    }

    /// Validate that all vectors are consistently sized for the given schema, and
    /// that every NULL is a zeroed cell of a nullable column.
    pub fn validate(&self, schema: &Schema) -> Result<(), String> {
        self.layout_matches(schema)?;
        self.check_region_lengths(&self.wire_regions())?;
        check_not_null(as_le_bytes(&self.nulls), schema)?;
        let col = |pi: usize| (self.payload[pi].bytes.as_slice(), self.payload[pi].stride());
        match gnitz_wire::first_valued_null(schema.layout().nullable_payload_slots(), as_le_bytes(&self.nulls), col) {
            None => Ok(()),
            Some((row, slot)) => Err(format!(
                "row {row} holds a value under NULL in column '{}'",
                schema.columns()[schema.layout().payload_col_idx(slot)].name
            )),
        }
    }

    /// The rows `rows` names, in order, each at most once. A gather naming every row in place is
    /// the batch itself. When every row survives the arena moves whole, else only survivors'
    /// strings are copied.
    pub fn gather(self, rows: &[u32]) -> ZSetBatch {
        if rows.len() == self.len() && rows.iter().enumerate().all(|(i, &r)| r as usize == i) {
            return self;
        }
        debug_assert!(
            {
                let mut seen = vec![false; self.len()];
                rows.iter().all(|&r| !std::mem::replace(&mut seen[r as usize], true))
            },
            "gather: a row named twice"
        );
        let whole = rows.len() == self.len();
        let mut blob = Vec::new();
        let pks = PkColumn {
            buf: gather_cells(&self.pks.buf, self.pks.stride(), rows),
            ..self.pks
        };
        let payload = self
            .payload
            .iter()
            .map(|src| {
                let bytes = if !whole && src.tc.is_german_string() {
                    let cells = src.bytes.as_chunks::<16>().0;
                    let mut bytes = Vec::with_capacity(rows.len() * 16);
                    for &r in rows {
                        bytes.extend_from_slice(&gnitz_wire::relocate_german_string(
                            &cells[r as usize],
                            &self.blob,
                            &mut blob,
                        ));
                    }
                    bytes
                } else {
                    gather_cells(&src.bytes, src.stride(), rows)
                };
                PayloadColumn { tc: src.tc, bytes }
            })
            .collect();
        ZSetBatch {
            pks,
            weights: rows.iter().map(|&r| self.weights[r as usize]).collect(),
            nulls: rows.iter().map(|&r| self.nulls[r as usize]).collect(),
            payload,
            blob: if whole { self.blob } else { blob },
        }
    }
}

/// The `stride`-wide cells of `src` that `rows` names, in order.
fn gather_cells(src: &[u8], stride: usize, rows: &[u32]) -> Vec<u8> {
    fn fixed<const N: usize>(src: &[u8], rows: &[u32]) -> Vec<u8> {
        let cells = src.as_chunks::<N>().0;
        let mut out = vec![0u8; rows.len() * N];
        for (dst, &r) in out.as_chunks_mut::<N>().0.iter_mut().zip(rows) {
            *dst = cells[r as usize];
        }
        out
    }
    match stride {
        1 => fixed::<1>(src, rows),
        2 => fixed::<2>(src, rows),
        4 => fixed::<4>(src, rows),
        8 => fixed::<8>(src, rows),
        16 => fixed::<16>(src, rows),
        // A compound key's stride.
        _ => {
            let mut out = Vec::with_capacity(rows.len() * stride);
            for &r in rows {
                out.extend_from_slice(&src[r as usize * stride..][..stride]);
            }
            out
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
    blob: usize,
}

/// Appends rows to a [`ZSetBatch`] one payload slot at a time. A fixed-width value is
/// checked against its slot's width and refused by a STRING/BLOB slot, `int_val` also
/// against the integer type's range, and `str_val`/`bytes_val` against the exact type.
///
/// Columns are appended in payload-slot order, so callers supply only payload
/// values; the cursor is the payload slot, and equals the payload count while no row
/// is open. In debug builds a row given fewer values than payload columns is caught
/// when the next row starts or the row is closed.
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
        self.debug_check_row_full();
    }
}

impl<'a> BatchAppender<'a> {
    pub fn new(batch: &'a mut ZSetBatch) -> Self {
        let cursor = batch.payload.len();
        BatchAppender { batch, cursor }
    }

    /// A row takes exactly one push per payload column; names the row `validate` would reject
    /// later as a bare region-length error.
    fn debug_check_row_full(&self) {
        debug_assert_eq!(
            self.cursor,
            self.batch.payload.len(),
            "BatchAppender: row got {} of {} payload columns",
            self.cursor,
            self.batch.payload.len(),
        );
    }

    /// Start a new row with the given single-column primary key and weight.
    pub fn add_row(&mut self, pk: u128, weight: i64) -> &mut Self {
        self.add_row_natives(&[pk], weight)
    }

    /// [`Self::add_row`] for a **compound** PK: `natives` are the PK columns'
    /// native values in PK-list order.
    pub fn add_row_natives(&mut self, natives: &[u128], weight: i64) -> &mut Self {
        self.debug_check_row_full();
        self.batch.pks.push_natives(natives);
        self.batch.weights.push(weight);
        self.batch.nulls.push(0);
        self.cursor = 0;
        self
    }

    /// Append one fixed-width cell, exactly the column's stride wide, to the next column.
    fn fixed_val(&mut self, bytes: &[u8]) -> &mut Self {
        let pi = self.col_index();
        let col = &mut self.batch.payload[pi];
        let tc = col.tc();
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
        col.bytes.extend_from_slice(bytes);
        self.cursor += 1;
        self
    }

    /// Append an integer to the next ≤8-byte integer column, at that column's
    /// own width and signedness; `v` must lie in the column type's range.
    pub fn int_val(&mut self, v: impl Into<i128>) -> &mut Self {
        let v = v.into();
        let tc = self.batch.payload[self.col_index()].tc();
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
    /// arena.
    fn german_val(&mut self, b: &[u8], want: TypeCode) -> &mut Self {
        let pi = self.col_index();
        let tc = self.batch.payload[pi].tc();
        assert!(
            tc == want,
            "BatchAppender: a {want:?} value cannot be written to the {tc:?} column at payload slot {pi}",
        );
        let cell = gnitz_wire::encode_german_string(b, &mut self.batch.blob);
        self.batch.payload[pi].bytes.extend_from_slice(&cell);
        self.cursor += 1;
        self
    }

    /// Append a SQL NULL to the next column, whatever its type: a zeroed cell under its bitmap bit.
    pub fn null(&mut self) -> &mut Self {
        let pi = self.col_index();
        self.batch.payload[pi].push_zero();
        // `col_index` admits a value only while a row is open, so a null word exists.
        let word = self.batch.nulls.last_mut().unwrap();
        gnitz_wire::null_word_set(word, pi, true);
        self.cursor += 1;
        self
    }

    /// The payload slot the next value goes to.
    fn col_index(&self) -> usize {
        // Hard assert (not debug-only): a misbehaving caller gets a clear panic
        // here instead of an OOB index panicking at the `payload[pi]` call site.
        assert!(
            self.cursor < self.batch.payload.len(),
            "BatchAppender: no payload slot open (cursor {} of {} columns): a value before add_row or past the row's last column",
            self.cursor,
            self.batch.payload.len(),
        );
        self.cursor
    }
}

/// Every method is `#[inline(always)]`; see [`gnitz_wire::RowSource`] for why
/// the plain hint is not enough. The bodies slice `&[u8]`, not `Vec<u8>`, whose
/// indexing stays an out-of-line call at opt-level 0.
impl gnitz_wire::RowSource for ZSetBatch {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        self.pks.get_bytes(row)
    }

    #[inline(always)]
    fn get_null_word(&self, row: usize) -> u64 {
        self.nulls[row]
    }

    #[inline(always)]
    fn get_col_ptr(&self, row: usize, pi: usize, sz: usize) -> &[u8] {
        let col: &[u8] = &self.payload[pi].bytes;
        &col[row * sz..row * sz + sz]
    }

    #[inline(always)]
    fn heap(&self) -> gnitz_wire::Heap<'_> {
        gnitz_wire::Heap::Plain(&self.blob)
    }

    #[inline(always)]
    fn row_count(&self) -> usize {
        self.weights.len()
    }
}

impl gnitz_expr::BatchView for ZSetBatch {
    #[inline(always)]
    fn col_data(&self, pi: usize, col_size: usize) -> &[u8] {
        let col: &[u8] = &self.payload[pi].bytes;
        debug_assert_eq!(
            col.len(),
            self.weights.len() * col_size,
            "col_data({pi}, {col_size}) width mismatch"
        );
        col
    }

    #[inline(always)]
    fn null_bmp(&self) -> &[u8] {
        as_le_bytes(&self.nulls)
    }

    #[inline(always)]
    fn pk_region(&self) -> (&[u8], usize) {
        (&self.pks.buf, self.pks.stride as usize)
    }
}

impl gnitz_expr::MapTarget for ZSetBatch {
    #[inline(always)]
    fn null_bmp_mut(&mut self) -> &mut [u8] {
        gnitz_wire::as_le_bytes_mut(&mut self.nulls)
    }

    #[inline(always)]
    fn slot_mut(&mut self, pi: usize) -> (&mut [u8], &mut [u8], &mut Vec<u8>) {
        (
            &mut self.payload[pi].bytes,
            gnitz_wire::as_le_bytes_mut(&mut self.nulls),
            &mut self.blob,
        )
    }
}

// ── WAL-block encode and decode ────────────────────────────────────────────

impl ZSetBatch {
    /// This batch as its canonical region list.
    pub(crate) fn wire_regions(&self) -> Regions<'_> {
        let mut out = Regions::new();
        out.push(self.pks.region());
        out.push(as_le_bytes(&self.weights));
        out.push(as_le_bytes(&self.nulls));
        for c in &self.payload {
            out.push(&c.bytes);
        }
        out.push(&self.blob);
        out
    }

    /// A batch of `schema` holding the rows of a canonical region list this process's own
    /// store produced. Its German cells are not checked, the store having checked them when
    /// it ingested them; its region lengths and NOT NULL are checked.
    pub fn from_own_regions(schema: &Schema, regions: &[&[u8]]) -> Result<Self, ProtocolError> {
        let mut b = Self::new(schema);
        b.check_region_lengths(regions).map_err(ProtocolError::DecodeError)?;
        check_not_null(regions[REG_NULL_BMP], schema).map_err(ProtocolError::DecodeError)?;
        b.append_region_list(regions);
        Ok(b)
    }

    /// Append rows given as `r`, a canonical region list in this batch's layout, verbatim:
    /// each region concatenates and the blob lands on this arena's tail, so every German
    /// cell has its heap offset shifted by the arena's old length.
    fn append_region_list(&mut self, r: &[&[u8]]) {
        let p = self.payload.len();
        self.pks.buf.extend_from_slice(r[REG_PK]);
        gnitz_wire::extend_from_le_bytes(&mut self.weights, r[REG_WEIGHT]);
        gnitz_wire::extend_from_le_bytes(&mut self.nulls, r[REG_NULL_BMP]);
        let base = self.blob.len();
        self.blob.extend_from_slice(r[REG_PAYLOAD_START + p]);
        for (col, region) in self.payload.iter_mut().zip(&r[REG_PAYLOAD_START..][..p]) {
            let at = col.bytes.len();
            col.bytes.extend_from_slice(region);
            if col.tc.is_german_string() {
                gnitz_wire::shift_german_string_heaps(&mut col.bytes[at..], base);
            }
        }
    }
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

/// Decode a WAL block under `schema`, appending its rows to `sink`. On error
/// `sink` is as it was.
fn decode_wal_block_into(sink: &mut ZSetBatch, data: &[u8], schema: &Schema) -> Result<(), ProtocolError> {
    let layout = schema.layout();
    let gnitz_wire::wal::ParsedBlock { rows, fixed, heap, .. } = parse_wal_block(data, schema)?;
    let mut regions = Regions::new();
    for r in 0..layout.num_fixed_regions() {
        regions.push(&fixed[layout.region_start(r, rows)..][..rows * layout.region_stride(r)]);
    }
    regions.push(heap);
    debug_assert!(sink.layout_matches(schema).is_ok() && sink.check_region_lengths(&sink.wire_regions()).is_ok());
    // The block's size was checked against its rows, so each region is as long as they are.
    check_not_null(regions[REG_NULL_BMP], schema).map_err(ProtocolError::DecodeError)?;
    // The block crossed a trust boundary: each German cell must be canonical against the
    // block's own heap.
    let bad = sink
        .payload
        .iter()
        .zip(&regions[REG_PAYLOAD_START..])
        .position(|(c, r)| c.tc.is_german_string() && !gnitz_wire::german_string_region_ok(r, heap));
    if let Some(pi) = bad {
        return Err(ProtocolError::DecodeError(format!(
            "column {}: German string cell is not in canonical form",
            layout.payload_col_idx(pi)
        )));
    }
    sink.append_region_list(&regions);
    Ok(())
}

/// Decode the WAL blocks `blocks` under `schema` into one batch, in iteration
/// order. Every block's header is parsed and the batch sized for all of them
/// before any rows are copied, so a reply split over several blocks grows each
/// region once.
pub(crate) fn decode_wal_blocks<'a>(
    mut blocks: impl Iterator<Item = &'a [u8]> + Clone,
    schema: &Schema,
) -> Result<ZSetBatch, ProtocolError> {
    let (mut rows, mut heap) = (0usize, 0usize);
    for block in blocks.clone() {
        let parsed = parse_wal_block(block, schema)?;
        rows += parsed.rows;
        heap += parsed.heap.len();
    }
    let mut batch = ZSetBatch::with_capacity(schema, rows);
    batch.blob.reserve(heap);
    blocks.try_for_each(|block| decode_wal_block_into(&mut batch, block, schema))?;
    Ok(batch)
}

#[cfg(test)]
#[path = "tests/batch.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/batch.rs"]
mod bench;
