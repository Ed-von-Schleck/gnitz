use gnitz_expr::{ColumnTable, SchemaFacts};
use std::sync::{Arc, OnceLock};

use gnitz_wire::{ColumnDef, PkKeys, TypeCode};
use gnitz_wire::{MAX_COLUMNS, MAX_PK_COLUMNS};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
    /// PK column indices in compound-key order; length >= 1.
    pub pk_cols: Vec<u32>,
}

impl Schema {
    /// The PK columns in PK-list order, hidden: the leading key of a reply that
    /// carries this schema's key.
    pub fn hidden_key_columns(&self) -> impl Iterator<Item = ColumnDef> + '_ {
        self.pk_cols.iter().map(|&c| self.columns[c as usize].clone().hidden())
    }

    /// The non-PK ("payload") columns as `(payload slot, col_idx, &ColumnDef)`,
    /// in slot order.
    #[inline]
    pub fn payload_columns(&self) -> impl Iterator<Item = (usize, usize, &ColumnDef)> {
        self.columns
            .iter()
            .enumerate()
            .filter_map(|(ci, c)| Some((self.payload_slot(ci)?, ci, c)))
    }

    /// Iterate over the *visible* (non-hidden) columns, yielding
    /// `(physical_col_idx, &ColumnDef)`. The index is the column's real position
    /// in the full physical schema — hidden slots are skipped but do not shift
    /// the indices of the visible ones, so every wildcard/presentation surface
    /// that enumerates through this stays byte-offset-correct. A base-table schema
    /// has a hidden column only after `ALTER … DROP COLUMN` (a logical drop); for
    /// a never-dropped table this is the full column list.
    #[inline]
    pub fn visible_columns(&self) -> impl Iterator<Item = (usize, &ColumnDef)> {
        self.columns.iter().enumerate().filter(|(_, c)| !c.is_hidden)
    }

    /// The single definition of "this is an admissible schema": the
    /// `MAX_COLUMNS` cap (the region null bitmap is one u64 word), the
    /// structural PK rules (non-empty, ≤ `PK_LIST_MAX_COLS`, every index
    /// `< columns.len()`, no duplicates), and the per-column invariants the
    /// engine's `SchemaDescriptor::new` hard-asserts — each PK column
    /// non-nullable and PK-eligible. Run by [`Schema::from_parts`] and the
    /// client's DDL gateways, so a malformed spec is a clean client error.
    ///
    /// The arity cap is the persisted PK-list codec capacity, not the wider
    /// in-memory `MAX_PK_COLUMNS`: a client never builds the engine-internal
    /// secondary-index schema that uses the extra slot, so a PK it accepts must
    /// round-trip through the codec.
    pub fn validate(&self) -> Result<(), String> {
        if self.columns.len() > MAX_COLUMNS {
            return Err(format!(
                "column count {} exceeds MAX_COLUMNS ({MAX_COLUMNS})",
                self.columns.len()
            ));
        }
        if let Some(cd) = self.columns.iter().find(|cd| !cd.ty.is_admissible()) {
            return Err(format!(
                "column '{}' ({:?}) carries scale {}",
                cd.name, cd.ty.tc, cd.ty.scale
            ));
        }
        gnitz_wire::validate_pk_tuple(&self.pk_cols, self.columns.len(), gnitz_wire::PK_LIST_MAX_COLS, |c| {
            let cd = &self.columns[c as usize];
            (cd.ty.tc, cd.is_nullable)
        })
        .map_err(|r| {
            r.named(gnitz_wire::PkListRole::PrimaryKey, |c| {
                self.columns.get(c as usize).map(|cd| cd.name.as_str())
            })
        })
    }

    /// Fallible constructor for a schema assembled from untrusted parts — a
    /// wire schema block or catalog rows. Runs [`Schema::validate`],
    /// so every decode boundary applies the same rule set.
    pub fn from_parts(columns: Vec<ColumnDef>, pk_cols: Vec<u32>) -> Result<Schema, String> {
        let s = Schema { columns, pk_cols };
        s.validate()?;
        Ok(s)
    }

    /// This schema's meta-schema record — the bytes a schema-bearing push embeds.
    pub fn to_block(&self) -> Vec<u8> {
        gnitz_wire::schema_block::encode(self.columns.iter().map(ColumnDef::block_col), &self.pk_cols)
    }

    /// The schema a meta-schema record describes.
    pub fn from_block(block: &[u8]) -> Result<Schema, String> {
        let mut columns = Vec::new();
        let pk = gnitz_wire::schema_block::decode(block, |c| {
            let name = std::str::from_utf8(c.name).map_err(|e| format!("utf8 in column name: {e}"))?;
            columns.push(ColumnDef {
                name: name.to_owned(),
                ty: c.ty,
                is_nullable: c.nullable,
                is_hidden: c.hidden,
            });
            Ok(())
        })?;
        Schema::from_parts(columns, pk.as_slice().to_vec())
    }
}

/// The client `Schema` of system table `tid`, built from `gnitz_wire::SYS_FAMILIES`.
///
/// Panics on an id that is not a system table.
pub fn sys_schema(tid: u64) -> &'static Arc<Schema> {
    static INSTANCE: OnceLock<Vec<Arc<Schema>>> = OnceLock::new();
    let schemas = INSTANCE.get_or_init(|| {
        gnitz_wire::SYS_FAMILIES
            .iter()
            .map(|f| {
                Arc::new(Schema {
                    columns: f
                        .cols
                        .iter()
                        .map(|c| ColumnDef::new(c.name, c.type_code, c.nullable))
                        .collect(),
                    pk_cols: f.pk_cols.to_vec(),
                })
            })
            .collect()
    });
    let idx = gnitz_wire::sys_family_index(tid).unwrap_or_else(|| panic!("not a system table id: {tid}"));
    &schemas[idx]
}

impl ColumnTable for Schema {
    fn pk_cols(&self) -> &[u32] {
        &self.pk_cols
    }

    fn num_columns(&self) -> usize {
        self.columns.len()
    }

    fn col_type_code(&self, ci: usize) -> TypeCode {
        self.columns[ci].ty.tc
    }

    fn col_nullable(&self, ci: usize) -> bool {
        self.columns[ci].is_nullable
    }
}

/// A key's columns as a schema of their own: every column a PK column, in order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct KeyLayout {
    /// `types[..n]` are the key columns' types in PK-list order; the rest is `U8`.
    types: [TypeCode; MAX_PK_COLUMNS],
    n: u8,
}

const _: () = assert!(MAX_PK_COLUMNS == 5, "KeyLayout::pk_cols lists every key position");

impl ColumnTable for KeyLayout {
    fn pk_cols(&self) -> &[u32] {
        &[0, 1, 2, 3, 4][..self.n as usize]
    }

    fn num_columns(&self) -> usize {
        self.n as usize
    }

    fn col_type_code(&self, ci: usize) -> TypeCode {
        self.types[ci]
    }

    fn col_nullable(&self, _: usize) -> bool {
        false
    }
}

/// A batch's PK region: `stride` bytes per row of **order-preserving key** (OPK,
/// §4) — the bytes the wire carries and the engine stores, so a client batch's
/// region and a `BatchBuilder`'s are byte-identical.
///
/// Private fields: routing hashes these bytes, so an un-encoded write would
/// mis-partition rather than error. Every append encodes or copies OPK.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PkColumn {
    layout: KeyLayout,
    /// `layout.pk_stride()`, held because `RowSource::get_pk_bytes` reads it per row.
    stride: u8,
    buf: Vec<u8>,
}

impl PkColumn {
    /// Empty `PkColumn` holding `schema`'s key column types — the only
    /// constructor, so the stride is never independent data to keep in sync,
    /// and never zero or wider than a key.
    pub fn empty_for_schema(schema: &Schema) -> Self {
        let n = schema.pk_cols.len();
        assert!(
            (1..=MAX_PK_COLUMNS).contains(&n),
            "PkColumn: {n} key columns is outside 1..={MAX_PK_COLUMNS}"
        );
        let mut types = [TypeCode::U8; MAX_PK_COLUMNS];
        for (t, &c) in types.iter_mut().zip(&schema.pk_cols) {
            *t = schema.columns[c as usize].ty.tc;
        }
        let layout = KeyLayout { types, n: n as u8 };
        PkColumn {
            layout,
            stride: layout.pk_stride() as u8,
            buf: vec![],
        }
    }

    /// A column of `schema`'s single-column keys from their native values. The
    /// counterpart of [`Self::get`].
    pub fn from_natives(schema: &Schema, vals: impl IntoIterator<Item = u128>) -> Self {
        let mut c = Self::empty_for_schema(schema);
        for v in vals {
            c.push_u128(v);
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
    pub fn key_types(&self) -> &[TypeCode] {
        &self.layout.types[..self.layout.n as usize]
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
        assert_eq!(self.layout.n, 1, "PkColumn::get: a compound key has no scalar form");
        let native = self.layout.native_le_key(self.get_bytes(i));
        u128::from_le_bytes(native[..gnitz_wire::NARROW_PK_MAX_BYTES].try_into().unwrap())
    }

    /// Borrow row `i`'s `stride` OPK bytes.
    pub fn get_bytes(&self, i: usize) -> &[u8] {
        let s = self.stride();
        &self.buf[i * s..(i + 1) * s]
    }

    /// Room for `n` more rows.
    pub fn reserve(&mut self, n: usize) {
        self.buf.reserve(n * self.stride());
    }

    /// Append a single-column key from its native value.
    pub fn push_u128(&mut self, pk: u128) {
        assert_eq!(self.layout.n, 1, "push_u128: a compound key takes push_natives");
        let s = self.stride();
        self.push_region_bytes(self.layout.opk_key(&pk.to_le_bytes()[..s]).pk_bytes());
    }

    /// Append one row: `write(k, buf)` appends the native little-endian bytes
    /// of the `k`-th PK column. An error appends nothing.
    pub fn push_row<E>(&mut self, mut write: impl FnMut(usize, &mut Vec<u8>) -> Result<(), E>) -> Result<(), E> {
        let row = self.buf.len();
        let layout = self.layout;
        for (k, &tc) in layout.types[..layout.n as usize].iter().enumerate() {
            let at = self.buf.len();
            if let Err(e) = write(k, &mut self.buf) {
                self.buf.truncate(row);
                return Err(e);
            }
            let w = self.buf.len() - at;
            assert_eq!(w, tc.wire_stride(), "push_row: PK column {k} is a {tc:?}");
            let mut native = [0u8; 16];
            native[..w].copy_from_slice(&self.buf[at..]);
            gnitz_wire::encode_pk_column(&native[..w], tc, &mut self.buf[at..]);
        }
        debug_assert_eq!(self.buf.len() - row, self.stride());
        Ok(())
    }

    /// Append one row from the PK columns' native values in PK-list order.
    pub fn push_natives(&mut self, natives: &[u128]) {
        // Hard, not debug-only: a short list would encode a short row and shift
        // every later one.
        assert_eq!(
            natives.len(),
            self.layout.n as usize,
            "push_natives: one native value per key column"
        );
        self.push_region_bytes(self.layout.opk_key_cols(natives).pk_bytes());
    }

    /// Append whole OPK rows verbatim — `opk` is a multiple of `stride` bytes
    /// already in region form.
    pub(crate) fn push_region_bytes(&mut self, opk: &[u8]) {
        debug_assert!(
            opk.len().is_multiple_of(self.stride()),
            "push_region_bytes: {} bytes is not a whole number of {}-byte rows",
            opk.len(),
            self.stride(),
        );
        self.buf.extend_from_slice(opk);
    }

    /// Move every row of `other` onto this column's tail.
    pub(crate) fn append(&mut self, other: &mut PkColumn) {
        debug_assert_eq!(self.layout, other.layout);
        self.buf.append(&mut other.buf);
    }

    fn truncate(&mut self, len: usize) {
        self.buf.truncate(len * self.stride());
    }

    /// Append the row at `src[i]` to `self`. Layouts must match.
    fn push_from(&mut self, src: &PkColumn, i: usize) {
        debug_assert_eq!(self.layout, src.layout);
        self.buf.extend_from_slice(src.get_bytes(i));
    }
}

/// Append one zero-filled cell of wire type `tc` to a payload region — the NULL
/// encoding and the non-null filler alike. The null bitmap is the NULL truth,
/// and a zeroed German cell *is* the empty value, which is what
/// `encode_german_string(&[], _)` writes.
pub fn push_zero_cell(col: &mut Vec<u8>, tc: TypeCode) {
    col.extend(std::iter::repeat_n(0u8, tc.wire_stride()));
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
    pub fn new(tc: TypeCode) -> Self {
        PayloadColumn { tc, bytes: Vec::new() }
    }

    /// `rows` zero cells.
    pub fn zeroed(tc: TypeCode, rows: usize) -> Self {
        PayloadColumn {
            tc,
            bytes: vec![0; rows * tc.wire_stride()],
        }
    }

    #[inline]
    pub fn tc(&self) -> TypeCode {
        self.tc
    }

    #[inline]
    pub fn stride(&self) -> usize {
        self.tc.wire_stride()
    }

    /// Append one zero cell: a NULL's filler, or a DROP COLUMN tombstone's value.
    pub fn push_zero(&mut self) {
        push_zero_cell(&mut self.bytes, self.tc)
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
            payload: schema
                .payload_columns()
                .map(|(_, _, c)| PayloadColumn::new(c.ty.tc))
                .collect(),
            blob: vec![],
        }
    }

    /// One region per payload slot, zero-filled for `count` rows.
    pub fn filler_columns(schema: &Schema, count: usize) -> Vec<PayloadColumn> {
        schema
            .payload_columns()
            .map(|(_, _, c)| PayloadColumn::zeroed(c.ty.tc, count))
            .collect()
    }

    /// Append the cell at row `i` of `src.payload[src_pi]` onto
    /// `self.payload[dst_pi]`. A German cell is re-encoded against this batch's
    /// arena — its heap offset is relative to `src`'s and means nothing here.
    pub fn push_cell_from(&mut self, dst_pi: usize, src: &ZSetBatch, src_pi: usize, i: usize) {
        let col = &src.payload[src_pi];
        let tc = col.tc();
        debug_assert_eq!(self.payload[dst_pi].tc(), tc, "push_cell_from: slot type mismatch");
        let w = tc.wire_stride();
        let cell = &col.bytes[i * w..(i + 1) * w];
        if tc.is_german_string() {
            let content = gnitz_wire::german_string_content(cell, &src.blob);
            let moved = gnitz_wire::encode_german_string(content, &mut self.blob);
            self.payload[dst_pi].bytes.extend_from_slice(&moved);
        } else {
            self.payload[dst_pi].bytes.extend_from_slice(cell);
        }
    }

    /// Append row `i` of `src` to `self` at `weight`, verbatim — so a `-1`
    /// reproduces the stored row rather than relying on a decoder and an encoder
    /// agreeing. `src` and `self` must share a layout.
    pub fn copy_row_at(&mut self, src: &ZSetBatch, i: usize, weight: i64) {
        debug_assert!(self.same_layout(src), "copy_row_at: layout mismatch");
        self.pks.push_from(&src.pks, i);
        self.weights.push(weight);
        self.nulls.push(src.nulls[i]);
        for pi in 0..self.payload.len() {
            self.push_cell_from(pi, src, pi, i);
        }
    }

    /// Same key column types and the same payload type list.
    fn same_layout(&self, other: &ZSetBatch) -> bool {
        self.pks.layout == other.pks.layout
            && self.payload.len() == other.payload.len()
            && self.payload.iter().zip(&other.payload).all(|(a, b)| a.tc() == b.tc())
    }

    /// Overwrite the STRING cell at `(row, pi)`. Addressable rather than "patch
    /// the row I just pushed", so a caller that copied two rows can name which
    /// one it means. The replaced cell's spill, if any, stays in the arena
    /// unreferenced.
    pub fn set_string_cell(&mut self, row: usize, pi: usize, v: &str) {
        let cell = gnitz_wire::encode_german_string(v.as_bytes(), &mut self.blob);
        self.payload[pi].bytes[row * 16..(row + 1) * 16].copy_from_slice(&cell);
    }

    /// Overwrite the 8-byte fixed-width cell at `(row, pi)` with `v`, little-endian.
    pub fn set_u64_cell(&mut self, row: usize, pi: usize, v: u64) {
        gnitz_wire::write_u64_le(&mut self.payload[pi].bytes, row * 8, v);
    }

    /// An empty batch with every growth stream sized for `n` rows: the PK
    /// buffer, the weights, the null words and each payload column. The form to
    /// use whenever the row count is known before the build loop — otherwise a
    /// column-at-a-time fill reallocates its way up from zero.
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
    pub fn live_rows(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).filter(move |&i| self.weights[i] > 0)
    }

    /// Append all rows of `other`, consuming it: each region concatenates, and
    /// `other`'s arena lands on this one's tail, so every German cell it carries
    /// has its heap offset shifted by that much.
    pub fn extend_from_owned(&mut self, mut other: ZSetBatch) {
        assert!(self.same_layout(&other), "extend_from_owned: layout mismatch");
        if self.is_empty() {
            *self = other;
            return;
        }
        self.pks.append(&mut other.pks);
        self.weights.append(&mut other.weights);
        self.nulls.append(&mut other.nulls);
        let delta = self.blob.len();
        self.blob.append(&mut other.blob);
        for (dst, src) in self.payload.iter_mut().zip(&mut other.payload) {
            let at = dst.bytes.len();
            dst.bytes.append(&mut src.bytes);
            if dst.tc().is_german_string() {
                gnitz_wire::shift_german_string_heaps(&mut dst.bytes[at..], delta);
            }
        }
    }

    /// The batch's current extent, for [`Self::rollback_to`].
    pub fn mark(&self) -> BatchMark {
        BatchMark {
            rows: self.weights.len(),
            blob: self.blob.len(),
        }
    }

    /// Drop everything appended since `mark` — used to undo a half-written row,
    /// its blob spill included.
    pub fn rollback_to(&mut self, mark: BatchMark) {
        self.pks.truncate(mark.rows);
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
        self.rollback_to(BatchMark { rows: dst, blob: self.blob.len() });
    }

    /// Every payload region is the length its type and the row count imply.
    pub(crate) fn check_columns(&self) -> Result<(), std::string::String> {
        let n = self.len();
        let regions = gnitz_wire::num_regions(self.payload.len());
        if regions > gnitz_wire::MAX_WIRE_REGIONS {
            return Err(format!(
                "{regions} regions exceeds the {} a region list holds",
                gnitz_wire::MAX_WIRE_REGIONS
            ));
        }
        for (pi, col) in self.payload.iter().enumerate() {
            let (got, want) = (col.bytes.len(), n * col.stride());
            if got != want {
                return Err(format!("payload slot {pi}: length {got} != expected {want}"));
            }
        }
        Ok(())
    }

    /// The batch's layout is `schema`'s: key column types, payload slot count,
    /// and each slot's type.
    pub fn layout_matches(&self, schema: &Schema) -> Result<(), std::string::String> {
        let want = schema.pk_cols.iter().map(|&c| schema.columns[c as usize].ty.tc);
        let got = self.pks.key_types();
        if !got.iter().copied().eq(want.clone()) {
            let want: Vec<TypeCode> = want.collect();
            return Err(format!("mismatched key column types: expected {want:?}, got {got:?}"));
        }
        if self.payload.len() != schema.num_payload_cols() {
            return Err(format!(
                "payload slot count {} != schema payload column count {}",
                self.payload.len(),
                schema.num_payload_cols()
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
    pub fn validate(&self, schema: &Schema) -> Result<(), std::string::String> {
        self.layout_matches(schema)?;
        let n = self.pks.len();
        if self.weights.len() != n {
            return Err(format!("weights length {} != row count {}", self.weights.len(), n));
        }
        if self.nulls.len() != n {
            return Err(format!("nulls length {} != row count {}", self.nulls.len(), n));
        }
        self.check_columns()?;
        check_not_null(&self.nulls, schema)?;
        let col = |pi: usize| (self.payload[pi].bytes.as_slice(), self.payload[pi].stride());
        match gnitz_wire::first_valued_null(
            schema.nullable_payload_slots(),
            gnitz_wire::as_le_bytes(&self.nulls),
            col,
        ) {
            None => Ok(()),
            Some((row, slot)) => Err(format!(
                "row {row} holds a value under NULL in column '{}'",
                schema.columns[schema.payload_col_idx(slot)].name
            )),
        }
    }

    /// The rows `rows` names, in order, at their paired weights; each row at most once.
    /// A gather naming every row in place is the batch itself. When every row survives the
    /// arena moves whole, else only survivors' strings are copied.
    pub fn gather(self, rows: &[(usize, i64)]) -> ZSetBatch {
        if rows.len() == self.len()
            && rows
                .iter()
                .enumerate()
                .all(|(i, &(r, w))| r == i && w == self.weights[i])
        {
            return self;
        }
        debug_assert!(
            {
                let mut seen = vec![false; self.len()];
                rows.iter().all(|&(r, _)| !std::mem::replace(&mut seen[r], true))
            },
            "gather: a row named twice"
        );
        let n = rows.len();
        let whole = n == self.len();
        let mut blob = Vec::new();
        let mut pks = PkColumn {
            layout: self.pks.layout,
            stride: self.pks.stride,
            buf: Vec::with_capacity(n * self.pks.stride()),
        };
        for &(r, _) in rows {
            pks.push_from(&self.pks, r);
        }
        let payload = self
            .payload
            .iter()
            .map(|src| {
                let s = src.stride();
                let mut bytes = Vec::with_capacity(n * s);
                if !whole && src.tc.is_german_string() {
                    for &(r, _) in rows {
                        let content = gnitz_wire::german_string_content(&src.bytes[r * s..(r + 1) * s], &self.blob);
                        bytes.extend_from_slice(&gnitz_wire::encode_german_string(content, &mut blob));
                    }
                } else {
                    for &(r, _) in rows {
                        bytes.extend_from_slice(&src.bytes[r * s..(r + 1) * s]);
                    }
                }
                PayloadColumn { tc: src.tc, bytes }
            })
            .collect();
        ZSetBatch {
            pks,
            weights: rows.iter().map(|&(_, w)| w).collect(),
            nulls: rows.iter().map(|&(r, _)| self.nulls[r]).collect(),
            payload,
            blob: if whole { self.blob } else { blob },
        }
    }
}

/// No row sets a null bit on a NOT NULL payload column of `schema`, whose declaration
/// every reader past the decoder trusts.
pub(crate) fn check_not_null(nulls: &[u64], schema: &Schema) -> Result<(), String> {
    match gnitz_wire::first_not_null_violation(schema.not_null_payload_slots(), gnitz_wire::as_le_bytes(nulls)) {
        None => Ok(()),
        Some((row, slot)) => Err(format!(
            "row {row} sets a null bit on NOT NULL column '{}'",
            schema.columns[schema.payload_col_idx(slot)].name
        )),
    }
}

/// A [`ZSetBatch`]'s extent at one moment, taken by [`ZSetBatch::mark`].
#[derive(Clone, Copy)]
pub struct BatchMark {
    rows: usize,
    blob: usize,
}

/// Builder for appending rows to a `ZSetBatch` with schema-aware column mapping.
///
/// Columns are appended in payload-slot order, so callers supply only payload
/// values; the cursor is the payload slot.
pub struct BatchAppender<'a> {
    batch: &'a mut ZSetBatch,
    cursor: usize,
}

/// The client half of the shared catalog row codecs: the sink
/// `gnitz_wire::sys_rows` writes a system-table row into.
impl gnitz_wire::sys_rows::SysRowSink for BatchAppender<'_> {
    fn begin_row(&mut self, pk: &[u128], weight: i64) {
        self.add_row_natives(pk, weight);
    }
    fn put_u64(&mut self, v: u64) {
        self.u64_val(v);
    }
    fn put_string(&mut self, s: &str) {
        self.str_val(s);
    }
    fn put_bytes(&mut self, b: &[u8]) {
        self.bytes_val(b);
    }
    fn put_null(&mut self) {
        self.null();
    }
    fn end_row(&mut self) {
        self.check_row_complete();
    }
}

impl<'a> BatchAppender<'a> {
    pub fn new(batch: &'a mut ZSetBatch) -> Self {
        BatchAppender { batch, cursor: 0 }
    }

    /// Start a new row with the given single-column primary key and weight.
    pub fn add_row(&mut self, pk: u128, weight: i64) -> &mut Self {
        self.open_row(weight);
        self.batch.pks.push_u128(pk);
        self
    }

    /// [`Self::add_row`] for a **compound** PK: `natives` are the PK columns'
    /// native values in PK-list order.
    pub fn add_row_natives(&mut self, natives: &[u128], weight: i64) -> &mut Self {
        self.open_row(weight);
        self.batch.pks.push_natives(natives);
        self
    }

    /// A row takes exactly one push per payload column; names the row `validate` would reject
    /// later as a bare region-length error.
    fn check_row_complete(&self) {
        debug_assert_eq!(
            self.cursor,
            self.batch.payload.len(),
            "BatchAppender: row got {} of {} payload columns",
            self.cursor,
            self.batch.payload.len(),
        );
    }

    /// The per-row bookkeeping both row starters owe, minus the key itself.
    fn open_row(&mut self, weight: i64) {
        self.batch.weights.push(weight);
        self.batch.nulls.push(0);
        self.cursor = 0;
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
    pub fn int_val(&mut self, v: i128) -> &mut Self {
        let tc = self.batch.payload[self.col_index()].tc();
        let fi = gnitz_wire::FixedInt::from_type_code(tc)
            .unwrap_or_else(|| panic!("BatchAppender: {tc:?} is not an integer column"));
        let (lo, hi) = fi.range();
        assert!((lo..=hi).contains(&v), "BatchAppender: {v} is out of range for {tc:?}");
        self.fixed_val(&fi.pack(v).to_le_bytes()[..fi.width()])
    }

    /// Append a u64 value to the next column.
    pub fn u64_val(&mut self, v: u64) -> &mut Self {
        self.fixed_val(&v.to_le_bytes())
    }

    /// Append an i64 value to the next column. Same eight bytes as
    /// [`Self::u64_val`].
    pub fn i64_val(&mut self, v: i64) -> &mut Self {
        self.fixed_val(&v.to_le_bytes())
    }

    /// Append an f64 value to the next column.
    pub fn f64_val(&mut self, v: f64) -> &mut Self {
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
        let word = self
            .batch
            .nulls
            .last_mut()
            .expect("BatchAppender: null called before add_row");
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
            "BatchAppender: payload cursor {} exceeds {} payload columns",
            self.cursor,
            self.batch.payload.len(),
        );
        self.cursor
    }
}

/// Every method is `#[inline(always)]`; see [`gnitz_expr::BatchView`] for why
/// the plain hint is not enough. The bodies slice `&[u8]`, not `Vec<u8>`, whose
/// indexing stays an out-of-line call at opt-level 0.
impl gnitz_expr::RowSource for ZSetBatch {
    #[inline(always)]
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        let (s, buf): (usize, &[u8]) = (self.pks.stride as usize, &self.pks.buf);
        &buf[row * s..row * s + s]
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
    fn blob(&self) -> &[u8] {
        &self.blob
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
        gnitz_wire::as_le_bytes(&self.nulls)
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

#[cfg(test)]
#[path = "tests/types.rs"]
mod tests;
