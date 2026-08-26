use super::error::ProtocolError;

pub use gnitz_wire::{FixedInt, ReduceOutKey, TypeCode};
pub use gnitz_wire::{MAX_COLUMNS, MAX_PK_BYTES, PK_LIST_MAX_COLS};

/// Convert a u64 wire value to TypeCode, returning an error for unknown codes.
/// Use at wire/network boundaries; internal data should use `TypeCode::from_validated_u8`.
pub fn type_code_from_u64(v: u64) -> Result<TypeCode, ProtocolError> {
    if v > u8::MAX as u64 {
        return Err(ProtocolError::UnknownTypeCode(v));
    }
    TypeCode::try_from_u8(v as u8).ok_or(ProtocolError::UnknownTypeCode(v))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: std::string::String,
    pub type_code: TypeCode,
    pub is_nullable: bool,
    pub fk_table_id: u64,
    pub fk_col_idx: u64,
    /// True for a Postgres-style SERIAL/BIGSERIAL/SMALLSERIAL primary key: an
    /// auto-assigned, client-stamped id the user may not supply. Round-trips
    /// through the wire meta-schema (`META_FLAG_SERIAL`) and `COL_TAB`, so a
    /// connection that only resolved the relation can still distinguish it from
    /// a user-supplied non-null integer PK. The engine stores the marker but has
    /// no SERIAL awareness.
    pub is_serial: bool,
    /// True for a hidden key slot — a physical schema column carrying a real
    /// PK/routing value (a synthetic view key like `_join_pk`/`_group_pk`, or an
    /// unprojected passthrough source PK) that no presentation surface exposes.
    /// Round-trips through the wire meta-schema (`META_FLAG_HIDDEN`) and
    /// `COL_TAB`. Presentation layers (wildcard expansion, name resolution,
    /// duplicate-name checks, client rows) skip it; physical layout, routing,
    /// sort, and consolidation are unaffected. A base-table column becomes hidden
    /// only via `ALTER TABLE … DROP COLUMN` (a logical drop): it stays physically
    /// present and zero-filled NOT NULL, and is excluded from every name-facing
    /// surface.
    pub is_hidden: bool,
}

impl ColumnDef {
    /// `fk_table_id` value meaning "this column references the table being
    /// created", whose id the planner cannot name yet. `append_col_row`
    /// rewrites it to the owner id, so no `COL_TAB` row carries it.
    ///
    /// `0` cannot serve, being the live "no FK" value. `u64::MAX` is
    /// unreachable as a table id, and an escaped marker fails closed: the
    /// engine reads the field as `i64`, so it arrives as `-1` and is rejected
    /// as an FK against an unknown table.
    pub const SELF_FK_TABLE_ID: u64 = u64::MAX;

    /// A non-FK, non-SERIAL column — the common case. Client-side schema builders
    /// (the SQL planner, the Python driver) synthesize columns through here; the
    /// planner's FK path assigns `fk_table_id`/`fk_col_idx` on the returned
    /// column once the referenced table resolves, and a SERIAL column chains
    /// [`ColumnDef::serial`].
    pub fn new(name: impl Into<String>, type_code: TypeCode, is_nullable: bool) -> Self {
        Self {
            name: name.into(),
            type_code,
            is_nullable,
            fk_table_id: 0,
            fk_col_idx: 0,
            is_serial: false,
            is_hidden: false,
        }
    }

    /// The column def of a *computed* projection item, from the expression's
    /// nominal type. One home for the three rules every computed column obeys,
    /// so the ad-hoc and CREATE VIEW binders (which each build their own
    /// projection schema) cannot drift:
    /// - `_expr{idx}` when the item has no alias;
    /// - always nullable — an expression over a NOT NULL column can still be
    ///   NULL (division by zero, an unmatched CASE);
    /// - typed by the register image the engine's `EMIT` stores whole, not the
    ///   nominal type: `-f32col` computes in f64, so declaring the column `F32`
    ///   would ship the low half of the double. STRING maps to itself, which
    ///   `register_image` already accounts for.
    pub fn computed(alias: Option<String>, idx: usize, nominal: TypeCode) -> Self {
        Self::new(
            alias.unwrap_or_else(|| format!("_expr{idx}")),
            nominal.register_image(),
            true,
        )
    }

    /// Mark this column a SERIAL primary key — an auto-assigned, client-stamped
    /// id. Chains onto [`ColumnDef::new`]; the CREATE TABLE planner is the only
    /// builder of SERIAL columns.
    pub fn serial(mut self) -> Self {
        self.is_serial = true;
        self
    }

    /// Mark this column a hidden key slot (see [`ColumnDef::is_hidden`]). Chains
    /// onto [`ColumnDef::new`]; the view emitters that fabricate synthetic keys
    /// and `place_pk_front`'s auto-prepend arm are the only builders.
    pub fn hidden(mut self) -> Self {
        self.is_hidden = true;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
    /// PK column indices in compound-key order; length >= 1.
    pub pk_cols: Vec<usize>,
}

impl Schema {
    /// Number of logical columns in this schema (PK + payload).
    #[inline]
    pub fn num_columns(&self) -> usize {
        self.columns.len()
    }

    /// All PK column indices, in compound-key order.
    #[inline]
    pub fn pk_indices(&self) -> &[usize] {
        &self.pk_cols
    }

    /// Number of PK columns (compound-key arity). Compound primary keys are
    /// supported end to end — `CREATE TABLE … PRIMARY KEY (a, b, …)` produces a
    /// schema with `count >= 2` — so callers must not assume a lone PK column.
    #[inline]
    pub fn pk_count(&self) -> usize {
        self.pk_cols.len()
    }

    /// On-wire PK region stride: sum of each PK column's `wire_stride()`,
    /// tightly packed (no inter-column padding), mirroring the engine
    /// `SchemaDescriptor` layout. For a single-PK schema this equals the lone
    /// PK column's `wire_stride()`, so the PK region is byte-for-byte
    /// unchanged.
    #[inline]
    pub fn pk_stride(&self) -> usize {
        self.pk_cols
            .iter()
            .map(|&ci| self.columns[ci].type_code.wire_stride())
            .sum()
    }

    /// The single PK column index. Use only where a compound PK has already
    /// been ruled out by the caller — the remaining production callers are in
    /// the SQL planner (`ddl::table`'s lone-PK foreign-key check, `dml::insert`'s
    /// conflict-target name lookup). Hard-asserts length-1: a `debug_assert!`
    /// would compile out in release and let the silent truncation to the first
    /// PK column ship to production.
    #[inline]
    #[track_caller]
    pub fn pk_index_single(&self) -> usize {
        assert_eq!(self.pk_cols.len(), 1, "compound PK not yet supported here");
        self.pk_cols[0]
    }

    /// Number of non-PK ("payload") columns: `columns.len() - pk_count`.
    #[inline]
    pub fn num_payload_cols(&self) -> usize {
        self.num_columns() - self.pk_indices().len()
    }

    /// True iff column `ci` is the PK column.
    #[inline]
    pub fn is_pk_col(&self, ci: usize) -> bool {
        self.pk_indices().contains(&ci)
    }

    /// Byte offset of PK column `col_idx` within the packed PK region.
    /// Mirrors the engine `SchemaDescriptor::pk_byte_offset` helper.
    #[inline]
    pub fn pk_byte_offset(&self, col_idx: usize) -> usize {
        debug_assert!(self.is_pk_col(col_idx));
        self.pk_cols
            .iter()
            .take_while(|&&pi| pi != col_idx)
            .map(|&pi| self.columns[pi].type_code.wire_stride())
            .sum()
    }

    /// Per-PK-column `(wire_stride, type_code)` in compound-key order, for the
    /// OPK encode/decode column walk. Collect once and reuse across rows so the
    /// per-row loop never re-iterates the schema.
    #[inline]
    pub fn pk_col_codes(&self) -> impl Iterator<Item = (usize, u8)> + '_ {
        self.pk_cols.iter().map(move |&ci| {
            let tc = self.columns[ci].type_code;
            (tc.wire_stride(), tc as u8)
        })
    }

    /// Map a logical column index to its dense payload index. Caller must
    /// ensure `col_idx` is not the PK column.
    #[inline]
    pub fn payload_idx(&self, col_idx: usize) -> usize {
        debug_assert!(!self.is_pk_col(col_idx), "payload_idx: col_idx must not be a PK column");
        col_idx - self.pk_cols.iter().filter(|&&p| p < col_idx).count()
    }

    /// Iterate over the non-PK ("payload") columns.
    ///
    /// Yields `(payload_idx, col_idx, &ColumnDef)`. The enumerated index is
    /// the dense payload index (null-bitmap bit position).
    #[inline]
    pub fn payload_columns(&self) -> impl Iterator<Item = (usize, usize, &ColumnDef)> {
        let n = self.num_columns();
        (0..n)
            .filter(move |ci| !self.is_pk_col(*ci))
            .enumerate()
            .map(move |(pi, ci)| (pi, ci, &self.columns[ci]))
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

    /// Physical index of the visible column named `name` (ASCII-case-insensitive),
    /// or `None`. Hidden (DROP COLUMN'd) slots never match: their names are
    /// excluded from resolution, so a new column may reuse one.
    #[inline]
    pub fn visible_column_named(&self, name: &str) -> Option<usize> {
        self.visible_columns()
            .find(|(_, c)| c.name.eq_ignore_ascii_case(name))
            .map(|(i, _)| i)
    }

    /// True iff any **non-PK** column is hidden — i.e. the schema carries a
    /// DROP COLUMN'd slot (a logical drop: physically present, zero-filled NOT
    /// NULL, flagged hidden). A view's synthetic hidden key slots
    /// (`_join_pk`/`_set_pk`/`_group_pk`, unprojected passthrough PKs) are PK
    /// columns and do NOT count: they are filtered at presentation, so the
    /// bare-`SELECT *` / `RETURNING *` fast paths stay on their raw-physical
    /// passthrough for every view. Only a dropped column forces the
    /// hidden-filtering projection.
    #[inline]
    pub fn has_hidden_payload(&self) -> bool {
        (0..self.columns.len()).any(|i| self.is_hidden_payload(i))
    }

    /// Whether column `i` is a hidden **payload** column — one a wildcard drops.
    /// A hidden PK column is not dropped: it carries the batch's key, and removing
    /// it would strip the row of its identity. One home for that distinction, so
    /// the passthrough decision (`has_hidden_payload`) and the projection that
    /// implements it cannot disagree.
    #[inline]
    pub fn is_hidden_payload(&self, i: usize) -> bool {
        self.columns[i].is_hidden && !self.is_pk_col(i)
    }

    /// The output-key kind a reduce grouped by `cols` over this schema gets —
    /// the planner-side decision shipped on the wire and validated (never
    /// re-decided) by the engine, both through
    /// [`ReduceOutKey::for_group_cols`].
    pub fn reduce_out_key(&self, cols: &[usize]) -> ReduceOutKey {
        let pk: Vec<u32> = self.pk_cols.iter().map(|&c| c as u32).collect();
        let group: Vec<u32> = cols.iter().map(|&c| c as u32).collect();
        ReduceOutKey::for_group_cols(&pk, &group, |c| {
            let cd = &self.columns[c as usize];
            (cd.type_code as u8, cd.is_nullable)
        })
    }

    /// The single definition of "these parts form an admissible schema": the
    /// `MAX_COLUMNS` cap (the region null bitmap is one u64 word), the
    /// structural PK rules (non-empty, ≤ `PK_LIST_MAX_COLS`, every index
    /// `< columns.len()`, no duplicates), and the per-column invariants the
    /// engine's `SchemaDescriptor::new` hard-asserts — each PK column
    /// non-nullable and PK-eligible. Shared by [`Schema::from_parts`] and the
    /// client's `create_table` / `create_view_chain` DDL gateways, so a
    /// malformed spec is a clean client error rather than a server-side assert.
    ///
    /// The arity cap is the persisted PK-list codec capacity, not the wider
    /// in-memory `MAX_PK_COLUMNS`: a client never builds the engine-internal
    /// secondary-index schema that uses the extra slot, so a PK it accepts must
    /// round-trip through the codec.
    pub fn validate_parts(pk_cols: &[u32], columns: &[ColumnDef]) -> Result<(), String> {
        if columns.len() > MAX_COLUMNS {
            return Err("column count exceeds MAX_COLUMNS".into());
        }
        gnitz_wire::validate_pk_indices(pk_cols, columns.len()).map_err(|r| r.to_string())?;
        gnitz_wire::validate_pk_column_types(pk_cols, |c| {
            let cd = &columns[c as usize];
            (cd.type_code as u8, cd.is_nullable)
        })
        .map(|_stride| ())
        .map_err(|r| r.to_string())
    }

    /// Fallible constructor for a schema assembled from untrusted parts — a
    /// wire schema block or catalog rows. Runs [`Schema::validate_parts`],
    /// so every decode boundary applies the same rule set.
    pub fn from_parts(columns: Vec<ColumnDef>, pk_cols: Vec<usize>) -> Result<Schema, String> {
        // Saturate rather than cast: the narrowing happens *before*
        // `validate_pk_indices` compares against the column count, so a plain
        // `as u32` would wrap an index of exactly 2^32 to 0 and let it pass.
        let idx: Vec<u32> = pk_cols.iter().map(|&c| c.min(u32::MAX as usize) as u32).collect();
        Self::validate_parts(&idx, &columns)?;
        Ok(Schema { columns, pk_cols })
    }

    /// Structural type-equality used by the warm-push guard. Mirrors the
    /// server's `validate_schema_match` field set: column count, per-column
    /// `type_code`, `pk_cols`, and per-column nullability. Deliberately does
    /// NOT compare column *names* (the server validator does not), so a
    /// name-only difference still takes the warm fast path.
    pub fn types_match(&self, other: &Schema) -> bool {
        self.columns.len() == other.columns.len()
            && self.pk_cols == other.pk_cols
            && self
                .columns
                .iter()
                .zip(&other.columns)
                .all(|(a, b)| a.type_code == b.type_code && a.is_nullable == b.is_nullable)
    }
}

/// The schema surface the shared expression compiler resolves and validates
/// against — the client's half of the impl the engine's `SchemaDescriptor`
/// provides, so one compiler serves both.
///
/// `num_columns` and `num_payload_cols` are written UFCS: the inherent method of
/// the same name would shadow the trait one in receiver-dot position and recurse.
/// The last two read the column table, which *is* the fact; everything else the
/// trait derives from `locate`.
impl gnitz_expr::SchemaFacts for Schema {
    fn locate(&self, ci: usize) -> gnitz_expr::ColumnLocator {
        let tc = self.columns[ci].type_code;
        // Every narrowing is in range: a payload slot is below MAX_COLUMNS, a PK
        // byte offset below MAX_PK_BYTES, every fixed width is <= 16.
        let size = tc.wire_stride() as u8;
        if Schema::is_pk_col(self, ci) {
            gnitz_expr::ColumnLocator::Pk {
                byte_off: Schema::pk_byte_offset(self, ci) as u8,
                size,
                type_code: tc as u8,
            }
        } else {
            gnitz_expr::ColumnLocator::Payload {
                slot: Schema::payload_idx(self, ci) as u8,
                size,
                type_code: tc as u8,
            }
        }
    }

    fn num_payload_cols(&self) -> usize {
        Schema::num_payload_cols(self)
    }

    fn num_columns(&self) -> usize {
        Schema::num_columns(self)
    }

    fn col_type_code(&self, ci: usize) -> u8 {
        self.columns[ci].type_code as u8
    }

    fn col_nullable(&self, ci: usize) -> bool {
        self.columns[ci].is_nullable
    }
}

/// A batch's PK buffer in memory: `stride` bytes per row, **native
/// little-endian**, columns packed in PK-list order. A client `ZSetBatch` holds
/// PK values as the wire delivered them, so a signed column reads as two's
/// complement.
///
/// **Not the PK region.** That name belongs to the §4 form —
/// `gnitz_wire::wal::encode` frames it, a `ZSetBatchView` presents it, and it is
/// OPK everywhere, client-side included. `build_pk_region_into` is where this
/// buffer becomes one. Call this the `PkColumn` buffer; a comment that calls it
/// a PK region is one step from asserting the region is native-LE.
///
/// One representation at every arity: a lone U32 key is 4 bytes per row, a lone
/// UUID 16, a compound `(u64, u32)` 12. The stride comes from the schema
/// (`Schema::pk_stride`), so it is never independent data to keep in sync.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PkColumn {
    pub stride: u8,
    pub buf: Vec<u8>,
}

impl PkColumn {
    /// An empty column of `stride` bytes per row.
    pub fn new(stride: u8) -> Self {
        PkColumn { stride, buf: vec![] }
    }

    /// Empty `PkColumn` matching `schema`'s PK layout. `ZSetBatch::new`,
    /// `GnitzClient::delete`, and the SQL DML helpers all route through this.
    pub fn empty_for_schema(schema: &Schema) -> Self {
        Self::new(schema.pk_stride() as u8)
    }

    /// A column of `stride`-byte keys from their packed u128 values — each
    /// value's low `stride` bytes. The counterpart of [`Self::get`].
    pub fn from_u128s(stride: u8, vals: impl IntoIterator<Item = u128>) -> Self {
        let mut c = Self::new(stride);
        for v in vals {
            c.push_u128(v);
        }
        c
    }

    fn width(&self) -> usize {
        self.stride as usize
    }

    /// The `size` bytes of the PK column at PK-region byte offset `off` for row
    /// `i`.
    pub fn col_window(&self, i: usize, off: usize, size: usize) -> &[u8] {
        let base = i * self.width() + off;
        &self.buf[base..base + size]
    }

    pub fn len(&self) -> usize {
        self.buf.len() / self.width()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Row `i` widened to a u128 — the low `stride` bytes are the key. Only
    /// meaningful for a key that fits in 16 bytes; a wider compound key has no
    /// scalar projection and callers read [`Self::get_bytes`] instead.
    pub fn get(&self, i: usize) -> u128 {
        low16_le(self.get_bytes(i))
    }

    /// Append one raw PK tuple, already in on-wire LE layout.
    pub fn push_bytes(&mut self, b: &[u8]) {
        debug_assert_eq!(b.len(), self.width());
        self.buf.extend_from_slice(b);
    }

    /// Borrow the raw `stride`-byte tuple at row `i`.
    pub fn get_bytes(&self, i: usize) -> &[u8] {
        let s = self.width();
        &self.buf[i * s..(i + 1) * s]
    }

    /// Append a key whose low `stride` bytes carry the LE-packed columns.
    pub fn push_u128(&mut self, pk: u128) {
        let s = self.width();
        // A hard assert, not debug-only: in release the slice below would
        // otherwise OOB-panic with an opaque "index out of range".
        assert!(s <= 16, "push_u128: stride {s} > 16 cannot come from a u128");
        self.buf.extend_from_slice(&pk.to_le_bytes()[..s]);
    }

    pub fn truncate(&mut self, len: usize) {
        self.buf.truncate(len * self.width());
    }

    /// Read row `i` into a `PkTuple`. The tuple's stride is this column's, so
    /// it cannot disagree with the bytes it carries.
    pub fn get_tuple(&self, i: usize) -> PkTuple {
        PkTuple::from_bytes(self.get_bytes(i))
    }

    /// Append the row at `src[i]` to `self`. Strides must match.
    pub fn push_from(&mut self, src: &PkColumn, i: usize) {
        debug_assert_eq!(self.stride, src.stride);
        self.buf.extend_from_slice(src.get_bytes(i));
    }

    /// Append `pk`'s bytes to `self`.
    pub fn push_tuple(&mut self, pk: &PkTuple) {
        debug_assert_eq!(pk.stride, self.stride);
        self.buf.extend_from_slice(&pk.buf[..self.width()]);
    }

    /// Every key widened to u128, for `assert_eq!(pks.to_vec_u128(), expected)`.
    #[cfg(test)]
    pub fn to_vec_u128(&self) -> Vec<u128> {
        (0..self.len()).map(|i| self.get(i)).collect()
    }
}

/// SQL→client carrier for one row's PK. Carries `(stride, bytes)` so callers
/// above the wire codec do not need to handle the `(seek_pk: u128 +
/// seek_pk_extra: BLOB)` wire-level split.
#[derive(Clone, Copy)]
pub struct PkTuple {
    pub stride: u8,
    pub buf: [u8; MAX_PK_BYTES],
}

impl PkTuple {
    /// The "no seek" PK tuple: stride 0, splitting to the inert wire pair
    /// `(0u128, &[])`. Passed by non-seek frames (push / scan / alloc) so the
    /// call layer never hand-writes the wire split.
    pub const EMPTY: PkTuple = PkTuple {
        stride: 0,
        buf: [0u8; MAX_PK_BYTES],
    };

    pub fn new(stride: u8) -> Self {
        debug_assert!(stride as usize <= MAX_PK_BYTES);
        Self {
            stride,
            buf: [0u8; MAX_PK_BYTES],
        }
    }

    /// Construct from a u128 whose low `stride` bytes carry the column's
    /// native LE bytes (as produced by `parse_pk_literal_packed`). Copies
    /// only `stride` bytes so callers cannot pollute the high padding.
    pub fn from_u128(stride: u8, v: u128) -> Self {
        debug_assert!(stride as usize <= 16);
        let s = stride as usize;
        let mut t = Self::new(stride);
        t.buf[..s].copy_from_slice(&v.to_le_bytes()[..s]);
        t
    }

    /// Build a tuple from a u128 with the full 16-byte narrow stride, without
    /// a schema lookup — for a caller holding a key value but not the schema.
    /// The server reads only the column's actual stride; the high padding bytes
    /// (if any) are inert.
    pub fn from_u128_narrow(v: u128) -> Self {
        Self::from_u128(16, v)
    }

    /// [`PkTuple::from_bytes`] for a caller holding a length it has not checked:
    /// the one rule (a packed PK region is 1..=`MAX_PK_BYTES` bytes) and the one
    /// message, instead of a per-caller pre-check ahead of the hard assert
    /// below.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() || bytes.len() > MAX_PK_BYTES {
            return Err(format!(
                "packed pk must be 1..={MAX_PK_BYTES} bytes, got {}",
                bytes.len()
            ));
        }
        Ok(Self::from_bytes(bytes))
    }

    /// Build a tuple from a raw byte slice. `bytes.len()` becomes the stride —
    /// for the paths that carry a packed PK region as an opaque byte buffer.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        // Hard assert (not debug-only): `bytes` is an externally-controlled
        // length, and in release `t.buf[..bytes.len()]` would OOB-panic (or, for
        // len ≥ 256, `bytes.len() as u8` would silently truncate the stride first).
        // The assert bounds the length, making the `as u8` cast lossless.
        assert!(
            bytes.len() <= MAX_PK_BYTES,
            "PkTuple::from_bytes: length {} exceeds MAX_PK_BYTES {MAX_PK_BYTES}",
            bytes.len(),
        );
        let mut t = Self::new(bytes.len() as u8);
        t.buf[..bytes.len()].copy_from_slice(bytes);
        t
    }

    /// On-wire PK region bytes 0..stride.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buf[..self.stride as usize]
    }

    /// Split the tuple into the wire form `(seek_pk: u128, seek_pk_extra: &[u8])`.
    /// `extra` is empty for a narrow PK. The engine's `seek_opk_bytes` is the
    /// exact inverse and must cut at the same constant for every SEEK.
    pub fn split_wire(&self) -> (u128, &[u8]) {
        let bytes = self.as_bytes();
        let extra: &[u8] = if bytes.len() > gnitz_wire::NARROW_PK_MAX_BYTES {
            &bytes[gnitz_wire::NARROW_PK_MAX_BYTES..]
        } else {
            &[]
        };
        (low16_le(bytes), extra)
    }
}

/// A packed PK's low `NARROW_PK_MAX_BYTES` bytes as a u128 — the scalar projection
/// both [`PkColumn::get`] and [`PkTuple::split_wire`] hand out. A wider key has no
/// scalar form; its remaining bytes travel separately.
///
/// Little-endian, so *not* `widen_pk_be`: that recovers a big-endian OPK value.
fn low16_le(key: &[u8]) -> u128 {
    let n = key.len().min(gnitz_wire::NARROW_PK_MAX_BYTES);
    let mut b = [0u8; gnitz_wire::NARROW_PK_MAX_BYTES];
    b[..n].copy_from_slice(&key[..n]);
    u128::from_le_bytes(b)
}

impl PartialEq for PkTuple {
    fn eq(&self, other: &Self) -> bool {
        self.stride == other.stride && self.as_bytes() == other.as_bytes()
    }
}
impl Eq for PkTuple {}

impl std::hash::Hash for PkTuple {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.stride.hash(state);
        self.as_bytes().hash(state);
    }
}

impl std::fmt::Debug for PkTuple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PkTuple")
            .field("stride", &self.stride)
            .field("bytes", &self.as_bytes())
            .finish()
    }
}

/// Per-column payload data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColData {
    /// Raw little-endian bytes; length = count * wire_stride. Covers every
    /// fixed-width type, the 16-byte ones (U128/UUID/I128) included — a u128's
    /// native LE bytes are exactly its wire region.
    Fixed(Vec<u8>),
    Strings(Vec<Option<std::string::String>>),
    /// Variable-length raw byte payloads. Same on-wire encoding as `Strings`
    /// (16-byte German-string struct + blob arena spill) but the bytes are
    /// not constrained to be valid UTF-8.
    Bytes(Vec<Option<Vec<u8>>>),
}

impl ColData {
    /// Append the element at row `idx` of `self` onto `dst`. Both columns are
    /// built from the same schema, so they are always the same variant — a
    /// mismatch is a construction bug, not a runtime condition. `fixed_stride`
    /// is the per-element byte width, used only by the `Fixed` variant.
    ///
    /// Matching on `self` alone (rather than the `(self, dst)` pair under a `_`
    /// wildcard) makes a new `ColData` variant a compile error here rather than
    /// a runtime panic.
    pub fn push_row_from(&self, idx: usize, fixed_stride: usize, dst: &mut ColData) {
        match self {
            ColData::Fixed(s) => {
                let ColData::Fixed(d) = dst else { variant_mismatch() };
                d.extend_from_slice(&s[idx * fixed_stride..(idx + 1) * fixed_stride]);
            }
            ColData::Strings(s) => {
                let ColData::Strings(d) = dst else { variant_mismatch() };
                d.push(s[idx].clone());
            }
            ColData::Bytes(s) => {
                let ColData::Bytes(d) = dst else { variant_mismatch() };
                d.push(s[idx].clone());
            }
        }
    }

    /// Consuming variant of [`Self::push_row_from`] for a source batch the
    /// caller owns and drops afterwards: a `Strings`/`Bytes` cell is *moved*
    /// (`Option::take`) instead of deep-cloned, leaving `None` behind.
    /// Correct only when each source row is taken at most once; fixed-width
    /// variants copy exactly as `push_row_from`.
    pub fn take_row_into(&mut self, idx: usize, fixed_stride: usize, dst: &mut ColData) {
        match self {
            ColData::Strings(s) => {
                let ColData::Strings(d) = dst else { variant_mismatch() };
                d.push(s[idx].take());
            }
            ColData::Bytes(s) => {
                let ColData::Bytes(d) = dst else { variant_mismatch() };
                d.push(s[idx].take());
            }
            ColData::Fixed(_) => self.push_row_from(idx, fixed_stride, dst),
        }
    }

    /// The wire cell at `row` of a fixed-width column: exactly `stride` bytes.
    /// `None` if the row is past the end or this is not a `Fixed` column —
    /// callers decide whether that is an error, a panic, or a fallback.
    pub fn cell(&self, row: usize, stride: usize) -> Option<&[u8]> {
        match self {
            ColData::Fixed(b) => b.get(row * stride..(row + 1) * stride),
            _ => None,
        }
    }

    /// Reserve room for `n` more cells of wire type `tc`.
    pub fn reserve(&mut self, tc: TypeCode, n: usize) {
        match self {
            ColData::Fixed(v) => v.reserve(n * tc.wire_stride()),
            ColData::Strings(v) => v.reserve(n),
            ColData::Bytes(v) => v.reserve(n),
        }
    }

    /// Append a SQL NULL cell for a column of wire type `tc`. The single NULL
    /// encoding across all three variants: fixed-width columns get zero filler
    /// (the null bitmap is the NULL source of truth, §6), German strings a
    /// `None` cell.
    pub fn push_null(&mut self, tc: TypeCode) {
        match self {
            ColData::Fixed(buf) => buf.extend(std::iter::repeat_n(0u8, tc.wire_stride())),
            ColData::Strings(v) => v.push(None),
            ColData::Bytes(v) => v.push(None),
        }
    }

    /// The empty column of the canonical variant for wire type `tc` — the single
    /// TypeCode→variant choice ([`ZSetBatch::filler_columns`], the appenders and
    /// [`Self::matches_type`] all build on it). The only question is whether the
    /// type uses the German-string layout; everything else is raw LE bytes.
    #[inline(always)]
    pub fn empty_for(tc: TypeCode) -> Self {
        match tc {
            TypeCode::String => ColData::Strings(vec![]),
            TypeCode::Blob => ColData::Bytes(vec![]),
            _ => ColData::Fixed(vec![]),
        }
    }

    /// True iff this column's variant is the one [`Self::empty_for`] builds for
    /// `tc`. Derived from that function rather than restating its table, so the
    /// canonical choice and the check that enforces it cannot disagree.
    #[inline(always)]
    pub fn matches_type(&self, tc: TypeCode) -> bool {
        std::mem::discriminant(self) == std::mem::discriminant(&Self::empty_for(tc))
    }

    /// Append one zero-filled **non-null** cell for a column of wire type `tc` —
    /// the single filler-cell encoding: a fixed column gets zero bytes, a German
    /// string/blob an empty `Some` cell. Used per-row for the
    /// `ALTER … DROP COLUMN` hidden slot (§6) and in bulk by
    /// [`ZSetBatch::filler_columns`]. Differs from [`Self::push_null`] only for
    /// Strings/Bytes (`Some("")` vs `None`): the null bit stays **unset**, so the
    /// cell must be a real value — which keeps validate happy against a NOT-NULL
    /// column and keeps the table on the `FixedIntNonnull` fast comparator.
    pub fn push_filler(&mut self, tc: TypeCode) {
        match self {
            ColData::Fixed(buf) => buf.extend(std::iter::repeat_n(0u8, tc.wire_stride())),
            ColData::Strings(v) => v.push(Some(std::string::String::new())),
            ColData::Bytes(v) => v.push(Some(Vec::new())),
        }
    }

    /// `count` filler cells in one allocation — the bulk [`Self::push_filler`],
    /// dispatched through [`Self::empty_for`] so the TypeCode→variant table stays
    /// defined once. `vec!`'s `SpecFromElem` reaches `alloc_zeroed`, a library
    /// specialisation that holds at `opt-level=0` where a fill loop would not.
    pub(crate) fn filled(tc: TypeCode, count: usize) -> Self {
        match Self::empty_for(tc) {
            ColData::Fixed(_) => ColData::Fixed(vec![0u8; count * tc.wire_stride()]),
            ColData::Strings(_) => ColData::Strings(vec![Some(std::string::String::new()); count]),
            ColData::Bytes(_) => ColData::Bytes(vec![Some(Vec::new()); count]),
        }
    }
}

/// The single read/write convention for the payload null bitmap (bit `pi` = the
/// `pi`-th non-PK column in schema order, `Schema::payload_idx`, §6). Defined in
/// `gnitz-wire` — the crate that already owns the §6 region indices — so the
/// client, the evaluator and the engine cannot spell it three different ways.
pub use gnitz_wire::{null_word_get, null_word_set};

#[cold]
#[inline(never)]
#[track_caller]
fn variant_mismatch() -> ! {
    panic!("ColData: source and destination column variants differ");
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZSetBatch {
    pub pks: PkColumn,
    pub weights: Vec<i64>,
    pub nulls: Vec<u64>,
    /// One entry per schema column. Entry at pk_index is a placeholder (Fixed(vec![])).
    pub columns: Vec<ColData>,
}

impl ZSetBatch {
    pub fn new(schema: &Schema) -> Self {
        ZSetBatch {
            pks: PkColumn::empty_for_schema(schema),
            weights: vec![],
            nulls: vec![],
            columns: Self::filler_columns(schema, 0),
        }
    }

    /// One `ColData` per schema column, zero-filled for `count` rows — the
    /// canonical TypeCode→variant choice the wire encoder accepts (PK slots are
    /// empty `Fixed` placeholders). `new` is the `count = 0` case; the client's
    /// `delete` uses `count > 0` as inert payload filler for retraction rows
    /// (the server's `retract_pk` matches by PK alone, so an empty `Some` value
    /// encoding to zero bytes under the all-present null bitmap is fine).
    pub(crate) fn filler_columns(schema: &Schema, count: usize) -> Vec<ColData> {
        schema
            .columns
            .iter()
            .enumerate()
            // A PK column's payload slot is an empty placeholder whatever `count`
            // is: its values live in the OPK region.
            .map(|(ci, col)| ColData::filled(col.type_code, if schema.is_pk_col(ci) { 0 } else { count }))
            .collect()
    }

    /// An empty batch with every growth stream sized for `n` rows: the PK
    /// buffer, the weights, the null words and each payload column. The form to
    /// use whenever the row count is known before the build loop — otherwise a
    /// column-at-a-time fill reallocates its way up from zero.
    pub fn with_capacity(schema: &Schema, n: usize) -> Self {
        let mut b = Self::new(schema);
        b.pks.buf.reserve(n * schema.pk_stride());
        b.weights.reserve(n);
        b.nulls.reserve(n);
        for (_pi, ci, col) in schema.payload_columns() {
            b.columns[ci].reserve(col.type_code, n);
        }
        b
    }

    pub fn len(&self) -> usize {
        self.pks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pks.is_empty()
    }

    /// Whether column `ci` is SQL NULL at `row`: a PK column is never NULL;
    /// a payload column reads its bit from the null bitmap — the single NULL
    /// source across every `ColData` variant (a `Fixed` NULL is zero-filled
    /// filler with no per-value sentinel).
    #[inline]
    pub fn is_null(&self, schema: &Schema, row: usize, ci: usize) -> bool {
        !schema.is_pk_col(ci) && null_word_get(self.nulls[row], schema.payload_idx(ci))
    }

    /// Indices of the live rows — those with positive weight. A `ZSetBatch`
    /// row with weight ≤ 0 is a retraction/ghost, not a present element, so a
    /// catalog scan over a batch iterates only its live rows.
    pub fn live_rows(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).filter(move |&i| self.weights[i] > 0)
    }

    /// The index of the live row whose PK is `pk`, for a batch keyed by a single
    /// integer column (the catalog tables). `None` = no such live row.
    pub fn live_row_with_pk(&self, pk: u64) -> Option<usize> {
        self.live_rows().find(|&i| self.pks.get(i) as u64 == pk)
    }

    /// Append all rows from `other` into `self`, consuming it and moving its
    /// String/Bytes buffers instead of deep-cloning each value. O(n) pointer
    /// copies, zero heap allocation for string/bytes content. Panics if column
    /// layouts differ. Used in scan continuation loops.
    pub fn extend_from_owned(&mut self, mut other: ZSetBatch) {
        assert_eq!(
            self.columns.len(),
            other.columns.len(),
            "extend_from_owned: column count mismatch",
        );
        assert_eq!(
            self.pks.stride, other.pks.stride,
            "extend_from_owned: PK stride mismatch",
        );
        self.pks.buf.append(&mut other.pks.buf);
        self.weights.append(&mut other.weights);
        self.nulls.append(&mut other.nulls);
        for (a, b) in self.columns.iter_mut().zip(other.columns.iter_mut()) {
            match (a, b) {
                (ColData::Fixed(a), ColData::Fixed(b)) => a.append(b),
                (ColData::Strings(a), ColData::Strings(b)) => a.append(b),
                (ColData::Bytes(a), ColData::Bytes(b)) => a.append(b),
                _ => panic!("extend_from_owned: column type mismatch"),
            }
        }
    }

    /// Truncate all per-row vectors back to `n` rows. PK column and
    /// payload columns (including string/blob spill buffers) are kept
    /// consistent — used to roll back partially appended rows.
    pub fn truncate(&mut self, n: usize, schema: &Schema) {
        self.pks.truncate(n);
        self.weights.truncate(n);
        self.nulls.truncate(n);
        for ci in 0..self.columns.len() {
            if schema.is_pk_col(ci) {
                continue;
            }
            match &mut self.columns[ci] {
                ColData::Fixed(buf) => {
                    let stride = schema.columns[ci].type_code.wire_stride();
                    buf.truncate(n * stride);
                }
                ColData::Strings(v) => v.truncate(n),
                ColData::Bytes(v) => v.truncate(n),
            }
        }
    }

    /// Every payload column carries the variant its declared type calls for, at
    /// the row count `self.len()` implies. Split out of [`Self::validate`]
    /// because the region builder needs the same rule for a batch that never
    /// went through the push path.
    ///
    /// The variant is decided by the *declared* type, never by the one found: a
    /// String-typed column carrying `Fixed` has a valid `n * 16` byte length, so
    /// the length check cannot see it — and it would reach the expression
    /// kernels as German cells with arbitrary heap offsets.
    pub fn check_columns(&self, schema: &Schema) -> Result<(), std::string::String> {
        let n = self.len();
        for (_pi, ci, col_def) in schema.payload_columns() {
            let col = &self.columns[ci];
            if !col.matches_type(col_def.type_code) {
                return Err(format!(
                    "column {ci}: ColData variant contradicts schema type {:?}",
                    col_def.type_code
                ));
            }
            let (got, want) = match col {
                ColData::Fixed(b) => (b.len(), n * col_def.type_code.wire_stride()),
                ColData::Strings(v) => (v.len(), n),
                ColData::Bytes(v) => (v.len(), n),
            };
            if got != want {
                return Err(format!("column {ci}: length {got} != expected {want}"));
            }
        }
        Ok(())
    }

    /// Validate that all vectors are consistently sized for the given schema.
    pub fn validate(&self, schema: &Schema) -> Result<(), std::string::String> {
        // The PK buffer's own shape, checked before `len()` divides by the
        // stride. A stride that disagrees with the schema would make the region
        // encoder read every row at the wrong offset.
        if self.pks.stride == 0 {
            return Err("PK stride must be non-zero".into());
        }
        if self.pks.stride as usize != schema.pk_stride() {
            return Err(format!(
                "mismatched PK stride: expected {}, got {}",
                schema.pk_stride(),
                self.pks.stride
            ));
        }
        if !self.pks.buf.len().is_multiple_of(self.pks.stride as usize) {
            return Err(format!(
                "PK buffer length {} is not a multiple of stride {}",
                self.pks.buf.len(),
                self.pks.stride
            ));
        }
        let n = self.pks.len();
        if self.weights.len() != n {
            return Err(format!("weights length {} != row count {}", self.weights.len(), n));
        }
        if self.nulls.len() != n {
            return Err(format!("nulls length {} != row count {}", self.nulls.len(), n));
        }
        if self.columns.len() != schema.num_columns() {
            return Err(format!(
                "column count {} != schema column count {}",
                self.columns.len(),
                schema.num_columns()
            ));
        }
        self.check_columns(schema)?;
        // A null bit on a NOT NULL payload column would make FK/unique validation
        // skip the value (treating it as absent) while consolidation and decoders
        // read the raw bytes as live data — an inconsistency the schema forbids.
        // Reject it. `pi` is the dense payload index (null-bitmap bit position),
        // matching the convention the FK/unique skips use.
        let not_null_mask = gnitz_expr::SchemaFacts::not_null_payload_slots(schema);
        if not_null_mask != 0 {
            for (row, &word) in self.nulls.iter().enumerate() {
                let offending = word & not_null_mask;
                if offending != 0 {
                    // Name the column: the mask says only "some NOT NULL column",
                    // and the caller cannot recover which from a bit position.
                    let pi = offending.trailing_zeros() as usize;
                    let name = schema
                        .payload_columns()
                        .find(|(p, _, _)| *p == pi)
                        .map_or("?", |(_, _, c)| c.name.as_str());
                    return Err(format!("row {row} sets a null bit on NOT NULL column '{name}'"));
                }
            }
        }
        Ok(())
    }
}

/// Builder for appending rows to a `ZSetBatch` with schema-aware column mapping.
///
/// Columns are appended in non-PK order: the cursor automatically skips the PK
/// column index, so callers supply only payload values.
pub struct BatchAppender<'a> {
    batch: &'a mut ZSetBatch,
    schema: &'a Schema,
    cursor: usize,
    row_active: bool,
    /// Payload cursor → schema column index (the N-th non-PK column), computed
    /// once so `col_index` is an array read rather than a per-value scan.
    payload_to_ci: Vec<usize>,
}

/// The client half of the shared catalog row codecs: the sink
/// `gnitz_wire::sys_rows` writes a system-table row into. `end_row` is a no-op
/// because this builder writes the null word eagerly in `add_row` and needs no
/// per-row close.
impl gnitz_wire::sys_rows::SysRowSink for BatchAppender<'_> {
    fn begin_row(&mut self, pk: u128, weight: i64) {
        self.add_row(pk, weight);
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
    fn end_row(&mut self) {}
}

impl<'a> BatchAppender<'a> {
    pub fn new(batch: &'a mut ZSetBatch, schema: &'a Schema) -> Self {
        let payload_to_ci: Vec<usize> = schema.payload_columns().map(|(_, ci, _)| ci).collect();
        BatchAppender {
            batch,
            schema,
            cursor: 0,
            row_active: false,
            payload_to_ci,
        }
    }

    /// Start a new row with the given primary key and weight.
    pub fn add_row(&mut self, pk: u128, weight: i64) -> &mut Self {
        // Each row must receive exactly `num_payload_cols()` payload pushes before
        // the next `add_row`. Symmetric counterpart to `col_index`'s over-push
        // assert; an under-pushed row otherwise desyncs the column vectors and
        // only surfaces later as an un-attributed `ZSetBatch::validate` length
        // error. `debug_assert` (not `assert`): `validate` already rejects the bad
        // batch totally and safely, so this is a diagnostic, not a safety guard.
        // `row_active` exempts the first `add_row` without assuming the batch started
        // empty.
        debug_assert!(
            !self.row_active || self.cursor == self.schema.num_payload_cols(),
            "BatchAppender::add_row: previous row got {} of {} payload columns",
            self.cursor,
            self.schema.num_payload_cols(),
        );
        self.batch.pks.push_u128(pk);
        self.batch.weights.push(weight);
        self.batch.nulls.push(0);
        self.cursor = 0;
        self.row_active = true;
        self
    }

    /// Append one fixed-width cell to the next column: `bytes` is the column's
    /// whole wire region entry, so its length must be the declared stride —
    /// otherwise the region ends up the wrong size and every later row reads at
    /// the wrong offset.
    fn fixed_val(&mut self, bytes: &[u8]) -> &mut Self {
        let ci = self.col_index();
        let tc = self.schema.columns[ci].type_code;
        match &mut self.batch.columns[ci] {
            ColData::Fixed(buf) => {
                assert_eq!(
                    bytes.len(),
                    tc.wire_stride(),
                    "BatchAppender: {tc:?} column at schema index {ci} takes {} bytes",
                    tc.wire_stride(),
                );
                buf.extend_from_slice(bytes);
            }
            _ => panic!("BatchAppender: fixed value written to {tc:?} column at schema index {ci}"),
        }
        self.cursor += 1;
        self
    }

    /// Append a u64 value to the next Fixed column.
    pub fn u64_val(&mut self, v: u64) -> &mut Self {
        self.fixed_val(&v.to_le_bytes())
    }

    /// Append an i64 value to the next Fixed column. Same eight bytes as
    /// [`Self::u64_val`]; the separate name keeps a signed column's writer
    /// honest at the call site.
    pub fn i64_val(&mut self, v: i64) -> &mut Self {
        self.fixed_val(&v.to_le_bytes())
    }

    /// Append a u128 value to the next Fixed column: its 16 native LE bytes,
    /// which are the column's wire region (U128/UUID/I128).
    pub fn u128_val(&mut self, v: u128) -> &mut Self {
        self.fixed_val(&v.to_le_bytes())
    }

    /// Append a string value to the next Strings column.
    pub fn str_val(&mut self, s: &str) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Strings(v) => v.push(Some(s.to_string())),
            _ => panic!("BatchAppender: str_val called on non-Strings column at schema index {ci}"),
        }
        self.cursor += 1;
        self
    }

    /// Append a raw byte slice to the next Bytes (BLOB) column.
    pub fn bytes_val(&mut self, b: &[u8]) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Bytes(v) => v.push(Some(b.to_vec())),
            _ => panic!("BatchAppender: bytes_val called on non-Bytes column at schema index {ci}"),
        }
        self.cursor += 1;
        self
    }

    /// Append a SQL NULL to the next column, whatever its type: the variant's
    /// null cell plus the row bitmap bit. Self-sufficient — no out-of-band
    /// `null_mask` call.
    ///
    /// The read side (the expression evaluator, the WAL encoder) gates on
    /// `nulls[row] & (1 << payload_idx)` and consults the `Option` only when that
    /// bit is clear, so the pushed cell and the bitmap must agree.
    pub fn null(&mut self) -> &mut Self {
        let ci = self.col_index();
        self.batch.columns[ci].push_null(self.schema.columns[ci].type_code);
        let pi = self.schema.payload_idx(ci);
        let word = self
            .batch
            .nulls
            .last_mut()
            .expect("BatchAppender: null called before add_row");
        null_word_set(word, pi, true);
        self.cursor += 1;
        self
    }

    /// Map the payload cursor to the actual schema column index, skipping every
    /// PK column. Supports compound PKs (e.g. the catalog circuit tables whose
    /// PK is (view_id, sub)): payload value N targets the N-th non-PK column.
    fn col_index(&self) -> usize {
        // Hard assert (not debug-only): a misbehaving caller gets a clear panic
        // here instead of an OOB index panicking at the `columns[ci]` call site.
        assert!(
            self.cursor < self.payload_to_ci.len(),
            "BatchAppender: payload cursor {} exceeds {} payload columns",
            self.cursor,
            self.payload_to_ci.len(),
        );
        self.payload_to_ci[self.cursor]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `Schema` must answer the shared `SchemaFacts` shape matrix exactly. This
    /// is where an OPK byte offset or a payload-slot off-by-one would
    /// miscompute silently rather than error, and the harness is the only way to
    /// reach the trait methods — two of them collide by name with an inherent
    /// method Rust prefers in receiver-dot position.
    #[test]
    fn schema_conforms_to_schema_facts() {
        gnitz_expr::assert_schema_facts_matrix(|cols, pk| {
            let columns: Vec<ColumnDef> = cols
                .iter()
                .enumerate()
                .map(|(i, &(tc, nullable))| ColumnDef::new(format!("c{i}"), TypeCode::from_validated_u8(tc), nullable))
                .collect();
            Schema::from_parts(columns, pk.to_vec()).expect("client-valid schema")
        });
    }

    #[test]
    fn validate_parts_enforces_full_rule_set() {
        let cols = vec![
            ColumnDef::new("a", TypeCode::U64, false),    // 0: eligible, non-null
            ColumnDef::new("b", TypeCode::I32, false),    // 1: eligible, non-null
            ColumnDef::new("s", TypeCode::String, false), // 2: ineligible type
            ColumnDef::new("n", TypeCode::U64, true),     // 3: nullable
            ColumnDef::new("f", TypeCode::F64, false),    // 4: ineligible type
        ];

        // Valid single and compound PKs (including the I128 join-key type).
        assert!(Schema::validate_parts(&[0], &cols).is_ok());
        assert!(Schema::validate_parts(&[0, 1], &cols).is_ok());

        // Each rule rejects, and names itself. The wording is `PkRule`'s.
        for (pk, want) in [
            (&[][..], "at least one column"),             // empty
            (&[0, 1, 0, 1, 0][..], "out of range 1..=4"), // over-long
            (&[9][..], "index 9 out of bounds"),          // out of range
            (&[0, 0][..], "column 0 twice"),              // duplicate
            (&[3][..], "must not be nullable"),           // nullable column
            (&[2][..], "only fixed-width integer"),       // STRING is ineligible
            (&[4][..], "only fixed-width integer"),       // F64 is ineligible
        ] {
            let got = Schema::validate_parts(pk, &cols).unwrap_err();
            assert!(got.contains(want), "pk {pk:?}: {got:?} does not mention {want:?}");
        }

        // Column-count cap: the null bitmap is one u64, so > MAX_COLUMNS rejects.
        let wide: Vec<ColumnDef> = (0..=MAX_COLUMNS)
            .map(|i| ColumnDef::new(format!("c{i}"), TypeCode::U64, i > 0))
            .collect();
        assert_eq!(
            Schema::validate_parts(&[0], &wide).unwrap_err(),
            "column count exceeds MAX_COLUMNS"
        );
        assert!(Schema::validate_parts(&[0], &wide[..MAX_COLUMNS]).is_ok());
    }

    #[test]
    fn filler_columns_encode_without_panic() {
        // The client delete path builds retraction batches from `filler_columns`
        // (bypassing BatchAppender, whose `add_row` takes a scalar PK). Cover
        // every payload family — including a nullable String — the same way
        // `push` exercises them: validate, then encode.
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("i", TypeCode::I64, false),    // Fixed, 8B
                ColumnDef::new("big", TypeCode::I128, false), // Fixed, 16B
                ColumnDef::new("u", TypeCode::U128, false),   // Fixed, 16B
                ColumnDef::new("uid", TypeCode::UUID, false), // Fixed, 16B
                ColumnDef::new("s", TypeCode::String, true),  // Strings, nullable
                ColumnDef::new("b", TypeCode::Blob, false),   // Bytes
            ],
            pk_cols: vec![0],
        };
        let count = 2;
        let batch = ZSetBatch {
            pks: PkColumn::from_u128s(8, [10, 20]),
            weights: vec![-1; count],
            nulls: vec![0; count],
            columns: ZSetBatch::filler_columns(&schema, count),
        };
        batch.validate(&schema).expect("filler batch must validate");
        let _ = crate::protocol::wal_block::encode_wal_block(&schema, 7, &batch);
    }

    #[test]
    fn test_num_payload_cols() {
        // 2-column schema → 1 payload column.
        let s = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        };
        assert_eq!(s.num_payload_cols(), 1);

        // pk_index not at column 0 → same answer (columns.len() - pk_indices().len()).
        let s = Schema {
            columns: vec![
                ColumnDef::new("a", TypeCode::U64, false),
                ColumnDef::new("b", TypeCode::U64, false),
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("c", TypeCode::U64, false),
            ],
            pk_cols: vec![2],
        };
        assert_eq!(s.num_payload_cols(), 3);
    }

    #[test]
    fn test_num_columns() {
        let s = Schema {
            columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
            pk_cols: vec![0],
        };
        assert_eq!(s.num_columns(), 1);

        let s = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::I64, false),
                ColumnDef::new("s", TypeCode::String, true),
            ],
            pk_cols: vec![0],
        };
        assert_eq!(s.num_columns(), 3);
    }

    #[test]
    fn test_wire_stride_string() {
        assert_eq!(
            TypeCode::String.wire_stride(),
            16,
            "String wire stride must be 16 (German String struct: 4B len + 4B prefix + 8B ptr/inline)"
        );
    }

    #[test]
    fn test_wire_stride_all() {
        assert_eq!(TypeCode::U8.wire_stride(), 1);
        assert_eq!(TypeCode::I8.wire_stride(), 1);
        assert_eq!(TypeCode::U16.wire_stride(), 2);
        assert_eq!(TypeCode::I16.wire_stride(), 2);
        assert_eq!(TypeCode::U32.wire_stride(), 4);
        assert_eq!(TypeCode::I32.wire_stride(), 4);
        assert_eq!(TypeCode::F32.wire_stride(), 4);
        assert_eq!(TypeCode::U64.wire_stride(), 8);
        assert_eq!(TypeCode::I64.wire_stride(), 8);
        assert_eq!(TypeCode::F64.wire_stride(), 8);
        assert_eq!(TypeCode::String.wire_stride(), 16);
        assert_eq!(TypeCode::U128.wire_stride(), 16);
    }

    // --- Step 1: Schema equality tests ---

    #[test]
    fn test_schema_eq() {
        let a = Schema {
            columns: vec![
                ColumnDef::new("id", TypeCode::U64, false),
                ColumnDef::new("name", TypeCode::String, true),
            ],
            pk_cols: vec![0],
        };
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_schema_ne_col_name() {
        let a = Schema {
            columns: vec![ColumnDef::new("id", TypeCode::U64, false)],
            pk_cols: vec![0],
        };
        let b = Schema {
            columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
            pk_cols: vec![0],
        };
        assert_ne!(a, b);
    }

    #[test]
    fn test_schema_ne_pk_index() {
        let a = Schema {
            columns: vec![
                ColumnDef::new("a", TypeCode::U64, false),
                ColumnDef::new("b", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        };
        let b = Schema {
            columns: vec![
                ColumnDef::new("a", TypeCode::U64, false),
                ColumnDef::new("b", TypeCode::U64, false),
            ],
            pk_cols: vec![1],
        };
        assert_ne!(a, b);
    }

    #[test]
    fn test_schema_ne_type_code() {
        let a = Schema {
            columns: vec![ColumnDef::new("x", TypeCode::U64, false)],
            pk_cols: vec![0],
        };
        let b = Schema {
            columns: vec![ColumnDef::new("x", TypeCode::I64, false)],
            pk_cols: vec![0],
        };
        assert_ne!(a, b);
    }

    // --- types_match (warm-push guard) tests ---

    fn one_col(name: &str, tc: TypeCode, nullable: bool) -> Schema {
        Schema {
            columns: vec![ColumnDef::new(name, tc, nullable)],
            pk_cols: vec![0],
        }
    }

    #[test]
    fn types_match_false_on_pk_type_u64_vs_i64() {
        // The core bug: a U64-pk batch against an I64 table must NOT take the
        // warm path (different OPK image for the same logical value).
        let u = one_col("pk", TypeCode::U64, false);
        let i = one_col("pk", TypeCode::I64, false);
        assert!(!u.types_match(&i));
    }

    #[test]
    fn types_match_ignores_column_names() {
        // A name-only difference must still take the warm fast path: the
        // server validator ignores names.
        let a = one_col("pk", TypeCode::U64, false);
        let b = one_col("id", TypeCode::U64, false);
        assert!(a.types_match(&b));
    }

    #[test]
    fn types_match_false_on_nullability() {
        let a = one_col("x", TypeCode::U64, false);
        let b = one_col("x", TypeCode::U64, true);
        assert!(!a.types_match(&b));
    }

    #[test]
    fn types_match_false_on_pk_cols() {
        let a = Schema {
            columns: vec![
                ColumnDef::new("a", TypeCode::U64, false),
                ColumnDef::new("b", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        };
        let mut b = a.clone();
        b.pk_cols = vec![1];
        assert!(!a.types_match(&b));
    }

    #[test]
    fn types_match_false_on_column_count() {
        let a = one_col("pk", TypeCode::U64, false);
        let b = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        };
        assert!(!a.types_match(&b));
    }

    // --- Step 2: validate() tests ---

    #[test]
    fn test_validate_empty_batch() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("val", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        };
        let batch = ZSetBatch::new(&schema);
        assert!(batch.validate(&schema).is_ok());
    }

    #[test]
    fn test_validate_valid_batch() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("val", TypeCode::I64, false),
                ColumnDef::new("name", TypeCode::String, true),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            a.add_row(1u128, 1).i64_val(10).str_val("a");
            a.add_row(2u128, 1).i64_val(20).str_val("b");
            a.add_row(3u128, 1).i64_val(30).null();
        }
        assert!(batch.validate(&schema).is_ok());
    }

    #[test]
    fn test_validate_mismatched_weights() {
        let schema = Schema {
            columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        // weights is empty — mismatch
        let err = batch.validate(&schema).unwrap_err();
        assert!(err.contains("weights"));
    }

    #[test]
    fn test_validate_mismatched_strings() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("s", TypeCode::String, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        batch.weights.push(1);
        batch.nulls.push(0);
        // Strings column is empty — mismatch (a cell count, not a byte length)
        let err = batch.validate(&schema).unwrap_err();
        assert!(err.contains("column 1: length 0 != expected 1"), "{err}");
    }

    #[test]
    fn test_validate_mismatched_fixed() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        batch.pks.push_u128(1);
        batch.weights.push(1);
        batch.nulls.push(0);
        // Fixed column 1 is empty (needs 8 bytes) — a byte length, not a count
        let err = batch.validate(&schema).unwrap_err();
        assert!(err.contains("column 1: length 0 != expected 8"), "{err}");
    }

    #[test]
    fn test_validate_rejects_null_bit_on_not_null_column() {
        // A null bit on a NOT NULL payload column must be rejected: FK/unique
        // validation would skip the value while decoders read it as live data.
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::I64, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            a.add_row(1u128, 1).i64_val(10);
        }
        assert!(batch.validate(&schema).is_ok(), "clean batch must pass");
        // Flip the null bit on payload col 0 (`v`, NOT NULL) → rejected.
        batch.nulls[0] |= 1 << 0;
        let err = batch.validate(&schema).unwrap_err();
        assert!(err.contains("NOT NULL"), "got: {err}");
    }

    #[test]
    fn test_validate_allows_null_bit_on_nullable_column() {
        // The same null bit on a NULLABLE payload column is fine.
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::I64, true),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            a.add_row(1u128, 1).i64_val(10);
        }
        batch.nulls[0] |= 1 << 0;
        assert!(
            batch.validate(&schema).is_ok(),
            "a null bit on a nullable column must be accepted"
        );
    }

    /// A two-column wide-PK schema whose `Bytes` PK buffer is not a whole
    /// number of `stride`-byte rows must be rejected by `validate`.
    fn wide_pk_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef::new("a", TypeCode::U64, false),
                ColumnDef::new("b", TypeCode::U64, false),
            ],
            pk_cols: vec![0, 1],
        }
    }

    #[test]
    fn test_validate_wide_pk_buffer_not_multiple_of_stride() {
        let schema = wide_pk_schema();
        // stride 16 (two U64 PK cols), buffer 20 bytes → 1.25 rows.
        // Keep weights/nulls consistent with the truncated row count (1) so
        // only the stride-divisibility check can trip.
        let batch = ZSetBatch {
            pks: PkColumn {
                stride: 16,
                buf: vec![0u8; 20],
            },
            weights: vec![1],
            nulls: vec![0],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(vec![])],
        };
        let err = batch.validate(&schema).unwrap_err();
        assert!(err.contains("multiple of stride"), "unexpected error: {err}");
    }

    #[test]
    fn test_validate_wide_pk_zero_stride() {
        let schema = wide_pk_schema();
        // A zero stride would panic the `len()`/modulo divides; validate must
        // reject it as the malformed-input gate.
        let batch = ZSetBatch {
            pks: PkColumn {
                stride: 0,
                buf: vec![0u8; 16],
            },
            weights: vec![],
            nulls: vec![],
            columns: vec![ColData::Fixed(vec![]), ColData::Fixed(vec![])],
        };
        let err = batch.validate(&schema).unwrap_err();
        assert!(err.contains("stride must be non-zero"), "unexpected error: {err}");
    }

    #[test]
    #[should_panic(expected = "column count mismatch")]
    fn test_extend_from_column_count_mismatch_panics() {
        let schema2 = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        };
        let schema1 = Schema {
            columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
            pk_cols: vec![0],
        };
        let mut a = ZSetBatch::new(&schema1);
        let b = ZSetBatch::new(&schema2);
        a.extend_from_owned(b);
    }

    /// `extend_from_owned` (move) concatenates rows across String and Bytes
    /// columns, preserving values and moving the heap buffers.
    #[test]
    fn test_extend_from_owned_concatenates() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("s", TypeCode::String, false),
                ColumnDef::new("b", TypeCode::Blob, false),
            ],
            pk_cols: vec![0],
        };
        let build = |base: u128| {
            let mut z = ZSetBatch::new(&schema);
            {
                let mut a = BatchAppender::new(&mut z, &schema);
                a.add_row(base, 1)
                    .str_val("hello world is long enough")
                    .bytes_val(&[1, 2, 3, 4, 5]);
                a.add_row(base + 1, -1).str_val("short").bytes_val(&[9, 9]);
            }
            z
        };

        let mut acc = build(1);
        acc.extend_from_owned(build(10));

        assert_eq!(acc.len(), 4);
        // Values from both halves survive in order.
        assert_eq!(acc.pks.to_vec_u128(), vec![1u128, 2, 10, 11]);
        assert_eq!(acc.weights, vec![1, -1, 1, -1]);
        match &acc.columns[1] {
            ColData::Strings(v) => assert_eq!(
                v,
                &[
                    Some("hello world is long enough".to_string()),
                    Some("short".to_string()),
                    Some("hello world is long enough".to_string()),
                    Some("short".to_string()),
                ]
            ),
            _ => panic!("expected Strings"),
        }
    }

    #[test]
    fn test_validate_wrong_column_count() {
        let schema2 = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        };
        let schema1 = Schema {
            columns: vec![ColumnDef::new("pk", TypeCode::U64, false)],
            pk_cols: vec![0],
        };
        let batch = ZSetBatch::new(&schema1);
        let err = batch.validate(&schema2).unwrap_err();
        assert!(err.contains("column count"));
    }

    // --- Step 3: BatchAppender tests ---

    #[test]
    fn test_appender_single_row() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("a", TypeCode::U64, false),
                ColumnDef::new("b", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(42u128, 1)
            .u64_val(100)
            .u64_val(200);
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.pks.get(0), 42);
        assert_eq!(batch.weights[0], 1);
        if let ColData::Fixed(buf) = &batch.columns[1] {
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 100);
        } else {
            panic!("expected Fixed");
        }
        if let ColData::Fixed(buf) = &batch.columns[2] {
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 200);
        } else {
            panic!("expected Fixed");
        }
    }

    #[test]
    fn test_appender_multi_row() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            a.add_row(1u128, 1).u64_val(10);
            a.add_row(2u128, 1).u64_val(20);
            a.add_row(3u128, -1).u64_val(30);
        }
        assert_eq!(batch.len(), 3);
        assert_eq!(batch.pks.to_vec_u128(), vec![1u128, 2u128, 3u128]);
        assert_eq!(batch.weights, vec![1, 1, -1]);
        if let ColData::Fixed(buf) = &batch.columns[1] {
            assert_eq!(buf.len(), 24);
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 10);
            assert_eq!(u64::from_le_bytes(buf[8..16].try_into().unwrap()), 20);
            assert_eq!(u64::from_le_bytes(buf[16..24].try_into().unwrap()), 30);
        } else {
            panic!("expected Fixed");
        }
    }

    #[test]
    fn test_appender_string_col() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::U64, false),
                ColumnDef::new("s", TypeCode::String, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(1u128, 1)
            .u64_val(42)
            .str_val("hello");
        assert_eq!(batch.len(), 1);
        if let ColData::Strings(v) = &batch.columns[2] {
            assert_eq!(v[0], Some("hello".to_string()));
        } else {
            panic!("expected Strings");
        }
    }

    #[test]
    fn test_appender_u128_col() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("big", TypeCode::U128, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(1u128, 1)
            .u128_val(((0xBEEF_u128) << 64) | 0xDEAD);
        let ColData::Fixed(b) = &batch.columns[1] else {
            panic!("expected Fixed");
        };
        assert_eq!(b, &(((0xBEEF_u128) << 64) | 0xDEAD).to_le_bytes());
    }

    #[test]
    fn test_appender_mixed_types() {
        // pk(0) + U64 + String + I64 + String
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("a", TypeCode::U64, false),
                ColumnDef::new("b", TypeCode::String, false),
                ColumnDef::new("c", TypeCode::I64, false),
                ColumnDef::new("d", TypeCode::String, true),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(1u128, 1)
            .u64_val(100)
            .str_val("hello")
            .i64_val(-5)
            .str_val("world");
        assert_eq!(batch.len(), 1);
        if let ColData::Fixed(buf) = &batch.columns[1] {
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 100);
        } else {
            panic!("expected Fixed for col 1");
        }
        if let ColData::Strings(v) = &batch.columns[2] {
            assert_eq!(v[0], Some("hello".to_string()));
        } else {
            panic!("expected Strings for col 2");
        }
        if let ColData::Fixed(buf) = &batch.columns[3] {
            assert_eq!(i64::from_le_bytes(buf[0..8].try_into().unwrap()), -5);
        } else {
            panic!("expected Fixed for col 3");
        }
        if let ColData::Strings(v) = &batch.columns[4] {
            assert_eq!(v[0], Some("world".to_string()));
        } else {
            panic!("expected Strings for col 4");
        }
    }

    #[test]
    fn test_appender_pk_not_at_zero() {
        // pk_index=2: columns [A(0), B(1), PK(2), C(3)]
        let schema = Schema {
            columns: vec![
                ColumnDef::new("a", TypeCode::U64, false),
                ColumnDef::new("b", TypeCode::U64, false),
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("c", TypeCode::U64, false),
            ],
            pk_cols: vec![2],
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(99u128, 1)
            .u64_val(10) // cursor 0 -> ci 0 (A)
            .u64_val(20) // cursor 1 -> ci 1 (B)
            .u64_val(30); // cursor 2 -> ci 3 (C), skips pk_index=2

        assert_eq!(batch.pks.get(0), 99);
        if let ColData::Fixed(buf) = &batch.columns[0] {
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 10);
        } else {
            panic!("expected Fixed for col 0");
        }
        if let ColData::Fixed(buf) = &batch.columns[1] {
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 20);
        } else {
            panic!("expected Fixed for col 1");
        }
        if let ColData::Fixed(buf) = &batch.columns[2] {
            assert!(buf.is_empty(), "PK column should be empty placeholder");
        }
        if let ColData::Fixed(buf) = &batch.columns[3] {
            assert_eq!(u64::from_le_bytes(buf[0..8].try_into().unwrap()), 30);
        } else {
            panic!("expected Fixed for col 3");
        }
    }

    // --- Step 4: Type-mismatch panics ---

    fn kv_schema(v: TypeCode) -> Schema {
        Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", v, false),
            ],
            pk_cols: vec![0],
        }
    }

    /// A fixed-width value written into a German-string column panics before the
    /// region can go out of shape — one message for `u64_val`/`i64_val`/`u128_val`,
    /// since they share one body.
    #[test]
    #[should_panic(expected = "fixed value written to String column")]
    fn a_fixed_value_in_a_string_column_panics() {
        let schema = kv_schema(TypeCode::String);
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).u64_val(42);
    }

    #[test]
    #[should_panic(expected = "str_val called on non-Strings")]
    fn a_string_in_a_fixed_column_panics() {
        let schema = kv_schema(TypeCode::U64);
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(1u128, 1)
            .str_val("oops");
    }

    /// A 16-byte write into an 8-byte column shares the `Fixed` variant, so only
    /// the declared stride can catch it — and it is caught at the write, not
    /// deferred to `validate`'s region-length check.
    #[test]
    #[should_panic(expected = "U64 column at schema index 1 takes 8 bytes")]
    fn a_wide_value_in_a_narrow_column_panics() {
        let schema = kv_schema(TypeCode::U64);
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).u128_val(1);
    }

    #[test]
    fn test_appender_then_validate() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::I64, false),
                ColumnDef::new("s", TypeCode::String, true),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            a.add_row(1u128, 1).i64_val(10).str_val("hello");
            a.add_row(2u128, -1).i64_val(20).null();
        }
        assert!(batch.validate(&schema).is_ok());
    }

    // --- Site C: PkTuple::from_bytes ---

    #[test]
    fn pk_tuple_from_bytes_max_accepts() {
        let bytes = vec![0xabu8; MAX_PK_BYTES];
        let t = PkTuple::from_bytes(&bytes);
        assert_eq!(t.stride as usize, MAX_PK_BYTES);
    }

    #[test]
    #[should_panic(expected = "PkTuple::from_bytes: length")]
    fn pk_tuple_from_bytes_over_panics() {
        PkTuple::from_bytes(&[0u8; MAX_PK_BYTES + 1]);
    }

    // --- `null()` sets the null bitmap bit ---

    fn nullable_str_blob_schema() -> Schema {
        Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("s", TypeCode::String, true),
                ColumnDef::new("b", TypeCode::Blob, true),
            ],
            pk_cols: vec![0],
        }
    }

    #[test]
    fn null_sets_the_bitmap_bit() {
        let schema = nullable_str_blob_schema();
        // payload_idx(col 1 = String) = 0 → bit 0; payload_idx(col 2 = Blob) = 1 → bit 1
        let str_bit = 1u64 << schema.payload_idx(1);
        let blob_bit = 1u64 << schema.payload_idx(2);
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            a.add_row(1, 1).null().null();
        }
        assert_eq!(batch.nulls[0], str_bit | blob_bit, "both null bits must be set");
        assert!(batch.validate(&schema).is_ok());
    }

    #[test]
    fn a_null_cell_round_trips_as_null() {
        use crate::protocol::wal_block::{decode_wal_block_verified, encode_wal_block};
        let schema = nullable_str_blob_schema();
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            // No null_mask call — `null()` must be self-sufficient.
            a.add_row(42, 1).null().null();
        }
        let encoded = encode_wal_block(&schema, 1, &batch);
        let (decoded, _) = decode_wal_block_verified(&encoded, &schema).unwrap();
        assert_eq!(decoded.nulls[0], batch.nulls[0], "null bitmap round-trips");
        // The read side gates on the bitmap, so both columns must decode as NULL.
        assert_eq!(
            &decoded.columns[1],
            &ColData::Strings(vec![None]),
            "String must decode as NULL"
        );
        assert_eq!(
            &decoded.columns[2],
            &ColData::Bytes(vec![None]),
            "Blob must decode as NULL"
        );
    }

    // --- §5.2: add_row under-push tripwire ---

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "BatchAppender::add_row: previous row got")]
    fn add_row_under_push_trips_tripwire() {
        let schema = nullable_str_blob_schema(); // 2 payload columns
        let mut batch = ZSetBatch::new(&schema);
        let mut a = BatchAppender::new(&mut batch, &schema);
        a.add_row(1, 1).null(); // only 1 of 2 payload cols pushed
        a.add_row(2, 1); // should panic: previous row incomplete
    }

    #[test]
    fn add_row_over_populated_batch_no_false_trip() {
        // Build a batch first, then attach a fresh appender; the first add_row
        // must not trip the under-push debug_assert (row_active starts false).
        let schema = nullable_str_blob_schema();
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            a.add_row(1, 1).str_val("x").bytes_val(b"y");
        }
        // Fresh appender over the already-populated batch.
        let mut a2 = BatchAppender::new(&mut batch, &schema);
        a2.add_row(2, 1).str_val("z").bytes_val(b"w"); // must not panic
        assert_eq!(batch.pks.len(), 2);
    }
}
