use super::error::ProtocolError;

pub use gnitz_wire::{ColType, FixedInt, PkBuf, ReduceOutKey, ScalarKind, TypeCode};
pub use gnitz_wire::{MAX_COLUMNS, MAX_PK_BYTES, PK_LIST_MAX_COLS};

/// Convert a u64 wire value to TypeCode, returning an error for unknown codes.
/// Use at wire/network boundaries; internal data should use `TypeCode::from_validated_u8`.
pub fn type_code_from_u64(v: u64) -> Result<TypeCode, ProtocolError> {
    if v > u8::MAX as u64 {
        return Err(ProtocolError::UnknownTypeCode(v));
    }
    TypeCode::try_from_u8(v as u8).ok_or(ProtocolError::UnknownTypeCode(v))
}

/// The relation and column a FOREIGN KEY column references. `SelfTable` is the
/// binding `CREATE TABLE` must defer — the id exists only once the table does —
/// which [`ColumnDef::col_tab_row`] resolves to the owner; a schema that came
/// back from a resolve always carries `Table`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FkTarget {
    Table { id: u64, col: u32 },
    SelfTable { col: u32 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef {
    pub name: std::string::String,
    pub type_code: TypeCode,
    pub is_nullable: bool,
    /// The FOREIGN KEY this column carries, if any.
    pub fk: Option<FkTarget>,
    /// True for a Postgres-style SERIAL/BIGSERIAL/SMALLSERIAL primary key: an
    /// auto-assigned, client-stamped id the user may not supply. Round-trips
    /// through the wire meta-schema (`ColMeta::serial`) and `COL_TAB`, so a
    /// connection that only resolved the relation can still distinguish it from
    /// a user-supplied non-null integer PK. The engine stores the marker but has
    /// no SERIAL awareness.
    pub is_serial: bool,
    /// True for a hidden key slot — a physical schema column carrying a real
    /// PK/routing value (a synthetic view key like `_join_pk`/`_group_pk`, or an
    /// unprojected passthrough source PK) that no presentation surface exposes.
    /// Round-trips through the wire meta-schema (`ColMeta::hidden`) and
    /// `COL_TAB`. Presentation layers (wildcard expansion, name resolution,
    /// duplicate-name checks, client rows) skip it; physical layout, routing,
    /// sort, and consolidation are unaffected. A base-table column becomes hidden
    /// only via `ALTER TABLE … DROP COLUMN` (a logical drop): it stays physically
    /// present and zero-filled NOT NULL, and is excluded from every name-facing
    /// surface.
    pub is_hidden: bool,
    /// A DECIMAL column's scale — the power of ten its stored `I64` is
    /// multiplied by — and zero for every other type. Round-trips through the
    /// wire meta-schema's scale bits and `COL_TAB.scale`; the engine stores it
    /// and never branches on it.
    pub scale: u8,
}

impl ColumnDef {
    /// A non-FK, non-SERIAL column — the common case. Client-side schema builders
    /// (the SQL planner, the Python driver) synthesize columns through here; the
    /// planner's FK path fills `fk` once the referenced table resolves, and a
    /// SERIAL column chains [`ColumnDef::serial`].
    pub fn new(name: impl Into<String>, type_code: TypeCode, is_nullable: bool) -> Self {
        Self::typed(name, ColType::of(type_code), is_nullable)
    }

    /// [`ColumnDef::new`] from a logical type, which is how a DECIMAL column,
    /// or a column declared from a computed expression, states its scale.
    pub fn typed(name: impl Into<String>, ty: ColType, is_nullable: bool) -> Self {
        Self {
            name: name.into(),
            type_code: ty.tc,
            is_nullable,
            fk: None,
            is_serial: false,
            is_hidden: false,
            scale: ty.scale,
        }
    }

    /// The column's logical type: its code and scale together.
    pub fn ty(&self) -> ColType {
        ColType { tc: self.type_code, scale: self.scale }
    }

    /// Retype a column in place, keeping every other fact about it.
    pub fn set_ty(&mut self, ty: ColType) {
        self.type_code = ty.tc;
        self.scale = ty.scale;
    }

    /// This column as the `COL_TAB` row recording it: column `col_idx` of
    /// `owner_id`, whose kind is `owner_kind`.
    ///
    /// The FK is resolved against the owner rather than taken verbatim:
    /// [`FkTarget::SelfTable`] becomes `owner_id`, and a non-table owner writes
    /// no FK — a view's defs are clones of the source columns, and the constraint
    /// belongs to the base table. The wire's `0` = "no FK" convention starts
    /// here.
    pub fn col_tab_row(&self, owner_id: u64, owner_kind: u64, col_idx: usize) -> gnitz_wire::sys_rows::ColTabRow<'_> {
        let (fk_table_id, fk_col_idx) = match self.fk {
            _ if owner_kind != gnitz_wire::OWNER_KIND_TABLE => (0, 0),
            None => (0, 0),
            Some(FkTarget::SelfTable { col }) => (owner_id, col as u64),
            Some(FkTarget::Table { id, col }) => (id, col as u64),
        };
        gnitz_wire::sys_rows::ColTabRow {
            owner_id,
            owner_kind,
            col_idx: col_idx as u64,
            name: &self.name,
            type_code: self.type_code as u64,
            is_nullable: self.is_nullable,
            fk_table_id,
            fk_col_idx,
            is_serial: self.is_serial,
            is_hidden: self.is_hidden,
            scale: self.scale,
        }
    }

    /// Mark this column a SERIAL primary key — an auto-assigned, client-stamped
    /// id. Chains onto [`ColumnDef::new`]; the CREATE TABLE planner is the only
    /// builder of SERIAL columns.
    pub fn serial(mut self) -> Self {
        self.is_serial = true;
        self
    }

    /// Mark this column hidden (see [`ColumnDef::is_hidden`]) — a slot the planner
    /// fabricated, which no name the user wrote may reach. Chains onto
    /// [`ColumnDef::new`].
    pub fn hidden(mut self) -> Self {
        self.is_hidden = true;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    pub columns: Vec<ColumnDef>,
    /// PK column indices in compound-key order; length >= 1. `u32` because that
    /// is what every consumer takes — the wire validators, `ReduceOutKey`, the
    /// PK-list packer, `create_table`.
    pub pk_cols: Vec<u32>,
}

impl Schema {
    /// Number of logical columns in this schema (PK + payload).
    #[inline]
    pub fn num_columns(&self) -> usize {
        self.columns.len()
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
            .map(|&ci| self.columns[ci as usize].type_code.wire_stride())
            .sum()
    }

    /// The lone PK column's index, or `None` for a compound key. Total, so a
    /// caller cannot reach the first of several PK columns by mistake.
    #[inline]
    pub fn pk_index_single(&self) -> Option<u32> {
        match self.pk_cols[..] {
            [ci] => Some(ci),
            _ => None,
        }
    }

    /// Number of non-PK ("payload") columns: `columns.len() - pk_count`.
    #[inline]
    pub fn num_payload_cols(&self) -> usize {
        self.num_columns() - self.pk_cols.len()
    }

    /// True iff column `ci` is a PK column. Total: every PK index is in range,
    /// so an out-of-range `ci` matches none of them.
    #[inline]
    pub fn is_pk_col(&self, ci: usize) -> bool {
        // Widen the stored index rather than narrowing `ci`: `ci as u32` would
        // truncate a large index onto a real PK column.
        self.pk_cols.iter().any(|&p| p as usize == ci)
    }

    /// Byte offset of PK column `col_idx` within the packed PK region — the
    /// running sum [`Self::locate`]'s `Pk` arm reports as `byte_off`.
    #[inline]
    pub fn pk_byte_offset(&self, col_idx: usize) -> usize {
        debug_assert!(self.is_pk_col(col_idx));
        self.pk_cols
            .iter()
            .take_while(|&&pi| pi as usize != col_idx)
            .map(|&pi| self.columns[pi as usize].type_code.wire_stride())
            .sum()
    }

    /// Per-PK-column `(wire_stride, type_code)` in compound-key order, for the
    /// OPK encode/decode column walk. Collect once and reuse across rows so the
    /// per-row loop never re-iterates the schema.
    #[inline]
    pub fn pk_col_codes(&self) -> impl Iterator<Item = (usize, u8)> + '_ {
        self.pk_cols.iter().map(move |&ci| {
            let tc = self.columns[ci as usize].type_code;
            (tc.wire_stride(), tc as u8)
        })
    }

    /// Map a logical column index to its dense payload index. Caller must
    /// ensure `col_idx` is not the PK column.
    #[inline]
    pub fn payload_idx(&self, col_idx: usize) -> usize {
        debug_assert!(!self.is_pk_col(col_idx), "payload_idx: col_idx must not be a PK column");
        col_idx - self.pk_cols.iter().filter(|&&p| (p as usize) < col_idx).count()
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

    /// True iff any **non-PK** column is hidden. A view's synthetic hidden keys
    /// are PK columns, so they never count.
    #[inline]
    pub fn has_hidden_payload(&self) -> bool {
        (0..self.columns.len()).any(|i| self.is_hidden_payload(i))
    }

    /// Whether column `i` is a hidden **payload** column. A wildcard *expansion*
    /// drops every hidden column, PK included; the raw passthrough this gates
    /// keeps a hidden PK column, which carries the batch's key. So the two agree
    /// on every payload slot, and differ only there.
    #[inline]
    pub fn is_hidden_payload(&self, i: usize) -> bool {
        self.columns[i].is_hidden && !self.is_pk_col(i)
    }

    /// The output-key kind a reduce or top-N grouped by `group` gets (the engine
    /// derives the same), and `group` in output-key order: sharding by it lands
    /// each output row on the worker owning its PK.
    pub fn reduce_key(&self, group: &[u32]) -> (ReduceOutKey, Vec<u32>) {
        let key = ReduceOutKey::for_group_cols(&self.pk_cols, group, |c| {
            let cd = &self.columns[c as usize];
            (cd.type_code as u8, cd.is_nullable)
        });
        (key, key.key_region(&self.pk_cols, group).unwrap_or(group).to_vec())
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
            return Err(format!(
                "column count {} exceeds MAX_COLUMNS ({MAX_COLUMNS})",
                columns.len()
            ));
        }
        if let Some(cd) = columns.iter().find(|cd| {
            cd.scale > gnitz_wire::decimal::MAX_DECIMAL_SCALE || (cd.scale != 0 && cd.type_code != TypeCode::Decimal)
        }) {
            return Err(format!(
                "column '{}' ({:?}) carries scale {}",
                cd.name, cd.type_code, cd.scale
            ));
        }
        gnitz_wire::validate_pk_tuple(pk_cols, columns.len(), |c| {
            let cd = &columns[c as usize];
            (cd.type_code as u8, cd.is_nullable)
        })
        .map(|_stride| ())
        .map_err(|r| r.to_string())
    }

    /// Fallible constructor for a schema assembled from untrusted parts — a
    /// wire schema block or catalog rows. Runs [`Schema::validate_parts`],
    /// so every decode boundary applies the same rule set.
    pub fn from_parts(columns: Vec<ColumnDef>, pk_cols: Vec<u32>) -> Result<Schema, String> {
        Self::validate_parts(&pk_cols, &columns)?;
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

/// One row's PK as OPK bytes, from the PK columns' native values in PK-list
/// order — the one native→OPK column walk on the client, and the same bytes a
/// [`PkColumn`] row holds. Crossing into the wire's *native* key space is named:
/// this and [`opk_key_packed`] in, [`native_le_key`] out.
pub fn opk_key_cols(schema: &Schema, natives: impl IntoIterator<Item = u128>) -> PkBuf {
    let key = gnitz_wire::encode_pk_natives(schema.pk_col_codes().map(|(_, tc)| (tc, tc)), natives);
    debug_assert_eq!(key.width(), schema.pk_stride(), "opk_key_cols: one value per PK column");
    key
}

/// [`opk_key_cols`] for a key already packed as its columns' native
/// little-endian bytes.
pub fn opk_key_native_bytes(schema: &Schema, native_le: &[u8]) -> PkBuf {
    gnitz_wire::encode_pk_tuple(schema.pk_col_codes(), native_le)
}

/// [`opk_key_native_bytes`] from a u128 whose low `pk_stride` bytes carry the PK
/// columns' native images — a parsed PK literal.
pub fn opk_key_packed(schema: &Schema, v: u128) -> PkBuf {
    let stride = schema.pk_stride();
    debug_assert!(stride <= 16);
    opk_key_native_bytes(schema, &v.to_le_bytes()[..stride])
}

/// `key`, one row of OPK bytes, in the wire's native key space: its columns
/// decoded back to their native little-endian images, at the same offsets. The
/// inverse of [`opk_key_packed`]; valid bytes are `0..key.len()`.
pub fn native_le_key(schema: &Schema, key: &[u8]) -> [u8; MAX_PK_BYTES] {
    let mut buf = [0u8; MAX_PK_BYTES];
    let mut off = 0;
    for (w, tc) in schema.pk_col_codes() {
        gnitz_wire::decode_pk_column(&key[off..off + w], tc, &mut buf[off..off + w]);
        off += w;
    }
    debug_assert_eq!(off, key.len(), "native_le_key: schema stride != key width");
    buf
}

/// A batch's PK region: `stride` bytes per row of **order-preserving key** (OPK,
/// §4) — the bytes the wire carries and the engine stores, so a client batch's
/// region and a `BatchBuilder`'s are byte-identical.
///
/// Private fields: routing hashes these bytes, so an un-encoded write would
/// mis-partition rather than error. Every append encodes or copies OPK.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PkColumn {
    stride: u8,
    buf: Vec<u8>,
}

impl PkColumn {
    /// Empty `PkColumn` matching `schema`'s PK layout — the only constructor, so
    /// the stride is never independent data to keep in sync.
    pub fn empty_for_schema(schema: &Schema) -> Self {
        PkColumn {
            stride: schema.pk_stride() as u8,
            buf: vec![],
        }
    }

    /// A column of `schema`'s keys from their native packed values. The
    /// counterpart of [`Self::get`].
    pub fn from_natives(schema: &Schema, vals: impl IntoIterator<Item = u128>) -> Self {
        let mut c = Self::empty_for_schema(schema);
        for v in vals {
            c.push_u128(schema, v);
        }
        c
    }

    /// Bytes per row.
    #[inline]
    pub fn stride(&self) -> u8 {
        self.stride
    }

    fn width(&self) -> usize {
        self.stride as usize
    }

    /// The whole §6 PK region: `len()` rows of `stride` OPK bytes.
    #[inline]
    pub fn region(&self) -> &[u8] {
        &self.buf
    }

    pub fn len(&self) -> usize {
        self.buf.len() / self.width()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Row `i`'s columns decoded and packed back into one native value. Defined
    /// only for a key that fits in 16 bytes; a wider one is read as bytes.
    pub fn get(&self, schema: &Schema, i: usize) -> u128 {
        let opk = self.get_bytes(i);
        assert!(
            opk.len() <= gnitz_wire::NARROW_PK_MAX_BYTES,
            "PkColumn::get: a {}-byte key has no scalar form",
            opk.len(),
        );
        let native = native_le_key(schema, opk);
        u128::from_le_bytes(native[..gnitz_wire::NARROW_PK_MAX_BYTES].try_into().unwrap())
    }

    /// Borrow row `i`'s `stride` OPK bytes.
    pub fn get_bytes(&self, i: usize) -> &[u8] {
        let s = self.width();
        &self.buf[i * s..(i + 1) * s]
    }

    /// Room for `n` more rows.
    pub fn reserve(&mut self, n: usize) {
        self.buf.reserve(n * self.width());
    }

    /// Append a key whose low `stride` bytes carry the PK columns' native
    /// little-endian images.
    pub fn push_u128(&mut self, schema: &Schema, pk: u128) {
        let s = self.width();
        // Hard, not debug-only: in release the slice below would OOB-panic with
        // an opaque "index out of range".
        assert!(s <= 16, "push_u128: stride {s} > 16 cannot come from a u128");
        self.push_bytes(schema, &pk.to_le_bytes()[..s]);
    }

    /// Append one row given as its `stride` native little-endian column bytes.
    pub fn push_bytes(&mut self, schema: &Schema, native_le: &[u8]) {
        debug_assert_eq!(native_le.len(), self.width());
        self.push_region_bytes(opk_key_native_bytes(schema, native_le).pk_bytes());
    }

    /// Append one row from the PK columns' native values in PK-list order.
    pub fn push_natives(&mut self, schema: &Schema, natives: &[u128]) {
        self.push_region_bytes(opk_key_cols(schema, natives.iter().copied()).pk_bytes());
    }

    /// Append whole OPK rows verbatim — `opk` is a multiple of `stride` bytes
    /// already in region form.
    pub fn push_region_bytes(&mut self, opk: &[u8]) {
        debug_assert!(
            opk.len().is_multiple_of(self.width()),
            "push_region_bytes: {} bytes is not a whole number of {}-byte rows",
            opk.len(),
            self.width(),
        );
        self.buf.extend_from_slice(opk);
    }

    /// Move every row of `other` onto this column's tail.
    pub(crate) fn append(&mut self, other: &mut PkColumn) {
        debug_assert_eq!(self.stride, other.stride);
        self.buf.append(&mut other.buf);
    }

    fn truncate(&mut self, len: usize) {
        self.buf.truncate(len * self.width());
    }

    /// Read row `i` into a [`PkBuf`] — a verbatim byte move, both being OPK. A
    /// caller only *looking a key up* passes [`Self::get_bytes`] straight to the
    /// map instead: `PkBuf` borrows as `[u8]`.
    pub fn get_tuple(&self, i: usize) -> PkBuf {
        PkBuf::from_bytes(self.get_bytes(i))
    }

    /// Append the row at `src[i]` to `self`. Strides must match.
    pub fn push_from(&mut self, src: &PkColumn, i: usize) {
        debug_assert_eq!(self.stride, src.stride);
        self.buf.extend_from_slice(src.get_bytes(i));
    }

    /// Every key decoded, for `assert_eq!(pks.to_vec_u128(schema), expected)`.
    #[cfg(test)]
    pub fn to_vec_u128(&self, schema: &Schema) -> Vec<u128> {
        (0..self.len()).map(|i| self.get(schema, i)).collect()
    }
}

/// Append one zero-filled cell of wire type `tc` to a payload region — the NULL
/// encoding and the non-null filler alike. The null bitmap is the NULL truth
/// (§6), and a zeroed German cell *is* the empty value, which is what
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

/// A batch in the §6 region shape: every payload slot is its own wire region,
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
                .map(|(_, _, c)| PayloadColumn::new(c.type_code))
                .collect(),
            blob: vec![],
        }
    }

    /// One region per payload slot, zero-filled for `count` rows. The client's
    /// `delete` uses these as inert payload filler for retraction rows, which
    /// the server's `retract_pk` matches by PK alone.
    pub(crate) fn filler_columns(schema: &Schema, count: usize) -> Vec<PayloadColumn> {
        schema
            .payload_columns()
            .map(|(_, _, c)| PayloadColumn::zeroed(c.type_code, count))
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
        if gnitz_wire::is_german_string(tc as u8) {
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

    /// Same PK stride and the same payload type list.
    fn same_layout(&self, other: &ZSetBatch) -> bool {
        self.pks.stride() == other.pks.stride()
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

    /// The index of the live row whose PK is `pk`, for a batch keyed by a single
    /// integer column (the catalog tables). `None` = no such live row.
    pub fn live_row_with_pk(&self, schema: &Schema, pk: u64) -> Option<usize> {
        self.live_rows().find(|&i| self.pks.get(schema, i) as u64 == pk)
    }

    /// Append all rows of `other`, consuming it: each region concatenates, and
    /// `other`'s arena lands on this one's tail, so every German cell it carries
    /// has its heap offset shifted by that much.
    pub fn extend_from_owned(&mut self, mut other: ZSetBatch) {
        assert_eq!(
            self.pks.stride(),
            other.pks.stride(),
            "extend_from_owned: PK stride mismatch",
        );
        assert!(self.same_layout(&other), "extend_from_owned: payload layout mismatch",);
        self.pks.append(&mut other.pks);
        self.weights.append(&mut other.weights);
        self.nulls.append(&mut other.nulls);
        let delta = self.blob.len();
        self.blob.append(&mut other.blob);
        for (dst, src) in self.payload.iter_mut().zip(&mut other.payload) {
            let at = dst.bytes.len();
            dst.bytes.append(&mut src.bytes);
            if gnitz_wire::is_german_string(dst.tc() as u8) && delta != 0 {
                for cell in dst.bytes[at..].as_chunks_mut::<16>().0 {
                    gnitz_wire::shift_german_string_heap(cell, delta);
                }
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
        let ps = self.pks.width();
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
    /// Split out of [`Self::validate`] because the region builder needs the same
    /// rule for a batch that never went through the push path.
    pub(crate) fn check_columns(&self) -> Result<(), std::string::String> {
        let n = self.len();
        for (pi, col) in self.payload.iter().enumerate() {
            let (got, want) = (col.bytes.len(), n * col.stride());
            if got != want {
                return Err(format!("payload slot {pi}: length {got} != expected {want}"));
            }
        }
        Ok(())
    }

    /// The batch's layout is `schema`'s: PK stride, payload slot count, and each
    /// slot's type.
    pub fn layout_matches(&self, schema: &Schema) -> Result<(), std::string::String> {
        if self.pks.stride() as usize != schema.pk_stride() {
            return Err(format!(
                "mismatched PK stride: expected {}, got {}",
                schema.pk_stride(),
                self.pks.stride()
            ));
        }
        if self.payload.len() != schema.num_payload_cols() {
            return Err(format!(
                "payload slot count {} != schema payload column count {}",
                self.payload.len(),
                schema.num_payload_cols()
            ));
        }
        for ((pi, _, def), col) in schema.payload_columns().zip(&self.payload) {
            if col.tc() != def.type_code {
                return Err(format!(
                    "payload slot {pi} ('{}'): type {:?} != schema type {:?}",
                    def.name,
                    col.tc(),
                    def.type_code
                ));
            }
        }
        Ok(())
    }

    /// Validate that all vectors are consistently sized for the given schema, and
    /// that no NULL sits under a NOT NULL column.
    pub fn validate(&self, schema: &Schema) -> Result<(), std::string::String> {
        // The PK buffer's own shape, checked before `PkColumn::len` divides by
        // the stride. A stride that disagrees with the schema would make every
        // row read at the wrong offset.
        if self.pks.stride() == 0 {
            return Err("PK stride must be non-zero".into());
        }
        self.layout_matches(schema)?;
        if !self.pks.region().len().is_multiple_of(self.pks.stride() as usize) {
            return Err(format!(
                "PK buffer length {} is not a multiple of stride {}",
                self.pks.region().len(),
                self.pks.stride()
            ));
        }
        let n = self.pks.len();
        if self.weights.len() != n {
            return Err(format!("weights length {} != row count {}", self.weights.len(), n));
        }
        if self.nulls.len() != n {
            return Err(format!("nulls length {} != row count {}", self.nulls.len(), n));
        }
        self.check_columns()?;
        check_not_null(&self.nulls, schema)
    }

    /// The rows `rows` names, in order, at their paired weights; each row at most once.
    /// When every row survives the arena moves whole, else only survivors' strings are copied.
    pub fn gather(self, rows: &[(usize, i64)]) -> ZSetBatch {
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
            stride: self.pks.stride,
            buf: Vec::with_capacity(n * self.pks.width()),
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
                if !whole && gnitz_wire::is_german_string(src.tc as u8) {
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
    let not_null_mask = gnitz_expr::SchemaFacts::not_null_payload_slots(schema);
    if nulls.iter().fold(0u64, |a, &w| a | w) & not_null_mask == 0 {
        return Ok(());
    }
    let (row, offending) = nulls
        .iter()
        .enumerate()
        .map(|(row, &w)| (row, w & not_null_mask))
        .find(|&(_, o)| o != 0)
        .expect("the fold found a NOT NULL bit set");
    let pi = offending.trailing_zeros() as usize;
    let name = schema
        .payload_columns()
        .find(|(p, _, _)| *p == pi)
        .map_or("?", |(_, _, c)| c.name.as_str());
    Err(format!("row {row} sets a null bit on NOT NULL column '{name}'"))
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
    schema: &'a Schema,
    cursor: usize,
}

/// The client half of the shared catalog row codecs: the sink
/// `gnitz_wire::sys_rows` writes a system-table row into.
impl gnitz_wire::sys_rows::SysRowSink for BatchAppender<'_> {
    fn begin_row(&mut self, pk: &[u128], weight: i64) {
        self.add_row_cols(pk, weight);
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
    pub fn new(batch: &'a mut ZSetBatch, schema: &'a Schema) -> Self {
        BatchAppender { batch, schema, cursor: 0 }
    }

    /// Start a new row with the given single-column primary key and weight.
    pub fn add_row(&mut self, pk: u128, weight: i64) -> &mut Self {
        self.open_row(weight);
        self.batch.pks.push_u128(self.schema, pk);
        self
    }

    /// [`Self::add_row`] for a **compound** PK: `natives` are the PK columns'
    /// native values in PK-list order.
    pub fn add_row_cols(&mut self, natives: &[u128], weight: i64) -> &mut Self {
        self.open_row(weight);
        self.batch.pks.push_natives(self.schema, natives);
        self
    }

    /// A row takes exactly one push per payload column — the `SysRowSink`
    /// contract's `end_row`, where an under-pushed row would otherwise desync the
    /// column regions and surface later as an un-attributed `validate` length
    /// error. A `debug_assert`, since `validate` rejects the batch safely
    /// anyway; this only attributes it.
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

    /// Append one fixed-width cell to the next column: `bytes` is the column's
    /// whole wire region entry, so its length must be the declared stride —
    /// otherwise the region ends up the wrong size and every later row reads at
    /// the wrong offset.
    fn fixed_val(&mut self, bytes: &[u8]) -> &mut Self {
        let pi = self.col_index();
        let col = &mut self.batch.payload[pi];
        let tc = col.tc();
        assert!(
            !gnitz_wire::is_german_string(tc as u8),
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

    /// Append a u64 value to the next column.
    pub fn u64_val(&mut self, v: u64) -> &mut Self {
        self.fixed_val(&v.to_le_bytes())
    }

    /// Append an i64 value to the next column. Same eight bytes as
    /// [`Self::u64_val`]; the separate name keeps a signed column's writer
    /// honest at the call site.
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
        self.german_val(s.as_bytes())
    }

    /// Append a raw byte slice to the next BLOB column.
    pub fn bytes_val(&mut self, b: &[u8]) -> &mut Self {
        self.german_val(b)
    }

    /// Append one German-string cell, spilling into the batch's arena.
    fn german_val(&mut self, b: &[u8]) -> &mut Self {
        let pi = self.col_index();
        let tc = self.batch.payload[pi].tc();
        assert!(
            gnitz_wire::is_german_string(tc as u8),
            "BatchAppender: a string/blob value cannot be written to the {tc:?} column at payload slot {pi}",
        );
        let cell = gnitz_wire::encode_german_string(b, &mut self.batch.blob);
        self.batch.payload[pi].bytes.extend_from_slice(&cell);
        self.cursor += 1;
        self
    }

    /// Append a SQL NULL to the next column, whatever its type: a zeroed cell
    /// plus the row bitmap bit. Self-sufficient — no out-of-band `null_mask`
    /// call.
    ///
    /// The read side gates on `nulls[row] & (1 << payload_idx)` and reads the
    /// cell only when that bit is clear, so the two must agree.
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

#[cfg(test)]
#[path = "tests/types.rs"]
mod tests;
