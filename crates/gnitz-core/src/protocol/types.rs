use super::error::ProtocolError;
use std::sync::OnceLock;

pub use gnitz_wire::{FixedInt, ReduceOutKey, TypeCode};
pub use gnitz_wire::{MAX_COLUMNS, MAX_PK_BYTES, MAX_PK_COLUMNS, PK_LIST_MAX_COLS};

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
    /// through `COL_TAB` so a connection that only fetched the schema can still
    /// distinguish it from a user-supplied non-null integer PK. The engine
    /// stores the marker but has no SERIAL awareness.
    pub is_serial: bool,
    /// True for a hidden key slot — a physical schema column carrying a real
    /// PK/routing value (a synthetic view key like `_join_pk`/`_group_pk`, or an
    /// unprojected passthrough source PK) that no presentation surface exposes.
    /// Round-trips through the wire meta-schema (`META_FLAG_HIDDEN`) and
    /// `COL_TAB`. Presentation layers (wildcard expansion, name resolution,
    /// duplicate-name checks, client rows) skip it; physical layout, routing,
    /// sort, and consolidation are unaffected. Base-table columns are never
    /// hidden.
    pub is_hidden: bool,
}

impl ColumnDef {
    /// A non-FK, non-SERIAL column — the common case. Client-side schema builders
    /// (the SQL planner, the C ABI, the Python driver) synthesize columns through
    /// here; the planner's FK path assigns `fk_table_id`/`fk_col_idx` on the
    /// returned column once the referenced table resolves, and a SERIAL column
    /// chains [`ColumnDef::serial`].
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

    /// The single PK column index. Use only at boundaries that have not yet
    /// been generalized (format encoders, catalog serialization, SQL parser
    /// path, wire/client BatchAppender). Hard-asserts length-1: a
    /// `debug_assert!` would compile out in release and let the silent
    /// truncation to the first PK column ship to production.
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
    /// that enumerates through this stays byte-offset-correct. Base-table schemas
    /// have no hidden columns, so this is the full column list there.
    #[inline]
    pub fn visible_columns(&self) -> impl Iterator<Item = (usize, &ColumnDef)> {
        self.columns.iter().enumerate().filter(|(_, c)| !c.is_hidden)
    }

    /// The output-key kind a reduce grouped by `cols` over this schema gets —
    /// the planner-side decision shipped on the wire and validated (never
    /// re-decided) by the engine, both through [`ReduceOutKey::decide`]. A
    /// nullable group column can never key the output (the PK region has no
    /// null bitmap), so it lands on the synthetic fold.
    pub fn reduce_out_key(&self, cols: &[usize]) -> ReduceOutKey {
        let eq_pk = cols.len() == self.pk_cols.len() && self.pk_cols.iter().all(|p| cols.contains(p));
        let single_natural = cols.len() == 1 && {
            let c = &self.columns[cols[0]];
            c.type_code.is_natural_reduce_key() && !c.is_nullable
        };
        ReduceOutKey::decide(eq_pk, single_natural)
    }

    /// Validate a candidate PK index list against the schema's arity and
    /// column count constraints. Shared by all FFI surfaces (capi, py) so the
    /// rules — non-empty, ≤ `PK_LIST_MAX_COLS`, all indices `< ncols`, no
    /// duplicates — stay in one place. `ncols` is passed in because the C ABI
    /// builds the PK list before adding columns. The cap is the persisted
    /// PK-list codec capacity (`PK_LIST_MAX_COLS`), not the wider in-memory
    /// `MAX_PK_COLUMNS`: these surfaces never build the engine-internal
    /// secondary-index schema that uses the extra `MAX_PK_COLUMNS` slot, so a
    /// PK they accept must round-trip through the codec.
    pub fn validate_pk_cols(pk_cols: &[usize], ncols: usize) -> Result<(), &'static str> {
        if pk_cols.is_empty() {
            return Err("pk_cols must not be empty");
        }
        if pk_cols.len() > PK_LIST_MAX_COLS {
            return Err("pk_cols exceeds PK_LIST_MAX_COLS");
        }
        if pk_cols.iter().any(|&c| c >= ncols) {
            return Err("pk_cols index out of range");
        }
        // O(n²) is faster than HashSet for n ≤ 5.
        for i in 0..pk_cols.len() {
            for j in i + 1..pk_cols.len() {
                if pk_cols[i] == pk_cols[j] {
                    return Err("pk_cols contains duplicates");
                }
            }
        }
        Ok(())
    }

    /// The single definition of "these parts form an admissible schema": the
    /// `MAX_COLUMNS` cap (the region null bitmap is one u64 word), the
    /// structural PK rules ([`Schema::validate_pk_cols`] — non-empty,
    /// `≤ PK_LIST_MAX_COLS`, every index `< columns.len()`, no duplicates), and
    /// the per-column invariants the engine's `SchemaDescriptor::new`
    /// hard-asserts — each PK column non-nullable and PK-eligible. Shared by
    /// [`Schema::from_parts`], the client's `create_table` / `create_view_chain`
    /// DDL gateways, and capi's `gnitz_batch_new`, so a malformed spec is a
    /// clean client error rather than a server-side assert.
    pub fn validate_parts(pk_cols: &[usize], columns: &[ColumnDef]) -> Result<(), &'static str> {
        if columns.len() > MAX_COLUMNS {
            return Err("column count exceeds MAX_COLUMNS");
        }
        Self::validate_pk_cols(pk_cols, columns.len())?;
        for &pk in pk_cols {
            if columns[pk].is_nullable {
                return Err("PK column must be non-nullable");
            }
            if !columns[pk].type_code.is_pk_eligible() {
                return Err("PK column type not PK-eligible");
            }
        }
        Ok(())
    }

    /// Fallible constructor for a schema assembled from untrusted parts — a
    /// wire schema block or catalog rows. Runs [`Schema::validate_parts`],
    /// so every decode boundary applies the same rule set.
    pub fn from_parts(columns: Vec<ColumnDef>, pk_cols: Vec<usize>) -> Result<Schema, &'static str> {
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

/// Returns the META_SCHEMA singleton (4 columns: col_idx/U64 pk=0, type_code/U64, flags/U64, name/String).
pub fn meta_schema() -> &'static Schema {
    static INSTANCE: OnceLock<Schema> = OnceLock::new();
    INSTANCE.get_or_init(|| Schema {
        columns: vec![
            ColumnDef::new("col_idx", TypeCode::U64, false),
            ColumnDef::new("type_code", TypeCode::U64, false),
            ColumnDef::new("flags", TypeCode::U64, false),
            ColumnDef::new("name", TypeCode::String, false),
        ],
        pk_cols: vec![0],
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PkColumn {
    U64s(Vec<u64>),
    U128s(Vec<u128>),
    /// Wide compound PK (pk_count >= 2): a flat `stride`-byte-per-row buffer
    /// in on-wire LE layout. Has no scalar u128 projection, so the numeric
    /// accessors panic. These panics are a real precondition (not dead code):
    /// the numeric path is reachable from SQL/binding callers, but the SQL
    /// planner still rejects compound PRIMARY KEY, so no such batch reaches
    /// them today.
    Bytes {
        stride: u8,
        buf: Vec<u8>,
    },
}

impl PkColumn {
    pub fn for_type(tc: TypeCode) -> Self {
        // I128 (a cross-sign `_join_pk`) is a 16-byte key: store its native bits
        // as u128 like U128/UUID; the signed interpretation happens only at the
        // value-surfacing boundary (gnitz-py).
        if tc == TypeCode::U128 || tc == TypeCode::UUID || tc == TypeCode::I128 {
            PkColumn::U128s(vec![])
        } else {
            PkColumn::U64s(vec![])
        }
    }
    pub fn len(&self) -> usize {
        match self {
            PkColumn::U64s(v) => v.len(),
            PkColumn::U128s(v) => v.len(),
            PkColumn::Bytes { stride, buf } => buf.len() / *stride as usize,
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn get(&self, i: usize) -> u128 {
        match self {
            PkColumn::U64s(v) => v[i] as u128,
            PkColumn::U128s(v) => v[i],
            PkColumn::Bytes { .. } => panic!("wide PK has no u128 projection"),
        }
    }
    /// Append one raw PK tuple (already in on-wire LE layout). Wide-PK only.
    pub fn push_bytes(&mut self, b: &[u8]) {
        match self {
            PkColumn::Bytes { stride, buf } => {
                debug_assert_eq!(b.len(), *stride as usize);
                buf.extend_from_slice(b);
            }
            _ => unreachable!("push_bytes on non-wide PK column"),
        }
    }
    /// Borrow the raw `stride`-byte tuple at row `i`. Wide-PK only.
    pub fn get_bytes(&self, i: usize) -> &[u8] {
        match self {
            PkColumn::Bytes { stride, buf } => {
                let s = *stride as usize;
                &buf[i * s..(i + 1) * s]
            }
            _ => unreachable!("get_bytes on non-wide PK column"),
        }
    }
    pub fn push_u128(&mut self, pk: u128) {
        match self {
            PkColumn::U64s(v) => v.push(pk as u64),
            PkColumn::U128s(v) => v.push(pk),
            // Compound PK ≤16 bytes packed into a u128 (low `stride` bytes hold
            // the LE-packed PK columns in order). Used by the catalog circuit
            // tables whose PK is (view_id, sub).
            PkColumn::Bytes { stride, buf } => {
                let s = *stride as usize;
                // Promoted from debug_assert to a full assert: in release the
                // slice `pk.to_le_bytes()[..s]` would otherwise OOB-panic with an
                // opaque "index out of range" message. This Bytes arm is reached
                // only by cold catalog circuit-table writes, so the branch cost is
                // irrelevant; a clear, attributable failure is worth it.
                assert!(s <= 16, "push_u128: stride {s} > 16 cannot come from a u128");
                buf.extend_from_slice(&pk.to_le_bytes()[..s]);
            }
        }
    }
    pub fn set_u128(&mut self, i: usize, pk: u128) {
        match self {
            PkColumn::U64s(v) => v[i] = pk as u64,
            PkColumn::U128s(v) => v[i] = pk,
            PkColumn::Bytes { .. } => panic!("wide PK has no u128 projection"),
        }
    }
    pub fn swap(&mut self, i: usize, j: usize) {
        match self {
            PkColumn::U64s(v) => v.swap(i, j),
            PkColumn::U128s(v) => v.swap(i, j),
            PkColumn::Bytes { stride, buf } => {
                if i == j {
                    return;
                }
                let s = *stride as usize;
                let (lo, hi) = if i < j { (i, j) } else { (j, i) };
                let (left, right) = buf.split_at_mut(hi * s);
                left[lo * s..lo * s + s].swap_with_slice(&mut right[..s]);
            }
        }
    }
    pub fn clear(&mut self) {
        match self {
            PkColumn::U64s(v) => v.clear(),
            PkColumn::U128s(v) => v.clear(),
            PkColumn::Bytes { buf, .. } => buf.clear(),
        }
    }
    pub fn truncate(&mut self, len: usize) {
        match self {
            PkColumn::U64s(v) => v.truncate(len),
            PkColumn::U128s(v) => v.truncate(len),
            PkColumn::Bytes { stride, buf } => buf.truncate(len * *stride as usize),
        }
    }
    pub fn to_vec_u128(&self) -> Vec<u128> {
        match self {
            PkColumn::U64s(v) => v.iter().map(|&x| x as u128).collect(),
            PkColumn::U128s(v) => v.clone(),
            PkColumn::Bytes { .. } => panic!("wide PK has no u128 projection"),
        }
    }

    /// Empty `PkColumn` matching `schema`'s PK layout. Single source of
    /// truth for the variant choice; `ZSetBatch::new`, `GnitzClient::delete`,
    /// and the SQL DML helpers all route through this.
    pub fn empty_for_schema(schema: &Schema) -> Self {
        if schema.pk_count() >= 2 {
            PkColumn::Bytes {
                stride: schema.pk_stride() as u8,
                buf: vec![],
            }
        } else {
            PkColumn::for_type(schema.columns[schema.pk_indices()[0]].type_code)
        }
    }

    /// Read row `i` into a `PkTuple`.
    pub fn get_tuple(&self, i: usize, stride: u8) -> PkTuple {
        let mut t = PkTuple::new(stride);
        match self {
            PkColumn::U64s(v) => t.buf[..8].copy_from_slice(&v[i].to_le_bytes()),
            PkColumn::U128s(v) => t.buf[..16].copy_from_slice(&v[i].to_le_bytes()),
            PkColumn::Bytes { stride: s, buf } => {
                let s = *s as usize;
                debug_assert_eq!(s, stride as usize);
                t.buf[..s].copy_from_slice(&buf[i * s..(i + 1) * s]);
            }
        }
        t
    }

    /// Append the row at `src[i]` to `self`. Variants must match.
    pub fn push_from(&mut self, src: &PkColumn, i: usize) {
        match (self, src) {
            (PkColumn::U64s(d), PkColumn::U64s(s)) => d.push(s[i]),
            (PkColumn::U128s(d), PkColumn::U128s(s)) => d.push(s[i]),
            (PkColumn::Bytes { stride: sd, buf: d }, PkColumn::Bytes { stride: ss, buf: s }) => {
                debug_assert_eq!(*sd, *ss);
                let w = *sd as usize;
                d.extend_from_slice(&s[i * w..(i + 1) * w]);
            }
            _ => unreachable!("push_from: pk variant mismatch"),
        }
    }

    /// Append `pk`'s bytes to `self`, dispatching on variant. Hides the
    /// `Bytes`-vs-scalar choice from DML callers.
    pub fn push_tuple(&mut self, pk: &PkTuple) {
        let s = pk.stride as usize;
        match self {
            PkColumn::U64s(v) => {
                debug_assert!(s <= 8);
                let mut b = [0u8; 8];
                b[..s].copy_from_slice(&pk.buf[..s]);
                v.push(u64::from_le_bytes(b));
            }
            PkColumn::U128s(v) => {
                debug_assert!(s <= 16);
                let mut b = [0u8; 16];
                b[..s].copy_from_slice(&pk.buf[..s]);
                v.push(u128::from_le_bytes(b));
            }
            PkColumn::Bytes { stride, buf } => {
                debug_assert_eq!(*stride as usize, s);
                buf.extend_from_slice(&pk.buf[..s]);
            }
        }
    }

    /// One-row column matching `schema`'s PK layout, initialized with
    /// `pk`'s bytes.
    pub fn one_row(schema: &Schema, pk: &PkTuple) -> Self {
        let mut col = Self::empty_for_schema(schema);
        col.push_tuple(pk);
        col
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

    /// FFI-boundary constructor: build a tuple from a u128 with the full
    /// 16-byte narrow stride, without a schema lookup. The server reads
    /// only the column's actual stride; the high padding bytes (if any)
    /// are inert. Used by C and Python seek shims.
    pub fn from_u128_narrow(v: u128) -> Self {
        Self::from_u128(16, v)
    }

    /// Build a tuple from a raw byte slice. `bytes.len()` becomes the stride.
    /// Used by all FFI paths that pass packed PK regions through opaque
    /// byte buffers.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        // Hard assert (not debug-only): `bytes` is an externally-controlled FFI
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

    /// The PK's value as a `u128`, or `None` for a wide PK (stride > 16) that
    /// has no scalar projection and must use the byte path (`split_wire`).
    pub fn to_u128(&self) -> Option<u128> {
        (self.stride as usize <= 16).then(|| {
            let mut b = [0u8; 16];
            b[..self.stride as usize].copy_from_slice(self.as_bytes());
            u128::from_le_bytes(b)
        })
    }

    /// Split the tuple into the wire form `(seek_pk: u128, seek_pk_extra: &[u8])`.
    /// For narrow PKs (stride ≤ 16) `extra` is empty and the frame is byte-
    /// identical to the pre-compound-PK path.
    pub fn split_wire(&self) -> (u128, &[u8]) {
        let stride = self.stride as usize;
        let n = stride.min(16);
        let mut lo = [0u8; 16];
        lo[..n].copy_from_slice(&self.buf[..n]);
        let low_16 = u128::from_le_bytes(lo);
        let extra: &[u8] = if stride > 16 { &self.buf[16..stride] } else { &[] };
        (low_16, extra)
    }
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

impl PartialEq<Vec<u128>> for PkColumn {
    fn eq(&self, other: &Vec<u128>) -> bool {
        if self.len() != other.len() {
            return false;
        }
        match self {
            PkColumn::U64s(v) => v.iter().zip(other).all(|(&a, &b)| a as u128 == b),
            PkColumn::U128s(v) => v.as_slice() == other.as_slice(),
            PkColumn::Bytes { .. } => panic!("cannot compare wide PK against Vec<u128>"),
        }
    }
}

/// Per-column payload data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColData {
    /// Raw little-endian bytes; length = count * wire_stride (for all non-String, non-U128, non-Blob).
    Fixed(Vec<u8>),
    Strings(Vec<Option<std::string::String>>),
    /// Variable-length raw byte payloads. Same on-wire encoding as `Strings`
    /// (16-byte German-string struct + blob arena spill) but the bytes are
    /// not constrained to be valid UTF-8.
    Bytes(Vec<Option<Vec<u8>>>),
    U128s(Vec<u128>),
}

impl ColData {
    /// Append the element at row `idx` of `self` onto `dst`. Both columns are
    /// built from the same schema, so they are always the same variant — a
    /// mismatch is a construction bug, not a runtime condition. `fixed_stride`
    /// is the per-element byte width, used only by the `Fixed` variant.
    ///
    /// Matching on `self` (rather than the `(self, dst)` pair under a `_`
    /// wildcard) makes the compiler force every variant to be handled here:
    /// adding a new `ColData` variant fails to compile until this copy path
    /// covers it, instead of silently falling into a runtime panic — which is
    /// exactly how the `Bytes` variant was once left uncopied.
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
            ColData::U128s(s) => {
                let ColData::U128s(d) = dst else { variant_mismatch() };
                d.push(s[idx]);
            }
        }
    }
}

#[cold]
#[inline(never)]
fn variant_mismatch() -> ! {
    panic!("ColData::push_row_from: source and destination column variants differ");
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
    pub fn filler_columns(schema: &Schema, count: usize) -> Vec<ColData> {
        schema
            .columns
            .iter()
            .enumerate()
            .map(|(ci, col)| {
                if schema.is_pk_col(ci) {
                    ColData::Fixed(vec![])
                } else {
                    match col.type_code {
                        TypeCode::String => ColData::Strings(vec![Some(String::new()); count]),
                        TypeCode::Blob => ColData::Bytes(vec![Some(Vec::new()); count]),
                        TypeCode::U128 | TypeCode::UUID | TypeCode::I128 => ColData::U128s(vec![0u128; count]),
                        _ => ColData::Fixed(vec![0u8; count * col.type_code.wire_stride()]),
                    }
                }
            })
            .collect()
    }

    pub fn len(&self) -> usize {
        self.pks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pks.is_empty()
    }

    /// Indices of the live rows — those with positive weight. A `ZSetBatch`
    /// row with weight ≤ 0 is a retraction/ghost, not a present element, so a
    /// catalog scan over a batch iterates only its live rows.
    pub fn live_rows(&self) -> impl Iterator<Item = usize> + '_ {
        (0..self.len()).filter(move |&i| self.weights[i] > 0)
    }

    /// Append all rows from `other` into `self`. Panics if column layouts differ.
    pub fn extend_from(&mut self, other: &ZSetBatch) {
        assert_eq!(
            self.columns.len(),
            other.columns.len(),
            "extend_from: column count mismatch (self {}, other {})",
            self.columns.len(),
            other.columns.len(),
        );
        match (&mut self.pks, &other.pks) {
            (PkColumn::U64s(a), PkColumn::U64s(b)) => a.extend_from_slice(b),
            (PkColumn::U128s(a), PkColumn::U128s(b)) => a.extend_from_slice(b),
            (PkColumn::Bytes { stride: sa, buf: a }, PkColumn::Bytes { stride: sb, buf: b }) => {
                assert_eq!(sa, sb, "extend_from: wide PK stride mismatch");
                a.extend_from_slice(b);
            }
            _ => panic!("extend_from: pk column type mismatch"),
        }
        self.weights.extend_from_slice(&other.weights);
        self.nulls.extend_from_slice(&other.nulls);
        for (a, b) in self.columns.iter_mut().zip(other.columns.iter()) {
            match (a, b) {
                (ColData::Fixed(a), ColData::Fixed(b)) => a.extend_from_slice(b),
                (ColData::Strings(a), ColData::Strings(b)) => a.extend(b.iter().cloned()),
                (ColData::Bytes(a), ColData::Bytes(b)) => a.extend(b.iter().cloned()),
                (ColData::U128s(a), ColData::U128s(b)) => a.extend_from_slice(b),
                _ => panic!("extend_from: column type mismatch"),
            }
        }
    }

    /// Like `extend_from` but consumes `other`, moving its String/Bytes buffers
    /// instead of deep-cloning each value. O(n) pointer copies, zero heap
    /// allocation for string/bytes content. Used in scan continuation loops.
    pub fn extend_from_owned(&mut self, mut other: ZSetBatch) {
        assert_eq!(
            self.columns.len(),
            other.columns.len(),
            "extend_from_owned: column count mismatch",
        );
        match (&mut self.pks, &mut other.pks) {
            (PkColumn::U64s(a), PkColumn::U64s(b)) => a.append(b),
            (PkColumn::U128s(a), PkColumn::U128s(b)) => a.append(b),
            (PkColumn::Bytes { stride: sa, buf: a }, PkColumn::Bytes { stride: sb, buf: b }) => {
                assert_eq!(sa, sb, "extend_from_owned: wide PK stride mismatch");
                a.append(b);
            }
            _ => panic!("extend_from_owned: pk column type mismatch"),
        }
        self.weights.append(&mut other.weights);
        self.nulls.append(&mut other.nulls);
        for (a, b) in self.columns.iter_mut().zip(other.columns.iter_mut()) {
            match (a, b) {
                (ColData::Fixed(a), ColData::Fixed(b)) => a.append(b),
                (ColData::Strings(a), ColData::Strings(b)) => a.append(b),
                (ColData::Bytes(a), ColData::Bytes(b)) => a.append(b),
                (ColData::U128s(a), ColData::U128s(b)) => a.append(b),
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
                ColData::U128s(v) => v.truncate(n),
            }
        }
    }

    /// Validate that all vectors are consistently sized for the given schema.
    pub fn validate(&self, schema: &Schema) -> Result<(), std::string::String> {
        // Front-line agreement check: the Pk buffer variant must match the
        // schema's PK arity. `empty_for_schema` always picks `Bytes` ⟺
        // pk_count() >= 2 (a single PK column is <= 16 bytes), so this never
        // fires on correct code — it is a cheap tripwire for exactly the
        // planner/engine schema disagreement a compound-PK view could introduce.
        // Without it a scalar/compound mismatch passes the stride check below
        // and only surfaces as a deep panic in sort-merge/consolidation.
        match &self.pks {
            PkColumn::Bytes { .. } if schema.pk_count() < 2 => {
                return Err("batch carries a wide PK buffer but schema PK is scalar".into())
            }
            PkColumn::U64s(_) | PkColumn::U128s(_) if schema.pk_count() >= 2 => {
                return Err("batch carries a scalar PK but schema PK is compound".into())
            }
            _ => {}
        }
        if let PkColumn::Bytes { stride, buf } = &self.pks {
            if *stride == 0 {
                return Err("wide PK stride must be non-zero".into());
            }
            if *stride as usize != schema.pk_stride() {
                return Err(format!(
                    "mismatched PK stride: expected {}, got {}",
                    schema.pk_stride(),
                    stride
                ));
            }
            if buf.len() % (*stride as usize) != 0 {
                return Err(format!(
                    "wide PK buffer length {} is not a multiple of stride {}",
                    buf.len(),
                    stride
                ));
            }
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
        for (_pi, ci, col_def) in schema.payload_columns() {
            let col = &self.columns[ci];
            match col {
                ColData::Fixed(bytes) => {
                    let expected = n * col_def.type_code.wire_stride();
                    if bytes.len() != expected {
                        return Err(format!(
                            "column {} Fixed byte length {} != expected {}",
                            ci,
                            bytes.len(),
                            expected
                        ));
                    }
                }
                ColData::Strings(v) => {
                    if v.len() != n {
                        return Err(format!("column {} Strings length {} != row count {}", ci, v.len(), n));
                    }
                }
                ColData::Bytes(v) => {
                    if v.len() != n {
                        return Err(format!("column {} Bytes length {} != row count {}", ci, v.len(), n));
                    }
                }
                ColData::U128s(v) => {
                    if v.len() != n {
                        return Err(format!("column {} U128s length {} != row count {}", ci, v.len(), n));
                    }
                }
            }
        }
        // A null bit on a NOT NULL payload column would make FK/unique validation
        // skip the value (treating it as absent) while consolidation and decoders
        // read the raw bytes as live data — an inconsistency the schema forbids.
        // Reject it. `pi` is the dense payload index (null-bitmap bit position),
        // matching the convention the FK/unique skips use.
        let mut not_null_mask: u64 = 0;
        for (pi, _ci, col_def) in schema.payload_columns() {
            if !col_def.is_nullable {
                not_null_mask |= 1u64 << pi;
            }
        }
        if not_null_mask != 0 {
            for (row, &word) in self.nulls.iter().enumerate() {
                if word & not_null_mask != 0 {
                    return Err(format!("row {row} sets a null bit on a NOT NULL column"));
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
}

impl<'a> BatchAppender<'a> {
    pub fn new(batch: &'a mut ZSetBatch, schema: &'a Schema) -> Self {
        BatchAppender {
            batch,
            schema,
            cursor: 0,
            row_active: false,
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

    /// Override the null mask for the current row (must be called after `add_row`).
    pub fn null_mask(&mut self, mask: u64) -> &mut Self {
        // `last_mut()` instead of `len() - 1`: a call before any `add_row` would
        // otherwise wrap to usize::MAX and OOB-panic in release.
        match self.batch.nulls.last_mut() {
            Some(last) => *last = mask,
            None => panic!("BatchAppender: null_mask called before add_row"),
        }
        self
    }

    /// Append a u64 value to the next Fixed column.
    pub fn u64_val(&mut self, v: u64) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Fixed(buf) => buf.extend_from_slice(&v.to_le_bytes()),
            _ => panic!("BatchAppender: u64_val called on non-Fixed column at schema index {ci}"),
        }
        self.cursor += 1;
        self
    }

    /// Append a SQL NULL to the next Fixed (U64) column: writes a zero filler and
    /// marks the column NULL in the row bitmap. The Fixed-column sibling of
    /// `str_null`/`bytes_null` — self-sufficient, so a nullable U64 needs no
    /// out-of-band `null_mask` call.
    pub fn u64_null(&mut self) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Fixed(buf) => buf.extend_from_slice(&0u64.to_le_bytes()),
            _ => panic!("BatchAppender: u64_null called on non-Fixed column at schema index {ci}"),
        }
        self.mark_current_null(ci);
        self.cursor += 1;
        self
    }

    /// Append an i64 value to the next Fixed column.
    pub fn i64_val(&mut self, v: i64) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Fixed(buf) => buf.extend_from_slice(&v.to_le_bytes()),
            _ => panic!("BatchAppender: i64_val called on non-Fixed column at schema index {ci}"),
        }
        self.cursor += 1;
        self
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

    /// Mark column `ci` NULL in the current row's null bitmap. The read side
    /// (`eval_expr`, the WAL encoder) gates on `nulls[row] & (1 << payload_idx)`
    /// and consults the `Option` only when that bit is clear, so a pushed `None`
    /// and the bitmap must agree. Setting the bit here makes `str_null`/`bytes_null`
    /// self-sufficient — no out-of-band `null_mask` call required.
    fn mark_current_null(&mut self, ci: usize) {
        let pi = self.schema.payload_idx(ci);
        *self
            .batch
            .nulls
            .last_mut()
            .expect("BatchAppender: mark_current_null called before add_row") |= 1u64 << pi;
    }

    /// Append a SQL NULL to the next Strings column.
    pub fn str_null(&mut self) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Strings(v) => v.push(None),
            _ => panic!("BatchAppender: str_null called on non-Strings column at schema index {ci}"),
        }
        self.mark_current_null(ci);
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

    /// Append a SQL NULL to the next Bytes (BLOB) column.
    pub fn bytes_null(&mut self) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::Bytes(v) => v.push(None),
            _ => panic!("BatchAppender: bytes_null called on non-Bytes column at schema index {ci}"),
        }
        self.mark_current_null(ci);
        self.cursor += 1;
        self
    }

    /// Append a u128 value to the next U128s column.
    pub fn u128_val(&mut self, v: u128) -> &mut Self {
        let ci = self.col_index();
        match &mut self.batch.columns[ci] {
            ColData::U128s(vec) => vec.push(v),
            _ => panic!("BatchAppender: u128_val called on non-U128s column at schema index {ci}"),
        }
        self.cursor += 1;
        self
    }

    /// Append a zero/empty value for the current column based on its schema type.
    pub fn zero_val(&mut self) -> &mut Self {
        let ci = self.col_index();
        let tc = self.schema.columns[ci].type_code;
        match &mut self.batch.columns[ci] {
            ColData::Fixed(buf) => {
                let stride = tc.wire_stride();
                buf.extend(std::iter::repeat_n(0u8, stride));
            }
            ColData::Strings(v) => {
                v.push(Some(std::string::String::new()));
            }
            ColData::Bytes(v) => {
                v.push(Some(Vec::new()));
            }
            ColData::U128s(v) => {
                v.push(0);
            }
        }
        self.cursor += 1;
        self
    }

    /// Map the payload cursor to the actual schema column index, skipping every
    /// PK column. Supports compound PKs (e.g. the catalog circuit tables whose
    /// PK is (view_id, sub)): payload value N targets the N-th non-PK column.
    fn col_index(&self) -> usize {
        // Hard assert (not debug-only) + bounded `for`: in release an
        // unbounded `loop` past `num_columns()` returns an OOB index that
        // panics at the `columns[ci]` call site. A misbehaving caller gets a
        // clear panic here instead.
        assert!(
            self.cursor < self.schema.num_payload_cols(),
            "BatchAppender: payload cursor {} exceeds {} payload columns",
            self.cursor,
            self.schema.num_payload_cols(),
        );
        let pk_cols = self.schema.pk_indices();
        let mut seen_payload = 0;
        for ci in 0..self.schema.num_columns() {
            if !pk_cols.contains(&ci) {
                if seen_payload == self.cursor {
                    return ci;
                }
                seen_payload += 1;
            }
        }
        panic!("BatchAppender: col_index resolution failed (cursor {})", self.cursor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // Structural rejects: empty, over-long, out-of-range, duplicate.
        assert_eq!(Schema::validate_parts(&[], &cols), Err("pk_cols must not be empty"));
        assert_eq!(
            Schema::validate_parts(&[0, 1, 0, 1, 0], &cols),
            Err("pk_cols exceeds PK_LIST_MAX_COLS")
        );
        assert_eq!(Schema::validate_parts(&[9], &cols), Err("pk_cols index out of range"));
        assert_eq!(
            Schema::validate_parts(&[0, 0], &cols),
            Err("pk_cols contains duplicates")
        );

        // Per-column rejects: nullable and PK-ineligible (String/float).
        assert_eq!(
            Schema::validate_parts(&[3], &cols),
            Err("PK column must be non-nullable")
        );
        assert_eq!(
            Schema::validate_parts(&[2], &cols),
            Err("PK column type not PK-eligible")
        );
        assert_eq!(
            Schema::validate_parts(&[4], &cols),
            Err("PK column type not PK-eligible")
        );

        // Column-count cap: the null bitmap is one u64, so > MAX_COLUMNS rejects.
        let wide: Vec<ColumnDef> = (0..=MAX_COLUMNS)
            .map(|i| ColumnDef::new(format!("c{i}"), TypeCode::U64, i > 0))
            .collect();
        assert_eq!(
            Schema::validate_parts(&[0], &wide),
            Err("column count exceeds MAX_COLUMNS")
        );
        assert!(Schema::validate_parts(&[0], &wide[..MAX_COLUMNS]).is_ok());
    }

    #[test]
    fn filler_columns_encode_without_panic() {
        // The client delete path builds retraction batches from `filler_columns`
        // (bypassing BatchAppender, whose `add_row` takes a scalar PK). Each
        // payload variant must match the wire encoder's expectation; regression:
        // a duplicated copy of this mapping once sent an I128 payload column as
        // `Fixed` and panicked in `encode_wal_block` ("expected U128s"). Cover
        // every payload family — including a nullable String — the same way
        // `push` exercises them: validate, then encode.
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("i", TypeCode::I64, false),    // Fixed
                ColumnDef::new("big", TypeCode::I128, false), // U128s (the regression)
                ColumnDef::new("u", TypeCode::U128, false),   // U128s
                ColumnDef::new("uid", TypeCode::UUID, false), // U128s
                ColumnDef::new("s", TypeCode::String, true),  // Strings, nullable
                ColumnDef::new("b", TypeCode::Blob, false),   // Bytes
            ],
            pk_cols: vec![0],
        };
        let count = 2;
        let batch = ZSetBatch {
            pks: PkColumn::U64s(vec![10, 20]),
            weights: vec![-1; count],
            nulls: vec![0; count],
            columns: ZSetBatch::filler_columns(&schema, count),
        };
        batch.validate(&schema).expect("filler batch must validate");
        // Must not panic: I128/U128/UUID → U128s, String/Blob → 16 zero bytes.
        let _ = crate::protocol::encode_wal_block(&schema, 7, &batch);
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
            a.add_row(3u128, 1).i64_val(30).str_null();
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
        // Strings column is empty — mismatch
        let err = batch.validate(&schema).unwrap_err();
        assert!(err.contains("Strings"));
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
        // Fixed column 1 is empty (needs 8 bytes) — mismatch
        let err = batch.validate(&schema).unwrap_err();
        assert!(err.contains("Fixed"));
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
            pks: PkColumn::Bytes {
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
            pks: PkColumn::Bytes {
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
        a.extend_from(&b);
    }

    /// `extend_from_owned` (move) must produce byte-identical results to
    /// `extend_from` (clone) for schemas with String and Bytes columns.
    #[test]
    fn test_extend_from_owned_matches_extend_from() {
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

        // Reference path: clone-based extend_from.
        let mut acc_ref = build(1);
        acc_ref.extend_from(&build(10));

        // Move-based extend_from_owned over identically-built inputs.
        let mut acc_owned = build(1);
        acc_owned.extend_from_owned(build(10));

        assert_eq!(acc_ref, acc_owned, "extend_from_owned must match extend_from");
        assert_eq!(acc_owned.len(), 4);
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
        assert_eq!(batch.pks, vec![1u128, 2u128, 3u128]);
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
    fn test_appender_null_mask() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::U64, true),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(1u128, 1)
            .null_mask(0x02)
            .u64_val(0);
        assert_eq!(batch.nulls[0], 0x02);
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
        if let ColData::U128s(v) = &batch.columns[1] {
            assert_eq!(v[0], ((0xBEEF_u128) << 64) | 0xDEAD);
        } else {
            panic!("expected U128s");
        }
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

    #[test]
    #[should_panic(expected = "u64_val called on non-Fixed")]
    fn test_appender_type_mismatch_u64_on_string() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("s", TypeCode::String, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).u64_val(42);
    }

    #[test]
    #[should_panic(expected = "i64_val called on non-Fixed")]
    fn test_appender_type_mismatch_i64_on_string() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("s", TypeCode::String, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).i64_val(-7);
    }

    #[test]
    #[should_panic(expected = "str_val called on non-Strings")]
    fn test_appender_type_mismatch_str_on_fixed() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema)
            .add_row(1u128, 1)
            .str_val("oops");
    }

    #[test]
    #[should_panic(expected = "str_null called on non-Strings")]
    fn test_appender_type_mismatch_str_null_on_fixed() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        };
        let mut batch = ZSetBatch::new(&schema);
        BatchAppender::new(&mut batch, &schema).add_row(1u128, 1).str_null();
    }

    #[test]
    #[should_panic(expected = "u128_val called on non-U128s")]
    fn test_appender_type_mismatch_u128_on_fixed() {
        let schema = Schema {
            columns: vec![
                ColumnDef::new("pk", TypeCode::U64, false),
                ColumnDef::new("v", TypeCode::U64, false),
            ],
            pk_cols: vec![0],
        };
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
            a.add_row(2u128, -1).i64_val(20).str_null();
        }
        assert!(batch.validate(&schema).is_ok());
    }

    // --- Site B: PkTuple::to_u128 ---

    #[test]
    fn pk_tuple_to_u128_narrow() {
        let uuid_bytes: [u8; 16] = [
            0x00, 0x44, 0x44, 0x55, 0x16, 0xa7, 0xd4, 0x41, 0x9b, 0xe2, 0x00, 0x84, 0x0e, 0x55, 0x0e, 0x55,
        ];
        let t = PkTuple::from_bytes(&uuid_bytes);
        assert_eq!(t.to_u128(), Some(u128::from_le_bytes(uuid_bytes)));
    }

    #[test]
    fn pk_tuple_to_u128_wide_returns_none() {
        // stride 24 = three U64 PKs; no scalar projection.
        let t = PkTuple::from_bytes(&[0u8; 24]);
        assert_eq!(t.to_u128(), None);
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

    // --- §5.1: str_null / bytes_null set the null bitmap bit ---

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
    fn str_null_sets_null_bit() {
        let schema = nullable_str_blob_schema();
        // payload_idx(col 1 = String) = 0 → bit 0; payload_idx(col 2 = Blob) = 1 → bit 1
        let str_bit = 1u64 << schema.payload_idx(1);
        let blob_bit = 1u64 << schema.payload_idx(2);
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            a.add_row(1, 1).str_null().bytes_null();
        }
        assert_eq!(batch.nulls[0], str_bit | blob_bit, "both null bits must be set");
        assert!(batch.validate(&schema).is_ok());
    }

    #[test]
    fn str_null_self_sufficiency_round_trips_as_null() {
        use crate::protocol::wal_block::{decode_wal_block, encode_wal_block, VerifyChecksum};
        let schema = nullable_str_blob_schema();
        let mut batch = ZSetBatch::new(&schema);
        {
            let mut a = BatchAppender::new(&mut batch, &schema);
            // No null_mask call — str_null / bytes_null must be self-sufficient.
            a.add_row(42, 1).str_null().bytes_null();
        }
        let encoded = encode_wal_block(&schema, 1, &batch);
        let (decoded, _, _) = decode_wal_block(&encoded, &schema, VerifyChecksum::Yes).unwrap();
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
        a.add_row(1, 1).str_null(); // only 1 of 2 payload cols pushed
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
