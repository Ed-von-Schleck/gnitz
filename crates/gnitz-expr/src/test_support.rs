//! Owned-buffer stand-ins for a physical batch and schema, so the evaluator's
//! tests run entirely inside this crate.
//!
//! The store's and the client's batches both live in crates that depend on this
//! one, so neither is reachable from here — and driving every kernel through a
//! batch and a schema they were not written for is what holds the addressing
//! paths to the [`BatchView`] / [`ColumnTable`] contracts alone.

use gnitz_wire::TypeCode;

use crate::eval::Resolved;
use crate::{
    BatchView, ColumnLocator, ColumnTable, ExprResults, LogicalInstr, LogicalProgram, MapEval, MapTarget, Reg,
    RowFilter, RowSource, ScalarEval, SchemaFacts, Sink,
};

/// A [`BatchView`] over owned buffers, laid out region-wise like the physical
/// batch: one packed OPK PK region, one null-bitmap word per row, and one
/// contiguous buffer per payload slot.
pub struct TestView {
    rows: usize,
    pk_stride: usize,
    pk: Vec<u8>,
    nulls: Vec<u8>,
    cols: Vec<Vec<u8>>,
    /// The variable-length heap the 16-byte German-string cells point into.
    blob: Vec<u8>,
}

impl TestView {
    pub fn new(rows: usize, pk_stride: usize) -> Self {
        TestView {
            rows,
            pk_stride,
            pk: vec![0u8; rows * pk_stride],
            nulls: vec![0u8; rows * 8],
            cols: Vec::new(),
            blob: Vec::new(),
        }
    }

    /// `rows` rows over `schema`: one payload column per slot, sized from its
    /// declared type, row `r`'s PK `r + 1`, nothing NULL.
    pub fn for_schema(schema: &TestSchema, rows: usize) -> Self {
        let mut v = TestView::new(rows, schema.pk_stride());
        for pi in 0..schema.num_payload_cols() {
            v.push_col(schema.locate(schema.payload_col_idx(pi)).size());
        }
        for row in 0..rows {
            v.set_pk(schema, row, row as u64 + 1);
        }
        v
    }

    /// Append a payload column whose cells are `col_size` bytes wide.
    pub fn push_col(&mut self, col_size: usize) {
        self.cols.push(vec![0u8; self.rows * col_size]);
    }

    /// Write `pk` into every PK column of `row`, addressed through the schema's
    /// own locator, so a fixture cannot disagree with the addressing the kernels
    /// use. A compound PK gets the same source value in each column, truncated to
    /// that column's width; `pk` is a bit pattern, so a signed column reads it
    /// two's-complement.
    pub fn set_pk(&mut self, schema: &TestSchema, row: usize, pk: u64) {
        for ci in 0..schema.num_columns() {
            if let ColumnLocator::Pk { byte_off, size, type_code } = schema.locate(ci) {
                self.set_pk_col(
                    row,
                    byte_off as usize,
                    &pk.to_le_bytes()[..(size as usize).min(8)],
                    type_code,
                );
            }
        }
    }

    /// OPK-encode `native` (native-LE bytes of type `type_code`) into the PK
    /// region of `row` at `byte_off`.
    pub fn set_pk_col(&mut self, row: usize, byte_off: usize, native: &[u8], type_code: TypeCode) {
        let base = row * self.pk_stride + byte_off;
        gnitz_wire::encode_pk_column(native, type_code, &mut self.pk[base..base + native.len()]);
    }

    /// Write `native` into the low bytes of `row`'s cell in payload slot `pi`.
    /// The cell stride comes from the column buffer, not from `native.len()`, so
    /// a narrower value lands correctly in a wider cell instead of shifting
    /// every subsequent row.
    pub fn set_payload(&mut self, row: usize, pi: usize, native: &[u8]) {
        let stride = self.cols[pi].len() / self.rows;
        let base = row * stride;
        self.cols[pi][base..base + native.len()].copy_from_slice(native);
    }

    /// Store `val` little-endian into the low bytes of `row`'s cell in slot `pi`,
    /// truncated to a narrower cell.
    pub fn set_int(&mut self, row: usize, pi: usize, val: i64) {
        let w = (self.cols[pi].len() / self.rows).min(8);
        self.set_payload(row, pi, &val.to_le_bytes()[..w]);
    }

    /// Encode `s` into this view's blob and store the resulting 16-byte
    /// German-string cell in payload slot `pi` of `row`.
    pub fn set_string(&mut self, row: usize, pi: usize, s: &[u8]) {
        let cell = gnitz_wire::encode_german_string(s, &mut self.blob);
        self.cols[pi][row * 16..row * 16 + 16].copy_from_slice(&cell);
    }

    pub fn set_null(&mut self, row: usize, slot: usize) {
        let mut word = self.get_null_word(row);
        gnitz_wire::null_word_set(&mut word, slot, true);
        self.set_null_word(row, word);
    }

    /// Overwrite `row`'s whole null word (bit N = payload slot N is NULL).
    pub fn set_null_word(&mut self, row: usize, word: u64) {
        gnitz_wire::write_u64_le(&mut self.nulls, row * 8, word);
    }
}

impl RowSource for TestView {
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        &self.pk[row * self.pk_stride..(row + 1) * self.pk_stride]
    }
    fn get_null_word(&self, row: usize) -> u64 {
        gnitz_wire::read_u64_le(&self.nulls, row * 8)
    }
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        &self.cols[payload_col][row * col_size..row * col_size + col_size]
    }
    fn blob(&self) -> &[u8] {
        &self.blob
    }
    #[inline(always)]
    fn row_count(&self) -> usize {
        self.rows
    }
}

impl BatchView for TestView {
    fn col_data(&self, payload_col: usize, col_size: usize) -> &[u8] {
        let c = &self.cols[payload_col];
        debug_assert_eq!(c.len(), self.rows * col_size);
        c
    }
    fn null_bmp(&self) -> &[u8] {
        &self.nulls
    }
    fn pk_region(&self) -> (&[u8], usize) {
        (&self.pk, self.pk_stride)
    }
}

/// A [`ColumnTable`] over a `(type_code, nullable)` column table and a PK list.
/// Type codes are reported verbatim, undecodable ones included.
pub struct TestSchema {
    cols: Vec<(TypeCode, bool)>,
    pk: Vec<u32>,
}

impl TestSchema {
    pub fn new(cols: &[(TypeCode, bool)], pk: &[u32]) -> Self {
        TestSchema { cols: cols.to_vec(), pk: pk.to_vec() }
    }

    /// As [`TestSchema::new`], with the PK at `pk_index` and every other column
    /// of `col_types` a nullable payload.
    pub fn with_pk_at(pk_index: usize, col_types: &[TypeCode]) -> Self {
        let cols: Vec<(TypeCode, bool)> = col_types.iter().enumerate().map(|(i, &t)| (t, i != pk_index)).collect();
        TestSchema::new(&cols, &[pk_index as u32])
    }
}

impl ColumnTable for TestSchema {
    fn pk_cols(&self) -> &[u32] {
        &self.pk
    }
    fn num_columns(&self) -> usize {
        self.cols.len()
    }
    fn col_type_code(&self, ci: usize) -> TypeCode {
        self.cols[ci].0
    }
    fn col_nullable(&self, ci: usize) -> bool {
        self.cols[ci].1
    }
}

/// Build a [`TestView`] of `(pk, null_word, payload values)` rows against
/// `schema`: each payload column is sized from its declared type and each row's
/// `i64` value is stored little-endian into the low bytes of its cell.
pub fn make_int_view(schema: &TestSchema, rows: &[(u64, u64, &[i64])]) -> TestView {
    let mut v = TestView::for_schema(schema, rows.len());
    for (row, &(pk, null_word, cols)) in rows.iter().enumerate() {
        v.set_pk(schema, row, pk);
        v.set_null_word(row, null_word);
        for (pi, &val) in cols.iter().enumerate() {
            v.set_int(row, pi, val);
        }
    }
    v
}

/// An `n`-row view over `schema`'s German-string payload columns: slot `c` of
/// `row` holds `cell(row, c)`. PKs are `1..=n`, nothing is NULL.
pub fn make_string_view<S: AsRef<[u8]>>(schema: &TestSchema, n: usize, cell: impl Fn(usize, usize) -> S) -> TestView {
    let mut v = TestView::for_schema(schema, n);
    for row in 0..n {
        for pi in 0..schema.num_payload_cols() {
            v.set_string(row, pi, cell(row, pi).as_ref());
        }
    }
    v
}

/// A `U64` PK plus `n` `I64` payload columns, all `nullable` or all not — the
/// shape almost every kernel test wants.
pub fn schema_pk_ints(n: usize, nullable: bool) -> TestSchema {
    schema_pk_cols(TypeCode::I64, n, nullable)
}

/// A `U64` PK plus `n` `STRING` payload columns.
pub fn schema_pk_strings(n: usize, nullable: bool) -> TestSchema {
    schema_pk_cols(TypeCode::String, n, nullable)
}

fn schema_pk_cols(payload_tc: TypeCode, n: usize, nullable: bool) -> TestSchema {
    let mut cols = vec![(TypeCode::U64, false)];
    cols.extend(std::iter::repeat_n((payload_tc, nullable), n));
    TestSchema::new(&cols, &[0])
}

/// An `n`-row view over `schema`'s `I64` payload columns: column `col` of `row`
/// holds `f(row, col)`, and its null bit is set when `null_pred(row, col)`.
/// PKs are `1..=n`.
pub fn make_n_col_view(
    schema: &TestSchema,
    n: usize,
    f: impl Fn(usize, usize) -> i64,
    null_pred: impl Fn(usize, usize) -> bool,
) -> TestView {
    let mut v = TestView::for_schema(schema, n);
    for row in 0..n {
        let mut null_word = 0u64;
        for col in 0..schema.num_payload_cols() {
            gnitz_wire::null_word_set(&mut null_word, col, null_pred(row, col));
            v.set_int(row, col, f(row, col));
        }
        v.set_null_word(row, null_word);
    }
    v
}

/// Build and resolve a scalar (non-filter, no output plan) program. A test
/// program that fails validation is a bug in the test, so it unwraps here rather
/// than at every site.
pub fn scalar_prog(
    schema: &TestSchema,
    instrs: Vec<LogicalInstr>,
    result_reg: Reg,
    const_strings: Vec<Vec<u8>>,
) -> ScalarEval {
    LogicalProgram::new(instrs, vec![Sink::Reg(result_reg)], const_strings)
        .resolve_scalar(schema)
        .expect("test program must validate")
}

/// A predicate as [`filter_prog`] takes it: `(instrs, result_reg)`.
pub type FilterShape = (Vec<LogicalInstr>, Reg);

/// Build and resolve a filter program — the arm where `result_reg` stays
/// bit_only-eligible. Same unwrap rule as [`scalar_prog`].
pub fn filter_prog(
    schema: &TestSchema,
    instrs: Vec<LogicalInstr>,
    result_reg: Reg,
    const_strings: Vec<Vec<u8>>,
) -> RowFilter {
    LogicalProgram::new(instrs, vec![Sink::Reg(result_reg)], const_strings)
        .resolve_filter(schema)
        .expect("test predicate must validate")
}

/// Build and resolve a map program, checked against both the schema it reads
/// and the one it writes. Same unwrap rule as [`scalar_prog`].
pub fn map_prog(
    in_schema: &TestSchema,
    out_schema: &TestSchema,
    instrs: Vec<LogicalInstr>,
    sinks: Vec<Sink>,
    const_strings: Vec<Vec<u8>>,
) -> MapEval {
    LogicalProgram::new(instrs, sinks, const_strings)
        .resolve_map(in_schema, out_schema)
        .expect("test map must validate")
}

/// Every row's integer result of a scalar program.
pub fn row_values(ev: &mut ScalarEval, mb: &dyn BatchView) -> Vec<Option<i128>> {
    match ev.eval_all(mb) {
        ExprResults::Int(vals) => vals,
        ExprResults::Str { .. } => panic!("row_values over a string-valued program; use row_strs"),
    }
}

/// Every row's string result of a scalar program, `None` for NULL.
pub fn row_strs(ev: &mut ScalarEval, mb: &dyn BatchView) -> Vec<Option<Vec<u8>>> {
    match ev.eval_all(mb) {
        ExprResults::Str { bytes, spans } => spans.iter().map(|s| s.map(|(o, l)| bytes[o..o + l].to_vec())).collect(),
        ExprResults::Int(_) => panic!("row_strs over a scalar program; use row_values"),
    }
}

/// Run `ev` as a filter and report a per-row verdict — the shape almost every
/// filter test wants, since `RowFilter::ranges` reports runs rather than rows.
pub fn passing_rows(ev: &mut RowFilter, mb: &TestView) -> Vec<bool> {
    let mut passed = vec![false; mb.row_count()];
    for (s, e) in passing_ranges(ev, mb) {
        passed[s..e].fill(true);
    }
    passed
}

/// The runs `ev` reports, verbatim, each checked to be a non-empty run inside
/// the batch. The buffer is seeded with a stale entry, so a `ranges` that
/// appended rather than cleared fails here.
pub fn passing_ranges(ev: &mut RowFilter, mb: &TestView) -> Vec<(usize, usize)> {
    let mut ranges = vec![(usize::MAX, usize::MAX)];
    ev.ranges(mb, &mut ranges);
    let n = mb.row_count();
    for &(s, e) in &ranges {
        assert!(s < e && e <= n, "run ({s}, {e}) is not a non-empty run of 0..{n}");
    }
    ranges
}

/// `build()` twice: as resolved, which must be the `no_nulls` arm, and forced
/// onto the nullable arm — a differential baseline running the kernels the fast
/// arm skips.
pub fn both_arms<E: Resolved>(label: &str, build: impl Fn() -> E) -> (E, E) {
    let fast = build();
    assert!(fast.prog().no_nulls, "{label}: the fast side must resolve no_nulls");
    let mut nullable = build();
    nullable.force_nullable_arm();
    (fast, nullable)
}

/// A [`MapTarget`] over owned buffers: one null word per row, one buffer per
/// output payload slot, and the heap a string cell spills into.
pub struct TestOut {
    pub nulls: Vec<u8>,
    pub cols: Vec<Vec<u8>>,
    pub blob: Vec<u8>,
}

impl TestOut {
    /// `rows` rows of output slots `strides[pi]` bytes wide.
    pub fn new(rows: usize, strides: &[usize]) -> Self {
        TestOut {
            nulls: vec![0; rows * 8],
            cols: strides.iter().map(|&w| vec![0; rows * w]).collect(),
            blob: Vec::new(),
        }
    }
}

impl MapTarget for TestOut {
    fn null_bmp_mut(&mut self) -> &mut [u8] {
        &mut self.nulls
    }
    fn slot_mut(&mut self, pi: usize) -> (&mut [u8], &mut [u8], &mut Vec<u8>) {
        (&mut self.cols[pi], &mut self.nulls, &mut self.blob)
    }
}

/// A three-row view with a compound `(U32, I64)` PK and payload slots
/// `0: I32`, `1: U128`, `2: U64`, and its schema.
pub fn locator_fixture() -> (TestSchema, TestView) {
    let schema = TestSchema::new(
        &[
            (TypeCode::U32, false),
            (TypeCode::I64, false),
            (TypeCode::I32, true),
            (TypeCode::U128, true),
            (TypeCode::U64, true),
        ],
        &[0, 1],
    );
    let mut v = TestView::for_schema(&schema, 3);
    for (row, (a, b)) in [(7u32, -1i64), (0, 0), (u32::MAX, i64::MIN)].into_iter().enumerate() {
        v.set_pk_col(row, 0, &a.to_le_bytes(), TypeCode::U32);
        v.set_pk_col(row, 4, &b.to_le_bytes(), TypeCode::I64);
        v.set_payload(row, 0, &(-3i32).to_le_bytes());
        v.set_payload(row, 1, &(1u128 << 100).to_le_bytes());
        v.set_payload(row, 2, &(row as u64).to_le_bytes());
    }
    (schema, v)
}

/// `IS NULL` / `IS NOT NULL` over column `col`. The two read one null-bitmap bit
/// at opposite polarity and share one instruction, so a fixture names the
/// polarity rather than spelling the struct literal — which rustfmt would break
/// across four lines at every call site.
pub fn is_null_op(col: u32) -> LogicalInstr {
    LogicalInstr::IsNull { col, invert: false }
}

pub fn is_not_null_op(col: u32) -> LogicalInstr {
    LogicalInstr::IsNull { col, invert: true }
}
