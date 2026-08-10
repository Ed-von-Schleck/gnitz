//! Owned-buffer stand-ins for the engine's physical batch and schema, so the
//! evaluator's tests run entirely inside this crate.
//!
//! The engine is a binary crate: its `Batch`/`SchemaDescriptor` are unreachable
//! from here. That is the point rather than a workaround — driving every kernel
//! through a batch and a schema they were *not* written for is what proves the
//! addressing paths hold for the client-side adapter that will read the same
//! expressions, and [`TestView`] is that adapter's seed.

use gnitz_wire::type_code as tc;

use crate::{BatchView, ColumnLocator, Evaluator, LogicalInstr, LogicalProgram, RowSource, SchemaFacts};

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
    pub blob: Vec<u8>,
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

    /// Append a payload column whose cells are `col_size` bytes wide.
    pub fn push_col(&mut self, col_size: usize) -> usize {
        self.cols.push(vec![0u8; self.rows * col_size]);
        self.cols.len() - 1
    }

    /// OPK-encode `native` (native-LE bytes of type `type_code`) into the PK
    /// region of `row` at `byte_off`.
    pub fn set_pk_col(&mut self, row: usize, byte_off: usize, native: &[u8], type_code: u8) {
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

/// A [`SchemaFacts`] over a `(type_code, nullable)` column table plus a PK list
/// in PK-list order (which is where each PK column's OPK byte offset comes from,
/// and which is independent of column order).
///
/// A type code is stored and reported verbatim — including an **undecodable**
/// one. A release engine can carry one (a corrupt SAL, a crafted wire schema)
/// and the validator tests exist to pin what happens then, so there is no
/// decodability assert here to escape from.
pub struct TestSchema {
    cols: Vec<(u8, bool)>,
    pk: Vec<usize>,
    /// Per column: OPK byte offset for a PK column, dense payload slot otherwise
    /// — two namespaces, disjoint by column.
    addr: Vec<u8>,
    payload_to_ci: Vec<usize>,
    pk_stride: usize,
}

impl TestSchema {
    pub fn new(cols: &[(u8, bool)], pk: &[usize]) -> Self {
        let mut addr = vec![0u8; cols.len()];
        let mut off = 0usize;
        for &ci in pk {
            addr[ci] = off as u8;
            off += gnitz_wire::wire_stride(cols[ci].0);
        }
        let mut payload_to_ci = Vec::new();
        for (ci, a) in addr.iter_mut().enumerate() {
            if !pk.contains(&ci) {
                *a = payload_to_ci.len() as u8;
                payload_to_ci.push(ci);
            }
        }
        TestSchema {
            cols: cols.to_vec(),
            pk: pk.to_vec(),
            addr,
            payload_to_ci,
            // The running offset after the whole PK list *is* the PK stride.
            pk_stride: off,
        }
    }

    /// A PK at `pk_index` plus the other columns of `col_types` as payload, all
    /// of them nullable. `col_types` may name an *undecodable* type code: a
    /// release engine can carry one (a corrupt SAL / crafted wire schema), and
    /// the validator tests exist to pin what happens then.
    pub fn with_pk_at(pk_index: usize, col_types: &[u8]) -> Self {
        let cols: Vec<(u8, bool)> = col_types.iter().enumerate().map(|(i, &t)| (t, i != pk_index)).collect();
        TestSchema::new(&cols, &[pk_index])
    }

    /// Total encoded PK width — the PK region's per-row stride.
    pub fn pk_stride(&self) -> usize {
        self.pk_stride
    }
}

impl SchemaFacts for TestSchema {
    fn locate(&self, ci: usize) -> ColumnLocator {
        let (type_code, _) = self.cols[ci];
        let size = gnitz_wire::wire_stride(type_code) as u8;
        if self.pk.contains(&ci) {
            ColumnLocator::Pk {
                byte_off: self.addr[ci],
                size,
                type_code,
            }
        } else {
            ColumnLocator::Payload {
                slot: self.addr[ci],
                size,
                type_code,
            }
        }
    }
    fn payload_col_idx(&self, pi: usize) -> usize {
        self.payload_to_ci[pi]
    }
    fn num_payload_cols(&self) -> usize {
        self.payload_to_ci.len()
    }
    fn num_columns(&self) -> usize {
        self.cols.len()
    }
    fn col_type_code(&self, ci: usize) -> u8 {
        self.cols[ci].0
    }
    fn col_nullable(&self, ci: usize) -> bool {
        self.cols[ci].1
    }
}

/// Push one payload column per `schema` payload slot, sized from its declared
/// type, and return the widths.
fn push_payload_cols(v: &mut TestView, schema: &TestSchema) -> Vec<usize> {
    let widths: Vec<usize> = (0..schema.num_payload_cols())
        .map(|pi| schema.locate(schema.payload_col_idx(pi)).size())
        .collect();
    for &w in &widths {
        v.push_col(w);
    }
    widths
}

/// Write `pk` into every PK column of `row`, addressed through the schema's own
/// locator — offset, width and type code all come from `locate`, so a fixture
/// cannot disagree with the addressing the kernels use. A compound PK gets the
/// same source value in each column, truncated to that column's width; `pk` is a
/// bit pattern, so a signed column reads it two's-complement.
fn set_row_pk(v: &mut TestView, schema: &TestSchema, row: usize, pk: u64) {
    for ci in 0..schema.num_columns() {
        if let ColumnLocator::Pk {
            byte_off,
            size,
            type_code,
        } = schema.locate(ci)
        {
            v.set_pk_col(
                row,
                byte_off as usize,
                &pk.to_le_bytes()[..(size as usize).min(8)],
                type_code,
            );
        }
    }
}

/// Build a [`TestView`] of `(pk, null_word, payload values)` rows against
/// `schema`: each payload column is sized from its declared type and each row's
/// `i64` value is stored little-endian into the low bytes of its cell.
pub fn make_int_view(schema: &TestSchema, rows: &[(u64, u64, &[i64])]) -> TestView {
    let mut v = TestView::new(rows.len(), schema.pk_stride());
    let widths = push_payload_cols(&mut v, schema);
    for (row, &(pk, null_word, cols)) in rows.iter().enumerate() {
        set_row_pk(&mut v, schema, row, pk);
        v.set_null_word(row, null_word);
        for (pi, &val) in cols.iter().enumerate() {
            let w = widths[pi].min(8);
            v.set_payload(row, pi, &val.to_le_bytes()[..w]);
        }
    }
    v
}

/// Build a [`TestView`] whose payload columns are German strings: `rows[r][c]`
/// is row `r`'s value for payload slot `c`. PKs are `1..=n`, nothing is NULL.
pub fn make_string_view(schema: &TestSchema, rows: &[&[&[u8]]]) -> TestView {
    let mut v = TestView::new(rows.len(), schema.pk_stride());
    push_payload_cols(&mut v, schema);
    for (row, cells) in rows.iter().enumerate() {
        set_row_pk(&mut v, schema, row, row as u64 + 1);
        for (pi, s) in cells.iter().enumerate() {
            v.set_string(row, pi, s);
        }
    }
    v
}

/// A `U64` PK plus `n` `I64` payload columns, all `nullable` or all not — the
/// shape almost every kernel test wants.
pub fn schema_pk_ints(n: usize, nullable: bool) -> TestSchema {
    schema_pk_cols(tc::I64, n, nullable)
}

/// A `U64` PK plus `n` `STRING` payload columns.
pub fn schema_pk_strings(n: usize, nullable: bool) -> TestSchema {
    schema_pk_cols(tc::STRING, n, nullable)
}

fn schema_pk_cols(payload_tc: u8, n: usize, nullable: bool) -> TestSchema {
    let mut cols = vec![(tc::U64, false)];
    cols.extend(std::iter::repeat_n((payload_tc, nullable), n));
    TestSchema::new(&cols, &[0])
}

/// A one-row view over `schema`: payload slot `i` holds `vals[i]`, and the
/// row's whole null word is `null_word` (bit `i` = payload slot `i` is NULL).
pub fn make_int_row(schema: &TestSchema, vals: &[i64], null_word: u64) -> TestView {
    make_int_view(schema, &[(1, null_word, vals)])
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
    let ncols = schema.num_payload_cols();
    let mut v = TestView::new(n, schema.pk_stride());
    push_payload_cols(&mut v, schema);
    for row in 0..n {
        set_row_pk(&mut v, schema, row, row as u64 + 1);
        let mut null_word = 0u64;
        for col in 0..ncols {
            gnitz_wire::null_word_set(&mut null_word, col, null_pred(row, col));
            v.set_payload(row, col, &f(row, col).to_le_bytes());
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
    num_regs: u32,
    result_reg: u32,
    const_strings: Vec<Vec<u8>>,
) -> Evaluator {
    LogicalProgram::new(instrs, num_regs, result_reg, const_strings)
        .resolve_scalar(schema)
        .expect("test program must validate")
}

/// Build and resolve a filter program — the arm where AND-chain detection runs
/// and `result_reg` stays bit_only-eligible. Same unwrap rule as
/// [`scalar_prog`].
pub fn filter_prog(
    schema: &TestSchema,
    instrs: Vec<LogicalInstr>,
    num_regs: u32,
    result_reg: u32,
    const_strings: Vec<Vec<u8>>,
) -> Evaluator {
    LogicalProgram::new(instrs, num_regs, result_reg, const_strings)
        .resolve_filter(schema)
        .expect("test predicate must validate")
}

/// Build and resolve a map program, checked against both the schema it reads
/// and the one it writes. Same unwrap rule as [`scalar_prog`].
pub fn map_prog(
    in_schema: &TestSchema,
    out_schema: &TestSchema,
    instrs: Vec<LogicalInstr>,
    num_regs: u32,
    result_reg: u32,
) -> Evaluator {
    LogicalProgram::new(instrs, num_regs, result_reg, vec![])
        .resolve_map(in_schema, out_schema)
        .expect("test map must validate")
}

/// Run `ev` as a filter and report a per-row verdict — the shape almost every
/// filter test wants, since `filter` reports runs rather than rows.
pub fn passing_rows(ev: &Evaluator, mb: &TestView, n: usize) -> Vec<bool> {
    let mut passed = vec![false; n];
    ev.filter(mb, n, |s, e| passed[s..e].fill(true));
    passed
}

/// The runs `ev` reports, verbatim. For tests whose subject is the range
/// stitching itself rather than which rows pass.
pub fn passing_ranges(ev: &Evaluator, mb: &TestView, n: usize) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    ev.filter(mb, n, |s, e| ranges.push((s, e)));
    ranges
}

/// Resolve one program twice: once as classification decides, once forced onto
/// the nullable arm. The forced side is a faithful baseline — `ensure_capacity`
/// re-reads `prog.no_nulls` on every drive and sizes the null buffers from it,
/// and the bit_only / bool_pack / chain_trigger masks are computed independently
/// of the arm — so it really runs the kernels the fast side skips.
///
/// The point is differential testing of programs that, after classification, can
/// no longer reach the nullable arm by construction. `label` names the subject in
/// the assertion that the A side is in fact the fast arm.
pub fn both_arms(label: &str, build: impl Fn() -> Evaluator) -> (Evaluator, Evaluator) {
    let fast = build();
    assert!(fast.prog.no_nulls, "{label}: the fast side must resolve no_nulls");
    let mut nullable = build();
    nullable.prog.no_nulls = false;
    (fast, nullable)
}

/// Reinterpret an `f64`'s bits as the `i64` a float register holds, and back —
/// spelled exactly as the kernel's own `encode_f64` / `decode_f64`, so a test
/// cannot disagree with it about the float-register encoding.
pub fn float_to_bits(f: f64) -> i64 {
    f64::to_bits(f) as i64
}

pub fn bits_to_float(bits: i64) -> f64 {
    f64::from_bits(bits as u64)
}

/// A three-row view with a compound `(U32, I64)` PK and payload slots
/// `0: I32`, `1: U128`, `2: U64`.
pub fn locator_fixture() -> TestView {
    let mut v = TestView::new(3, 12);
    assert_eq!(v.push_col(4), 0);
    assert_eq!(v.push_col(16), 1);
    assert_eq!(v.push_col(8), 2);
    for (row, (a, b)) in [(7u32, -1i64), (0, 0), (u32::MAX, i64::MIN)].into_iter().enumerate() {
        v.set_pk_col(row, 0, &a.to_le_bytes(), tc::U32);
        v.set_pk_col(row, 4, &b.to_le_bytes(), tc::I64);
        v.set_payload(row, 0, &(-3i32).to_le_bytes());
        v.set_payload(row, 1, &(1u128 << 100).to_le_bytes());
        v.set_payload(row, 2, &(row as u64).to_le_bytes());
    }
    v
}
