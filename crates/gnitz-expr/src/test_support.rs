//! Owned-buffer stand-ins for a physical batch and schema. The real ones live in
//! crates that depend on this one, and a stand-in holds the kernels to the
//! [`BatchView`] / [`ColumnTable`] contracts alone.

use std::fmt::Debug;

use gnitz_wire::TypeCode;

use crate::batch::MORSEL;
use crate::eval::Resolved;
use crate::{
    BatchView, ColumnLocator, ColumnTable, ExprResults, LogicalInstr, LogicalProgram, MapEval, MapTarget, Reg,
    RowFilter, RowSource, ScalarEval, SchemaFacts, Sink,
};

/// A [`BatchView`] and [`MapTarget`] over owned buffers, laid out region-wise
/// like the physical batch.
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
    /// `rows` rows over `schema`: one payload column per slot, sized from its
    /// declared type, every PK column of row `r` holding `r + 1`, nothing NULL.
    pub fn for_schema(schema: &TestSchema, rows: usize) -> Self {
        let mut v = TestView {
            rows,
            pk_stride: schema.pk_stride(),
            pk: vec![0u8; rows * schema.pk_stride()],
            nulls: vec![0u8; rows * 8],
            cols: (0..schema.num_payload_cols())
                .map(|pi| vec![0u8; rows * schema.locate(schema.payload_col_idx(pi)).size()])
                .collect(),
            blob: Vec::new(),
        };
        for row in 0..rows {
            for &ci in schema.pk_cols() {
                v.set_native(schema, row, ci as usize, row as u128 + 1);
            }
        }
        v
    }

    /// Write column `ci` of `row` from the low bytes of `native`, wherever the
    /// schema places the column.
    pub fn set_native(&mut self, schema: &TestSchema, row: usize, ci: usize, native: u128) {
        let loc = schema.locate(ci);
        match loc {
            ColumnLocator::Pk { byte_off, size, type_code } => {
                let at = row * self.pk_stride + byte_off as usize;
                gnitz_wire::store_opk(&mut self.pk[at..at + size as usize], native, type_code.is_signed_int());
            }
            ColumnLocator::Payload { slot, size, .. } => {
                self.set_payload(row, slot as usize, &native.to_le_bytes()[..size as usize])
            }
        }
    }

    /// Write `native` into the low bytes of `row`'s cell in payload slot `pi`.
    pub fn set_payload(&mut self, row: usize, pi: usize, native: &[u8]) {
        let base = row * self.stride(pi);
        self.cols[pi][base..base + native.len()].copy_from_slice(native);
    }

    /// Store `val` little-endian into the low bytes of `row`'s cell in slot `pi`,
    /// truncated to a narrower cell.
    pub fn set_int(&mut self, row: usize, pi: usize, val: i64) {
        let w = self.stride(pi).min(8);
        self.set_payload(row, pi, &val.to_le_bytes()[..w]);
    }

    /// Encode `s` into this view's blob and store the resulting 16-byte
    /// German-string cell in payload slot `pi` of `row`.
    pub fn set_string(&mut self, row: usize, pi: usize, s: &[u8]) {
        let cell = gnitz_wire::encode_german_string(s, &mut self.blob);
        self.set_payload(row, pi, &cell);
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

    /// A view whose row `i` is this one's row `source[i]`. A string cell keeps
    /// its heap offset, so the blob is shared verbatim.
    pub fn tiled(&self, source: &[usize]) -> TestView {
        let copy = |src: &[u8], w: usize| -> Vec<u8> {
            source.iter().flat_map(|&r| &src[r * w..(r + 1) * w]).copied().collect()
        };
        TestView {
            rows: source.len(),
            pk_stride: self.pk_stride,
            pk: copy(&self.pk, self.pk_stride),
            nulls: copy(&self.nulls, 8),
            cols: (0..self.cols.len())
                .map(|pi| copy(&self.cols[pi], self.stride(pi)))
                .collect(),
            blob: self.blob.clone(),
        }
    }

    fn stride(&self, pi: usize) -> usize {
        self.cols[pi].len() / self.rows
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

impl MapTarget for TestView {
    fn null_bmp_mut(&mut self) -> &mut [u8] {
        &mut self.nulls
    }
    fn slot_mut(&mut self, pi: usize) -> (&mut [u8], &mut [u8], &mut Vec<u8>) {
        (&mut self.cols[pi], &mut self.nulls, &mut self.blob)
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

/// An `n`-row view over `schema`'s German-string payload columns: slot `c` of
/// `row` holds `cell(row, c)`, NULL when `null_pred(row, c)`. PKs are `1..=n`.
pub fn make_string_view<S: AsRef<[u8]>>(
    schema: &TestSchema,
    n: usize,
    cell: impl Fn(usize, usize) -> S,
    null_pred: impl Fn(usize, usize) -> bool,
) -> TestView {
    make_view(
        schema,
        n,
        |v, row, pi| v.set_string(row, pi, cell(row, pi).as_ref()),
        null_pred,
    )
}

/// An `n`-row view over `schema`'s integer payload columns: column `col` of
/// `row` holds `f(row, col)`, truncated to its cell, NULL when
/// `null_pred(row, col)`. PKs are `1..=n`.
pub fn make_n_col_view(
    schema: &TestSchema,
    n: usize,
    f: impl Fn(usize, usize) -> i64,
    null_pred: impl Fn(usize, usize) -> bool,
) -> TestView {
    make_view(schema, n, |v, row, pi| v.set_int(row, pi, f(row, pi)), null_pred)
}

fn make_view(
    schema: &TestSchema,
    n: usize,
    set: impl Fn(&mut TestView, usize, usize),
    null_pred: impl Fn(usize, usize) -> bool,
) -> TestView {
    let mut v = TestView::for_schema(schema, n);
    for row in 0..n {
        let mut null_word = 0u64;
        for pi in 0..schema.num_payload_cols() {
            gnitz_wire::null_word_set(&mut null_word, pi, null_pred(row, pi));
            set(&mut v, row, pi);
        }
        v.set_null_word(row, null_word);
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

/// `instrs` as a program whose one register sink is its last instruction.
fn result_prog(instrs: Vec<LogicalInstr>, const_strings: Vec<Vec<u8>>) -> LogicalProgram {
    let result = Reg(instrs.len() as u16 - 1);
    LogicalProgram::new(instrs, vec![Sink::Reg(result)], const_strings)
}

/// Build and resolve a scalar program; its result is its last instruction. A
/// test program that fails validation is a bug in the test, so it unwraps here
/// rather than at every site.
pub fn scalar_prog(schema: &TestSchema, instrs: Vec<LogicalInstr>, const_strings: Vec<Vec<u8>>) -> ScalarEval {
    result_prog(instrs, const_strings)
        .resolve_scalar(schema)
        .expect("test program must validate")
}

/// Build and resolve a filter program; its verdict is its last instruction.
/// Same unwrap rule as [`scalar_prog`].
pub fn filter_prog(schema: &TestSchema, instrs: Vec<LogicalInstr>, const_strings: Vec<Vec<u8>>) -> RowFilter {
    result_prog(instrs, const_strings)
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

/// Which of `n` rows each row of a longer batch copies: the rows cycled across
/// several morsels, and each row repeated to fill a whole word.
fn placements(n: usize) -> [(&'static str, Vec<usize>); 2] {
    [
        ("cycled across morsels", (0..2 * MORSEL + 37).map(|i| i % n).collect()),
        ("filling whole words", (0..n.min(128) * 64).map(|i| i / 64).collect()),
    ]
}

/// `read(ev, mb)`, asserted to give each row the same result wherever it is
/// placed and on every arm the program can run.
fn checked<E: Resolved, T: PartialEq + Debug>(
    ev: &mut E,
    mb: &TestView,
    read: impl Fn(&mut E, &TestView) -> Vec<T>,
) -> Vec<T> {
    let n = mb.row_count();
    let want = read(ev, mb);
    if n == 0 {
        return want;
    }
    let fast = ev.no_nulls();
    let arms: &[bool] = if fast { &[true, false] } else { &[false] };
    for &no_nulls in arms {
        ev.set_no_nulls(no_nulls);
        assert_eq!(read(ev, mb), want, "no_nulls={no_nulls}");
        for (label, source) in placements(n) {
            let got = read(ev, &mb.tiled(&source));
            for (i, (got, &r)) in got.iter().zip(&source).enumerate() {
                assert_eq!(got, &want[r], "{label}: row {i} is row {r}, no_nulls={no_nulls}");
            }
        }
    }
    ev.set_no_nulls(fast);
    want
}

/// Every row's integer result of a scalar program — see [`checked`].
pub fn row_values(ev: &mut ScalarEval, mb: &TestView) -> Vec<Option<i128>> {
    checked(ev, mb, |ev, mb| match ev.eval_all(mb) {
        ExprResults::Int(vals) => vals,
        ExprResults::Str { .. } => panic!("row_values over a string-valued program; use row_strs"),
    })
}

/// Every row's string result of a scalar program, `None` for NULL — see
/// [`checked`].
pub fn row_strs(ev: &mut ScalarEval, mb: &TestView) -> Vec<Option<Vec<u8>>> {
    checked(ev, mb, |ev, mb| match ev.eval_all(mb) {
        ExprResults::Str { bytes, spans } => spans.iter().map(|s| s.map(|(o, l)| bytes[o..o + l].to_vec())).collect(),
        ExprResults::Int(_) => panic!("row_strs over a scalar program; use row_values"),
    })
}

/// Each row's filter verdict — see [`checked`].
pub fn passing_rows(ev: &mut RowFilter, mb: &TestView) -> Vec<bool> {
    checked(ev, mb, |ev, mb| {
        let mut passed = vec![false; mb.row_count()];
        for (s, e) in passing_ranges(ev, mb) {
            passed[s..e].fill(true);
        }
        passed
    })
}

/// The maximal runs of `true` in `verdicts`, half-open.
pub fn runs(verdicts: &[bool]) -> Vec<(usize, usize)> {
    let mut out: Vec<(usize, usize)> = Vec::new();
    for (row, _) in verdicts.iter().enumerate().filter(|(_, &v)| v) {
        match out.last_mut() {
            Some((_, e)) if *e == row => *e += 1,
            _ => out.push((row, row + 1)),
        }
    }
    out
}

/// The runs `ev` reports, each checked to be a non-empty run inside the batch.
pub fn passing_ranges(ev: &mut RowFilter, mb: &TestView) -> Vec<(usize, usize)> {
    let stale = (usize::MAX, usize::MAX);
    let mut ranges = vec![stale];
    ev.ranges(mb, &mut ranges);
    let n = mb.row_count();
    for &(s, e) in &ranges {
        assert!(s < e && e <= n, "run ({s}, {e}) is not a non-empty run of 0..{n}");
    }
    ranges
}

pub fn is_null_op(col: u32) -> LogicalInstr {
    LogicalInstr::IsNull { col, invert: false }
}

pub fn is_not_null_op(col: u32) -> LogicalInstr {
    LogicalInstr::IsNull { col, invert: true }
}

/// Every instruction set [`crate::simd`]'s kernels can run at on this CPU: the
/// one it reports, and below it the one the build targets when the two differ —
/// the level a CPU without the wider set runs.
pub(crate) fn simd_levels() -> Vec<(&'static str, crate::simd::Level)> {
    use crate::simd::Level;
    #[allow(unused_mut)]
    let mut levels = vec![("native", Level::new())];
    // A build that itself targets the wider set compiles no narrower kernel.
    #[cfg(target_arch = "x86_64")]
    if let (Some(_), None, Some(avx2)) = (
        Level::new().as_avx512(),
        Level::baseline().as_avx512(),
        Level::new().as_avx2(),
    ) {
        levels.push(("avx2", Level::Avx2(avx2)));
    }
    levels
}

/// The level an evaluator's kernels run at under test: the CPU's, or the one
/// of [`simd_levels`] that `GNITZ_BENCH_LEVEL` names, so an evaluator bench
/// reads the level a CPU without the wider set runs. On such a CPU `avx2` is
/// the level it has.
pub(crate) fn eval_level() -> crate::simd::Level {
    let want = std::env::var("GNITZ_BENCH_LEVEL").unwrap_or_else(|_| "native".to_string());
    let levels = simd_levels();
    match levels.iter().find(|(name, _)| *name == want) {
        Some(&(_, level)) => level,
        None if want == "avx2" && levels[0].1.as_avx2().is_some() => levels[0].1,
        None => panic!(
            "GNITZ_BENCH_LEVEL={want:?} names no level of {:?}",
            levels.iter().map(|l| l.0).collect::<Vec<_>>()
        ),
    }
}
