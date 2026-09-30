//! Owned-buffer stand-ins for a physical batch and schema, so the evaluator's
//! tests run entirely inside this crate.
//!
//! The store's and the client's batches both live in crates that depend on this
//! one, so neither is reachable from here — and driving every kernel through a
//! batch and a schema they were not written for is what holds the addressing
//! paths to the [`BatchView`] / [`ColumnTable`] contracts alone.

use std::fmt::Debug;

use gnitz_wire::TypeCode;

use crate::batch::MORSEL;
use crate::eval::Resolved;
use crate::{
    BatchView, ColumnTable, ExprResults, LogicalInstr, LogicalProgram, MapEval, MapTarget, Reg, RowFilter, RowSource,
    ScalarEval, SchemaFacts, Sink,
};

/// A [`BatchView`] over owned buffers, laid out region-wise like the physical
/// batch: one packed OPK PK region, one null-bitmap word per row, and one
/// contiguous buffer per payload slot. Also a [`MapTarget`], so a map writes
/// into the same layout a test reads through the production locators.
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
            v.set_key(schema, row, &vec![row as u128 + 1; schema.pk_cols().len()]);
        }
        v
    }

    /// Write `row`'s key from one native value per PK column, in PK-list order,
    /// through the schema's own key encoder.
    pub fn set_key(&mut self, schema: &TestSchema, row: usize, natives: &[u128]) {
        let key = schema.opk_key_cols(natives);
        self.pk[row * self.pk_stride..(row + 1) * self.pk_stride].copy_from_slice(key.pk_bytes());
    }

    /// Write `native` into the low bytes of `row`'s cell in payload slot `pi`.
    /// The cell stride comes from the column buffer, not from `native.len()`, so
    /// a narrower value lands correctly in a wider cell instead of shifting
    /// every subsequent row.
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

    /// A `len`-row view whose row `i` is this one's row `map(i)`. A string cell
    /// keeps its heap offset, so the blob is shared verbatim.
    pub fn tiled(&self, len: usize, map: impl Fn(usize) -> usize) -> TestView {
        let copy = |src: &[u8], w: usize| -> Vec<u8> {
            (0..len)
                .flat_map(|i| &src[map(i) * w..(map(i) + 1) * w])
                .copied()
                .collect()
        };
        TestView {
            rows: len,
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

/// `read(ev, mb)`, held to what makes a per-row result trustworthy beyond the
/// rows `mb` happens to hold: a row's result is a function of that row alone,
/// whichever word or morsel it lands in, and the `no_nulls` arm agrees with the
/// nullable one it skips. So every read is repeated over `mb`'s rows cycled
/// across several morsels, and over each row repeated to fill whole 64-row
/// words — the regime where a word-at-a-time kernel takes its uniform-word
/// shortcuts — on each arm the program can run.
fn checked<E: Resolved, T: PartialEq + Debug>(
    ev: &mut E,
    mb: &TestView,
    read: impl Fn(&mut E, &TestView) -> Vec<T>,
) -> Vec<T> {
    let n = mb.row_count();
    let want = read(ev, mb);
    let fast = ev.no_nulls();
    let arms: &[bool] = if fast { &[true, false] } else { &[false] };
    for &no_nulls in arms {
        ev.set_no_nulls(no_nulls);
        assert_eq!(read(ev, mb), want, "the nullable arm disagrees with no_nulls");
        if n == 0 {
            continue;
        }
        for (label, len, blocked) in [
            ("cycled", 2 * MORSEL + 37, false),
            ("word-blocked", n.min(128) * 64, true),
        ] {
            let map = |i: usize| if blocked { i / 64 } else { i % n };
            let got = read(ev, &mb.tiled(len, map));
            for (i, got) in got.iter().enumerate() {
                assert_eq!(
                    got,
                    &want[map(i)],
                    "{label} row {i} (base row {}), no_nulls={no_nulls}",
                    map(i)
                );
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

/// Run `ev` as a filter and report a per-row verdict — the shape almost every
/// filter test wants, since `RowFilter::ranges` reports runs rather than rows.
/// See [`checked`].
pub fn passing_rows(ev: &mut RowFilter, mb: &TestView) -> Vec<bool> {
    checked(ev, mb, |ev, mb| {
        let mut passed = vec![false; mb.row_count()];
        for (s, e) in passing_ranges(ev, mb) {
            passed[s..e].fill(true);
        }
        passed
    })
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
