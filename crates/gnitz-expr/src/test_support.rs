//! The crate's test and bench helpers, around an owned-buffer stand-in for a
//! physical batch. The real one lives in a crate that depends on this one, and a
//! stand-in holds the kernels to the [`BatchView`] contract alone.

use std::fmt::Debug;

use gnitz_foundation::perf::Counter;
use gnitz_wire::TypeCode;

use crate::eval::Resolved;
use crate::morsel::MORSEL;
use crate::simd::Level;
use crate::simd::encode_f64;
use crate::{
    BatchView, ColumnLocator, ConstIdx, LogicalInstr, LogicalProgram, MapEval, MapTarget, Reg, RowFilter, ScalarCell,
    ScalarEval, SchemaColumn, SchemaDescriptor, Sink,
};
use gnitz_wire::{Heap, RowSource};

/// A [`BatchView`] and [`MapTarget`] over owned buffers, laid out region-wise
/// like the physical batch, holding the schema its cells are addressed by.
pub struct TestView {
    schema: SchemaDescriptor,
    rows: usize,
    pk: Vec<u8>,
    nulls: Vec<u8>,
    cols: Vec<Vec<u8>>,
    /// The variable-length heap the 16-byte German-string cells point into.
    blob: Vec<u8>,
}

impl TestView {
    /// `rows` rows over `schema`: one payload column per slot, sized from its
    /// declared type, every PK column of row `r` holding `r + 1`, nothing NULL.
    pub fn new(schema: &SchemaDescriptor, rows: usize) -> Self {
        let mut v = TestView {
            schema: *schema,
            rows,
            pk: vec![0u8; rows * schema.pk_stride()],
            nulls: vec![0u8; rows * 8],
            cols: (0..schema.num_payload_cols())
                .map(|pi| vec![0u8; rows * schema.payload_locator(pi).size()])
                .collect(),
            blob: Vec::new(),
        };
        for row in 0..rows {
            for &ci in schema.pk_cols() {
                v.set(row, ci as usize, row as u128 + 1);
            }
        }
        v
    }

    pub fn schema(&self) -> &SchemaDescriptor {
        &self.schema
    }

    /// Write column `ci` of `row`: a PK column OPK-encoded, a German-string
    /// column as a cell over this view's blob, any other column as the low
    /// bytes of the integer. The cell's variant must match the column's type.
    pub fn set(&mut self, row: usize, ci: usize, cell: impl Into<Cell>) {
        let cell = cell.into();
        debug_assert_eq!(
            matches!(cell, Cell::Str(_)),
            self.schema.columns()[ci].type_code.is_german_string(),
            "column {ci}: a cell of the other kind"
        );
        match (self.schema.locate(ci), cell) {
            (ColumnLocator::Pk { byte_off, size, type_code }, Cell::Int(v)) => {
                let at = row * self.schema.pk_stride() + byte_off as usize;
                gnitz_wire::store_opk(
                    &mut self.pk[at..at + size as usize],
                    gnitz_wire::key_image(type_code, v),
                );
            }
            (ColumnLocator::Payload { slot, size, .. }, Cell::Int(v)) => {
                self.write(row, slot as usize, &v.to_le_bytes()[..size as usize])
            }
            (ColumnLocator::Payload { slot, .. }, Cell::Str(s)) => {
                let cell = gnitz_wire::encode_german_string(&s, &mut self.blob);
                self.write(row, slot as usize, &cell);
            }
            (ColumnLocator::Pk { .. }, Cell::Str(_)) => panic!("column {ci}: a string into a key column"),
        }
    }

    /// Write `native` into the low bytes of `row`'s cell in payload slot `pi`.
    fn write(&mut self, row: usize, pi: usize, native: &[u8]) {
        let base = row * self.stride(pi);
        self.cols[pi][base..base + native.len()].copy_from_slice(native);
    }

    /// Set the null bit of payload column `ci` in `row`.
    pub fn set_null(&mut self, row: usize, ci: usize) {
        let slot = self.schema.payload_slot(ci).expect("a key column is never NULL");
        let mut word = self.get_null_word(row);
        gnitz_wire::null_word_set(&mut word, slot, true);
        self.set_null_word(row, word);
    }

    /// Overwrite `row`'s whole null word (bit N = payload slot N is NULL).
    pub fn set_null_word(&mut self, row: usize, word: u64) {
        gnitz_wire::write_u64_le(&mut self.nulls, row * 8, word);
    }

    /// Empty the heap a map appends its long strings to, keeping its capacity.
    pub fn clear_blob(&mut self) {
        self.blob.clear();
    }

    /// A view whose row `i` is this one's row `source[i]`. A string cell keeps
    /// its heap offset, so the blob is shared verbatim.
    fn tiled(&self, source: &[usize]) -> TestView {
        let copy = |src: &[u8], w: usize| -> Vec<u8> {
            source.iter().flat_map(|&r| &src[r * w..(r + 1) * w]).copied().collect()
        };
        TestView {
            schema: self.schema,
            rows: source.len(),
            pk: copy(&self.pk, self.schema.pk_stride()),
            nulls: copy(&self.nulls, 8),
            cols: (0..self.cols.len())
                .map(|pi| copy(&self.cols[pi], self.stride(pi)))
                .collect(),
            blob: self.blob.clone(),
        }
    }

    fn stride(&self, pi: usize) -> usize {
        self.schema.payload_locator(pi).size()
    }
}

impl RowSource for TestView {
    fn get_pk_bytes(&self, row: usize) -> &[u8] {
        let stride = self.schema.pk_stride();
        &self.pk[row * stride..(row + 1) * stride]
    }
    fn get_null_word(&self, row: usize) -> u64 {
        gnitz_wire::read_u64_le(&self.nulls, row * 8)
    }
    fn get_col_ptr(&self, row: usize, payload_col: usize, col_size: usize) -> &[u8] {
        &self.cols[payload_col][row * col_size..row * col_size + col_size]
    }
    fn heap(&self) -> Heap<'_> {
        Heap::Plain(&self.blob)
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
        (&self.pk, self.schema.pk_stride())
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

/// One cell's value: an integer or a float's bit pattern, or a string's bytes.
#[derive(Clone)]
pub enum Cell {
    Int(u128),
    Str(Vec<u8>),
}

/// Sign-extended, so a narrower cell keeps the low bytes of the value.
impl From<i64> for Cell {
    fn from(v: i64) -> Self {
        Cell::Int(v as i128 as u128)
    }
}

impl From<u128> for Cell {
    fn from(v: u128) -> Self {
        Cell::Int(v)
    }
}

macro_rules! str_cell {
    ($($t:ty),*) => {$(
        impl From<$t> for Cell {
            fn from(s: $t) -> Self {
                Cell::Str(AsRef::<[u8]>::as_ref(&s).to_vec())
            }
        }
    )*};
}
str_cell!(&str, &String, &[u8], &Vec<u8>);

impl From<Vec<u8>> for Cell {
    fn from(s: Vec<u8>) -> Self {
        Cell::Str(s)
    }
}

impl From<String> for Cell {
    fn from(s: String) -> Self {
        Cell::Str(s.into_bytes())
    }
}

impl<const N: usize> From<&[u8; N]> for Cell {
    fn from(s: &[u8; N]) -> Self {
        Cell::Str(s.to_vec())
    }
}

/// One payload column of a [`view`]: its type and each row's cell, `None` for
/// NULL.
#[derive(Clone)]
pub struct Col {
    pub tc: TypeCode,
    cells: Vec<Option<Cell>>,
}

impl Col {
    fn nullable(&self) -> bool {
        self.cells.iter().any(Option::is_none)
    }

    /// What a NULL cell stores: a value no live row holds, truthy and past the
    /// string inline threshold, so a kernel that read it shows in its result.
    fn poison(&self) -> Cell {
        match self.tc.is_german_string() {
            true => Cell::Str(b"poison-in-a-null-cell".to_vec()),
            false => Cell::Int(0x2A2A_2A2A_2A2A_2A2A),
        }
    }
}

/// A column of `tc`, each value truncated to its cell.
pub fn int<T: Into<Option<i64>>>(tc: TypeCode, vals: impl IntoIterator<Item = T>) -> Col {
    let cells = vals.into_iter().map(|v| v.into().map(Cell::from)).collect();
    Col { tc, cells }
}

pub fn i64s<T: Into<Option<i64>>>(vals: impl IntoIterator<Item = T>) -> Col {
    int(TypeCode::I64, vals)
}

pub fn f64s<T: Into<Option<f64>>>(vals: impl IntoIterator<Item = T>) -> Col {
    int(TypeCode::F64, vals.into_iter().map(|v| v.into().map(encode_f64)))
}

pub fn text<'a, T: Into<Option<&'a str>>>(vals: impl IntoIterator<Item = T>) -> Col {
    strings(vals.into_iter().map(|v| v.into().map(|s| s.as_bytes().to_vec())))
}

/// A `STRING` column of each row's bytes, `None` for NULL.
pub fn strings(cells: impl IntoIterator<Item = Option<Vec<u8>>>) -> Col {
    let cells = cells.into_iter().map(|c| c.map(Cell::Str)).collect();
    Col { tc: TypeCode::String, cells }
}

/// A `U64` PK numbered from 1, then `cols` as the payload, each nullable iff it
/// holds a NULL — and the view holding their rows.
pub fn view(cols: &[Col]) -> (SchemaDescriptor, TestView) {
    let mut schema_cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    schema_cols.extend(cols.iter().map(|c| SchemaColumn::new(c.tc, c.nullable())));
    let schema = SchemaDescriptor::new(&schema_cols, &[0]);
    let mut v = TestView::new(&schema, cols[0].cells.len());
    for (ci, c) in (1..).zip(cols) {
        for (row, cell) in c.cells.iter().enumerate() {
            v.set(row, ci, cell.clone().unwrap_or_else(|| c.poison()));
            if cell.is_none() {
                v.set_null(row, ci);
            }
        }
    }
    (schema, v)
}

/// An `n`-row view over `schema`, PKs `1..=n`: payload slot `pi` of `row` holds
/// `cell(row, pi)`, and its null bit is `null(row, pi)` whether or not the
/// column is nullable. The one entry point addressed by payload slot rather
/// than by schema column.
pub fn fill<C: Into<Cell>>(
    schema: &SchemaDescriptor,
    n: usize,
    cell: impl Fn(usize, usize) -> C,
    null: impl Fn(usize, usize) -> bool,
) -> TestView {
    let mut v = TestView::new(schema, n);
    for row in 0..n {
        let mut null_word = 0u64;
        for pi in 0..schema.num_payload_cols() {
            gnitz_wire::null_word_set(&mut null_word, pi, null(row, pi));
            v.set(row, schema.payload_col_idx(pi), cell(row, pi));
        }
        v.set_null_word(row, null_word);
    }
    v
}

/// A `U64` PK plus a payload column of each of `payload`, all `nullable` or all
/// not.
pub fn schema_pk(payload: &[TypeCode], nullable: bool) -> SchemaDescriptor {
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend(payload.iter().map(|&tc| SchemaColumn::new(tc, nullable)));
    SchemaDescriptor::new(&cols, &[0])
}

/// A `U64` PK plus `n` `I64` payload columns — the shape almost every kernel
/// test wants.
pub fn schema_pk_ints(n: usize, nullable: bool) -> SchemaDescriptor {
    schema_pk(&vec![TypeCode::I64; n], nullable)
}

/// A `U64` PK plus `n` `STRING` payload columns.
pub fn schema_pk_strings(n: usize, nullable: bool) -> SchemaDescriptor {
    schema_pk(&vec![TypeCode::String; n], nullable)
}

/// `instrs` as a program whose one register sink is its last instruction.
pub(crate) fn result_prog(instrs: Vec<LogicalInstr>, const_strings: Vec<Vec<u8>>) -> LogicalProgram {
    let result = Reg(instrs.len() as u16 - 1);
    LogicalProgram::new(instrs, vec![Sink::Reg(result)], const_strings).expect("test program must validate")
}

/// Build and resolve a scalar program; its result is its last instruction. A
/// test program that fails validation is a bug in the test, so it unwraps here
/// rather than at every site.
pub fn scalar_prog(schema: &SchemaDescriptor, instrs: Vec<LogicalInstr>, const_strings: Vec<Vec<u8>>) -> ScalarEval {
    result_prog(instrs, const_strings)
        .resolve_scalar(schema)
        .expect("test program must validate")
}

/// Build and resolve a filter program; its verdict is its last instruction.
/// Same unwrap rule as [`scalar_prog`].
pub fn filter_prog(schema: &SchemaDescriptor, instrs: Vec<LogicalInstr>, const_strings: Vec<Vec<u8>>) -> RowFilter {
    result_prog(instrs, const_strings)
        .resolve_filter(schema)
        .expect("test predicate must validate")
}

/// Build and resolve a map program, checked against both the schema it reads
/// and the one it writes. Same unwrap rule as [`scalar_prog`].
pub fn map_prog(
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    instrs: Vec<LogicalInstr>,
    sinks: Vec<Sink>,
    const_strings: Vec<Vec<u8>>,
) -> MapEval {
    LogicalProgram::new(instrs, sinks, const_strings)
        .and_then(|p| p.resolve_map(in_schema, out_schema))
        .expect("test map must validate")
}

/// Which of `n` rows each row of a longer batch copies: the rows cycled across
/// several morsels, and each row repeated to fill a whole word; none of an
/// empty batch.
fn placements(n: usize) -> Vec<(&'static str, Vec<usize>)> {
    if n == 0 {
        return Vec::new();
    }
    vec![
        ("cycled across morsels", (0..2 * MORSEL + 37).map(|i| i % n).collect()),
        ("filling whole words", (0..n.min(128) * 64).map(|i| i / 64).collect()),
    ]
}

/// `read(ev, mb)` on every arm the program can run and at every instruction set
/// its kernels can run at, asserted equal to the resolved arm's; returns that.
pub fn arms_agree<E: Resolved, T: PartialEq + Debug>(
    ev: &mut E,
    mb: &TestView,
    read: impl Fn(&mut E, &TestView) -> Vec<T>,
) -> Vec<T> {
    let want = read(ev, mb);
    // The resolved null tracking, and every register's.
    let resolved = ev.null_regs();
    for (level_name, level) in simd_levels() {
        for null_regs in [resolved, u64::MAX] {
            ev.set_arm(null_regs, level);
            assert_eq!(read(ev, mb), want, "null_regs={null_regs:#x} at {level_name}");
        }
    }
    ev.set_arm(resolved, Level::new());
    want
}

/// [`arms_agree`] over a `read` asserted, on each arm, to give each row the
/// same result wherever it is placed.
fn checked<E: Resolved, T: PartialEq + Debug>(
    ev: &mut E,
    mb: &TestView,
    read: impl Fn(&mut E, &TestView) -> Vec<T>,
) -> Vec<T> {
    arms_agree(ev, mb, |ev, mb| {
        let want = read(ev, mb);
        for (label, source) in placements(mb.row_count()) {
            let got = read(ev, &mb.tiled(&source));
            for (i, (got, &r)) in got.iter().zip(&source).enumerate() {
                assert_eq!(got, &want[r], "{label}: row {i} is row {r}");
            }
        }
        want
    })
}

/// `take` of every row's result of a scalar program, on the arm it is on.
pub fn scalar_cells<T>(ev: &mut ScalarEval, mb: &TestView, take: impl Fn(ScalarCell<'_>) -> T) -> Vec<T> {
    let mut out = Vec::new();
    ev.for_each(mb, |cell| out.push(take(cell)));
    out
}

/// `take` of every row's result of a scalar program — see [`checked`].
fn row_cells<T: PartialEq + Debug>(ev: &mut ScalarEval, mb: &TestView, take: impl Fn(ScalarCell<'_>) -> T) -> Vec<T> {
    checked(ev, mb, |ev, mb| scalar_cells(ev, mb, &take))
}

/// Every row's integer result of a scalar program — see [`checked`].
pub fn row_values(ev: &mut ScalarEval, mb: &TestView) -> Vec<Option<i128>> {
    row_cells(ev, mb, |cell| match cell {
        ScalarCell::Int(v) => v,
        ScalarCell::Str(_) => panic!("row_values over a string-valued program; use row_strs"),
    })
}

/// Every row's string result of a scalar program, `None` for NULL — see
/// [`checked`].
pub fn row_strs(ev: &mut ScalarEval, mb: &TestView) -> Vec<Option<Vec<u8>>> {
    row_cells(ev, mb, |cell| match cell {
        ScalarCell::Str(s) => s.map(<[u8]>::to_vec),
        ScalarCell::Int(_) => panic!("row_strs over a scalar program; use row_values"),
    })
}

/// Every row's value of `instrs` read as a scalar, asserted to be the same
/// program's filter verdict — truthy and not NULL. A register only a filter
/// reads is written as packed bits and never as lanes, so the two readings run
/// different kernels.
pub fn verdicts(instrs: Vec<LogicalInstr>, consts: Vec<Vec<u8>>, mb: &TestView) -> Vec<Option<i128>> {
    let vals = row_values(&mut scalar_prog(mb.schema(), instrs.clone(), consts.clone()), mb);
    let passed = passing_rows(&mut filter_prog(mb.schema(), instrs, consts), mb);
    let truthy: Vec<bool> = vals.iter().map(|v| v.is_some_and(|x| x != 0)).collect();
    assert_eq!(passed, truthy, "the filter verdicts against the values");
    vals
}

/// Each row's filter verdict — see [`checked`].
pub fn passing_rows(ev: &mut RowFilter, mb: &TestView) -> Vec<bool> {
    checked(ev, mb, |ev, mb| filter_verdicts(ev, mb))
}

/// Each row's verdict from one [`RowFilter::ranges`], whose runs are asserted
/// to be exactly the maximal runs of those verdicts: ordered, non-empty, apart
/// and inside the batch.
pub fn filter_verdicts(ev: &mut RowFilter, mb: &dyn BatchView) -> Vec<bool> {
    let ranges = ev.ranges(mb);
    let mut passed = vec![false; mb.row_count()];
    for &(s, e) in &*ranges {
        assert!(
            s < e && e <= passed.len(),
            "run ({s}, {e}) is not a non-empty run of the batch"
        );
        passed[s..e].fill(true);
    }
    assert_eq!(
        *ranges,
        runs(&passed),
        "the runs are not the maximal runs of their rows"
    );
    passed
}

/// The maximal runs of `true` in `verdicts`, half-open.
fn runs(verdicts: &[bool]) -> Vec<(usize, usize)> {
    let mut out: Vec<(usize, usize)> = Vec::new();
    for (row, _) in verdicts.iter().enumerate().filter(|&(_, &v)| v) {
        match out.last_mut() {
            Some((_, e)) if *e == row => *e += 1,
            _ => out.push((row, row + 1)),
        }
    }
    out
}

/// Each row a map writes into `out`, three rows into it, on the arm it is on:
/// the row's null word and every payload slot's cell, a string slot read as
/// its bytes. A copied slot is the caller's to move, so its cell reads zero.
pub fn map_cells(ev: &mut MapEval, out: &SchemaDescriptor, mb: &TestView) -> Vec<(u64, Vec<Vec<u8>>)> {
    const DST_START: usize = 3;
    let n = mb.row_count();
    let mut dst = TestView::new(out, n + DST_START);
    ev.write_computed(mb, 0, n, &mut dst, DST_START);
    (DST_START..DST_START + n)
        .map(|row| {
            let cells = (0..out.num_payload_cols())
                .map(|pi| {
                    let loc = out.payload_locator(pi);
                    match loc.type_code().is_german_string() {
                        true => loc.content(&dst, row).into_owned(),
                        false => loc.bytes(&dst, row).to_vec(),
                    }
                })
                .collect();
            (dst.get_null_word(row), cells)
        })
        .collect()
}

/// [`map_cells`] — see [`checked`].
pub fn map_rows(ev: &mut MapEval, out: &SchemaDescriptor, mb: &TestView) -> Vec<(u64, Vec<Vec<u8>>)> {
    checked(ev, mb, |ev, mb| map_cells(ev, out, mb))
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
pub(crate) fn simd_levels() -> Vec<(&'static str, Level)> {
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

/// Every sequence of up to `max` symbols over `alphabet`.
pub fn words<T: Copy>(alphabet: &[T], max: u32) -> Vec<Vec<T>> {
    (0..=max)
        .flat_map(|len| {
            (0..alphabet.len().pow(len)).map(move |code| {
                (0..len)
                    .map(|i| alphabet[code / alphabet.len().pow(i) % alphabet.len()])
                    .collect()
            })
        })
        .collect()
}

/// Every ordered pair of `corpus`, as two operand columns.
pub fn pairs<T: Copy>(corpus: &[T]) -> (Vec<T>, Vec<T>) {
    corpus.iter().flat_map(|&x| corpus.iter().map(move |&y| (x, y))).unzip()
}

/// Key values that tie, that differ only in the low half of 16 bytes, that cross
/// each width's sign bit, and one whose every byte differs.
pub(crate) const PATTERNS: &[u128] = &[
    0,
    1,
    0x7F,
    0x80,
    0xFF,
    0x7FFF,
    0x8000,
    1 << 31,
    i64::MAX as u128,
    1 << 63,
    u64::MAX as u128,
    0x0123_4567_89AB_CDEF,
    1 << 64,
    (1 << 64) + 1,
    i128::MAX as u128,
    1 << 127,
    u128::MAX,
];

/// The counters every instruction bench reads its cases through. The benches
/// are `#[ignore]`d and meaningful only in release:
///
///   cargo test -p gnitz-expr --release <bench> -- --ignored --nocapture --test-threads=1
///
/// Each case prints the user-mode instructions and cycles per row of one warmed
/// pass, at every instruction set its kernels can run at: `native`, and `avx2`
/// on a CPU that has more, which is what a CPU without it runs. Between levels
/// compare cycles: a wider instruction retires as one whatever it costs to
/// execute.
pub(crate) struct Meter {
    instructions: Counter,
    cycles: Counter,
}

impl Meter {
    pub(crate) fn new() -> Self {
        Meter {
            instructions: Counter::instructions(),
            cycles: Counter::cycles(),
        }
    }

    /// Print `case`'s instructions and cycles per row of `pass`, which covers
    /// `rows` rows, after one pass of warm-up.
    pub(crate) fn pass<T>(&self, bench: &str, case: &str, rows: usize, mut pass: impl FnMut() -> T) {
        std::hint::black_box(pass());
        let ((_, instructions), cycles) = self
            .cycles
            .measure(|| self.instructions.measure(|| std::hint::black_box(pass())));
        println!(
            "{bench} {case:<36} {:8.2} instr/row {:8.2} cycles/row",
            instructions as f64 / rows as f64,
            cycles as f64 / rows as f64
        );
    }

    /// [`Self::pass`] of `ev` at every instruction set of [`simd_levels`], on
    /// the arm it is on.
    pub(crate) fn levels<E: Resolved, T>(
        &self,
        bench: &str,
        case: &str,
        rows: usize,
        ev: &mut E,
        mut pass: impl FnMut(&mut E) -> T,
    ) {
        let null_regs = ev.null_regs();
        for (level_name, level) in simd_levels() {
            ev.set_arm(null_regs, level);
            self.pass(bench, &format!("{case} @{level_name}"), rows, || pass(ev));
        }
        ev.set_arm(null_regs, Level::new());
    }

    /// [`Self::levels`] of a filter's `ranges` over `view`.
    pub(crate) fn filter(&self, bench: &str, case: &str, f: &mut RowFilter, view: &TestView) {
        self.levels(bench, case, view.row_count(), f, |f| f.ranges(view).len());
    }
}

/// The rows a batch of the evaluator benches holds, from `GNITZ_BENCH_ROWS`: a
/// count below [`MORSEL`] measures what a small delta pays per batch.
pub(crate) fn bench_rows() -> usize {
    gnitz_foundation::env::env_num("GNITZ_BENCH_ROWS", 200_000)
}

/// `col1 IN (…)` over `set`, which is sorted and distinct.
pub(crate) fn in_set(set: impl IntoIterator<Item = i64>) -> (Vec<LogicalInstr>, Vec<Vec<u8>>) {
    let set: Vec<i64> = set.into_iter().collect();
    let instrs = vec![
        LogicalInstr::LoadCol { col: 1 },
        LogicalInstr::IntInSet { value_reg: Reg(0), set_idx: ConstIdx(0) },
    ];
    (instrs, vec![gnitz_wire::as_le_bytes(&set).to_vec()])
}

/// `ranges_in` over a window of `v` names the runs `ranges` names over the whole
/// of `v`, cut to the window, whatever the window's alignment to a word.
pub(crate) fn assert_windows_match_whole(f: &mut crate::RowFilter, v: &dyn BatchView) {
    let n = v.row_count();
    let whole = f.ranges(v).to_vec();
    for (s, e) in [
        (0, n),
        (n.min(1), n),
        (n.min(37), n.min(101)),
        (n.min(64), n),
        (n / 2, n / 2),
    ] {
        let want: Vec<(usize, usize)> = whole
            .iter()
            .map(|&(a, b)| (a.max(s), b.min(e)))
            .filter(|&(a, b)| a < b)
            .collect();
        assert_eq!(f.ranges_in(v, s..e), want, "window {s}..{e}");
    }
}
