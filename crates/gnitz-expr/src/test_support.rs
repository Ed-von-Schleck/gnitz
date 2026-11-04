//! The crate's test and bench helpers, around an owned-buffer stand-in for a
//! physical batch. The real one lives in a crate that depends on this one, and a
//! stand-in holds the kernels to the [`BatchView`] contract alone.

use std::convert::Infallible;
use std::fmt::Debug;

use gnitz_foundation::perf::Counter;
use gnitz_wire::TypeCode;

use crate::eval::Resolved;
use crate::morsel::MORSEL;
use crate::simd::encode_f64;
use crate::simd::Level;
use crate::{
    BatchView, ColumnLocator, ConstIdx, LogicalInstr, LogicalProgram, MapEval, MapTarget, Reg, RowFilter, ScalarCell,
    ScalarEval, SchemaColumn, SchemaDescriptor, Sink,
};
use gnitz_wire::{Heap, RowSource};

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
    pub fn for_schema(schema: &SchemaDescriptor, rows: usize) -> Self {
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
    pub fn set_native(&mut self, schema: &SchemaDescriptor, row: usize, ci: usize, native: u128) {
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
    fn set_payload(&mut self, row: usize, pi: usize, native: &[u8]) {
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

/// One payload column of a [`view`]: its type and each row's cell, `None` for
/// NULL.
#[derive(Clone)]
pub struct Col {
    tc: TypeCode,
    cells: Vec<Option<Cell>>,
}

/// An integer, a float's bit pattern, or a string.
#[derive(Clone)]
enum Cell {
    Num(i64),
    Str(Vec<u8>),
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
            false => Cell::Num(0x2A2A_2A2A_2A2A_2A2A),
        }
    }

    /// The instruction loading column `col` of this type into a register.
    pub fn load(&self, col: u32) -> LogicalInstr {
        match self.tc.is_german_string() {
            true => LogicalInstr::LoadColStr { col },
            false => LogicalInstr::LoadCol { col },
        }
    }
}

/// A column of `tc`, each value truncated to its cell.
pub fn int<T: Into<Option<i64>>>(tc: TypeCode, vals: impl IntoIterator<Item = T>) -> Col {
    let cells = vals.into_iter().map(|v| v.into().map(Cell::Num)).collect();
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
    let mut v = TestView::for_schema(&schema, cols[0].cells.len());
    for (pi, c) in cols.iter().enumerate() {
        for (row, cell) in c.cells.iter().enumerate() {
            match cell.clone().unwrap_or_else(|| c.poison()) {
                Cell::Num(x) => v.set_int(row, pi, x),
                Cell::Str(s) => v.set_string(row, pi, &s),
            }
            if cell.is_none() {
                v.set_null(row, pi);
            }
        }
    }
    (schema, v)
}

/// An `n`-row view over `schema`'s German-string payload columns: slot `c` of
/// `row` holds `cell(row, c)`, NULL when `null_pred(row, c)`. PKs are `1..=n`.
pub fn make_string_view<S: AsRef<[u8]>>(
    schema: &SchemaDescriptor,
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
    schema: &SchemaDescriptor,
    n: usize,
    f: impl Fn(usize, usize) -> i64,
    null_pred: impl Fn(usize, usize) -> bool,
) -> TestView {
    make_view(schema, n, |v, row, pi| v.set_int(row, pi, f(row, pi)), null_pred)
}

fn make_view(
    schema: &SchemaDescriptor,
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
/// several morsels, and each row repeated to fill a whole word.
fn placements(n: usize) -> [(&'static str, Vec<usize>); 2] {
    [
        ("cycled across morsels", (0..2 * MORSEL + 37).map(|i| i % n).collect()),
        ("filling whole words", (0..n.min(128) * 64).map(|i| i / 64).collect()),
    ]
}

/// `read(ev, mb)`, asserted to give each row the same result wherever it is
/// placed, on every arm the program can run and at every instruction set its
/// kernels can run at.
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
    // The resolved null tracking, and every register's.
    let resolved = ev.null_regs();
    for (level_name, level) in simd_levels() {
        for null_regs in [resolved, u64::MAX] {
            ev.set_arm(null_regs, level);
            let arm = format!("null_regs={null_regs:#x} at {level_name}");
            assert_eq!(read(ev, mb), want, "{arm}");
            for (label, source) in placements(n) {
                let got = read(ev, &mb.tiled(&source));
                for (i, (got, &r)) in got.iter().zip(&source).enumerate() {
                    assert_eq!(got, &want[r], "{label}: row {i} is row {r}, {arm}");
                }
            }
        }
    }
    ev.set_arm(resolved, Level::new());
    want
}

/// `take` of every row's result of a scalar program — see [`checked`].
fn row_cells<T: PartialEq + Debug>(ev: &mut ScalarEval, mb: &TestView, take: impl Fn(ScalarCell<'_>) -> T) -> Vec<T> {
    checked(ev, mb, |ev, mb| {
        let mut out = Vec::new();
        ev.for_each(mb, |_, cell| {
            out.push(take(cell));
            Ok::<_, Infallible>(())
        })
        .unwrap();
        out
    })
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
pub fn verdicts(
    schema: &SchemaDescriptor,
    instrs: Vec<LogicalInstr>,
    consts: Vec<Vec<u8>>,
    mb: &TestView,
) -> Vec<Option<i128>> {
    let vals = row_values(&mut scalar_prog(schema, instrs.clone(), consts.clone()), mb);
    let passed = passing_rows(&mut filter_prog(schema, instrs, consts), mb);
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
    for (row, _) in verdicts.iter().enumerate().filter(|(_, &v)| v) {
        match out.last_mut() {
            Some((_, e)) if *e == row => *e += 1,
            _ => out.push((row, row + 1)),
        }
    }
    out
}

/// Each row a map writes into `out`, three rows into it: the row's null word
/// and every payload slot's cell, a string slot read as its bytes — see
/// [`checked`]. A copied slot is the caller's to move, so its cell reads zero.
pub fn map_rows(ev: &mut MapEval, out: &SchemaDescriptor, mb: &TestView) -> Vec<(u64, Vec<Vec<u8>>)> {
    const DST_START: usize = 3;
    checked(ev, mb, |ev, mb| {
        let n = mb.row_count();
        let mut dst = TestView::for_schema(out, n + DST_START);
        ev.write_computed(mb, 0, n, &mut dst, DST_START);
        (DST_START..DST_START + n)
            .map(|row| {
                let cells = (0..out.num_payload_cols())
                    .map(|pi| match out.locate(out.payload_col_idx(pi)) {
                        ColumnLocator::Payload { type_code, .. } if type_code.is_german_string() => {
                            gnitz_wire::payload_bytes(&dst, row, pi).into_owned()
                        }
                        loc => dst.get_col_ptr(row, pi, loc.size()).to_vec(),
                    })
                    .collect();
                (dst.get_null_word(row), cells)
            })
            .collect()
    })
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

/// One step of a xorshift generator over `state`: the deterministic stream the randomized
/// tests draw from.
pub(crate) fn xorshift(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
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
