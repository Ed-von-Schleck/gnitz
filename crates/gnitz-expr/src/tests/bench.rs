//! Retired-instruction benchmarks for the evaluator's kernels, `#[ignore]`d and
//! meaningful only in release. Build first, so `perf` does not count the
//! compile, then run a bench at two pass counts and difference them:
//!
//!   cargo build -p gnitz-expr --release --tests
//!   GNITZ_BENCH_PASSES=<1, then 201> <GNITZ_BENCH_* selectors> \
//!     perf stat -e instructions:u cargo test -p gnitz-expr --release <bench> \
//!     -- --ignored --nocapture --test-threads=1

use gnitz_wire::{FixedInt, TypeCode};

use crate::batch::MORSEL;
use crate::simd;

use crate::eval::Resolved;
use crate::test_support::{
    filter_prog, is_null_op, make_n_col_view, make_string_view, map_prog, passing_rows, scalar_prog, schema_pk_ints,
    schema_pk_strings, simd_levels, TestSchema, TestView,
};
use crate::{
    payload_u64, BatchView, CalendarOp, CmpOp, ConstIdx, ExprBuilder, IntArithOp, LikePattern, LogicalInstr,
    LogicalProgram, MapEval, Reg, RowFilter, ScalarEval, Sink,
};

/// The one case a `GNITZ_BENCH_*` variable drives, every case when unset; every
/// case is still built and checked. A value that names no case fails the bench,
/// listing the cases.
struct Selector {
    var: &'static str,
    value: String,
    cases: Vec<String>,
    hit: bool,
}

impl Selector {
    fn new(var: &'static str) -> Self {
        let value = std::env::var(var).unwrap_or_else(|_| "all".to_string());
        Selector {
            var,
            value,
            cases: Vec::new(),
            hit: false,
        }
    }

    fn drives(&mut self, case: &str) -> bool {
        if !self.cases.iter().any(|c| c == case) {
            self.cases.push(case.to_string());
        }
        let hit = self.value == "all" || self.value == case;
        self.hit |= hit;
        hit
    }
}

impl Drop for Selector {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            assert!(
                self.hit,
                "{}={:?} names no case of {:?}",
                self.var, self.value, self.cases
            );
        }
    }
}

/// `build()` twice: as resolved, which must be the `no_nulls` arm, and forced
/// onto the nullable arm — the pair an arm A/B drives.
fn both_arms<E: Resolved>(label: &str, build: impl Fn() -> E) -> (E, E) {
    let fast = build();
    assert!(fast.prog().no_nulls, "{label}: the fast side must resolve no_nulls");
    let mut nullable = build();
    nullable.set_no_nulls(false);
    (fast, nullable)
}

/// The register a program's result lands in: its last instruction's.
fn last(instrs: &[LogicalInstr]) -> Reg {
    Reg(instrs.len() as u16 - 1)
}

/// Run `f` over `view` `passes` times, the driven region of every filter bench.
fn drive_filter(f: &mut RowFilter, view: &TestView, passes: usize) {
    let mut ranges = Vec::new();
    let mut runs = 0usize;
    for _ in 0..passes {
        f.ranges(view, &mut ranges);
        runs += ranges.len();
    }
    std::hint::black_box(runs);
}

/// The fused `StrColConst` against the register channel (`GNITZ_BENCH_CHANNEL`)
/// on `col <op> 'const'` (`GNITZ_BENCH_OP`) over ~1M NOT NULL strings, per
/// `GNITZ_BENCH_DOMAIN`. `digits-first` and `abcd-shared-prefix` differ only in
/// whether the prefix the fused compare short-circuits on collides; `long` and
/// `long-distinct` only in whether the heap strings share theirs.
#[test]
#[ignore]
fn str_const_filter_bench() {
    let passes = bench_passes();
    let mut channel = Selector::new("GNITZ_BENCH_CHANNEL");
    let mut only = Selector::new("GNITZ_BENCH_DOMAIN");
    let mut only_op = Selector::new("GNITZ_BENCH_OP");

    let schema = schema_pk_strings(1, false);
    let n = 1_000_000usize;
    // (domain, constant, value per row). `mixed` is the original fixture:
    // ~1/16 rows match and every 7th row is a long (heap-backed) string.
    type Domain = (&'static str, &'static str, fn(usize) -> String);
    let domains: [Domain; 5] = [
        ("mixed", "match_target", |row| {
            if row % 16 == 0 {
                "match_target".to_string()
            } else if row % 7 == 0 {
                format!("long_string_variant_number_{row}")
            } else {
                format!("k{}", row % 97)
            }
        }),
        ("long", "long_string_variant_number_42", |row| {
            format!("long_string_variant_number_{}", row % 97)
        }),
        // The controlled pair: `{i}abcd` and `abcd{i}` hold the same bytes at the
        // same lengths, so the prefix is the only thing that differs.
        ("long-distinct", "42_long_string_variant_number", |row| {
            format!("{}_long_string_variant_number", row % 97)
        }),
        ("digits-first", "42abcd", |row| format!("{}abcd", row % 97)),
        ("abcd-shared-prefix", "abcd42", |row| format!("abcd{}", row % 97)),
    ];

    for (domain, constant, value) in domains {
        let mb = make_string_view(&schema, n, |row, _| value(row), |_, _| false);

        for (name, op) in [("eq", CmpOp::Eq), ("ne", CmpOp::Ne), ("lt", CmpOp::Lt)] {
            let consts = vec![constant.as_bytes().to_vec()];
            let mut fused = filter_prog(
                &schema,
                vec![LogicalInstr::StrColConst { op, col: 1, const_idx: ConstIdx(0) }],
                consts.clone(),
            );
            let mut regs = filter_prog(
                &schema,
                vec![
                    LogicalInstr::LoadColStr { col: 1 },
                    LogicalInstr::LoadConstStr { const_idx: ConstIdx(0) },
                    LogicalInstr::StrCmp { op, a: Reg(0), b: Reg(1) },
                ],
                consts,
            );

            // Also the warm-up, outside the driven region.
            let passed = passing_rows(&mut fused, &mb);
            assert_eq!(
                passed,
                passing_rows(&mut regs, &mb),
                "{domain}/{name}: the channels disagree"
            );
            let hits = passed.iter().filter(|&&p| p).count();
            for (chan, ev) in [("fused", &mut fused), ("registers", &mut regs)] {
                if only.drives(domain) && channel.drives(chan) && only_op.drives(name) {
                    drive_filter(ev, &mb, passes);
                }
            }
            println!("str_const_filter_bench {domain}/{name}: passes={passes} n={n} hits={hits}");
        }
    }
}

/// The pass count the `#[ignore]`d benches loop over, from `GNITZ_BENCH_PASSES`.
/// Two runs at different counts, differenced, cancel everything that happens
/// once per process.
fn bench_passes() -> usize {
    std::env::var("GNITZ_BENCH_PASSES").map_or(1, |v| v.parse().expect("GNITZ_BENCH_PASSES must be a count"))
}

/// The filter kernels, per `GNITZ_BENCH_SHAPE`.
#[test]
#[ignore]
fn filter_kernel_bench() {
    let passes = bench_passes();
    let mut shape = Selector::new("GNITZ_BENCH_SHAPE");
    let n = 200_000usize;

    // `col <op> k` over one column of `schema`.
    let col_cmp = |schema: &TestSchema, col: u32, op, val| {
        let instrs = vec![
            LogicalInstr::LoadCol { col },
            LogicalInstr::LoadConst { val, unsigned: false },
            LogicalInstr::Cmp { op, a: Reg(0), b: Reg(1) },
        ];
        filter_prog(schema, instrs, vec![])
    };

    // `pk > n/2` — the PK-region load.
    let pk_schema = schema_pk_ints(1, false);
    let pk_view = make_n_col_view(&pk_schema, n, |_, _| 1, |_, _| false);
    let mut pk_filter = col_cmp(&pk_schema, 0, CmpOp::Gt, (n / 2) as i64);

    // The same compare over an `I64` PK: the signed OPK decode, which `pk`'s
    // `U64` key does not reach.
    let pk_i64_schema = TestSchema::new(&[(TypeCode::I64, false), (TypeCode::I64, false)], &[0]);
    let pk_i64_view = make_n_col_view(&pk_i64_schema, n, |_, _| 1, |_, _| false);
    let mut pk_i64_filter = col_cmp(&pk_i64_schema, 0, CmpOp::Gt, (n / 2) as i64);

    // `a > 500` over one NOT NULL `I32` payload column: a payload load narrower
    // than the 8-byte arm every other shape reads.
    let i32_schema = TestSchema::new(&[(TypeCode::U64, false), (TypeCode::I32, false)], &[0]);
    let i32_view = make_n_col_view(&i32_schema, n, |row, _| (row % 1000) as i64, |_, _| false);
    let mut i32_filter = col_cmp(&i32_schema, 1, CmpOp::Gt, 500);

    // `f > 500.0` over one NOT NULL `F32` payload column: the widening load.
    let f32_schema = TestSchema::new(&[(TypeCode::U64, false), (TypeCode::F32, false)], &[0]);
    let mut f32_view = TestView::for_schema(&f32_schema, n);
    for row in 0..n {
        f32_view.set_payload(row, 0, &((row % 1000) as f32).to_le_bytes());
    }
    let mut f32_filter = filter_prog(
        &f32_schema,
        vec![
            LogicalInstr::LoadCol { col: 1 },
            LogicalInstr::LoadConst {
                val: crate::batch::encode_f64(500.0),
                unsigned: false,
            },
            LogicalInstr::FCmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        ],
        vec![],
    );

    // `-a > 0 AND b < 80` over nullable columns — unary, the 3VL AND, and the
    // per-column null-bit gather.
    let nn_schema = schema_pk_ints(2, true);
    let nn_view = make_n_col_view(
        &nn_schema,
        n,
        |row, col| ((row * 7 + col) % 100) as i64,
        |row, _| row % 32 == 0,
    );
    let mut nn_filter = filter_prog(
        &nn_schema,
        vec![
            LogicalInstr::LoadCol { col: 1 },
            LogicalInstr::IntUnary {
                op: crate::program::IntUnaryOp::Neg,
                a: Reg(0),
            },
            LogicalInstr::LoadConst { val: 0, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(1), b: Reg(2) },
            LogicalInstr::LoadCol { col: 2 },
            LogicalInstr::LoadConst { val: 80, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Lt, a: Reg(4), b: Reg(5) },
            LogicalInstr::BoolBinary { is_or: false, a: Reg(3), b: Reg(6) },
        ],
        vec![],
    );

    // Five literal comparisons over one NOT NULL column: the `no_nulls` arm's
    // cheapest per-row work carrying the most constant registers, which is where
    // a per-morsel constant refill would show.
    let lit_schema = schema_pk_ints(1, false);
    let lit_view = make_n_col_view(&lit_schema, n, |row, _| (row % 1000) as i64, |_, _| false);
    let mut lit_instrs = vec![LogicalInstr::LoadCol { col: 1 }];
    for (i, (op, val)) in [
        (CmpOp::Gt, 1i64),
        (CmpOp::Lt, 999),
        (CmpOp::Ne, 5),
        (CmpOp::Ne, 7),
        (CmpOp::Ne, 9),
    ]
    .into_iter()
    .enumerate()
    {
        let acc = last(&lit_instrs);
        lit_instrs.push(LogicalInstr::LoadConst { val, unsigned: false });
        lit_instrs.push(LogicalInstr::Cmp { op, a: Reg(0), b: last(&lit_instrs) });
        if i > 0 {
            lit_instrs.push(LogicalInstr::BoolBinary {
                is_or: false,
                a: acc,
                b: last(&lit_instrs),
            });
        }
    }
    let mut lit_filter = filter_prog(&lit_schema, lit_instrs, vec![]);

    // `a IN (…)` over one NOT NULL column whose values spread over 0..1000, the
    // list sized either side of where the kernel stops scanning it.
    let in_list = |k: usize| {
        let mut b = ExprBuilder::new();
        let set_idx = b.add_const_int_set((0..k as i64).map(|i| i * (1000 / k as i64) + 1).collect());
        let col = b.emit(LogicalInstr::LoadCol { col: 1 });
        let hit = b.emit(LogicalInstr::IntInSet { value_reg: col, set_idx });
        b.build(vec![Sink::Reg(hit)])
            .expect("a well-formed predicate")
            .resolve_filter(&lit_schema)
            .expect("resolves")
    };
    let mut in_lists: Vec<(String, RowFilter)> = [2, 4, 8, 16, 32, 64, 128, 1000]
        .into_iter()
        .map(|k| (format!("in{k}"), in_list(k)))
        .collect();

    // `s1 = s2` / `s1 < s2` over two NOT NULL string columns: short strings that
    // agree on every third row, and long ones sharing a prefix on every seventh.
    let cc_schema = schema_pk_strings(2, false);
    let cc_view = make_string_view(
        &cc_schema,
        n,
        |row, pi| match (row % 7, row % 3) {
            (0, _) => format!("long_string_variant_number_{}", (row + pi * (row % 2)) % 97),
            (_, 0) => format!("k{}", row % 97),
            _ => format!("k{}", (row + pi) % 97),
        },
        |_, _| false,
    );
    let col_col = |op| {
        filter_prog(
            &cc_schema,
            vec![LogicalInstr::StrColCol { op, col_a: 1, col_b: 2 }],
            vec![],
        )
    };
    let (mut cc_eq, mut cc_lt) = (col_col(CmpOp::Eq), col_col(CmpOp::Lt));

    for (name, ev, view) in [
        ("colcol_eq", &mut cc_eq, &cc_view),
        ("colcol_lt", &mut cc_lt, &cc_view),
        ("pk", &mut pk_filter, &pk_view),
        ("pk_i64", &mut pk_i64_filter, &pk_i64_view),
        ("i32", &mut i32_filter, &i32_view),
        ("f32", &mut f32_filter, &f32_view),
        ("nullable", &mut nn_filter, &nn_view),
        ("literals", &mut lit_filter, &lit_view),
    ] {
        if shape.drives(name) {
            drive_filter(ev, view, passes);
        }
    }
    for (name, ev) in &mut in_lists {
        if shape.drives(name) {
            drive_filter(ev, &lit_view, passes);
        }
    }
    println!("filter_kernel_bench passes={passes} n={n}");
}

/// `col1 IS NULL AND col2 > k AND …` over `n_cmp` NOT NULL columns, so the
/// whole predicate resolves `no_nulls`.
fn is_null_chain(k: i64, n_cmp: u32) -> Vec<LogicalInstr> {
    let mut instrs = vec![is_null_op(1)];
    if n_cmp == 0 {
        // No compare, so no constant to load — an unread `LoadConst` would still
        // cost a register write per morsel and blunt the bare shape's figure.
        return instrs;
    }
    instrs.push(LogicalInstr::LoadConst { val: k, unsigned: false });
    for col in 2..n_cmp + 2 {
        let acc = if col == 2 { Reg(0) } else { last(&instrs) };
        instrs.push(LogicalInstr::LoadCol { col });
        instrs.push(LogicalInstr::Cmp {
            op: CmpOp::Gt,
            a: last(&instrs),
            b: Reg(1),
        });
        instrs.push(LogicalInstr::BoolBinary { is_or: false, a: acc, b: last(&instrs) });
    }
    instrs
}

/// One nullable column for the null test, four NOT NULL ones for the compares.
fn is_null_bench_schema() -> TestSchema {
    let mut cols = vec![(TypeCode::U64, false), (TypeCode::I64, true)];
    cols.extend(std::iter::repeat_n((TypeCode::I64, false), 4));
    TestSchema::new(&cols, &[0])
}

/// The `no_nulls` arm against the nullable one (`GNITZ_BENCH_ARM`) for an
/// `IS [NOT] NULL` predicate, per `GNITZ_BENCH_SHAPE`. Take `cycles:u` too: only
/// it sees a stall.
#[test]
#[ignore]
fn is_null_arm_bench() {
    let passes = bench_passes();
    let mut arm = Selector::new("GNITZ_BENCH_ARM");
    let mut shape = Selector::new("GNITZ_BENCH_SHAPE");
    let n = 200_000usize;
    let schema = is_null_bench_schema();

    // NULLs 16 per morsel, 4 per morsel, and none in 7 of every 8 morsels.
    let value = |row: usize, col: usize| ((row * 7 + col * 13) % 100) as i64;
    let spread = make_n_col_view(&schema, n, value, |row, col| col == 0 && row.is_multiple_of(16));
    let clustered = make_n_col_view(&schema, n, value, |row, col| {
        col == 0 && (row / MORSEL).is_multiple_of(8)
    });
    let rare = make_n_col_view(&schema, n, value, |row, col| col == 0 && row.is_multiple_of(64));

    // `k = 50` passes about half the rows; `k = -1` passes every row, which is
    // the non-selective variant.
    let shapes: [(&str, &TestView, Vec<LogicalInstr>); 6] = [
        ("bare", &spread, is_null_chain(50, 0)),
        ("one_and", &spread, is_null_chain(50, 1)),
        ("chain_spread", &spread, is_null_chain(50, 4)),
        ("chain_clustered", &clustered, is_null_chain(50, 4)),
        ("chain_nonselective", &spread, is_null_chain(-1, 4)),
        ("chain_rare", &rare, is_null_chain(50, 4)),
    ];

    for (name, view, instrs) in &shapes {
        let (mut fast, mut nullable) = both_arms(name, || filter_prog(&schema, instrs.clone(), vec![]));
        // Also the warm-up, outside the driven region.
        let hits = passing_rows(&mut fast, view).iter().filter(|&&p| p).count();

        for (arm_name, ev) in [("fast", &mut fast), ("nullable", &mut nullable)] {
            if shape.drives(name) && arm.drives(arm_name) {
                drive_filter(ev, view, passes);
            }
        }
        println!("is_null_arm_bench {name}: passes={passes} n={n} hits={hits}");
    }

    // The map drive, which leaves through a register sink rather than a bitmap.
    let out_schema = schema_pk_ints(1, false);
    let map_instrs = vec![is_null_op(1)];
    let (mut fast, mut nullable) = both_arms("map", || {
        map_prog(
            &schema,
            &out_schema,
            map_instrs.clone(),
            vec![Sink::Reg(Reg(0))],
            vec![],
        )
    });
    // The warm-up doubles as the agreement check, as it does per filter shape.
    let emitted = |ev: &mut MapEval| {
        let mut vals = Vec::with_capacity(n);
        ev.eval_morsels(&spread, 0, n, |_, out| vals.extend_from_slice(out.reg_values(0)));
        vals
    };
    assert_eq!(emitted(&mut fast), emitted(&mut nullable), "map: the arms disagree");
    let run = |ev: &mut MapEval| {
        let mut acc = 0i64;
        for _ in 0..passes {
            ev.eval_morsels(&spread, 0, n, |_, out| acc += out.reg_values(0).iter().sum::<i64>());
        }
        std::hint::black_box(acc);
    };
    for (arm_name, ev) in [("fast", &mut fast), ("nullable", &mut nullable)] {
        if shape.drives("map") && arm.drives(arm_name) {
            run(ev);
        }
    }
    println!("is_null_arm_bench map: passes={passes} n={n}");
}

/// `n` rows of `unit` repeated to `len` bytes, one ASCII byte per row turned
/// into a digit so no matcher hoists out of the row loop.
fn fixed_len_str_view(schema: &TestSchema, n: usize, len: usize, unit: &[u8]) -> TestView {
    let base: Vec<u8> = unit.iter().copied().cycle().take(len).collect();
    make_string_view(
        schema,
        n,
        |row, _| {
            let mut s = base.clone();
            if s[row % len].is_ascii() {
                s[row % len] = b'0' + (row % 10) as u8;
            }
            s
        },
        |_, _| false,
    )
}

/// One German string per payload slot, alternating either side of
/// the 12-byte inline boundary so a string bench drives both the in-place inline
/// view and the blob view.
fn str_bench_view(schema: &TestSchema, n: usize) -> TestView {
    make_string_view(
        schema,
        n,
        |row, pi| match row % 3 {
            0 => format!("row-{row}-col-{pi}-past-the-inline-boundary"),
            _ => format!("r{}{pi}", row % 100),
        },
        |_, _| false,
    )
}

/// The kernels whose result is not a predicate, per `GNITZ_BENCH_SHAPE`; a
/// numeric suffix is the haystack length the family's cost is a slope in.
#[test]
#[ignore]
fn expr_kernel_bench() {
    let passes = bench_passes();
    let mut shape = Selector::new("GNITZ_BENCH_SHAPE");
    let n = 200_000usize;

    // --- scalar shapes over two nullable I64 columns ---
    let ints = schema_pk_ints(2, true);
    let int_view = make_n_col_view(
        &ints,
        n,
        |row, col| ((row * 7 + col) % 1000 + 1) as i64,
        |row, _| row % 32 == 0,
    );
    let load2 = |c: u32| LogicalInstr::LoadCol { col: c };

    let int_cast = scalar_prog(
        &ints,
        vec![load2(1), LogicalInstr::IntCast { a: Reg(0), fi: FixedInt::I32 }],
        vec![],
    );
    let int_div = scalar_prog(
        &ints,
        vec![
            load2(1),
            LogicalInstr::LoadConst { val: 7, unsigned: false },
            LogicalInstr::IntArith {
                op: IntArithOp::Div,
                a: Reg(0),
                b: Reg(1),
            },
        ],
        vec![],
    );
    // `CASE WHEN a > b THEN a ELSE b END` over nullable columns — the blend's
    // nullable arm, which nothing else in the tree drives.
    let select = scalar_prog(
        &ints,
        vec![
            load2(1),
            load2(2),
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
            LogicalInstr::Select { cond: Reg(2), a: Reg(0), b: Reg(1) },
        ],
        vec![],
    );
    let int_to_str = scalar_prog(&ints, vec![load2(1), LogicalInstr::IntToStr { a: Reg(0) }], vec![]);
    // `GREATEST(a, b)`: over the nullable columns the null-skipping arm, over the
    // NOT NULL ones the plain pick.
    let minmax_of = |schema: &TestSchema| {
        scalar_prog(
            schema,
            vec![
                load2(1),
                load2(2),
                LogicalInstr::IntMinMax2 { a: Reg(0), b: Reg(1), is_max: true },
            ],
            vec![],
        )
    };
    let minmax = minmax_of(&ints);

    // --- string shapes over two NOT NULL STRING columns ---
    let strs = schema_pk_strings(2, false);
    let str_view = str_bench_view(&strs, n);
    let load_str = |c: u32| LogicalInstr::LoadColStr { col: c };

    let str_len = scalar_prog(
        &strs,
        vec![load_str(1), LogicalInstr::StrLen { a: Reg(0), chars: false }],
        vec![],
    );
    let str_upper = scalar_prog(
        &strs,
        vec![load_str(1), LogicalInstr::StrCase { a: Reg(0), upper: true }],
        vec![],
    );
    let str_like = scalar_prog(
        &strs,
        vec![
            load_str(1),
            LogicalInstr::StrLike {
                src: Reg(0),
                pat_idx: ConstIdx(0),
                ci: false,
            },
        ],
        vec![LikePattern::encode("%boundary", None).unwrap().as_bytes().to_vec()],
    );
    let str_substr = scalar_prog(
        &strs,
        vec![
            load_str(1),
            LogicalInstr::LoadConst { val: 2, unsigned: false },
            LogicalInstr::LoadConst { val: 6, unsigned: false },
            LogicalInstr::StrSubstr {
                src: Reg(0),
                start_reg: Reg(1),
                len_reg: Some(Reg(2)),
            },
        ],
        vec![],
    );
    let str_concat = scalar_prog(
        &strs,
        vec![
            load_str(1),
            load_str(2),
            LogicalInstr::StrConcat { a: Reg(0), b: Reg(1), skip_null: false },
        ],
        vec![],
    );

    // --- LIKE shapes whose cost is a slope in the haystack. One STRING column,
    //     so the view is the fixture and the pattern is the shape.
    let str1 = schema_pk_strings(1, false);
    let like_lens = [12usize, 128, 512];
    let like_views: Vec<TestView> = like_lens
        .iter()
        .map(|&l| fixed_len_str_view(&str1, n, l, b"x"))
        .collect();
    let like_prog = |pat: &str, ci: bool| {
        scalar_prog(
            &str1,
            vec![
                load_str(1),
                LogicalInstr::StrLike { src: Reg(0), pat_idx: ConstIdx(0), ci },
            ],
            vec![LikePattern::encode(pat, None).unwrap().as_bytes().to_vec()],
        )
    };
    let strpos = || {
        scalar_prog(
            &str1,
            vec![
                load_str(1),
                LogicalInstr::LoadConstStr { const_idx: ConstIdx(0) },
                LogicalInstr::StrPos { hay: Reg(0), needle: Reg(1) },
            ],
            vec![b"needle".to_vec()],
        )
    };
    let freq_views: Vec<TestView> = [128, 512]
        .iter()
        .map(|&l| fixed_len_str_view(&str1, n, l, b"xxxxxxxn"))
        .collect();
    let mixed_views: Vec<TestView> = [12, 128, 512]
        .iter()
        .map(|&l| fixed_len_str_view(&str1, n, l, b"xxxNxxxn"))
        .collect();
    let dense_views: Vec<TestView> = [128, 512]
        .iter()
        .map(|&l| fixed_len_str_view(&str1, n, l, b"nexn"))
        .collect();

    // --- `RIGHT(c, 10)`: the one kernel whose walk is from the far end, over a
    //     64-byte view and the 512-byte LIKE one.
    let side_64 = fixed_len_str_view(&str1, n, 64, b"x");
    let str_side = || {
        scalar_prog(
            &str1,
            vec![
                load_str(1),
                LogicalInstr::LoadConst { val: 10, unsigned: false },
                LogicalInstr::StrSide { src: Reg(0), n_reg: Reg(1), left: false },
            ],
            vec![],
        )
    };

    // --- the character-granular kernels, over ASCII and over UTF-8 views.
    let utf8_views: Vec<TestView> = [12usize, 128, 512]
        .iter()
        .map(|&l| fixed_len_str_view(&str1, n, l, "aéb".as_bytes()))
        .collect();
    let str_chars = || {
        scalar_prog(
            &str1,
            vec![load_str(1), LogicalInstr::StrLen { a: Reg(0), chars: true }],
            vec![],
        )
    };
    let str_reverse = || scalar_prog(&str1, vec![load_str(1), LogicalInstr::StrReverse { a: Reg(0) }], vec![]);
    let str_lpad = scalar_prog(
        &str1,
        vec![
            load_str(1),
            LogicalInstr::LoadConst { val: 40, unsigned: false },
            LogicalInstr::LoadConstStr { const_idx: ConstIdx(0) },
            LogicalInstr::StrPad {
                s: Reg(0),
                n_reg: Reg(1),
                fill: Reg(2),
                left: true,
            },
        ],
        vec![b" ".to_vec()],
    );
    // Windows far into the string, where the offset walks skip whole words.
    let side200 = |left| {
        scalar_prog(
            &str1,
            vec![
                load_str(1),
                LogicalInstr::LoadConst { val: 200, unsigned: false },
                LogicalInstr::StrSide { src: Reg(0), n_reg: Reg(1), left },
            ],
            vec![],
        )
    };
    let str_substr_far = scalar_prog(
        &str1,
        vec![
            load_str(1),
            LogicalInstr::LoadConst { val: 200, unsigned: false },
            LogicalInstr::LoadConst { val: 10, unsigned: false },
            LogicalInstr::StrSubstr {
                src: Reg(0),
                start_reg: Reg(1),
                len_reg: Some(Reg(2)),
            },
        ],
        vec![],
    );

    // --- a real map: six compute opcodes plus two register sinks, driven through
    //     `write_computed` the way a maintained view's projection is ---
    let map_in = schema_pk_ints(3, false);
    let map_out = schema_pk_ints(2, false);
    let map_view = make_n_col_view(&map_in, n, |row, col| ((row * 7 + col) % 1000) as i64, |_, _| false);
    let mut map = map_prog(
        &map_in,
        &map_out,
        vec![
            load2(1),
            load2(2),
            LogicalInstr::LoadCol { col: 3 },
            LogicalInstr::IntArith {
                op: IntArithOp::Add,
                a: Reg(0),
                b: Reg(1),
            },
            LogicalInstr::IntArith {
                op: IntArithOp::Mul,
                a: Reg(3),
                b: Reg(2),
            },
            LogicalInstr::IntArith {
                op: IntArithOp::Sub,
                a: Reg(4),
                b: Reg(0),
            },
        ],
        vec![Sink::Reg(Reg(5)), Sink::Reg(Reg(3))],
        vec![],
    );

    // --- the `no_nulls` arms of the extremum and the blend, over the map's view.
    let minmax_nn = minmax_of(&map_in);
    let select_nn = scalar_prog(
        &map_in,
        vec![
            load2(1),
            load2(2),
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
            LogicalInstr::Select { cond: Reg(2), a: Reg(0), b: Reg(1) },
        ],
        vec![],
    );

    // --- calendar kernels over a NOT NULL column, read as days or scaled to
    //     microseconds with a time of day.
    let cal = |op, micros: bool| {
        let mut instrs = vec![load2(1)];
        if micros {
            instrs.push(LogicalInstr::LoadConst { val: 86_400_000_123, unsigned: false });
            instrs.push(LogicalInstr::IntArith {
                op: IntArithOp::Mul,
                a: Reg(0),
                b: Reg(1),
            });
        }
        instrs.push(LogicalInstr::Calendar { op, a: last(&instrs), micros });
        scalar_prog(&map_in, instrs, vec![])
    };

    // --- `CASE WHEN k > 500 THEN s1 ELSE s2 END`: a condition no branch
    //     predictor learns, and a NULL in the first branch so the blend runs on
    //     the nullable arm.
    let sel_schema = TestSchema::new(
        &[
            (TypeCode::U64, false),
            (TypeCode::I64, false),
            (TypeCode::String, true),
            (TypeCode::String, true),
        ],
        &[0],
    );
    let mut sel_view = TestView::for_schema(&sel_schema, n);
    for row in 0..n {
        let k = ((row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 40) % 1000;
        sel_view.set_int(row, 0, k as i64);
        for pi in [1, 2] {
            let s = match row % 3 {
                0 => format!("row-{row}-col-{pi}-past-the-inline-boundary"),
                _ => format!("r{}{pi}", row % 100),
            };
            sel_view.set_string(row, pi, s.as_bytes());
        }
        if row % 32 == 0 {
            sel_view.set_null(row, 1);
        }
    }
    let str_select = scalar_prog(
        &sel_schema,
        vec![
            load2(1),
            LogicalInstr::LoadConst { val: 500, unsigned: false },
            LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
            load_str(2),
            load_str(3),
            LogicalInstr::StrSelect { cond: Reg(2), a: Reg(3), b: Reg(4) },
        ],
        vec![],
    );

    let mut acc = 0i64;
    if shape.drives("map") {
        let mut out = TestView::for_schema(&map_out, n);
        for _ in 0..passes {
            map.write_computed(&map_view, 0, n, &mut out, 0);
            acc += payload_u64(&out, 0, 0) as i64;
        }
    }
    // Scalar-result shapes: sum the result register, as a register sink into an
    // 8-byte slot would read it. Each resolves its own evaluator, outside the
    // driven region.
    let mut scalar_shapes: Vec<(String, ScalarEval, &TestView)> = vec![
        ("int_cast".into(), int_cast, &int_view),
        ("int_div".into(), int_div, &int_view),
        ("select".into(), select, &int_view),
        ("select_nn".into(), select_nn, &map_view),
        ("minmax".into(), minmax, &int_view),
        ("minmax_nn".into(), minmax_nn, &map_view),
        ("cal_year_date".into(), cal(CalendarOp::Year, false), &map_view),
        ("cal_year_ts".into(), cal(CalendarOp::Year, true), &map_view),
        ("cal_hour_ts".into(), cal(CalendarOp::Hour, true), &map_view),
        (
            "cal_trunc_month_ts".into(),
            cal(CalendarOp::TruncMonth, true),
            &map_view,
        ),
        ("cal_to_days".into(), cal(CalendarOp::ToDays, true), &map_view),
        ("str_len".into(), str_len, &str_view),
        ("str_like".into(), str_like, &str_view),
    ];
    // Named per length, so one `perf` pair reads one point of the slope.
    for (k, len) in like_lens.into_iter().enumerate() {
        scalar_shapes.push((
            format!("str_contains_{len}"),
            like_prog("%needle%", false),
            &like_views[k],
        ));
        scalar_shapes.push((format!("str_generic_{len}"), like_prog("%a%b%", false), &like_views[k]));
        scalar_shapes.push((format!("str_chars_{len}"), str_chars(), &utf8_views[k]));
    }
    scalar_shapes.extend([
        (
            "str_contains_freq_128".into(),
            like_prog("%needle%", false),
            &freq_views[0],
        ),
        (
            "str_contains_freq_512".into(),
            like_prog("%needle%", false),
            &freq_views[1],
        ),
        ("str_icontains_12".into(), like_prog("%NeedLe%", true), &mixed_views[0]),
        ("str_icontains_512".into(), like_prog("%NeedLe%", true), &mixed_views[2]),
        ("str_iprefix_128".into(), like_prog("NeedLe%", true), &mixed_views[1]),
        (
            "str_generic_resume_128".into(),
            like_prog("%n_q%", false),
            &dense_views[0],
        ),
        (
            "str_generic_resume_512".into(),
            like_prog("%n_q%", false),
            &dense_views[1],
        ),
        ("str_strpos_128".into(), strpos(), &freq_views[0]),
        ("str_strpos_512".into(), strpos(), &freq_views[1]),
        // A `%` ahead of `_`s.
        ("str_like_any3".into(), like_prog("%___", false), &utf8_views[1]),
    ]);
    for (name, mut ev, view) in scalar_shapes {
        if !shape.drives(&name) {
            continue;
        }
        let reg = ev.result_reg();
        for _ in 0..passes {
            ev.eval_morsels(view, 0, n, |_, out| acc += out.reg_values(reg).iter().sum::<i64>());
        }
    }
    // String-result shapes: resolve every view, as a string sink does.
    let str_shapes: Vec<(&str, ScalarEval, &TestView)> = vec![
        ("int_to_str", int_to_str, &int_view),
        ("str_upper", str_upper, &str_view),
        ("str_substr", str_substr, &str_view),
        ("str_concat", str_concat, &str_view),
        ("str_select", str_select, &sel_view),
        ("str_side_64", str_side(), &side_64),
        ("str_side_512", str_side(), &like_views[2]),
        ("str_reverse_128", str_reverse(), &like_views[1]),
        ("str_reverse_utf8_128", str_reverse(), &utf8_views[1]),
        ("str_lpad_12", str_lpad, &like_views[0]),
        ("str_left200", side200(true), &utf8_views[2]),
        ("str_right200", side200(false), &utf8_views[2]),
        ("str_substr_far", str_substr_far, &utf8_views[2]),
    ];
    for (name, mut ev, view) in str_shapes {
        if !shape.drives(name) {
            continue;
        }
        let reg = ev.result_reg();
        for _ in 0..passes {
            ev.eval_morsels(view, 0, n, |_, out| {
                for i in 0..out.rows() {
                    acc += out.str_bytes(reg, i).len() as i64;
                }
            });
        }
    }
    println!(
        "expr_kernel_bench passes={passes} n={n} acc={}",
        std::hint::black_box(acc)
    );
}

/// The null-word permutation of a copy-only map over eight nullable columns,
/// per `GNITZ_BENCH_SHAPE`: a column prefix, every column moved one slot down,
/// and the columns reversed.
#[test]
#[ignore]
fn null_perm_bench() {
    let passes = bench_passes();
    let mut shape = Selector::new("GNITZ_BENCH_SHAPE");
    let n = 200_000usize;
    let in_schema = schema_pk_ints(8, true);
    let view = make_n_col_view(
        &in_schema,
        n,
        |row, col| (row + col) as i64,
        |row, col| (row * 7 + col * 13) % 5 == 0,
    );
    let shapes: [(&str, Vec<u32>); 3] = [
        ("prefix", (1..=7).collect()),
        ("shift", (2..=8).collect()),
        ("reversed", (1..=8).rev().collect()),
    ];
    let mut acc = 0u64;
    for (name, cols) in shapes {
        let out_schema = schema_pk_ints(cols.len(), true);
        let mut map = LogicalProgram::copy_cols(&cols)
            .resolve_map(&in_schema, &out_schema)
            .expect("a copy-only map");
        let mut out = TestView::for_schema(&out_schema, n);
        if shape.drives(name) {
            for _ in 0..passes {
                map.write_computed(&view, 0, n, &mut out, 0);
                acc ^= gnitz_wire::read_u64_le(out.null_bmp(), (n - 1) * 8);
            }
        }
    }
    println!(
        "null_perm_bench passes={passes} n={n} acc={}",
        std::hint::black_box(acc)
    );
}

/// The decode a predicate-only read pays per request per worker: `from_blob`
/// and the `resolve_filter` that decodes an IN list's pool entry.
#[test]
#[ignore]
fn from_blob_bench() {
    let passes = bench_passes();
    let schema = schema_pk_ints(1, false);
    let n = 262_144usize;
    let set: Vec<i64> = (0..n as i64).collect();
    let mut b = ExprBuilder::new();
    let set_idx = b.add_const_int_set(set);
    let col = b.emit(LogicalInstr::LoadCol { col: 1 });
    let hit = b.emit(LogicalInstr::IntInSet { value_reg: col, set_idx });
    let blob = b
        .build(vec![Sink::Reg(hit)])
        .expect("a well-formed predicate")
        .to_blob_bytes();

    let mut acc = 0usize;
    for _ in 0..passes {
        let prog = LogicalProgram::from_blob(std::hint::black_box(&blob)).expect("decodes");
        acc += prog.resolve_filter(&schema).expect("resolves").prog().int_sets[0].len();
    }
    println!(
        "from_blob_bench passes={passes} n={n} acc={}",
        std::hint::black_box(acc)
    );
}

/// [`crate::simd`]'s kernels alone, per `GNITZ_BENCH_SHAPE`, over one morsel
/// that stays in cache — a pass is [`MORSEL`] rows. `GNITZ_BENCH_LEVEL` picks
/// the instruction set: `native`, or `avx2` on a CPU that has more, which is
/// what a CPU without it runs. Between levels compare `cycles:u`: a wider
/// instruction retires as one whatever it costs to execute.
#[test]
#[ignore]
fn mask_kernel_bench() {
    use std::hint::black_box;
    let passes = bench_passes();
    let mut shape = Selector::new("GNITZ_BENCH_SHAPE");
    let mut at = Selector::new("GNITZ_BENCH_LEVEL");

    let a: Vec<i64> = (0..MORSEL).map(|i| (i * 7919 % 1000) as i64).collect();
    let b = vec![500i64; MORSEL];
    let floats = |v: &[i64]| -> Vec<i64> { v.iter().map(|&x| crate::batch::encode_f64(x as f64)).collect() };
    let (fa, fb) = (floats(&a), floats(&b));
    let nulls: Vec<u8> = (0..MORSEL as u64)
        .flat_map(|i| (i.is_multiple_of(5) as u64 * 2).to_le_bytes())
        .collect();
    let take = [0x5a5a_1234_dead_beef, 7, u64::MAX, 0x0f0f_0f0f_0f0f_0f0f];
    let set: Vec<i64> = (0..32).map(|i| i * 31 + 1).collect();
    let mut bits = [0u64; MORSEL / 64];
    let mut lanes = vec![0i64; MORSEL];
    let mut acc = 0u64;

    for (level_name, level) in simd_levels() {
        if !at.drives(level_name) {
            continue;
        }
        macro_rules! drive {
            ($name:expr, $kernel:expr) => {
                if shape.drives($name) {
                    for _ in 0..passes {
                        $kernel;
                        acc ^= black_box(bits[0]) ^ black_box(lanes[3]) as u64;
                    }
                }
            };
        }
        let (a, b, fa, fb) = (
            black_box(&a[..]),
            black_box(&b[..]),
            black_box(&fa[..]),
            black_box(&fb[..]),
        );
        drive!("gt", simd::pred_bits::<simd::GtSigned>(level, a, b, &mut bits));
        drive!(
            "gt_unsigned",
            simd::pred_bits::<simd::GtUnsigned>(level, a, b, &mut bits)
        );
        drive!("gt_float", simd::pred_bits::<simd::GtFloat>(level, fa, fb, &mut bits));
        drive!("truthy", simd::truthy_bits(level, a, &mut bits));
        for k in [2, 4, 8, 32] {
            drive!(
                &format!("in{k}"),
                simd::in_set_bits(level, a, black_box(&set[..k]), &mut bits)
            );
        }
        drive!("nulls", simd::null_bits(level, black_box(&nulls), 2, &mut bits));
        drive!("blend", simd::blend(level, black_box(&take), a, b, &mut lanes));
        drive!("bit_lanes", simd::bit_lanes(level, black_box(&take), &mut lanes));
    }
    println!("mask_kernel_bench passes={passes} n={MORSEL} acc={acc}");
}
