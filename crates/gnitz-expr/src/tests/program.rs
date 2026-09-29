use gnitz_wire::{FixedInt, TypeCode};

// `tests/program.rs` is `#[path]`-attached to `program.rs`, so `super` is that
// module — one import line rather than three spellings of it.
use super::{ColKind, ExprOp, FloatUnaryOp, IntUnaryOp, INSTR_WORDS, MAX_CONST_POOL, SINK_WORDS};
use crate::eval::Resolved;
use crate::test_support::{
    filter_prog, is_not_null_op, is_null_op, make_int_view, make_n_col_view, make_string_view, row_values, scalar_prog,
    schema_pk_ints, schema_pk_strings, TestSchema,
};
use crate::{
    CalendarOp, CmpOp, ColumnLocator, ConstIdx, ExprValidateErr, IntArithOp, LogicalInstr, LogicalProgram, NullPerm,
    PoolEntry, Reg, ScalarEval, Sink, TrimMode,
};

/// The phrase a `ColKindMismatch` renders for `kind`, read from its one source
/// rather than re-spelled here — so widening a kind's predicate without its
/// wording fails these tests instead of passing them.
fn type_phrase(kind: ColKind) -> &'static str {
    kind.type_test().expect("a kind whose rejection has a sentence").1
}

/// A scalar and a filter each read exactly one register back, so a program whose
/// sinks are none, two, or a column is neither — while the register-free column
/// copy is exactly what a map is.
#[test]
fn a_program_whose_sinks_do_not_fit_its_role_is_refused() {
    let schema = schema_pk_ints(1, true);
    for sinks in [vec![], vec![Sink::Reg(Reg(0)), Sink::Reg(Reg(0))], vec![Sink::Col(1)]] {
        let prog = || LogicalProgram::new(vec![LogicalInstr::LoadColInt { col: 1 }], sinks.clone(), vec![]);
        let refused = Some(ExprValidateErr::OutputRoleMismatch);
        assert_eq!(prog().resolve_scalar(&schema).err(), refused, "scalar over {sinks:?}");
        assert_eq!(prog().resolve_filter(&schema).err(), refused, "filter over {sinks:?}");
    }
    assert!(LogicalProgram::copy_cols(&[1]).resolve_map(&schema, &schema).is_ok());
}

/// `analyze` puts a program on the `no_nulls` arm exactly when nothing in it can
/// produce a NULL against the schema. A nullable column loaded or compared
/// carries its NULL into a register; `LoadNull` and the domain-checked producers
/// manufacture one. A null test, a PK column (which has no null bit, even
/// declared nullable — which `TestSchema`, validating nothing, can state) and the
/// total transforms add none. Each program's result is its last instruction.
#[test]
fn no_nulls_holds_exactly_when_nothing_can_produce_a_null() {
    use LogicalInstr as L;
    let ints = |nullable| schema_pk_ints(3, nullable);
    let strs = |nullable| schema_pk_strings(2, nullable);
    let f64s = |nullable| TestSchema::new(&[(TypeCode::U64, false), (TypeCode::F64, nullable)], &[0]);
    let k = |val| L::LoadConst { val, unsigned: false };
    let (col, text) = (L::LoadColInt { col: 1 }, L::LoadColStr { col: 1 });
    let vs_const = L::StrColConst {
        op: CmpOp::Lt,
        col: 1,
        const_idx: ConstIdx(0),
    };
    let pair = L::StrColCol { op: CmpOp::Lt, col_a: 1, col_b: 2 };
    let select = L::Select { cond: Reg(0), a: Reg(1), b: Reg(2) };
    let substr = |len_reg| L::StrSubstr { src: Reg(0), start_reg: Reg(1), len_reg };
    let calendar = |op| L::Calendar { op, a: Reg(0), micros: false };
    let cases: Vec<(&str, TestSchema, Vec<LogicalInstr>, bool)> = vec![
        ("nullable int load", ints(true), vec![col], false),
        ("NOT NULL int load", ints(false), vec![col], true),
        (
            "nullable float load",
            f64s(true),
            vec![L::LoadColFloat { col: 1 }],
            false,
        ),
        (
            "NOT NULL float load",
            f64s(false),
            vec![L::LoadColFloat { col: 1 }],
            true,
        ),
        ("nullable string compare", strs(true), vec![vs_const], false),
        ("NOT NULL string compare", strs(false), vec![vs_const], true),
        ("nullable column pair", strs(true), vec![pair], false),
        ("NOT NULL column pair", strs(false), vec![pair], true),
        (
            "nullable PK load",
            TestSchema::new(&[(TypeCode::U64, true), (TypeCode::I64, false)], &[0]),
            vec![L::LoadColInt { col: 0 }],
            true,
        ),
        ("IS NULL", ints(true), vec![is_null_op(1)], true),
        ("IS NOT NULL", ints(true), vec![is_not_null_op(1)], true),
        ("IS NULL, then a load", ints(true), vec![is_null_op(1), col], false),
        (
            "SELECT",
            ints(false),
            vec![col, L::LoadColInt { col: 2 }, L::LoadColInt { col: 3 }, select],
            true,
        ),
        (
            "CASE without ELSE",
            ints(false),
            vec![col, k(1), L::LoadNull, select],
            false,
        ),
        (
            "ABS",
            ints(false),
            vec![col, L::IntUnary { op: IntUnaryOp::Abs, a: Reg(0) }],
            true,
        ),
        (
            "ROUND",
            ints(false),
            vec![col, L::FloatUnary { op: FloatUnaryOp::Round, a: Reg(0) }],
            true,
        ),
        (
            "GREATEST",
            ints(false),
            vec![col, L::IntMinMax2 { a: Reg(0), b: Reg(0), is_max: true }],
            true,
        ),
        ("to F32", ints(false), vec![col, L::FloatToF32 { a: Reg(0) }], false),
        (
            "CAST to I8",
            ints(false),
            vec![col, L::IntCast { a: Reg(0), fi: FixedInt::I8 }],
            false,
        ),
        (
            "float to I8",
            ints(false),
            vec![col, L::FloatToInt { a: Reg(0), fi: FixedInt::I8 }],
            false,
        ),
        (
            "UPPER",
            strs(false),
            vec![text, L::StrCase { a: Reg(0), upper: true }],
            true,
        ),
        ("REVERSE", strs(false), vec![text, L::StrReverse { a: Reg(0) }], true),
        (
            "LEFT",
            strs(false),
            vec![text, k(1), L::StrSide { src: Reg(0), n_reg: Reg(1), left: true }],
            true,
        ),
        ("SUBSTR without FOR", strs(false), vec![text, k(1), substr(None)], true),
        (
            "SUBSTR with FOR",
            strs(false),
            vec![text, k(1), k(3), substr(Some(Reg(2)))],
            false,
        ),
        (
            "||",
            strs(false),
            vec![
                text,
                L::LoadColStr { col: 2 },
                L::StrConcat { a: Reg(0), b: Reg(1), skip_null: false },
            ],
            false,
        ),
        (
            "REPLACE",
            strs(false),
            vec![text, L::StrReplace { s: Reg(0), from: Reg(0), to: Reg(0) }],
            false,
        ),
        (
            "RPAD",
            strs(false),
            vec![
                text,
                k(1),
                L::StrPad {
                    s: Reg(0),
                    n_reg: Reg(1),
                    fill: Reg(0),
                    left: false,
                },
            ],
            false,
        ),
        (
            "SPLIT_PART",
            strs(false),
            vec![text, k(1), L::StrSplitPart { s: Reg(0), delim: Reg(0), n_reg: Reg(1) }],
            false,
        ),
        (
            "text to int",
            strs(false),
            vec![text, L::StrToInt { a: Reg(0), fi: FixedInt::I64 }],
            false,
        ),
        // A calendar field is total; widening a day count to microseconds can
        // overflow.
        ("EXTRACT YEAR", ints(false), vec![col, calendar(CalendarOp::Year)], true),
        (
            "days to micros",
            ints(false),
            vec![col, calendar(CalendarOp::ToMicros)],
            false,
        ),
    ];
    for (label, schema, instrs, want) in cases {
        let result = Reg(instrs.len() as u16 - 1);
        let prog = scalar_prog(&schema, instrs, result, vec![b"m".to_vec()]);
        assert_eq!(prog.prog().no_nulls, want, "{label}");
    }
}

/// A logical column index resolves either to the PK read or to a *dense payload
/// slot*, renumbered around wherever the PK sits — so the slot is neither `ci`
/// nor `ci - 1` in general. Both PK positions are swept, because a leading PK is
/// the one arrangement where the two closed forms happen to agree; every value is
/// distinct, so the value read pins the slot.
#[test]
fn a_column_index_resolves_to_the_pk_or_to_its_dense_payload_slot() {
    // (PK column index, column types, PK value, payload values, the value each
    // logical column reads)
    let cases = [
        (
            0,
            [TypeCode::U64, TypeCode::I64, TypeCode::I64],
            42,
            [10, 20],
            [42i128, 10, 20],
        ),
        (1, [TypeCode::I64, TypeCode::U64, TypeCode::I64], 99, [5, 7], [5, 99, 7]),
    ];
    for (pk_index, cols, pk_val, payloads, want) in cases {
        let schema = TestSchema::with_pk_at(pk_index, &cols);
        let mb = make_int_view(&schema, &[(pk_val, 0, &payloads)]);
        for (ci, want) in want.into_iter().enumerate() {
            let mut prog = scalar_prog(
                &schema,
                vec![LogicalInstr::LoadColInt { col: ci as u32 }],
                Reg(0),
                vec![],
            );
            assert_eq!(row_values(&mut prog, &mb), [Some(want)], "pk_index={pk_index} col {ci}");
        }
    }
}

/// `Select` feeding `BoolBinary`: `cond` is a bool_input, the select result feeds the
/// AND as a bool_input, and the select dst (a value register) is never bit_only.
#[test]
fn select_classification_of_cond_and_result() {
    let schema = schema_pk_ints(4, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 }, // cond
        LogicalInstr::LoadColInt { col: 2 }, // a
        LogicalInstr::LoadColInt { col: 3 }, // b
        LogicalInstr::Select { cond: Reg(0), a: Reg(1), b: Reg(2) },
        LogicalInstr::LoadColInt { col: 4 }, // other bool
        LogicalInstr::BoolBinary { is_or: false, a: Reg(3), b: Reg(4) },
    ];
    let ev = filter_prog(&schema, instrs, Reg(5), vec![]);
    let prog = ev.prog();
    // Neither r0 nor r3 is bool-produced, so neither can be bit_only and their
    // packed bits can only come from being read as a bool.
    assert!(prog.needs_bool_pack(0), "cond is read as a bool_input");
    assert!(prog.needs_bool_pack(3), "select result feeds BoolBinary as bool_input");
    assert!(!prog.is_bit_only(3), "select dst is a value register, never bit_only");
}

// ---------------------------------------------------------------------------
// Expr-program validation (ExprValidateErr): one crafted input per vector.
// Wire code is flat five-word instructions `[opcode, selector, a1, a2, a3]`;
// several opcodes read an operand word as a full u32 (col / const_idx), not as a
// register's truncated u16.
// ---------------------------------------------------------------------------

/// The instruction words of `(opcode, selector, operands)` entries, each
/// instruction's unused operand words zeroed as the encoder leaves them — and
/// the forgeries the typed builders cannot spell, an unknown opcode among them.
fn code(instrs: &[(ExprOp, u32, &[u32])]) -> Vec<[u32; INSTR_WORDS]> {
    instrs
        .iter()
        .map(|&(op, sel, ops)| {
            assert!(ops.len() <= 3, "an instruction carries at most three operands");
            let mut w = [op.as_wire(), sel, 0, 0, 0];
            w[2..2 + ops.len()].copy_from_slice(ops);
            w
        })
        .collect()
}

/// One instruction, the common case of [`code`].
fn one(op: ExprOp, sel: u32, ops: &[u32]) -> Vec<[u32; INSTR_WORDS]> {
    code(&[(op, sel, ops)])
}

/// Encode the regions as a blob and decode it back.
fn from_regions(
    code: &[[u32; INSTR_WORDS]],
    sinks: &[[u32; SINK_WORDS]],
    pool: Vec<Vec<u8>>,
) -> Result<LogicalProgram, ExprValidateErr> {
    let blob = crate::encode_expr_blob(code.iter().copied(), sinks.iter().copied(), &pool);
    LogicalProgram::from_blob(&blob)
}

/// A program round-trips through its own blob: an empty entry, multi-byte UTF-8,
/// a non-UTF-8 compare constant, and entries no instruction names.
#[test]
fn a_program_round_trips_through_its_blob() {
    let pool = vec![
        b"alpha".to_vec(),
        Vec::new(),
        "längre sträng".as_bytes().to_vec(),
        vec![0xFF, 0x00, 0xFE, 0x80],
    ];
    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::StrColConst {
            op: CmpOp::Eq,
            col: 1,
            const_idx: ConstIdx(2),
        },
    ];
    let prog = LogicalProgram::new(instrs.clone(), vec![Sink::Reg(Reg(1))], pool.clone());
    let back = LogicalProgram::from_blob(&prog.to_blob_bytes()).expect("its own blob decodes");
    assert_eq!(back.instrs(), instrs.as_slice());
    assert_eq!(back.const_strings(), pool.as_slice());
    assert_eq!(back.sinks, vec![Sink::Reg(Reg(1))]);

    // The degenerate program is a valid one, not an absence.
    let empty = LogicalProgram::new(Vec::new(), Vec::new(), Vec::new());
    let back = LogicalProgram::from_blob(&empty.to_blob_bytes()).expect("a valid empty program decodes");
    assert!(back.instrs().is_empty() && back.const_strings().is_empty());
    assert!(back.sinks.is_empty());
}

/// The blob layout and the opcode vocabulary, pinned to the version word.
#[test]
fn the_blob_layout_is_pinned_to_its_version_word() {
    // One instruction, both sink kinds, one pool entry — every region non-empty.
    let prog = LogicalProgram::new(
        vec![LogicalInstr::LoadColInt { col: 1 }],
        vec![Sink::Col(1), Sink::Reg(Reg(0))],
        vec![b"ab".to_vec()],
    );
    #[rustfmt::skip]
    let want: Vec<u8> = vec![
        1, 0, 0, 0,             // instruction count
        1, 0, 0, 0,             // LoadColInt: opcode
        0, 0, 0, 0,             //   selector
        1, 0, 0, 0,             //   a1 = column 1
        0, 0, 0, 0,             //   a2
        0, 0, 0, 0,             //   a3
        2, 0, 0, 0,             // sink count
        0, 0, 0, 0,             // Sink::Col kind
        1, 0, 0, 0,             //   source column 1
        1, 0, 0, 0,             // Sink::Reg kind
        0, 0, 0, 0,             //   register 0
        1, 0, 0, 0,             // const-pool count
        2, 0, 0, 0,             // entry 0 length
        b'a', b'b',             // entry 0 bytes
    ];
    // Every decodable word and what it decodes to.
    let vocabulary: Vec<u8> = decodable_words()
        .iter()
        .flat_map(|(w, x)| {
            gnitz_wire::as_le_bytes(w)
                .iter()
                .copied()
                .chain(format!("{x:?}").into_bytes())
        })
        .collect();
    assert_eq!(
        (
            prog.to_blob_bytes(),
            gnitz_wire::checksum(&vocabulary),
            gnitz_wire::EXPR_BLOB_VERSION
        ),
        (want, 1858342113949671658, 6),
        "the expr-blob layout or opcode vocabulary changed: bump EXPR_BLOB_VERSION if a number moved, \
         then paste what is reported here"
    );
}

/// A valid empty program's blob; the guard table below mutates a clone, so each
/// case differs from a decodable blob by exactly one flaw.
fn valid_empty_blob() -> Vec<u8> {
    crate::encode_expr_blob(std::iter::empty(), std::iter::empty(), &[])
}

/// Every framing guard in [`LogicalProgram::from_blob`], against the forgery
/// that trips it — and the message it answers with, so a corrupt blob is
/// diagnosable from the log alone.
#[test]
fn each_framing_guard_rejects_its_own_forgery() {
    // An empty program's three counts: instructions at [0..4], sinks at [4..8],
    // the const pool at [8..12].
    let count = |off: usize, n: u32| {
        let mut b = valid_empty_blob();
        gnitz_wire::write_u32_le(&mut b, off, n);
        b
    };
    let short_entry = {
        let mut b = count(8, 1);
        b.extend_from_slice(&5u32.to_le_bytes());
        b.extend_from_slice(&[0xAA, 0xBB]); // only 2 of the 5 declared bytes
        b
    };
    let trailing = {
        let mut b = valid_empty_blob();
        b.push(0);
        b
    };
    let cases: &[(&str, Vec<u8>, &str)] = &[
        // Bytes before cap: an over-cap count with nothing behind it reports
        // the truncation.
        ("truncated before the code region", count(0, 5), "truncated"),
        ("truncated before the sink region", count(4, 2), "truncated"),
        (
            "an over-cap instruction count with no bytes",
            count(0, u32::MAX),
            "truncated",
        ),
        // The pool is the one count whose cap comes first: no region to bound it.
        (
            "a huge declared pool count",
            count(8, u32::MAX),
            "declared const-pool count",
        ),
        ("a pool entry declared but absent", count(8, 1), "truncated"),
        ("truncated mid pool entry", short_entry, "truncated"),
        // The trailing-bytes guard, which no truncation case reaches — those
        // trip the reader first.
        ("trailing bytes", trailing, "trailing"),
    ];
    for (what, blob, want) in cases {
        let err = LogicalProgram::from_blob(blob).expect_err(&format!("{what} must be rejected"));
        let ExprValidateErr::CorruptBlob(msg) = &err else {
            panic!("{what}: expected a CorruptBlob, got {err:?}");
        };
        assert!(
            msg.starts_with("expr blob: ") && msg.matches("expr blob").count() == 1 && msg.contains(want),
            "{what}: the message must be labelled once and name the fault, got: {msg}"
        );
    }
}

/// A pool of exactly `MAX_CONST_POOL` entries is reachable — a projection of
/// that many distinct string literals — so the cap is `>`, never `>=`.
#[test]
fn a_const_pool_at_the_cap_is_accepted_and_one_past_it_is_not() {
    let pool: Vec<Vec<u8>> = (0..MAX_CONST_POOL).map(|i| format!("c{i}").into_bytes()).collect();
    let code: Vec<[u32; INSTR_WORDS]> = (0..MAX_CONST_POOL as u32)
        .map(|i| LogicalInstr::LoadConstStr { const_idx: ConstIdx(i) }.to_wire())
        .collect();
    let last = Reg(MAX_CONST_POOL as u16 - 1);
    assert!(from_regions(&code, &[[1, last.0 as u32]], pool).is_ok());

    // Refused off the declared count alone, so the blob need carry no entry.
    let mut b = valid_empty_blob();
    gnitz_wire::write_u32_le(&mut b, 8, MAX_CONST_POOL as u32 + 1);
    let err = LogicalProgram::from_blob(&b).expect_err("an over-cap pool must be refused");
    let ExprValidateErr::CorruptBlob(msg) = &err else {
        panic!("expected a CorruptBlob, got {err:?}");
    };
    let want = format!("declared const-pool count {}", MAX_CONST_POOL + 1);
    assert!(msg.contains(&want), "got: {msg}");
}

/// A word outside its region's vocabulary: an opcode, or a sink kind outside the
/// two the encoder writes.
#[test]
fn from_blob_rejects_an_unknown_opcode_or_sink_kind() {
    // 0 and u32::MAX are holes permanently. Deliberately NOT "one past the
    // current maximum": that couples the test to every opcode addition while
    // adding no coverage a new opcode's own decode test does not already give.
    for op in [0, u32::MAX] {
        assert_eq!(
            from_regions(&[[op, 0, 0, 0, 0]], &[], vec![]).unwrap_err(),
            ExprValidateErr::UnknownOpcode(op)
        );
    }
    assert_eq!(
        from_regions(&[], &[[7, 0]], vec![]).unwrap_err(),
        ExprValidateErr::BadSinkKind(7)
    );
}

#[test]
fn validate_err_display_names_the_register_limit() {
    assert_eq!(
        ExprValidateErr::TooManyRegs(66).to_string(),
        format!(
            "expression needs 66 registers; the limit is {} — split the predicate, or project fewer computed columns",
            crate::MAX_REGS
        )
    );
    // The type half of the requirement. The region half is its own variant, and
    // its own sentence below — a PK column is exactly what a client is likely to
    // have named.
    let mismatch = |want| ExprValidateErr::ColKindMismatch { col: 2, type_code: TypeCode::U64, want }.to_string();
    assert_eq!(
        mismatch(type_phrase(ColKind::FixedIntCol)),
        "column 2 (type code U64) cannot be used here; this operator needs a fixed-width integer column"
    );
    assert_eq!(
        mismatch(type_phrase(ColKind::FloatPayload)),
        "column 2 (type code U64) cannot be used here; this operator needs a floating-point column"
    );
    assert_eq!(
        ExprValidateErr::ColNotPayload { col: 0 }.to_string(),
        "column 0 is part of the primary key; this operator needs a payload column"
    );
}

#[test]
fn from_blob_rejects_a_bad_register_file() {
    // One instruction past the 64-register limit: the register file *is* the
    // instruction list, so a 65th instruction is what overflows it. The count is
    // the header's, so the cap is applied to it without decoding one word.
    let load_const = one(ExprOp::LoadConst, 0, &[0]);
    let over_cap = load_const.repeat(crate::MAX_REGS + 1);
    assert_eq!(
        from_regions(&over_cap, &[], vec![]).unwrap_err(),
        ExprValidateErr::TooManyRegs(65)
    );
    // A register sink no instruction writes.
    assert_eq!(
        from_regions(&load_const, &[[1, 3]], vec![]).unwrap_err(),
        ExprValidateErr::RegOutOfRange { reg: 3, num_regs: 1 }
    );
    // A register word past `u16` whose low half names a written register.
    assert_eq!(
        from_regions(&load_const, &[[1, 0x1_0000]], vec![]).unwrap_err(),
        ExprValidateErr::RegOutOfRange { reg: u16::MAX, num_regs: 1 }
    );
    let neg = code(&[
        (ExprOp::LoadConst, 0, &[0]),
        (ExprOp::IntUnary, IntUnaryOp::Neg.as_wire(), &[0x1_0000]),
    ]);
    assert_eq!(
        from_regions(&neg, &[], vec![]).unwrap_err(),
        ExprValidateErr::RegReadBeforeWrite { reg: u16::MAX }
    );
}

#[test]
fn validate_rejects_an_out_of_range_column() {
    let prog = LogicalProgram::new(vec![LogicalInstr::LoadColInt { col: 200 }], vec![], vec![]);
    assert_eq!(
        prog.validate(&schema_pk_ints(2, true), None),
        Err(ExprValidateErr::ColOutOfRange { col: 200, num_columns: 3 })
    );
}

/// Each column-reading opcode's type rule, over every type code. An integer load
/// decodes little-endian integers a register holds; a float load branches on
/// width alone, so an integer column would make it slice an 8-byte stride out of
/// a narrower region; the string opcodes read 16-byte German-string cells; a
/// null test reads the bitmap alone, so any column will do. All but the integer
/// load and the null test are payload-only.
#[test]
fn each_column_opcode_admits_exactly_its_column_kinds() {
    use LogicalInstr as L;
    use TypeCode as T;
    // The calendar types and DECIMAL are fixed-width integers under other names.
    let ints = [
        T::U8,
        T::I8,
        T::U16,
        T::I16,
        T::U32,
        T::I32,
        T::U64,
        T::I64,
        T::Date,
        T::Timestamp,
        T::Decimal,
    ];
    let strs = [T::String, T::Blob];
    // (opcode, the instruction over column `c`, the kind its mismatch names, the
    // accepted types, whether a PK column is accepted)
    type Rule<'a> = (&'a str, fn(u32) -> LogicalInstr, ColKind, &'a [TypeCode], bool);
    let rules: [Rule<'_>; 7] = [
        (
            "LoadColInt",
            |c| L::LoadColInt { col: c },
            ColKind::FixedIntCol,
            &ints,
            true,
        ),
        (
            "LoadColFloat",
            |c| L::LoadColFloat { col: c },
            ColKind::FloatPayload,
            &[T::F32, T::F64],
            false,
        ),
        (
            "LoadColStr",
            |c| L::LoadColStr { col: c },
            ColKind::StringPayload,
            &strs,
            false,
        ),
        (
            "StrColConst",
            |c| L::StrColConst {
                op: CmpOp::Eq,
                col: c,
                const_idx: ConstIdx(0),
            },
            ColKind::StringPayload,
            &strs,
            false,
        ),
        (
            "StrColCol a",
            |c| L::StrColCol { op: CmpOp::Eq, col_a: c, col_b: 2 },
            ColKind::StringPayload,
            &strs,
            false,
        ),
        (
            "StrColCol b",
            |c| L::StrColCol { op: CmpOp::Eq, col_a: 2, col_b: c },
            ColKind::StringPayload,
            &strs,
            false,
        ),
        (
            "IsNull",
            |c| L::IsNull { col: c, invert: false },
            ColKind::AnyCol,
            T::ALL,
            true,
        ),
    ];
    // Column 0 is a U64 PK, column 1 the subject, column 2 the string the column
    // pair compares against.
    let validate = |tc: TypeCode, instr: LogicalInstr| {
        let schema = TestSchema::with_pk_at(0, &[T::U64, tc, T::String]);
        LogicalProgram::new(vec![instr], vec![], vec![b"x".to_vec()]).validate(&schema, None)
    };
    for (name, instr, kind, accepted, pk_ok) in rules {
        for &tc in T::ALL {
            let want = match accepted.contains(&tc) {
                true => Ok(()),
                false => Err(ExprValidateErr::ColKindMismatch {
                    col: 1,
                    type_code: tc,
                    want: type_phrase(kind),
                }),
            };
            assert_eq!(validate(tc, instr(1)), want, "{name} over {tc}");
        }
        let want = if pk_ok {
            Ok(())
        } else {
            Err(ExprValidateErr::ColNotPayload { col: 0 })
        };
        assert_eq!(validate(T::I64, instr(0)), want, "{name} over the PK");
    }
}

/// `copy_column` byte-copies at equal width and otherwise widens a narrower
/// integer into a wider slot: no narrowing, no representation change. The
/// widening set is exactly the cross-width set-op coercion the client emits.
#[test]
fn copy_col_admits_only_a_widening_destination() {
    // in: [U64 PK, <src>]; out: [U64 PK, <dst>] — one payload slot each.
    let pair = |src: TypeCode, dst: TypeCode| {
        let in_schema = TestSchema::with_pk_at(0, &[TypeCode::U64, src]);
        let out_schema = TestSchema::with_pk_at(0, &[TypeCode::U64, dst]);
        LogicalProgram::copy_cols(&[1]).validate(&in_schema, Some(&out_schema))
    };
    for (src, dst) in [
        (TypeCode::I64, TypeCode::I64),
        (TypeCode::String, TypeCode::String),
        (TypeCode::Blob, TypeCode::Blob),
        (TypeCode::U128, TypeCode::U128),
        (TypeCode::F64, TypeCode::F64),
        (TypeCode::U32, TypeCode::U64),
        (TypeCode::U32, TypeCode::I64),
        (TypeCode::U8, TypeCode::I16),
    ] {
        assert_eq!(pair(src, dst), Ok(()), "{src} -> {dst} must be accepted");
    }
    for (src, dst) in [
        (TypeCode::String, TypeCode::U64),
        (TypeCode::U64, TypeCode::String),
        (TypeCode::U128, TypeCode::U64),
        (TypeCode::U64, TypeCode::F64),
        (TypeCode::F32, TypeCode::F64),
        (TypeCode::U64, TypeCode::I64),
    ] {
        assert_eq!(
            pair(src, dst),
            Err(ExprValidateErr::CopyTypeMismatch { col: 1, src_tc: src, out: 0, out_tc: dst }),
            "{src} -> {dst} must be rejected"
        );
    }
    // A PK source into a payload slot of the same type is a copy, not a promotion.
    let in_pk = TestSchema::with_pk_at(0, &[TypeCode::U64, TypeCode::I64]);
    let out_pk = TestSchema::with_pk_at(0, &[TypeCode::I64, TypeCode::U64]);
    let prog = LogicalProgram::copy_cols(&[0]);
    assert_eq!(prog.validate(&in_pk, Some(&out_pk)), Ok(()));
    // Both output-side checks are inert for a filter (`out_schema = None`).
    assert_eq!(prog.validate(&in_pk, None), Ok(()));
}

/// A register sink's slot must hold what its register does: the register's class,
/// and its whole 8-byte image — or fewer bytes exactly when the producer
/// range-checked the value to that integer width, where the low bytes are the
/// whole value. A string slot is refused on class before the width rule; the two
/// errors name different faults.
#[test]
fn a_register_sinks_slot_holds_its_whole_value() {
    use TypeCode as T;
    let in_schema = TestSchema::new(&[(T::U64, false), (T::I64, true), (T::String, true)], &[0]);
    let int = [
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::IntUnary { op: IntUnaryOp::Neg, a: Reg(0) },
    ];
    let to_i16 = [
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::IntCast { a: Reg(0), fi: FixedInt::I16 },
    ];
    let text = [
        LogicalInstr::LoadColStr { col: 2 },
        LogicalInstr::StrReverse { a: Reg(0) },
    ];
    let width = |type_code| Err(ExprValidateErr::EmitSlotWidth { out: 0, type_code });
    let class = |type_code| Err(ExprValidateErr::EmitClassMismatch { out: 0, type_code });
    for (tc, instrs, want) in [
        (T::I64, int, Ok(())),
        (T::U64, int, Ok(())),
        (T::F64, int, Ok(())),
        (T::I16, to_i16, Ok(())),
        (T::I16, int, width(T::I16)),
        (T::I32, to_i16, width(T::I32)),
        (T::U128, int, width(T::U128)),
        (T::F32, int, width(T::F32)),
        (T::String, int, class(T::String)),
        (T::Blob, int, class(T::Blob)),
        (T::String, text, Ok(())),
        (T::I64, text, class(T::I64)),
    ] {
        let out = TestSchema::with_pk_at(0, &[T::U64, tc]);
        let prog = LogicalProgram::new(instrs.to_vec(), vec![Sink::Reg(Reg(1))], vec![]);
        assert_eq!(prog.validate(&in_schema, Some(&out)), want, "{instrs:?} into {tc}");
    }
}

/// Output coverage: with an `out_schema`, the sink list must be exactly as long
/// as the declared payload. A sink writes the slot at its own position, so a
/// permuted or duplicated destination cannot be expressed and a short list is
/// the only failure left.
#[test]
fn validate_rejects_a_sink_list_that_does_not_cover_the_output() {
    // in/out: [U64 PK, I64, I64] — two output payload slots.
    let schema = schema_pk_ints(2, false);
    let map = |sinks| LogicalProgram::new(vec![], sinks, vec![]);
    assert_eq!(
        map(vec![Sink::Col(1), Sink::Col(2)]).validate(&schema, Some(&schema)),
        Ok(())
    );
    assert_eq!(
        map(vec![Sink::Col(1)]).validate(&schema, Some(&schema)),
        Err(ExprValidateErr::OutputSlotCountMismatch { sinks: 1, num_payload_cols: 2 })
    );
    // A predicate over the same program is unaffected — no output plan.
    assert_eq!(map(vec![Sink::Col(1)]).validate(&schema, None), Ok(()));
}

#[test]
#[should_panic(expected = "RegReadBeforeWrite")]
fn new_panics_on_a_forward_register_reference() {
    // A compiler-built (trusted) program that reads an unwritten register still
    // panics from `new`.
    let _ = LogicalProgram::new(
        vec![LogicalInstr::IntArith {
            op: IntArithOp::Add,
            a: Reg(0),
            b: Reg(1),
        }],
        Vec::new(),
        vec![],
    );
}

// ---------------------------------------------------------------------------
// IntInSet — set membership as one opcode (O(1) registers, O(log N) per row)
// ---------------------------------------------------------------------------

/// `r0 = col1; r1 = r0 IN set`, result_reg = 1 — the compiled shape of
/// `col1 IN (…)`. `col_tc` picks col1's type (I64 / U64 / …).
fn in_set_prog(col_tc: TypeCode, set: &[i64]) -> (TestSchema, ScalarEval) {
    let schema = TestSchema::with_pk_at(0, &[TypeCode::U64, col_tc]);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::IntInSet { value_reg: Reg(0), set_idx: ConstIdx(0) },
    ];
    let prog = scalar_prog(&schema, instrs, Reg(1), vec![gnitz_wire::as_le_bytes(set).to_vec()]);
    (schema, prog)
}

/// Membership at every pool shape that can change the answer, plus the 3VL rule
/// that a NULL operand is NULL out whatever the pool holds.
///
/// The `U64` case is the one that is not plain integer comparison: the column
/// loads as a bare bit-reinterpret and the folded literal `-1` is `i64::-1`, so
/// `u64::MAX` matches — the same bit-equality `col = -1` gives. The 1000-element
/// pool is here because membership is a binary search rather than an OR-chain,
/// so pool size costs no registers; at ~4000 registers the OR-chain form would
/// exceed the cap.
#[test]
fn int_in_set_membership_over_every_pool_shape() {
    // (column type, pool, [(value, expected membership)]) — aliased because the
    // inline tuple trips `clippy::type_complexity`.
    type Case<'a> = (TypeCode, &'a [i64], &'a [(i64, i128)]);
    let big: Vec<i64> = (0..1000).collect();
    let cases: [Case<'_>; 4] = [
        (
            TypeCode::I64,
            &[-5, -1, 0, 3],
            &[(-5, 1), (-1, 1), (0, 1), (3, 1), (2, 0), (100, 0)],
        ),
        (TypeCode::U64, &[-1], &[(u64::MAX as i64, 1), (7, 0)]),
        (TypeCode::I64, &[], &[(42, 0)]),
        (TypeCode::I64, &big, &[(0, 1), (777, 1), (999, 1), (1000, 0), (-1, 0)]),
    ];

    for (col_tc, pool, probes) in cases {
        let (schema, mut prog) = in_set_prog(col_tc, pool);
        // One row per probe, then a NULL operand.
        let n = probes.len() + 1;
        let mb = make_n_col_view(
            &schema,
            n,
            |row, _| probes.get(row).map_or(0, |p| p.0),
            |row, _| row == n - 1,
        );
        let want: Vec<Option<i128>> = probes.iter().map(|p| Some(p.1)).chain([None]).collect();
        assert_eq!(row_values(&mut prog, &mb), want, "tc={col_tc} pool_len={}", pool.len());
    }
}

/// `NOT IN` is `bool_not(IN)`, so the 3VL rule is the composition's: a NULL
/// operand makes `IN` NULL and `NOT(NULL)` NULL, which excludes the row.
#[test]
fn not_in_set_excludes_a_null_operand() {
    let schema = schema_pk_ints(1, true);
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::IntInSet { value_reg: Reg(0), set_idx: ConstIdx(0) },
        LogicalInstr::BoolNot { a: Reg(1) },
    ];
    let mut prog = scalar_prog(
        &schema,
        instrs,
        Reg(2),
        vec![gnitz_wire::as_le_bytes(&[1i64, 2, 3]).to_vec()],
    );
    let mb = make_n_col_view(&schema, 3, |row, _| [0, 9, 2][row], |row, _| row == 0);
    assert_eq!(row_values(&mut prog, &mb), [None, Some(1), Some(0)]);
}

/// A const-pool entry is held to what its opcode reads it as: a STRING value and
/// a LIKE pattern's text are UTF-8 (`LIKE ''` included), a TRIM set is ASCII, an
/// IN set is strictly ascending 8-byte integers, and a compare constant, which
/// also serves BLOB columns, is any bytes. An index past the pool is refused
/// whichever opcode names it.
#[test]
fn a_pool_entry_is_held_to_its_kind() {
    use crate::like::{ANY_MANY, ANY_ONE};
    use LogicalInstr as L;
    // Register 0 is a string and register 1 an integer, for the instruction under
    // test to read.
    let check = |instr: LogicalInstr, entry: &[u8]| {
        let instrs = vec![L::LoadColStr { col: 1 }, L::LoadColInt { col: 2 }, instr];
        LogicalProgram::from_instrs(instrs, vec![Sink::Reg(Reg(2))], vec![entry.to_vec()]).err()
    };
    let text = |i| L::LoadConstStr { const_idx: ConstIdx(i) };
    let trim = |i| L::StrTrim {
        a: Reg(0),
        mode: TrimMode::Both,
        set_idx: ConstIdx(i),
    };
    let like = |i| L::StrLike {
        src: Reg(0),
        pat_idx: ConstIdx(i),
        ci: false,
    };
    let cmp = |i| L::StrColConst {
        op: CmpOp::Eq,
        col: 1,
        const_idx: ConstIdx(i),
    };
    let in_set = |i| L::IntInSet { value_reg: Reg(1), set_idx: ConstIdx(i) };
    let set = |vals: &[i64]| gnitz_wire::as_le_bytes(vals).to_vec();
    let malformed = |want| Some(ExprValidateErr::PoolEntryMalformed { const_idx: 0, want });
    for (instr, entry, want) in [
        (text(0), vec![0xC3], malformed(PoolEntry::Text)),
        (text(0), "é".as_bytes().to_vec(), None),
        (trim(0), "é".as_bytes().to_vec(), malformed(PoolEntry::TrimSet)),
        (trim(0), b" x".to_vec(), None),
        (
            like(0),
            vec![ANY_MANY, 0xC3, ANY_ONE],
            malformed(PoolEntry::LikePattern),
        ),
        (like(0), vec![ANY_MANY, 0xC3, 0xA9, ANY_ONE], None),
        (like(0), vec![], None),
        (cmp(0), vec![0xFF, 0x00], None),
        (in_set(0), set(&[42, 7, 3]), malformed(PoolEntry::IntSet)),
        (in_set(0), set(&[1, 3, 3, 7]), malformed(PoolEntry::IntSet)),
        (in_set(0), vec![0; 5], malformed(PoolEntry::IntSet)),
        (in_set(0), set(&[1, 3]), None),
    ] {
        assert_eq!(check(instr, &entry), want, "{instr:?} over {entry:?}");
    }
    for instr in [text(9), trim(9), like(9), cmp(9), in_set(9)] {
        assert_eq!(
            check(instr, b"x"),
            Some(ExprValidateErr::ConstIdxOutOfRange { const_idx: 9, n: 1 }),
            "{instr:?}"
        );
    }
}

/// Classifier: every CMP and every AND in a pure conjunction is bit_only, and so
/// is a filter's result register.
#[test]
fn classifier_pure_conjunction_filter() {
    let schema = schema_pk_ints(2, true);
    // r5 = (col1 > 1) AND (col2 > 1)
    let instrs = vec![
        LogicalInstr::LoadColInt { col: 1 },
        LogicalInstr::LoadConst { val: 1, unsigned: false },
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(0), b: Reg(1) },
        LogicalInstr::LoadColInt { col: 2 },
        LogicalInstr::Cmp { op: CmpOp::Gt, a: Reg(3), b: Reg(1) },
        LogicalInstr::BoolBinary { is_or: false, a: Reg(2), b: Reg(4) },
    ];
    let ev = filter_prog(&schema, instrs, Reg(5), vec![]);
    let mask = |pred: &dyn Fn(usize) -> bool| (0..6).filter(|&r| pred(r)).fold(0u64, |m, r| m | 1 << r);
    // The CMPs read r0/r1/r3 as values, so those never qualify; r2 and r4 are
    // read only by `BoolBinary`, and the result register is read as a verdict.
    // Every bit_only register is packed too, and here the two sets coincide.
    let want = (1 << 2) | (1 << 4) | (1 << 5);
    assert_eq!(mask(&|r| ev.prog().is_bit_only(r)), want);
    assert_eq!(mask(&|r| ev.prog().needs_bool_pack(r)), want);
}

/// A selector word is narrowed at decode, so a forged one is refused before eval
/// could index a table that has no arm for it — narrowed from the full `u32`, so
/// a word past 255 cannot truncate into a valid code on the way in.
#[test]
fn a_forged_selector_is_refused_at_decode() {
    use TypeCode as T;
    let code_of = |tcs: &[TypeCode]| tcs.iter().map(|&t| t.as_wire() as u32).collect::<Vec<_>>();
    let int_targets = code_of(&[T::I8, T::U8, T::I16, T::U16, T::I32, T::U32, T::I64, T::U64]);
    let i64_code = T::I64.as_wire() as u32;
    let mut forged_targets = code_of(&[T::String, T::U128, T::F64]);
    forged_targets.extend([0, 255, 0x100 | i64_code, 0x1_0000 | i64_code]);
    for (op, source, good, bad) in [
        (ExprOp::IntCast, ExprOp::LoadColInt, &int_targets, &forged_targets),
        (ExprOp::FloatToInt, ExprOp::LoadColInt, &int_targets, &forged_targets),
        (ExprOp::StrToInt, ExprOp::LoadColStr, &int_targets, &forged_targets),
        (ExprOp::StrTrim, ExprOp::LoadColStr, &vec![0, 1, 2], &vec![3]),
    ] {
        let decode = |sel| {
            from_regions(
                &code(&[(source, 0, &[1]), (op, sel, &[0, 0])]),
                &[],
                vec![b" ".to_vec()],
            )
        };
        for &sel in good {
            assert!(decode(sel).is_ok(), "{op:?} selector {sel}");
        }
        for &sel in bad {
            assert_eq!(
                decode(sel).unwrap_err(),
                ExprValidateErr::BadSelector { op: op.as_wire(), selector: sel },
                "{op:?} selector {sel}"
            );
        }
    }
}

/// A register is the index of the instruction that writes it, so an operand at
/// or above its reader's own index is a forward reference into a lane the morsel
/// has not filled — which covers the out-of-range, the self-referencing and the
/// not-yet-written operand alike. Placed first, every decodable instruction's
/// register operands are all such references, so each must be refused unless it
/// reads none; that is what catches an operand `operands()` leaves out.
#[test]
fn every_register_operand_is_checked_read_before_write() {
    use LogicalInstr as L;
    for (_, x) in decodable_words() {
        let reads_none = matches!(
            x,
            L::LoadColInt { .. }
                | L::LoadColFloat { .. }
                | L::LoadConst { .. }
                | L::LoadNull
                | L::IsNull { .. }
                | L::StrColConst { .. }
                | L::StrColCol { .. }
                | L::LoadColStr { .. }
                | L::LoadConstStr { .. }
                | L::LoadNullStr
        );
        // Eight zero bytes are a well-formed entry of every pool kind.
        let got = LogicalProgram::from_instrs(vec![x], vec![], vec![vec![0u8; 8]; 14]).err();
        match reads_none {
            true => assert_eq!(got, None, "{x:?}"),
            false => assert!(
                matches!(got, Some(ExprValidateErr::RegReadBeforeWrite { .. })),
                "{x:?}: {got:?}"
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// Register classes
// ---------------------------------------------------------------------------

/// A mixed-class register operand is refused whichever way round it goes: a
/// string opcode reading the scalar file would resolve a lane that was never
/// written, and an integer opcode reading a string register would interpret
/// whatever i64 sits at that index.
#[test]
fn operand_class_is_enforced_in_both_directions() {
    use LogicalInstr as L;
    let refused = |instrs: Vec<LogicalInstr>| LogicalProgram::from_instrs(instrs, vec![], vec![]).err();
    let (int, text) = (L::LoadColInt { col: 1 }, L::LoadColStr { col: 1 });
    let substr = |start_reg| L::StrSubstr { src: Reg(0), start_reg, len_reg: None };
    let mismatch = |reg| Some(ExprValidateErr::RegClassMismatch { reg });
    assert_eq!(refused(vec![int, L::StrCase { a: Reg(0), upper: true }]), mismatch(0));
    assert_eq!(
        refused(vec![
            text,
            L::IntArith {
                op: IntArithOp::Add,
                a: Reg(0),
                b: Reg(0)
            }
        ]),
        mismatch(0)
    );
    // The mixed-class opcodes police each half separately: SUBSTR's source must
    // be a string and its bounds must not be.
    assert_eq!(refused(vec![int, substr(Reg(0))]), mismatch(0));
    assert_eq!(refused(vec![text, text, substr(Reg(1))]), mismatch(1));
}

/// The result register's class splits the two resolvers that make the identical
/// `validate` call: a filter reads its verdict out of the scalar file, while a
/// SET right-hand side wants exactly a string back.
#[test]
fn a_string_result_register_resolves_as_a_scalar_but_not_as_a_filter() {
    let schema = schema_pk_strings(1, true);
    let prog = || {
        LogicalProgram::new(
            vec![LogicalInstr::LoadColStr { col: 1 }],
            vec![Sink::Reg(Reg(0))],
            vec![],
        )
    };
    assert_eq!(
        prog().resolve_filter(&schema).err(),
        Some(ExprValidateErr::RegClassMismatch { reg: 0 })
    );
    assert!(prog().resolve_scalar(&schema).is_ok());
}

/// A matcher is compiled per instruction, not per pool index: the same pattern
/// under LIKE and under ILIKE are two different matchers.
#[test]
fn two_like_opcodes_over_one_pool_index_get_a_matcher_each() {
    let schema = schema_pk_strings(1, false);
    let like = |ci| LogicalInstr::StrLike { src: Reg(0), pat_idx: ConstIdx(0), ci };
    let instrs = vec![LogicalInstr::LoadColStr { col: 1 }, like(false), like(true)];
    let view = make_string_view(&schema, 1, |_, _| b"ABC");
    // LIKE misses and ILIKE matches, each read as its own program's result.
    for (result, want) in [(1, 0), (2, 1)] {
        let mut ev = scalar_prog(&schema, instrs.clone(), Reg(result), vec![b"abc".to_vec()]);
        assert_eq!(row_values(&mut ev, &view), [Some(want)], "result register {result}");
    }
}

// ---------------------------------------------------------------------------
// Encoder / decoder drift — the two tables over one opcode space
// ---------------------------------------------------------------------------

/// The number of [`LogicalInstr`] variants. A new variant stops the match below
/// compiling until it is listed and counted.
impl LogicalInstr {
    pub(crate) const VARIANT_COUNT: usize = 46;

    /// Exhaustive by construction — the compile error on a new variant is the
    /// whole point, so this must never gain a `_` arm.
    #[allow(dead_code)]
    fn assert_every_variant_is_listed(&self) {
        use LogicalInstr as L;
        match *self {
            L::LoadColInt { .. }
            | L::LoadColFloat { .. }
            | L::LoadConst { .. }
            | L::IntArith { .. }
            | L::FloatArith { .. }
            | L::Cmp { .. }
            | L::FCmp { .. }
            | L::IntToFloat { .. }
            | L::FloatUnary { .. }
            | L::IntUnary { .. }
            | L::Calendar { .. }
            | L::FloatToInt { .. }
            | L::IntCast { .. }
            | L::FloatToF32 { .. }
            | L::IntMinMax2 { .. }
            | L::FloatMinMax2 { .. }
            | L::Select { .. }
            | L::LoadNull
            | L::BoolBinary { .. }
            | L::BoolNot { .. }
            | L::IsNull { .. }
            | L::IsNullReg { .. }
            | L::StrColConst { .. }
            | L::StrColCol { .. }
            | L::IntInSet { .. }
            | L::LoadColStr { .. }
            | L::LoadConstStr { .. }
            | L::LoadNullStr
            | L::StrSelect { .. }
            | L::StrCmp { .. }
            | L::StrLen { .. }
            | L::StrCase { .. }
            | L::StrSubstr { .. }
            | L::StrTrim { .. }
            | L::StrLike { .. }
            | L::StrConcat { .. }
            | L::IntToStr { .. }
            | L::FloatToStr { .. }
            | L::StrToInt { .. }
            | L::StrToFloat { .. }
            | L::StrSide { .. }
            | L::StrPos { .. }
            | L::StrReverse { .. }
            | L::StrReplace { .. }
            | L::StrPad { .. }
            | L::StrSplitPart { .. } => {}
        }
    }
}

/// Every instruction the decoder accepts over each opcode's selector word and a
/// few operand shapes. Operands are distinct so a swapped pair cannot
/// round-trip; 0/1 reach a flag word, `u32::MAX` SUBSTR's absent length.
fn decodable_words() -> Vec<([u32; INSTR_WORDS], LogicalInstr)> {
    let shapes = [[11, 12, 13], [11, 0, 13], [11, 1, 13], [11, 12, u32::MAX]];
    ExprOp::ALL
        .iter()
        .flat_map(|op| (0..=64u32).flat_map(move |sel| shapes.map(|[a, b, c]| [op.as_wire(), sel, a, b, c])))
        .filter_map(|w| {
            Some((
                w,
                LogicalProgram::decode_instr(gnitz_wire::as_le_bytes(&w).try_into().unwrap()).ok()?,
            ))
        })
        .collect()
}

/// `decode_instr ∘ to_wire == id` over everything the decoder accepts, and the
/// encoder emits the opcode and selector it was decoded from. The sweep reaches
/// every [`LogicalInstr`] variant, and every selector family ends inside it.
#[test]
fn every_decodable_instruction_round_trips_through_the_wire_form() {
    let words = decodable_words();
    for &(w, x) in &words {
        let back = x.to_wire();
        assert_eq!(back[..2], w[..2], "{x:?}: opcode and selector");
        let again = LogicalProgram::decode_instr(gnitz_wire::as_le_bytes(&back).try_into().unwrap());
        assert_eq!(again, Ok(x), "{x:?}: decode of its own encoding");
    }
    assert_eq!(ExprOp::ALL.len(), LogicalInstr::VARIANT_COUNT);
    let seen: std::collections::HashSet<_> = words.iter().map(|(_, x)| std::mem::discriminant(x)).collect();
    assert_eq!(
        seen.len(),
        LogicalInstr::VARIANT_COUNT,
        "variants the decoder accepts a word for"
    );
    assert!(
        words.iter().all(|(w, _)| w[1] < 64),
        "a selector family reaches the sweep's last selector: widen the sweep"
    );
}

/// `LoadConst`'s `i64` is the one operand spanning two words: the split and the
/// join are inverses over the whole range, sign bit of the low half included.
#[test]
fn load_const_round_trips() {
    for v in [
        0i64,
        1,
        -1,
        i64::MIN,
        i64::MAX,
        0x0000_0001_0000_0000,
        -0x0000_0001_0000_0000,
    ] {
        let (a1, a2) = super::encode_load_const(v);
        assert_eq!(super::decode_load_const(a1, a2), v, "load_const round-trip for {v}");
    }
}

/// Two IN lists over the same values, in any order, share one pool entry and one
/// decoded pool.
#[test]
fn two_in_sets_over_one_pool_index_share_one_decoded_pool() {
    let schema = schema_pk_ints(2, true);
    let mut b = crate::ExprBuilder::new();
    let set = b.add_const_int_set(vec![3, 1, 2, 1]);
    let a = b.emit(LogicalInstr::LoadColInt { col: 1 });
    let a_in = b.emit(LogicalInstr::IntInSet { value_reg: a, set_idx: set });
    let c = b.emit(LogicalInstr::LoadColInt { col: 2 });
    let c_in = b.emit(LogicalInstr::IntInSet { value_reg: c, set_idx: set });
    let or = b.emit(LogicalInstr::BoolBinary { a: a_in, b: c_in, is_or: true });
    let prog = b.build(vec![Sink::Reg(or)]).expect("a well-formed program");
    assert_eq!(prog.const_strings().len(), 1, "the two lists intern to one pool entry");

    let ev = prog.resolve_filter(&schema).expect("resolves");
    assert_eq!(ev.prog().int_sets.len(), 1, "and to one decoded pool");
    assert_eq!(ev.prog().int_sets[0], vec![1, 2, 3]);
}

/// The scratch's i64 lanes are sized by the highest *non*-string register, so a
/// program that interleaves the two classes must still reach every scalar lane
/// it names — here r3, past the r1 a count of scalar registers would stop at.
#[test]
fn a_scalar_register_past_a_string_one_has_its_lane() {
    let schema = schema_pk_strings(2, true);
    let instrs = vec![
        LogicalInstr::LoadColStr { col: 1 },
        LogicalInstr::StrLen { a: Reg(0), chars: false },
        LogicalInstr::LoadColStr { col: 2 },
        LogicalInstr::StrLen { a: Reg(2), chars: false },
    ];
    let view = make_string_view(&schema, 1, |_, c| ["ab", "cdef"][c]);
    assert_eq!(
        row_values(&mut scalar_prog(&schema, instrs, Reg(3), vec![]), &view),
        [Some(4)]
    );
}

/// A map reproduces its input only when it copies every payload column into its
/// own slot, computes nothing, and the two schemas locate every column alike.
#[test]
fn is_identity_map_requires_an_in_order_copy_over_one_layout() {
    let schema = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::I64, true), (TypeCode::I64, false)],
        &[0],
    );
    assert!(LogicalProgram::copy_cols(&[1, 2]).is_identity_map(&schema, &schema));
    assert!(
        !LogicalProgram::copy_cols(&[2, 1]).is_identity_map(&schema, &schema),
        "reordered copy"
    );
    let computed = LogicalProgram::new(
        vec![LogicalInstr::LoadColInt { col: 2 }],
        vec![Sink::Col(1), Sink::Reg(Reg(0))],
        vec![],
    );
    assert!(!computed.is_identity_map(&schema, &schema), "computed sink");
    let other_payload = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::I64, true), (TypeCode::I32, false)],
        &[0],
    );
    assert!(
        !LogicalProgram::copy_cols(&[1, 2]).is_identity_map(&schema, &other_payload),
        "different payload type"
    );
    let other_pk = TestSchema::new(
        &[(TypeCode::U64, false), (TypeCode::I64, false), (TypeCode::I64, false)],
        &[1],
    );
    assert!(
        !LogicalProgram::copy_cols(&[1, 2]).is_identity_map(&schema, &other_pk),
        "different PK layout"
    );
    // A PK that does not lead: the payload columns are 0 and 2.
    let mid_pk = TestSchema::new(
        &[(TypeCode::I64, true), (TypeCode::U64, false), (TypeCode::I64, false)],
        &[1],
    );
    assert!(LogicalProgram::copy_cols(&[0, 2]).is_identity_map(&mid_pk, &mid_pk));
    assert!(
        !LogicalProgram::copy_cols(&[1, 2]).is_identity_map(&mid_pk, &mid_pk),
        "a PK column copied into a payload slot"
    );
    // A PK-only schema: nothing to copy, so the empty program is its identity.
    let pk_only = TestSchema::new(&[(TypeCode::U64, false)], &[0]);
    assert!(LogicalProgram::copy_cols(&[]).is_identity_map(&pk_only, &pk_only));
}

/// The three shapes [`NullPerm`] collapses a copy list to, and the window each
/// writes. A source that cannot carry a set bit contributes no pair.
#[test]
fn null_perm_collapses_a_copy_list_to_three_shapes() {
    let payload = |slot: u8| ColumnLocator::Payload { slot, size: 8, type_code: TypeCode::I64 };
    let pk = ColumnLocator::Pk {
        byte_off: 0,
        size: 8,
        type_code: TypeCode::U64,
    };
    let copy = |src, slot| crate::ColCopy { src, slot, width: 8 };
    // Payload slots 0 and 2 admit NULL; slot 1 is NOT NULL.
    let nullable = 0b101;

    // Only sources with no bit of their own: nothing to move.
    let zero = NullPerm::new(&[copy(pk, 0), copy(payload(1), 1)], nullable);
    assert!(matches!(zero, NullPerm::Zero));

    // Every bit stays in its slot — one AND against the kept slots.
    let mask = NullPerm::new(
        &[copy(payload(0), 0), copy(payload(1), 1), copy(payload(2), 2)],
        nullable,
    );
    assert!(matches!(mask, NullPerm::Mask(0b101)));

    // Slot 2 -> slot 0 moves a bit, so the whole list permutes.
    let perm = NullPerm::new(&[copy(payload(2), 0), copy(payload(0), 1)], nullable);
    assert!(matches!(&perm, NullPerm::Permute(p) if p == &[(2u8, 0u8), (0, 1)]));

    // Two source rows, both bits set; the destination starts non-zero, so a
    // window an arm left alone reads as a stale bit rather than as a zero.
    let mut src = [0u8; 16];
    gnitz_wire::write_u64_le(&mut src, 0, 0b101);
    gnitz_wire::write_u64_le(&mut src, 8, 0b100);
    for (perm, want) in [
        (zero, [0u64, 0]),
        (mask, [0b101, 0b100]),
        // Row 0: slot 2 -> 0 and slot 0 -> 1. Row 1: only slot 2 is set.
        (perm, [0b11, 0b1]),
    ] {
        let mut dst = [0xAAu8; 24];
        perm.write_rows(&src, 0, &mut dst, 1, 2);
        assert_eq!(
            gnitz_wire::read_u64_le(&dst, 0),
            u64::from_le_bytes([0xAA; 8]),
            "row 0 is outside the window"
        );
        assert_eq!(
            [gnitz_wire::read_u64_le(&dst, 8), gnitz_wire::read_u64_le(&dst, 16)],
            want
        );
    }
}
