use crate::SchemaColumn;
use gnitz_wire::{FixedInt, TypeCode};

use super::{ColKind, ExprOp, FloatUnaryOp, IntUnaryOp, ReadAs, INSTR_WORDS, MAX_CONST_POOL, SINK_WORDS};
use crate::eval::Resolved;
use crate::test_support::{
    is_not_null_op, is_null_op, make_n_col_view, make_string_view, row_values, scalar_prog, schema_pk, schema_pk_ints,
    schema_pk_strings, verdicts,
};
use crate::SchemaDescriptor;
use crate::{
    CalendarOp, CmpOp, ColumnLocator, ConstIdx, ExprValidateErr, IntArithOp, LogicalInstr, LogicalProgram, NullPerm,
    PoolEntry, Reg, Sink, TrimMode,
};

/// A scalar and a filter each read exactly one register back, so a program whose
/// sinks are none, two, or a column is neither — while the register-free column
/// copy is exactly what a map is.
#[test]
fn a_program_whose_sinks_do_not_fit_its_role_is_refused() {
    let schema = schema_pk_ints(1, true);
    for sinks in [vec![], vec![Sink::Reg(Reg(0)), Sink::Reg(Reg(0))], vec![Sink::Col(1)]] {
        let prog = || LogicalProgram::new(vec![LogicalInstr::LoadCol { col: 1 }], sinks.clone(), vec![]);
        let refused = Some(ExprValidateErr::OutputRoleMismatch);
        assert_eq!(prog().resolve_scalar(&schema).err(), refused, "scalar over {sinks:?}");
        assert_eq!(prog().resolve_filter(&schema).err(), refused, "filter over {sinks:?}");
    }
    assert!(LogicalProgram::copy_cols(&[1]).resolve_map(&schema, &schema).is_ok());
}

/// A program resolves `no_nulls` exactly when nothing in it can produce a NULL
/// against the schema.
#[test]
fn no_nulls_holds_exactly_when_nothing_can_produce_a_null() {
    use LogicalInstr as L;
    let ints = |nullable| schema_pk_ints(3, nullable);
    let strs = |nullable| schema_pk_strings(2, nullable);
    let f64s = |nullable| schema_pk(&[TypeCode::F64], nullable);
    let k = |val| L::LoadConst { val, unsigned: false };
    let (col, text) = (L::LoadCol { col: 1 }, L::LoadColStr { col: 1 });
    let vs_const = L::StrColConst {
        op: CmpOp::Lt,
        col: 1,
        const_idx: ConstIdx(0),
    };
    let pair = L::StrColCol { op: CmpOp::Lt, col_a: 1, col_b: 2 };
    let select = L::Select { cond: Reg(0), a: Reg(1), b: Reg(2) };
    let substr = |len_reg| L::StrSubstr { src: Reg(0), start_reg: Reg(1), len_reg };
    let calendar = |op| L::Calendar { op, a: Reg(0), micros: false };
    let cases: Vec<(&str, SchemaDescriptor, Vec<LogicalInstr>, bool)> = vec![
        ("nullable int load", ints(true), vec![col], false),
        ("NOT NULL int load", ints(false), vec![col], true),
        ("nullable float load", f64s(true), vec![L::LoadCol { col: 1 }], false),
        ("NOT NULL float load", f64s(false), vec![L::LoadCol { col: 1 }], true),
        ("nullable string compare", strs(true), vec![vs_const], false),
        ("NOT NULL string compare", strs(false), vec![vs_const], true),
        ("nullable column pair", strs(true), vec![pair], false),
        ("NOT NULL column pair", strs(false), vec![pair], true),
        ("IS NULL", ints(true), vec![is_null_op(1)], true),
        ("IS NOT NULL", ints(true), vec![is_not_null_op(1)], true),
        ("IS NULL, then a load", ints(true), vec![is_null_op(1), col], false),
        (
            "SELECT",
            ints(false),
            vec![col, L::LoadCol { col: 2 }, L::LoadCol { col: 3 }, select],
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
        let prog = scalar_prog(&schema, instrs, vec![b"m".to_vec()]);
        assert_eq!(prog.prog().no_nulls, want, "{label}");
    }
}

// ---------------------------------------------------------------------------
// Expr-program validation (ExprValidateErr): one crafted input per vector,
// over five-word instructions `[opcode, selector, a1, a2, a3]`.
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
        vec![LogicalInstr::LoadCol { col: 1 }],
        vec![Sink::Col(1), Sink::Reg(Reg(0))],
        vec![b"ab".to_vec()],
    );
    #[rustfmt::skip]
    let want: Vec<u8> = vec![
        1, 0, 0, 0,             // instruction count
        1, 0, 0, 0,             // LoadCol: opcode
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
        (want, 5729912631311061321, 7),
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
    let too_many_sinks = {
        let m = gnitz_wire::MAX_COLUMNS + 1;
        let mut b = count(4, m as u32);
        b.splice(8..8, vec![0u8; m * SINK_WORDS * 4]);
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
            "a pool count past the cap",
            count(8, MAX_CONST_POOL as u32 + 1),
            "declared const-pool count",
        ),
        ("a pool entry declared but absent", count(8, 1), "truncated"),
        ("truncated mid pool entry", short_entry, "truncated"),
        // A sink region the bytes cover, past the most columns an output has.
        ("a sink count past the cap", too_many_sinks, "sink count"),
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

/// Each region decodes to exactly what it may hold: a word outside its
/// vocabulary is refused, as is a register file past the limit — counted off the
/// header without decoding one word — and a register no instruction wrote. A
/// pool of exactly `MAX_CONST_POOL` entries is reachable, a projection of that
/// many distinct literals, so its cap is `>`; one past it is a framing guard's.
#[test]
fn from_regions_refuses_each_word_outside_its_region() {
    let load_const = code(&[(ExprOp::LoadConst, 0, &[0])]);
    let pool: Vec<Vec<u8>> = (0..MAX_CONST_POOL).map(|i| format!("c{i}").into_bytes()).collect();
    let full_pool: Vec<[u32; INSTR_WORDS]> = (0..MAX_CONST_POOL as u32)
        .map(|i| LogicalInstr::LoadConstStr { const_idx: ConstIdx(i) }.to_wire())
        .collect();
    let neg_past_u16 = code(&[
        (ExprOp::LoadConst, 0, &[0]),
        (ExprOp::IntUnary, IntUnaryOp::Neg.as_wire(), &[0x1_0000]),
    ]);
    type Case = (
        &'static str,
        Vec<[u32; INSTR_WORDS]>,
        Vec<[u32; SINK_WORDS]>,
        Vec<Vec<u8>>,
        Option<ExprValidateErr>,
    );
    let cases: Vec<Case> = vec![
        // 0 and u32::MAX are never opcodes.
        (
            "opcode 0",
            vec![[0, 0, 0, 0, 0]],
            vec![],
            vec![],
            Some(ExprValidateErr::UnknownOpcode(0)),
        ),
        (
            "opcode u32::MAX",
            vec![[u32::MAX, 0, 0, 0, 0]],
            vec![],
            vec![],
            Some(ExprValidateErr::UnknownOpcode(u32::MAX)),
        ),
        (
            "sink kind 7",
            vec![],
            vec![[7, 0]],
            vec![],
            Some(ExprValidateErr::BadSinkKind(7)),
        ),
        (
            "one register past the limit",
            load_const.repeat(crate::MAX_REGS + 1),
            vec![],
            vec![],
            Some(ExprValidateErr::TooManyRegs(65)),
        ),
        (
            "a sink no instruction writes",
            load_const.clone(),
            vec![[1, 3]],
            vec![],
            Some(ExprValidateErr::RegOutOfRange { reg: 3, num_regs: 1 }),
        ),
        // Past `u16`, its low half naming a written register.
        (
            "a sink register word past u16",
            load_const.clone(),
            vec![[1, 0x1_0000]],
            vec![],
            Some(ExprValidateErr::RegOutOfRange { reg: u16::MAX, num_regs: 1 }),
        ),
        (
            "an operand register word past u16",
            neg_past_u16,
            vec![],
            vec![],
            Some(ExprValidateErr::RegReadBeforeWrite { reg: u16::MAX }),
        ),
        (
            "a pool at the cap",
            full_pool,
            vec![[1, MAX_CONST_POOL as u32 - 1]],
            pool,
            None,
        ),
    ];
    for (label, code, sinks, pool, want) in cases {
        assert_eq!(from_regions(&code, &sinks, pool).err(), want, "{label}");
    }
}

/// Each refusal a client sees names what it refused: the count and the limit,
/// the column and the type it holds against the kind the operator needs, or the
/// PK column an operator cannot read.
#[test]
fn a_validate_error_names_its_operands() {
    let regs = ExprValidateErr::TooManyRegs(66).to_string();
    assert!(
        regs.contains("66") && regs.contains(&crate::MAX_REGS.to_string()),
        "{regs}"
    );
    for want in [ColKind::ScalarCol, ColKind::StringPayload] {
        let msg = ExprValidateErr::ColKindMismatch { col: 2, type_code: TypeCode::U128, want }.to_string();
        assert!(
            msg.contains("column 2") && msg.contains("U128") && msg.contains(&want.to_string()),
            "{msg}"
        );
    }
    let msg = ExprValidateErr::ColNotPayload { col: 0 }.to_string();
    assert!(msg.contains("column 0") && msg.contains("primary key"), "{msg}");
}

/// Each column-reading opcode's type rule over every type code, whether it
/// reads a key column, and its refusal of a column past the schema.
#[test]
fn each_column_opcode_admits_exactly_its_column_kinds() {
    use LogicalInstr as L;
    use TypeCode as T;
    // The calendar types, DECIMAL and BOOLEAN are fixed-width integers under other names.
    const SCALARS: &[TypeCode] = &[
        T::Bool,
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
        T::F32,
        T::F64,
    ];
    const STRS: &[TypeCode] = &[T::String, T::Blob];
    const PK: u32 = 0;
    const SUBJECT: u32 = 1;
    const OTHER_STRING: u32 = 2;
    struct Rule {
        name: &'static str,
        over: fn(u32) -> LogicalInstr,
        mismatch: ColKind,
        accepts: &'static [TypeCode],
        reads_pk: bool,
    }
    let rules = [
        Rule {
            name: "LoadCol",
            over: |col| L::LoadCol { col },
            mismatch: ColKind::ScalarCol,
            accepts: SCALARS,
            reads_pk: true,
        },
        Rule {
            name: "LoadColStr",
            over: |col| L::LoadColStr { col },
            mismatch: ColKind::StringPayload,
            accepts: STRS,
            reads_pk: false,
        },
        Rule {
            name: "StrColConst",
            over: |col| L::StrColConst {
                op: CmpOp::Eq,
                col,
                const_idx: ConstIdx(0),
            },
            mismatch: ColKind::StringPayload,
            accepts: STRS,
            reads_pk: false,
        },
        Rule {
            name: "StrColCol a",
            over: |col_a| L::StrColCol {
                op: CmpOp::Eq,
                col_a,
                col_b: OTHER_STRING,
            },
            mismatch: ColKind::StringPayload,
            accepts: STRS,
            reads_pk: false,
        },
        Rule {
            name: "StrColCol b",
            over: |col_b| L::StrColCol {
                op: CmpOp::Eq,
                col_a: OTHER_STRING,
                col_b,
            },
            mismatch: ColKind::StringPayload,
            accepts: STRS,
            reads_pk: false,
        },
        Rule {
            name: "IsNull",
            over: |col| L::IsNull { col, invert: false },
            mismatch: ColKind::AnyCol,
            accepts: T::ALL,
            reads_pk: true,
        },
    ];
    let validate = |tc: TypeCode, instr: LogicalInstr| {
        let schema = schema_pk(&[tc, T::String], true);
        LogicalProgram::new(vec![instr], vec![], vec![b"x".to_vec()])
            .resolve_program(&schema, ReadAs::Value)
            .map(drop)
    };
    for rule in rules {
        let name = rule.name;
        for &tc in T::ALL {
            let want = match rule.accepts.contains(&tc) {
                true => Ok(()),
                false => Err(ExprValidateErr::ColKindMismatch {
                    col: SUBJECT,
                    type_code: tc,
                    want: rule.mismatch,
                }),
            };
            assert_eq!(validate(tc, (rule.over)(SUBJECT)), want, "{name} over {tc}");
        }
        let want = match rule.reads_pk {
            true => Ok(()),
            false => Err(ExprValidateErr::ColNotPayload { col: PK }),
        };
        assert_eq!(validate(T::I64, (rule.over)(PK)), want, "{name} over the PK");
        assert_eq!(
            validate(T::I64, (rule.over)(200)),
            Err(ExprValidateErr::ColOutOfRange { col: 200, num_columns: 3 }),
            "{name} past the schema"
        );
    }
}

/// A map's sinks against its output slots. A copy byte-copies at equal width
/// and otherwise widens a narrower integer: no narrowing, no representation
/// change — exactly the cross-width set-op coercion the client emits. A register
/// sink's slot holds the register's class and whole value, fewer bytes only
/// where the producer range-checked the value to that width; a BOOLEAN slot
/// stores any scalar register's truth. The sinks cover exactly the output's
/// payload slots.
#[test]
fn each_map_sink_is_held_to_its_output_slot() {
    use LogicalInstr as L;
    use TypeCode as T;
    let copy_err = |src_tc, out_tc| Err(ExprValidateErr::CopyTypeMismatch { col: 1, src_tc, out: 0, out_tc });
    let width = |type_code| Err(ExprValidateErr::EmitSlotWidth { out: 0, type_code });
    let class = |type_code| Err(ExprValidateErr::EmitClassMismatch { out: 0, type_code });
    // Register 1 of each, over `[I64, STRING]`.
    let int = || vec![L::LoadCol { col: 1 }, L::IntUnary { op: IntUnaryOp::Neg, a: Reg(0) }];
    let to_i16 = || vec![L::LoadCol { col: 1 }, L::IntCast { a: Reg(0), fi: FixedInt::I16 }];
    let to_u8 = || vec![L::LoadCol { col: 1 }, L::IntCast { a: Reg(0), fi: FixedInt::U8 }];
    let text = || vec![L::LoadColStr { col: 2 }, L::StrReverse { a: Reg(0) }];
    let truth = || vec![L::LoadCol { col: 1 }, L::Cmp { op: CmpOp::Eq, a: Reg(0), b: Reg(0) }];
    let reg1 = |instrs| LogicalProgram::new(instrs, vec![Sink::Reg(Reg(1))], vec![]);
    let sinks = |sinks| LogicalProgram::new(vec![], sinks, vec![]);
    let reg_in = vec![T::I64, T::String];
    type Case = (
        Vec<TypeCode>,
        LogicalProgram,
        Vec<TypeCode>,
        Result<(), ExprValidateErr>,
    );
    let mut cases: Vec<Case> = vec![
        // A PK source into a payload slot of its own type is a copy.
        (vec![T::I64], LogicalProgram::copy_cols(&[0]), vec![T::U64], Ok(())),
        (
            vec![T::I64, T::I64],
            sinks(vec![Sink::Col(1), Sink::Col(2)]),
            vec![T::I64, T::I64],
            Ok(()),
        ),
        (
            vec![T::I64, T::I64],
            sinks(vec![Sink::Col(1)]),
            vec![T::I64, T::I64],
            Err(ExprValidateErr::OutputSlotCountMismatch { sinks: 1, num_payload_cols: 2 }),
        ),
    ];
    for (src, dst, want) in [
        (T::I64, T::I64, Ok(())),
        (T::String, T::String, Ok(())),
        (T::Blob, T::Blob, Ok(())),
        (T::U128, T::U128, Ok(())),
        (T::F64, T::F64, Ok(())),
        (T::U32, T::U64, Ok(())),
        (T::U32, T::I64, Ok(())),
        (T::U8, T::I16, Ok(())),
        (T::String, T::U64, copy_err(T::String, T::U64)),
        (T::U64, T::String, copy_err(T::U64, T::String)),
        (T::U128, T::U64, copy_err(T::U128, T::U64)),
        (T::U64, T::F64, copy_err(T::U64, T::F64)),
        (T::F32, T::F64, copy_err(T::F32, T::F64)),
        (T::U64, T::I64, copy_err(T::U64, T::I64)),
    ] {
        cases.push((vec![src], LogicalProgram::copy_cols(&[1]), vec![dst], want));
    }
    for (tc, instrs, want) in [
        (T::I64, int(), Ok(())),
        (T::U64, int(), Ok(())),
        (T::F64, int(), Ok(())),
        (T::I16, to_i16(), Ok(())),
        (T::I16, int(), width(T::I16)),
        (T::Bool, truth(), Ok(())),
        (T::Bool, int(), Ok(())),
        (T::Bool, text(), class(T::Bool)),
        (T::U8, to_u8(), Ok(())),
        (T::U8, truth(), width(T::U8)),
        (T::I32, to_i16(), width(T::I32)),
        (T::U128, int(), width(T::U128)),
        (T::F32, int(), width(T::F32)),
        (T::String, int(), class(T::String)),
        (T::Blob, int(), class(T::Blob)),
        (T::String, text(), Ok(())),
        (T::I64, text(), class(T::I64)),
    ] {
        cases.push((reg_in.clone(), reg1(instrs), vec![tc], want));
    }
    for (input, prog, output, want) in cases {
        let label = format!("{:?} into {output:?}: {:?}", prog.instrs(), prog.sinks);
        let got = prog.resolve_map(&schema_pk(&input, true), &schema_pk(&output, true));
        assert_eq!(got.map(drop), want, "{label}");
    }
}

// ---------------------------------------------------------------------------
// IntInSet — set membership as one opcode (O(1) registers)
// ---------------------------------------------------------------------------

/// `col1 IN (…)` over every pool shape that can change the answer; a NULL
/// operand is NULL. A U64 column holding `u64::MAX` matches `-1` bit for bit, as
/// `col = -1` does.
#[test]
fn int_in_set_membership_over_every_pool_shape() {
    for (col_tc, pool, probes) in [
        (
            TypeCode::I64,
            vec![-5, -1, 0, 3],
            vec![(-5, 1), (-1, 1), (0, 1), (3, 1), (2, 0), (100, 0)],
        ),
        (TypeCode::U64, vec![-1], vec![(u64::MAX as i64, 1), (7, 0)]),
        (TypeCode::I64, vec![], vec![(42, 0)]),
        (
            TypeCode::I64,
            (0..1000).collect(),
            vec![(0, 1), (777, 1), (999, 1), (1000, 0), (-1, 0)],
        ),
        // Either side of the size at which the kernel stops scanning the set
        // and searches it.
        (
            TypeCode::I64,
            (0..64).map(|v| v * 3).collect(),
            vec![(0, 1), (189, 1), (45, 1), (46, 0), (190, 0), (-3, 0)],
        ),
        (
            TypeCode::I64,
            (0..65).map(|v| v * 3).collect(),
            vec![(0, 1), (192, 1), (45, 1), (46, 0), (193, 0), (-3, 0)],
        ),
    ] {
        let schema = schema_pk(&[col_tc], true);
        let instrs = vec![
            LogicalInstr::LoadCol { col: 1 },
            LogicalInstr::IntInSet { value_reg: Reg(0), set_idx: ConstIdx(0) },
        ];
        let pool: Vec<i64> = pool;
        let consts = vec![gnitz_wire::as_le_bytes(&pool).to_vec()];
        // One row per probe, then a NULL operand.
        let n = probes.len() + 1;
        let mb = make_n_col_view(
            &schema,
            n,
            |row, _| probes.get(row).map_or(0, |&(value, _)| value),
            |row, _| row == n - 1,
        );
        let want: Vec<Option<i128>> = probes.iter().map(|&(_, member)| Some(member)).chain([None]).collect();
        assert_eq!(
            verdicts(&schema, instrs, consts, &mb),
            want,
            "tc={col_tc} pool_len={}",
            pool.len()
        );
    }
}

/// A const-pool entry is held to what its opcode reads it as, and an index past
/// the pool is refused whichever opcode names it.
#[test]
fn a_pool_entry_is_held_to_its_kind() {
    use crate::like::{ANY_MANY, ANY_ONE};
    use LogicalInstr as L;
    // Register 0 is a string and register 1 an integer, for the instruction under
    // test to read.
    let check = |instr: LogicalInstr, entry: &[u8]| {
        let instrs = vec![L::LoadColStr { col: 1 }, L::LoadCol { col: 2 }, instr];
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

/// A selector word is narrowed at decode, so a forged one is refused before eval
/// could index a table that has no arm for it — narrowed from the full `u32`, so
/// a word past 255 cannot truncate into a valid code on the way in.
#[test]
fn a_forged_selector_is_refused_at_decode() {
    use TypeCode as T;
    let code_of = |tcs: &[TypeCode]| tcs.iter().map(|&t| t.as_wire() as u32).collect::<Vec<_>>();
    let int_targets: Vec<u32> = T::ALL
        .iter()
        .filter_map(|&t| FixedInt::exact(t))
        .map(|fi| fi.type_code().as_wire() as u32)
        .collect();
    let i64_code = T::I64.as_wire() as u32;
    let mut forged_targets = code_of(&[T::String, T::U128, T::F64]);
    forged_targets.extend([0, 255, 0x100 | i64_code, 0x1_0000 | i64_code]);
    for (op, source, good, bad) in [
        (ExprOp::IntCast, ExprOp::LoadCol, &int_targets, &forged_targets),
        (ExprOp::FloatToInt, ExprOp::LoadCol, &int_targets, &forged_targets),
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

/// Every decodable instruction placed first reads only registers not yet
/// written, so it is refused unless it reads none.
#[test]
fn every_register_operand_is_checked_read_before_write() {
    use LogicalInstr as L;
    for (_, x) in decodable_words() {
        let reads_none = matches!(
            x,
            L::LoadCol { .. }
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

/// A register operand of the wrong class is refused, whichever class the opcode
/// wants.
#[test]
fn operand_class_is_enforced_in_both_directions() {
    use LogicalInstr as L;
    let refused = |instrs: Vec<LogicalInstr>| LogicalProgram::from_instrs(instrs, vec![], vec![]).err();
    let (int, text) = (L::LoadCol { col: 1 }, L::LoadColStr { col: 1 });
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

/// The result register's class splits the two resolvers: a filter reads its
/// verdict out of the scalar file, while a SET right-hand side wants exactly a
/// string back.
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
    let instrs = [LogicalInstr::LoadColStr { col: 1 }, like(false), like(true)];
    let view = make_string_view(&schema, 1, |_, _| b"ABC", |_, _| false);
    // LIKE misses and ILIKE matches, each read as its own program's result.
    for (result, want) in [(1, 0), (2, 1)] {
        let mut ev = scalar_prog(&schema, instrs[..=result].to_vec(), vec![b"abc".to_vec()]);
        assert_eq!(row_values(&mut ev, &view), [Some(want)], "result register {result}");
    }
}

// ---------------------------------------------------------------------------
// Encoder / decoder drift — the two tables over one opcode space
// ---------------------------------------------------------------------------

/// The number of [`LogicalInstr`] variants. A new variant stops the match below
/// compiling until it is listed and counted.
impl LogicalInstr {
    pub(crate) const VARIANT_COUNT: usize = 45;

    /// Exhaustive by construction — the compile error on a new variant is the
    /// whole point, so this must never gain a `_` arm.
    #[allow(dead_code)]
    fn assert_every_variant_is_listed(&self) {
        use LogicalInstr as L;
        match *self {
            L::LoadCol { .. }
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
    let a = b.emit(LogicalInstr::LoadCol { col: 1 });
    let a_in = b.emit(LogicalInstr::IntInSet { value_reg: a, set_idx: set });
    let c = b.emit(LogicalInstr::LoadCol { col: 2 });
    let c_in = b.emit(LogicalInstr::IntInSet { value_reg: c, set_idx: set });
    let or = b.emit(LogicalInstr::BoolBinary { a: a_in, b: c_in, is_or: true });
    let prog = b.build(vec![Sink::Reg(or)]).expect("a well-formed program");
    assert_eq!(prog.const_strings().len(), 1, "the two lists intern to one pool entry");

    let ev = prog.resolve_filter(&schema).expect("resolves");
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
    let view = make_string_view(&schema, 1, |_, c| ["ab", "cdef"][c], |_, _| false);
    assert_eq!(row_values(&mut scalar_prog(&schema, instrs, vec![]), &view), [Some(4)]);
}

/// A map reproduces its input only when it copies every payload column into its
/// own slot, computes nothing, and the two schemas locate every column alike.
#[test]
fn is_identity_map_requires_an_in_order_copy_over_one_layout() {
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    assert!(LogicalProgram::copy_cols(&[1, 2]).is_identity_map(&schema, &schema));
    assert!(
        !LogicalProgram::copy_cols(&[2, 1]).is_identity_map(&schema, &schema),
        "reordered copy"
    );
    let computed = LogicalProgram::new(
        vec![LogicalInstr::LoadCol { col: 2 }],
        vec![Sink::Col(1), Sink::Reg(Reg(0))],
        vec![],
    );
    assert!(!computed.is_identity_map(&schema, &schema), "computed sink");
    let other_payload = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::I32, false),
        ],
        &[0],
    );
    assert!(
        !LogicalProgram::copy_cols(&[1, 2]).is_identity_map(&schema, &other_payload),
        "different payload type"
    );
    let other_pk = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[1],
    );
    assert!(
        !LogicalProgram::copy_cols(&[1, 2]).is_identity_map(&schema, &other_pk),
        "different PK layout"
    );
    // A PK that does not lead: the payload columns are 0 and 2.
    let mid_pk = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I64, true),
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[1],
    );
    assert!(LogicalProgram::copy_cols(&[0, 2]).is_identity_map(&mid_pk, &mid_pk));
    assert!(
        !LogicalProgram::copy_cols(&[1, 2]).is_identity_map(&mid_pk, &mid_pk),
        "a PK column copied into a payload slot"
    );
    // A PK-only schema: nothing to copy, so the empty program is its identity.
    let pk_only = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false)], &[0]);
    assert!(LogicalProgram::copy_cols(&[]).is_identity_map(&pk_only, &pk_only));
}

/// [`NullPerm`] against a bit-by-bit move over a window spanning several
/// blocks: a source that cannot carry a bit — a PK column or a NOT NULL slot —
/// clears its slot.
#[test]
fn null_perm_moves_each_copied_bit_into_its_slot() {
    let payload = |slot: u8| ColumnLocator::Payload { slot, size: 8, type_code: TypeCode::I64 };
    let pk = ColumnLocator::Pk {
        byte_off: 0,
        size: 8,
        type_code: TypeCode::U64,
    };
    let copies = |srcs: &[ColumnLocator]| -> Vec<crate::ColCopy> {
        srcs.iter()
            .enumerate()
            .map(|(slot, &src)| crate::ColCopy { src, slot, width: 8 })
            .collect()
    };
    // Payload slots 0, 2 and 3 admit NULL; slot 1 is NOT NULL.
    let nullable = 0b1101;
    let (src_start, dst_base) = (3, 5);
    for (srcs, shape) in [
        (vec![pk, payload(1)], "Zero"),
        (vec![payload(0), payload(1), payload(2)], "Mask"),
        (vec![payload(2), payload(0), pk, payload(3), payload(1)], "Permute"),
    ] {
        let copies = copies(&srcs);
        let perm = NullPerm::new(&copies, nullable);
        for n in [0, 1, 255, 256, 257, 600] {
            let src_word = |row: usize| {
                (row as u64 + 1)
                    .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                    .rotate_left(row as u32 % 64)
            };
            let src: Vec<u8> = (0..src_start + n).flat_map(|row| src_word(row).to_le_bytes()).collect();
            let mut dst = vec![0xAAu8; (dst_base + n + 1) * 8];
            perm.write_rows(&src, src_start, &mut dst, dst_base, n);
            for row in 0..dst_base + n + 1 {
                let want = match row.checked_sub(dst_base).filter(|&r| r < n) {
                    None => u64::from_le_bytes([0xAA; 8]),
                    Some(r) => copies.iter().fold(0u64, |w, c| match c.src {
                        ColumnLocator::Payload { slot, .. } if nullable >> slot & 1 != 0 => {
                            w | (src_word(src_start + r) >> slot & 1) << c.slot
                        }
                        _ => w,
                    }),
                };
                assert_eq!(gnitz_wire::read_u64_le(&dst, row * 8), want, "{shape}: n={n} row {row}");
            }
        }
    }
}
