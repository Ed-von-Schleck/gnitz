use gnitz_wire::{FixedInt, TypeCode};

use super::{ColKind, FloatUnaryOp, IntUnaryOp, ReadAs};
use crate::SchemaDescriptor;
use crate::eval::Resolved;
use crate::test_support::{
    fill, in_set, is_not_null_op, is_null_op, result_prog, row_values, scalar_prog, schema_pk, schema_pk_ints,
    schema_pk_strings, verdicts,
};
use crate::{
    CalendarOp, CmpOp, ConstIdx, ExprValidateErr, IntArithOp, LogicalInstr, LogicalProgram, PoolEntry, Reg, Sink,
    TrimMode,
};

/// A scalar and a filter each read exactly one register back, so a program whose
/// sinks are none, two, or a column is neither — while the register-free column
/// copy is exactly what a map is.
#[test]
fn a_program_whose_sinks_do_not_fit_its_role_is_refused() {
    let schema = schema_pk_ints(1, true);
    for sinks in [vec![], vec![Sink::Reg(Reg(0)), Sink::Reg(Reg(0))], vec![Sink::Col(1)]] {
        let prog = LogicalProgram::new(vec![LogicalInstr::LoadCol { col: 1 }], sinks.clone(), vec![]).unwrap();
        let refused = Some(ExprValidateErr::OutputRoleMismatch);
        assert_eq!(prog.resolve_scalar(&schema).err(), refused, "scalar over {sinks:?}");
        assert_eq!(prog.resolve_filter(&schema).err(), refused, "filter over {sinks:?}");
    }
    assert!(LogicalProgram::copy_cols(&[1]).resolve_map(&schema, &schema).is_ok());
}

/// A program resolves no register that tracks NULLs exactly when nothing in it
/// can produce a NULL against the schema.
#[test]
fn no_register_tracks_nulls_exactly_when_nothing_can_produce_one() {
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
        assert_eq!(prog.null_regs() == 0, want, "{label}");
    }
}

/// A register tracks NULLs exactly when its kernel can make one, it decodes a
/// nullable column, or it reads such a register other than by a null test.
#[test]
fn null_regs_holds_exactly_the_registers_that_can_be_null() {
    use LogicalInstr as L;
    let ints = schema_pk_ints(2, true);
    let (col, not_null_col) = (L::LoadCol { col: 1 }, L::LoadCol { col: 0 });
    let neg = |a| L::IntUnary { op: IntUnaryOp::Neg, a: Reg(a) };
    let cases: Vec<(&str, Vec<LogicalInstr>, u64)> = vec![
        ("a nullable operand", vec![col, neg(0)], 0b11),
        ("a NOT NULL operand", vec![not_null_col, neg(0), col], 0b100),
        (
            "a null test of a nullable register",
            vec![col, L::IsNullReg { a: Reg(0), invert: false }, neg(1)],
            0b001,
        ),
        (
            "a NULL its kernel makes",
            vec![not_null_col, L::IntCast { a: Reg(0), fi: FixedInt::I8 }, neg(1)],
            0b110,
        ),
    ];
    for (label, instrs, want) in cases {
        let prog = scalar_prog(&ints, instrs, vec![]);
        assert_eq!(prog.null_regs(), want, "{label}");
    }
}

// ---------------------------------------------------------------------------
// Expr-program validation (ExprValidateErr): one crafted input per vector.
// ---------------------------------------------------------------------------

/// Each refusal a client sees names what it refused: the count and the limit,
/// or the column and the type it holds against the kind the operator needs.
#[test]
fn a_validate_error_names_its_operands() {
    let regs = ExprValidateErr::TooManyRegs(66).to_string();
    assert!(
        regs.contains("66") && regs.contains(&crate::MAX_REGS.to_string()),
        "{regs}"
    );
    for want in [ColKind::ScalarCol, ColKind::StringCol] {
        let msg = ExprValidateErr::ColKindMismatch { col: 2, type_code: TypeCode::U128, want }.to_string();
        assert!(
            msg.contains("column 2") && msg.contains("U128") && msg.contains(&want.to_string()),
            "{msg}"
        );
    }
}

/// Each column-reading opcode's type rule over every type code, its verdict
/// over the integer PK column, and its refusal of a column past the schema.
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
    }
    let rules = [
        Rule {
            name: "LoadCol",
            over: |col| L::LoadCol { col },
            mismatch: ColKind::ScalarCol,
            accepts: SCALARS,
        },
        Rule {
            name: "LoadColStr",
            over: |col| L::LoadColStr { col },
            mismatch: ColKind::StringCol,
            accepts: STRS,
        },
        Rule {
            name: "StrColConst",
            over: |col| L::StrColConst {
                op: CmpOp::Eq,
                col,
                const_idx: ConstIdx(0),
            },
            mismatch: ColKind::StringCol,
            accepts: STRS,
        },
        Rule {
            name: "StrColPrefix",
            over: |col| L::StrColPrefix { col, const_idx: ConstIdx(0) },
            mismatch: ColKind::StringCol,
            accepts: STRS,
        },
        Rule {
            name: "StrColCol a",
            over: |col_a| L::StrColCol {
                op: CmpOp::Eq,
                col_a,
                col_b: OTHER_STRING,
            },
            mismatch: ColKind::StringCol,
            accepts: STRS,
        },
        Rule {
            name: "StrColCol b",
            over: |col_b| L::StrColCol {
                op: CmpOp::Eq,
                col_a: OTHER_STRING,
                col_b,
            },
            mismatch: ColKind::StringCol,
            accepts: STRS,
        },
        Rule {
            name: "IsNull",
            over: |col| L::IsNull { col, invert: false },
            mismatch: ColKind::AnyCol,
            accepts: T::ALL,
        },
    ];
    let validate = |tc: TypeCode, instr: LogicalInstr| {
        let schema = schema_pk(&[tc, T::String], true);
        LogicalProgram::new(vec![instr], vec![], vec![b"x".to_vec()])
            .unwrap()
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
        // `schema_pk`'s key column is a U64.
        let want = match rule.accepts.contains(&T::U64) {
            true => Ok(()),
            false => Err(ExprValidateErr::ColKindMismatch {
                col: PK,
                type_code: T::U64,
                want: rule.mismatch,
            }),
        };
        assert_eq!(validate(T::I64, (rule.over)(PK)), want, "{name} over the PK");
        assert_eq!(
            validate(T::I64, (rule.over)(200)),
            Err(ExprValidateErr::ColOutOfRange { col: 200, num_columns: 3 }),
            "{name} past the schema"
        );
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
        let (instrs, consts) = in_set(pool.iter().copied());
        // One row per probe, then a NULL operand.
        let n = probes.len() + 1;
        let mb = fill(
            &schema,
            n,
            |row, _| probes.get(row).map_or(0, |&(value, _)| value),
            |row, _| row == n - 1,
        );
        let want: Vec<Option<i128>> = probes.iter().map(|&(_, member)| Some(member)).chain([None]).collect();
        assert_eq!(
            verdicts(instrs, consts, &mb),
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
        LogicalProgram::new(instrs, vec![Sink::Reg(Reg(2))], vec![entry.to_vec()]).err()
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

// ---------------------------------------------------------------------------
// Register classes
// ---------------------------------------------------------------------------

/// A register operand of the wrong class is refused, whichever class the opcode
/// wants.
#[test]
fn operand_class_is_enforced_in_both_directions() {
    use LogicalInstr as L;
    let refused = |instrs: Vec<LogicalInstr>| LogicalProgram::new(instrs, vec![], vec![]).err();
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
    let prog = result_prog(vec![LogicalInstr::LoadColStr { col: 1 }], vec![]);
    assert_eq!(
        prog.resolve_filter(&schema).err(),
        Some(ExprValidateErr::RegClassMismatch { reg: 0 })
    );
    assert!(prog.resolve_scalar(&schema).is_ok());
}

/// A matcher is compiled per instruction, not per pool index: the same pattern
/// under LIKE and under ILIKE are two different matchers.
#[test]
fn two_like_opcodes_over_one_pool_index_get_a_matcher_each() {
    let schema = schema_pk_strings(1, false);
    let like = |ci| LogicalInstr::StrLike { src: Reg(0), pat_idx: ConstIdx(0), ci };
    let instrs = [LogicalInstr::LoadColStr { col: 1 }, like(false), like(true)];
    let view = fill(&schema, 1, |_, _| b"ABC", |_, _| false);
    // LIKE misses and ILIKE matches, each read as its own program's result.
    for (result, want) in [(1, 0), (2, 1)] {
        let mut ev = scalar_prog(&schema, instrs[..=result].to_vec(), vec![b"abc".to_vec()]);
        assert_eq!(row_values(&mut ev, &view), [Some(want)], "result register {result}");
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
    let view = fill(&schema, 1, |_, c| ["ab", "cdef"][c], |_, _| false);
    assert_eq!(row_values(&mut scalar_prog(&schema, instrs, vec![]), &view), [Some(4)]);
}
