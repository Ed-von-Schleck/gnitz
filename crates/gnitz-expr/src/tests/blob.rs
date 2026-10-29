//! The expression blob: its framing guards, its layout pinned to
//! `EXPR_BLOB_VERSION`, and the encoder and decoder tables held to each other.

use gnitz_wire::{FixedInt, TypeCode};

use super::{decode_load_const, encode_expr_blob, encode_load_const, ExprOp, INSTR_WORDS, MAX_CONST_POOL, SINK_WORDS};
use crate::{CmpOp, ConstIdx, ExprValidateErr, IntUnaryOp, LogicalInstr, LogicalProgram, Reg, Sink};

// ---------------------------------------------------------------------------
// Framing: one crafted input per guard, over five-word instructions
// `[opcode, selector, a1, a2, a3]`.
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
    let blob = encode_expr_blob(code.iter().copied(), sinks.iter().copied(), &pool);
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
    let prog = LogicalProgram::new(instrs.clone(), vec![Sink::Reg(Reg(1))], pool.clone()).unwrap();
    let back = LogicalProgram::from_blob(&prog.to_blob_bytes()).expect("its own blob decodes");
    assert_eq!(back.instrs(), instrs.as_slice());
    assert_eq!(back.const_strings(), pool.as_slice());
    assert_eq!(back.sinks(), [Sink::Reg(Reg(1))]);

    // The degenerate program is a valid one, not an absence.
    let empty = LogicalProgram::new(Vec::new(), Vec::new(), Vec::new()).unwrap();
    let back = LogicalProgram::from_blob(&empty.to_blob_bytes()).expect("a valid empty program decodes");
    assert!(back.instrs().is_empty() && back.const_strings().is_empty());
    assert!(back.sinks().is_empty());
}

/// The blob layout and the opcode vocabulary, pinned to the version word.
#[test]
fn the_blob_layout_is_pinned_to_its_version_word() {
    // One instruction, both sink kinds, one pool entry — every region non-empty.
    let prog = LogicalProgram::new(
        vec![LogicalInstr::LoadCol { col: 1 }],
        vec![Sink::Col(1), Sink::Reg(Reg(0))],
        vec![b"ab".to_vec()],
    )
    .unwrap();
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
        (want, 3025886851799597962, 8),
        "the expr-blob layout or opcode vocabulary changed: bump EXPR_BLOB_VERSION if a number moved, \
         then paste what is reported here"
    );
}

/// A valid empty program's blob; the guard table below mutates a clone, so each
/// case differs from a decodable blob by exactly one flaw.
fn valid_empty_blob() -> Vec<u8> {
    encode_expr_blob(std::iter::empty(), std::iter::empty(), &[])
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
pub(crate) fn decodable_words() -> Vec<([u32; INSTR_WORDS], LogicalInstr)> {
    let shapes = [[11, 12, 13], [11, 0, 13], [11, 1, 13], [11, 12, u32::MAX]];
    ExprOp::ALL
        .iter()
        .flat_map(|op| (0..=64u32).flat_map(move |sel| shapes.map(|[a, b, c]| [op.as_wire(), sel, a, b, c])))
        .filter_map(|w| Some((w, LogicalInstr::from_wire(w).ok()?)))
        .collect()
}

/// `from_wire ∘ to_wire == id` over everything the decoder accepts, and the
/// encoder emits the opcode and selector it was decoded from. The sweep reaches
/// every [`LogicalInstr`] variant, and every selector family ends inside it.
#[test]
fn every_decodable_instruction_round_trips_through_the_wire_form() {
    let words = decodable_words();
    for &(w, x) in &words {
        let back = x.to_wire();
        assert_eq!(back[..2], w[..2], "{x:?}: opcode and selector");
        let again = LogicalInstr::from_wire(back);
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
        let (a1, a2) = encode_load_const(v);
        assert_eq!(decode_load_const(a1, a2), v, "load_const round-trip for {v}");
    }
}
