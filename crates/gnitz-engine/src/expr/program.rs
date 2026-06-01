//! Expression bytecode interpreter for SQL scalar functions.
//!
//! Evaluates compiled expression programs against columnar batch rows.

use crate::schema::{
    german_string_tail, SchemaDescriptor, PAYLOAD_MAPPING_PK_SENTINEL,
};
use crate::util::read_u32_le;

gnitz_wire::cast_consts! { pub(crate) i64;
    EXPR_LOAD_COL_INT, EXPR_LOAD_COL_FLOAT, EXPR_LOAD_CONST,
    EXPR_INT_ADD, EXPR_INT_SUB, EXPR_INT_MUL, EXPR_INT_DIV,
    EXPR_INT_MOD, EXPR_INT_NEG,
    EXPR_FLOAT_ADD, EXPR_FLOAT_SUB, EXPR_FLOAT_MUL, EXPR_FLOAT_DIV, EXPR_FLOAT_NEG,
    EXPR_CMP_EQ, EXPR_CMP_NE, EXPR_CMP_GT, EXPR_CMP_GE, EXPR_CMP_LT, EXPR_CMP_LE,
    EXPR_FCMP_EQ, EXPR_FCMP_NE, EXPR_FCMP_GT, EXPR_FCMP_GE, EXPR_FCMP_LT, EXPR_FCMP_LE,
    EXPR_BOOL_AND, EXPR_BOOL_OR, EXPR_BOOL_NOT,
    EXPR_IS_NULL, EXPR_IS_NOT_NULL,
    EXPR_EMIT, EXPR_INT_TO_FLOAT, EXPR_COPY_COL,
    EXPR_STR_COL_EQ_CONST, EXPR_STR_COL_LT_CONST, EXPR_STR_COL_LE_CONST,
    EXPR_STR_COL_EQ_COL, EXPR_STR_COL_LT_COL, EXPR_STR_COL_LE_COL,
    EXPR_EMIT_NULL,
    EXPR_LOAD_PAYLOAD_INT, EXPR_LOAD_PAYLOAD_FLOAT,
    // PK-region integer loads. a1 packs (pk_byte_offset << 16) | (col_size << 8)
    // | type_code so a single column of a compound OPK PK can be addressed.
    EXPR_LOAD_PK_UNSIGNED_INT, EXPR_LOAD_PK_SIGNED_INT,
    // Unsigned (U64) comparison/arithmetic/cast: the i64 register holds the raw
    // u64 bit pattern, so signed ops misbehave for values >= 2^63.
    EXPR_UCMP_GT, EXPR_UCMP_GE, EXPR_UCMP_LT, EXPR_UCMP_LE,
    EXPR_UDIV, EXPR_UMOD, EXPR_UINT_TO_FLOAT,
}

// ---------------------------------------------------------------------------
// ExprProgram — immutable bytecode container
// ---------------------------------------------------------------------------

pub struct ExprProgram {
    pub code: Vec<i64>,
    pub num_regs: u32,
    pub result_reg: u32,
    pub num_instrs: u32,
    pub const_strings: Vec<Vec<u8>>,
    pub const_prefixes: Vec<u32>,
    pub const_lengths: Vec<u32>,
    /// (size, type_code) for each physical payload column. Populated by
    /// `resolve_column_indices`; empty until then.
    pub(in crate::expr) payload_col_info: Vec<(u8, u8)>,
    /// True once `resolve_column_indices` has rewritten every column-bearing
    /// opcode's operand byte into payload-index-or-SENTINEL form. Eval entry
    /// points debug-assert this; a Plan-bypass caller that forgets to resolve
    /// gets a clear assertion rather than silent miscompute.
    pub(in crate::expr) resolved: bool,
    /// Bit `r` set iff register `r` is only consumed by boolean ops, so
    /// producers can skip the i64 unpack into `regs[r]` and downstream BOOL
    /// reads `bool_bits[r]` directly. Default 0 means no demotion — behaves
    /// like the pre-mask code path.
    pub(in crate::expr) bit_only_mask: u64,
    /// Bit `r` set iff register `r` is read by a boolean consumer; the
    /// producer must populate `bool_bits[r]`.
    pub(in crate::expr) bool_input_mask: u64,
}

impl ExprProgram {
    pub fn new(
        code: Vec<i64>,
        num_regs: u32,
        result_reg: u32,
        const_strings: Vec<Vec<u8>>,
    ) -> Self {
        assert_eq!(
            code.len() % 4,
            0,
            "ExprProgram: code length {} is not a multiple of 4",
            code.len()
        );
        let num_instrs = code.len() as u32 / 4;
        // The BOOL_AND/BOOL_OR 3VL paths and null-bit propagation in
        // `eval_batch` operate on `u64` words indexed by register, so the
        // register file is capped at 64.
        assert!(
            num_regs <= 64,
            "ExprProgram: num_regs={num_regs} exceeds the 64-register limit",
        );
        assert!(
            num_regs == 0 || result_reg < num_regs,
            "ExprProgram: result_reg={} >= num_regs={}",
            result_reg,
            num_regs
        );
        let mut const_prefixes = Vec::with_capacity(const_strings.len());
        let mut const_lengths = Vec::with_capacity(const_strings.len());
        for s in &const_strings {
            const_prefixes.push(compute_prefix(s));
            const_lengths.push(s.len() as u32);
        }
        gnitz_debug!("expr_program: instrs={} regs={} consts={}", num_instrs, num_regs, const_strings.len());
        let prog = ExprProgram {
            code,
            num_regs,
            result_reg,
            num_instrs,
            const_strings,
            const_prefixes,
            const_lengths,
            payload_col_info: Vec::new(),
            resolved: false,
            bit_only_mask: 0,
            bool_input_mask: 0,
        };
        // SSA assert: binary ALU ops must not alias dst with either source.
        // Hard assert (not debug-only): fires at compile time for compiler bugs.
        let nr = num_regs as i64;
        for instr in prog.code.chunks_exact(4) {
            let (op, dst, a1, a2) = (instr[0], instr[1], instr[2], instr[3]);
            if is_binary_alu_op(op) {
                assert!(
                    dst != a1 && dst != a2,
                    "ExprProgram: register aliasing dst={dst} a1={a1} a2={a2} op={op}",
                );
            }
            // Register-operand bounds: an out-of-range index would slice
            // arbitrary heap memory in `reg3` / `reg_mut`. Column- and
            // payload-bearing operands (loads, COPY_COL, STR_COL_*, EMIT
            // payloads) are not register indices and are excluded via the
            // `op_*` predicates.
            if op_writes_dst_reg(op) {
                assert!(dst >= 0 && dst < nr,
                    "ExprProgram: dst register {dst} out of range (num_regs={num_regs}) op={op}");
            }
            if op_reads_a1(op) {
                assert!(a1 >= 0 && a1 < nr,
                    "ExprProgram: a1 register {a1} out of range (num_regs={num_regs}) op={op}");
            }
            if op_reads_a2(op) {
                assert!(a2 >= 0 && a2 < nr,
                    "ExprProgram: a2 register {a2} out of range (num_regs={num_regs}) op={op}");
            }
        }
        prog
    }

    /// Returns true if no opcode in this program can produce a NULL result.
    /// Used by the batch evaluator to skip null-bit tracking entirely.
    ///
    /// Must be called after `resolve_column_indices`: column-bearing operands
    /// are read as resolved payload bytes (SENTINEL → PK, never nullable;
    /// otherwise dense payload index 0..N-1).
    pub fn is_strictly_non_nullable(&self, schema: &crate::schema::SchemaDescriptor) -> bool {
        let nullable_payload = |a: usize| -> bool {
            a < schema.num_payload_cols()
                && schema.columns[schema.payload_col_idx(a)].nullable != 0
        };
        for instr in self.code.chunks_exact(4) {
            let op = instr[0];
            let a1 = instr[2] as usize;
            let a2 = instr[3] as usize;
            match op {
                // Division/modulo produce NULL on zero divisor
                EXPR_INT_DIV | EXPR_INT_MOD | EXPR_UDIV | EXPR_UMOD | EXPR_FLOAT_DIV => return false,
                // IS_NULL / IS_NOT_NULL read null bits from the batch
                EXPR_IS_NULL | EXPR_IS_NOT_NULL => return false,
                // Column reads: null if the underlying column is nullable.
                // STR_COL_*_CONST and one-side of STR_COL_*_COL use a1 as the
                // resolved payload byte; STR_COL_*_COL also uses a2.
                EXPR_LOAD_PAYLOAD_INT | EXPR_LOAD_PAYLOAD_FLOAT
                | EXPR_STR_COL_EQ_CONST | EXPR_STR_COL_LT_CONST | EXPR_STR_COL_LE_CONST
                    if nullable_payload(a1) => return false,
                EXPR_STR_COL_EQ_COL | EXPR_STR_COL_LT_COL | EXPR_STR_COL_LE_COL
                    if nullable_payload(a1) || nullable_payload(a2) => return false,
                _ => {}
            }
        }
        true
    }

    /// True iff every instruction is COPY_COL with source columns 1, 2, 3, … in order.
    /// An identity projection that the compiler can elide.
    pub(crate) fn is_sequential_copy_projection(&self) -> bool {
        if self.code.is_empty() {
            return false;
        }
        self.code
            .chunks_exact(4)
            .enumerate()
            .all(|(i, instr)| instr[0] == EXPR_COPY_COL && instr[2] == (i + 1) as i64)
    }

    /// Classify each output-producing instruction in bytecode order.
    pub(crate) fn classify_output_cols(&self) -> Vec<OutputColKind> {
        let mut out_cols = Vec::new();
        let mut emit_count = 0usize;
        for i in 0..self.num_instrs as usize {
            let base = i * 4;
            let op = self.code[base];
            if op == EXPR_COPY_COL {
                // a1 is either a payload index (>= 0) or, for a PK source
                // column, `-(pk_byte_offset) - 1` (always negative).
                let raw = self.code[base + 2];
                let (src_pi, pk_off) = if raw < 0 {
                    (PAYLOAD_MAPPING_PK_SENTINEL, (-raw - 1) as u8)
                } else {
                    (raw as u8, 0u8)
                };
                out_cols.push(OutputColKind::CopyCol { src_pi, pk_off });
            } else if op == EXPR_EMIT {
                out_cols.push(OutputColKind::Emit(emit_count));
                emit_count += 1;
            } else if op == EXPR_EMIT_NULL {
                out_cols.push(OutputColKind::EmitNull);
            }
        }
        out_cols
    }

}

/// Classify each output-producing instruction.
pub(crate) enum OutputColKind {
    /// A column copied verbatim from the input. `src_pi` is the dense payload
    /// index, or `PAYLOAD_MAPPING_PK_SENTINEL` when the source is the PK
    /// region — in which case `pk_off` is the byte offset of the addressed
    /// column within the (compound) OPK PK region.
    CopyCol { src_pi: u8, pk_off: u8 },
    Emit(usize),
    EmitNull,
}

fn is_binary_alu_op(op: i64) -> bool {
    matches!(op,
        EXPR_INT_ADD | EXPR_INT_SUB | EXPR_INT_MUL | EXPR_INT_DIV | EXPR_INT_MOD |
        EXPR_UDIV | EXPR_UMOD |
        EXPR_FLOAT_ADD | EXPR_FLOAT_SUB | EXPR_FLOAT_MUL | EXPR_FLOAT_DIV |
        EXPR_CMP_EQ | EXPR_CMP_NE | EXPR_CMP_GT | EXPR_CMP_GE | EXPR_CMP_LT | EXPR_CMP_LE |
        EXPR_UCMP_GT | EXPR_UCMP_GE | EXPR_UCMP_LT | EXPR_UCMP_LE |
        EXPR_FCMP_EQ | EXPR_FCMP_NE | EXPR_FCMP_GT | EXPR_FCMP_GE | EXPR_FCMP_LT | EXPR_FCMP_LE |
        EXPR_BOOL_AND | EXPR_BOOL_OR
    )
}

/// 4-byte prefix of `s` as big-endian u32 for ordered comparison.
/// Short strings zero-pad on the right so that integer `<`/`>` matches
/// lexicographic byte order against any other zero-padded 4-byte prefix.
fn compute_prefix(s: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    let n = s.len().min(4);
    buf[..n].copy_from_slice(&s[..n]);
    u32::from_be_bytes(buf)
}

impl ExprProgram {
    /// Rewrite every column-bearing opcode's operand byte from a logical
    /// column index into either a dense payload index (0..N-1) or the
    /// `PAYLOAD_MAPPING_PK_SENTINEL` byte. After this pass eval handlers
    /// branch on `a1 == SENTINEL` instead of `ci == pki`, and the
    /// pk_index/payload arithmetic disappears from the inner loop.
    ///
    /// Opcode rewrites:
    /// - `LOAD_COL_INT` for PK → `LOAD_PK_{UNSIGNED,SIGNED}_INT` with a1 packed
    ///   as `(pk_byte_offset << 16) | (col_size << 8) | type_code`; otherwise
    ///   `LOAD_PAYLOAD_INT` with a1 ← `payload_mapping_byte(ci)`
    /// - `LOAD_COL_FLOAT` → `LOAD_PAYLOAD_FLOAT` (PK is never float)
    /// - `COPY_COL`: a1 ← `payload_mapping_byte(ci)` for payload columns, or
    ///   `-(pk_byte_offset) - 1` (negative sentinel) for PK columns
    /// - `IS_NULL`, `IS_NOT_NULL`, `STR_COL_{EQ,LT,LE}_CONST`:
    ///   a1 ← `payload_mapping_byte(ci)` (opcode unchanged)
    /// - `STR_COL_{EQ,LT,LE}_COL`: a1 AND a2 ← `payload_mapping_byte(ci)`
    /// - `CMP_{GT,GE,LT,LE}`, `INT_DIV`, `INT_MOD`, `INT_TO_FLOAT` → their
    ///   `UCMP_*` / `UDIV` / `UMOD` / `UINT_TO_FLOAT` unsigned forms when a U64
    ///   operand is detected via per-register type tracking.
    ///
    /// Idempotent: a second call is a no-op (gated by `self.resolved`).
    /// Must be called once per program, before the first call to
    /// `eval_predicate` / `eval_with_emit` / `eval_batch`. Called
    /// automatically by `Plan::from_predicate` and `Plan::from_map`.
    pub fn resolve_column_indices(&mut self, schema: &SchemaDescriptor) {
        if self.resolved {
            return;
        }
        use crate::schema::type_code;
        // Per-register type code of the most recently produced integer value.
        // Drives the signed→unsigned opcode swap for U64 operands, whose i64
        // register bit pattern is negative for values >= 2^63. 0 = unknown
        // (treated as signed). num_regs <= 64 is asserted in `new`.
        let mut reg_tc = [0u8; 64];
        let is_u64 = |tc: u8| tc == type_code::U64;
        for instr in self.code.chunks_exact_mut(4) {
            let op = instr[0];
            let dst = instr[1] as usize;
            match op {
                EXPR_LOAD_COL_INT => {
                    let logical_idx = instr[2] as usize;
                    let tc = schema.columns[logical_idx].type_code;
                    if schema.is_pk_col(logical_idx) {
                        let signed = crate::schema::is_signed_int(tc);
                        instr[0] = if signed { EXPR_LOAD_PK_SIGNED_INT } else { EXPR_LOAD_PK_UNSIGNED_INT };
                        let offset = schema.pk_byte_offset(logical_idx) as u64;
                        let size = schema.columns[logical_idx].size() as u64;
                        instr[2] = ((offset << 16) | (size << 8) | tc as u64) as i64;
                    } else {
                        instr[0] = EXPR_LOAD_PAYLOAD_INT;
                        instr[2] = schema.payload_mapping_byte(logical_idx) as i64;
                    }
                    reg_tc[dst] = tc;
                }
                EXPR_LOAD_COL_FLOAT => {
                    let ci = instr[2] as usize;
                    debug_assert!(
                        !schema.is_pk_col(ci),
                        "resolve_column_indices: LOAD_COL_FLOAT references PK column {ci} (PK is never float)",
                    );
                    instr[0] = EXPR_LOAD_PAYLOAD_FLOAT;
                    instr[2] = schema.payload_mapping_byte(ci) as i64;
                    reg_tc[dst] = 0;
                }
                EXPR_LOAD_CONST => {
                    reg_tc[dst] = 0;
                }
                EXPR_COPY_COL => {
                    // PK columns encode their OPK byte offset as a negative
                    // sentinel (always distinct from a payload index >= 0) so
                    // `from_map` / `classify_output_cols` can address the exact
                    // column within a compound PK region.
                    let logical_idx = instr[2] as usize;
                    if schema.is_pk_col(logical_idx) {
                        instr[2] = -(schema.pk_byte_offset(logical_idx) as i64) - 1;
                    } else {
                        instr[2] = schema.payload_mapping_byte(logical_idx) as i64;
                    }
                }
                EXPR_IS_NULL | EXPR_IS_NOT_NULL
                | EXPR_STR_COL_EQ_CONST | EXPR_STR_COL_LT_CONST | EXPR_STR_COL_LE_CONST => {
                    instr[2] = schema.payload_mapping_byte(instr[2] as usize) as i64;
                }
                EXPR_STR_COL_EQ_COL | EXPR_STR_COL_LT_COL | EXPR_STR_COL_LE_COL => {
                    instr[2] = schema.payload_mapping_byte(instr[2] as usize) as i64;
                    instr[3] = schema.payload_mapping_byte(instr[3] as usize) as i64;
                }
                EXPR_CMP_GT | EXPR_CMP_GE | EXPR_CMP_LT | EXPR_CMP_LE => {
                    if is_u64(reg_tc[instr[2] as usize]) || is_u64(reg_tc[instr[3] as usize]) {
                        instr[0] = match op {
                            EXPR_CMP_GT => EXPR_UCMP_GT,
                            EXPR_CMP_GE => EXPR_UCMP_GE,
                            EXPR_CMP_LT => EXPR_UCMP_LT,
                            _ => EXPR_UCMP_LE,
                        };
                    }
                    reg_tc[dst] = 0;
                }
                EXPR_CMP_EQ | EXPR_CMP_NE => {
                    // EQ/NE are bit-identical signed/unsigned; no swap needed.
                    reg_tc[dst] = 0;
                }
                EXPR_INT_DIV | EXPR_INT_MOD => {
                    let u = is_u64(reg_tc[instr[2] as usize]) || is_u64(reg_tc[instr[3] as usize]);
                    if u {
                        instr[0] = if op == EXPR_INT_DIV { EXPR_UDIV } else { EXPR_UMOD };
                    }
                    reg_tc[dst] = if u { type_code::U64 } else { 0 };
                }
                EXPR_INT_ADD | EXPR_INT_SUB | EXPR_INT_MUL => {
                    reg_tc[dst] = if is_u64(reg_tc[instr[2] as usize]) || is_u64(reg_tc[instr[3] as usize]) {
                        type_code::U64
                    } else {
                        0
                    };
                }
                EXPR_INT_NEG => {
                    reg_tc[dst] = reg_tc[instr[2] as usize];
                }
                EXPR_INT_TO_FLOAT => {
                    if is_u64(reg_tc[instr[2] as usize]) {
                        instr[0] = EXPR_UINT_TO_FLOAT;
                    }
                    reg_tc[dst] = 0;
                }
                _ => {}
            }
        }
        self.payload_col_info.clear();
        self.payload_col_info.extend(
            schema.payload_columns().map(|(_, _, col)| (col.size(), col.type_code))
        );
        self.resolved = true;
        // Default to map-context masks. The bool-bits reads in the BOOL_AND/OR
        // nullable arms require `bool_input_mask` to be populated by the
        // producers of their source registers, so classification is mandatory
        // for any program that may execute on the nullable path.
        // `Plan::from_predicate` overrides with `is_filter = true` to keep
        // `result_reg` eligible for bit_only.
        self.classify(false);
    }

    /// Populate `bit_only_mask` / `bool_input_mask` from the resolved bytecode.
    pub(in crate::expr) fn classify(&mut self, is_filter: bool) {
        let (bit_only, bool_input) = self.classify_registers(is_filter);
        self.bit_only_mask = bit_only;
        self.bool_input_mask = bool_input;
    }

    #[inline]
    pub(in crate::expr) fn is_bit_only(&self, reg: usize) -> bool {
        (self.bit_only_mask >> reg) & 1 != 0
    }

    /// True iff `reg`'s producer must write `bool_bits[reg]` — either because
    /// a downstream BOOL consumer reads it, or because `reg` is bit_only and
    /// run_filter reads `bool_bits[result_reg]` directly.
    #[inline]
    pub(in crate::expr) fn needs_bool_pack(&self, reg: usize) -> bool {
        ((self.bit_only_mask | self.bool_input_mask) >> reg) & 1 != 0
    }

    /// Classify each register's role on the nullable-arm hot path. Returns
    /// `(bit_only_mask, bool_input_mask)`.
    ///
    /// `is_filter = true` keeps `result_reg` eligible for `bit_only` (the
    /// `run_filter` fast path reads `bool_bits[result_reg]` directly). For
    /// maps, `result_reg` is demoted: EMIT already drags its source register
    /// through `is_unary_reg_reader`, but `result_reg` carries no map-side
    /// semantics and demoting it keeps any stray `regs[result_reg]` read live.
    ///
    /// Operand-shape note: `a1`/`a2` are register indices only for opcodes
    /// in `is_*_reg_reader` / `is_bool_consumer`; other opcodes pack constant
    /// bits, payload byte indices, or type codes into those slots, so the
    /// classifier enumerates opcode shapes rather than blindly indexing.
    pub(in crate::expr) fn classify_registers(&self, is_filter: bool) -> (u64, u64) {
        let mut bool_produced: u64 = 0;
        let mut non_bool_read: u64 = 0;
        let mut bool_input: u64 = 0;

        for instr in self.code.chunks_exact(4) {
            let op = instr[0];
            let dst = instr[1] as usize;
            let a1 = instr[2] as usize;
            let a2 = instr[3] as usize;

            if is_bool_producer(op) {
                debug_assert!(dst < 64, "classify_registers: dst {dst} ≥ 64");
                bool_produced |= 1u64 << dst;
            }
            if is_bool_consumer(op) {
                debug_assert!(a1 < 64, "classify_registers: BOOL src a1={a1} ≥ 64");
                bool_input |= 1u64 << a1;
                if is_binary_bool_consumer(op) {
                    debug_assert!(a2 < 64, "classify_registers: BOOL src a2={a2} ≥ 64");
                    bool_input |= 1u64 << a2;
                }
                continue;
            }
            if is_binary_reg_reader(op) {
                non_bool_read |= (1u64 << a1) | (1u64 << a2);
            } else if is_unary_reg_reader(op) {
                non_bool_read |= 1u64 << a1;
            }
        }

        let mut bit_only = bool_produced & !non_bool_read;
        if !is_filter && self.num_regs > 0 {
            bit_only &= !(1u64 << self.result_reg as usize);
        }
        (bit_only, bool_input)
    }
}

#[inline]
fn is_bool_producer(op: i64) -> bool {
    matches!(op,
        EXPR_CMP_EQ | EXPR_CMP_NE | EXPR_CMP_GT | EXPR_CMP_GE | EXPR_CMP_LT | EXPR_CMP_LE |
        EXPR_UCMP_GT | EXPR_UCMP_GE | EXPR_UCMP_LT | EXPR_UCMP_LE |
        EXPR_FCMP_EQ | EXPR_FCMP_NE | EXPR_FCMP_GT | EXPR_FCMP_GE | EXPR_FCMP_LT | EXPR_FCMP_LE |
        EXPR_STR_COL_EQ_CONST | EXPR_STR_COL_LT_CONST | EXPR_STR_COL_LE_CONST |
        EXPR_STR_COL_EQ_COL | EXPR_STR_COL_LT_COL | EXPR_STR_COL_LE_COL |
        EXPR_IS_NULL | EXPR_IS_NOT_NULL |
        EXPR_BOOL_AND | EXPR_BOOL_OR | EXPR_BOOL_NOT)
}

#[inline]
fn is_bool_consumer(op: i64) -> bool {
    matches!(op, EXPR_BOOL_AND | EXPR_BOOL_OR | EXPR_BOOL_NOT)
}

#[inline]
fn is_binary_bool_consumer(op: i64) -> bool {
    matches!(op, EXPR_BOOL_AND | EXPR_BOOL_OR)
}

#[inline]
fn is_binary_reg_reader(op: i64) -> bool {
    matches!(op,
        EXPR_INT_ADD | EXPR_INT_SUB | EXPR_INT_MUL | EXPR_INT_DIV | EXPR_INT_MOD |
        EXPR_UDIV | EXPR_UMOD |
        EXPR_FLOAT_ADD | EXPR_FLOAT_SUB | EXPR_FLOAT_MUL | EXPR_FLOAT_DIV |
        EXPR_CMP_EQ | EXPR_CMP_NE | EXPR_CMP_GT | EXPR_CMP_GE | EXPR_CMP_LT | EXPR_CMP_LE |
        EXPR_UCMP_GT | EXPR_UCMP_GE | EXPR_UCMP_LT | EXPR_UCMP_LE |
        EXPR_FCMP_EQ | EXPR_FCMP_NE | EXPR_FCMP_GT | EXPR_FCMP_GE | EXPR_FCMP_LT | EXPR_FCMP_LE)
}

#[inline]
fn is_unary_reg_reader(op: i64) -> bool {
    matches!(op, EXPR_INT_NEG | EXPR_FLOAT_NEG | EXPR_INT_TO_FLOAT | EXPR_UINT_TO_FLOAT | EXPR_EMIT)
}

/// True iff `instr[1]` (dst) is a register index. COPY_COL/EMIT/EMIT_NULL put
/// a type code or payload index there, not a register, so they are excluded.
#[inline]
fn op_writes_dst_reg(op: i64) -> bool {
    !matches!(op, EXPR_COPY_COL | EXPR_EMIT | EXPR_EMIT_NULL)
}

/// True iff `instr[2]` (a1) is a register index. Binary ALU, unary readers and
/// BOOL_NOT read a1 from a register; loads/COPY_COL/STR_COL_*/IS_NULL carry a
/// column or payload index there instead.
#[inline]
fn op_reads_a1(op: i64) -> bool {
    is_binary_alu_op(op) || is_unary_reg_reader(op) || op == EXPR_BOOL_NOT
}

/// True iff `instr[3]` (a2) is a register index — only the binary ALU ops.
#[inline]
fn op_reads_a2(op: i64) -> bool {
    is_binary_alu_op(op)
}

// ---------------------------------------------------------------------------
// String comparison helpers (column vs constant)
// ---------------------------------------------------------------------------

/// Compare a German String column value against a constant byte string.
/// Returns Ordering.
pub(in crate::expr) fn compare_col_string_vs_const(
    struct_bytes: &[u8],
    blob: &[u8],
    const_bytes: &[u8],
    const_prefix: u32,
    const_len: u32,
) -> std::cmp::Ordering {
    let col_len = read_u32_le(struct_bytes, 0) as usize;
    let c_len = const_len as usize;
    let min_len = col_len.min(c_len);

    // Single big-endian integer comparison of the 4-byte prefix.
    // Both sides zero-pad bytes beyond their actual length, so no masking
    // is needed: short strings naturally sort below longer ones with the
    // same prefix, and the zero padding never causes false equality when
    // lengths differ (the trailing length comparison resolves that).
    let col_pfx = u32::from_be_bytes(struct_bytes[4..8].try_into().unwrap());
    if col_pfx != const_prefix {
        return col_pfx.cmp(&const_prefix);
    }

    if min_len <= 4 {
        return col_len.cmp(&c_len);
    }

    // Bulk suffix comparison — vectorised memcmp via [u8]::cmp.
    let col_tail = german_string_tail(struct_bytes, blob, col_len, min_len);
    match col_tail.cmp(&const_bytes[4..min_len]) {
        std::cmp::Ordering::Equal => col_len.cmp(&c_len),
        ord => ord,
    }
}
