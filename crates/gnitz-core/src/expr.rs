use gnitz_wire::{
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
};

/// A compiled expression program: a flat list of 4-word instructions
/// (opcode, dst_reg, arg1, arg2) plus metadata for embedding in filter params.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExprProgram {
    pub num_regs:     u32,
    pub result_reg:   u32,
    pub code:         Vec<u32>,
    pub const_strings: Vec<String>,
}

/// Wire-format magic for serialised `ExprProgram` blobs: ASCII "EXPR".
pub const EXPR_BLOB_MAGIC:   u32 = 0x5258_5045; // "EXPR" little-endian
/// Current wire-format version for `ExprProgram` blobs.
pub const EXPR_BLOB_VERSION: u8  = 1;
const EXPR_BLOB_HEADER_SIZE: usize = 16;

/// Errors returned by [`ExprProgram::decode`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExprDecodeErr {
    /// Magic bytes did not match `EXPR_BLOB_MAGIC` ("EXPR").
    BadMagic,
    /// Version byte was not `EXPR_BLOB_VERSION`.
    BadVersion(u8),
    /// A reserved byte was non-zero (forwards-incompatibility guard).
    NonZeroReserved,
    /// Code length was not a multiple of 4 (every instruction is 4 u32 words).
    UnalignedCode(u32),
    /// The blob ended before the declared regions could be read in full.
    Truncated,
}

impl std::fmt::Display for ExprDecodeErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExprDecodeErr::BadMagic         => write!(f, "ExprProgram magic mismatch"),
            ExprDecodeErr::BadVersion(v)    => write!(f, "ExprProgram unknown version {}", v),
            ExprDecodeErr::NonZeroReserved  => write!(f, "ExprProgram reserved bytes must be zero"),
            ExprDecodeErr::UnalignedCode(n) => write!(f, "ExprProgram code length {} not a multiple of 4", n),
            ExprDecodeErr::Truncated        => write!(f, "ExprProgram blob truncated"),
        }
    }
}

impl std::error::Error for ExprDecodeErr {}

impl ExprProgram {
    /// Serialise to a self-contained byte sequence suitable for storage in a
    /// BLOB column. Format (all little-endian):
    ///
    /// ```text
    /// 0   4   magic "EXPR" (u32)
    /// 4   1   version (u8)
    /// 5   1   reserved (must be 0)
    /// 6   2   num_regs (u16)
    /// 8   2   result_reg (u16)
    /// 10  2   reserved (must be 0)
    /// 12  4   code word count N (u32; multiple of 4)
    /// 16  4N  code words (u32 each)
    /// ..  4   string count S (u32)
    /// ..  S × { 4-byte length L, L bytes UTF-8 }
    /// ```
    pub fn encode(&self) -> Vec<u8> {
        debug_assert!(self.code.len() % 4 == 0, "code length {} not 4-aligned", self.code.len());
        let mut buf = Vec::with_capacity(
            EXPR_BLOB_HEADER_SIZE + self.code.len() * 4 + 4
                + self.const_strings.iter().map(|s| 4 + s.len()).sum::<usize>(),
        );
        buf.extend_from_slice(&EXPR_BLOB_MAGIC.to_le_bytes());
        buf.push(EXPR_BLOB_VERSION);
        buf.push(0); // reserved
        buf.extend_from_slice(&(self.num_regs as u16).to_le_bytes());
        buf.extend_from_slice(&(self.result_reg as u16).to_le_bytes());
        buf.extend_from_slice(&[0, 0]); // reserved
        buf.extend_from_slice(&(self.code.len() as u32).to_le_bytes());
        for &word in &self.code {
            buf.extend_from_slice(&word.to_le_bytes());
        }
        buf.extend_from_slice(&(self.const_strings.len() as u32).to_le_bytes());
        for s in &self.const_strings {
            buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
            buf.extend_from_slice(s.as_bytes());
        }
        buf
    }

    /// Inverse of [`ExprProgram::encode`]. Validates magic, version, reserved
    /// fields, code-length alignment, and length consistency.
    pub fn decode(blob: &[u8]) -> Result<Self, ExprDecodeErr> {
        if blob.len() < EXPR_BLOB_HEADER_SIZE {
            return Err(ExprDecodeErr::Truncated);
        }
        let magic = u32::from_le_bytes(blob[0..4].try_into().unwrap());
        if magic != EXPR_BLOB_MAGIC {
            return Err(ExprDecodeErr::BadMagic);
        }
        let version = blob[4];
        if version != EXPR_BLOB_VERSION {
            return Err(ExprDecodeErr::BadVersion(version));
        }
        if blob[5] != 0 || blob[10] != 0 || blob[11] != 0 {
            return Err(ExprDecodeErr::NonZeroReserved);
        }
        let num_regs   = u16::from_le_bytes(blob[6..8].try_into().unwrap()) as u32;
        let result_reg = u16::from_le_bytes(blob[8..10].try_into().unwrap()) as u32;
        let n          = u32::from_le_bytes(blob[12..16].try_into().unwrap());
        if n % 4 != 0 {
            return Err(ExprDecodeErr::UnalignedCode(n));
        }
        let code_bytes = (n as usize) * 4;
        let code_end   = EXPR_BLOB_HEADER_SIZE + code_bytes;
        if blob.len() < code_end + 4 {
            return Err(ExprDecodeErr::Truncated);
        }
        let mut code = Vec::with_capacity(n as usize);
        for i in 0..n as usize {
            let off = EXPR_BLOB_HEADER_SIZE + i * 4;
            code.push(u32::from_le_bytes(blob[off..off + 4].try_into().unwrap()));
        }
        let s_count = u32::from_le_bytes(blob[code_end..code_end + 4].try_into().unwrap());
        let mut cur = code_end + 4;
        let mut const_strings = Vec::with_capacity(s_count as usize);
        for _ in 0..s_count {
            if blob.len() < cur + 4 { return Err(ExprDecodeErr::Truncated); }
            let l = u32::from_le_bytes(blob[cur..cur + 4].try_into().unwrap()) as usize;
            cur += 4;
            if blob.len() < cur + l { return Err(ExprDecodeErr::Truncated); }
            let s = std::string::String::from_utf8_lossy(&blob[cur..cur + l]).into_owned();
            cur += l;
            const_strings.push(s);
        }
        Ok(ExprProgram { num_regs, result_reg, code, const_strings })
    }
}

/// Builds an expression bytecode program with automatic register allocation.
pub struct ExprBuilder {
    code:          Vec<u32>,
    next_reg:      u32,
    const_strings: Vec<String>,
}

impl ExprBuilder {
    pub fn new() -> Self {
        ExprBuilder { code: Vec::new(), next_reg: 0, const_strings: Vec::new() }
    }

    fn alloc_reg(&mut self) -> u32 {
        let r = self.next_reg;
        self.next_reg += 1;
        r
    }

    fn emit(&mut self, op: u32, dst: u32, a1: u32, a2: u32) {
        self.code.extend_from_slice(&[op, dst, a1, a2]);
    }

    pub fn load_col_int(&mut self, col_idx: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_LOAD_COL_INT, dst, col_idx as u32, 0);
        dst
    }

    pub fn load_col_float(&mut self, col_idx: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_LOAD_COL_FLOAT, dst, col_idx as u32, 0);
        dst
    }

    /// Full i64 constant. Low 32 bits in arg1, high 32 bits in arg2.
    /// Reconstructed as: `(a2 << 32) | (a1 & 0xFFFFFFFF)`.
    pub fn load_const(&mut self, value: i64) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_LOAD_CONST, dst, value as u32, (value >> 32) as u32);
        dst
    }

    // --- Integer arithmetic ---

    pub fn add(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_ADD, dst, a, b);
        dst
    }

    pub fn sub(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_SUB, dst, a, b);
        dst
    }

    pub fn mul(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_MUL, dst, a, b);
        dst
    }

    pub fn div(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_DIV, dst, a, b);
        dst
    }

    pub fn modulo(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_MOD, dst, a, b);
        dst
    }

    pub fn neg_int(&mut self, a: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_NEG, dst, a, 0);
        dst
    }

    // --- Float arithmetic ---

    pub fn float_add(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FLOAT_ADD, dst, a, b);
        dst
    }

    pub fn float_sub(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FLOAT_SUB, dst, a, b);
        dst
    }

    pub fn float_mul(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FLOAT_MUL, dst, a, b);
        dst
    }

    pub fn float_div(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FLOAT_DIV, dst, a, b);
        dst
    }

    pub fn float_neg(&mut self, a: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FLOAT_NEG, dst, a, 0);
        dst
    }

    // --- Integer comparison ---

    pub fn cmp_eq(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_CMP_EQ, dst, a, b);
        dst
    }

    pub fn cmp_ne(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_CMP_NE, dst, a, b);
        dst
    }

    pub fn cmp_gt(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_CMP_GT, dst, a, b);
        dst
    }

    pub fn cmp_ge(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_CMP_GE, dst, a, b);
        dst
    }

    pub fn cmp_lt(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_CMP_LT, dst, a, b);
        dst
    }

    pub fn cmp_le(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_CMP_LE, dst, a, b);
        dst
    }

    // --- Float comparison ---

    pub fn fcmp_eq(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FCMP_EQ, dst, a, b);
        dst
    }

    pub fn fcmp_ne(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FCMP_NE, dst, a, b);
        dst
    }

    pub fn fcmp_gt(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FCMP_GT, dst, a, b);
        dst
    }

    pub fn fcmp_ge(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FCMP_GE, dst, a, b);
        dst
    }

    pub fn fcmp_lt(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FCMP_LT, dst, a, b);
        dst
    }

    pub fn fcmp_le(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_FCMP_LE, dst, a, b);
        dst
    }

    // --- Boolean ---

    pub fn bool_and(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_BOOL_AND, dst, a, b);
        dst
    }

    pub fn bool_or(&mut self, a: u32, b: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_BOOL_OR, dst, a, b);
        dst
    }

    pub fn bool_not(&mut self, a: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_BOOL_NOT, dst, a, 0);
        dst
    }

    // --- Null checks ---

    pub fn is_null(&mut self, col_idx: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_IS_NULL, dst, col_idx as u32, 0);
        dst
    }

    pub fn is_not_null(&mut self, col_idx: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_IS_NOT_NULL, dst, col_idx as u32, 0);
        dst
    }

    // --- Type conversion ---

    pub fn int_to_float(&mut self, src: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_INT_TO_FLOAT, dst, src, 0);
        dst
    }

    // --- Output opcodes ---

    pub fn emit_col(&mut self, src_reg: u32, payload_col_idx: u32) {
        self.emit(EXPR_EMIT, 0, src_reg, payload_col_idx);
    }

    pub fn copy_col(&mut self, type_code: u32, src_col_idx: u32, payload_col_idx: u32) {
        self.emit(EXPR_COPY_COL, type_code, src_col_idx, payload_col_idx);
    }

    pub fn emit_null(&mut self, payload_col_idx: u32) {
        self.emit(EXPR_EMIT_NULL, 0, payload_col_idx, 0);
    }

    // --- String constants ---

    pub fn add_const_string(&mut self, s: String) -> u32 {
        let idx = self.const_strings.len() as u32;
        self.const_strings.push(s);
        idx
    }

    // --- String comparisons ---

    pub fn str_col_eq_const(&mut self, col_idx: usize, const_idx: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_COL_EQ_CONST, dst, col_idx as u32, const_idx);
        dst
    }

    pub fn str_col_lt_const(&mut self, col_idx: usize, const_idx: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_COL_LT_CONST, dst, col_idx as u32, const_idx);
        dst
    }

    pub fn str_col_le_const(&mut self, col_idx: usize, const_idx: u32) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_COL_LE_CONST, dst, col_idx as u32, const_idx);
        dst
    }

    pub fn str_col_eq_col(&mut self, col_a: usize, col_b: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_COL_EQ_COL, dst, col_a as u32, col_b as u32);
        dst
    }

    pub fn str_col_lt_col(&mut self, col_a: usize, col_b: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_COL_LT_COL, dst, col_a as u32, col_b as u32);
        dst
    }

    pub fn str_col_le_col(&mut self, col_a: usize, col_b: usize) -> u32 {
        let dst = self.alloc_reg();
        self.emit(EXPR_STR_COL_LE_COL, dst, col_a as u32, col_b as u32);
        dst
    }

    pub fn build(self, result_reg: u32) -> ExprProgram {
        ExprProgram {
            num_regs:      self.next_reg,
            result_reg,
            code:          self.code,
            const_strings: self.const_strings,
        }
    }
}

#[cfg(test)]
mod expr_blob_tests {
    use super::*;

    #[test]
    fn round_trip_empty_program() {
        let p = ExprProgram { num_regs: 0, result_reg: 0, code: vec![], const_strings: vec![] };
        let enc = p.encode();
        let dec = ExprProgram::decode(&enc).unwrap();
        assert_eq!(p, dec);
    }

    #[test]
    fn round_trip_program_with_strings() {
        let p = ExprProgram {
            num_regs: 5,
            result_reg: 4,
            code: vec![1, 2, 3, 4, 5, 6, 7, 8],
            const_strings: vec!["alpha".into(), "".into(), "längre sträng".into()],
        };
        let enc = p.encode();
        let dec = ExprProgram::decode(&enc).unwrap();
        assert_eq!(p, dec);
    }

    #[test]
    fn rejects_bad_magic() {
        let mut bad = ExprProgram { num_regs: 0, result_reg: 0, code: vec![], const_strings: vec![] }.encode();
        bad[0] = 0;
        assert_eq!(ExprProgram::decode(&bad), Err(ExprDecodeErr::BadMagic));
    }

    #[test]
    fn rejects_unaligned_code_length() {
        // Manually craft a header with a non-multiple-of-4 code length.
        let mut buf = Vec::new();
        buf.extend_from_slice(&EXPR_BLOB_MAGIC.to_le_bytes());
        buf.push(EXPR_BLOB_VERSION);
        buf.push(0);
        buf.extend_from_slice(&0u16.to_le_bytes()); // num_regs
        buf.extend_from_slice(&0u16.to_le_bytes()); // result_reg
        buf.extend_from_slice(&[0, 0]);             // reserved
        buf.extend_from_slice(&3u32.to_le_bytes()); // n=3 — not multiple of 4
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes()); // string count
        assert!(matches!(ExprProgram::decode(&buf), Err(ExprDecodeErr::UnalignedCode(3))));
    }
}
