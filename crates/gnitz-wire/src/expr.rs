//! Expr-blob framing: the length-prefixed regions a compiled expression program
//! travels in, generic over the words they carry — the instruction vocabulary
//! those words spell lives in `gnitz-expr`. The layout stays here beside
//! [`EXPR_BLOB_VERSION`], which [`crate::SYS_SCHEMA_DIGEST`] folds in.

use crate::codec::{Reader, Writer};

/// Current version of the layout below. Carried by no blob — each rides a slot
/// of an already-versioned container — but folded into
/// [`crate::SYS_SCHEMA_DIGEST`], which rejects one stored under another layout.
pub(crate) const EXPR_BLOB_VERSION: u8 = 4;

/// Fixed header width, in bytes: the output word and the code count.
const EXPR_BLOB_HEADER_SIZE: usize = 8;

/// The regions of one encoded program, borrowed. `code` and `sinks` stay bytes:
/// what their words mean is the vocabulary's rule, checked where they decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExprBlobView<'a> {
    /// The program's output, as its vocabulary spells it — a sentinel naming the
    /// sink region, or the register a result is read out of.
    pub output: u32,
    pub code: &'a [u8],
    pub sinks: &'a [u8],
    pub const_strings: Vec<&'a [u8]>,
}

/// Serialise an expr program. Layout (all little-endian):
///
/// ```text
/// 0   4   output (u32)
/// 4   4   code word count N (u32)
/// 8   4N  code words (u32 each)
/// ..  4   sink word count M (u32)
/// ..  4M  sink words (u32 each)
/// ..  4   string count S (u32)
/// ..  S × { 4-byte length L, L bytes }
/// ```
///
/// The register count is not carried: a register is the index of the
/// instruction that writes it, so the code region's length *is* the register
/// file's size.
pub fn encode_expr_blob(
    output: u32,
    code: impl ExactSizeIterator<Item = [u32; 5]>,
    sinks: impl ExactSizeIterator<Item = [u32; 2]>,
    const_strings: &[impl AsRef<[u8]>],
) -> Vec<u8> {
    let (code_words, sink_words) = (code.len() * 5, sinks.len() * 2);
    let mut w = Writer::with_capacity(
        EXPR_BLOB_HEADER_SIZE
            + (code_words + sink_words + 2) * 4
            + const_strings.iter().map(|s| 4 + s.as_ref().len()).sum::<usize>(),
    );
    w.u32(output).u32(code_words as u32);
    for instr in code {
        for word in instr {
            w.u32(word);
        }
    }
    w.u32(sink_words as u32);
    for sink in sinks {
        for word in sink {
            w.u32(word);
        }
    }
    w.u32(const_strings.len() as u32);
    for s in const_strings {
        w.bytes32(s.as_ref());
    }
    w.into_vec()
}

/// Inverse of [`encode_expr_blob`]: the regions, borrowed. Every count is held to
/// the bytes present and the trailing bytes to none — but this is not a program.
/// Opcodes, registers, columns and region alignment are the vocabulary's to check.
pub fn decode_expr_blob(blob: &[u8]) -> Result<ExprBlobView<'_>, String> {
    let mut r = Reader::new(blob, "expr blob");
    let output = r.u32()?;
    // `take` bounds each count against the bytes left, so a forged one is an
    // `Err` rather than a region overlapping the next.
    let code = {
        let n = r.u32()? as usize;
        r.take(n * 4)?
    };
    let sinks = {
        let n = r.u32()? as usize;
        r.take(n * 4)?
    };
    let s_count = r.u32()? as usize;
    // Each string costs at least its 4-byte length prefix; bound s_count against the
    // remaining bytes before reserving, so a corrupt count can't drive a huge with_capacity.
    if s_count > r.remaining() / 4 {
        return Err(format!(
            "expr blob: string count {s_count} exceeds the {} bytes remaining",
            r.remaining()
        ));
    }
    let mut const_strings = Vec::with_capacity(s_count);
    for _ in 0..s_count {
        const_strings.push(r.bytes32()?);
    }
    r.expect_consumed()?;
    Ok(ExprBlobView { output, code, sinks, const_strings })
}

#[cfg(test)]
#[path = "tests/expr.rs"]
mod tests;
