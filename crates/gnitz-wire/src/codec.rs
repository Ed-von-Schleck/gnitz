//! The little-endian byte cursor pair every variable-length codec encodes and
//! decodes through — this crate's, and the `gnitz-expr` program blob it carries
//! opaquely. A fixed-offset frame does not go through it: `wal` frames in
//! place, so a scatter can fill the region slices it hands back.
//! [`Writer`] appends, [`Reader`] consumes, and the two are field-for-field
//! inverses — a format is written and parsed against one shared definition of
//! what each width means, so no `Reader`/`Writer` format hand-rolls its own
//! offset arithmetic or `to_le_bytes` chain.
//!
//! Every [`Reader`] access is bounds-checked and reports a truncated blob as an
//! `Err` naming the format it was reading (`Reader::new`'s `ctx`), never a panic.
//! A primitive with fewer legal values than its byte has is read strictly, by
//! the one `Reader` method for it.
//!
//! Both types are `pub`; a method is `pub` only where that framing needs it.

use crate::TypeCode;

/// A little-endian forward writer — the inverse of [`Reader`].
pub struct Writer(Vec<u8>);

impl Writer {
    pub fn with_capacity(n: usize) -> Self {
        Writer(Vec::with_capacity(n))
    }

    pub(crate) fn u8(&mut self, v: u8) -> &mut Self {
        self.0.push(v);
        self
    }
    pub(crate) fn u16(&mut self, v: u16) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn u32(&mut self, v: u32) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn u64(&mut self, v: u64) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub(crate) fn u128(&mut self, v: u128) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub(crate) fn bool(&mut self, v: bool) -> &mut Self {
        self.u8(v as u8)
    }
    pub(crate) fn type_code(&mut self, tc: TypeCode) -> &mut Self {
        self.u8(tc.as_wire())
    }

    /// Raw bytes, no length prefix — for a section whose length the format
    /// already pins (a fixed-width sub-blob, or one preceded by its own count).
    pub(crate) fn raw(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.extend_from_slice(bytes);
        self
    }

    /// A WAL block, framed straight onto the end of the buffer.
    pub(crate) fn block(&mut self, rows: usize, regions: &[&[u8]]) -> &mut Self {
        crate::wal::append_block(rows, regions, &mut self.0);
        self
    }

    /// A `u32`-length-prefixed byte section — the inverse of [`Reader::bytes32`].
    pub fn bytes32(&mut self, bytes: &[u8]) -> &mut Self {
        self.u32(bytes.len() as u32).raw(bytes)
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

/// A bounds-checked forward reader over an encoded blob — every field access is
/// a `take` that rejects a truncated frame rather than panicking.
pub struct Reader<'a> {
    buf: &'a [u8],
    off: usize,
    /// The format being decoded, prefixed onto every error so a truncated blob
    /// names itself rather than the codec whose message was copied first.
    ctx: &'static str,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8], ctx: &'static str) -> Self {
        Reader { buf, off: 0, ctx }
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let ctx = self.ctx;
        let end = self
            .off
            .checked_add(n)
            .ok_or_else(|| format!("{ctx}: length overflow"))?;
        if end > self.buf.len() {
            return Err(format!(
                "{ctx}: truncated (need {n} bytes at offset {}, {} remain)",
                self.off,
                self.buf.len() - self.off
            ));
        }
        let s = &self.buf[self.off..end];
        self.off = end;
        Ok(s)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(crate) fn u128(&mut self) -> Result<u128, String> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }

    /// A boolean byte: `0` or `1`, and nothing else.
    pub(crate) fn bool(&mut self) -> Result<bool, String> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            b => Err(format!("{}: boolean byte {b} is neither 0 nor 1", self.ctx)),
        }
    }

    /// A flag byte whose bits all lie within `defined`.
    pub(crate) fn flags(&mut self, defined: u8) -> Result<u8, String> {
        let f = self.u8()?;
        if f & !defined != 0 {
            return Err(format!("{}: unknown flag bits {f:#04x}", self.ctx));
        }
        Ok(f)
    }

    /// A column type code byte naming a known [`TypeCode`].
    pub(crate) fn type_code(&mut self) -> Result<TypeCode, String> {
        let v = self.u8()?;
        TypeCode::from_wire(v).ok_or_else(|| format!("{}: invalid type code {v}", self.ctx))
    }

    /// A `u32`-length-prefixed byte section — the inverse of [`Writer::bytes32`].
    /// The length is bounds-checked by `take`, so a hostile prefix is a clean
    /// `Err` rather than an over-large allocation.
    pub fn bytes32(&mut self) -> Result<&'a [u8], String> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    /// A self-sizing WAL block — the inverse of [`Writer::block`].
    pub(crate) fn block(&mut self) -> Result<&'a [u8], String> {
        let n = crate::wal::block_slice_at(self.buf, self.off)
            .map_err(|e| format!("{}: {e}", self.ctx))?
            .len();
        self.take(n)
    }

    /// The next byte without consuming it — used to compute a variable-length
    /// `RangeDescriptor`'s span from its leading `n_eq`.
    pub(crate) fn peek_u8(&self) -> Result<u8, String> {
        self.buf
            .get(self.off)
            .copied()
            .ok_or_else(|| format!("{}: truncated reading descriptor length", self.ctx))
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.off
    }

    /// Reject leftover bytes: they mean the sender and this decoder disagree
    /// about the layout, which is what a format's version field exists to catch.
    pub fn expect_consumed(&self) -> Result<(), String> {
        match self.remaining() {
            0 => Ok(()),
            n => Err(format!("{}: {n} trailing bytes", self.ctx)),
        }
    }
}

/// The extent of the `u32`-length-prefixed section at `off`, or `None` when
/// `buf` does not hold it whole — [`Reader::bytes32`] for a walk that carries
/// absolute offsets instead of a cursor.
pub(crate) fn bytes32_extent(buf: &[u8], off: usize) -> Option<std::ops::Range<usize>> {
    let body = off.checked_add(4)?;
    let n = u32::from_le_bytes(buf.get(off..body)?.try_into().unwrap()) as usize;
    let end = body.checked_add(n)?;
    (end <= buf.len()).then_some(body..end)
}
