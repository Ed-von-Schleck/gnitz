//! The little-endian byte cursor pair every variable-length codec in this crate
//! encodes and decodes through. [`Writer`] appends, [`Reader`] consumes, and the
//! two are field-for-field inverses — a format is written and parsed against one
//! shared definition of what each width means, so no codec hand-rolls its own
//! offset arithmetic or `to_le_bytes` chain.
//!
//! Every [`Reader`] access is bounds-checked and reports a truncated blob as an
//! `Err` naming the format it was reading (`Reader::new`'s `ctx`), never a panic.

/// A little-endian forward writer — the inverse of [`Reader`].
pub(crate) struct Writer(Vec<u8>);

impl Writer {
    pub(crate) fn with_capacity(n: usize) -> Self {
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
    pub(crate) fn u32(&mut self, v: u32) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub(crate) fn u64(&mut self, v: u64) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub(crate) fn u128(&mut self, v: u128) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }

    /// Raw bytes, no length prefix — for a section whose length the format
    /// already pins (a fixed-width sub-blob, or one preceded by its own count).
    pub(crate) fn raw(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.extend_from_slice(bytes);
        self
    }

    /// A `u32`-length-prefixed byte section — the inverse of [`Reader::bytes32`].
    pub(crate) fn bytes32(&mut self, bytes: &[u8]) -> &mut Self {
        self.u32(bytes.len() as u32).raw(bytes)
    }

    pub(crate) fn into_vec(self) -> Vec<u8> {
        self.0
    }
}

/// A bounds-checked forward reader over an encoded blob — every field access is
/// a `take` that rejects a truncated frame rather than panicking.
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    off: usize,
    /// The format being decoded, prefixed onto every error so a truncated blob
    /// names itself rather than the codec whose message was copied first.
    ctx: &'static str,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8], ctx: &'static str) -> Self {
        Reader { buf, off: 0, ctx }
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
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
    pub(crate) fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub(crate) fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(crate) fn u128(&mut self) -> Result<u128, String> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }

    /// A `u32`-length-prefixed byte section — the inverse of [`Writer::bytes32`].
    /// The length is bounds-checked by `take`, so a hostile prefix is a clean
    /// `Err` rather than an over-large allocation.
    pub(crate) fn bytes32(&mut self) -> Result<&'a [u8], String> {
        let n = self.u32()? as usize;
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

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.off
    }

    /// Reject leftover bytes: they mean the sender and this decoder disagree
    /// about the layout, which is what a format's version field exists to catch.
    pub(crate) fn expect_consumed(&self) -> Result<(), String> {
        match self.remaining() {
            0 => Ok(()),
            n => Err(format!("{}: {n} trailing bytes", self.ctx)),
        }
    }
}
