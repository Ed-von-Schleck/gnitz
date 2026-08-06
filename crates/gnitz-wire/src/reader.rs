//! The bounds-checked forward reader every variable-length codec in this crate
//! decodes through — every field access is a `take` that rejects a truncated
//! frame rather than panicking, so no codec hand-rolls its own offset
//! arithmetic.

/// A bounds-checked forward reader over the encoded blob — every field access
/// is a `take` that rejects a truncated frame rather than panicking.
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
    off: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Reader { buf, off: 0 }
    }

    pub(crate) fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        let end = self.off.checked_add(n).ok_or("read_spec: length overflow")?;
        if end > self.buf.len() {
            return Err(format!(
                "read_spec: truncated (need {n} bytes at offset {}, {} remain)",
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

    /// A `u32`-length-prefixed byte section — the inverse of [`put_bytes32`].
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
            .ok_or_else(|| "read_spec: truncated reading descriptor length".to_string())
    }

    pub(crate) fn remaining(&self) -> usize {
        self.buf.len() - self.off
    }
}
