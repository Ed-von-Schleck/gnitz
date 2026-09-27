//! A little-endian cursor pair: [`Writer`] appends, [`Reader`] consumes, every
//! read bounds-checked. Reader errors are bare; [`decode_all`] labels a format's
//! errors once, at its entry point.

/// A little-endian forward writer — the inverse of [`Reader`].
#[derive(Default)]
pub struct Writer(Vec<u8>);

impl Writer {
    pub fn new() -> Self {
        Writer(Vec::new())
    }

    pub fn with_capacity(n: usize) -> Self {
        Writer(Vec::with_capacity(n))
    }

    pub fn u8(&mut self, v: u8) -> &mut Self {
        self.0.push(v);
        self
    }
    pub fn u16(&mut self, v: u16) -> &mut Self {
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
    pub fn u128(&mut self, v: u128) -> &mut Self {
        self.0.extend_from_slice(&v.to_le_bytes());
        self
    }
    pub fn bool(&mut self, v: bool) -> &mut Self {
        self.u8(v as u8)
    }

    /// Raw bytes, no length prefix — for a section whose length the format
    /// already pins (a fixed-width sub-blob, or one preceded by its own count).
    pub fn raw(&mut self, bytes: &[u8]) -> &mut Self {
        self.0.extend_from_slice(bytes);
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
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Reader { buf, off: 0 }
    }

    #[inline]
    pub fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        match self.buf.get(self.off..).and_then(|rest| rest.get(..n)) {
            Some(s) => {
                self.off += n;
                Ok(s)
            }
            None => Err(self.truncated(n)),
        }
    }

    #[cold]
    #[inline(never)]
    fn truncated(&self, n: usize) -> String {
        format!(
            "truncated (need {n} bytes at offset {}, {} remain)",
            self.off,
            self.buf.len() - self.off
        )
    }

    #[inline]
    pub fn u8(&mut self) -> Result<u8, String> {
        Ok(self.take(1)?[0])
    }
    #[inline]
    pub fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    #[inline]
    pub fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    #[inline]
    pub fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn u128(&mut self) -> Result<u128, String> {
        Ok(u128::from_le_bytes(self.take(16)?.try_into().unwrap()))
    }

    /// A boolean byte: `0` or `1`, and nothing else.
    pub fn bool(&mut self) -> Result<bool, String> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            b => Err(format!("boolean byte {b} is neither 0 nor 1")),
        }
    }

    /// A flag byte whose bits all lie within `defined`.
    #[inline]
    pub fn flags(&mut self, defined: u8) -> Result<u8, String> {
        let f = self.u8()?;
        if f & !defined != 0 {
            return Err(format!("unknown flag bits {f:#04x}"));
        }
        Ok(f)
    }

    /// A `u32`-length-prefixed byte section — the inverse of [`Writer::bytes32`].
    /// The length is bounds-checked by `take`, so a hostile prefix is a clean
    /// `Err` rather than an over-large allocation.
    #[inline]
    pub fn bytes32(&mut self) -> Result<&'a [u8], String> {
        let n = self.u32()? as usize;
        self.take(n)
    }

    /// Bytes consumed so far.
    pub fn pos(&self) -> usize {
        self.off
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.off
    }

    /// Reject leftover bytes: they mean the sender and this decoder disagree
    /// about the layout, which is what a format's version field exists to catch.
    fn expect_consumed(&self) -> Result<(), String> {
        match self.remaining() {
            0 => Ok(()),
            n => Err(format!("{n} trailing bytes")),
        }
    }
}

/// Decode all of `buf` with `f`, labelling any error with the format name `ctx`.
pub fn decode_all<'a, T>(
    buf: &'a [u8],
    ctx: &str,
    f: impl FnOnce(&mut Reader<'a>) -> Result<T, String>,
) -> Result<T, String> {
    let mut r = Reader::new(buf);
    f(&mut r)
        .and_then(|v| r.expect_consumed().map(|()| v))
        .map_err(|e| format!("{ctx}: {e}"))
}
