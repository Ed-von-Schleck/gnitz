//! A little-endian cursor pair: [`Writer`] appends, [`Reader`] consumes, every
//! read bounds-checked. Reader errors are bare; [`decode_all`] labels a format's
//! errors once, at its entry point.

/// The longest list a [`Writer::count`] states without saturating, so a reader
/// capping at it still refuses one longer.
pub const MAX_LIST_LEN: usize = u16::MAX as usize - 1;

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

    pub fn put<T: Wire>(&mut self, v: &T) -> &mut Self {
        v.write(self);
        self
    }

    /// A list's length. One longer than [`MAX_LIST_LEN`] saturates, which every
    /// reader's cap refuses.
    pub fn count(&mut self, n: usize) -> &mut Self {
        self.u16(u16::try_from(n).unwrap_or(u16::MAX))
    }

    /// A counted list: its length, then each item — the inverse of [`Reader::list`].
    pub fn list<T: Wire>(&mut self, items: &[T]) -> &mut Self {
        self.count(items.len());
        for item in items {
            item.write(self);
        }
        self
    }

    pub fn into_vec(self) -> Vec<u8> {
        self.0
    }

    /// The bytes written so far.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Drop every byte written, keeping the buffer for the next encoding.
    pub fn clear(&mut self) {
        self.0.clear();
    }
}

/// A value with one layout, spliced into a larger blob.
pub trait Wire: Sized {
    fn write(&self, w: &mut Writer);
    fn read(r: &mut Reader) -> Result<Self, String>;
}

impl Wire for u32 {
    fn write(&self, w: &mut Writer) {
        w.u32(*self);
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        r.u32()
    }
}

impl Wire for bool {
    fn write(&self, w: &mut Writer) {
        w.bool(*self);
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        r.bool()
    }
}

/// A presence flag, then the value when present.
impl<T: Wire> Wire for Option<T> {
    fn write(&self, w: &mut Writer) {
        match self {
            Some(v) => w.bool(true).put(v),
            None => w.bool(false),
        };
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        r.bool()?.then(|| r.get()).transpose()
    }
}

/// UTF-8 text as a [`Writer::bytes32`] section.
impl Wire for String {
    fn write(&self, w: &mut Writer) {
        w.bytes32(self.as_bytes());
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        r.str32().map(str::to_owned)
    }
}

impl<A: Wire, B: Wire> Wire for (A, B) {
    fn write(&self, w: &mut Writer) {
        w.put(&self.0).put(&self.1);
    }
    fn read(r: &mut Reader) -> Result<Self, String> {
        Ok((r.get()?, r.get()?))
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

    #[inline(always)]
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

    /// The next `N` bytes, as the one integer field they are.
    #[inline]
    fn int<const N: usize>(&mut self) -> Result<[u8; N], String> {
        let bytes = self.take(N)?;
        #[cfg(feature = "field-map")]
        crate::fields::note_field(bytes);
        Ok(bytes.try_into().unwrap())
    }

    #[inline]
    pub fn u8(&mut self) -> Result<u8, String> {
        Ok(self.int::<1>()?[0])
    }
    #[inline]
    pub fn u16(&mut self) -> Result<u16, String> {
        Ok(u16::from_le_bytes(self.int()?))
    }
    #[inline]
    pub fn u32(&mut self) -> Result<u32, String> {
        Ok(u32::from_le_bytes(self.int()?))
    }
    #[inline]
    pub fn u64(&mut self) -> Result<u64, String> {
        Ok(u64::from_le_bytes(self.int()?))
    }
    #[inline]
    pub fn u128(&mut self) -> Result<u128, String> {
        Ok(u128::from_le_bytes(self.int()?))
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
        let content = self.take(n)?;
        #[cfg(feature = "field-map")]
        crate::fields::note_section(&self.buf[self.off - n - 4..self.off - n], content);
        Ok(content)
    }

    /// A [`Self::bytes32`] section that is UTF-8 text.
    pub fn str32(&mut self) -> Result<&'a str, String> {
        std::str::from_utf8(self.bytes32()?).map_err(|_| "text is not UTF-8".to_string())
    }

    pub fn get<T: Wire>(&mut self) -> Result<T, String> {
        T::read(self)
    }

    /// A counted list of at most `cap` items, each read by `item`: the inverse of
    /// a [`Writer::count`] and that many items. A count past `cap` is refused
    /// before anything is sized off it.
    pub fn items<T>(
        &mut self,
        what: &str,
        cap: usize,
        mut item: impl FnMut(&mut Self) -> Result<T, String>,
    ) -> Result<Vec<T>, String> {
        debug_assert!(cap <= MAX_LIST_LEN, "a saturated count must stay refusable");
        let n = self.u16()? as usize;
        if n > cap {
            return Err(format!("{what}: {n} entries exceeds cap {cap}"));
        }
        #[cfg(feature = "field-map")]
        let (count, mut spans) = (self.off - 2, Vec::with_capacity(n));
        // Sized once: a fallible `collect` has no lower size hint.
        let mut items = Vec::with_capacity(n);
        for _ in 0..n {
            #[cfg(feature = "field-map")]
            let start = self.off - count;
            items.push(item(self)?);
            #[cfg(feature = "field-map")]
            spans.push(start..self.off - count);
        }
        #[cfg(feature = "field-map")]
        crate::fields::note_list(&self.buf[count..count + 2], &spans);
        Ok(items)
    }

    /// A counted list of at most `cap` items — the inverse of [`Writer::list`].
    pub fn list<T: Wire>(&mut self, what: &str, cap: usize) -> Result<Vec<T>, String> {
        self.items(what, cap, T::read)
    }

    /// Bytes consumed so far.
    pub fn pos(&self) -> usize {
        self.off
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.off
    }

    /// The bytes not yet consumed, left unconsumed.
    pub(crate) fn rest(&self) -> &'a [u8] {
        &self.buf[self.off..]
    }
}

/// Decode all of `buf` with `f`, labelling any error with the format name `ctx`.
/// Leftover bytes are refused: they mean the sender and this decoder disagree
/// about the layout, which is what a format's version field exists to catch.
pub fn decode_all<'a, T>(
    buf: &'a [u8],
    ctx: &str,
    f: impl FnOnce(&mut Reader<'a>) -> Result<T, String>,
) -> Result<T, String> {
    let mut r = Reader::new(buf);
    f(&mut r)
        .and_then(|v| match r.remaining() {
            0 => Ok(v),
            n => Err(format!("{n} trailing bytes")),
        })
        .map_err(|e| format!("{ctx}: {e}"))
}

#[cfg(test)]
#[path = "tests/codec.rs"]
mod tests;
