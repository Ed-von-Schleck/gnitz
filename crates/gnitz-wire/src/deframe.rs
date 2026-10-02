//! The deframer both ends of a connection read through: a `u32` LE length prefix,
//! then exactly that many payload bytes.

use std::mem::MaybeUninit;

/// The payload ceiling of every frame, in both directions.
pub const MAX_FRAME_PAYLOAD: usize = 64 << 20;

/// Width of the `u32` LE payload-length prefix in front of every frame. Zero is
/// never a legal length.
pub const FRAME_LEN_PREFIX_BYTES: usize = 4;

/// The length prefix of a `len`-byte payload, `len` in `1..=MAX_FRAME_PAYLOAD`.
pub fn frame_len_prefix(len: usize) -> [u8; FRAME_LEN_PREFIX_BYTES] {
    debug_assert!((1..=MAX_FRAME_PAYLOAD).contains(&len), "frame payload of {len} bytes");
    (len as u32).to_le_bytes()
}

/// Why a frame was refused at its length prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameLenError {
    /// Zero: every frame carries a payload, so no sender emits one.
    Zero,
    /// Past [`MAX_FRAME_PAYLOAD`].
    Oversize { len: usize },
    /// The payload's buffer could not be allocated.
    Alloc { len: usize },
}

/// Turns a byte stream into frames, each allocated at exactly its declared
/// length once `admit` has accepted that length; `A` is what `admit` answered,
/// handed back with the frame. A split length prefix is held here, never in the
/// caller.
pub struct Deframer<A = ()> {
    hdr: [u8; FRAME_LEN_PREFIX_BYTES],
    hdr_len: usize,
    pending: Option<Pending<A>>,
}

/// The payload being filled.
struct Pending<A> {
    buf: Box<[MaybeUninit<u8>]>,
    /// How much of `buf` has arrived.
    pos: usize,
    admitted: A,
}

/// One whole payload, and what `admit` answered for it.
pub type Frame<A> = (Box<[u8]>, A);

impl<A> Default for Deframer<A> {
    fn default() -> Self {
        Deframer {
            hdr: [0; FRAME_LEN_PREFIX_BYTES],
            hdr_len: 0,
            pending: None,
        }
    }
}

impl<A> Deframer<A> {
    /// Mid-frame: a split prefix or a partial payload is buffered.
    pub fn is_mid_frame(&self) -> bool {
        self.hdr_len > 0 || self.pending.is_some()
    }

    /// Consume `src` from the front until one frame is whole and return it with
    /// its admission, or `None` once `src` is exhausted.
    pub fn feed<E: From<FrameLenError>>(
        &mut self,
        src: &mut &[u8],
        mut admit: impl FnMut(usize) -> Result<A, E>,
    ) -> Result<Option<Frame<A>>, E> {
        loop {
            if let Some(p) = self.pending.take_if(|p| p.pos == p.buf.len()) {
                // SAFETY: every byte below `pos` was written by `feed`'s own copy or
                // vouched for through `filled`, and `pos` is the length.
                return Ok(Some((unsafe { p.buf.assume_init() }, p.admitted)));
            }
            if let Some(Pending { buf, pos, .. }) = &mut self.pending {
                let take = (buf.len() - *pos).min(src.len());
                if take == 0 {
                    return Ok(None);
                }
                buf[*pos..][..take].write_copy_of_slice(&src[..take]);
                *pos += take;
                *src = &src[take..];
                continue;
            }
            if src.is_empty() {
                return Ok(None);
            }
            let take = (FRAME_LEN_PREFIX_BYTES - self.hdr_len).min(src.len());
            self.hdr[self.hdr_len..][..take].copy_from_slice(&src[..take]);
            self.hdr_len += take;
            *src = &src[take..];
            if self.hdr_len == FRAME_LEN_PREFIX_BYTES {
                self.hdr_len = 0;
                let len = match u32::from_le_bytes(self.hdr) as usize {
                    0 => return Err(FrameLenError::Zero.into()),
                    len if len > MAX_FRAME_PAYLOAD => return Err(FrameLenError::Oversize { len }.into()),
                    len => len,
                };
                let admitted = admit(len)?;
                let mut v: Vec<MaybeUninit<u8>> = Vec::new();
                // A failure drops `admitted` here.
                v.try_reserve_exact(len).map_err(|_| FrameLenError::Alloc { len })?;
                // SAFETY: `len` elements are reserved, and `MaybeUninit` needs no init.
                unsafe { v.set_len(len) };
                self.pending = Some(Pending {
                    buf: v.into_boxed_slice(),
                    pos: 0,
                    admitted,
                });
            }
        }
    }

    /// The unfilled tail of the payload in progress, for a reader that reads
    /// straight into it.
    pub fn payload_tail(&mut self) -> Option<&mut [MaybeUninit<u8>]> {
        self.pending.as_mut().map(|p| &mut p.buf[p.pos..])
    }

    /// Record `n` bytes written at the head of [`Self::payload_tail`]. The next
    /// `feed` hands the frame out once it is whole.
    ///
    /// # Safety
    /// Those `n` bytes must be initialised.
    pub unsafe fn filled(&mut self, n: usize) {
        self.pending.as_mut().expect("a payload in progress").pos += n;
    }
}

#[cfg(test)]
#[path = "tests/deframe.rs"]
mod tests;
