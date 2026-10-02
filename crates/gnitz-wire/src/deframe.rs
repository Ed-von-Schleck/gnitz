//! The deframer both ends of a connection read through: a `u32` LE length prefix,
//! then exactly that many payload bytes.

use std::borrow::Cow;
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

/// Turns a byte stream into frames. `A` is what `admit` answered for a frame's
/// declared length, handed back with the frame.
pub struct Deframer<A = ()> {
    hdr: [u8; FRAME_LEN_PREFIX_BYTES],
    hdr_len: usize,
    pending: Option<Pending<A>>,
    /// The window a read is out on, until it lands or `feed` runs.
    lent: Option<Lent>,
}

/// Which storage [`Deframer::window`] handed out.
enum Lent {
    Carry,
    Payload,
}

/// The payload being filled.
struct Pending<A> {
    buf: Box<[MaybeUninit<u8>]>,
    /// How much of `buf` has arrived.
    pos: usize,
    admitted: A,
}

/// One whole payload, and what `admit` answered for it. Borrowed where it lay
/// whole in the bytes fed.
pub type Frame<'s, A> = (Cow<'s, [u8]>, A);

impl<A> Default for Deframer<A> {
    fn default() -> Self {
        Deframer {
            hdr: [0; FRAME_LEN_PREFIX_BYTES],
            hdr_len: 0,
            pending: None,
            lent: None,
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
    pub fn feed<'s, E: From<FrameLenError>>(
        &mut self,
        src: &mut &'s [u8],
        mut admit: impl FnMut(usize) -> Result<A, E>,
    ) -> Result<Option<Frame<'s, A>>, E> {
        self.lent = None;
        loop {
            if let Some(p) = self.pending.take_if(|p| p.pos == p.buf.len()) {
                // SAFETY: every byte below `pos` was written by `feed`'s own copy or
                // vouched for through `landed`, and `pos` is the length.
                return Ok(Some((
                    Cow::Owned(unsafe { p.buf.assume_init() }.into_vec()),
                    p.admitted,
                )));
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
                // A payload wholly in `src` is handed out in place.
                if let Some((payload, rest)) = src.split_at_checked(len) {
                    *src = rest;
                    return Ok(Some((Cow::Borrowed(payload), admitted)));
                }
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

    /// Where the next read lands: the payload in progress when at least a
    /// carry of it is still to come, else `carry`.
    pub fn window<'a>(&'a mut self, carry: &'a mut [MaybeUninit<u8>]) -> &'a mut [MaybeUninit<u8>] {
        let min = carry.len();
        let tail = self.pending.as_mut().map(|p| &mut p.buf[p.pos..]);
        match tail.filter(|t| t.len() >= min) {
            Some(tail) => {
                self.lent = Some(Lent::Payload);
                tail
            }
            None => {
                self.lent = Some(Lent::Carry);
                carry
            }
        }
    }

    /// The bytes a read of `n` into the last [`Self::window`] leaves to be fed:
    /// the carried ones, or none where the read went into the payload.
    ///
    /// # Safety
    /// The read initialised `n` bytes at the head of that window.
    pub unsafe fn landed<'a>(&mut self, carry: &'a [MaybeUninit<u8>], n: usize) -> &'a [u8] {
        match self.lent.take().expect("a read landed with no window out") {
            Lent::Payload => {
                let p = self.pending.as_mut().expect("a window in the payload");
                assert!(n <= p.buf.len() - p.pos, "a read past its window");
                p.pos += n;
                &[]
            }
            // SAFETY: the window was the carry.
            Lent::Carry => unsafe { carry[..n].assume_init_ref() },
        }
    }
}

#[cfg(test)]
#[path = "tests/deframe.rs"]
mod tests;
