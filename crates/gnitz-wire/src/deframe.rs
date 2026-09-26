//! The deframer both ends of a connection read through: a `u32` LE length prefix,
//! then exactly that many payload bytes.

use std::mem::MaybeUninit;

/// The payload ceiling of every frame, in both directions.
pub const MAX_FRAME_PAYLOAD: usize = 64 << 20;

/// Width of the `u32` LE payload-length prefix in front of every frame. Zero is
/// never a legal length.
pub const FRAME_LEN_PREFIX_BYTES: usize = 4;

/// Why a received length prefix was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameLenError {
    /// Zero: every frame carries a payload, so no sender emits one.
    Zero,
    /// Past [`MAX_FRAME_PAYLOAD`].
    Oversize { len: usize },
}

/// Turns a byte stream into frames, each a `B` the caller allocates at exactly
/// its declared length. A split length prefix is held here, never in the caller.
pub struct Deframer<B> {
    hdr: [u8; FRAME_LEN_PREFIX_BYTES],
    hdr_len: usize,
    /// The payload being filled, and how much of it has arrived.
    pending: Option<(B, usize)>,
}

impl<B: AsMut<[MaybeUninit<u8>]>> Default for Deframer<B> {
    fn default() -> Self {
        Deframer {
            hdr: [0; FRAME_LEN_PREFIX_BYTES],
            hdr_len: 0,
            pending: None,
        }
    }
}

impl<B: AsMut<[MaybeUninit<u8>]>> Deframer<B> {
    /// Mid-frame: a split prefix or a partial payload is buffered.
    pub fn is_mid_frame(&self) -> bool {
        self.hdr_len > 0 || self.pending.is_some()
    }

    /// Consume `src` from the front until one frame is whole and return it, or
    /// `None` once `src` is exhausted. A returned buffer has every byte written.
    pub fn feed<E: From<FrameLenError>>(
        &mut self,
        src: &mut &[u8],
        mut alloc: impl FnMut(usize) -> Result<B, E>,
    ) -> Result<Option<B>, E> {
        loop {
            if let Some((buf, pos)) = &mut self.pending {
                let buf = buf.as_mut();
                if *pos == buf.len() {
                    return Ok(self.pending.take().map(|(b, _)| b));
                }
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
                self.pending = Some((alloc(len)?, 0));
            }
        }
    }

    /// The unfilled tail of the payload in progress, for a reader that reads
    /// straight into it.
    pub fn payload_tail(&mut self) -> Option<&mut [MaybeUninit<u8>]> {
        self.pending.as_mut().map(|(buf, pos)| &mut buf.as_mut()[*pos..])
    }

    /// Record `n` bytes written at the head of [`Self::payload_tail`]. The next
    /// `feed` hands the frame out once it is whole.
    ///
    /// # Safety
    /// Those `n` bytes must be initialised.
    pub unsafe fn filled(&mut self, n: usize) {
        self.pending.as_mut().expect("a payload in progress").1 += n;
    }
}

#[cfg(test)]
#[path = "tests/deframe.rs"]
mod tests;
