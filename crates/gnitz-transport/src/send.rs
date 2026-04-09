/// Double-buffered send queue for response coalescing.
///
/// `front` is referenced by the in-flight SEND SQE — never modified while
/// `inflight`. `back` is the staging buffer that freely grows via
/// `extend_from_slice`. When `front` is fully sent, swap `front ↔ back`
/// and submit a new SEND from `front`.
pub struct SendQueue {
    front: Vec<u8>,
    back: Vec<u8>,
    front_pos: usize,
    pub(crate) inflight: bool,
}

impl SendQueue {
    pub fn new() -> Self {
        SendQueue {
            front: Vec::new(),
            back: Vec::new(),
            front_pos: 0,
            inflight: false,
        }
    }

    pub fn is_inflight(&self) -> bool {
        self.inflight
    }

    /// Mark an in-flight send as cancelled (error path).
    pub fn cancel_inflight(&mut self) {
        self.inflight = false;
    }

    /// Append a framed response (4-byte header + payload) to the staging buffer.
    pub fn push(&mut self, header: &[u8; 4], payload: &[u8]) {
        self.back.reserve(4 + payload.len());
        self.back.extend_from_slice(header);
        self.back.extend_from_slice(payload);
    }

    /// Reserve space for a framed message (4-byte LE header + payload) in the
    /// staging buffer and invoke `f` to write the payload in-place.
    ///
    /// `f` receives a zeroed `&mut [u8]` of exactly `payload_len` bytes and
    /// must return `payload_len` (the number of bytes written).
    pub fn encode_into<F>(&mut self, payload_len: usize, f: F)
    where
        F: FnOnce(&mut [u8]) -> usize,
    {
        let start = self.back.len();
        let total = 4 + payload_len;
        self.back.resize(start + total, 0);
        self.back[start..start + 4]
            .copy_from_slice(&(payload_len as u32).to_le_bytes());
        let written = f(&mut self.back[start + 4..start + total]);
        debug_assert_eq!(written, payload_len);
    }

    /// Yield `(ptr, len)` for one SEND SQE, or `None` if nothing to send
    /// or a send is already in-flight.
    pub fn prepare(&mut self) -> Option<(*const u8, usize)> {
        if self.inflight {
            return None;
        }

        if self.front_pos == self.front.len() {
            if self.back.is_empty() {
                return None;
            }
            self.front.clear();
            self.front_pos = 0;
            std::mem::swap(&mut self.front, &mut self.back);
        }

        let remaining = &self.front[self.front_pos..];
        self.inflight = true;
        Some((remaining.as_ptr(), remaining.len()))
    }

    /// Handle SEND CQE — advance position by bytes acknowledged.
    pub fn complete(&mut self, bytes_sent: usize) {
        self.front_pos += bytes_sent;
        self.inflight = false;
    }

    /// Returns true if there are unsent bytes in front or staged bytes in back.
    pub fn has_pending(&self) -> bool {
        self.front_pos < self.front.len() || !self.back.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_single_push_prepare_complete() {
        let mut sq = SendQueue::new();
        assert!(!sq.has_pending());
        assert!(sq.prepare().is_none());

        sq.push(&[4, 0, 0, 0], b"test");
        assert!(sq.has_pending());

        let (ptr, len) = sq.prepare().unwrap();
        assert_eq!(len, 8); // 4-byte header + 4-byte payload
        assert!(sq.inflight);

        // Cannot prepare again while inflight
        assert!(sq.prepare().is_none());

        // Complete the full send
        sq.complete(len);
        assert!(!sq.inflight);
        assert!(!sq.has_pending());

        let _ = ptr;
    }

    #[test]
    fn test_coalescing_multiple_pushes() {
        let mut sq = SendQueue::new();
        sq.push(&[2, 0, 0, 0], b"ab");
        sq.push(&[3, 0, 0, 0], b"cde");

        let (_, len) = sq.prepare().unwrap();
        // Both messages coalesced: (4+2) + (4+3) = 13
        assert_eq!(len, 13);
        sq.complete(len);
        assert!(!sq.has_pending());
    }

    #[test]
    fn test_partial_write_recovery() {
        let mut sq = SendQueue::new();
        sq.push(&[4, 0, 0, 0], b"abcd");

        let (_, len) = sq.prepare().unwrap();
        assert_eq!(len, 8);

        // Partial write: kernel only sent 3 bytes
        sq.complete(3);
        assert!(sq.has_pending());

        // Prepare again — should send remaining 5 bytes
        let (_, len) = sq.prepare().unwrap();
        assert_eq!(len, 5);
        sq.complete(5);
        assert!(!sq.has_pending());
    }

    #[test]
    fn test_double_buffer_swap() {
        let mut sq = SendQueue::new();
        sq.push(&[1, 0, 0, 0], b"a");

        // Prepare moves back→front
        let (_, len) = sq.prepare().unwrap();
        assert_eq!(len, 5);

        // Push while front is inflight — goes to back
        sq.push(&[1, 0, 0, 0], b"b");
        assert!(sq.has_pending());

        // Complete front
        sq.complete(len);
        // front is consumed, back has data
        assert!(sq.has_pending());

        // Next prepare swaps back→front
        let (_, len) = sq.prepare().unwrap();
        assert_eq!(len, 5);
        sq.complete(len);
        assert!(!sq.has_pending());
    }

    #[test]
    fn test_push_while_inflight_goes_to_back() {
        let mut sq = SendQueue::new();
        sq.push(&[2, 0, 0, 0], b"aa");
        let (ptr1, len1) = sq.prepare().unwrap();

        // Push during inflight — back buffer
        sq.push(&[2, 0, 0, 0], b"bb");

        // Complete first send
        sq.complete(len1);

        // Second prepare gets the back-buffer data
        let (ptr2, len2) = sq.prepare().unwrap();
        assert_eq!(len2, 6);
        // ptr2 should be different from ptr1 (swapped buffers)
        assert_ne!(ptr1, ptr2);
        sq.complete(len2);
        assert!(!sq.has_pending());
    }

    #[test]
    fn test_empty_prepare_returns_none() {
        let mut sq = SendQueue::new();
        assert!(sq.prepare().is_none());
    }

    #[test]
    fn test_encode_into_basic() {
        let mut sq = SendQueue::new();
        sq.encode_into(5, |buf| {
            buf.copy_from_slice(b"hello");
            5
        });
        assert!(sq.has_pending());

        let (ptr, len) = sq.prepare().unwrap();
        assert_eq!(len, 9); // 4-byte header + 5-byte payload
        let data = unsafe { std::slice::from_raw_parts(ptr, len) };
        assert_eq!(&data[..4], &5u32.to_le_bytes());
        assert_eq!(&data[4..], b"hello");
        sq.complete(len);
        assert!(!sq.has_pending());
    }

    #[test]
    fn test_encode_into_coalesces_with_push() {
        let mut sq = SendQueue::new();
        sq.push(&[2, 0, 0, 0], b"ab");
        sq.encode_into(3, |buf| {
            buf.copy_from_slice(b"cde");
            3
        });

        let (ptr, len) = sq.prepare().unwrap();
        // (4+2) + (4+3) = 13
        assert_eq!(len, 13);
        let data = unsafe { std::slice::from_raw_parts(ptr, len) };
        // First message
        assert_eq!(&data[..4], &2u32.to_le_bytes());
        assert_eq!(&data[4..6], b"ab");
        // Second message
        assert_eq!(&data[6..10], &3u32.to_le_bytes());
        assert_eq!(&data[10..], b"cde");
        sq.complete(len);
        assert!(!sq.has_pending());
    }
}
