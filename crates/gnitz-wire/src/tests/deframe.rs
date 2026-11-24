use super::*;

/// One `feed` over `src`: the payload, borrowed where it was handed out in place.
fn feed_one<'s>(d: &mut Deframer, src: &mut &'s [u8]) -> Option<FrameBytes<'s>> {
    d.feed(src, |_| Ok::<_, FrameLenError>(()))
        .expect("no refusal")
        .map(|(b, ())| b)
}

/// A pipelined run split at every byte offset yields each frame exactly once,
/// in order, borrowed iff its payload lay whole in the half that completed its
/// prefix.
#[test]
fn a_pipelined_run_split_anywhere_yields_each_frame_once() {
    let payloads: Vec<Vec<u8>> = (1..=3u8).map(|i| vec![i; i as usize * 7]).collect();
    let wire: Vec<u8> = payloads.iter().flat_map(|p| framed(p)).collect();
    let boundaries: Vec<usize> = payloads
        .iter()
        .scan(0, |end, p| {
            *end += FRAME_LEN_PREFIX_BYTES + p.len();
            Some(*end)
        })
        .collect();
    for split in 0..=wire.len() {
        let mut d = Deframer::default();
        let mut got = Vec::new();
        let mut borrowed = Vec::new();
        for (i, half) in [&wire[..split], &wire[split..]].into_iter().enumerate() {
            let mut src = half;
            while let Some(p) = feed_one(&mut d, &mut src) {
                borrowed.push(matches!(p, FrameBytes::Borrowed(_)));
                got.push(p.to_vec());
            }
            assert!(src.is_empty(), "split={split}: each half is consumed whole");
            if i == 0 {
                let on_boundary = split == 0 || boundaries.contains(&split);
                assert_eq!(d.is_mid_frame(), !on_boundary, "split={split}");
            }
        }
        assert_eq!(got, payloads, "split={split}");
        // Whole in the half that completed its prefix: the split falls neither
        // between prefix and payload nor inside the payload.
        let whole: Vec<bool> = payloads
            .iter()
            .zip(&boundaries)
            .map(|(p, &end)| split < end - p.len() || split >= end)
            .collect();
        assert_eq!(borrowed, whole, "split={split}");
        assert!(!d.is_mid_frame(), "split={split}: nothing left buffered");
    }
}

/// A zero prefix and a prefix over the ceiling are refused before `admit` is
/// asked, so before any allocation, and leave nothing buffered; a prefix of
/// exactly the ceiling is admitted once and opens a payload.
#[test]
fn a_prefix_is_bounded_before_allocating() {
    let open = |len: usize| {
        let mut d = Deframer::default();
        let mut admits = 0;
        let prefix = (len as u32).to_le_bytes();
        let got = d
            .feed(&mut &prefix[..], |_| {
                admits += 1;
                Ok::<_, FrameLenError>(())
            })
            .map(|p| p.is_some());
        (got, admits, d.is_mid_frame())
    };
    assert_eq!(open(0), (Err(FrameLenError::Zero), 0, false));
    assert_eq!(
        open(MAX_FRAME_PAYLOAD + 1),
        (Err(FrameLenError::Oversize { len: MAX_FRAME_PAYLOAD + 1 }), 0, false)
    );
    assert_eq!(open(MAX_FRAME_PAYLOAD), (Ok(false), 1, true));
}

const CARRY_BYTES: usize = 32 * 1024;

/// A `Deframer` beside its carry, and the window it last handed out, driven one
/// simulated read at a time.
struct Feeder {
    deframer: Deframer,
    carry: Box<[MaybeUninit<u8>]>,
    window: (*mut u8, usize),
    frames: Vec<Vec<u8>>,
}

impl Feeder {
    fn new() -> Feeder {
        let mut f = Feeder {
            deframer: Deframer::default(),
            carry: Box::new_uninit_slice(CARRY_BYTES),
            window: (std::ptr::null_mut(), 0),
            frames: Vec::new(),
        };
        f.take_window();
        f
    }

    /// Keep the next window's address and length, as an armed read does.
    fn take_window(&mut self) {
        let w = self.deframer.window(&mut self.carry);
        self.window = (w.as_mut_ptr().cast(), w.len());
    }

    /// One read: hand the window as many of `bytes` as it takes. Returns how many.
    fn read(&mut self, bytes: &[u8]) -> usize {
        assert!(self.window.1 > 0, "a window is never zero-length");
        let n = bytes.len().min(self.window.1);
        // SAFETY: the window is `self.window.1 >= n` writable bytes the
        // deframer handed out and nothing else holds.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.window.0, n) };
        // SAFETY: `n` bytes were just written at the head of the window.
        let mut src = unsafe { self.deframer.landed(&self.carry, n) };
        while let Some(p) = feed_one(&mut self.deframer, &mut src) {
            self.frames.push(p.to_vec());
        }
        self.take_window();
        n
    }

    /// Feed `wire` as whole reads until it is exhausted; returns the read count.
    fn feed(&mut self, mut wire: &[u8]) -> usize {
        let mut reads = 0;
        while !wire.is_empty() {
            let n = self.read(wire);
            wire = &wire[n..];
            reads += 1;
        }
        reads
    }
}

/// A run of frames fed as whole reads costs the reads named, yields every frame
/// in order, and leaves nothing buffered.
#[test]
fn a_run_of_frames_costs_the_reads_it_needs() {
    const P: usize = FRAME_LEN_PREFIX_BYTES;
    let mut cases: Vec<(Vec<Vec<u8>>, usize)> = vec![
        // A pipelined run fits one read.
        ((0..16u8).map(|i| vec![i; 700]).collect(), 1),
        // A frame larger than the carry reads straight into its own buffer,
        // and the frame after it parses from the carry again.
        (vec![vec![0xC3; 2 * CARRY_BYTES + 500], vec![0x11; 300]], 3),
        // A payload received into a pooled buffer larger than it: the window
        // ends at its length, never reaching into the next frame.
        (
            vec![vec![0x5A; 3 * crate::frame_buf::POOL_MIN_BYTES / 2], b"next".to_vec()],
            3,
        ),
    ];
    // A frame whose carry read ends in its prefix, at its payload's start or in
    // its payload completes on the next read.
    cases.extend((1..=P + 1).map(|cut| (vec![vec![0xA1; CARRY_BYTES - P - cut], vec![0xC3; 64]], 2)));
    for (payloads, reads) in cases {
        let at = payloads.iter().map(Vec::len).collect::<Vec<_>>();
        let wire: Vec<u8> = payloads.iter().flat_map(|p| framed(p)).collect();
        let mut f = Feeder::new();
        assert_eq!(f.feed(&wire), reads, "{at:?}");
        assert_eq!(f.frames, payloads, "{at:?}");
        assert!(!f.deframer.is_mid_frame(), "{at:?}");
    }
}

/// Opens a payload longer than the carry, so the next window lies in it.
fn mid_large_payload() -> Deframer {
    let mut d = Deframer::default();
    let prefix = frame_len_prefix(4 * CARRY_BYTES);
    assert!(feed_one(&mut d, &mut &prefix[..]).is_none());
    d
}

#[test]
#[should_panic(expected = "no window out")]
fn a_window_lands_once() {
    let mut d = mid_large_payload();
    let mut carry = Box::new_uninit_slice(CARRY_BYTES);
    d.window(&mut carry)[0].write(7);
    // SAFETY: one byte was written at the head of the window.
    unsafe { d.landed(&carry, 1) };
    unsafe { d.landed(&carry, 1) };
}

#[test]
#[should_panic(expected = "no window out")]
fn a_feed_revokes_the_window() {
    let mut d = mid_large_payload();
    let mut carry = Box::new_uninit_slice(CARRY_BYTES);
    d.window(&mut carry)[0].write(7);
    feed_one(&mut d, &mut &[1u8][..]);
    unsafe { d.landed(&carry, 1) };
}

#[test]
#[should_panic(expected = "past its window")]
fn a_read_past_the_payload_window_is_refused() {
    let mut d = mid_large_payload();
    let mut carry = Box::new_uninit_slice(CARRY_BYTES);
    d.window(&mut carry);
    unsafe { d.landed(&carry, 4 * CARRY_BYTES + 1) };
}
