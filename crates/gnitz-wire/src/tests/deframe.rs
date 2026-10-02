use super::*;

fn framed(payload: &[u8]) -> Vec<u8> {
    [&frame_len_prefix(payload.len())[..], payload].concat()
}

/// One `feed` over `src`: the payload, borrowed where it was handed out in place.
fn feed_one<'s>(d: &mut Deframer, src: &mut &'s [u8]) -> Option<Cow<'s, [u8]>> {
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
                borrowed.push(matches!(p, Cow::Borrowed(_)));
                got.push(p.into_owned());
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
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.window.0, n) };
        // SAFETY: `n` bytes were just written at the head of the window.
        let mut src = unsafe { self.deframer.landed(&self.carry, n) };
        while let Some(p) = feed_one(&mut self.deframer, &mut src) {
            self.frames.push(p.into_owned());
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

/// One read carrying a whole pipelined run yields every frame in it, in order,
/// and leaves nothing behind.
#[test]
fn one_read_queues_every_frame_it_carries() {
    let mut f = Feeder::new();
    let payloads: Vec<Vec<u8>> = (0..16u8).map(|i| vec![i; 700]).collect();
    let wire: Vec<u8> = payloads.iter().flat_map(|p| framed(p)).collect();

    assert_eq!(f.feed(&wire), 1, "a 16-frame run must cost one read");
    assert_eq!(f.frames, payloads, "every frame, in order");
    assert!(
        !f.deframer.is_mid_frame(),
        "a fully consumed run leaves nothing buffered"
    );
}

/// A frame larger than the carry takes the reads behind its first straight into
/// its own buffer, and the stream parses from the carry again afterwards.
#[test]
fn a_frame_larger_than_the_carry_reads_into_its_own_buffer() {
    let big = vec![0xC3u8; 2 * CARRY_BYTES + 500];
    let small = vec![0x11u8; 300];
    let mut wire = framed(&big);
    wire.extend_from_slice(&framed(&small));

    let mut f = Feeder::new();
    assert_eq!(
        f.feed(&wire),
        3,
        "one carry read, one direct read, one carry read for the tail"
    );
    assert_eq!(f.frames, vec![big, small]);
    assert!(!f.deframer.is_mid_frame());
}

/// A frame straddling the end of a carry read completes on the next read, with
/// whatever follows it — its cut in the prefix, at the payload's start or in
/// the payload.
#[test]
fn a_frame_straddling_a_full_carry_completes_on_the_next_read() {
    const P: usize = FRAME_LEN_PREFIX_BYTES;
    for cut in 1..=P + 1 {
        let payloads = vec![vec![0xA1u8; CARRY_BYTES - P - cut], vec![0xC3u8; 64]];
        let wire: Vec<u8> = payloads.iter().flat_map(|p| framed(p)).collect();
        let mut f = Feeder::new();
        assert_eq!(f.feed(&wire), 2, "cut={cut}");
        assert_eq!(f.frames, payloads, "cut={cut}");
        assert!(!f.deframer.is_mid_frame(), "cut={cut}");
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
