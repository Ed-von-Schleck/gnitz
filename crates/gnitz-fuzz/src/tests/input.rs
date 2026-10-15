use super::*;

/// Any bytes decode, and an input encodes to bytes that decode to it.
#[test]
fn any_bytes_are_an_input_and_inputs_round_trip() {
    for bytes in [&[][..], &[0xFF], &[1, 2, 3, 4, 5], &[0x1F; 40]] {
        let input = Input::decode(bytes);
        assert_eq!(Input::decode(&input.encode()), input);
    }
    let input = Input {
        boot: Boot {
            workers: 2,
            tls: true,
            crash: true,
            checkpoints: false,
        },
        seed: 513,
        edits: vec![Edit::SwapFrame { frame: 1 }],
    };
    assert_eq!(Input::decode(&input.encode()), input);
    assert_eq!(input.boot.workers(), 4);
}
