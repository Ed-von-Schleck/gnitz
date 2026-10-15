use super::*;

/// Any bytes decode, and a seed's unedited input decodes to that seed.
#[test]
fn any_bytes_are_an_input() {
    for bytes in [&[][..], &[0xFF], &[1, 2, 3, 4, 5], &[0x1F; 40]] {
        let input = Input::decode(bytes);
        assert!(WORKERS.contains(&input.boot.workers));
        assert_eq!(input.edits.len(), bytes.len().saturating_sub(HEAD_BYTES) / EDIT_BYTES);
    }
    let input = Input::decode(&Input::unedited(513));
    assert_eq!((input.seed, input.edits.len()), (513, 0));
    assert_eq!(Input::decode(&[0x0E]).boot.workers, 4);
}
