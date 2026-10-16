use super::*;

#[test]
fn each_train_is_its_own_segment() {
    assert_eq!(segments(&[]), []);
    assert_eq!(segments(&[3, 3, 7]), [(0, 3), (3, 3), (3, 7)]);
    assert_eq!(segments(&[0, 0]), [(0, 0), (0, 0)]);
}
