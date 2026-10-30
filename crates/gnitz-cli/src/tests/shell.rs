use super::*;

#[test]
fn text_fits_a_terminal_with_a_row_to_spare_and_no_wider() {
    assert!(fits("a\nb\n", None));
    assert!(fits("a\nb\n", Some((1, 4))));
    assert!(!fits("a\nb\n", Some((1, 3))));
    assert!(!fits("ab\n", Some((1, 10))));
    assert!(fits("数\n", Some((2, 10))));
    assert!(!fits("数\n", Some((1, 10))));
}
