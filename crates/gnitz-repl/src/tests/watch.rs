use super::*;

#[test]
fn a_bare_name_watches_the_view_whole() {
    assert_eq!(watch_query("hot"), "SELECT * FROM hot");
    assert_eq!(watch_query("sales.hot;"), "SELECT * FROM sales.hot");
    assert_eq!(
        watch_query("SELECT kind FROM hot WHERE n > 1;"),
        "SELECT kind FROM hot WHERE n > 1"
    );
}
