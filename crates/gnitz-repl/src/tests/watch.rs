use super::*;

#[test]
fn a_relation_name_alone_watches_the_view_whole() {
    assert_eq!(watch_query("hot", "public"), "SELECT * FROM hot");
    assert_eq!(watch_query("sales.hot;", "public"), "SELECT * FROM sales.hot");
    assert_eq!(watch_query("\"hot\"", "public"), "SELECT * FROM \"hot\"");
    assert_eq!(watch_query("hot oops", "public"), "hot oops");
    assert_eq!(
        watch_query("SELECT kind FROM hot WHERE n > 1;", "public"),
        "SELECT kind FROM hot WHERE n > 1"
    );
}
