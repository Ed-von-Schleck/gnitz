use super::*;
use gnitz_wire::TypeCode;

#[test]
fn schema_roundtrip_catalog_preserves_pk_order() {
    let cols = vec![
        col_def("payload", TypeCode::U64),
        col_def("a", TypeCode::U64),
        col_def("b", TypeCode::U64),
    ];
    let dir = temp_dir("cpk_pk_order_roundtrip");

    {
        let mut engine = CatalogEngine::open(&dir, 1).unwrap();
        engine.create_table("public.cpk_order", &cols, &[2, 1]).unwrap();
        engine.close();
    }
    {
        let engine = CatalogEngine::open(&dir, 1).unwrap();
        let tid = engine.get_by_name("public", "cpk_order").unwrap();
        let schema = engine.registry.relation(tid).map(Relation::schema).unwrap();
        assert_eq!(
            schema.pk_indices(),
            &[2, 1],
            "PK order (b, a) was not preserved across catalog restart",
        );
        engine.close();
    }

    let _ = fs::remove_dir_all(&dir);
}
