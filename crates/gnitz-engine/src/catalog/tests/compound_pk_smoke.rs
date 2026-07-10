use super::*;
use crate::schema::type_code;

// The routing-symmetry tests (master `compute_worker_indices` vs worker
// `partition_for_pk_bytes`) live next to the code they exercise, in
// `ops::exchange::router`.

#[test]
fn schema_roundtrip_catalog_preserves_pk_order() {
    let cols = vec![
        col_def("payload", type_code::U64),
        col_def("a", type_code::U64),
        col_def("b", type_code::U64),
    ];
    let dir = temp_dir("cpk_pk_order_roundtrip");

    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        engine.create_table("public.cpk_order", &cols, &[2, 1], true).unwrap();
        engine.close();
    }
    {
        let mut engine = CatalogEngine::open(&dir).unwrap();
        let tid = engine.get_by_name("public", "cpk_order").unwrap();
        let schema = engine.get_schema(tid).unwrap();
        assert_eq!(
            schema.pk_indices(),
            &[2, 1],
            "PK order (b, a) was not preserved across catalog restart",
        );
        engine.close();
    }

    let _ = fs::remove_dir_all(&dir);
}
