use super::*;

/// Six sources of six payload columns, past the inline sizes of the per-source
/// views, the payload column table and the kept counts: every pick reads back
/// in pick order at its weight, whether its source's heap is carried whole or
/// relocated cell by cell, and a carried heap is charged each row its source
/// left out.
#[test]
fn materialize_carrying_reads_past_the_inline_tables() {
    use crate::repr::BatchBuilder;
    use crate::schema::SchemaColumn;
    use crate::test_support::{u64_pk_schema, weighted_rows};
    use gnitz_wire::TypeCode;
    const SOURCES: u64 = 6;
    const ROWS: u64 = 48;
    let mut cols = vec![SchemaColumn::new(TypeCode::I64, false); 5];
    cols.push(SchemaColumn::new(TypeCode::String, false));
    let schema = u64_pk_schema(&cols);
    assert!(SOURCES as usize * schema.num_payload_cols() > 32);
    let put = |b: &mut BatchBuilder, key: u64, weight: i64| {
        b.begin_row(key as u128, weight);
        for c in 0..5 {
            b.put_int((key * 10 + c) as u128);
        }
        b.put_string(&format!("a string past the inline prefix {key:04}"));
        b.end_row();
    };
    // Source `s` holds the keys `s` mod `SOURCES`.
    let batches: Vec<Batch> = (0..SOURCES)
        .map(|s| {
            let mut b = BatchBuilder::new(&schema);
            for key in (s..ROWS).step_by(SOURCES as usize) {
                put(&mut b, key, 1);
            }
            b.finish()
        })
        .collect();
    let sources: Vec<MemBatch> = batches.iter().map(Batch::as_mem_batch).collect();
    // The last source keeps one row of eight, so its heap is relocated; the
    // others carry theirs, sources 0 and 1 leaving one row out.
    let picked = |key: u64| (key % SOURCES != SOURCES - 1 || key < SOURCES) && key != 6 && key != 7;
    let rows: Vec<Pick> = (0..ROWS)
        .filter(|&key| picked(key))
        .map(|key| ((key % SOURCES) as u32, (key / SOURCES) as u32, 1 + (key % 3) as i64))
        .collect();
    let out = materialize_carrying(&sources, &schema, &rows);
    let mut want = BatchBuilder::new(&schema);
    for key in (0..ROWS).filter(|&key| picked(key)) {
        put(&mut want, key, 1 + (key % 3) as i64);
    }
    assert_eq!(weighted_rows(&out), weighted_rows(&want.finish()));
    let carried: usize = batches[..SOURCES as usize - 1].iter().map(|b| b.blob().len()).sum();
    assert!(out.blob().len() >= carried && out.blob().len() < carried + batches[SOURCES as usize - 1].blob().len());
    assert_eq!(out.dead_heap(), 2 * 36, "the 36-byte strings of keys 6 and 7");
    out.debug_verify_dead_heap();
}
