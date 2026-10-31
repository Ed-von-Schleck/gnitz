use super::*;

/// Carrying two whole heaps charges each source's rows the output leaves out —
/// survivors interleaved across both — and every kept string reads back.
#[test]
fn materialize_carrying_charges_each_sources_dropped_rows() {
    use crate::test_support::{make_batch_bytes, make_schema_pk_u64_payload_string, read_strings};
    let schema = make_schema_pk_u64_payload_string();
    // Eight rows a side, one dropped from each: too little to make either
    // carried heap wasteful.
    let side = |first: u64, fill: u8, dropped: &'static [u8]| {
        let kept = [fill; 40];
        let mut rows: Vec<(u64, i64, &[u8])> = (0..8).map(|i| (first + 2 * i, 1, &kept[..])).collect();
        rows[3].2 = dropped;
        make_batch_bytes(&schema, &rows)
    };
    let (a, b) = (side(1, b'a', &[b'x'; 30]), side(2, b'b', &[b'y'; 50]));
    let sources = [a.as_mem_batch(), b.as_mem_batch()];
    let rows: Vec<Pick> = (0..8)
        .filter(|&r| r != 3)
        .flat_map(|r| [(0, r, 1), (1, r, 1)])
        .collect();
    let out = materialize_carrying(&sources, &schema, &rows);
    assert_eq!(out.dead_heap(), 30 + 50);
    assert_eq!(out.blob().len(), a.blob().len() + b.blob().len());
    let want: Vec<Vec<u8>> = (0..14).map(|i| vec![[b'a', b'b'][i % 2]; 40]).collect();
    assert_eq!(read_strings(&out), want);
}

/// [`gather_rows`] reads the same rows whichever way its sources hold their
/// regions — in place through one `ColPtr` per source, or through each row
/// accessor — each at its pick's weight, a column a file predates read NULL.
#[test]
fn gather_rows_reads_every_backing_alike() {
    use crate::repr::{BatchBuilder, MappedShard, Run};
    use crate::schema::SchemaColumn;
    use crate::test_support::{map_shard, u64_pk_schema, weighted_rows};
    use gnitz_wire::TypeCode;
    use std::rc::Rc;
    const SOURCES: usize = 3;
    const ROWS: usize = 300;
    let col = SchemaColumn::new;
    let narrow = u64_pk_schema(&[col(TypeCode::I64, false), col(TypeCode::String, false)]);
    let wide = u64_pk_schema(&[
        col(TypeCode::I64, false),
        col(TypeCode::String, false),
        col(TypeCode::I64, true),
    ]);
    // Source `s` holds the keys `s` mod `SOURCES`. `spread` values no encoding
    // shrinks, so a shard stores them as they stand; the rest pack.
    let source = |schema: &SchemaDescriptor, s: usize, spread: bool| {
        let mut b = BatchBuilder::new(schema);
        for key in (s..ROWS).step_by(SOURCES) {
            b.begin_row(key as u128, 1);
            b.put_int(match spread {
                true => (key as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) as u128,
                false => key as u128,
            });
            b.put_string(&format!("gathered-string-{key:08}"));
            if schema.num_payload_cols() == 3 {
                b.put_null();
            }
            b.end_row();
        }
        let mut b = b.finish();
        b.certify_consolidated();
        b
    };
    let dir = tempfile::tempdir().unwrap();
    let shard = |name: &str, b: &Batch| map_shard(&dir.path().join(format!("{name}.db")), b);
    let picks: Vec<Pick> = (0..ROWS)
        .map(|key| ((key % SOURCES) as u32, (key / SOURCES) as u32, 2))
        .collect();

    for spread in [true, false] {
        let mem: Vec<Run> = (0..SOURCES)
            .map(|s| Run::Mem(Rc::new(source(&wide, s, spread))))
            .collect();
        // One pick at a time is too few to read in place: each goes through
        // the row accessors.
        let want: Vec<_> = picks
            .chunks(1)
            .flat_map(|pick| weighted_rows(&gather_rows(&mem, &wide, pick)))
            .collect();
        assert!(want.iter().all(|&(_, w)| w == 2));
        let shards: Vec<Rc<MappedShard>> = (0..SOURCES)
            .map(|s| shard(&format!("wide_{spread}_{s}"), &source(&wide, s, spread)))
            .collect();
        let windows: Vec<_> = shards.iter().map(|s| s.window(0..s.row_count())).collect();
        let narrowed: Vec<Run> = (0..SOURCES)
            .map(|s| shard(&format!("narrow_{spread}_{s}"), &source(&narrow, s, spread)))
            .map(|s| Run::Shard(Rc::new(s.rebind(&wide).unwrap())))
            .collect();
        let mut mixed: Vec<Run> = shards.iter().cloned().map(Run::Shard).collect();
        mixed[1] = mem[1].clone();
        // What each set reads in place.
        assert!(mem
            .iter()
            .all(|m| m.direct_fixed().is_some() && m.direct_col(0).is_some()));
        assert!(windows
            .iter()
            .all(|w| w.direct_fixed().is_some() && w.direct_col(2).is_some()));
        assert!(windows.iter().all(|w| w.direct_col(0).is_some() == spread));
        assert!(narrowed
            .iter()
            .all(|n| n.direct_fixed().is_none() && n.direct_col(2).is_some()));
        let got = [
            gather_rows(&mem, &wide, &picks),
            gather_rows(&windows, &wide, &picks),
            gather_rows(&narrowed, &wide, &picks),
            gather_rows(&mixed, &wide, &picks),
        ];
        for (i, got) in got.iter().enumerate() {
            assert_eq!(weighted_rows(got), want, "spread={spread}, backing {i}");
        }
    }
}
