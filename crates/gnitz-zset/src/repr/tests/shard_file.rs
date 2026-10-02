use super::*;
use crate::repr::{BatchBuilder, MappedShard};
use crate::schema::{SchemaColumn, TypeCode};
use crate::test_support::{make_batch, make_schema_pk_u64_payload_string, make_schema_u64_i64, make_string_batch};

/// Any change to the written bytes, the writer's own or a dependency's, needs a
/// `SHARD_EPOCH` bump.
#[test]
fn shard_bytes_are_pinned() {
    const PINNED: (u64, u64) = (24, 11567886519790207916);
    let int = SchemaColumn::new(TypeCode::I64, false);
    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            int,
            int,
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    );
    let mut b = BatchBuilder::new(&schema);
    for i in 0..64i64 {
        b.begin_row((i * 3 + 1) as u128, 1 + i % 2);
        // A narrow range packs as FoR, a full-range one stays Raw.
        b.put_int((1_000_000 + i * 3) as u128);
        b.put_int((i.wrapping_mul(0x0123_4567_89AB_CDEF) ^ (i << 60)) as u128);
        // Five values, inline and on the heap, make a dictionary.
        b.put_string(&"shard".repeat(1 + (i % 5) as usize));
        b.end_row();
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("golden.db");
    b.finish()
        .write_as_shard(
            path.to_str().unwrap(),
            ShardWriteOpts { pack_ints: true, ..Default::default() },
        )
        .unwrap();
    let mut bytes = std::fs::read(&path).unwrap();

    let encodings: Vec<Encoding> = (0..=gnitz_wire::num_regions(3))
        .map(|i| region_dir(&bytes, i).1)
        .collect();
    use Encoding::*;
    assert_eq!(
        encodings,
        [Raw, TwoValue, Constant, For, Raw, Dict, Raw, Raw],
        "the batch covers every encoding"
    );
    assert!(region_dir(&bytes, gnitz_wire::num_regions(3)).0 > 0, "and a PK filter");

    // Both vary with things other than the writer: the system schema, the path.
    write_u64_le(&mut bytes, OFF_VERSION, 0);
    write_u64_le(&mut bytes, OFF_DESC_CHECKSUM, 0);
    assert_eq!(
        (SHARD_EPOCH, gnitz_wire::checksum(&bytes)),
        PINNED,
        "shard bytes changed: bump SHARD_EPOCH and re-pin"
    );
}

/// A replicated relayout hard-links one shard inode into several stores, so a
/// write at an existing name must leave that file untouched.
#[test]
fn write_refuses_an_existing_path_and_leaves_it_intact() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("taken.db");
    let path = path.to_str().unwrap();
    let schema = make_schema_u64_i64();
    make_batch(&schema, &[(1, 1, 10)])
        .write_as_shard(path, ShardWriteOpts::default())
        .unwrap();
    let before = std::fs::read(path).unwrap();

    let other = make_batch(&schema, &[(2, 1, 20)]);
    assert_eq!(
        other.write_as_shard(path, ShardWriteOpts::default()),
        Err(StorageError::Io(libc::EEXIST))
    );
    assert_eq!(std::fs::read(path).unwrap(), before);
}

/// A shard carries no dead heap bytes: a run whose fold left dead spans behind
/// writes only the spans its rows reference, and every row reads back.
#[test]
fn a_shard_drops_its_runs_dead_heap() {
    use crate::test_support::{make_schema_pk_u64_payload_string, make_string_batch, map_shard, read_strings};
    use gnitz_expr::RowSource;
    let (x, y) = ([b'x'; 20], [b'y'; 20]);
    let a = make_string_batch(&[(1, 1, &x), (2, 1, &y)]);
    let run = a.merged_consolidated(&make_string_batch(&[(1, -1, &x)]), &make_schema_pk_u64_payload_string());
    assert!(run.dead_heap > 0, "premise: the fold left dead bytes");

    let dir = tempfile::tempdir().unwrap();
    let shard = map_shard(&dir.path().join("s.db"), &run, ShardWriteOpts::default());
    assert_eq!(shard.blob().len(), y.len(), "only the referenced span reaches disk");
    assert_eq!(read_strings(&shard.slice_to_owned_batch(0, 1)), [y.to_vec()]);
}

/// The heap of the shard `rows` are written as, and its string region's encoding.
fn written_strings(rows: &[(u64, i64, &[u8])]) -> (usize, Encoding) {
    use gnitz_expr::RowSource;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.db");
    let batch = make_string_batch(rows);
    batch
        .write_as_shard(path.to_str().unwrap(), ShardWriteOpts::default())
        .unwrap();
    let image = std::fs::read(&path).unwrap();
    let shard = MappedShard::open(path.to_str().unwrap(), batch.schema()).unwrap();
    for (row, &(_, _, want)) in rows.iter().enumerate() {
        assert_eq!(gnitz_expr::payload_bytes(&shard, row, 0), want, "row {row}");
    }
    (shard.blob().len(), region_dir(&image, REG_PAYLOAD_START).1)
}

/// A long value several rows hold reaches the heap once, whichever image the
/// column takes.
#[test]
fn a_repeated_long_string_reaches_the_heap_once() {
    let (x, y) = ([b'x'; 20], [b'y'; 33]);
    // Three rows: a dictionary would not be the smaller image.
    let few: Vec<(u64, i64, &[u8])> = vec![(1, 1, &x), (2, 1, &y), (3, 1, &x)];
    assert_eq!(written_strings(&few), (x.len() + y.len(), Encoding::Raw));
    let many: Vec<(u64, i64, &[u8])> = (1..=40)
        .map(|pk| (pk, 1, if pk % 3 == 0 { &y[..] } else { &x[..] }))
        .collect();
    assert_eq!(written_strings(&many), (x.len() + y.len(), Encoding::Dict));
    let one: Vec<(u64, i64, &[u8])> = (1..=40).map(|pk| (pk, 1, &y[..])).collect();
    assert_eq!(written_strings(&one), (y.len(), Encoding::Constant));
}

/// A packed heap holds no dead bytes either: a run whose fold left dead spans
/// behind writes the values its rows still hold.
#[test]
fn a_packed_shard_drops_its_runs_dead_heap() {
    use crate::test_support::map_shard;
    use gnitz_expr::RowSource;
    let (x, y) = ([b'x'; 20], [b'y'; 20]);
    let a = make_string_batch(&[(1, 1, &x), (2, 1, &y), (3, 1, &y), (4, 1, &y)]);
    let run = a.merged_consolidated(&make_string_batch(&[(1, -1, &x)]), &make_schema_pk_u64_payload_string());
    assert!(run.dead_heap > 0, "premise: the fold left dead bytes");

    let dir = tempfile::tempdir().unwrap();
    let shard = map_shard(&dir.path().join("s.db"), &run, ShardWriteOpts::default());
    assert_eq!(shard.blob().len(), y.len());
    assert_eq!(shard.row_count(), 3);
}

/// Past a dictionary's worth of values the column keeps a cell per row, over a
/// heap that still holds each value once.
#[test]
fn more_values_than_a_dictionary_holds_stay_raw_over_a_shared_heap() {
    let values = DICT_MAX_ENTRIES + 1;
    let mut src_heap = Vec::new();
    let cells: Vec<[u8; 16]> = (0..2 * values)
        .map(|i| gnitz_wire::encode_german_string(format!("value-{:012}", i % values).as_bytes(), &mut src_heap))
        .collect();
    let content = |row: usize| gnitz_wire::german_string_content(&cells[row], &src_heap);
    assert!(sample_repeats(cells.len(), content) || cells.len() > SAMPLE_RUN * SAMPLE_RUNS);
    let mut heap = Vec::new();
    let (encoding, image) = pack_string_column(&cells, &src_heap, &mut heap);
    assert_eq!(encoding, Encoding::Raw);
    assert_eq!(heap.len() * 2, src_heap.len(), "each value once");
    let packed = image.as_chunks::<16>().0;
    assert_eq!(packed.len(), cells.len());
    for (i, (cell, src)) in packed.iter().zip(&cells).enumerate() {
        assert_eq!(
            gnitz_wire::german_string_content(cell, &heap),
            gnitz_wire::german_string_content(src, &src_heap),
            "row {i}"
        );
    }
    assert_eq!(packed[..values], packed[values..], "a value's rows share its cell");
}

/// The sample finds a value repeated across the column and one repeated only
/// next to itself, and nothing in a column of distinct values.
#[test]
fn the_sample_sees_spread_and_adjacent_repeats() {
    let n = 4 * SAMPLE_RUN * SAMPLE_RUNS;
    let column = |value: &dyn Fn(usize) -> usize| {
        let mut heap = Vec::new();
        let cells: Vec<[u8; 16]> = (0..n)
            .map(|i| gnitz_wire::encode_german_string(format!("value-{:012}", value(i)).as_bytes(), &mut heap))
            .collect();
        sample_repeats(n, |row| gnitz_wire::german_string_content(&cells[row], &heap))
    };
    assert!(!column(&|i| i), "distinct");
    assert!(column(&|i| i % 1000), "a thousand values, spread");
    assert!(column(&|i| i / 2), "each value in two adjacent rows");
}
