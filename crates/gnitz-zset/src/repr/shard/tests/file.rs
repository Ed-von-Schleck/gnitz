use super::*;
use crate::repr::{merge_consolidated, BatchBuilder};
use crate::test_support::{
    make_batch, make_schema_pk_u64_payload_string, make_schema_u64_i64, make_string_batch, map_shard, read_strings,
    string_batch, u64_pk_schema,
};
use gnitz_wire::{write_u64_le, Heap, TypeCode};

/// Any change to the written bytes, the writer's own or a dependency's, needs a
/// `SHARD_EPOCH` bump.
#[test]
fn shard_bytes_are_pinned() {
    const PINNED: (u64, u64) = (30, 5542345507825367142);
    let int = SchemaColumn::new(TypeCode::I64, false);
    let str = SchemaColumn::new(TypeCode::String, false);
    let schema = u64_pk_schema(&[int, int, str, str, SchemaColumn::new(TypeCode::I64, true), int, str]);
    let mut b = BatchBuilder::new(&schema);
    for i in 0..64i64 {
        b.begin_row((i * 3 + 1) as u128, 1 + i % 2);
        // A narrow range packs as FoR, a full-range one stays Raw.
        b.put_int((1_000_000 + i * 3) as u128);
        b.put_int((i.wrapping_mul(0x0123_4567_89AB_CDEF) ^ (i << 60)) as u128);
        // Five values, inline and on the heap, make a dictionary.
        b.put_string(&"shard".repeat(1 + (i % 5) as usize));
        // No value twice, inline and on the heap, is its lengths.
        b.put_string(&format!("{i:0w$}", w = 6 + i as usize % 12));
        // One row in eight holds a value: the others' cells are left out.
        b.put_opt_int((i % 8 == 0).then_some((1 << 40) + i as u128));
        b.put_int(7);
        // Words a symbol table codes: the heap the strings share is coded.
        b.put_string(&format!(
            "row {i} of the golden shard: {}",
            "words a table codes ".repeat(10)
        ));
        b.end_row();
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("golden.db");
    let batch = b.finish();
    let codebook = Codebook::for_batch(None, &batch);
    let opts = ShardWriteOpts {
        codebook: codebook.as_deref(),
        ..Default::default()
    };
    batch.write_as_shard(path.to_str().unwrap(), opts).unwrap();
    let mut bytes = std::fs::read(&path).unwrap();

    let spans = spans_of(&bytes);
    let encodings: Vec<Encoding> = spans.iter().map(|s| s.encoding).collect();
    use Encoding::*;
    assert_eq!(
        encodings,
        [Key, TwoValue, TwoValue, For, Raw, Dict, Seq, Sparse, Constant, Seq, Symbols, Raw],
        "the batch covers every encoding"
    );
    assert!(spans.last().unwrap().size > 0, "and a PK filter");

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

/// The heap of the shard `batch` is written as, and its string region's
/// encoding; every row reads back.
fn written_strings(batch: &Batch) -> (usize, Encoding) {
    use gnitz_wire::RowSource;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.db");
    let shard = map_shard(&path, batch);
    assert_eq!(shard.row_count(), batch.len());
    for row in 0..batch.len() {
        assert_eq!(
            gnitz_wire::payload_bytes(&*shard, row, 0),
            gnitz_wire::payload_bytes(batch, row, 0),
            "row {row}"
        );
    }
    let image = std::fs::read(&path).unwrap();
    (shard.heap().plain_len(), spans_of(&image)[REG_PAYLOAD_START].encoding)
}

/// A shard carries no dead heap bytes: a run whose fold left dead spans behind
/// writes only the spans its rows reference, held by one row or by several.
#[test]
fn a_shard_drops_its_runs_dead_heap() {
    let (x, y) = ([b'x'; 20], [b'y'; 20]);
    for copies in [1, 3] {
        let mut rows = vec![(1, 1, &x[..])];
        rows.extend((2..2 + copies).map(|pk| (pk, 1, &y[..])));
        let retract = make_string_batch(&[(1, -1, &x)]);
        let run = make_string_batch(&rows);
        let run = merge_consolidated(
            &[run.as_mem_batch(), retract.as_mem_batch()],
            &make_schema_pk_u64_payload_string(),
        );
        assert!(run.dead_heap() > 0, "premise: the fold left dead bytes");
        assert_eq!(written_strings(&run), (y.len(), Encoding::Constant), "{copies} copies");
    }
}

/// A long value several rows hold reaches the heap once, whichever image the
/// column takes.
#[test]
fn a_repeated_long_string_reaches_the_heap_once() {
    let (x, y) = ([b'x'; 20], [b'y'; 33]);
    // Three rows: a dictionary would not be the smaller image.
    let few: Vec<(u64, i64, &[u8])> = vec![(1, 1, &x), (2, 1, &y), (3, 1, &x)];
    assert_eq!(
        written_strings(&make_string_batch(&few)),
        (x.len() + y.len(), Encoding::Raw)
    );
    let many: Vec<(u64, i64, &[u8])> = (1..=40)
        .map(|pk| (pk, 1, if pk % 3 == 0 { &y[..] } else { &x[..] }))
        .collect();
    assert_eq!(
        written_strings(&make_string_batch(&many)),
        (x.len() + y.len(), Encoding::Dict)
    );
    let one: Vec<(u64, i64, &[u8])> = (1..=40).map(|pk| (pk, 1, &y[..])).collect();
    assert_eq!(written_strings(&make_string_batch(&one)), (y.len(), Encoding::Constant));
}

/// `2 * values` cells holding each of `values` strings of `width` bytes twice,
/// and their heap.
fn each_value_twice(values: usize, width: usize) -> (Vec<[u8; 16]>, Vec<u8>) {
    let mut heap = Vec::new();
    let cells = (0..2 * values)
        .map(|i| gnitz_wire::encode_german_string(format!("{:0width$}", i % values).as_bytes(), &mut heap))
        .collect();
    (cells, heap)
}

/// Past a dictionary's worth of values a column whose cells cost less than a
/// second copy of each value keeps a cell per row, over a heap that holds each
/// value once.
#[test]
fn more_values_than_a_dictionary_holds_stay_raw_over_a_shared_heap() {
    let values = DICT_MAX_ENTRIES + 1;
    let (cells, src_heap) = each_value_twice(values, 40);
    let mut heap = HeapWriter::plain();
    let (encoding, image) = pack_string_column(&cells, Heap::Plain(&src_heap), &mut heap);
    let heap = heap.finish().1;
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

/// The same column of values a cell outweighs is its lengths, every row's value
/// on the heap.
#[test]
fn more_short_values_than_a_dictionary_holds_are_their_lengths() {
    let (cells, src_heap) = each_value_twice(DICT_MAX_ENTRIES + 1, 20);
    let mut heap = HeapWriter::plain();
    heap.push(Heap::Plain(&[0; 5]).value(0, 5).unwrap());
    let (encoding, image) = pack_string_column(&cells, Heap::Plain(&src_heap), &mut heap);
    let heap = heap.finish().1;
    assert_eq!(encoding, Encoding::Seq);
    assert_eq!(
        heap.len() - 5,
        src_heap.len(),
        "every row's value, and nothing a refused image left"
    );
    let mut packed = vec![0u8; cells.len() * 16];
    SeqImage::parse(&image, cells.len())
        .unwrap()
        .decode(Heap::Plain(&heap), 0, &mut packed);
    for (i, (cell, src)) in packed.as_chunks::<16>().0.iter().zip(&cells).enumerate() {
        assert_eq!(
            gnitz_wire::german_string_content(cell, &heap),
            gnitz_wire::german_string_content(src, &src_heap),
            "row {i}"
        );
    }
}

/// A column of values that never repeat is its lengths once those are the
/// smaller region.
#[test]
fn distinct_strings_are_their_lengths() {
    let distinct = |n: u64| {
        string_batch(
            &(1..=n)
                .map(|pk| (pk, format!("{pk:0w$}", w = 8 + pk as usize % 9)))
                .collect::<Vec<_>>(),
        )
    };
    let long = |b: &Batch| {
        read_strings(b)
            .iter()
            .map(Vec::len)
            .filter(|&len| len > 12)
            .sum::<usize>()
    };
    let (two, forty) = (distinct(2), distinct(40));
    assert_eq!(written_strings(&two), (long(&two), Encoding::Raw));
    assert_eq!(written_strings(&forty), (long(&forty), Encoding::Seq));
    assert_eq!(written_strings(&distinct(1)).1, Encoding::Constant);
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
