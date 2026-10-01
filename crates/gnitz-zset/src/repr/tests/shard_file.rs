use super::*;
use crate::repr::BatchBuilder;
use crate::test_support::{make_batch, make_schema_u64_i64, pk_u64_two_i64_schema};

/// Any change to the written bytes, the writer's own or a dependency's, needs a
/// `SHARD_EPOCH` bump.
#[test]
fn shard_bytes_are_pinned() {
    const PINNED: (u64, u64) = (21, 8604968671891605139);
    let mut b = BatchBuilder::new(&pk_u64_two_i64_schema());
    for i in 0..64i64 {
        b.begin_row((i * 3 + 1) as u128, 1 + i % 2);
        // A narrow range packs as FoR, a full-range one stays Raw.
        b.put_int((1_000_000 + i * 3) as u128);
        b.put_int((i.wrapping_mul(0x0123_4567_89AB_CDEF) ^ (i << 60)) as u128);
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

    let encodings: Vec<Encoding> = (0..=gnitz_wire::num_regions(2))
        .map(|i| region_dir(&bytes, i).1)
        .collect();
    use Encoding::*;
    assert_eq!(
        encodings,
        [Raw, TwoValue, Constant, For, Raw, Raw, Raw],
        "the batch covers every encoding"
    );
    assert!(region_dir(&bytes, gnitz_wire::num_regions(2)).0 > 0, "and a PK filter");

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
