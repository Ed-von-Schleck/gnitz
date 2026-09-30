use super::*;
use crate::storage::BatchBuilder;
use crate::test_support::{make_batch, make_schema_u64_i64, pk_u64_two_i64_schema};

/// Any change to the written bytes, the writer's own or a dependency's, needs a
/// `SHARD_EPOCH` bump.
#[test]
fn shard_bytes_are_pinned() {
    const PINNED: (u64, u64) = (21, 8604968671891605139);
    let mut b = BatchBuilder::new(pk_u64_two_i64_schema());
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
        .write_as_shard(path.to_str().unwrap(), ShardWriteOpts::COMPACTION)
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
