use super::super::batch::{Batch, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::super::batch_builder::BatchBuilder;
use super::super::layout::*;
use super::super::merge::ColumnarSource;
use super::super::scatter::UnifiedSet;
use super::super::shard_file::{region_dir, write_i64_shard, ShardWriteOpts};
use super::*;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::storage::error::StorageError;
use crate::test_support::{make_batch, make_schema_pk_u64_payload_string, make_schema_u64_i64, read_german_string};
use gnitz_expr::RowSource;
use gnitz_wire::num_regions;
use gnitz_wire::{read_i64_le, write_u64_le};

/// Write `batch` through the production writer to `dir/name`; `pack` admits FoR
/// on its integer payload columns.
fn write(dir: &std::path::Path, name: &str, batch: &Batch, pack: bool) -> String {
    let path = dir.join(name).to_str().unwrap().to_owned();
    batch
        .write_as_shard(&path, ShardWriteOpts { pack_ints: pack, ..Default::default() })
        .unwrap();
    path
}

/// A `(U64 PK | I64)` shard of `rows` at weight 1 and payload `pk * mul`, at
/// `dir/test.db` — the fixture the integrity tests patch.
fn small_shard(dir: &std::path::Path, rows: u64, mul: i64) -> String {
    let rows: Vec<(u64, i64, i64)> = (1..=rows).map(|i| (i, 1, i as i64 * mul)).collect();
    write(dir, "test.db", &make_batch(&make_schema_u64_i64(), &rows), false)
}

/// Patch a copy of `base` — the image already written at `path` — write it
/// back to `path`, and open it. The descriptive digest is left stale, so a
/// patch inside the prefix is rejected by the digest. Writing back to the
/// same path keeps the basename, and with it the digest's seed, unchanged;
/// under a fresh name every case would fail on the name alone.
fn open_patched(
    path: &str,
    schema: &SchemaDescriptor,
    base: &[u8],
    patch: impl FnOnce(&mut Vec<u8>),
) -> Result<MappedShard, StorageError> {
    let mut data = base.to_vec();
    patch(&mut data);
    std::fs::write(path, &data).unwrap();
    MappedShard::open(path, schema)
}

/// As [`open_patched`], with the descriptive digest re-stamped over `schema`'s
/// prefix, so the forgery reaches the check under test.
fn open_patched_restamped(
    path: &str,
    schema: &SchemaDescriptor,
    base: &[u8],
    patch: impl FnOnce(&mut Vec<u8>),
) -> Result<MappedShard, StorageError> {
    open_patched(path, schema, base, |data| {
        patch(data);
        let cs = desc_digest(path, &data[..desc_len(schema.num_payload_cols())]);
        write_u64_le(data, OFF_DESC_CHECKSUM, cs);
    })
}

/// Rewrite directory entry `i` of `data` through `patch`.
fn patch_entry(data: &mut [u8], i: usize, patch: impl FnOnce(&mut DirEntry)) {
    let mut e = DirEntry::read(data, i);
    patch(&mut e);
    e.write(data, i);
}

/// Every region of `image` placed in it.
fn spans_of(image: &[u8]) -> Vec<Span> {
    region_spans(image, ShardHeader::read(image).unwrap().file_npc).unwrap()
}

/// `(U64 PK, I64, <tail>)` where `tail` is nullable — the shape an
/// `ADD COLUMN` leaves behind. `make_schema_u64_i64` is its narrow twin.
fn schema_with_appended(tail: TypeCode) -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(tail, true),
        ],
        &[0],
    )
}

/// A `width`-byte string unique to row `i`, long enough to spill to the heap.
fn wide_string(i: usize, width: usize) -> String {
    let mut s = format!("row-{i:08}-");
    s.extend(std::iter::repeat_n('x', width.saturating_sub(s.len())));
    s
}

// -----------------------------------------------------------------------
// The differential oracle
// -----------------------------------------------------------------------

/// One row as every read surface must report it: PK bytes, weight, null word,
/// and each payload cell under `schema` (a string cell as its content).
type RowImage = (Vec<u8>, i64, u64, Vec<Vec<u8>>);

fn rows_of<S: RowSource>(src: &S, weight: impl Fn(usize) -> i64, schema: &SchemaDescriptor) -> Vec<RowImage> {
    (0..src.row_count())
        .map(|r| {
            let cells = schema
                .payload_columns()
                .map(|(pi, col)| {
                    if col.type_code.is_german_string() {
                        gnitz_expr::payload_bytes(src, r, pi).to_vec()
                    } else {
                        src.get_col_ptr(r, pi, col.size() as usize).to_vec()
                    }
                })
                .collect();
            (src.get_pk_bytes(r).to_vec(), weight(r), src.get_null_word(r), cells)
        })
        .collect()
}

/// `shard` reads as `want` — the written batch widened to the reader's schema —
/// through every surface: the per-row accessors, a slice on both blob arms, a
/// `UnifiedSet` over a window, and the OPK seeks.
fn assert_reads_as(label: &str, shard: &MappedShard, want: &Batch) {
    let schema = want.schema();
    let n = want.count;
    assert_eq!(shard.row_count(), n, "{label}: row count");
    let want_rows = rows_of(want, |r| want.get_weight(r), schema);
    assert_eq!(
        rows_of(shard, |r| shard.get_weight(r), schema),
        want_rows,
        "{label}: per-row"
    );

    // An odd-length window starting mid-shard.
    let mid_start = n / 3;
    let mid = mid_start..mid_start + (((n - mid_start) / 2) | 1).min(n - mid_start);
    for relocate in [false, true] {
        for w in [0..n, 0..0, mid.clone()] {
            let b = shard.slice_to_owned_batch_with(w.start, w.len(), relocate);
            assert_eq!(
                rows_of(&b, |r| b.get_weight(r), schema),
                want_rows[w.clone()],
                "{label}: slice {w:?} relocate={relocate}"
            );
        }
    }
    for w in [0..n, mid] {
        let rows: Vec<(u32, u32, i64)> = w.clone().map(|r| (0, r as u32, want.get_weight(r))).collect();
        let b = UnifiedSet::of(std::slice::from_ref(shard), schema, std::iter::once(w.clone()))
            .materialize(&rows, rows.len());
        assert_eq!(
            rows_of(&b, |r| b.get_weight(r), schema),
            want_rows[w.clone()],
            "{label}: materialize {w:?}"
        );
    }
    for r in 0..n {
        let key = want.get_pk_bytes(r);
        let lb = want.find_lower_bound_bytes(key);
        assert_eq!(shard.find_lower_bound_bytes(key), lb, "{label}: lower bound row {r}");
        for hint in [0, lb] {
            assert_eq!(
                shard.advance_to(key, hint),
                want.advance_to(key, hint),
                "{label}: advance row {r}"
            );
        }
    }
}

/// A shape: its label, the batch written, the schema it is read under, whether
/// the writer may pack, and the encodings its directory must carry.
struct Shape {
    label: &'static str,
    written: Batch,
    reader: SchemaDescriptor,
    pack: bool,
    encodings: Vec<(usize, Encoding)>,
}

/// A batch over `schema` of `n` rows, row `i` begun and filled by `row`.
fn build(schema: SchemaDescriptor, n: usize, mut row: impl FnMut(&mut BatchBuilder, usize)) -> Batch {
    let mut b = BatchBuilder::new(schema);
    for i in 0..n {
        row(&mut b, i);
        b.end_row();
    }
    b.finish()
}

fn shapes() -> Vec<Shape> {
    use Encoding::{Constant, For, Raw, TwoValue};
    let u64_i64 = make_schema_u64_i64();
    let nullable_i64 = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let u64_i32 = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I32, false),
        ],
        &[0],
    );
    let u128_i64 = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U128, false),
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let all_pk = crate::test_support::pk_only_schema(&[TypeCode::U64; 3]);
    let string = make_schema_pk_u64_payload_string();
    let for_i64 = |b: &mut BatchBuilder, i: usize| {
        b.begin_row(i as u128, 1);
        b.put_int((1_000_000 + (i % 300) as i64) as u128);
    };
    let narrow = build(u64_i64, 40, |b, i| {
        b.begin_row(i as u128 + 1, 1);
        b.put_int((i as i64 * 7) as u128);
    });
    let (w1, w2) = (narrow.clone(), narrow.clone());
    let wide_key = |i: usize| (u64::MAX as u128 - 2 + i as u128) * 3;

    vec![
        Shape {
            label: "raw weight, nullable raw i64",
            written: build(nullable_i64, 30, |b, i| {
                b.begin_row(i as u128 * 5, [1, -1, 2][i % 3]);
                match i % 4 {
                    0 => b.put_null(),
                    _ => b.put_int((i as i64).wrapping_mul(0x9E37_79B9_7F4A_7C15_u64 as i64) as u128),
                }
            }),
            reader: nullable_i64,
            pack: true,
            encodings: vec![
                (REG_PK, Raw),
                (REG_WEIGHT, Raw),
                (REG_NULL_BMP, Raw),
                (REG_PAYLOAD_START, Raw),
            ],
        },
        Shape {
            label: "constant weight, null and payload",
            written: build(u64_i64, 20, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                b.put_int(42);
            }),
            reader: u64_i64,
            pack: false,
            encodings: vec![
                (REG_WEIGHT, Constant),
                (REG_NULL_BMP, Constant),
                (REG_PAYLOAD_START, Constant),
            ],
        },
        Shape {
            label: "two-value weight, for i64",
            written: build(u64_i64, 64, |b, i| {
                b.begin_row(i as u128, if i % 2 == 0 { 1 } else { -1 });
                b.put_int((1_000_000 + (i % 300) as i64) as u128);
            }),
            reader: u64_i64,
            pack: true,
            encodings: vec![(REG_WEIGHT, TwoValue), (REG_PAYLOAD_START, For)],
        },
        Shape {
            label: "for i32",
            written: build(u64_i32, 400, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_int((-70_000i32 + (i % 250) as i32) as i64 as u128);
            }),
            reader: u64_i32,
            pack: true,
            encodings: vec![(REG_PAYLOAD_START, For)],
        },
        Shape {
            label: "for i64 across decode blocks",
            written: build(u64_i64, 2 * DECODE_BLOCK_ROWS + 17, for_i64),
            reader: u64_i64,
            pack: true,
            encodings: vec![(REG_PAYLOAD_START, For)],
        },
        Shape {
            label: "strings, inline and heap",
            written: build(string, 24, |b, i| {
                b.begin_row(i as u128, 1);
                match i % 3 {
                    0 => b.put_string("short"),
                    _ => b.put_string(&wide_string(i, 20 + i)),
                }
            }),
            reader: string,
            pack: true,
            encodings: vec![(REG_PAYLOAD_START, Raw)],
        },
        Shape {
            label: "u128 pk across the u64 boundary",
            written: build(u128_i64, 6, |b, i| {
                b.begin_row(wide_key(i), 1);
                b.put_int(i as u128);
            }),
            reader: u128_i64,
            pack: false,
            encodings: vec![(REG_PK, Raw)],
        },
        Shape {
            label: "3xu64 all-pk",
            written: build(all_pk, 9, |b, i| {
                b.begin_row_opk(&[1, (i / 4) as u128, (i * 7) as u128], 1)
            }),
            reader: all_pk,
            pack: false,
            encodings: vec![(REG_PK, Raw)],
        },
        Shape {
            label: "repeated pk",
            written: build(u64_i64, 12, |b, i| {
                b.begin_row(77, 1);
                b.put_int(i as u128 * 3);
            }),
            reader: u64_i64,
            pack: false,
            encodings: vec![(REG_PK, Constant)],
        },
        Shape {
            label: "widened by i64",
            written: w1,
            reader: schema_with_appended(TypeCode::I64),
            pack: false,
            encodings: vec![(REG_PAYLOAD_START, Raw)],
        },
        Shape {
            label: "widened by string",
            written: w2,
            reader: schema_with_appended(TypeCode::String),
            pack: false,
            encodings: vec![(REG_PAYLOAD_START, Raw)],
        },
    ]
}

#[test]
fn every_shape_reads_back_through_every_surface() {
    let dir = tempfile::tempdir().unwrap();
    for (i, s) in shapes().into_iter().enumerate() {
        let path = write(dir.path(), &format!("shape_{i}.db"), &s.written, s.pack);
        let image = std::fs::read(&path).unwrap();
        for &(region, enc) in &s.encodings {
            assert_eq!(region_dir(&image, region).1, enc, "{}: region {region}", s.label);
        }
        let shard = MappedShard::open(&path, &s.reader).unwrap();
        assert_reads_as(s.label, &shard, &s.written.widened_with_nulls(&s.reader, false));
    }
}

// -----------------------------------------------------------------------
// What a read leaves decoded
// -----------------------------------------------------------------------

/// A `(U64 PK | FoR I64)` shard of three decode blocks, the last partial.
fn packed_three_blocks(dir: &std::path::Path) -> MappedShard {
    let schema = make_schema_u64_i64();
    let batch = build(schema, 2 * DECODE_BLOCK_ROWS + 17, |b, i| {
        b.begin_row(i as u128, 1);
        b.put_int((5_000 + (i % 200) as i64) as u128);
    });
    let shard = MappedShard::open(&write(dir, "blocks.db", &batch, true), &schema).unwrap();
    assert!(matches!(shard.col_regions[0], PayloadRegion::Packed(_)));
    shard
}

#[test]
fn a_point_read_decodes_one_block_and_keeps_its_address() {
    let dir = tempfile::tempdir().unwrap();
    let shard = packed_three_blocks(dir.path());
    let row = DECODE_BLOCK_ROWS + 3;
    let first = shard.get_col_ptr(row, 0, 8);
    assert_eq!(read_i64_le(first, 0), 5_000 + (row % 200) as i64);
    assert_eq!(shard.decoded_blocks(0), 1);
    assert_eq!(shard.get_col_ptr(row, 0, 8).as_ptr(), first.as_ptr());
    assert_eq!(shard.decoded_blocks(0), 1);
}

#[test]
fn a_slice_and_a_materialize_decode_no_block() {
    let dir = tempfile::tempdir().unwrap();
    let shard = packed_three_blocks(dir.path());
    let schema = make_schema_u64_i64();
    let n = shard.row_count();
    std::hint::black_box(shard.slice_to_owned_batch(100, n - 200));
    let rows: Vec<(u32, u32, i64)> = (0..n as u32).map(|r| (0, r, 1)).collect();
    std::hint::black_box(UnifiedSet::whole(std::slice::from_ref(&shard), &schema).materialize(&rows, rows.len()));
    assert_eq!(shard.decoded_blocks(0), 0);
}

// -----------------------------------------------------------------------
// Binding
// -----------------------------------------------------------------------

#[test]
fn rebind_matches_a_fresh_open() {
    let dir = tempfile::tempdir().unwrap();
    let narrow = packed_three_blocks(dir.path());
    let path = dir.path().join("blocks.db");
    let n = narrow.row_count();
    // Decode a block on the narrow handle first.
    std::hint::black_box(narrow.get_col_ptr(0, 0, 8));

    let wide = schema_with_appended(TypeCode::I64);
    let rebound = narrow.rebind(&wide).unwrap();
    let fresh = MappedShard::open(path.to_str().unwrap(), &wide).unwrap();
    assert!(std::rc::Rc::ptr_eq(&narrow.mmap, &rebound.mmap));
    assert_eq!(rebound.decoded_blocks(0), 0, "a rebound handle starts undecoded");
    assert!(matches!(rebound.col_regions[1], PayloadRegion::Mapped(cp) if std::ptr::eq(cp.base, ZERO_CELL.as_ptr())));
    assert_eq!(rebound.null_pad_mask, fresh.null_pad_mask);
    assert!(rebound.schema == wide);
    assert_eq!(
        rows_of(&rebound, |r| rebound.get_weight(r), &wide),
        rows_of(&fresh, |r| fresh.get_weight(r), &wide)
    );
    assert_eq!(rebound.row_count(), n);
}

#[test]
fn shard_wider_than_reader_schema_opens() {
    // Reachable from a correct crash: a checkpoint publishes base manifests
    // before it makes the catalog durable, so a boot can read the catalog
    // back at width N and find a shard written at N+1. Rejecting it would
    // make the database unbootable, so the reader narrows instead.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("wide.db");
    let wide = schema_with_appended(TypeCode::I64);
    let rows: Vec<_> = (1u64..=3)
        .map(|p| (p.to_be_bytes().to_vec(), 1, 0, vec![p as i64 * 10, p as i64 * 11]))
        .collect();
    let shard_path = path.to_str().unwrap().to_owned();
    write_i64_shard(&shard_path, &wide, &rows, &[], ShardWriteOpts::default());

    let narrow = make_schema_u64_i64();
    let shard = MappedShard::open(&shard_path, &narrow).unwrap();
    // Every column the reader's schema names is served; the surplus directory
    // entry is left unmapped and needs no pad.
    assert_eq!(shard.col_regions.len(), 1);
    assert_eq!(shard.null_pad_mask, 0);
    for row in 0..3 {
        assert_eq!(read_i64_le(shard.get_col_ptr(row, 0, 8), 0), (row as i64 + 1) * 10);
    }
    // The blob region came from the *file's* index, not the reader's.
    assert!(shard.blob().is_empty());
}

#[test]
fn forged_file_npc_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = small_shard(dir.path(), 1, 10);
    let schema = make_schema_u64_i64();
    let base = std::fs::read(&path).unwrap();

    for forged in [u64::MAX, gnitz_wire::MAX_COLUMNS as u64 + 1] {
        assert_eq!(
            open_patched(&path, &schema, &base, |d| write_u64_le(d, OFF_FILE_NPC, forged)).err(),
            Some(StorageError::Corrupt("payload arity")),
            "file_npc {forged}",
        );
    }

    // In range but wrong: the digest rejects it.
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |d| write_u64_le(d, OFF_FILE_NPC, 2)).err(),
        Some(StorageError::Corrupt("descriptor digest"))
    );
}

/// An all-PK table widened by `ADD COLUMN` has a skeleton's region shape, and
/// its NULL columns are real: only the flag makes a skeleton.
#[test]
fn a_widened_all_pk_shard_is_not_a_skeleton() {
    let dir = tempfile::tempdir().unwrap();
    let all_pk = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false)], &[0]);
    let widened = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::I64, true),
        ],
        &[0],
    );
    let shard_path = dir.path().join("allpk.db").to_str().unwrap().to_owned();
    write_i64_shard(
        &shard_path,
        &all_pk,
        &[(1u64.to_be_bytes().to_vec(), 1, 0, vec![])],
        &[],
        ShardWriteOpts::default(),
    );
    let shard = MappedShard::open(&shard_path, &widened).unwrap();
    assert!(gnitz_wire::null_word_get(shard.get_null_word(0), 0));
    assert!(!shard.is_skeleton());
}

#[test]
fn a_restamped_skeleton_flag_opens_as_a_skeleton() {
    let dir = tempfile::tempdir().unwrap();
    let path = small_shard(dir.path(), 1, 42);
    let schema = make_schema_u64_i64();
    let base = std::fs::read(&path).unwrap();
    assert!(!MappedShard::open(&path, &schema).unwrap().is_skeleton());
    let forge = |d: &mut Vec<u8>| write_u64_le(d, OFF_FLAGS, SHARD_FLAG_SKELETON);
    assert!(open_patched_restamped(&path, &schema, &base, forge)
        .unwrap()
        .is_skeleton());
}

#[test]
fn a_zero_row_count_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = small_shard(dir.path(), 1, 10);
    let schema = make_schema_u64_i64();
    let base = std::fs::read(&path).unwrap();
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |d| write_u64_le(d, OFF_ROW_COUNT, 0)).err(),
        Some(StorageError::Corrupt("no rows")),
    );
}

/// Each region role admits only certain encodings. Forging one that a role may
/// not carry must be refused at open, whatever the byte means elsewhere. The
/// digest is re-stamped so the verdict is the decode site's, not the digest's.
#[test]
fn an_encoding_a_role_may_not_carry_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = small_shard(dir.path(), 8, 1);
    let schema = make_schema_u64_i64();
    let base = std::fs::read(&path).unwrap();
    let blob = num_regions(schema.num_payload_cols()) - 1;
    let filter = blob + 1;

    // (region, forged encoding byte)
    let cases: &[(usize, u8)] = &[
        (REG_PK, 0x10), // not an encoding at all
        (REG_PK, Encoding::TwoValue as u8),
        (REG_PK, Encoding::For as u8),
        (REG_WEIGHT, Encoding::For as u8),
        (REG_NULL_BMP, Encoding::For as u8),
        (blob, Encoding::For as u8),
        (REG_PAYLOAD_START, Encoding::TwoValue as u8),
        (blob, Encoding::Constant as u8),
        (filter, Encoding::Constant as u8),
    ];
    for &(region, enc) in cases {
        let opened = open_patched_restamped(&path, &schema, &base, |data| {
            patch_entry(data, region, |e| e.encoding = enc);
        });
        assert_eq!(
            opened.err(),
            Some(StorageError::Corrupt("encoding")),
            "encoding {enc:#x} on region {region} must be rejected",
        );
    }
}

// -----------------------------------------------------------------------
// Slice blob relocation
// -----------------------------------------------------------------------

/// The blob arm, both ways on one shard. 128 rows of 64-byte strings, of which
/// rows 0 and 1 share a span, is a 8128-byte heap at 63 bytes/row, so
/// `should_relocate_blob` cuts over at 15 sliced rows. Below it the slice
/// carries only its own rows' bytes — once per *distinct* span, since the
/// relocation dedup cache keys on `(src_blob, offset, length)`. At and past it,
/// and for the whole shard, it copies the region verbatim. Every arm decodes
/// back to the original strings.
#[test]
fn slice_relocates_only_its_own_strings() {
    let dir = tempfile::tempdir().unwrap();
    const N: usize = 128;
    const W: usize = 64;
    const CUT: usize = 14;
    let schema = make_schema_pk_u64_payload_string();
    let mut batch = Batch::with_capacity(&schema, N);
    // Row 1 reuses row 0's cell verbatim, so the two share one heap span — which
    // a per-row `encode_german_string` never produces (it always appends).
    let shared = gnitz_wire::encode_german_string(wide_string(0, W).as_bytes(), &mut batch.blob);
    for i in 0..N {
        let cell = match i {
            0 | 1 => shared,
            _ => gnitz_wire::encode_german_string(wide_string(i, W).as_bytes(), &mut batch.blob),
        };
        batch.begin_row(&(i as u64 + 1).to_be_bytes(), 1);
        batch.extend_col(0, &cell);
        batch.commit_row(0);
    }
    let shard = MappedShard::open(&write(dir.path(), "reloc.db", &batch, false), &schema).unwrap();
    assert_eq!(shard.blob().len(), (N - 1) * W, "row 1 added no bytes");
    let string = |b: &Batch, i: usize| String::from_utf8(read_german_string(b, 0, i)).unwrap();

    let one = shard.slice_to_owned_batch(37, 1);
    assert_eq!(one.blob.len(), W, "a one-row slice carries one string");
    assert_eq!(string(&one, 0), wide_string(37, W));

    let under = shard.slice_to_owned_batch(0, CUT);
    assert_eq!(
        under.blob.len(),
        (CUT - 1) * W,
        "relocates, and rows 0/1 share one span"
    );
    // Rows 0 and 1 both resolve to row 0's string, through the one copied span.
    assert_eq!(string(&under, 0), wide_string(0, W));
    assert_eq!(string(&under, 1), wide_string(0, W));

    let at = shard.slice_to_owned_batch(0, CUT + 1);
    assert_eq!(at.blob.len(), (N - 1) * W, "at the cut the whole region is copied");
    let full = shard.slice_to_owned_batch(0, N);
    assert_eq!(full.blob.as_slice(), shard.blob(), "whole shard: verbatim");

    for i in 2..CUT {
        assert_eq!(string(&under, i), wide_string(i, W), "relocated row {i}");
        assert_eq!(string(&at, i), wide_string(i, W), "whole-region row {i}");
    }
}

/// The measurement `RELOCATE_CELL_COST_BYTES` is set from: whole-region memcpy
/// against per-cell relocation on the *same* slice, swept over slice fraction ×
/// string width.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn slice_blob_relocate_bench() {
    use std::time::Instant;
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_pk_u64_payload_string();
    const N: usize = 20_000;
    const ITERS: usize = 50;

    let time_arm = |shard: &MappedShard, rc: usize, relocate: bool| -> f64 {
        // One untimed pass faults in the cold mmap pages.
        std::hint::black_box(shard.slice_to_owned_batch_with(0, rc, relocate));
        let t = Instant::now();
        for _ in 0..ITERS {
            std::hint::black_box(shard.slice_to_owned_batch_with(0, rc, relocate));
        }
        t.elapsed().as_secs_f64() * 1e9 / ITERS as f64
    };

    for &w in &[16usize, 40, 256, 1024] {
        let batch = build(schema, N, |b, i| {
            b.begin_row(i as u128 + 1, 1);
            b.put_string(&wide_string(i, w));
        });
        let shard = MappedShard::open(&write(dir.path(), &format!("bench_{w}.db"), &batch, false), &schema).unwrap();
        for &pct in &[
            1usize, 2, 3, 4, 6, 8, 12, 16, 20, 25, 33, 40, 50, 60, 68, 75, 85, 90, 99,
        ] {
            let rc = (N * pct / 100).max(1);
            let reloc = time_arm(&shard, rc, true);
            let copy = time_arm(&shard, rc, false);
            let picks = if super::super::merge::should_relocate_blob(shard.blob().len(), shard.row_count(), rc) {
                "relocate"
            } else {
                "memcpy  "
            };
            println!(
                "width={w:>5} slice={pct:>3}% picks {picks}: \
                 relocate {reloc:9.0} ns  memcpy {copy:9.0} ns  speedup {:5.2}x",
                copy / reloc,
            );
        }
    }
}

// -----------------------------------------------------------------------
// Descriptive-prefix, body and filter integrity
// -----------------------------------------------------------------------

#[test]
fn directory_sizes_that_do_not_tile_the_file_are_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let path = small_shard(dir.path(), 10, 100);
    let base = std::fs::read(&path).unwrap();
    let filter = num_regions(schema.num_payload_cols());
    let (fsz, _) = region_dir(&base, filter);
    assert!(fsz > 0);

    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |d| patch_entry(d, filter, |e| e.size = fsz - 1)).err(),
        Some(StorageError::Corrupt("directory does not span the file")),
        "last entry ends short of the file",
    );
    assert_eq!(
        open_patched_restamped(&path, &schema, &base, |d| patch_entry(d, filter, |e| e.size = fsz + 1)).err(),
        Some(StorageError::Corrupt("region past the end")),
        "last entry ends past the file",
    );
    // An intact prefix over a file cut short of its body.
    assert_eq!(
        open_patched(&path, &schema, &base, |d| d.truncate(d.len() - 1)).err(),
        Some(StorageError::Corrupt("region past the end")),
        "truncated body",
    );
}

#[test]
fn truncated_directory_is_reported_as_such() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let path = small_shard(dir.path(), 4, 1);
    let base = std::fs::read(&path).unwrap();
    assert_eq!(
        open_patched(&path, &schema, &base, |d| d.truncate(dir_entry_off(2))).err(),
        Some(StorageError::Corrupt("shorter than its directory")),
    );
}

/// The patch is in the body, so no digest stands in front of the parse.
#[test]
fn a_structurally_invalid_filter_fails_the_open() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let path = small_shard(dir.path(), 10, 1);
    let base = std::fs::read(&path).unwrap();
    let xoff = spans_of(&base)[num_regions(schema.num_payload_cols())].off;

    assert_eq!(
        open_patched(&path, &schema, &base, |d| {
            // `segment_length_mask` (descriptor bytes 12..16) no longer
            // agrees with `segment_length`.
            d[xoff + 12] ^= 0x01;
        })
        .err(),
        Some(StorageError::Corrupt("filter descriptor")),
    );
}

#[test]
fn a_filterless_shard_has_an_empty_trailing_entry() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let path = dir.path().join("nofilter.db");
    let rows = vec![(1u64.to_be_bytes().to_vec(), 1, 7)];
    let shard_path = super::super::shard_file::write_test_shard(
        &path,
        &schema,
        &rows,
        ShardWriteOpts {
            skip_pk_filter: true,
            ..Default::default()
        },
    );
    let image = std::fs::read(&path).unwrap();
    assert_eq!(
        region_dir(&image, num_regions(schema.num_payload_cols())),
        (0, Encoding::Raw)
    );
    let shard = MappedShard::open(&shard_path, &schema).unwrap();
    assert!(!shard.has_shard_filter());
    assert_eq!(shard.verify_body(), Ok(()));
}

/// The digest is seeded with the basename and nothing else: a shard renamed
/// out from under the manifest fails to open, while one hard-linked into
/// another directory under the same name still opens. The second half is
/// what the replicated relayout relies on when it links a sibling child.
#[test]
fn the_digest_seed_separates_names_not_directories() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let path = small_shard(dir.path(), 4, 1);
    let open = |p: &std::path::Path| MappedShard::open(p.to_str().unwrap(), &schema);

    let sibling = dir.path().join("child");
    std::fs::create_dir(&sibling).unwrap();
    let linked = sibling.join("test.db");
    std::fs::hard_link(&path, &linked).unwrap();
    assert_eq!(open(&linked).unwrap().row_count(), 4, "same name, another directory");

    let moved = dir.path().join("renamed.db");
    std::fs::rename(&path, &moved).unwrap();
    assert_eq!(
        open(&moved).err(),
        Some(StorageError::Corrupt("descriptor digest")),
        "new name"
    );
}

/// The shard shapes the sweeps below run over: one per *region count*, since
/// that is what sets the digest's span, plus the encodings that vary within
/// one. `(label, path, schema)`.
fn sweep_shapes(dir: &std::path::Path) -> Vec<(&'static str, String, SchemaDescriptor)> {
    let n = 32usize;
    let u64_i64 = make_schema_u64_i64();
    let string = make_schema_pk_u64_payload_string();
    let all_pk = crate::test_support::pk_only_schema(&[TypeCode::U64]);
    vec![
        // One row: a repeated PK is not a consolidated run, so that is the only
        // shape whose every region is Constant-encoded.
        (
            "all-constant",
            write(dir, "sw_const.db", &make_batch(&u64_i64, &[(1, 1, 7)]), false),
            u64_i64,
        ),
        (
            "two-value weight, for-packed payload",
            write(
                dir,
                "sw_twoval_for.db",
                &build(u64_i64, n, |b, i| {
                    b.begin_row(i as u128 + 1, if i % 2 == 0 { 1 } else { -1 });
                    b.put_int((5_000_000 + i as i64) as u128);
                }),
                true,
            ),
            u64_i64,
        ),
        // A blob region with real content.
        (
            "string payload",
            write(
                dir,
                "sw_string.db",
                &build(string, 24, |b, i| {
                    b.begin_row(i as u128 + 1, 1);
                    b.put_string(&wide_string(i, 48));
                }),
                false,
            ),
            string,
        ),
        (
            "all-pk (4 regions)",
            write(
                dir,
                "sw_pkonly.db",
                &build(all_pk, n, |b, i| b.begin_row(i as u128 + 1, 1)),
                false,
            ),
            all_pk,
        ),
    ]
}

/// The verdicts a corruption at `off` inside the prefix may produce: a header
/// field `ShardHeader::read` checks can fail its own check before the digest.
fn prefix_verdicts(off: usize) -> &'static [StorageError] {
    match off {
        o if o < OFF_VERSION => &[StorageError::Corrupt("magic")],
        o if o < OFF_ROW_COUNT => &[StorageError::Corrupt("version")],
        OFF_ROW_COUNT..OFF_DESC_CHECKSUM => &[
            StorageError::Corrupt("no rows"),
            StorageError::Corrupt("descriptor digest"),
        ],
        o if (OFF_FILE_NPC..OFF_FILE_NPC + 8).contains(&o) => &[
            StorageError::Corrupt("payload arity"),
            StorageError::Corrupt("shorter than its directory"),
            StorageError::Corrupt("descriptor digest"),
        ],
        _ => &[StorageError::Corrupt("descriptor digest")],
    }
}

/// Every bit of the descriptive prefix, including the digest field itself,
/// is inside the digest: no single-bit change to it opens.
#[test]
fn every_single_bit_flip_in_the_prefix_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    for (label, path, schema) in sweep_shapes(dir.path()) {
        let base = std::fs::read(&path).unwrap();
        let n_desc = desc_len(schema.num_payload_cols());
        assert!(n_desc <= base.len());
        for off in 0..n_desc {
            for bit in 0..8u32 {
                let got = open_patched(&path, &schema, &base, |d| d[off] ^= 1 << bit).err();
                let want = prefix_verdicts(off);
                assert!(
                    got.is_some_and(|e| want.contains(&e)),
                    "{label}: byte {off} bit {bit}: got {got:?}, want one of {want:?}",
                );
            }
        }
    }
}

/// A flipped byte anywhere in the body — any region's last byte, or padding —
/// still opens, and fails `verify_body`.
#[test]
fn every_body_region_and_its_padding_is_inside_the_body_checksum() {
    let dir = tempfile::tempdir().unwrap();
    for (label, path, schema) in sweep_shapes(dir.path()) {
        let base = std::fs::read(&path).unwrap();
        assert_eq!(
            MappedShard::open(&path, &schema).unwrap().verify_body(),
            Ok(()),
            "{label}"
        );
        let spans = spans_of(&base);
        let pad = spans[0].off - 1;
        assert!(
            pad >= desc_len(schema.num_payload_cols()),
            "{label}: the first region is padded"
        );
        let targets = spans
            .iter()
            .enumerate()
            .filter(|(_, s)| s.size > 0)
            .map(|(i, s)| (format!("region {i}"), s.off + s.size - 1))
            .chain([("padding".to_owned(), pad)]);
        for (what, at) in targets {
            let shard = open_patched(&path, &schema, &base, |d| d[at] ^= 0x01).unwrap();
            assert_eq!(
                shard.verify_body(),
                Err(StorageError::Corrupt("body checksum")),
                "{label}: {what}",
            );
        }
    }
}

/// A region's size is fully determined by the row count and its encoding, so a
/// directory size that is off by a byte in either direction must be refused —
/// for the direct roles, the TwoValue weight and the FoR payload alike.
/// Re-stamped, so the verdict is the size check's rather than the digest's.
#[test]
fn a_region_size_that_disagrees_with_the_row_count_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let direct = small_shard(dir.path(), 10, 3);
    let packed = write(
        dir.path(),
        "packed_size.db",
        &build(schema, 64, |b, i| {
            b.begin_row(i as u128 + 1, if i % 2 == 0 { 1 } else { -1 });
            b.put_int((2_000 + (i % 5) as i64) as u128);
        }),
        true,
    );
    assert_eq!(
        region_dir(&std::fs::read(&packed).unwrap(), REG_WEIGHT).1,
        Encoding::TwoValue
    );
    assert_eq!(
        region_dir(&std::fs::read(&packed).unwrap(), REG_PAYLOAD_START).1,
        Encoding::For
    );

    for (what, path, regions) in [
        (
            "direct",
            direct,
            &[REG_PK, REG_WEIGHT, REG_NULL_BMP, REG_PAYLOAD_START][..],
        ),
        (
            "two-value weight, for payload",
            packed,
            &[REG_WEIGHT, REG_PAYLOAD_START][..],
        ),
    ] {
        let base = std::fs::read(&path).unwrap();
        for &region in regions {
            let (sz, _) = region_dir(&base, region);
            for delta in [-1isize, 1] {
                let forged = sz.checked_add_signed(delta).unwrap();
                assert_eq!(
                    open_patched_restamped(&path, &schema, &base, |d| patch_entry(d, region, |e| e.size = forged))
                        .err(),
                    Some(StorageError::Corrupt("region size")),
                    "{what}: region {region} size {sz}{delta:+}",
                );
            }
        }
    }
}

// -----------------------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------------------

/// Per-pass cost of slicing a FoR shard window by window.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_slice_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const N: usize = 1_000_000;
    const WINDOW: usize = 1024;
    const HANDLES: usize = 20;
    let schema = make_schema_u64_i64();
    let dir = tempfile::tempdir().unwrap();
    let rows: Vec<(Vec<u8>, i64, i64)> = (0..N)
        .map(|i| ((i as u64).to_be_bytes().to_vec(), 1, 3_000_000_000 + (i % 4000) as i64))
        .collect();
    let path = super::super::shard_file::write_test_shard(
        &dir.path().join("for_slice.db"),
        &schema,
        &rows,
        ShardWriteOpts::COMPACTION,
    );
    let handles: Vec<MappedShard> = (0..HANDLES)
        .map(|_| MappedShard::open(&path, &schema).unwrap())
        .collect();
    assert!(matches!(handles[0].col_regions[0], PayloadRegion::Packed(_)));
    let slice_all = |shard: &MappedShard| {
        for start in (0..N).step_by(WINDOW) {
            black_box(shard.slice_to_owned_batch(start, WINDOW.min(N - start)));
        }
    };
    let (cycles, instructions) = (Counter::cycles().unwrap(), Counter::instructions().unwrap());
    for label in ["first pass", "second pass"] {
        let (((), i), c) = cycles.measure(|| instructions.measure(|| handles.iter().for_each(slice_all)));
        println!(
            "{label}: {} cycles, {} instructions per pass",
            c / HANDLES as u64,
            i / HANDLES as u64,
        );
    }
}

/// First-touch cost of a per-row read on a packed column: a fresh handle, one
/// `get_col_ptr` per FoR column at a mid row, then the same read again.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_point_touch_bench() {
    use crate::test_support::{pk_u64_two_i64_schema, settled_rss};
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const N: usize = 1_000_000;
    let schema = pk_u64_two_i64_schema();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("point.db").to_str().unwrap().to_owned();
    let rows: Vec<_> = (0..N)
        .map(|i| {
            (
                (i as u64).to_be_bytes().to_vec(),
                1,
                0,
                vec![(i % 1000) as i64, 3 * i as i64],
            )
        })
        .collect();
    write_i64_shard(&path, &schema, &rows, &[], ShardWriteOpts::COMPACTION);
    let shard = MappedShard::open(&path, &schema).unwrap();
    let (cycles, instructions) = (Counter::cycles().unwrap(), Counter::instructions().unwrap());
    let read = || {
        for pi in 0..2 {
            black_box(shard.get_col_ptr(black_box(N / 2), pi, 8));
        }
    };
    let rss0 = settled_rss();
    for label in ["cold", "warm"] {
        let (((), i), c) = cycles.measure(|| instructions.measure(read));
        let retained = settled_rss().saturating_sub(rss0);
        println!("{label}: {i} instructions, {c} cycles; {retained} bytes retained");
    }
}
