use super::super::batch::{Batch, REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT};
use super::super::batch_builder::BatchBuilder;
use super::super::merge::ColumnarSource;
use super::super::scatter::UnifiedSet;
use super::super::shard_file::ShardWriteOpts;
use super::*;
use crate::repr::error::StorageError;
use crate::schema::{SchemaColumn, SchemaDescriptor, TypeCode};
use crate::test_support::{
    make_batch, make_schema_pk_u64_payload_string, make_schema_u128_i64, make_schema_u64_i64, pk_only_schema,
    read_german_string, sweep_bit_flips, u64_pk_schema,
};
use gnitz_wire::num_regions;
use gnitz_wire::{read_i64_le, write_u64_le};

/// Write `batch` through the production writer to `dir/name`.
fn write(dir: &std::path::Path, name: &str, batch: &Batch) -> String {
    let path = dir.join(name).to_str().unwrap().to_owned();
    batch.write_as_shard(&path, ShardWriteOpts::default()).unwrap();
    path
}

/// A `(U64 PK | I64)` shard of `rows` at weight 1 and payload `pk * mul`, at
/// `dir/test.db` — the fixture the integrity tests patch.
fn small_shard(dir: &std::path::Path, rows: u64, mul: i64) -> String {
    let rows: Vec<(u64, i64, i64)> = (1..=rows).map(|i| (i, 1, i as i64 * mul)).collect();
    write(dir, "test.db", &make_batch(&make_schema_u64_i64(), &rows))
}

/// Open `base` patched and written back over `path`: the digest is left stale,
/// and its seed, the basename, unchanged.
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

fn rows_of(src: &impl ColumnarSource, schema: &SchemaDescriptor) -> Vec<RowImage> {
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
            (
                src.get_pk_bytes(r).to_vec(),
                src.get_weight(r),
                src.get_null_word(r),
                cells,
            )
        })
        .collect()
}

/// `shard` reads as `want` — the written batch widened to the reader's schema —
/// through every surface: the per-row accessors, a slice on both blob arms, a
/// `UnifiedSet` over a window, the OPK seeks and the PK filter.
fn assert_reads_as(label: &str, shard: &MappedShard, want: &Batch) {
    let schema = want.schema();
    let n = want.count;
    assert_eq!(shard.row_count(), n, "{label}: row count");
    let want_rows = rows_of(&want.as_mem_batch(), schema);
    assert_eq!(rows_of(shard, schema), want_rows, "{label}: per-row");
    assert_eq!(
        shard.retraction_rows(),
        want.retracted_rows().count(),
        "{label}: retractions"
    );

    // An odd-length window starting mid-shard.
    let mid_start = n / 3;
    let mid = mid_start..mid_start + (((n - mid_start) / 2) | 1).min(n - mid_start);
    for relocate in [false, true] {
        for w in [0..n, 0..0, mid.clone()] {
            // A whole heap charged dead bounds any slice's.
            let carried = (!relocate).then(|| shard.blob().len());
            let b = shard.slice_to_owned_batch_with(w.start, w.len(), carried);
            assert_eq!(
                rows_of(&b.as_mem_batch(), schema),
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
            rows_of(&b.as_mem_batch(), schema),
            want_rows[w.clone()],
            "{label}: materialize {w:?}"
        );
    }
    for r in 0..n {
        let key = want.get_pk_bytes(r);
        assert!(
            shard.shard_filter_may_contain(crate::schema::key::probe_key(key)),
            "{label}: PK filter false negative on row {r}"
        );
        let lb = (0..n).find(|&i| want.get_pk_bytes(i) == key).unwrap();
        assert_eq!(shard.find_lower_bound_bytes(key), lb, "{label}: lower bound row {r}");
        assert_eq!(
            want.find_lower_bound_bytes(key),
            lb,
            "{label}: batch lower bound row {r}"
        );
        for hint in [0, lb] {
            assert_eq!(
                shard.advance_to(key, hint),
                want.advance_to(key, hint),
                "{label}: advance row {r}"
            );
        }
    }
}

/// A shape: its label, the batch written, the schema it is read under, and the
/// encodings its directory must carry.
struct Shape {
    label: &'static str,
    written: Batch,
    reader: SchemaDescriptor,
    encodings: Vec<(usize, Encoding)>,
}

/// A batch over `schema` of `n` rows, row `i` begun and filled by `row`.
fn build(schema: SchemaDescriptor, n: usize, mut row: impl FnMut(&mut BatchBuilder, usize)) -> Batch {
    let mut b = BatchBuilder::new(&schema);
    for i in 0..n {
        row(&mut b, i);
        b.end_row();
    }
    b.finish()
}

fn shapes() -> Vec<Shape> {
    use Encoding::{Constant, Dict, For, Raw, Seq, Sparse, TwoValue};
    let u64_i64 = make_schema_u64_i64();
    let nullable_i64 = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    let u64_i32 = u64_pk_schema(SchemaColumn::new(TypeCode::I32, false));
    let u64_f64 = u64_pk_schema(SchemaColumn::new(TypeCode::F64, false));
    let u64_u8 = u64_pk_schema(SchemaColumn::new(TypeCode::U8, false));
    let u64_u128 = u64_pk_schema(SchemaColumn::new(TypeCode::U128, false));
    let opt_i64 = SchemaColumn::new(TypeCode::I64, true);
    let three_nullable = SchemaDescriptor::new(
        &[SchemaColumn::new(TypeCode::U64, false), opt_i64, opt_i64, opt_i64],
        &[0],
    );
    let sparse_mix = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::U128, true),
            opt_i64,
            opt_i64,
        ],
        &[0],
    );
    let u128_i64 = make_schema_u128_i64();
    let all_pk = pk_only_schema(&[TypeCode::U64; 3]);
    let string = make_schema_pk_u64_payload_string();
    let nullable_string = u64_pk_schema(SchemaColumn::new(TypeCode::String, true));
    let str_col = SchemaColumn::new(TypeCode::String, false);
    let two_strings = SchemaDescriptor::new(&[SchemaColumn::new(TypeCode::U64, false), str_col, str_col], &[0]);
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
        // One row: every region Constant-encoded.
        Shape {
            label: "all-constant",
            written: make_batch(&u64_i64, &[(1, 1, 7)]),
            reader: u64_i64,
            encodings: vec![
                (REG_PK, Constant),
                (REG_WEIGHT, Constant),
                (REG_NULL_BMP, Constant),
                (REG_PAYLOAD_START, Constant),
            ],
        },
        Shape {
            label: "for weight, two-value null, raw i64",
            written: build(nullable_i64, 30, |b, i| {
                b.begin_row(i as u128 * 5, [1, -1, 2][i % 3]);
                match i % 4 {
                    0 => b.put_null(),
                    _ => b.put_int((i as i64).wrapping_mul(0x9E37_79B9_7F4A_7C15_u64 as i64) as u128),
                }
            }),
            reader: nullable_i64,
            encodings: vec![
                (REG_PK, Raw),
                (REG_WEIGHT, For),
                (REG_NULL_BMP, TwoValue),
                (REG_PAYLOAD_START, Raw),
            ],
        },
        // Eight null words: more than two, in a frame of one byte.
        Shape {
            label: "for null across decode blocks",
            written: build(three_nullable, DECODE_BLOCK_ROWS + 9, |b, i| {
                b.begin_row(i as u128, 1);
                for bit in 0..3 {
                    b.put_opt_int((i >> bit & 1 == 0).then_some(i as u128 * 0x0101_0101_0101));
                }
            }),
            reader: three_nullable,
            encodings: vec![(REG_NULL_BMP, For)],
        },
        // One row in six holds a value: the frame over those alone is two bytes,
        // where the zero of a NULL cell would stretch it to six.
        Shape {
            label: "sparse i64 across decode blocks",
            written: build(nullable_i64, 2 * DECODE_BLOCK_ROWS + 17, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_opt_int((i % 6 == 2).then_some(1_700_000_000_000 + i as u128 * 13));
            }),
            reader: nullable_i64,
            encodings: vec![(REG_NULL_BMP, TwoValue), (REG_PAYLOAD_START, Sparse)],
        },
        // The first holds its cells, a u128 taking no frame; the third is under
        // half NULL and keeps its frame.
        Shape {
            label: "sparse columns beside a framed one",
            written: build(sparse_mix, 900, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_opt_int((i % 9 == 0).then_some((i as u128 + 1) * (u128::MAX / 1000)));
                b.put_opt_int((i % 4 == 1).then_some((i as u128 * 0x9E37_79B9_7F4A_7C15) & u64::MAX as u128));
                b.put_opt_int((i % 3 != 0).then_some(40_000 + i as u128));
            }),
            reader: sparse_mix,
            encodings: vec![
                (REG_PAYLOAD_START, Sparse),
                (REG_PAYLOAD_START + 1, Sparse),
                (REG_PAYLOAD_START + 2, For),
            ],
        },
        // Two bits a row, where no frame is narrower than the cell.
        Shape {
            label: "dictionary of one-byte cells",
            written: build(u64_u8, 2000, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_int((i % 3) as u128 * 100);
            }),
            reader: u64_u8,
            encodings: vec![(REG_PAYLOAD_START, Dict)],
        },
        Shape {
            label: "dictionary of floats",
            written: build(u64_f64, 300, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_int(((i % 7) as f64 * 0.25 - 1.0).to_bits() as u128);
            }),
            reader: u64_f64,
            encodings: vec![(REG_PAYLOAD_START, Dict)],
        },
        Shape {
            label: "dictionary of 16-byte integers, two-byte codes",
            written: build(u64_u128, 1200, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_int(((i % 300) as u128 + 1) * (u128::MAX / 301));
            }),
            reader: u64_u128,
            encodings: vec![(REG_PAYLOAD_START, Dict)],
        },
        // Five values a frame of four bytes spans: the dictionary is the smaller.
        Shape {
            label: "dictionary of i64 under a wider frame",
            written: build(u64_i64, 300, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_int(((i % 5) as i64 * 500_000_000 - 7) as u128);
            }),
            reader: u64_i64,
            encodings: vec![(REG_PAYLOAD_START, Dict)],
        },
        // Three weights spanning more than a FoR offset holds.
        Shape {
            label: "raw weight",
            written: build(u64_i64, 12, |b, i| {
                b.begin_row(i as u128, [1, -1, i64::MAX][i % 3]);
                b.put_int(i as u128);
            }),
            reader: u64_i64,
            encodings: vec![(REG_WEIGHT, Raw)],
        },
        Shape {
            label: "constant weight, null and payload",
            written: build(u64_i64, 20, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                b.put_int(42);
            }),
            reader: u64_i64,
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
            encodings: vec![(REG_WEIGHT, TwoValue), (REG_PAYLOAD_START, For)],
        },
        Shape {
            label: "for i32",
            written: build(u64_i32, 400, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_int((-70_000i32 + (i % 250) as i32) as i64 as u128);
            }),
            reader: u64_i32,
            encodings: vec![(REG_PAYLOAD_START, For)],
        },
        Shape {
            label: "for i64 across decode blocks",
            written: build(u64_i64, 2 * DECODE_BLOCK_ROWS + 17, for_i64),
            reader: u64_i64,
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
            // One value in a third of the rows: a dictionary's entries outweigh the lengths.
            encodings: vec![(REG_PAYLOAD_START, Seq)],
        },
        Shape {
            label: "strings, no value twice",
            written: build(string, 24, |b, i| {
                b.begin_row(i as u128, 1);
                match i % 3 {
                    0 => b.put_string(&format!("s{i}")),
                    _ => b.put_string(&wide_string(i, 20 + i)),
                }
            }),
            reader: string,
            encodings: vec![(REG_PAYLOAD_START, Seq)],
        },
        // Two rows: their lengths would not be the smaller image.
        Shape {
            label: "two strings",
            written: build(string, 2, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_string(&wide_string(i, 20 + i));
            }),
            reader: string,
            encodings: vec![(REG_PAYLOAD_START, Raw)],
        },
        // Lengths of two bytes, and a NULL the sample's runs do not reach twice.
        Shape {
            label: "distinct strings across decode blocks",
            written: build(nullable_string, 2 * DECODE_BLOCK_ROWS + 17, |b, i| {
                b.begin_row(i as u128, 1);
                match i % 700 {
                    0 => b.put_null(),
                    1 => b.put_string(&wide_string(i, 300)),
                    v if v % 2 == 0 => b.put_string(&format!("s{i}")),
                    _ => b.put_string(&wide_string(i, 13 + i % 40)),
                }
            }),
            reader: nullable_string,
            encodings: vec![(REG_PAYLOAD_START, Seq)],
        },
        Shape {
            label: "one long string in every row",
            written: build(string, 9, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_string(&wide_string(0, 40));
            }),
            reader: string,
            encodings: vec![(REG_PAYLOAD_START, Constant)],
        },
        // Too few rows for a dictionary to be the smaller image.
        Shape {
            label: "a repeated string in three rows",
            written: build(string, 3, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_string(&wide_string(i / 2, 40));
            }),
            reader: string,
            encodings: vec![(REG_PAYLOAD_START, Raw)],
        },
        Shape {
            label: "dictionary of two-byte codes",
            written: build(string, 1500, |b, i| {
                b.begin_row(i as u128, 1);
                match i % 400 {
                    v if v % 2 == 0 => b.put_string(&format!("v{v}")),
                    v => b.put_string(&wide_string(v, 13 + v % 50)),
                }
            }),
            reader: string,
            encodings: vec![(REG_PAYLOAD_START, Dict)],
        },
        Shape {
            label: "nullable dictionary",
            written: build(nullable_string, 60, |b, i| {
                b.begin_row(i as u128, 1);
                match i % 5 {
                    0 => b.put_null(),
                    v => b.put_string(&wide_string(v, 10 * v)),
                }
            }),
            reader: nullable_string,
            encodings: vec![(REG_NULL_BMP, TwoValue), (REG_PAYLOAD_START, Dict)],
        },
        // The second column's sample repeats nothing, so its long values follow the
        // first one's dictionary entries on the heap.
        Shape {
            label: "a repeating string column beside a distinct one",
            written: build(two_strings, 70, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_string(&wide_string(i % 4, 30));
                b.put_string(&wide_string(i, 25));
            }),
            reader: two_strings,
            encodings: vec![(REG_PAYLOAD_START, Dict), (REG_PAYLOAD_START + 1, Seq)],
        },
        Shape {
            label: "u128 pk across the u64 boundary",
            written: build(u128_i64, 6, |b, i| {
                b.begin_row(wide_key(i), 1);
                b.put_int(i as u128);
            }),
            reader: u128_i64,
            encodings: vec![(REG_PK, Raw)],
        },
        Shape {
            label: "3xu64 all-pk",
            written: build(all_pk, 9, |b, i| {
                b.begin_row_natives(&[1, (i / 4) as u128, (i * 7) as u128], 1)
            }),
            reader: all_pk,
            encodings: vec![(REG_PK, Raw)],
        },
        Shape {
            label: "repeated pk",
            written: build(u64_i64, 12, |b, i| {
                b.begin_row(77, 1);
                b.put_int(i as u128 * 3);
            }),
            reader: u64_i64,
            encodings: vec![(REG_PK, Constant)],
        },
        Shape {
            label: "widened by i64",
            written: w1,
            reader: schema_with_appended(TypeCode::I64),
            encodings: vec![(REG_PAYLOAD_START, For)],
        },
        Shape {
            label: "widened by string",
            written: w2,
            reader: schema_with_appended(TypeCode::String),
            encodings: vec![(REG_PAYLOAD_START, For)],
        },
    ]
}

/// Every shape written under `dir`, with its path.
fn written_shapes(dir: &std::path::Path) -> Vec<(Shape, String)> {
    shapes()
        .into_iter()
        .enumerate()
        .map(|(i, s)| {
            let path = write(dir, &format!("shape_{i}.db"), &s.written);
            (s, path)
        })
        .collect()
}

#[test]
fn every_shape_reads_back_through_every_surface() {
    let dir = tempfile::tempdir().unwrap();
    for (s, path) in written_shapes(dir.path()) {
        let image = std::fs::read(&path).unwrap();
        for &(region, enc) in &s.encodings {
            assert_eq!(spans_of(&image)[region].encoding, enc, "{}: region {region}", s.label);
        }
        let shard = MappedShard::open(&path, &s.reader).unwrap();
        assert_reads_as(s.label, &shard, &s.written.widened_with_nulls(&s.reader, false));
    }
}

/// Whatever image the writer picks for a column the reader admits and reads
/// back: every fixed-width payload type, nullable or not, over values that
/// repeat, that span little, that are mostly NULL and that are arbitrary.
#[test]
fn every_column_type_reads_back_whatever_its_values() {
    use TypeCode::*;
    type Value = fn(usize) -> Option<u128>;
    let values: [(&str, Value); 5] = [
        ("two values", |i| Some((i % 2) as u128 * 77)),
        ("forty values", |i| Some((i % 40) as u128 * 3)),
        ("a narrow span", |i| Some(100 + (i as u128 * 7919) % 150)),
        ("mostly NULL", |i| {
            (i % 9 == 4).then_some(1 + i as u128 * 0x0101_0101_0101_0101_0101)
        }),
        ("arbitrary", |i| {
            Some((i as u128 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C835))
        }),
    ];
    let dir = tempfile::tempdir().unwrap();
    let mut seen = std::collections::BTreeSet::new();
    for tc in [
        U8, I8, U16, I16, U32, I32, F32, U64, I64, F64, U128, UUID, I128, Date, Timestamp, Decimal,
    ] {
        for nullable in [false, true] {
            let schema = u64_pk_schema(SchemaColumn::new(tc, nullable));
            let mask = u128::MAX >> (128 - 8 * schema.columns[1].size());
            for (what, value) in values {
                for n in [1, 700, DECODE_BLOCK_ROWS + 3] {
                    let batch = build(schema, n, |b, i| {
                        b.begin_row(i as u128, 1);
                        // A column that takes no NULL holds a zero in its place.
                        b.put_opt_int(value(i).map(|v| v & mask).or((!nullable).then_some(0)));
                    });
                    let label = format!("{tc:?} nullable={nullable}, {what}, {n} rows");
                    let path = write(dir.path(), "column.db", &batch);
                    seen.insert(
                        spans_of(&std::fs::read(&path).unwrap())[REG_PAYLOAD_START]
                            .encoding
                            .name(),
                    );
                    let shard = MappedShard::open(&path, &schema).unwrap_or_else(|e| panic!("{label}: {e:?}"));
                    assert_reads_as(&label, &shard, &batch);
                    std::fs::remove_file(&path).unwrap();
                }
            }
        }
    }
    assert_eq!(
        seen.into_iter().collect::<Vec<_>>(),
        ["constant", "dict", "for", "raw", "sparse"],
        "every encoding a fixed-width column takes"
    );
}

/// The schema-free directory read agrees with the image on every shape, and
/// refuses a prefix the digest does not cover.
#[test]
fn the_directory_reads_without_a_schema() {
    let dir = tempfile::tempdir().unwrap();
    for (s, path) in written_shapes(dir.path()) {
        let image = std::fs::read(&path).unwrap();
        let d = ShardDirectory::read(&path).unwrap();
        assert_eq!(d.rows, s.written.count, "{}", s.label);
        assert!(!d.skeleton, "{}", s.label);
        let npc = s.written.schema().num_payload_cols();
        assert_eq!(d.regions.len(), num_regions(npc) + 1, "{}", s.label);
        assert_eq!(
            d.body_checksum,
            gnitz_wire::checksum(&image[desc_len(npc)..]),
            "{}",
            s.label
        );
        let roles = ["pk", "weight", "null"].into_iter().map(String::from);
        let roles = roles.chain((0..npc).map(|pi| format!("p{pi}")));
        let want: Vec<(String, &str, usize)> = roles
            .chain(["blob", "filter"].map(String::from))
            .zip(spans_of(&image))
            .map(|(role, span)| (role, span.encoding.name(), span.size))
            .collect();
        assert_eq!(d.regions, want, "{}", s.label);
    }
    let path = small_shard(dir.path(), 4, 1);
    let mut image = std::fs::read(&path).unwrap();
    patch_entry(&mut image, REG_PK, |e| e.size += 1);
    std::fs::write(&path, &image).unwrap();
    assert_eq!(
        ShardDirectory::read(&path).err(),
        Some(StorageError::Corrupt("descriptor digest"))
    );
    std::fs::write(&path, &image[..HEADER_SIZE + 3]).unwrap();
    assert_eq!(
        ShardDirectory::read(&path).err(),
        Some(StorageError::Corrupt("shorter than its directory"))
    );
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
    let shard = MappedShard::open(&write(dir, "blocks.db", &batch), &schema).unwrap();
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
    assert_eq!(rebound.row_count(), n);
    assert_eq!(rows_of(&rebound, &wide), rows_of(&fresh, &wide));
}

#[test]
fn shard_wider_than_reader_schema_opens() {
    // A crash between a checkpoint's manifest publish and its catalog write
    // leaves a shard one column wider than the catalog.
    let dir = tempfile::tempdir().unwrap();
    let wide = build(schema_with_appended(TypeCode::I64), 3, |b, i| {
        b.begin_row(i as u128 + 1, 1);
        b.put_int((i as u128 + 1) * 10);
        b.put_int((i as u128 + 1) * 11);
    });
    let shard_path = write(dir.path(), "wide.db", &wide);

    let shard = MappedShard::open(&shard_path, &make_schema_u64_i64()).unwrap();
    for row in 0..3 {
        assert_eq!(read_i64_le(shard.get_col_ptr(row, 0, 8), 0), (row as i64 + 1) * 10);
    }
    // The blob region came from the *file's* index, not the reader's.
    assert!(shard.blob().is_empty());
}

/// An all-PK table widened by `ADD COLUMN` has a skeleton's region shape, and
/// its NULL columns are real: only the flag makes a skeleton.
#[test]
fn a_widened_all_pk_shard_is_not_a_skeleton() {
    let dir = tempfile::tempdir().unwrap();
    let all_pk = build(pk_only_schema(&[TypeCode::U64]), 1, |b, _| b.begin_row(1, 1));
    let widened = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    let shard = MappedShard::open(&write(dir.path(), "allpk.db", &all_pk), &widened).unwrap();
    assert!(gnitz_wire::null_word_get(shard.get_null_word(0), 0));
    assert!(!shard.is_skeleton());
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
        (blob, Encoding::For as u8),
        (REG_PAYLOAD_START, Encoding::TwoValue as u8),
        (REG_PK, Encoding::Dict as u8),
        (REG_WEIGHT, Encoding::Dict as u8),
        (REG_NULL_BMP, Encoding::Dict as u8),
        (blob, Encoding::Dict as u8),
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

/// A slice relocates its own strings until the rows it leaves out would leave a
/// carried heap under a quarter dead.
#[test]
fn slice_relocates_only_its_own_strings() {
    let dir = tempfile::tempdir().unwrap();
    const N: usize = 128;
    const W: usize = 64;
    // The fewest rows whose 32 × 64 excluded bytes are no more than a quarter of the heap.
    const CUT: usize = 96;
    let schema = make_schema_pk_u64_payload_string();
    let batch = build(schema, N, |b, i| {
        b.begin_row(i as u128 + 1, 1);
        b.put_string(&wide_string(i, W));
    });
    let shard = MappedShard::open(&write(dir.path(), "reloc.db", &batch), &schema).unwrap();
    assert_eq!(shard.blob().len(), N * W);
    let string = |b: &Batch, i: usize| String::from_utf8(read_german_string(b, 0, i)).unwrap();

    let one = shard.slice_to_owned_batch(37, 1);
    assert_eq!(one.blob.len(), W, "a one-row slice carries one string");
    assert_eq!(string(&one, 0), wide_string(37, W));

    let under = shard.slice_to_owned_batch(0, CUT - 1);
    assert_eq!((under.blob.len(), under.dead_heap), ((CUT - 1) * W, 0), "relocates");

    let at = shard.slice_to_owned_batch(0, CUT);
    assert_eq!(
        (at.blob.len(), at.dead_heap),
        (N * W, (N - CUT) * W),
        "at the cut the whole region is copied, the rows left out charged dead"
    );
    let full = shard.slice_to_owned_batch(0, N);
    assert_eq!(full.blob.as_slice(), shard.blob(), "whole shard: verbatim");

    for i in 0..CUT - 1 {
        assert_eq!(string(&under, i), wide_string(i, W), "relocated row {i}");
        assert_eq!(string(&at, i), wide_string(i, W), "whole-region row {i}");
    }
}

/// A relocating slice copies a span two of its cells share once.
#[test]
fn a_relocating_slice_copies_a_shared_span_once() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_pk_u64_payload_string();
    // Too few rows for any image but their cells, which share a repeated value's span.
    let batch = build(schema, 3, |b, i| {
        b.begin_row(i as u128 + 1, 1);
        b.put_string(&wide_string(i % 2, 40));
    });
    let shard = MappedShard::open(&write(dir.path(), "shared.db", &batch), &schema).unwrap();
    assert_eq!(shard.blob().len(), 2 * 40, "premise: rows 0 and 2 share one span");
    let slice = shard.slice_to_owned_batch_with(0, 3, None);
    assert_eq!(slice.blob.len(), 2 * 40);
    for i in 0..3 {
        assert_eq!(
            read_german_string(&slice, 0, i),
            wide_string(i % 2, 40).as_bytes(),
            "row {i}"
        );
    }
}

/// The measurement `RELOCATE_CELL_COST_BYTES` is set from: whole-region memcpy
/// against per-cell relocation on the *same* slice, swept over slice fraction ×
/// string width.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn slice_blob_relocate_bench() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_pk_u64_payload_string();
    const N: usize = 20_000;
    const ITERS: usize = 50;

    let time_arm = |shard: &MappedShard, rc: usize, relocate: bool| -> f64 {
        let carried = (!relocate).then(|| shard.blob().len());
        // The untimed warmup faults in the cold mmap pages.
        let t = crate::test_support::bench_time(ITERS, || {
            std::hint::black_box(shard.slice_to_owned_batch_with(0, rc, carried));
        });
        t.as_secs_f64() * 1e9 / ITERS as f64
    };

    for &w in &[16usize, 40, 256, 1024] {
        let batch = build(schema, N, |b, i| {
            b.begin_row(i as u128 + 1, 1);
            b.put_string(&wide_string(i, w));
        });
        let shard = MappedShard::open(&write(dir.path(), &format!("bench_{w}.db"), &batch), &schema).unwrap();
        for &pct in &[
            1usize, 2, 3, 4, 6, 8, 12, 16, 20, 25, 33, 40, 50, 60, 68, 75, 85, 90, 99,
        ] {
            let rc = (N * pct / 100).max(1);
            let reloc = time_arm(&shard, rc, true);
            let copy = time_arm(&shard, rc, false);
            let picks = if super::super::string_heap::should_relocate_blob(shard.blob().len(), shard.row_count(), rc) {
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

/// Each forgery fails the open with its own verdict. `restamp` re-signs the
/// prefix digest, so a prefix forgery reaches the check under test.
#[test]
fn each_forgery_is_refused_with_its_own_verdict() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let path = small_shard(dir.path(), 10, 100);
    let base = std::fs::read(&path).unwrap();
    let filter = num_regions(schema.num_payload_cols());
    let Span { off: xoff, size: fsz, .. } = spans_of(&base)[filter];

    type Forge = Box<dyn Fn(&mut Vec<u8>)>;
    let npc = |v: u64| -> Forge { Box::new(move |d| write_u64_le(d, OFF_FILE_NPC, v)) };
    let filter_size = |sz: usize| -> Forge { Box::new(move |d| patch_entry(d, filter, |e| e.size = sz)) };
    let cases: Vec<(&str, bool, Forge, &str)> = vec![
        ("file_npc u64::MAX", false, npc(u64::MAX), "payload arity"),
        (
            "file_npc past MAX_COLUMNS",
            false,
            npc(gnitz_wire::MAX_COLUMNS as u64 + 1),
            "payload arity",
        ),
        ("file_npc in range but wrong", false, npc(2), "descriptor digest"),
        (
            "zero row count",
            false,
            Box::new(|d| write_u64_le(d, OFF_ROW_COUNT, 0)),
            "no rows",
        ),
        (
            "last entry ends short of the file",
            true,
            filter_size(fsz - 1),
            "directory does not span the file",
        ),
        (
            "last entry ends past the file",
            true,
            filter_size(fsz + 1),
            "region past the end",
        ),
        (
            "an intact prefix over a truncated body",
            false,
            Box::new(|d| d.truncate(d.len() - 1)),
            "region past the end",
        ),
        ("empty file", false, Box::new(|d| d.clear()), "empty file"),
        (
            "truncated directory",
            false,
            Box::new(|d| d.truncate(dir_entry_off(2))),
            "shorter than its directory",
        ),
        // `segment_length_mask` (descriptor bytes 12..16) no longer agrees
        // with `segment_length`.
        (
            "filter descriptor",
            false,
            Box::new(move |d| d[xoff + 12] ^= 0x01),
            "filter descriptor",
        ),
    ];
    for (what, restamp, forge, verdict) in cases {
        let opened = match restamp {
            true => open_patched_restamped(&path, &schema, &base, |d| forge(d)),
            false => open_patched(&path, &schema, &base, |d| forge(d)),
        };
        assert_eq!(opened.err(), Some(StorageError::Corrupt(verdict)), "{what}");
    }
}

#[test]
fn a_filterless_shard_has_an_empty_trailing_entry() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let shard_path = dir.path().join("nofilter.db").to_str().unwrap().to_owned();
    let no_filter = ShardWriteOpts {
        skip_pk_filter: true,
        ..Default::default()
    };
    make_batch(&schema, &[(1, 1, 7)])
        .write_as_shard(&shard_path, no_filter)
        .unwrap();
    let image = std::fs::read(&shard_path).unwrap();
    let filter = spans_of(&image).pop().unwrap();
    assert_eq!((filter.size, filter.encoding), (0, Encoding::Raw));
    let shard = MappedShard::open(&shard_path, &schema).unwrap();
    assert!(!shard.has_shard_filter());
    assert_eq!(shard.verify_body(), Ok(()));
}

/// The digest is seeded with the basename alone: a renamed shard fails to open,
/// one hard-linked under its name elsewhere, as a replicated relayout does, opens.
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
    for (s, path) in written_shapes(dir.path()) {
        let schema = s.written.schema();
        let mut image = std::fs::read(&path).unwrap();
        sweep_bit_flips(
            &mut image,
            0..desc_len(schema.num_payload_cols()),
            |off, bit, damaged| {
                std::fs::write(&path, damaged).unwrap();
                let got = MappedShard::open(&path, schema).err();
                let want = prefix_verdicts(off);
                assert!(
                    got.is_some_and(|e| want.contains(&e)),
                    "{}: byte {off} bit {bit}: got {got:?}, want one of {want:?}",
                    s.label,
                );
            },
        );
    }
}

/// A flipped byte anywhere in the body — any region's last byte, or padding —
/// still opens, and fails `verify_body`.
#[test]
fn every_body_region_and_its_padding_is_inside_the_body_checksum() {
    let dir = tempfile::tempdir().unwrap();
    for (s, path) in written_shapes(dir.path()) {
        let (label, schema) = (s.label, s.written.schema());
        let base = std::fs::read(&path).unwrap();
        assert_eq!(
            MappedShard::open(&path, schema).unwrap().verify_body(),
            Ok(()),
            "{label}"
        );
        let spans = spans_of(&base);
        // The byte before the first region that starts past the end of what precedes it.
        let ends = std::iter::once(desc_len(schema.num_payload_cols())).chain(spans.iter().map(|s| s.off + s.size));
        let padded = ends.zip(&spans).find(|(end, s)| s.off > *end);
        let pad = padded.unwrap_or_else(|| panic!("{label}: no region is padded")).1.off - 1;
        let targets = spans
            .iter()
            .enumerate()
            .filter(|(_, s)| s.size > 0)
            .map(|(i, s)| (format!("region {i}"), s.off + s.size - 1))
            .chain([("padding".to_owned(), pad)]);
        for (what, at) in targets {
            let shard = open_patched(&path, schema, &base, |d| d[at] ^= 0x01).unwrap();
            assert_eq!(
                shard.verify_body(),
                Err(StorageError::Corrupt("body checksum")),
                "{label}: {what}",
            );
        }
    }
}

/// A directory size a byte off what the row count and encoding determine is
/// refused, for every fixed region of every shape. A size that pushes the
/// regions after it past the end of the file is refused there first.
#[test]
fn a_region_size_that_disagrees_with_the_row_count_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    for (s, path) in written_shapes(dir.path()) {
        let schema = s.written.schema();
        let base = std::fs::read(&path).unwrap();
        for region in REG_PK..REG_PAYLOAD_START + schema.num_payload_cols() {
            let Span { size: sz, encoding: enc, .. } = spans_of(&base)[region];
            for delta in [-1isize, 1] {
                let forged = sz.checked_add_signed(delta).unwrap();
                let got = open_patched_restamped(&path, schema, &base, |d| patch_entry(d, region, |e| e.size = forged));
                assert!(
                    matches!(got, Err(StorageError::Corrupt("region size" | "region past the end"))),
                    "{}: region {region} ({enc:?}) size {sz}{delta:+}: {:?}",
                    s.label,
                    got.err(),
                );
            }
        }
    }
}

// -----------------------------------------------------------------------
// Benchmarks
// -----------------------------------------------------------------------

/// Per-pass cost of slicing a packed shard window by window: a framed column,
/// and a column holding a value in one row of ten as a frame over its zeroes
/// and as its non-NULL cells alone.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn for_slice_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const N: usize = 1_000_000;
    const WINDOW: usize = 1024;
    const HANDLES: usize = 20;
    let nullable = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    fn value(i: usize) -> Option<u128> {
        Some(3_000_000_000 + i as u128)
    }
    fn tenth(i: usize) -> Option<u128> {
        value(i).filter(|_| i.is_multiple_of(10))
    }
    type Value = fn(usize) -> Option<u128>;
    let shapes: [(&str, SchemaDescriptor, Value); 3] = [
        ("framed", make_schema_u64_i64(), value),
        ("a value in one row of ten, framed", make_schema_u64_i64(), |i| {
            tenth(i).or(Some(0))
        }),
        ("a value in one row of ten, the rest NULL", nullable, tenth),
    ];
    let dir = tempfile::tempdir().unwrap();
    let (cycles, instructions) = (Counter::cycles().unwrap(), Counter::instructions().unwrap());
    for (label, schema, value) in shapes {
        let batch = build(schema, N, |b, i| {
            b.begin_row(i as u128, 1);
            b.put_opt_int(value(i));
        });
        let path = write(dir.path(), "for_slice.db", &batch);
        let handles: Vec<MappedShard> = (0..HANDLES)
            .map(|_| MappedShard::open(&path, &schema).unwrap())
            .collect();
        assert!(matches!(handles[0].col_regions[0], PayloadRegion::Packed(_)));
        let slice_all = |shard: &MappedShard| {
            for start in (0..N).step_by(WINDOW) {
                black_box(shard.slice_to_owned_batch(start, WINDOW.min(N - start)));
            }
        };
        println!("{label}: {} bytes", handles[0].file_len());
        for pass in ["first pass", "second pass"] {
            let (((), i), c) = cycles.measure(|| instructions.measure(|| handles.iter().for_each(slice_all)));
            println!(
                "  {pass}: {} cycles, {} instructions per pass",
                c / HANDLES as u64,
                i / HANDLES as u64,
            );
        }
        std::fs::remove_file(&path).unwrap();
    }
}

/// Instructions per row to read a weight and a null word through the per-row
/// accessors, and to decode both regions in one whole slice, by the encoding
/// the two regions take.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn word_region_bench() {
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const N: usize = 1_000_000;
    let schema = u64_pk_schema(SchemaColumn::new(TypeCode::I64, true));
    type Row = fn(usize) -> (i64, Option<u128>);
    let shapes: [(&str, Row); 3] = [
        ("constant", |i| (1, Some(i as u128))),
        ("two-value", |i| {
            (if i % 3 == 0 { -1 } else { 1 }, (i % 5 != 0).then_some(i as u128))
        }),
        ("for", |i| ((i % 7) as i64 + 1, (i % 5 != 0).then_some(i as u128))),
    ];
    let dir = tempfile::tempdir().unwrap();
    let instructions = Counter::instructions().unwrap();
    for (label, row) in shapes {
        let batch = build(schema, N, |b, i| {
            let (weight, value) = row(i);
            b.begin_row(i as u128, weight);
            b.put_opt_int(value);
        });
        let path = write(dir.path(), "words.db", &batch);
        let shard = MappedShard::open(&path, &schema).unwrap();
        let (sum, per_row) = instructions.measure(|| {
            let mut sum = 0u64;
            for r in 0..N {
                sum = sum.wrapping_add(shard.get_weight(black_box(r)) as u64 ^ shard.get_null_word(r));
            }
            sum
        });
        black_box(sum);
        let (_, slice) = instructions.measure(|| black_box(shard.slice_to_owned_batch(0, N)));
        println!(
            "{label} weight: {:.2} instructions per row for both per-row reads, {:.2} for a whole slice",
            per_row as f64 / N as f64,
            slice as f64 / N as f64,
        );
        std::fs::remove_file(&path).unwrap();
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
    let batch = build(schema, N, |b, i| {
        b.begin_row(i as u128, 1);
        b.put_int((i % 1000) as u128);
        b.put_int(3 * i as u128);
    });
    let path = write(dir.path(), "point.db", &batch);
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
    // Every block: the second pass is the read of a block already held.
    for pass in ["every row, first pass", "every row, second pass"] {
        let ((), i) = instructions.measure(|| {
            for row in 0..N {
                for pi in 0..2 {
                    black_box(shard.get_col_ptr(black_box(row), pi, 8));
                }
            }
        });
        println!("{pass}: {} instructions per read", i / (2 * N as u64));
    }
}

/// What one shard of each string shape costs on disk, to write and to read
/// back. Per row: the file's bytes, then instructions to write it, to slice it
/// whole and in 1024-row windows, to read every string cell through the per-row
/// accessor, and to materialize it through a `UnifiedSet` as a compaction does.
#[test]
#[ignore = "benchmark; run with --release --ignored --nocapture --test-threads=1"]
fn shard_string_footprint_bench() {
    use crate::test_support::Rng;
    use gnitz_foundation::perf::Counter;
    use std::hint::black_box;
    const N: usize = 200_000;
    let str_col = SchemaColumn::new(TypeCode::String, false);
    let events = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            str_col,
            str_col,
            str_col,
            str_col,
            SchemaColumn::new(TypeCode::I64, false),
        ],
        &[0],
    );
    let one_string = make_schema_pk_u64_payload_string();
    let nullable_string = u64_pk_schema(SchemaColumn::new(TypeCode::String, true));

    let tenants: Vec<String> = (0..50).map(|i| format!("tenant-{i:03}")).collect();
    let urls: Vec<String> = (0..2_000)
        .map(|i| format!("https://app.example.com/api/v2/resources/{:09}/items/{i:05}", i * 7919))
        .collect();
    let agents: Vec<String> = (0..30)
        .map(|i| format!("Mozilla/5.0 (X11; Linux x86_64; rv:{i}.0) Gecko/20100101 Firefox/{i}.0 build-{i:04}"))
        .collect();
    let statuses = ["ok", "ok", "ok", "client_error", "server_error_upstream_timeout"];

    let mut rng = Rng::new(7);
    let mut pick = |n: usize| rng.gen_range(n as u64) as usize;
    let shapes: Vec<(&str, Batch)> = vec![
        (
            "four repeating columns (50 inline, 2000 long, 30 long, 3 mixed) and an i64",
            build(events, N, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                b.put_string(&tenants[pick(50)]);
                b.put_string(&urls[pick(2_000)]);
                b.put_string(&agents[pick(30)]);
                b.put_string(statuses[pick(5)]);
                b.put_int(pick(1000) as u128);
            }),
        ),
        (
            "one column, distinct 60-byte values",
            build(one_string, N, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                b.put_string(&wide_string(i, 60));
            }),
        ),
        (
            "one column, distinct inline values",
            build(one_string, N, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                b.put_string(&format!("r{i:09}"));
            }),
        ),
        (
            "one nullable column, distinct inline values, one row in twenty NULL",
            build(nullable_string, N, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                match i % 20 {
                    7 => b.put_null(),
                    _ => b.put_string(&format!("r{i:09}")),
                }
            }),
        ),
        (
            "one nullable column, distinct 60-byte values, one row in twenty NULL",
            build(nullable_string, N, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                match i % 20 {
                    7 => b.put_null(),
                    _ => b.put_string(&wide_string(i, 60)),
                }
            }),
        ),
        (
            "one column, 60-byte values drawn from N/2",
            build(one_string, N, |b, i| {
                b.begin_row(i as u128 + 1, 1);
                b.put_string(&wide_string(pick(N / 2), 60));
            }),
        ),
    ];

    let instructions = Counter::instructions().unwrap();
    let dir = tempfile::tempdir().unwrap();
    for (si, (label, batch)) in shapes.iter().enumerate() {
        let schema = *batch.schema();
        let name = format!("{si}.db");
        let (path, write_i) = instructions.measure(|| write(dir.path(), &name, batch));
        let image = std::fs::read(&path).unwrap();
        let regions: Vec<String> = (0..=num_regions(schema.num_payload_cols()))
            .map(|r| {
                let Span { size, encoding, .. } = spans_of(&image)[r];
                format!("{size}/{encoding:?}")
            })
            .collect();
        let shard = MappedShard::open(&path, &schema).unwrap();
        let (_, slice_i) = instructions.measure(|| black_box(shard.slice_to_owned_batch(0, N)));
        let (_, window_i) = instructions.measure(|| {
            for start in (0..N).step_by(1024) {
                black_box(shard.slice_to_owned_batch(start, 1024.min(N - start)));
            }
        });
        let (content, cell_i) = instructions.measure(|| {
            let mut content = 0usize;
            for row in 0..N {
                for (pi, col) in schema.payload_columns() {
                    if col.type_code.is_german_string() {
                        content += gnitz_expr::payload_bytes(&shard, row, pi).len();
                    }
                }
            }
            content
        });
        let rows: Vec<(u32, u32, i64)> = (0..N as u32).map(|r| (0, r, 1)).collect();
        let (_, merge_i) = instructions.measure(|| {
            let set = UnifiedSet::whole(std::slice::from_ref(&shard), &schema);
            black_box(set.materialize(&rows, N))
        });
        let per_row = |i: u64| i / N as u64;
        println!(
            "{label}: {N} rows of {content} string bytes\n  {} bytes, {:.1} per row; instructions per row: \
             write {}, slice {}, windows {}, cells {}, materialize {}\n  regions {}",
            image.len(),
            image.len() as f64 / N as f64,
            per_row(write_i),
            per_row(slice_i),
            per_row(window_i),
            per_row(cell_i),
            per_row(merge_i),
            regions.join(" "),
        );
    }
}
