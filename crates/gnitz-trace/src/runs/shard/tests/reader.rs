use super::super::encoding::{SYMBOLS_AT, SYMBOLS_COUNT_AT};
use super::super::file::ShardWriteOpts;
use super::*;
use crate::runs::Codebook;
use crate::runs::StorageError;
use crate::runs::write_shard;
use crate::test_support::{
    certified, make_batch, make_batch_bytes, make_schema_pk_u64_payload_string, make_schema_u64_i64,
    make_schema_u128_i64, map_coded_shard, payload0_i64, pk_only_schema, prose, sweep_bit_flips, u64_pk_schema,
};
use gnitz_foundation::rng::{Rng, mix};
use gnitz_wire::{MAX_SYMBOL_LEN, write_u64_le};
use gnitz_wire::{REG_NULL_BMP, REG_PAYLOAD_START, REG_PK, REG_WEIGHT, TypeCode, num_regions};
use gnitz_zset::repr::HeapCoder;
use gnitz_zset::repr::{Batch, BatchBuilder};
use gnitz_zset::repr::{ColumnarSource, Pick, UnifiedSet};
use gnitz_zset::schema::{SchemaColumn, SchemaDescriptor};

/// Write `batch` through the production writer to `dir/name`, its heap coded
/// under a codebook trained on it where that pays.
fn write(dir: &std::path::Path, name: &str, batch: &Batch) -> String {
    let path = dir.join(name).to_str().unwrap().to_owned();
    let codebook = Codebook::for_batch(None, batch);
    let opts = ShardWriteOpts {
        codebook: codebook.as_deref(),
        ..Default::default()
    };
    write_shard(batch, &path, opts).unwrap();
    path
}

/// A `(U64 PK | I64)` shard of `rows` at weight 1 and payload `pk * mul`, at
/// `dir/test.db` — the fixture the integrity tests patch.
fn small_shard(dir: &std::path::Path, rows: u64, mul: i64) -> String {
    let rows: Vec<(u64, i64, i64)> = (1..=rows).map(|i| (i, 1, i as i64 * mul)).collect();
    write(dir, "test.db", &certified(make_batch(&make_schema_u64_i64(), &rows)))
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
    u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, false), SchemaColumn::new(tail, true)])
}

/// A `width`-byte string unique to row `i`, long enough to spill to the heap.
pub(super) fn wide_string(i: usize, width: usize) -> String {
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
                        gnitz_wire::payload_bytes(src, r, pi).to_vec()
                    } else {
                        src.get_col_ptr(r, pi, col.size()).to_vec()
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
/// `UnifiedSet` over a window, the OPK seeks and the PK filter. A relocating
/// slice of every row copies a span its cells share once, so it holds no more
/// heap than the shard's.
fn assert_reads_as(label: &str, shard: &MappedShard, want: &Batch) {
    let schema = want.schema();
    let n = want.len();
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
            let carried = shard.heap().plain().filter(|_| !relocate).map(|h| (h, h.len()));
            let b = shard.slice_to_owned_batch_with(w.start, w.len(), carried, 0);
            if relocate
                && w.len() == n
                && let Some(heap) = shard.heap().plain()
            {
                assert!(b.blob().len() <= heap.len(), "{label}: a relocated span copied twice");
            }
            assert_eq!(
                rows_of(&b.as_mem_batch(), schema),
                want_rows[w.clone()],
                "{label}: slice {w:?} relocate={relocate}"
            );
        }
    }
    for w in [0..n, mid] {
        let rows: Vec<Pick> = w.clone().map(|r| (0, r as u32, want.get_weight(r))).collect();
        let b = UnifiedSet::with(
            std::slice::from_ref(shard),
            schema,
            std::iter::once(w.clone()),
            0,
            false,
            |set| set.materialize(&rows, None),
        );
        assert_eq!(
            rows_of(&b.as_mem_batch(), schema),
            want_rows[w.clone()],
            "{label}: materialize {w:?}"
        );
    }
    for r in 0..n {
        let key = want.get_pk_bytes(r);
        assert_eq!(shard.pk_at(r).pk_bytes(), key, "{label}: key by value, row {r}");
        assert_eq!(shard.pk_rows(key), want.pk_rows(key), "{label}: rows of row {r}'s key");
        assert!(
            shard.pk_filter_may_contain(gnitz_zset::schema::probe_key(key)),
            "{label}: PK filter false negative on row {r}"
        );
        let lb = (0..n).find(|&i| want.get_pk_bytes(i) == key).unwrap();
        assert_eq!(shard.seek(key, None), lb, "{label}: lower bound row {r}");
        for hint in [0, lb] {
            assert_eq!(
                shard.seek(key, Some(hint)),
                want.seek(key, Some(hint)),
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
pub(super) fn build(schema: SchemaDescriptor, n: usize, mut row: impl FnMut(&mut BatchBuilder, usize)) -> Batch {
    let mut b = BatchBuilder::new(&schema);
    for i in 0..n {
        row(&mut b, i);
        b.end_row();
    }
    b.finish()
}

fn shapes() -> Vec<Shape> {
    use Encoding::{Constant, Dict, For, Key, Raw, Seq, Sparse, Symbols, TwoValue};
    let u64_i64 = make_schema_u64_i64();
    let nullable_i64 = u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, true)]);
    let u64_i32 = u64_pk_schema(&[SchemaColumn::new(TypeCode::I32, false)]);
    let u64_f64 = u64_pk_schema(&[SchemaColumn::new(TypeCode::F64, false)]);
    let u64_u8 = u64_pk_schema(&[SchemaColumn::new(TypeCode::U8, false)]);
    let u64_u128 = u64_pk_schema(&[SchemaColumn::new(TypeCode::U128, false)]);
    let opt_i64 = SchemaColumn::new(TypeCode::I64, true);
    let three_nullable = u64_pk_schema(&[opt_i64; 3]);
    let sparse_mix = u64_pk_schema(&[SchemaColumn::new(TypeCode::U128, true), opt_i64, opt_i64]);
    let u128_i64 = make_schema_u128_i64();
    let all_pk = pk_only_schema(&[TypeCode::U64; 3]);
    let compound = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::I16, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I64, false),
            SchemaColumn::new(TypeCode::I8, false),
        ],
        &[0, 1, 3],
    );
    let string = make_schema_pk_u64_payload_string();
    let nullable_string = u64_pk_schema(&[SchemaColumn::new(TypeCode::String, true)]);
    let str_col = SchemaColumn::new(TypeCode::String, false);
    let two_strings = u64_pk_schema(&[str_col; 2]);
    let three_strings = u64_pk_schema(&[str_col; 3]);
    let heap_of = |schema: &SchemaDescriptor| REG_PAYLOAD_START + schema.num_payload_cols();
    let narrow = || {
        build(u64_i64, 40, |b, i| {
            b.begin_row(i as u128 + 1, 1);
            b.put_int((i as i64 * 7) as u128);
        })
    };
    let wide_key = |i: usize| (u64::MAX as u128 - 2 + i as u128) * 3;

    vec![
        // One row: every region Constant-encoded.
        Shape {
            label: "all-constant",
            written: certified(make_batch(&u64_i64, &[(1, 1, 7)])),
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
                    0 => {
                        b.put_null();
                    }
                    _ => {
                        b.put_int(mix(i as u64) as i64 as u128);
                    }
                }
            }),
            reader: nullable_i64,
            encodings: vec![
                (REG_PK, Key),
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
                b.put_opt_int((i % 4 == 1).then_some(mix(i as u64) as u128));
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
            written: build(u64_i64, 2 * DECODE_BLOCK_ROWS + 17, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_int((1_000_000 + (i % 300) as i64) as u128);
            }),
            reader: u64_i64,
            encodings: vec![(REG_PAYLOAD_START, For)],
        },
        Shape {
            label: "strings, inline and heap",
            written: build(string, 24, |b, i| {
                b.begin_row(i as u128, 1);
                match i % 3 {
                    0 => {
                        b.put_string("short");
                    }
                    _ => {
                        b.put_string(&wide_string(i, 20 + i));
                    }
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
                    0 => {
                        b.put_string(&format!("s{i}"));
                    }
                    _ => {
                        b.put_string(&wide_string(i, 20 + i));
                    }
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
                    0 => {
                        b.put_null();
                    }
                    1 => {
                        b.put_string(&wide_string(i, 300));
                    }
                    v if v % 2 == 0 => {
                        b.put_string(&format!("s{i}"));
                    }
                    _ => {
                        b.put_string(&wide_string(i, 13 + i % 40));
                    }
                }
            }),
            reader: nullable_string,
            encodings: vec![(REG_PAYLOAD_START, Seq)],
        },
        // Eight weights and their image both take one 64-byte block.
        Shape {
            label: "two weights that pack to no fewer blocks",
            written: build(u64_i64, 8, |b, i| {
                b.begin_row(i as u128, if i % 2 == 0 { 1 } else { -1 });
                b.put_int(i as u128 * 0x0123_4567_89AB_CDEF);
            }),
            reader: u64_i64,
            encodings: vec![(REG_WEIGHT, Raw)],
        },
        Shape {
            label: "a NULL string in every row",
            written: build(nullable_string, 40, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_null();
            }),
            reader: nullable_string,
            encodings: vec![(REG_PAYLOAD_START, Constant)],
        },
        Shape {
            label: "one inline string in every row",
            written: build(string, 40, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_string("active");
            }),
            reader: string,
            encodings: vec![(REG_PAYLOAD_START, Constant)],
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
                    v if v % 2 == 0 => {
                        b.put_string(&format!("v{v}"));
                    }
                    v => {
                        b.put_string(&wide_string(v, 13 + v % 50));
                    }
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
                    0 => {
                        b.put_null();
                    }
                    v => {
                        b.put_string(&wide_string(v, 10 * v));
                    }
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
            label: "a coded heap under a column of distinct values",
            written: build(string, 2 * DECODE_BLOCK_ROWS + 7, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_string(&prose(i));
            }),
            reader: string,
            encodings: vec![(REG_PAYLOAD_START, Seq), (heap_of(&string), Symbols)],
        },
        // A dictionary's, a constant's and a sequence's values share one coded heap.
        Shape {
            label: "a coded heap under three images",
            written: build(three_strings, DECODE_BLOCK_ROWS + 300, |b, i| {
                b.begin_row(i as u128, 1);
                b.put_string(&prose(i % 40));
                b.put_string(&prose(1_000_000));
                b.put_string(&prose(i));
            }),
            reader: three_strings,
            encodings: vec![
                (REG_PAYLOAD_START, Dict),
                (REG_PAYLOAD_START + 1, Constant),
                (REG_PAYLOAD_START + 2, Seq),
                (heap_of(&three_strings), Symbols),
            ],
        },
        // Random letters: no table codes them to three quarters of their bytes.
        Shape {
            label: "an incompressible heap stays plain",
            written: {
                let mut rng = Rng::new(7);
                build(string, 600, |b, i| {
                    b.begin_row(i as u128, 1);
                    b.put_string(
                        &(0..40)
                            .map(|_| (b'a' + (rng.next_u64() % 26) as u8) as char)
                            .collect::<String>(),
                    );
                })
            },
            reader: string,
            encodings: vec![(REG_PAYLOAD_START, Seq), (heap_of(&string), Raw)],
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
        // Each half of the key is a field, and a packed row is past one u64.
        Shape {
            label: "u128 pk packed across decode blocks",
            written: build(u128_i64, DECODE_BLOCK_ROWS + 90, |b, i| {
                b.begin_row((1u128 << 64) - 300 + i as u128, 1);
                b.put_int(i as u128);
            }),
            reader: u128_i64,
            encodings: vec![(REG_PK, Key)],
        },
        // The first column is one value: a field of no bits.
        Shape {
            label: "3xu64 all-pk",
            written: build(all_pk, 9, |b, i| {
                b.begin_row_natives(&[1, (i / 4) as u128, (i * 7) as u128], 1);
            }),
            reader: all_pk,
            encodings: vec![(REG_PK, Key)],
        },
        // Signed columns of three widths, a PK several rows share, and a last
        // decode block of one row.
        Shape {
            label: "compound signed pk across decode blocks",
            written: build(compound, 2 * DECODE_BLOCK_ROWS + 1, |b, i| {
                let (i, lo) = (i as i128, DECODE_BLOCK_ROWS as i128);
                b.begin_row_natives(&[(i / 400 - 1) as u128, (i / 3 - lo) as u128, 0x80], 1 + i as i64 % 2);
                b.put_int(i as u128 % 3);
            }),
            reader: compound,
            encodings: vec![(REG_PK, Key)],
        },
        // Three columns whose spans a packed row does not hold.
        Shape {
            label: "3xu64 pk too wide to pack",
            written: build(all_pk, 200, |b, i| {
                let spread = |c: u64| (mix(i as u64 * 3 + c) | i as u64 >> 1 << 60) as u128;
                b.begin_row_natives(&[(i as u128) << 56, spread(1), spread(2)], 1);
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
            written: narrow(),
            reader: schema_with_appended(TypeCode::I64),
            encodings: vec![(REG_PAYLOAD_START, For)],
        },
        Shape {
            label: "widened by string",
            written: narrow(),
            reader: schema_with_appended(TypeCode::String),
            encodings: vec![(REG_PAYLOAD_START, For)],
        },
        // The region shape of a skeleton, its NULL column real: only the flag
        // makes a skeleton.
        Shape {
            label: "all-pk widened by a column",
            written: build(pk_only_schema(&[TypeCode::U64]), 1, |b, _| {
                b.begin_row(1, 1);
            }),
            reader: nullable_i64,
            encodings: vec![(REG_PK, Constant)],
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
        let want = s.written.widened_with_nulls(&s.reader, false);
        let shard = MappedShard::open(&path, &s.reader).unwrap();
        assert!(!shard.is_skeleton(), "{}", s.label);
        assert_reads_as(s.label, &shard, &want);
        let written = MappedShard::open(&path, s.written.schema()).unwrap();
        assert_reads_as(s.label, &written.rebind(&s.reader).unwrap(), &want);
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
            let schema = u64_pk_schema(&[SchemaColumn::new(tc, nullable)]);
            let mask = gnitz_wire::image_mask(schema.columns()[1].size());
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
        assert_eq!(d.rows, s.written.len(), "{}", s.label);
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

/// The schema-free directory read refuses a heap or a PK filter the open
/// refuses, with the same verdict.
#[test]
fn the_directory_refuses_the_heap_and_filter_the_open_refuses() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_u64_i64();
    let path = small_shard(dir.path(), 10, 100);
    let base = std::fs::read(&path).unwrap();
    let filter = num_regions(schema.num_payload_cols());
    let blob = filter - 1;
    let xoff = spans_of(&base)[filter].off;

    type Forge = Box<dyn Fn(&mut Vec<u8>)>;
    let encoding = |region: usize, enc: Encoding| -> Forge {
        Box::new(move |d| patch_entry(d, region, |e| e.encoding = enc as u8))
    };
    let cases: [(Forge, &str); 4] = [
        (Box::new(move |d| d[xoff + 12] ^= 0x01), "filter descriptor"),
        (encoding(blob, Encoding::Symbols), "region size"),
        (encoding(blob, Encoding::For), "encoding"),
        (encoding(filter, Encoding::Constant), "encoding"),
    ];
    for (forge, verdict) in cases {
        let opened = open_patched_restamped(&path, &schema, &base, |d| forge(d));
        assert_eq!(opened.err(), Some(StorageError::Corrupt(verdict)));
        assert_eq!(ShardDirectory::read(&path).err(), Some(StorageError::Corrupt(verdict)));
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
    let shard = MappedShard::open(&write(dir, "blocks.db", &batch), &schema).unwrap();
    assert!(matches!(shard.col_regions[0], PayloadRegion::Packed(_)));
    shard
}

/// Seeks, keys by value, a slice and a materialize decode no block; a point read
/// of a payload or a key decodes the block it lands in, once, and keeps its
/// address.
#[test]
fn a_point_read_decodes_one_block_and_keeps_its_address() {
    let dir = tempfile::tempdir().unwrap();
    let shard = packed_three_blocks(dir.path());
    assert!(matches!(shard.pk, KeyRegion::Packed(_)));
    let (n, row) = (shard.row_count(), DECODE_BLOCK_ROWS + 3);
    let key = (row as u64).to_be_bytes();

    assert_eq!(shard.seek(&key, None), row);
    assert_eq!(shard.seek(&key, Some(3)), row);
    assert_eq!(shard.pk_at(row).pk_bytes(), key);
    assert_eq!(shard.pk_rows(&key), row..row + 1);
    std::hint::black_box(shard.slice_to_owned_batch(100, n - 200, 0));
    let rows: Vec<Pick> = (0..n as u32).map(|r| (0, r, 1)).collect();
    let schema = make_schema_u64_i64();
    std::hint::black_box(UnifiedSet::with_whole(std::slice::from_ref(&shard), &schema, |set| {
        set.materialize(&rows, None)
    }));
    assert_eq!((shard.decoded_blocks(0), shard.decoded_key_blocks()), (0, 0));

    let cell = shard.get_col_ptr(row, 0, 8);
    assert_eq!(payload0_i64(&shard, row), 5_000 + (row % 200) as i64);
    assert_eq!(shard.get_col_ptr(row, 0, 8).as_ptr(), cell.as_ptr());
    assert_eq!(shard.decoded_blocks(0), 1);

    let first = shard.get_pk_bytes(row);
    assert_eq!(first, key);
    assert_eq!(shard.get_pk_bytes(row).as_ptr(), first.as_ptr());
    assert_eq!(shard.decoded_key_blocks(), 1);
    // The last block is a partial one.
    assert_eq!(shard.get_pk_bytes(n - 1), (n as u64 - 1).to_be_bytes());
    assert_eq!(shard.decoded_key_blocks(), 2);
}

// -----------------------------------------------------------------------
// Binding
// -----------------------------------------------------------------------

/// A rebound handle shares its mapping and starts undecoded.
#[test]
fn a_rebound_handle_shares_the_mapping_and_starts_undecoded() {
    let dir = tempfile::tempdir().unwrap();
    let narrow = packed_three_blocks(dir.path());
    std::hint::black_box(narrow.get_col_ptr(0, 0, 8));
    let rebound = narrow.rebind(&schema_with_appended(TypeCode::I64)).unwrap();
    assert!(std::rc::Rc::ptr_eq(&narrow.mmap, &rebound.mmap));
    assert_eq!(rebound.decoded_blocks(0), 0);
}

#[test]
fn shard_wider_than_reader_schema_opens() {
    // A crash between a checkpoint's manifest publish and its catalog write
    // leaves a shard one column wider than the catalog.
    let dir = tempfile::tempdir().unwrap();
    let wide = build(schema_with_appended(TypeCode::I64), 3, |b, i| {
        b.begin_row(i as u128 + 1, 1);
        b.put_int((i as u128 + 1) * 10);
        b.put_null();
    });
    let shard_path = write(dir.path(), "wide.db", &wide);

    let narrow = make_schema_u64_i64();
    // The surplus column is NULL in every row; its bit names no column of the
    // reader's schema, so every surface reads the narrow batch, null word and all.
    let want = rows_of(
        &certified(make_batch(&narrow, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)])).as_mem_batch(),
        &narrow,
    );
    let shard = MappedShard::open(&shard_path, &narrow).unwrap();
    assert_eq!(rows_of(&shard, &narrow), want);
    assert_eq!(
        rows_of(&shard.slice_to_owned_batch(0, 3, 0).as_mem_batch(), &narrow),
        want
    );
    // The blob region came from the *file's* index, not the reader's.
    assert_eq!(shard.heap().plain_len(), 0);
}

/// A coded heap's table is body bytes the open parses: a table its image cannot
/// hold, or one no decode may read, refuses the open.
#[test]
fn a_symbol_table_its_image_cannot_hold_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let schema = make_schema_pk_u64_payload_string();
    let batch = build(schema, 400, |b, i| {
        b.begin_row(i as u128, 1);
        b.put_string(&prose(i));
    });
    let path = write(dir.path(), "coded.db", &batch);
    let base = std::fs::read(&path).unwrap();
    let blob = num_regions(schema.num_payload_cols()) - 1;
    let heap = &spans_of(&base)[blob];
    assert_eq!(heap.encoding, Encoding::Symbols, "premise: the heap is coded");
    let (count, n) = (
        heap.off + SYMBOLS_COUNT_AT,
        gnitz_wire::read_u64_le(&base, heap.off + SYMBOLS_COUNT_AT),
    );
    let first_len = heap.off + SYMBOLS_AT + n as usize * MAX_SYMBOL_LEN;
    // (label, offset, bytes written there)
    let cases: [(&str, usize, &[u8]); 4] = [
        ("more symbols than codes", count, &256u64.to_le_bytes()),
        ("more symbols than the image", count, &u64::MAX.to_le_bytes()),
        ("an empty symbol", first_len, &[0]),
        ("a symbol past eight bytes", first_len, &[9]),
    ];
    for (label, at, bytes) in cases {
        let opened = open_patched(&path, &schema, &base, |d| {
            d[at..at + bytes.len()].copy_from_slice(bytes)
        });
        assert_eq!(opened.err(), Some(StorageError::Corrupt("region size")), "{label}");
    }
}

/// A table under a body that fails its checksum is refused, never handed to
/// the rebuild: two equal three-byte symbols are a table no trained encoder
/// writes, and one the rebuild asserts against.
#[test]
fn a_table_under_a_corrupt_body_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("s.db");
    let batch = make_batch_bytes(
        &make_schema_pk_u64_payload_string(),
        &(0..400).map(|i| (i as u64, 1, prose(i))).collect::<Vec<_>>(),
    );
    let book = Codebook::for_batch(None, &batch).unwrap();
    map_coded_shard(&path, &batch, &book);
    let mut image = std::fs::read(&path).unwrap();
    let heap = spans_of(&image)[gnitz_wire::REG_PAYLOAD_START + batch.schema().num_payload_cols()].off;
    let n = gnitz_wire::read_u64_le(&image, heap + SYMBOLS_COUNT_AT) as usize;
    let (symbols, lens) = image[heap + SYMBOLS_AT..].split_at_mut(n * MAX_SYMBOL_LEN);
    let i = lens[..n].iter().position(|&len| len >= 3).unwrap();
    assert!(lens[i + 1] >= 3, "premise: two symbols of three bytes or more");
    lens[i..i + 2].fill(3);
    let three = [symbols[i * MAX_SYMBOL_LEN..][..3].to_vec(), vec![0; MAX_SYMBOL_LEN - 3]].concat();
    symbols[i * MAX_SYMBOL_LEN..][..2 * MAX_SYMBOL_LEN].copy_from_slice(&three.repeat(2));
    std::fs::write(&path, &image).unwrap();
    let forged = MappedShard::open(path.to_str().unwrap(), batch.schema()).unwrap();
    assert!(forged.coded_heap().is_some(), "the table still parses");
    assert!(forged.codebook().is_none());
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
    assert_eq!(shard.heap().plain().map(<[u8]>::len), Some(N * W));

    let one = shard.slice_to_owned_batch(37, 1, 0);
    assert_eq!(one.blob().len(), W, "a one-row slice carries one string");

    let under = shard.slice_to_owned_batch(0, CUT - 1, 0);
    assert_eq!((under.blob().len(), under.dead_heap()), ((CUT - 1) * W, 0), "relocates");

    let at = shard.slice_to_owned_batch(0, CUT, 0);
    assert_eq!(
        (at.blob().len(), at.dead_heap()),
        (N * W, (N - CUT) * W),
        "at the cut the whole region is copied, the rows left out charged dead"
    );
    let full = shard.slice_to_owned_batch(0, N, 0);
    assert_eq!(Some(full.blob()), shard.heap().plain(), "whole shard: verbatim");
}

/// A slice that skips a string column and carries the heap whole charges
/// the skipped column's long values dead, kept rows' included: the heap's
/// dead-byte count still bounds its unreferenced bytes.
#[test]
fn a_carried_slice_charges_its_skipped_strings_dead() {
    let dir = tempfile::tempdir().unwrap();
    const N: usize = 128;
    let string = SchemaColumn::new(TypeCode::String, false);
    let schema = u64_pk_schema(&[string, string]);
    let batch = build(schema, N, |b, i| {
        b.begin_row(i as u128 + 1, 1);
        b.put_string(&wide_string(i, 64));
        b.put_string(&wide_string(i + N, 16));
    });
    let path = dir.path().join("skip.db").to_str().unwrap().to_owned();
    write_shard(&batch, &path, ShardWriteOpts::default()).unwrap();
    let shard = MappedShard::open(&path, &schema).unwrap();
    for (start, rows) in [(0, N), (0, N - 8)] {
        let b = shard.slice_to_owned_batch(start, rows, 0b10);
        assert_eq!(b.blob().len(), shard.heap().plain().unwrap().len(), "premise: carried");
        b.debug_verify_dead_heap();
    }
}

// -----------------------------------------------------------------------
// Descriptive-prefix, body and filter integrity
// -----------------------------------------------------------------------

/// Each forgery fails the open with its own verdict. `restamp` re-signs the
/// prefix digest, so a prefix forgery reaches the check under test. Among them,
/// each region role admits only certain encodings, whatever the byte means
/// elsewhere, and an empty heap read as coded holds no symbol table.
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
    let encoding =
        |region: usize, enc: u8| -> Forge { Box::new(move |d| patch_entry(d, region, |e| e.encoding = enc)) };
    let blob = filter - 1;
    let mut cases: Vec<(&str, bool, Forge, &str)> = vec![
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
            "shorter than the header",
            false,
            Box::new(|d| d.truncate(HEADER_SIZE - 1)),
            "shorter than the header",
        ),
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
        (
            "an empty heap read as coded",
            true,
            encoding(blob, Encoding::Symbols as u8),
            "region size",
        ),
    ];
    for (region, enc) in [
        (REG_PK, 0x10), // not an encoding at all
        (REG_PK, Encoding::TwoValue as u8),
        (REG_PK, Encoding::For as u8),
        (REG_PK, Encoding::Dict as u8),
        (REG_WEIGHT, Encoding::Dict as u8),
        (REG_NULL_BMP, Encoding::Dict as u8),
        (REG_PAYLOAD_START, Encoding::TwoValue as u8),
        (REG_PAYLOAD_START, Encoding::Symbols as u8),
        (blob, Encoding::For as u8),
        (blob, Encoding::Dict as u8),
        (blob, Encoding::Constant as u8),
        (filter, Encoding::Constant as u8),
        (filter, Encoding::Symbols as u8),
    ] {
        cases.push((
            "an encoding the role may not carry",
            true,
            encoding(region, enc),
            "encoding",
        ));
    }
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
    write_shard(&certified(make_batch(&schema, &[(1, 1, 7)])), &shard_path, no_filter).unwrap();
    let image = std::fs::read(&shard_path).unwrap();
    let filter = spans_of(&image).pop().unwrap();
    assert_eq!((filter.size, filter.encoding), (0, Encoding::Raw));
    let shard = MappedShard::open(&shard_path, &schema).unwrap();
    assert!(!shard.has_pk_filter());
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
/// refused, for every fixed region of every shape. A size that moves where the
/// regions after it start is refused there first.
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
                    matches!(
                        got,
                        Err(StorageError::Corrupt(
                            "region size" | "region past the end" | "directory does not span the file"
                        ))
                    ),
                    "{}: region {region} ({enc:?}) size {sz}{delta:+}: {:?}",
                    s.label,
                    got.err(),
                );
            }
        }
    }
}

/// A table read back from a shard codes every value as the encoder trained
/// over it did: what lets a merge copy a value's codes.
#[test]
fn a_table_read_back_codes_as_the_one_trained() {
    let dir = tempfile::tempdir().unwrap();
    let batch = make_batch_bytes(
        &make_schema_pk_u64_payload_string(),
        &(0..400).map(|i| (i as u64, 1, prose(i))).collect::<Vec<_>>(),
    );
    let book = Codebook::for_batch(None, &batch).expect("enough long bytes to train");
    let shard = map_coded_shard(&dir.path().join("s.db"), &batch, &book);
    let read = shard.codebook().unwrap();
    assert_eq!(read.table().digest(), book.table().digest());
    assert_eq!(read.trained_sixteenths(), book.trained_sixteenths());
    for i in 0..2000 {
        let (mut a, mut b) = (Vec::new(), Vec::new());
        book.encode_into(prose(i).as_bytes(), &mut a);
        read.encode_into(prose(i).as_bytes(), &mut b);
        assert_eq!(a, b, "value {i}");
    }
}
