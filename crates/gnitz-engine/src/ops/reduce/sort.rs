//! Group-column comparator and PK-ordered argsort used by the reduce operator.

use std::cmp::Ordering;

use crate::schema::{key::PkSortKey, SchemaDescriptor, TypeCode, PAYLOAD_MAPPING_PK_SENTINEL};
use crate::storage::{cmp_col_window, compare_pk_bytes, Batch, ColumnarSource, MemBatch};

/// Pre-computed per-column sort descriptor to avoid repeated schema lookups.
///
/// Four bytes per entry, no padding (all fields are u8-sized).
/// A stack array of MAX_COLUMNS = 65 entries is 260 bytes — well within five cache lines.
#[derive(Clone, Copy)]
pub(crate) struct SortDesc {
    pub(crate) pi: u8,
    pub(crate) cs: u8,
    pub(crate) tc: TypeCode,
    /// Byte offset of the addressed PK column within the row's PK region.
    /// Meaningful only when `pi == PAYLOAD_MAPPING_PK_SENTINEL`; zero otherwise.
    pub(crate) pk_off: u8,
}

/// Compare two rows by group columns using a pre-computed SortDesc array.
/// Generic over two [`ColumnarSource`]s, so an intra-batch argsort compare and
/// the trace-cursor-vs-exemplar group-membership test share this one body.
pub(super) fn compare_by_group_cols<A: ColumnarSource, B: ColumnarSource>(
    src_a: &A,
    row_a: usize,
    src_b: &B,
    row_b: usize,
    descs: &[SortDesc],
) -> Ordering {
    let a_null_word = src_a.get_null_word(row_a);
    let b_null_word = src_b.get_null_word(row_b);

    for desc in descs {
        if desc.pi == PAYLOAD_MAPPING_PK_SENTINEL {
            // Isolate the addressed PK column's byte window. Comparing the
            // whole PK region (the previous `get_pk(row)` widen) splits
            // compound-PK groups that share the addressed column but differ
            // in other PK columns.
            // PK region holds OPK bytes, which are order-preserving — compare
            // them raw. (A `cmp_typed_le` LE decode would invert order for
            // big-endian/sign-flipped OPK bytes.)
            let off = desc.pk_off as usize;
            let cs = desc.cs as usize;
            let a = &src_a.get_pk_bytes(row_a)[off..off + cs];
            let b = &src_b.get_pk_bytes(row_b)[off..off + cs];
            let ord = a.cmp(b);
            if ord != Ordering::Equal {
                return ord;
            }
            continue;
        }

        let pi = desc.pi as usize;

        // NULL is never set on non-nullable columns, so the bit is always 0
        // there and this branch is harmless. NULLs sort before non-NULLs
        // (NULLS FIRST), so all NULLs are adjacent and form a single group.
        let a_is_null = (a_null_word >> pi) & 1 != 0;
        let b_is_null = (b_null_word >> pi) & 1 != 0;
        match (a_is_null, b_is_null) {
            (true, true) => continue,
            (true, false) => return Ordering::Less,
            (false, true) => return Ordering::Greater,
            (false, false) => {}
        }

        // SortDesc.cs is already tc.stride() = 16 for STRING/BLOB (build_sort_descs),
        // so a single `cs`-wide read feeds both the German-string content compare
        // and the fixed-width path; each source's blob arena backs its own row's
        // string tail.
        let cs = desc.cs as usize;
        let a = src_a.get_col_ptr(row_a, pi, cs);
        let b = src_b.get_col_ptr(row_b, pi, cs);
        let ord = cmp_col_window(a, src_a.blob_slice(), b, src_b.blob_slice(), desc.tc as u8);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    Ordering::Equal
}

/// Build the SortDesc array for a given schema and group_by_cols slice.
/// Returns (array, length); only `&array[..length]` is valid.
pub(super) fn build_sort_descs(
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> ([SortDesc; crate::schema::MAX_COLUMNS], usize) {
    let mut arr = [SortDesc {
        pi: 0,
        cs: 0,
        tc: TypeCode::U64,
        pk_off: 0,
    }; crate::schema::MAX_COLUMNS];
    for (i, &c_idx_u32) in group_by_cols.iter().enumerate() {
        let c_idx = c_idx_u32 as usize;
        let tc = TypeCode::from_validated_u8(schema.columns[c_idx].type_code);
        let pi = schema.payload_mapping_byte(c_idx);
        let pk_off = if pi == PAYLOAD_MAPPING_PK_SENTINEL {
            schema.pk_byte_offset(c_idx)
        } else {
            0
        };
        arr[i] = SortDesc {
            pi,
            cs: tc.stride(),
            tc,
            pk_off,
        };
    }
    (arr, group_by_cols.len())
}

/// Argsort delta batch by group columns. `descs` is the pre-built descriptor
/// slice for `group_by_cols` (the reduce plan's baked `sort_descs`).
pub(super) fn argsort_delta(
    batch: &Batch,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
    descs: &[SortDesc],
) -> Vec<u32> {
    let mb = batch.as_mem_batch();
    let n = batch.count;
    if n <= 1 {
        return (0..n as u32).collect();
    }

    // packed_sort fast path: non-nullable single non-PK col of a packable
    // int type. Reads via `try_payload_idx` (PK-guarded) and stores keys in a
    // dense Vec so the comparator skips the per-call TypeCode dispatch.
    // NULL stores as zero bytes — would interleave with integer 0, so
    // nullable falls through to compare_by_group_cols below.
    if group_by_cols.len() == 1
        && !schema.is_pk_col(group_by_cols[0] as usize)
        && schema.columns[group_by_cols[0] as usize].nullable == 0
    {
        let ci = group_by_cols[0] as usize;
        let tc = TypeCode::from_validated_u8(schema.columns[ci].type_code);
        let pi = schema
            .try_payload_idx(ci)
            .expect("non-PK: guarded by !is_pk_col on the fast-path entry above");
        macro_rules! packed_sort {
            ($T:ty, $stride:expr) => {{
                let keys: Vec<$T> = (0..n)
                    .map(|i| {
                        let ptr = mb.get_col_ptr(i, pi, $stride);
                        <$T>::from_le_bytes(ptr.try_into().unwrap())
                    })
                    .collect();
                let mut indices: Vec<u32> = (0..n as u32).collect();
                indices.sort_unstable_by_key(|&i| keys[i as usize]);
                return indices;
            }};
        }
        match tc {
            TypeCode::I64 => packed_sort!(i64, 8),
            TypeCode::U64 => packed_sort!(u64, 8),
            TypeCode::I32 => packed_sort!(i32, 4),
            TypeCode::U32 => packed_sort!(u32, 4),
            _ => {}
        }
    }

    let mut indices: Vec<u32> = (0..n as u32).collect();
    indices.sort_unstable_by(|&a, &b| compare_by_group_cols(&mb, a as usize, &mb, b as usize, descs));
    indices
}

/// Sort `indices` by a width-matched `PkSortKey` (materialised once per row). The
/// key is the whole OPK image, so the compare is exact; rows sharing a PK keep
/// arbitrary relative order (the reduce groups them regardless). Keys are read by
/// reference (not `sort_unstable_by_key`, which would re-copy the 32-byte
/// `[u128; 2]` key per comparison).
fn sort_indices_keyed<K: PkSortKey>(mb: &MemBatch, indices: &mut [u32]) {
    let keys: Vec<K> = (0..mb.count).map(|i| K::from_opk(mb.get_pk_bytes(i))).collect();
    indices.sort_unstable_by(|&a, &b| keys[a as usize].cmp(&keys[b as usize]));
}

/// Sort `indices` into canonical PK order. `pk_stride` selects the width-matched
/// key — `u64`/`u128`/`[u128; 2]` for strides ≤8/≤16/≤32 (the cutoffs are the key
/// widths) — each the full OPK image, so a plain key compare is exact for unsigned,
/// signed, and compound PKs alike. PKs too wide to pack (`> 32` B — exotic 3–5
/// wide-column composites) byte-walk the OPK regions via `compare_pk_bytes`. Rows
/// sharing a PK keep arbitrary relative order (the reduce groups them regardless).
fn sort_indices_by_pk(mb: &MemBatch, indices: &mut [u32]) {
    match mb.pk_stride as usize {
        0..=8 => sort_indices_keyed::<u64>(mb, indices),
        9..=16 => sort_indices_keyed::<u128>(mb, indices),
        17..=32 => sort_indices_keyed::<[u128; 2]>(mb, indices),
        _ => indices
            .sort_unstable_by(|&a, &b| compare_pk_bytes(mb.get_pk_bytes(a as usize), mb.get_pk_bytes(b as usize))),
    }
}

/// Argsort indices into canonical PK order (`compare_pk_bytes` order).
pub(super) fn argsort_pk_canonical(mb: &MemBatch) -> Vec<u32> {
    let mut idx: Vec<u32> = (0..mb.count as u32).collect();
    sort_indices_by_pk(mb, &mut idx);
    idx
}

#[cfg(test)]
mod tests {
    use super::argsort_pk_canonical;
    use crate::schema::{type_code, SchemaColumn, SchemaDescriptor};
    use crate::storage::{compare_pk_bytes, Batch};

    /// One batch row per supplied OPK byte vector (each must be `pk_stride` bytes),
    /// weight 1, null word 0, one zeroed I64 payload column — all the PK-sort paths
    /// need (they read only `pk_stride` + `get_pk_bytes`). Shared by the tests and
    /// the bench.
    fn build_pk_batch(schema: &SchemaDescriptor, pk_rows: &[Vec<u8>]) -> Batch {
        let mut b = Batch::with_schema(*schema, pk_rows.len().max(1));
        for pk in pk_rows {
            b.extend_pk_bytes(pk);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &0i64.to_le_bytes());
            b.count += 1;
        }
        b
    }

    fn schema_single_pk(pk_tc: u8) -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[SchemaColumn::new(pk_tc, 0), SchemaColumn::new(type_code::I64, 0)],
            &[0],
        )
    }

    fn schema_3xu64_pk() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1, 2],
        )
    }

    /// `argsort_pk_canonical` must reproduce the authoritative `compare_pk_bytes`
    /// order. PKs are distinct, so the (unstable) sort yields a unique order and the
    /// ordered key bytes compare exactly.
    fn assert_canonical_order(schema: &SchemaDescriptor, pk_rows: &[Vec<u8>]) {
        let mut want: Vec<u32> = (0..pk_rows.len() as u32).collect();
        want.sort_by(|&a, &b| compare_pk_bytes(&pk_rows[a as usize], &pk_rows[b as usize]));
        let want_keys: Vec<&[u8]> = want.iter().map(|&i| pk_rows[i as usize].as_slice()).collect();

        let batch = build_pk_batch(schema, pk_rows);
        let mb = batch.as_mem_batch();
        let got = argsort_pk_canonical(&mb);
        let got_keys: Vec<&[u8]> = got.iter().map(|&i| mb.get_pk_bytes(i as usize)).collect();
        assert_eq!(got_keys, want_keys, "argsort_pk_canonical order mismatch");
    }

    #[test]
    fn argsort_u64_arm_full_and_subwidth() {
        // stride 8 (u64 arm): high-bit / signed-OPK byte shapes must order by raw
        // unsigned byte compare (the OPK sign-flip lives in the bytes).
        assert_canonical_order(
            &schema_single_pk(type_code::U64),
            &[
                vec![0x80, 0, 0, 0, 0, 0, 0, 1],
                vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfd],
                vec![0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
                vec![0, 0, 0, 0, 0, 0, 0, 5],
                vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
            ],
        );
        // stride 4 (u64 arm, sub-width): left-align into the high bytes preserves order.
        assert_canonical_order(
            &schema_single_pk(type_code::U32),
            &[
                vec![0xff, 0, 0, 1],
                vec![0, 0, 0, 9],
                vec![0x80, 0, 0, 0],
                vec![0, 0, 0, 1],
            ],
        );
    }

    #[test]
    fn argsort_u128_arm() {
        let mk = |hi: u8, lo: u8| {
            let mut v = vec![0u8; 16];
            v[0] = hi;
            v[15] = lo;
            v
        };
        assert_canonical_order(
            &schema_single_pk(type_code::U128),
            &[mk(0xff, 2), mk(0, 9), mk(0x80, 1), mk(0, 1), mk(0xff, 1)],
        );
    }

    #[test]
    fn argsort_u128x2_arm_with_leading16_collision() {
        // stride 24 ([u128;2] arm). Rows sharing their leading 16 bytes and differing
        // only in the tail MUST be ordered by the second limb — a bare u128 prefix
        // would tie them and risk mis-merging distinct compound PKs (weight corruption).
        let shared = [0xab_u8; 16];
        let mk_tail = |tail: u64| {
            let mut v = Vec::with_capacity(24);
            v.extend_from_slice(&shared);
            v.extend_from_slice(&tail.to_be_bytes());
            v
        };
        let mk = |a: u64, b: u64, c: u64| {
            let mut v = Vec::with_capacity(24);
            v.extend_from_slice(&a.to_be_bytes());
            v.extend_from_slice(&b.to_be_bytes());
            v.extend_from_slice(&c.to_be_bytes());
            v
        };
        assert_canonical_order(
            &schema_3xu64_pk(),
            &[
                mk_tail(7),
                mk(0, 0, 0),
                mk_tail(2),
                mk(0xffff_ffff_ffff_ffff, 0, 0),
                mk_tail(5),
            ],
        );
    }

    /// Regression guard — time `argsort_pk_canonical` over a shuffled ~1M-row batch
    /// at each keyed arm. `#[ignore]`; run release:
    ///   cargo test -p gnitz-engine --release reduce_sort -- --ignored --nocapture --test-threads=1
    #[test]
    #[ignore]
    fn reduce_sort_argsort_bench() {
        let n = 1_000_000usize;
        for &stride in &[8usize, 16, 24] {
            let schema = match stride {
                8 => schema_single_pk(type_code::U64),
                16 => schema_single_pk(type_code::U128),
                _ => schema_3xu64_pk(),
            };
            let rows: Vec<Vec<u8>> = (0..n)
                .map(|i| {
                    let mut v = vec![0u8; stride];
                    let mut chunk = 0usize;
                    while chunk * 8 < stride {
                        let seed = (i as u64)
                            .wrapping_add((chunk as u64).wrapping_mul(0x1000))
                            .wrapping_mul(0x9E37_79B9_7F4A_7C15);
                        let start = chunk * 8;
                        let end = (start + 8).min(stride);
                        v[start..end].copy_from_slice(&seed.to_be_bytes()[..end - start]);
                        chunk += 1;
                    }
                    v
                })
                .collect();
            let batch = build_pk_batch(&schema, &rows);
            let mb = batch.as_mem_batch();

            let t = std::time::Instant::now();
            let idx = argsort_pk_canonical(&mb);
            let dt = t.elapsed();
            std::hint::black_box(&idx);

            let mrps = n as f64 / dt.as_secs_f64() / 1e6;
            println!("argsort stride {stride}: {n} rows in {dt:?} = {mrps:.1} M rows/s");
        }
    }
}
