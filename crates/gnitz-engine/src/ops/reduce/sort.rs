//! Group-column comparator, argsort, and PK-ordered sort used by reduce/gather.

use std::cmp::Ordering;

use crate::schema::{SchemaDescriptor, TypeCode, PAYLOAD_MAPPING_PK_SENTINEL};
use crate::storage::{
    Batch, MemBatch,
    compare_rows, compare_rows_int_nonnull, schema_is_int_nonnull,
    scatter_copy, write_to_batch,
};

use super::super::util::cmp_typed_le;

/// Pre-computed per-column sort descriptor to avoid repeated schema lookups.
///
/// Five bytes per entry, no padding (all fields are u8-sized).
/// A stack array of MAX_COLUMNS = 65 entries is 325 bytes — well within five cache lines.
#[derive(Clone, Copy)]
pub(super) struct SortDesc {
    pub(super) pi: u8,
    pub(super) cs: u8,
    pub(super) tc: TypeCode,
    pub(super) c_idx: u8,
    /// Byte offset of the addressed PK column within the row's PK region.
    /// Meaningful only when `pi == PAYLOAD_MAPPING_PK_SENTINEL`; zero otherwise.
    pub(super) pk_off: u8,
}

/// Compare two rows by group columns using pre-computed SortDesc array.
pub(super) fn compare_by_group_cols(
    mb: &MemBatch,
    row_a: usize,
    row_b: usize,
    descs: &[SortDesc],
) -> Ordering {
    let a_null_word = mb.get_null_word(row_a);
    let b_null_word = mb.get_null_word(row_b);

    for desc in descs {
        if desc.pi == PAYLOAD_MAPPING_PK_SENTINEL {
            // Isolate the addressed PK column's byte window. Comparing the
            // whole PK region (the previous `mb.get_pk(row)` widen) splits
            // compound-PK groups that share the addressed column but differ
            // in other PK columns.
            let off = desc.pk_off as usize;
            let cs = desc.cs as usize;
            let a = &mb.get_pk_bytes(row_a)[off..off + cs];
            let b = &mb.get_pk_bytes(row_b)[off..off + cs];
            let ord = cmp_typed_le(a, b, desc.tc);
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

        let ord = if desc.tc == TypeCode::String {
            let a_bytes = mb.get_col_ptr(row_a, pi, 16);
            let b_bytes = mb.get_col_ptr(row_b, pi, 16);
            crate::schema::compare_german_strings(a_bytes, mb.blob, b_bytes, mb.blob)
        } else if desc.tc == TypeCode::Blob {
            unreachable!("BLOB columns are not valid group-by keys")
        } else {
            let cs = desc.cs as usize;
            let a = mb.get_col_ptr(row_a, pi, cs);
            let b = mb.get_col_ptr(row_b, pi, cs);
            cmp_typed_le(a, b, desc.tc)
        };
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
        pi: 0, cs: 0, tc: TypeCode::U64, c_idx: 0, pk_off: 0,
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
        arr[i] = SortDesc { pi, cs: tc.stride(), tc, c_idx: c_idx as u8, pk_off };
    }
    (arr, group_by_cols.len())
}

/// Argsort delta batch by group columns.
pub(super) fn argsort_delta(
    batch: &Batch,
    schema: &SchemaDescriptor,
    group_by_cols: &[u32],
) -> Vec<u32> {
    let mb = batch.as_mem_batch();
    let n = batch.count;
    if n <= 1 {
        return (0..n as u32).collect();
    }

    // packed_sort fast path: non-nullable single non-PK col of a packable
    // int type. Reads via `payload_idx` (PK-invalid) and stores keys in a
    // dense Vec so the comparator skips the per-call TypeCode dispatch.
    // NULL stores as zero bytes — would interleave with integer 0, so
    // nullable falls through to compare_by_group_cols below.
    if group_by_cols.len() == 1
        && !schema.is_pk_col(group_by_cols[0] as usize)
        && schema.columns[group_by_cols[0] as usize].nullable == 0
    {
        let ci = group_by_cols[0] as usize;
        let tc = TypeCode::from_validated_u8(schema.columns[ci].type_code);
        let pi = schema.payload_idx(ci);
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
    let (sort_descs, len) = build_sort_descs(schema, group_by_cols);
    let descs = &sort_descs[..len];
    indices.sort_unstable_by(|&a, &b| {
        compare_by_group_cols(&mb, a as usize, b as usize, descs)
    });
    indices
}

/// Argsort indices into canonical PK order — `pk_indices` order as defined
/// by `compare_pk_bytes`. Routes through `make_slow_pk_cmp` for signed
/// single-col PKs, float single-col PKs, and compound PKs; `pk_is_fast`
/// schemas (single-col unsigned U8..U64/U128/UUID) collapse to a tight
/// `u128.cmp` on the widened PK. Caller must ensure `pk_stride ∈ {8, 16}`
/// (so `mb.get_pk` is well-defined).
pub(super) fn argsort_pk_canonical(mb: &MemBatch, schema: &SchemaDescriptor) -> Vec<u32> {
    let n = mb.count;
    // Materialise the u128 PK keys once: get_pk widens via widen_pk_le on
    // every call (stride match + bounds-checked sub-slice), so caching the
    // keys avoids that overhead inside the sort comparator. 16n bytes is
    // bounded (1.6 MB at n=100k) and lives only for the duration of this
    // argsort.
    let pks: Vec<u128> = (0..n).map(|i| mb.get_pk(i)).collect();
    let mut idx: Vec<u32> = (0..n as u32).collect();
    if schema.pk_is_fast() {
        // Single-col unsigned PK: u128 ascending == canonical unsigned
        // ascending. No dispatch needed.
        idx.sort_unstable_by_key(|&i| pks[i as usize]);
    } else {
        // Signed / float / compound: route via the typed comparator. The
        // closure is `Copy` so it inlines cleanly through sort_unstable_by.
        let pk_cmp = crate::storage::make_slow_pk_cmp(schema);
        idx.sort_unstable_by(|&a, &b| pk_cmp(pks[a as usize], pks[b as usize]));
    }
    idx
}

/// Sort `indices` by `pk_cmp(pks[i], pks[j])`, tie-breaking on `row_cmp`.
/// Generic over both closure types so the comparator monomorphises and
/// inlines through `sort_unstable_by` — load-bearing for sort speed.
fn sort_indices_by_pk_then_row<PkCmp, RowCmp>(
    indices: &mut [u32],
    pks: &[u128],
    pk_cmp: PkCmp,
    row_cmp: RowCmp,
) where
    PkCmp: Fn(u128, u128) -> Ordering + Copy,
    RowCmp: Fn(usize, usize) -> Ordering + Copy,
{
    indices.sort_unstable_by(|&a, &b| match pk_cmp(pks[a as usize], pks[b as usize]) {
        Ordering::Equal => row_cmp(a as usize, b as usize),
        ord => ord,
    });
}

/// Sort batch by (PK, payload) without consolidation.
/// Used by op_gather_reduce where we need to see each partial separately.
pub(super) fn sort_owned(batch: &Batch, schema: &SchemaDescriptor) -> Batch {
    let n = batch.count;
    if n <= 1 || batch.sorted {
        return batch.clone_batch();
    }

    let mb = batch.as_mem_batch();

    let pks: Vec<u128> = (0..n).map(|i| mb.get_pk(i)).collect();
    let mut indices: Vec<u32> = (0..n as u32).collect();

    // Direct `u128.cmp` on the widened PK is sound only for `pk_is_fast`
    // schemas (single-col unsigned). Signed / float / compound PKs must
    // dispatch through `make_slow_pk_cmp` — otherwise negatives sort after
    // positives (signed), total_cmp ordering is skipped (float), and the
    // low-priority PK column dominates (compound), all of which make the
    // `output.sorted = true` mark below a lie.
    let slow_pk_cmp = crate::storage::make_slow_pk_cmp(schema);
    let fast_pk_cmp = |a: u128, b: u128| a.cmp(&b);
    let row_int = |a: usize, b: usize| compare_rows_int_nonnull(schema, &mb, a, &mb, b);
    let row_full = |a: usize, b: usize| compare_rows(schema, &mb, a, &mb, b);
    match (schema_is_int_nonnull(schema), schema.pk_is_fast()) {
        (true, true)   => sort_indices_by_pk_then_row(&mut indices, &pks, fast_pk_cmp, row_int),
        (true, false)  => sort_indices_by_pk_then_row(&mut indices, &pks, slow_pk_cmp, row_int),
        (false, true)  => sort_indices_by_pk_then_row(&mut indices, &pks, fast_pk_cmp, row_full),
        (false, false) => sort_indices_by_pk_then_row(&mut indices, &pks, slow_pk_cmp, row_full),
    }

    let blob_cap = mb.blob.len().max(1);
    let mut output = write_to_batch(schema, n, blob_cap, |writer| {
        scatter_copy(&mb, &indices, &[], writer);
    });
    output.sorted = true;
    output
}
