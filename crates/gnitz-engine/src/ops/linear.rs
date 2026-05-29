//! Linear operators: op_filter, op_map, op_negate, op_union.
//! Also includes PK promotion for reindex (GROUP BY).

use std::cmp::Ordering;

use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
use crate::storage::{Batch, ConsolidatedBatch, MemBatch, compare_rows, compare_rows_int_nonnull, schema_is_int_nonnull};
use crate::expr::ScalarFuncKind;
use crate::xxh;


// ---------------------------------------------------------------------------
// Linear operators
// ---------------------------------------------------------------------------

/// Filter: retain rows where predicate returns true.
/// Uses contiguous-range bulk copy for efficiency.
pub fn op_filter(
    batch: &Batch,
    func: &ScalarFuncKind,
    schema: &SchemaDescriptor,
) -> Batch {
    let n = batch.count;
    if n == 0 {
        return Batch::empty_with_schema(schema);
    }

    let mb = batch.as_mem_batch();
    let mut output = Batch::with_schema(*schema, n);

    // When the schema has a STRING/BLOB column, copy 16-byte string structs
    // verbatim (no per-row relocation); offsets inside the structs stay valid
    // because both blobs are identical. No `!batch.blob.is_empty()` guard: a
    // batch whose strings are all short (≤12 bytes, stored inline) has an empty
    // blob, but the bulk copy is still correct — short strings are self-contained
    // in their struct and an empty shared blob is a no-op for the absent long
    // strings. Gating on a non-empty blob needlessly dropped all-short-string
    // batches to the slow `relocate_string_cell` path.
    let blob_passthrough = (0..schema.num_columns()).any(|ci| {
        let tc = schema.columns[ci].type_code;
        tc == type_code::STRING || tc == type_code::BLOB
    });
    if blob_passthrough {
        output.share_blob_from(batch);
    }

    func.run_filter(&mb, n, |start, end| {
        if blob_passthrough {
            output.append_batch_no_blob_reloc(batch, start, end);
        } else {
            output.append_batch(batch, start, end);
        }
    });

    // append_batch resets sorted+consolidated to false; restore them based on input.
    if ConsolidatedBatch::from_batch_ref(batch).is_some() {
        output.sorted = true;
        output.consolidated = true;
    } else {
        output.sorted = batch.sorted;
        output.consolidated = false;
    }

    gnitz_debug!("op_filter: in={} out={} func={}", n, output.count, func.kind_name());
    output
}

/// Sentinel `reindex_col` value: set the PK to a hash of the full output row
/// (all payload columns) rather than from a single source column. Used by
/// `MapKind::HashRow` for EXCEPT/INTERSECT/DISTINCT full-row set identity.
pub const HASH_ROW_REINDEX: i32 = -2;

/// Map: transform batch via scalar function.
/// If reindex_col >= 0, computes new PK from that column value (GROUP BY).
/// If reindex_col == HASH_ROW_REINDEX, sets PK = hash of the full output row.
pub fn op_map(
    batch: &Batch,
    func: &ScalarFuncKind,
    in_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    reindex_col: i32,
    branch_id: u8,
) -> Batch {
    if batch.count == 0 {
        return Batch::empty_with_schema(out_schema);
    }

    if reindex_col == HASH_ROW_REINDEX {
        // Copy-all map produced the payload; set each PK to a hash of the full
        // row so rows with identical content collide and distinct content does
        // not. Rehashing scrambles PK order, so the result is unsorted.
        let mut output = func.evaluate_map_batch(batch, out_schema);
        debug_assert_eq!(
            output.count, batch.count,
            "MAP output row count must equal input row count",
        );
        reindex_hash_row(out_schema, &mut output, branch_id);
        output.sorted = false;
        output.consolidated = false;
        gnitz_debug!("op_map: in={} out={} reindex=HASH func={}", batch.count, output.count, func.kind_name());
        return output;
    }

    if reindex_col < 0 {
        // Batch path
        let mut result = func.evaluate_map_batch(batch, out_schema);
        debug_assert_eq!(
            result.count, batch.count,
            "MAP output row count must equal input row count",
        );
        result.sorted = batch.sorted;
        gnitz_debug!("op_map: in={} out={} reindex=-1 func={}", batch.count, result.count, func.kind_name());
        return result;
    }

    // Per-row path with reindex
    let in_mb = batch.as_mem_batch();
    let ri_col = reindex_col as usize;

    // Evaluate the map batch (without reindex) to get column data
    let mut output = func.evaluate_map_batch(batch, out_schema);
    debug_assert_eq!(
        output.count, batch.count,
        "MAP output row count must equal input row count",
    );

    // Overwrite PK with promoted values from reindex column. The per-batch
    // kind dispatch is hoisted out of the row loop by `promote_into`.
    let promoter = PkPromoter::new(in_schema, ri_col);
    promoter.promote_into(&in_mb, &mut output);

    output.sorted = false;
    output.consolidated = false;
    gnitz_debug!("op_map: in={} out={} reindex={} func={}", batch.count, output.count, reindex_col, func.kind_name());
    output
}

/// Negate: flip the sign of every weight.
pub fn op_negate(batch: &Batch) -> Batch {
    if batch.count == 0 {
        return Batch::empty(batch.num_payload_cols(), batch.pk_stride());
    }

    // clone_batch copies all column regions and the blob verbatim — no
    // per-string relocation is needed since we keep the same blob content.
    let mut output = batch.clone_batch();
    // Negate weights in-place.
    let weights = output.weight_data_mut();
    for i in 0..batch.count {
        let off = i * 8;
        let w = i64::from_le_bytes(weights[off..off + 8].try_into().unwrap());
        weights[off..off + 8].copy_from_slice(&w.wrapping_neg().to_le_bytes());
    }

    // clone_batch preserves sorted and consolidated; negating weights does not change element
    // identity, so both invariants survive as-is.
    gnitz_debug!("op_negate: count={}", batch.count);
    output
}

/// Union: algebraic addition of two Z-Set streams.
/// When both inputs are sorted, performs O(N) merge preserving sort order.
pub fn op_union(
    batch_a: Batch,
    batch_b: Option<&Batch>,
    schema: &SchemaDescriptor,
) -> Batch {
    let b = match batch_b {
        Some(b) if b.count > 0 => b,
        // O(1) pass-through: no allocation, sorted/consolidated preserved.
        _ => {
            gnitz_debug!("op_union: a={} b=0 identity", batch_a.count);
            return batch_a;
        }
    };
    if batch_a.count == 0 {
        return b.clone_batch();
    }

    if batch_a.sorted && b.sorted {
        return op_union_merge(&batch_a, b, schema);
    }

    // Unsorted: concatenate
    let mut output = Batch::with_schema(*schema, batch_a.count + b.count);
    output.append_batch(&batch_a, 0, batch_a.count);
    output.append_batch(b, 0, b.count);
    output.sorted = false;
    output.consolidated = false;
    gnitz_debug!("op_union: a={} b={} out={} concat", batch_a.count, b.count, output.count);
    output
}

/// Sorted merge of two sorted batches with contiguous-run batching.
fn op_union_merge(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
) -> Batch {
    if schema_is_int_nonnull(schema) {
        op_union_merge_inner(batch_a, batch_b, schema,
            |s, a, ai, b, bi| compare_rows_int_nonnull(s, a, ai, b, bi))
    } else {
        op_union_merge_inner(batch_a, batch_b, schema,
            |s, a, ai, b, bi| compare_rows(s, a, ai, b, bi))
    }
}

#[inline]
fn op_union_merge_inner<RowCmp>(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
    row_cmp: RowCmp,
) -> Batch
where
    RowCmp: Fn(&SchemaDescriptor, &MemBatch, usize, &MemBatch, usize) -> Ordering + Copy,
{
    let n_a = batch_a.count;
    let n_b = batch_b.count;
    let mut output = Batch::with_schema(*schema, n_a + n_b);

    let mb_a = batch_a.as_mem_batch();
    let mb_b = batch_b.as_mem_batch();

    enum RunSrc {
        None,
        A { start: usize },
        B { start: usize },
    }

    let mut i = 0usize;
    let mut j = 0usize;
    let mut run_src = RunSrc::None;

    while i < n_a && j < n_b {
        // OPK bytes are order-preserving for all PK widths, so a raw byte
        // compare replaces the former `get_pk(u128)` (narrow-only) comparison.
        let ord = mb_a.get_pk_bytes(i).cmp(mb_b.get_pk_bytes(j));

        if ord == Ordering::Less {
            match run_src {
                RunSrc::A { .. } => {}
                RunSrc::B { start } => {
                    output.append_batch(batch_b, start, j);
                    run_src = RunSrc::A { start: i };
                }
                RunSrc::None => {
                    run_src = RunSrc::A { start: i };
                }
            }
            i += 1;
        } else if ord == Ordering::Greater {
            match run_src {
                RunSrc::B { .. } => {}
                RunSrc::A { start } => {
                    output.append_batch(batch_a, start, i);
                    run_src = RunSrc::B { start: j };
                }
                RunSrc::None => {
                    run_src = RunSrc::B { start: j };
                }
            }
            j += 1;
        } else {
            // Equal PKs: flush pending run, then merge-sort the equal-PK
            // sub-ranges from both batches by payload to preserve (PK, payload)
            // sort order.
            match run_src {
                RunSrc::A { start } => output.append_batch(batch_a, start, i),
                RunSrc::B { start } => output.append_batch(batch_b, start, j),
                RunSrc::None => {}
            }
            run_src = RunSrc::None;

            // Find the end of the equal-PK run in each batch (byte-wise; `i`/`j`
            // stay fixed as the reference row until `i = i_end; j = j_end`).
            let mut i_end = i + 1;
            while i_end < n_a
                && mb_a.get_pk_bytes(i_end) == mb_a.get_pk_bytes(i)
            {
                i_end += 1;
            }
            let mut j_end = j + 1;
            while j_end < n_b
                && mb_b.get_pk_bytes(j_end) == mb_b.get_pk_bytes(j)
            {
                j_end += 1;
            }

            // Merge the two equal-PK sub-ranges by payload order. Coalesces
            // contiguous single-source runs into one `append_batch` per run
            // because the row count per group is often single-digit and
            // `append_batch` has fixed per-call overhead from offset math.
            let (mut ia, mut jb) = (i, j);
            if ia < i_end && jb < j_end {
                let mut prev_a = row_cmp(schema, &mb_a, ia, &mb_b, jb)
                    != Ordering::Greater;
                let mut run_start = if prev_a { ia } else { jb };
                if prev_a { ia += 1; } else { jb += 1; }
                while ia < i_end && jb < j_end {
                    let pick_a = row_cmp(schema, &mb_a, ia, &mb_b, jb)
                        != Ordering::Greater;
                    if pick_a != prev_a {
                        if prev_a {
                            output.append_batch(batch_a, run_start, ia);
                            run_start = jb;
                        } else {
                            output.append_batch(batch_b, run_start, jb);
                            run_start = ia;
                        }
                        prev_a = pick_a;
                    }
                    if pick_a { ia += 1; } else { jb += 1; }
                }
                // Flush the in-progress run, folding its side's still-unpicked
                // tail (rows the loop never reached because the *other* side
                // exhausted first) into the same append. The opposite side
                // then drains as a separate run.
                if prev_a {
                    output.append_batch(batch_a, run_start, i_end);
                    if jb < j_end {
                        output.append_batch(batch_b, jb, j_end);
                    }
                } else {
                    output.append_batch(batch_b, run_start, j_end);
                    if ia < i_end {
                        output.append_batch(batch_a, ia, i_end);
                    }
                }
            } else {
                if ia < i_end {
                    output.append_batch(batch_a, ia, i_end);
                }
                if jb < j_end {
                    output.append_batch(batch_b, jb, j_end);
                }
            }

            i = i_end;
            j = j_end;
        }
    }

    // Flush final run
    match run_src {
        RunSrc::A { start } => output.append_batch(batch_a, start, i),
        RunSrc::B { start } => output.append_batch(batch_b, start, j),
        RunSrc::None => {}
    }

    // Remaining
    if i < n_a {
        output.append_batch(batch_a, i, n_a);
    }
    if j < n_b {
        output.append_batch(batch_b, j, n_b);
    }

    output.sorted = true;
    output.consolidated = false;
    gnitz_debug!("op_union: a={} b={} out={} sorted_merge", n_a, n_b, output.count);
    output
}

/// Null-extend: copy input batch and append N null-filled payload columns.
/// Used in LEFT JOIN decomposition to convert anti-join output (left-only rows)
/// into null-filled outer join rows.
pub fn op_null_extend(
    batch: &Batch,
    in_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    assert!(
        in_schema.num_columns() + right_schema.num_payload_cols()
            <= crate::schema::MAX_COLUMNS,
        "op_null_extend: combined column count {} + {} exceeds MAX_COLUMNS={}",
        in_schema.num_columns(), right_schema.num_payload_cols(),
        crate::schema::MAX_COLUMNS,
    );

    let in_npc = in_schema.num_payload_cols();
    let right_npc = right_schema.num_payload_cols();
    let out_npc = in_npc + right_npc;
    let n = batch.count;

    if n == 0 {
        // Output PK region is identical to the input PK, so its stride is the
        // input schema's PK stride (8 for U64, 16 for U128, etc.).
        return Batch::empty(out_npc, in_schema.pk_stride());
    }

    // Build a merged schema for the output batch.
    let mut out_columns = [SchemaColumn::new(0, 0); crate::schema::MAX_COLUMNS];
    let mut ci_out = 0;
    for ci in 0..in_schema.num_columns() {
        out_columns[ci_out] = in_schema.columns[ci];
        ci_out += 1;
    }
    for (_rpi, _ci, col) in right_schema.payload_columns() {
        out_columns[ci_out] = *col;
        ci_out += 1;
    }
    let out_schema = SchemaDescriptor::new(&out_columns[..ci_out], in_schema.pk_indices());

    let mut output = Batch::with_schema(out_schema, n);
    output.count = n;

    // Propagate the input blob so long (> 12 byte) STRING/BLOB values whose
    // 16-byte structs are copied verbatim below still resolve against the
    // shared heap. Without this they would point into an empty blob.
    let needs_blob = in_schema.payload_columns().any(|(_, _, col)| {
        col.type_code == type_code::STRING || col.type_code == type_code::BLOB
    });
    if needs_blob && !batch.blob.is_empty() {
        output.share_blob_from(batch);
    }

    // Copy system columns
    output.pk_data_mut().copy_from_slice(batch.pk_data());
    output.weight_data_mut().copy_from_slice(batch.weight_data());

    // Copy input payload columns
    for (pi, _ci, col) in in_schema.payload_columns() {
        let stride = col.size() as usize;
        output.col_data_mut(pi).copy_from_slice(&batch.col_data(pi)[..n * stride]);
    }

    // Right-side payload columns are already zero-filled by with_schema.

    // Set null bits for all appended right-side columns
    let right_null_bits = if right_npc < 64 {
        (1u64 << right_npc) - 1
    } else {
        u64::MAX
    };
    for row in 0..n {
        let off = row * 8;
        let in_null = u64::from_le_bytes(batch.null_bmp_data()[off..off + 8].try_into().unwrap());
        let out_null = super::util::merge_null_words(in_null, right_null_bits, in_npc);
        output.null_bmp_data_mut()[off..off + 8].copy_from_slice(&out_null.to_le_bytes());
    }

    output.sorted = batch.sorted;
    output.consolidated = batch.consolidated;
    gnitz_debug!("op_null_extend: in={} right_npc={}", n, right_npc);
    output
}

// ---------------------------------------------------------------------------
// PK promotion for reindex (GROUP BY)
// ---------------------------------------------------------------------------

/// Set every row's PK to a hash of its full payload content. Identical row
/// content (including null pattern and string/blob bytes) yields an identical
/// 128-bit PK; any difference yields a distinct PK. This implements full-row
/// set membership for EXCEPT/INTERSECT/DISTINCT.
///
/// The canonical byte stream per row is, for each payload column in order: a
/// 1-byte null marker, then (if non-null) the column's content — fixed-width
/// columns by their raw little-endian bytes, STRING/BLOB by length-prefixed
/// content following the heap pointer for long strings. This is independent of
/// physical inline-vs-heap string layout, so equal logical rows hash equally.
///
/// Limitation: set membership is keyed on the 128-bit hash, so two logically
/// distinct rows that collide (~2^-64 birthday-bound) would be treated as the
/// same element and silently coalesce in DISTINCT/EXCEPT/INTERSECT. This is an
/// accepted tradeoff for the synthetic-PK set-op path, not a checked error.
fn reindex_hash_row(out_schema: &SchemaDescriptor, output: &mut Batch, branch_id: u8) {
    use xxhash_rust::xxh3::Xxh3Default;
    let n = output.count;
    let mut pks: Vec<u128> = Vec::with_capacity(n);
    {
        let mb = output.as_mem_batch();
        // ~280-byte stack-allocated streaming hasher; `reset()` between rows
        // costs only a handful of word stores, and fixed-width columns are fed
        // straight from the column slot with no intermediate copy.
        let mut hasher = Xxh3Default::new();
        for row in 0..n {
            hasher.reset();
            // Branch discriminator: distinguishes identical payloads arriving on
            // the left vs right side of a UNION ALL so they do not collide to a
            // single PK (which would collapse their +2 weight to +1).
            hasher.update(&[branch_id]);
            let null_word = mb.get_null_word(row);
            for (pi, _ci, col) in out_schema.payload_columns() {
                let is_null = (null_word >> pi) & 1 != 0;
                hasher.update(&[is_null as u8]);
                if is_null { continue; }
                let tc = col.type_code;
                if tc == type_code::STRING || tc == type_code::BLOB {
                    let sb = mb.get_col_ptr(row, pi, 16);
                    let content = crate::schema::german_string_content(sb, mb.blob);
                    // Length-prefix the content so "ab"+"c" can't alias "a"+"bc".
                    hasher.update(&(content.len() as u32).to_le_bytes());
                    hasher.update(content);
                } else {
                    let cs = col.size() as usize;
                    hasher.update(mb.get_col_ptr(row, pi, cs));
                }
            }
            pks.push(hasher.digest128());
        }
    }
    for (row, pk) in pks.iter().enumerate() {
        // The reindex output PK is a synthetic U128 (unsigned), so OPK == BE.
        output.set_pk_at_bytes(row, &pk.to_be_bytes());
    }
}

/// Murmur3 64-bit finalizer.
#[inline]
fn mix64(mut v: u64) -> u64 {
    v ^= v >> 33;
    v = v.wrapping_mul(0xFF51AFD7ED558CCD);
    v ^= v >> 33;
    v = v.wrapping_mul(0xC4CEB9FE1A85EC53);
    v ^= v >> 33;
    v
}

/// Per-batch dispatch for "read column value, project to 128-bit PK". The
/// `match` on type code happens once at construction; per-row work is just
/// the read + the kind-specific decode.
pub(super) struct PkPromoter {
    kind: PromoteKind,
}

enum PromoteKind {
    /// Reindex on a PK column: copy that column's OPK bytes verbatim. The PK
    /// region is already OPK at rest (sign bit flipped for signed), so a verbatim
    /// byte copy — right-aligned into the output stride — is the sign-aware
    /// synthetic key. `widen_pk_be` of the result equals `extract_col_key`'s
    /// `widen_pk_be(get_pk_bytes[col])`, so the reindexed (delta) side and the
    /// OPK-routed (trace) side land on the same worker.
    Pk { off: usize, cs: usize },
    /// U128/UUID payload (unsigned): OPK == big-endian.
    Wide { pi: usize },
    /// STRING/BLOB payload: sign-agnostic XXH3 hash key.
    String { pi: usize },
    /// Reindex on an integer payload column: OPK-encode the native value
    /// (sign-flipped for signed), matching `extract_col_key`'s payload arm so
    /// equal logical values from the Pk and Narrow arms produce identical bytes.
    Narrow { pi: usize, cs: usize, tc: u8 },
}

impl PkPromoter {
    pub(super) fn new(schema: &SchemaDescriptor, col_idx: usize) -> Self {
        if schema.is_pk_col(col_idx) {
            let off = schema.pk_byte_offset(col_idx) as usize;
            let cs = schema.columns[col_idx].size() as usize;
            return PkPromoter { kind: PromoteKind::Pk { off, cs } };
        }
        let tc = schema.columns[col_idx].type_code;
        let pi = schema.payload_idx(col_idx);
        let kind = match tc {
            type_code::U128 | type_code::UUID => PromoteKind::Wide { pi },
            // BLOB shares the 16-byte German-string struct layout with STRING,
            // so it must take the same hash path (not the narrow ≤8-byte copy).
            type_code::STRING | type_code::BLOB => PromoteKind::String { pi },
            // All ≤8-byte integer and float types: OPK-encode the native value
            // (sign-flip for signed integers), matching extract_col_key's
            // payload arm. Float bit patterns OPK-encode as their unsigned image.
            _ => PromoteKind::Narrow { pi, cs: crate::schema::type_size(tc) as usize, tc },
        };
        PkPromoter { kind }
    }

    #[inline]
    fn read_wide(batch: &MemBatch, pi: usize, row: usize) -> u128 {
        let ptr = batch.get_col_ptr(row, pi, 16);
        u128::from_le_bytes(ptr[0..16].try_into().unwrap())
    }

    #[inline]
    fn read_string(batch: &MemBatch, pi: usize, row: usize) -> u128 {
        let struct_bytes = batch.get_col_ptr(row, pi, 16);
        let content = crate::schema::german_string_content(struct_bytes, batch.blob);
        if content.is_empty() {
            return 0;
        }
        let h = xxh::checksum(content);
        let h_hi = mix64(h);
        ((h_hi as u128) << 64) | (h as u128)
    }

    /// Test-only oracle: the **OPK-widened** value (what `get_pk` returns after
    /// `promote_into`), via the same `pk_route_key`/`payload_route_key` helpers
    /// the routing surfaces use — sign-aware for signed columns, so it discriminates
    /// the OPK encoding (an unsigned-native oracle would pass on the broken code).
    #[cfg(test)]
    #[inline]
    pub(super) fn promote(&self, batch: &MemBatch, row: usize) -> u128 {
        match self.kind {
            PromoteKind::Pk { off, cs } =>
                crate::schema::pk_route_key(batch.get_pk_bytes(row), off, cs),
            PromoteKind::Narrow { pi, cs, tc } =>
                crate::schema::payload_route_key(batch.get_col_ptr(row, pi, cs), 0, cs, tc),
            PromoteKind::Wide { pi } => Self::read_wide(batch, pi, row),
            PromoteKind::String { pi } => Self::read_string(batch, pi, row),
        }
    }

    /// Promote every row of `output` from `batch`, hoisting the per-batch
    /// kind dispatch out of the row loop (the kind is a batch-invariant). Every
    /// arm emits sign-aware OPK bytes right-aligned into `output.pk_stride()`, so
    /// the synthetic reindex key is byte-identical to how `extract_col_key`,
    /// `partition_for_pk_bytes`, and storage encode the same value — the
    /// co-partition contract — and equal logical values from the `Pk` and
    /// `Narrow` arms produce identical bytes.
    pub(super) fn promote_into(&self, batch: &MemBatch, output: &mut Batch) {
        let stride = output.pk_stride() as usize;
        // The fixed `[0u8; 16]` scratch buffers below right-align into this
        // stride; a stride > 16 would underflow `16 - stride`. The compiler types
        // every current synthetic reindex key as U128 (stride 16). When compound
        // reindex output (stride > 16) lands, the buffers must grow to
        // `MAX_PK_BYTES` — this assert is the tripwire for that work.
        debug_assert!(stride <= 16, "promote_into: synthetic key stride {stride} > 16");
        match self.kind {
            // Source PK column is already OPK at rest — copy it verbatim, right-
            // aligned with left zero-pad. widen_pk_be of the result equals
            // extract_col_key's `widen_pk_be(get_pk_bytes[col])`, so routing agrees.
            PromoteKind::Pk { off, cs } => {
                for row in 0..output.count {
                    let src = &batch.get_pk_bytes(row)[off..off + cs];
                    let mut buf = [0u8; 16];
                    buf[16 - cs..].copy_from_slice(src);
                    output.set_pk_at_bytes(row, &buf[16 - stride..]);
                }
            }
            // Payload integer: OPK-encode the native value (sign-flipped for
            // signed), matching extract_col_key's payload arm.
            PromoteKind::Narrow { pi, cs, tc } => {
                for row in 0..output.count {
                    let native = batch.get_col_ptr(row, pi, cs);
                    let mut opk = [0u8; 16];
                    gnitz_wire::encode_pk_column(native, tc, &mut opk[16 - cs..]);
                    output.set_pk_at_bytes(row, &opk[16 - stride..]);
                }
            }
            // U128/UUID are unsigned: OPK == big-endian.
            PromoteKind::Wide { pi } => {
                for row in 0..output.count {
                    let v = Self::read_wide(batch, pi, row);
                    output.set_pk_at_bytes(row, &v.to_be_bytes()[16 - stride..]);
                }
            }
            // Synthetic XXH3 hash key (unsigned U128): OPK == big-endian.
            PromoteKind::String { pi } => {
                for row in 0..output.count {
                    let h = Self::read_string(batch, pi, row);
                    output.set_pk_at_bytes(row, &h.to_be_bytes()[16 - stride..]);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code};
    use crate::storage::Batch;

    fn make_schema_u64_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64)],
    ) -> Batch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));
        for &(pk, w, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    fn get_payload_i64(b: &Batch, row: usize) -> i64 {
        crate::util::read_i64_le(b.col_data(0), row * 8)
    }

    // -----------------------------------------------------------------------
    // Union merge sort-invariant tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_union_merge_same_pk_payload_order() {
        // batch_a has val=20, batch_b has val=10 — output must be [10, 20]
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 20)]);
        let b = make_batch(&schema, &[(1, 1, 10)]);
        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 2);
        assert!(out.sorted);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 20);
    }

    #[test]
    fn test_union_merge_same_pk_multiple_entries() {
        // a: [(1,1,20),(1,1,30)], b: [(1,1,10),(1,1,25)] → payloads [10,20,25,30]
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 20), (1, 1, 30)]);
        let b = make_batch(&schema, &[(1, 1, 10), (1, 1, 25)]);
        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 4);
        assert!(out.sorted);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert_eq!(get_payload_i64(&out, 2), 25);
        assert_eq!(get_payload_i64(&out, 3), 30);
    }

    #[test]
    fn test_union_merge_mixed_same_diff_pk() {
        // a: [(1,1,20),(3,1,300)], b: [(1,1,10),(2,1,200)]
        // output PKs [1,1,2,3], vals [10,20,200,300]
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 20), (3, 1, 300)]);
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 200)]);
        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 4);
        assert!(out.sorted);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!((out.get_pk(1) as u64), 1);
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert_eq!((out.get_pk(2) as u64), 2);
        assert_eq!(get_payload_i64(&out, 2), 200);
        assert_eq!((out.get_pk(3) as u64), 3);
        assert_eq!(get_payload_i64(&out, 3), 300);
    }

    #[test]
    fn test_union_merge_same_pk_equal_payload() {
        // Same (PK, payload), opposite weights — must be adjacent for consolidation
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10)]);
        let b = make_batch(&schema, &[(1, -1, 10)]);
        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 2);
        assert!(out.sorted);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 10);
    }

    // -----------------------------------------------------------------------
    // op_filter tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_filter_basic() {
        use crate::expr::{ExprProgram, Plan};

        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 5), (2, 1, 15), (3, 1, 25)]);

        let code = vec![
            1i64, 0, 1, 0,  // LOAD_COL_INT r0 = col[1]
            3, 1, 10, 0,    // LOAD_CONST r1 = 10
            17, 2, 0, 1,    // CMP_GT r2 = (r0 > r1)
        ];
        let prog = ExprProgram::new(code, 3, 2, vec![]);
        let func = ScalarFuncKind::Plan(Plan::from_predicate(prog, &schema));

        let out = op_filter(&batch, &func, &schema);
        assert_eq!(out.count, 2, "only pk=2 and pk=3 pass val>10");
        assert_eq!((out.get_pk(0) as u64), 2);
        assert_eq!((out.get_pk(1) as u64), 3);
    }

    #[test]
    fn test_op_filter_consolidated_flag() {
        use crate::expr::{ExprProgram, Plan};

        let code = vec![
            3i64, 0, 1, 0,  // LOAD_CONST r0 = 1 (always true)
        ];
        let prog = ExprProgram::new(code, 1, 0, vec![]);
        let schema = make_schema_u64_i64();
        let func = ScalarFuncKind::Plan(Plan::from_predicate(prog, &schema));

        let mut batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        batch.consolidated = true;

        let out = op_filter(&batch, &func, &schema);
        assert_eq!(out.count, 2);
        assert!(out.consolidated, "consolidated input + pass-all → consolidated output");
        assert!(out.sorted);
    }

    // -----------------------------------------------------------------------
    // op_negate tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_negate_weights() {
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 3, 10), (2, -1, 20)]);
        let out = op_negate(&batch);
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), -3);
        assert_eq!(out.get_weight(1), 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert!(out.consolidated);
    }

    // -----------------------------------------------------------------------
    // promote_col_to_pk tests
    // -----------------------------------------------------------------------

    fn make_schema_pk_u64_payload_u32() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U32, 0),
            ],
            &[0],
        )
    }

    fn make_schema_pk_u64_payload_uuid() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::UUID, 0),
            ],
            &[0],
        )
    }

    fn build_batch_u32_payload(schema: &SchemaDescriptor, rows: &[(u64, u32)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b
    }

    fn build_batch_uuid_payload(schema: &SchemaDescriptor, rows: &[(u64, u128)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, val) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&1i64.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b
    }

    #[test]
    fn test_promote_col_to_pk_u32_all_rows_correct() {
        // U32 payload columns have stride 4. Passing col_size=8 to get_col_ptr gives
        // wrong offsets for row > 0. Verify all three rows return the right value.
        let schema = make_schema_pk_u64_payload_u32();
        let batch = build_batch_u32_payload(&schema, &[(1, 100), (2, 200), (3, 300)]);
        let mb = batch.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 1);

        assert_eq!(promoter.promote(&mb, 0), 100u128, "row 0 U32 promote");
        assert_eq!(promoter.promote(&mb, 1), 200u128, "row 1 U32 promote — wrong col_size corrupts this");
        assert_eq!(promoter.promote(&mb, 2), 300u128, "row 2 U32 promote");
    }

    #[test]
    fn test_promote_col_to_pk_uuid_preserves_high_bits() {
        // UUID has stride 16. Passing col_size=8 truncates the high 64 bits.
        let schema = make_schema_pk_u64_payload_uuid();
        let uuid_a: u128 = 0x550e8400_e29b_41d4_a716_446655440000u128;
        let uuid_b: u128 = 0xdeadbeef_cafe_1234_5678_000000000001u128;
        let batch = build_batch_uuid_payload(&schema, &[(1, uuid_a), (2, uuid_b)]);
        let mb = batch.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 1);

        assert_eq!(promoter.promote(&mb, 0), uuid_a, "row 0 UUID promote must keep all 128 bits");
        assert_eq!(promoter.promote(&mb, 1), uuid_b, "row 1 UUID promote must keep all 128 bits");
    }

    // -----------------------------------------------------------------------
    // op_map tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_map_empty_batch() {
        use crate::expr::Plan;
        let schema = make_schema_u64_i64();
        let empty_batch = Batch::empty(1, 16);

        let func = ScalarFuncKind::Plan(Plan::from_projection(
            &[1], &[type_code::I64], &schema, &schema,
        ));
        let out = op_map(&empty_batch, &func, &schema, &schema, -1, 0);
        assert_eq!(out.count, 0);
    }

    // -----------------------------------------------------------------------
    // PkPromoter — gap-coverage for STRING hashing and PK-column reindex
    // -----------------------------------------------------------------------

    fn make_schema_pk_u64_payload_string() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        )
    }

    #[test]
    fn test_pk_promoter_string_short_and_long_hash() {
        // Two rows: one short ("foo", inline) and one long string (> 12 bytes,
        // stored in blob). Both PromoteKind::String code paths execute, and
        // distinct strings hash to distinct PKs.
        let schema = make_schema_pk_u64_payload_string();
        let mut b = Batch::with_schema(schema, 2);

        // Row 0: short string "foo" (3 bytes, inline).
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        let mut gs0 = [0u8; 16];
        gs0[0..4].copy_from_slice(&3u32.to_le_bytes());
        gs0[4..7].copy_from_slice(b"foo");
        b.extend_col(0, &gs0);
        b.count += 1;

        // Row 1: long string (15 bytes > SHORT_STRING_THRESHOLD=12), heap-allocated.
        let long_str: &[u8] = b"hello-world-xyz";
        let heap_off = b.blob.len() as u64;
        b.blob.extend_from_slice(long_str);
        b.extend_pk(2u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        let mut gs1 = [0u8; 16];
        gs1[0..4].copy_from_slice(&(long_str.len() as u32).to_le_bytes());
        gs1[4..8].copy_from_slice(&long_str[..4]); // prefix
        gs1[8..16].copy_from_slice(&heap_off.to_le_bytes());
        b.extend_col(0, &gs1);
        b.count += 1;

        let mb = b.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 1);

        let pk_short = promoter.promote(&mb, 0);
        let pk_long = promoter.promote(&mb, 1);

        // Hashed via xxh + mix64 — exact value is deterministic but we don't
        // pin it. The load-bearing invariants are: (1) both produce a
        // non-zero PK for non-empty strings, (2) different strings hash to
        // different PKs, (3) the high 64 bits are populated (mix64 lifts
        // collisions out of the low half).
        assert_ne!(pk_short, 0);
        assert_ne!(pk_long, 0);
        assert_ne!(pk_short, pk_long);
        assert_ne!(pk_short >> 64, 0, "short string PK must populate high half via mix64");
        assert_ne!(pk_long >> 64, 0, "long string PK must populate high half via mix64");
    }

    #[test]
    fn test_pk_promoter_empty_string_hashes_to_zero() {
        // PromoteKind::String early-returns 0 for length==0 — assert this is
        // the contract, not an accidental side-effect of xxh on empty input.
        let schema = make_schema_pk_u64_payload_string();
        let mut b = Batch::with_schema(schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        // 16-byte German string struct, length=0.
        let gs = [0u8; 16];
        b.extend_col(0, &gs);
        b.count += 1;

        let mb = b.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 1);
        assert_eq!(promoter.promote(&mb, 0), 0);
    }

    #[test]
    fn test_pk_promoter_pk_column_passthrough() {
        // PromoteKind::Pk: reindexing on the PK column itself reads the PK
        // value verbatim, no hashing, no type dispatch.
        let schema = make_schema_pk_u64_payload_u32();
        let batch = build_batch_u32_payload(&schema, &[(7, 100), (42, 200), (1234567890, 300)]);
        let mb = batch.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 0); // col_idx 0 == PK

        assert_eq!(promoter.promote(&mb, 0), 7u128);
        assert_eq!(promoter.promote(&mb, 1), 42u128);
        assert_eq!(promoter.promote(&mb, 2), 1234567890u128);
    }

    // -----------------------------------------------------------------------
    // op_map reindex — end-to-end exercise of the PkPromoter via op_map
    // -----------------------------------------------------------------------

    // -----------------------------------------------------------------------
    // Wide-PK union merge (pk_stride > 16)
    // -----------------------------------------------------------------------

    fn make_schema_wide_pk_3xu64_i64() -> SchemaDescriptor {
        // PK = (U64, U64, U64) = 24 bytes (wide); one I64 payload column.
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

    fn make_wide_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, u64, u64, i64, i64)],
    ) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(k0, k1, k2, w, val) in rows {
            let mut pk = [0u8; 24];
            pk[0..8].copy_from_slice(&k0.to_le_bytes());
            pk[8..16].copy_from_slice(&k1.to_le_bytes());
            pk[16..24].copy_from_slice(&k2.to_le_bytes());
            b.extend_pk_bytes(&pk);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    fn wide_pk_triple(b: &Batch, row: usize) -> (u64, u64, u64) {
        let pk = b.get_pk_bytes(row);
        (
            u64::from_le_bytes(pk[0..8].try_into().unwrap()),
            u64::from_le_bytes(pk[8..16].try_into().unwrap()),
            u64::from_le_bytes(pk[16..24].try_into().unwrap()),
        )
    }

    #[test]
    fn test_op_union_merge_wide_pk() {
        // Regression: op_union on wide-PK (pk_stride=24) batches previously
        // panicked in get_pk -> widen_pk_le. Verify it merges correctly:
        // sorted by (PK, payload), all rows present, equal-PK groups
        // payload-sorted, weights not summed (union, not consolidation).
        let schema = make_schema_wide_pk_3xu64_i64();
        let a = make_wide_batch(&schema, &[(0, 0, 1, 1, 20), (0, 0, 3, 1, 300)]);
        let b = make_wide_batch(&schema, &[(0, 0, 1, 1, 10), (0, 0, 2, 1, 200)]);

        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 4);
        assert!(out.sorted);
        assert!(!out.consolidated);

        assert_eq!(wide_pk_triple(&out, 0), (0, 0, 1));
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(wide_pk_triple(&out, 1), (0, 0, 1));
        assert_eq!(get_payload_i64(&out, 1), 20);
        assert_eq!(wide_pk_triple(&out, 2), (0, 0, 2));
        assert_eq!(get_payload_i64(&out, 2), 200);
        assert_eq!(wide_pk_triple(&out, 3), (0, 0, 3));
        assert_eq!(get_payload_i64(&out, 3), 300);
    }

    #[test]
    fn test_op_union_empty_a_returns_b() {
        // batch_a empty, batch_b non-empty → result equals batch_b content.
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[]);
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 2);
        assert_eq!(out.get_pk(0) as u64, 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
        assert_eq!(out.get_pk(1) as u64, 2);
        assert_eq!(get_payload_i64(&out, 1), 20);
    }

    #[test]
    fn test_op_union_b_none_passthrough() {
        // batch_b None → batch_a returned verbatim (sorted/consolidated kept).
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10)]);
        let out = op_union(a, None, &schema);
        assert_eq!(out.count, 1);
        assert!(out.sorted && out.consolidated);
        assert_eq!(get_payload_i64(&out, 0), 10);
    }

    // -----------------------------------------------------------------------
    // op_union_merge signed I64 PK ordering (item 33)
    // -----------------------------------------------------------------------

    fn make_schema_i64pk_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_batch_i64pk(schema: &SchemaDescriptor, rows: &[(i64, i64, i64)]) -> Batch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk_opk(schema, &[(pk as u64) as u128]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        b
    }

    #[test]
    fn test_op_union_merge_signed_i64_pk() {
        // Signed PK has high bit set for negatives. Raw-u128 comparison (narrow
        // path) would sort negatives after positives. The merge must produce
        // signed ascending order.
        let schema = make_schema_i64pk_i64();
        let a = make_batch_i64pk(&schema, &[(-5, 1, 100), (0, 1, 200), (7, 1, 300)]);
        let b = make_batch_i64pk(&schema, &[(-1, 1, 400), (3, 1, 500)]);
        let out = op_union(a, Some(&b), &schema);
        assert_eq!(out.count, 5);
        assert!(out.sorted);
        let pks: Vec<i64> = (0..out.count)
            .map(|i| {
                let mut le = [0u8; 8];
                gnitz_wire::decode_pk_column(out.get_pk_bytes(i), type_code::I64, &mut le);
                i64::from_le_bytes(le)
            })
            .collect();
        assert_eq!(pks, vec![-5, -1, 0, 3, 7], "signed ascending PK order");
    }

    // -----------------------------------------------------------------------
    // PkPromoter::promote_into matches per-row promote across all variants (item 31)
    // -----------------------------------------------------------------------

    #[test]
    fn test_pk_promoter_promote_into_matches_per_row() {
        // Narrow (U32) variant.
        let schema = make_schema_pk_u64_payload_u32();
        let rows: Vec<(u64, u32)> = (0..1000).map(|i| (i, (i * 7 + 1) as u32)).collect();
        let batch = build_batch_u32_payload(&schema, &rows);
        let mb = batch.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 1);

        let mut out = batch.clone_batch();
        promoter.promote_into(&mb, &mut out);
        for row in 0..batch.count {
            assert_eq!(out.get_pk(row), promoter.promote(&mb, row), "row {} narrow", row);
        }

        // Pk variant (reindex on PK column itself).
        let promoter_pk = PkPromoter::new(&schema, 0);
        let mut out_pk = batch.clone_batch();
        promoter_pk.promote_into(&mb, &mut out_pk);
        for row in 0..batch.count {
            assert_eq!(out_pk.get_pk(row), promoter_pk.promote(&mb, row), "row {} pk", row);
        }

        // Signed (I64) schema — the discriminating case. The oracle is sign-aware
        // (route_key helpers), so promote_into must produce OPK-encoded bytes for
        // both the PK-column (Pk) and payload-column (Narrow) arms.
        let s_schema = make_schema_i64pk_i64();
        let s_rows: Vec<(i64, i64, i64)> =
            (-500..500).map(|i| (i, 1, (i * 3 - 7))).collect();
        let s_batch = make_batch_i64pk(&s_schema, &s_rows);
        let s_mb = s_batch.as_mem_batch();

        let s_narrow = PkPromoter::new(&s_schema, 1);
        let mut s_out = s_batch.clone_batch();
        s_narrow.promote_into(&s_mb, &mut s_out);
        for row in 0..s_batch.count {
            assert_eq!(s_out.get_pk(row), s_narrow.promote(&s_mb, row), "row {} signed narrow", row);
        }

        let s_pk = PkPromoter::new(&s_schema, 0);
        let mut s_out_pk = s_batch.clone_batch();
        s_pk.promote_into(&s_mb, &mut s_out_pk);
        for row in 0..s_batch.count {
            assert_eq!(s_out_pk.get_pk(row), s_pk.promote(&s_mb, row), "row {} signed pk", row);
        }
    }

    #[test]
    fn test_pk_promoter_signed_opk_encoding() {
        // Reindex on a signed I64 PAYLOAD column (Narrow arm). The synthetic key
        // must be the sign-aware OPK image (matching extract_col_key's payload
        // encoding), NOT the raw unsigned value — for -3 the OPK leading byte is
        // 0x7F, not 0xFF. This is the assertion the old unsigned-only test could
        // not make.
        let schema = make_schema_i64pk_i64();
        let batch = make_batch_i64pk(&schema, &[(1, 1, -3), (2, 1, 5), (3, 1, -1)]);
        let mb = batch.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 1); // payload col → Narrow arm
        // Output PK stride is 8 here (single I64 PK), so the synthetic key is the
        // 8-byte OPK image directly (right-aligned into stride 8 == verbatim).
        let mut out = batch.clone_batch();
        promoter.promote_into(&mb, &mut out);

        let mut opk = [0u8; 8];
        gnitz_wire::encode_pk_column(&(-3i64).to_le_bytes(), type_code::I64, &mut opk);
        assert_eq!(out.get_pk_bytes(0), &opk[..], "synthetic key must be sign-aware OPK");
        assert_eq!(opk[0], 0x7F, "OPK leading byte of -3:I64 is 0x7F (sign-flipped), not 0xFF");

        for row in 0..batch.count {
            assert_eq!(out.get_pk(row), promoter.promote(&mb, row), "row {} narrow signed", row);
        }
    }

    #[test]
    fn test_pk_promoter_signed_copartition_pk_vs_narrow() {
        // Co-partition contract: a value reindexed via the Pk arm (as a PK column)
        // and via the Narrow arm (as a payload column) must produce byte-identical
        // synthetic PK bytes, and widen_pk_be of those bytes must equal the routing
        // key for that value — including negatives. This is what makes the
        // reindexed (delta) side and the OPK-routed (trace) side co-partition.
        let schema = make_schema_i64pk_i64();
        // Each row carries the same logical value V in both the PK and the payload.
        let vals = [-7i64, -1, 0, 3, i64::MIN, i64::MAX];
        let rows: Vec<(i64, i64, i64)> = vals.iter().map(|&v| (v, 1, v)).collect();
        let batch = make_batch_i64pk(&schema, &rows);
        let mb = batch.as_mem_batch();

        let promoter_pk = PkPromoter::new(&schema, 0);     // PK column → Pk arm
        let promoter_narrow = PkPromoter::new(&schema, 1);  // payload column → Narrow arm
        let mut out_pk = batch.clone_batch();
        let mut out_narrow = batch.clone_batch();
        promoter_pk.promote_into(&mb, &mut out_pk);
        promoter_narrow.promote_into(&mb, &mut out_narrow);

        // `row` indexes out_pk, out_narrow, and vals together — enumerate over
        // any one would not cover the others.
        #[allow(clippy::needless_range_loop)]
        for row in 0..batch.count {
            assert_eq!(
                out_pk.get_pk_bytes(row), out_narrow.get_pk_bytes(row),
                "row {}: Pk-arm and Narrow-arm synthetic bytes must match for V={}",
                row, vals[row],
            );
            // widen_pk_be of the synthetic bytes equals the sign-aware routing key
            // (what extract_col_key returns for this value).
            let expect =
                crate::schema::payload_route_key(&vals[row].to_le_bytes(), 0, 8, type_code::I64);
            assert_eq!(out_pk.get_pk(row), expect, "row {}: routing key mismatch", row);
        }
    }

    // -----------------------------------------------------------------------
    // op_null_extend blob propagation + combined-column guard
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_null_extend_blob_propagation() {
        // Regression: op_null_extend did not propagate the input blob, so a
        // long (> 12 byte) string in the output resolved against an empty
        // blob and returned garbage.
        let in_schema = make_schema_pk_u64_payload_string();
        let mut b = Batch::with_schema(in_schema, 1);
        let long_str: &[u8] = b"a-fairly-long-string-value"; // 26 bytes > 12
        let heap_off = b.blob.len() as u64;
        b.blob.extend_from_slice(long_str);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        let mut gs = [0u8; 16];
        gs[0..4].copy_from_slice(&(long_str.len() as u32).to_le_bytes());
        gs[4..8].copy_from_slice(&long_str[..4]);
        gs[8..16].copy_from_slice(&heap_off.to_le_bytes());
        b.extend_col(0, &gs);
        b.count += 1;

        // Right side: a single I64 payload column.
        let right_schema = make_schema_u64_i64();
        let out = op_null_extend(&b, &in_schema, &right_schema);

        assert!(!out.blob.is_empty(), "output blob must be propagated");

        // Resolve the long string from the output's STRING column (col 0).
        let struct_bytes = out.col_data(0);
        let length = u32::from_le_bytes(struct_bytes[0..4].try_into().unwrap()) as usize;
        let off = u64::from_le_bytes(struct_bytes[8..16].try_into().unwrap()) as usize;
        let resolved = crate::schema::long_string_bytes(&out.blob, off, length);
        assert_eq!(resolved, long_str, "long string must resolve to the original");
    }

    #[test]
    #[should_panic(expected = "op_null_extend: combined column count")]
    fn test_op_null_extend_combined_column_guard() {
        // Regression: op_null_extend wrote in_schema.num_columns() +
        // right_schema.num_payload_cols() entries into a [_; MAX_COLUMNS]
        // stack array with no bounds check. Combined > MAX_COLUMNS must
        // panic with a clear message, not a generic index-out-of-bounds.
        let mut in_cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..40 {
            in_cols.push(SchemaColumn::new(type_code::I64, 0));
        }
        let in_schema = SchemaDescriptor::new(&in_cols, &[0]);

        let mut right_cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..40 {
            right_cols.push(SchemaColumn::new(type_code::I64, 0));
        }
        let right_schema = SchemaDescriptor::new(&right_cols, &[0]);

        // 41 + 40 = 81 > MAX_COLUMNS (65).
        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        for pi in 0..in_schema.num_payload_cols() {
            b.extend_col(pi, &0i64.to_le_bytes());
        }
        b.count += 1;
        let _ = op_null_extend(&b, &in_schema, &right_schema);
    }

    // -----------------------------------------------------------------------
    // PkPromoter BLOB
    // -----------------------------------------------------------------------

    fn make_schema_pk_u64_payload_blob() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::BLOB, 0),
            ],
            &[0],
        )
    }

    #[test]
    fn test_op_filter_blob_passthrough_long_value() {
        // op_filter over a batch with a BLOB payload column holding a long
        // (> 12 byte) value. The output BLOB must resolve to the original.
        let schema = make_schema_pk_u64_payload_blob();
        let mut b = Batch::with_schema(schema, 2);
        let long_blob: &[u8] = b"a-fairly-long-blob-value-xyz"; // 28 bytes > 12
        let heap_off = b.blob.len() as u64;
        b.blob.extend_from_slice(long_blob);
        // Row 0: long blob, val passes always-true filter.
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        let mut gs = [0u8; 16];
        gs[0..4].copy_from_slice(&(long_blob.len() as u32).to_le_bytes());
        gs[4..8].copy_from_slice(&long_blob[..4]);
        gs[8..16].copy_from_slice(&heap_off.to_le_bytes());
        b.extend_col(0, &gs);
        b.count += 1;
        b.sorted = true;
        b.consolidated = true;

        let func = always_true_func(&schema);
        let out = op_filter(&b, &func, &schema);
        assert_eq!(out.count, 1);
        assert!(!out.blob.is_empty(), "BLOB filter output must carry the blob buffer");
        let struct_bytes = out.col_data(0);
        let length = u32::from_le_bytes(struct_bytes[0..4].try_into().unwrap()) as usize;
        let off = u64::from_le_bytes(struct_bytes[8..16].try_into().unwrap()) as usize;
        let resolved = crate::schema::long_string_bytes(&out.blob, off, length);
        assert_eq!(resolved, long_blob, "long BLOB must resolve to the original bytes");
    }

    #[test]
    fn test_pk_promoter_blob_no_panic() {
        // Regression: PkPromoter mapped BLOB to Narrow { cs: 16 }, copying 16
        // bytes into an 8-byte buffer and panicking. BLOB shares the
        // German-string layout and must take the String hash path.
        let schema = make_schema_pk_u64_payload_blob();
        let mut b = Batch::with_schema(schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        // Short inline BLOB "abc" in a 16-byte German-string struct.
        let mut gs = [0u8; 16];
        gs[0..4].copy_from_slice(&3u32.to_le_bytes());
        gs[4..7].copy_from_slice(b"abc");
        b.extend_col(0, &gs);
        b.count += 1;

        let mb = b.as_mem_batch();
        let promoter = PkPromoter::new(&schema, 1);
        let pk = promoter.promote(&mb, 0);
        assert_ne!(pk, 0, "non-empty BLOB must hash to a non-zero key");
    }

    // -----------------------------------------------------------------------
    // Stride consistency on early-exit empty batches (item 3)
    // -----------------------------------------------------------------------

    fn always_true_func(schema: &SchemaDescriptor) -> ScalarFuncKind {
        use crate::expr::{ExprProgram, Plan};
        let code = vec![3i64, 0, 1, 0]; // LOAD_CONST r0 = 1
        let prog = ExprProgram::new(code, 1, 0, vec![]);
        ScalarFuncKind::Plan(Plan::from_predicate(prog, schema))
    }

    #[test]
    fn test_op_filter_empty_stride_u64() {
        // U64 PK → stride 8. Early-exit empty batch must carry the schema
        // stride, not a hardcoded 16.
        let schema = make_schema_u64_i64();
        let empty = make_batch(&schema, &[]);
        assert_eq!(empty.count, 0);
        let func = always_true_func(&schema);
        let out = op_filter(&empty, &func, &schema);
        assert_eq!(out.count, 0);
        assert_eq!(out.pk_stride(), 8, "op_filter empty must use schema stride 8 for U64 PK");
    }

    #[test]
    fn test_op_negate_empty_stride_u64() {
        let schema = make_schema_u64_i64();
        let empty = make_batch(&schema, &[]);
        let out = op_negate(&empty);
        assert_eq!(out.count, 0);
        assert_eq!(out.pk_stride(), 8, "op_negate empty must use schema stride 8 for U64 PK");
    }

    #[test]
    fn test_op_null_extend_empty_stride_u64() {
        let in_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();
        let empty = make_batch(&in_schema, &[]);
        let out = op_null_extend(&empty, &in_schema, &right_schema);
        assert_eq!(out.count, 0);
        assert_eq!(out.pk_stride(), 8, "op_null_extend empty must use combined-schema stride 8");
    }

    // -----------------------------------------------------------------------
    // op_null_extend shift guard (item 4a)
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_null_extend_shift_guard_64_payload_cols() {
        // in_npc == 64 (65-column schema, 1 PK col) and right_npc == 0.
        // shift == 64 makes `right_null_bits << 64` panic in debug builds even
        // though right_null_bits is 0.
        let mut in_cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..64 {
            in_cols.push(SchemaColumn::new(type_code::I64, 0));
        }
        let in_schema = SchemaDescriptor::new(&in_cols, &[0]);
        assert_eq!(in_schema.num_payload_cols(), 64);

        // Right schema: PK only, no payload columns → right_npc == 0.
        let right_schema = SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0]);
        assert_eq!(right_schema.num_payload_cols(), 0);

        let mut b = Batch::with_schema(in_schema, 1);
        b.extend_pk(1u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        for pi in 0..in_schema.num_payload_cols() {
            b.extend_col(pi, &0i64.to_le_bytes());
        }
        b.count += 1;

        // Must not panic.
        let out = op_null_extend(&b, &in_schema, &right_schema);
        assert_eq!(out.count, 1);
        let in_null = u64::from_le_bytes(b.null_bmp_data()[0..8].try_into().unwrap());
        let out_null = u64::from_le_bytes(out.null_bmp_data()[0..8].try_into().unwrap());
        assert_eq!(out_null, in_null, "no right cols → output null word equals input");
    }

    // -----------------------------------------------------------------------
    // op_negate i64::MIN (item 46)
    // -----------------------------------------------------------------------

    #[test]
    fn test_op_negate_i64_min_no_panic() {
        // -i64::MIN overflows; debug builds panic, release wraps silently.
        // wrapping_neg must leave i64::MIN unchanged without panicking.
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, i64::MIN, 10), (2, 5, 20)]);
        let out = op_negate(&batch);
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), i64::MIN, "wrapping_neg(i64::MIN) == i64::MIN");
        assert_eq!(out.get_weight(1), -5);
    }

    #[test]
    fn test_op_map_with_reindex_promotes_payload_to_pk() {
        // op_map with reindex_col >= 0 rewrites the output PK by reading the
        // referenced column through PkPromoter. Verifies (1) every row's
        // output PK matches the source column value, (2) the resulting
        // batch is correctly marked unsorted/unconsolidated (sort order on
        // the new PK is not preserved by the row-by-row promote).
        use crate::expr::Plan;

        // Input: PK u64, payload i64. Reindex on the payload (col 1) — the
        // new output PK is each row's payload value.
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[
            (1, 1, 200),
            (2, 1, 100),
            (3, 1, 300),
        ]);

        // Projection plan: output keeps the same single payload column.
        let func = ScalarFuncKind::Plan(Plan::from_projection(
            &[1], &[type_code::I64], &schema, &schema,
        ));

        let out = op_map(&batch, &func, &schema, &schema, /* reindex_col = */ 1, 0);
        assert_eq!(out.count, 3);
        // Each output row's PK is the sign-aware OPK image of its source payload
        // value (col 1 is I64): `widen_pk_be(encode_pk_column(v))`, i.e. the value
        // with its sign bit flipped, matching how extract_col_key routes the same
        // value. A raw-native `== 200` assertion would falsely fail signed reindex.
        let opk_i64 = |v: i64| ((v as u64) ^ 0x8000_0000_0000_0000) as u128;
        assert_eq!(out.get_pk(0), opk_i64(200));
        assert_eq!(out.get_pk(1), opk_i64(100));
        assert_eq!(out.get_pk(2), opk_i64(300));
        // Payload itself is unchanged by the projection.
        assert_eq!(get_payload_i64(&out, 0), 200);
        assert_eq!(get_payload_i64(&out, 1), 100);
        assert_eq!(get_payload_i64(&out, 2), 300);
        // Reindex destroys PK order — output must be marked accordingly.
        assert!(!out.sorted, "reindex output must not be marked sorted");
        assert!(!out.consolidated, "reindex output must not be marked consolidated");
    }
}
