//! Join operators: anti-join, semi-join, inner join, outer join (delta-trace + delta-delta).

use std::cmp::Ordering;

use crate::schema::SchemaDescriptor;
use gnitz_wire::{is_german_string, RangeRel};
use crate::storage::{
    range_cut_points, range_group_cut_points, write_to_batch, Batch, ConsolidatedBatch,
    MemBatch, ReadCursor,
    scatter_copy, with_payload_cmp,
};

use super::cogroup::{cogroup_intersection, cogroup_left, BatchCursor};
use super::util::{all_payload_null_mask, merge_null_words, write_string_from_batch};

thread_local! {
    /// Reused emit-index scratch for the delta-trace anti/semi joins. Cleared
    /// per operator call rather than allocating a fresh `Vec<u32>` each tick —
    /// same hold-across-work, no-cap shape as exchange.rs's pools. The borrow is
    /// confined to one operator body; `scatter_copy` does not re-enter.
    static JOIN_EMIT_INDICES: std::cell::RefCell<Vec<u32>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

// ---------------------------------------------------------------------------
// Anti-join delta-trace
// ---------------------------------------------------------------------------

/// Emit delta rows whose key has NO positive-weight match in the trace.
/// `cogroup_left` visits every delta group (the match side galloping-skips to
/// it); the group is emitted iff `group_has_positive` reports no live row in the
/// trace group — i.e. the key is absent from Distinct(trace).
pub fn op_anti_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> ConsolidatedBatch {
    let npc = schema.num_payload_cols();
    let cs = Batch::consolidate_if_needed(delta, schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return ConsolidatedBatch::new_unchecked(Batch::empty(npc, schema.pk_stride()));
    }

    JOIN_EMIT_INDICES.with(|pool| {
        let mut emit_indices = pool.borrow_mut();
        emit_indices.clear();

        cogroup_left(consolidated, cursor, |key, range, m| {
            if !m.group_has_positive(key) {
                for i in range { emit_indices.push(i as u32); }
            }
        });

        if emit_indices.is_empty() {
            return ConsolidatedBatch::new_unchecked(Batch::empty(npc, schema.pk_stride()));
        }

        let mb = consolidated.as_mem_batch();
        let blob_cap = mb.blob.len().max(1);
        let mut output =
            write_to_batch(schema, emit_indices.len(), blob_cap, |writer| {
                scatter_copy(&mb, &emit_indices, &[], writer);
            });
        // emit_indices ascend (groups visited in key order, ranges ascending),
        // so rows come out in consolidated-delta order: (PK, payload)-sorted.
        output.sorted = true;
        output.consolidated = true;
        ConsolidatedBatch::new_unchecked(output)
    })
}

// ---------------------------------------------------------------------------
// Semi-join delta-trace
// ---------------------------------------------------------------------------

/// Emit delta rows whose key HAS a positive-weight match in the trace.
/// `cogroup_intersection` visits only shared keys (both pointers galloping-skip
/// to catch up, optimal whichever side is larger — replacing the old size
/// selector + swapped path); the delta group is emitted iff `group_has_positive`
/// reports a live row in the trace group.
pub fn op_semi_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    schema: &SchemaDescriptor,
) -> ConsolidatedBatch {
    let npc = schema.num_payload_cols();
    let cs = Batch::consolidate_if_needed(delta, schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return ConsolidatedBatch::new_unchecked(Batch::empty(npc, schema.pk_stride()));
    }

    JOIN_EMIT_INDICES.with(|pool| {
        let mut emit_indices = pool.borrow_mut();
        emit_indices.clear();

        cogroup_intersection(consolidated, cursor, |key, range, m| {
            if m.group_has_positive(key) {
                for i in range { emit_indices.push(i as u32); }
            }
        });

        if emit_indices.is_empty() {
            return ConsolidatedBatch::new_unchecked(Batch::empty(npc, schema.pk_stride()));
        }

        let mb = consolidated.as_mem_batch();
        let blob_cap = mb.blob.len().max(1);
        let mut output =
            write_to_batch(schema, emit_indices.len(), blob_cap, |writer| {
                scatter_copy(&mb, &emit_indices, &[], writer);
            });
        output.sorted = true;
        output.consolidated = true;
        ConsolidatedBatch::new_unchecked(output)
    })
}

// ---------------------------------------------------------------------------
// Inner join delta-trace
// ---------------------------------------------------------------------------

/// Join delta rows against trace. Output schema: [left_PK, left_payload..., right_payload...].
///
/// `cogroup_intersection` on the join key: the match (trace) side is forward-only,
/// so each trace group is walked once and producted against the whole
/// (random-access) delta group `[i, j)` — trace-major emission. Galloping skips
/// make this optimal in both former selector regimes (huge delta + tiny trace,
/// and the reverse) with no `rewind` / `estimated_length` / size selector.
pub fn op_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let out_npc = left_schema.num_payload_cols() + right_schema.num_payload_cols();

    let cs = Batch::consolidate_if_needed(delta, left_schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty(out_npc, left_schema.pk_stride());
    }

    let delta_mb = consolidated.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    cogroup_intersection(consolidated, cursor, |key, range, m| {
        m.for_each_pk_group_row(key, |c| {
            let w_trace = c.current_weight;
            for i in range.clone() {
                let w_out = consolidated.get_weight(i).wrapping_mul(w_trace);
                if w_out != 0 {
                    write_join_row(
                        &mut output, &delta_mb, i, c, w_out,
                        left_schema, right_schema,
                    );
                }
            }
        });
    });

    // Trace-major cartesian emission is PK-sorted but NOT (PK, payload)-sorted
    // (left payloads interleave across trace entries); downstream re-sorts.
    output.sorted = false;
    output
}

// ---------------------------------------------------------------------------
// Outer join delta-trace
// ---------------------------------------------------------------------------

/// Left outer join: like inner join, but a delta group with no positive-weight
/// trace match additionally emits null-filled right sides. `cogroup_left` visits
/// every delta group; `matched` is a property of the **trace group** (any
/// positive-weight row), shared by every delta row in the group, so the
/// null-fill decision is made once per group after the (trace-major) product.
pub fn op_join_delta_trace_outer(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let out_npc = left_schema.num_payload_cols() + right_schema.num_payload_cols();

    let cs = Batch::consolidate_if_needed(delta, left_schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty(out_npc, left_schema.pk_stride());
    }

    let delta_mb = consolidated.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    cogroup_left(consolidated, cursor, |key, range, m| {
        // Walk the trace group once: product against the whole delta group and
        // record whether any positive-weight trace row exists. A tombstone
        // (weight ≤ 0) still products but does NOT suppress the null-fill.
        let mut matched = false;
        m.for_each_pk_group_row(key, |c| {
            let w_trace = c.current_weight;
            if w_trace > 0 { matched = true; }
            for i in range.clone() {
                let w_out = consolidated.get_weight(i).wrapping_mul(w_trace);
                if w_out != 0 {
                    write_join_row(
                        &mut output, &delta_mb, i, c, w_out,
                        left_schema, right_schema,
                    );
                }
            }
        });
        if !matched {
            for i in range {
                write_join_row_null_right(
                    &mut output, &delta_mb, i, consolidated.get_weight(i),
                    left_schema, right_schema,
                );
            }
        }
    });

    output.sorted = false;
    output
}

// ---------------------------------------------------------------------------
// Non-equi (range) join delta-trace
// ---------------------------------------------------------------------------

/// Range join delta-trace: like `op_join_delta_trace`, but each consolidated
/// delta row matches the trace by an ordered half-open `[start, end)` range
/// (the §3 cut-point table) instead of an equal-key seek. `n_eq` leading PK
/// slots are equality-pinned (the band-join prefix), `rel` is the relation the
/// trace slot must satisfy versus the delta slot.
///
/// Both sides reindex to `[eq slots…, range slot]` at the pair's common promoted
/// type, so the trace PK region is an ordered arrangement by the range key and
/// the probe bound is the delta row's own packed PK bytes — no decode/re-encode.
///
/// Size-adaptive. Unlike the equi-join (now a single symmetric co-group), the
/// two range strategies are structurally distinct algorithms — not two spellings
/// of one merge — so the selector is load-bearing, not redundant: rewind so
/// `estimated_length` sees the full trace, then drive from whichever side is
/// smaller. When `|delta| ≤ |trace|` keep the delta-driven per-row probe
/// (`range_per_row_seek`); when the delta outnumbers the trace, sweep the trace
/// forward exactly once with a monotone delta pointer (`range_merge_walk`),
/// eliminating the per-row re-seek and the shared-region re-advance. Both emit
/// the identical match multiset, unsorted, re-ordered downstream.
pub fn op_join_delta_trace_range(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
    n_eq: usize,
    rel: RangeRel,
) -> Batch {
    let out_npc = left_schema.num_payload_cols() + right_schema.num_payload_cols();

    let cs = Batch::consolidate_if_needed(delta, left_schema);
    let consolidated: &Batch = cs.as_deref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty(out_npc, left_schema.pk_stride());
    }

    // Trace PK = [eq slots…, range slot]. Both sides pack at the same common type,
    // so the delta PK shape equals the trace PK shape.
    debug_assert_eq!(left_schema.pk_stride(), right_schema.pk_stride(),
        "range join: delta and trace PK strides must match (common-T reindex)");
    debug_assert_eq!(right_schema.pk_indices().len(), n_eq + 1,
        "range join: trace PK arity must be n_eq + 1");
    // (`range_cut_points` asserts the range slot is non-empty per row.)

    // The same trace register may be reused across a tick; reset to position 0 so
    // `estimated_length` sees the full trace and both strategies' first
    // `advance_to` galloping-seeks forward from the head.
    cursor.rewind();
    let trace_len = cursor.estimated_length();
    if n > trace_len {
        range_merge_walk(consolidated, cursor, left_schema, right_schema, n_eq, rel)
    } else {
        range_per_row_seek(consolidated, cursor, left_schema, right_schema, n_eq, rel)
    }
}

/// Strategy 1 — delta-driven per-row probe (`|delta| ≤ |trace|`). For each
/// consolidated delta row, derive its `[start, end)` cut interval and walk the
/// matching trace run. The start cut is globally monotone non-decreasing across
/// rows (except `Lt`/`Le` at `n_eq == 0`, where every row seeks the minimum), so
/// the hint-seeded `advance_to` galloping-skips forward across the monotone runs
/// and falls back to a bounded backward search on the rare reset — same matches.
fn range_per_row_seek(
    consolidated: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
    n_eq: usize,
    rel: RangeRel,
) -> Batch {
    let eq_size = right_schema.leading_key_size(n_eq);
    let n = consolidated.count;

    let delta_mb = consolidated.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);

    for i in 0..n {
        let pk_i = consolidated.get_pk_bytes(i);
        let w_delta = consolidated.get_weight(i);
        let (eq, d) = pk_i.split_at(eq_size);

        // Provably-empty interval (e.g. `Gt` of the maximal slot): no match.
        let Some((start, end)) = range_cut_points(eq, d, rel) else { continue };

        // Galloping forward skip seeded at the live position; falls back to a
        // bounded backward search on the `Lt`/`Le` `n_eq == 0` reset (all rows
        // seek 0x00…), so it stays correct for that non-monotone case too.
        cursor.advance_to(start.as_slice());
        while cursor.valid
            && end.as_ref().is_none_or(|e| cursor.current_pk_cmp_bytes(e.as_slice()).is_lt())
        {
            let w_out = w_delta.wrapping_mul(cursor.current_weight);
            if w_out != 0 {
                write_join_row(
                    &mut output, &delta_mb, i, cursor, w_out,
                    left_schema, right_schema,
                );
            }
            cursor.advance();
        }
    }

    // Probe order is delta-major with arbitrary trace payload runs per row — not
    // (PK, payload)-sorted. The re-key + output exchange + consolidation downstream
    // restore order; do not let a merge trust this batch as sorted.
    output.sorted = false;
    output
}

/// Strategy 2 — trace-driven eq-group merge walk (`|delta| > |trace|`). Per delta
/// eq-group, `range_group_cut_points` gives the one `[start, end)` span covering
/// the whole group; seek to `start` and sweep only that span with a monotone
/// delta pointer, emitting the matching contiguous delta sub-range per trace row.
/// The seek skips untouched trace groups *and* the intra-group dead head/tail, so
/// every walked trace row matches some delta row — none is walked redundantly.
/// Structurally a sort-merge band join: `O(g·log r + covered + m + output)`,
/// `g` = #delta groups, `covered` = matched trace rows.
fn range_merge_walk(
    consolidated: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
    n_eq: usize,
    rel: RangeRel,
) -> Batch {
    let eq_size = right_schema.leading_key_size(n_eq);
    // `MemBatch::get_pk_bytes` returns `&'a` tied to the batch data (not a `&self`
    // borrow), so `e` is held freely across grouping while `delta_mb` is read again.
    let delta_mb = consolidated.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);
    let m = consolidated.count;

    let mut lo = 0; // start of the current delta eq group
    while lo < m {
        // Delta eq group [lo, hi): contiguous rows sharing the eq prefix E, range
        // slots ascending in [d_lo, d_hi].
        let e = &delta_mb.get_pk_bytes(lo)[..eq_size];
        let mut hi = lo + 1;
        while hi < m && &delta_mb.get_pk_bytes(hi)[..eq_size] == e {
            hi += 1;
        }

        // One cut spanning the whole group. `None` ⇒ provably-empty group (e.g.
        // Gt of an all-maximal slot) — skip it.
        let d_lo = &delta_mb.get_pk_bytes(lo)[eq_size..];
        let d_hi = &delta_mb.get_pk_bytes(hi - 1)[eq_size..];
        let Some((start, end)) = range_group_cut_points(e, d_lo, d_hi, rel) else {
            lo = hi;
            continue;
        };

        // Galloping forward skip to the covered start: the targets ascend across
        // groups, so `advance_to` seeded at the live position skips untouched
        // trace groups and the intra-group dead head in one hop.
        cursor.advance_to(start.as_slice());
        let mut ptr = lo; // monotone delta pointer
        while cursor.valid {
            // Bind the PK once: both the span-end bound and the range-slot read
            // use it, so the row is fetched from the cursor a single time. Every
            // key in [start, end) carries eq prefix E, so `end` alone delimits
            // the group — no per-row eq check needed.
            let pk = cursor.current_pk_bytes();
            if end.as_ref().is_some_and(|e2| pk >= e2.as_slice()) {
                break; // reached the covered-span end (the dead tail follows)
            }
            let s = &pk[eq_size..]; // trace range slot (PK is exactly stride bytes)
            let w_t = cursor.current_weight;
            // Advance `ptr` and select the matching contiguous delta sub-range.
            // The `pk`/`s` cursor borrow dies in the pointer advance — before the
            // emission reborrows the cursor and the trailing `advance()`.
            let (rs, re) = match rel {
                // prefix [lo, ub): grows as s↑
                RangeRel::Gt => { advance_delta_ptr(&delta_mb, eq_size, &mut ptr, hi, |d| d < s); (lo, ptr) }
                RangeRel::Ge => { advance_delta_ptr(&delta_mb, eq_size, &mut ptr, hi, |d| d <= s); (lo, ptr) }
                // suffix [lb, hi): its lower bound rises (shrinks) as s↑
                RangeRel::Lt => { advance_delta_ptr(&delta_mb, eq_size, &mut ptr, hi, |d| d <= s); (ptr, hi) }
                RangeRel::Le => { advance_delta_ptr(&delta_mb, eq_size, &mut ptr, hi, |d| d < s); (ptr, hi) }
            };
            for k in rs..re {
                let w_out = delta_mb.get_weight(k).wrapping_mul(w_t);
                if w_out != 0 {
                    write_join_row(
                        &mut output, &delta_mb, k, cursor, w_out,
                        left_schema, right_schema,
                    );
                }
            }
            cursor.advance();
        }
        lo = hi;
    }

    // Trace-major with delta runs per trace row — not (PK, payload)-sorted.
    output.sorted = false;
    output
}

/// Advance the monotone delta pointer `*ptr` forward over the eq group `[*, hi)`
/// while the range slot at `*ptr` satisfies `pred` against the current trace
/// slot. Serves as both the prefix upper bound (`Gt`/`Ge`) and the suffix lower
/// bound (`Lt`/`Le`): as the trace slot ascends the prefix end and the suffix
/// start each move forward only, so starting at the group head and advancing is
/// exact and amortizes to `O(m_group)` across the whole trace group. Compares the
/// delta row's range slot by `memcmp` (OPK order = typed order).
#[inline]
fn advance_delta_ptr(
    delta_mb: &MemBatch,
    eq_size: usize,
    ptr: &mut usize,
    hi: usize,
    pred: impl Fn(&[u8]) -> bool,
) {
    while *ptr < hi && pred(&delta_mb.get_pk_bytes(*ptr)[eq_size..]) {
        *ptr += 1;
    }
}

// ---------------------------------------------------------------------------
// Join row writer helpers
// ---------------------------------------------------------------------------

/// Copy left-side payload columns from a MemBatch into the output.
/// Shared by all join row writers (inner, outer, DD).
fn write_left_payload(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    left_null: u64,
    left_schema: &SchemaDescriptor,
) {
    for (pi, _ci, col) in left_schema.payload_columns() {
        let cs = col.size() as usize;
        let is_null = (left_null >> pi) & 1 != 0;
        if is_null {
            output.fill_col_zero(pi, cs);
        } else if is_german_string(col.type_code) {
            write_string_from_batch(output, pi, left_batch, left_row, pi);
        } else {
            output.extend_col(pi, left_batch.get_col_ptr(left_row, pi, cs));
        }
    }
}

/// Write one composite join output row: [left_PK, left_payload..., right_payload...].
///
/// Left columns come from the delta MemBatch. Right columns come from the
/// cursor's current row, accessed via `col_ptr()` / `blob_ptr()`.
fn write_join_row(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    right_cursor: &ReadCursor,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) {
    let left_null = left_batch.get_null_word(left_row);
    let right_null = right_cursor.current_null_word;

    let left_npc = left_schema.num_payload_cols();
    let null_word = merge_null_words(left_null, right_null, left_npc);

    output.extend_pk_bytes(left_batch.get_pk_bytes(left_row));
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    write_left_payload(output, left_batch, left_row, left_null, left_schema);

    // Right payload columns (from cursor public API)
    let right_blob = right_cursor.blob_slice();  // &[u8], bounds carried
    for (rpi, ci, col) in right_schema.payload_columns() {
        let cs = col.size() as usize;
        let is_null = (right_null >> rpi) & 1 != 0;
        let out_pi = left_npc + rpi;
        if is_null {
            output.fill_col_zero(out_pi, cs);
        } else if is_german_string(col.type_code) {
            let ptr = right_cursor.col_ptr(ci, 16);
            if ptr.is_null() {
                output.fill_col_zero(out_pi, 16);
            } else {
                // SAFETY: ptr is a valid col_ptr (non-null checked above) to a
                // 16-byte German-string struct; relocate_german_string_vec
                // bounds-checks the blob offset against right_blob.
                let src = unsafe { std::slice::from_raw_parts(ptr, 16) };
                let dest = crate::schema::relocate_german_string_vec(
                    src, right_blob, &mut output.blob, None,
                );
                output.extend_col(out_pi, &dest);
            }
        } else {
            let ptr = right_cursor.col_ptr(ci, cs);
            if ptr.is_null() {
                output.fill_col_zero(out_pi, cs);
            } else {
                let src = unsafe { std::slice::from_raw_parts(ptr, cs) };
                output.extend_col(out_pi, src);
            }
        }
    }

    output.count += 1;
}

/// Write one null-filled join output row: left columns from batch, right columns all NULL.
fn write_join_row_null_right(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) {
    let left_null = left_batch.get_null_word(left_row);

    let left_npc = left_schema.num_payload_cols();
    let right_npc = right_schema.num_payload_cols();
    let right_null_bits = all_payload_null_mask(right_npc);
    let null_word = merge_null_words(left_null, right_null_bits, left_npc);

    output.extend_pk_bytes(left_batch.get_pk_bytes(left_row));
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    write_left_payload(output, left_batch, left_row, left_null, left_schema);

    // Right payload columns: all zeros (null)
    for (rpi, _ci, col) in right_schema.payload_columns() {
        output.fill_col_zero(left_npc + rpi, col.size() as usize);
    }

    output.count += 1;
}

// ---------------------------------------------------------------------------
// Anti-join / Semi-join delta-delta
// ---------------------------------------------------------------------------

/// Emit batch_a rows whose (PK, payload) has NO positive-weight match in batch_b.
/// For SQL EXCEPT: a row is excluded only if B has a matching (PK, payload), not just PK.
pub fn op_anti_join_delta_delta(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
) -> ConsolidatedBatch {
    ConsolidatedBatch::new_unchecked(filter_join_dd_with_payload(batch_a, batch_b, schema))
}

/// Emit batch_a rows whose PK HAS a positive-weight match in batch_b.
pub fn op_semi_join_delta_delta(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
) -> ConsolidatedBatch {
    ConsolidatedBatch::new_unchecked(semi_join_dd(batch_a, batch_b, schema))
}

/// PK-only semi-join DD: emit each A PK group iff B has a positive-weight row at
/// that PK. `cogroup_intersection` over a `BatchCursor` on B visits only shared
/// PKs (galloping past the gaps — the skewed tiny ΔB ⋈ huge ΔA case gets the same
/// speedup the delta-trace joins do), and the callback brackets B's equal-PK
/// group by index to test presence.
fn semi_join_dd(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
) -> Batch {
    let npc = schema.num_payload_cols();
    let cs_a = Batch::consolidate_if_needed(batch_a, schema);
    let cs_b = Batch::consolidate_if_needed(batch_b, schema);
    let ca: &Batch = cs_a.as_deref().unwrap_or(batch_a);
    let cb: &Batch = cs_b.as_deref().unwrap_or(batch_b);
    let n_a = ca.count;
    if n_a == 0 {
        return Batch::empty(npc, schema.pk_stride());
    }

    let mut output = Batch::empty_with_schema(schema);
    let mut bc = BatchCursor::new(cb);

    cogroup_intersection(ca, &mut bc, |_key, a_range, m| {
        // Intersection ⇒ m is at B's equal-PK group; bracket and test presence.
        let b_end = m.group_end();
        let present = (m.pos..b_end).any(|j| cb.get_weight(j) > 0);
        m.seek(b_end);
        if present {
            output.append_batch(ca, a_range.start, a_range.end);
        }
    });

    output.sorted = true;
    output.consolidated = true;
    gnitz_debug!("op_semi_join_dd: a={} b={} out={}", n_a, cb.count, output.count);
    output
}

/// Payload-aware anti-join DD: excludes A rows only when B has a matching
/// (PK, payload) with positive weight. Used for SQL EXCEPT.
fn filter_join_dd_with_payload(
    batch_a: &Batch,
    batch_b: &Batch,
    schema: &SchemaDescriptor,
) -> Batch {
    let cs_a = Batch::consolidate_if_needed(batch_a, schema);
    let cs_b = Batch::consolidate_if_needed(batch_b, schema);
    let ca: &Batch = cs_a.as_deref().unwrap_or(batch_a);
    let cb: &Batch = cs_b.as_deref().unwrap_or(batch_b);

    with_payload_cmp!(schema, filter_join_dd_with_payload_inner, ca, cb, schema)
}

#[inline]
fn filter_join_dd_with_payload_inner<RowCmp>(
    ca: &Batch,
    cb: &Batch,
    schema: &SchemaDescriptor,
    row_cmp: RowCmp,
) -> Batch
where
    RowCmp: Fn(&SchemaDescriptor, &MemBatch, usize, &MemBatch, usize) -> Ordering + Copy,
{
    let npc = schema.num_payload_cols();
    let n_a = ca.count;
    let n_b = cb.count;

    if n_a == 0 {
        return Batch::empty(npc, schema.pk_stride());
    }

    let mb_a = ca.as_mem_batch();
    let mb_b = cb.as_mem_batch();
    let mut output = Batch::empty_with_schema(schema);
    let mut bc = BatchCursor::new(cb);

    // `cogroup_left` visits every A PK group (anti emits per A row whether or not
    // B matches). Inside each group, a (PK, payload) sub-merge walks A's rows and
    // B's equal-PK group in lockstep — both payload-sorted — emitting each A row
    // with no positive-weight (PK, payload) match in B, coalescing unmatched runs.
    cogroup_left(ca, &mut bc, |key, a_range, m| {
        // B's equal-PK group, or an empty range when B lacks the key (cogroup_left
        // lands m at lower_bound(key), which may be a later key's row).
        let has_b = m.pos < cb.count && cb.get_pk_bytes(m.pos) == key;
        let (b_start, b_end) = if has_b {
            (m.pos, m.group_end())
        } else {
            (m.pos, m.pos)
        };
        let mut scan_b = b_start;
        let mut unmatched_start: Option<usize> = None;
        for idx_a_row in a_range.clone() {
            // Reuse the comparison that ends the advance: when the loop breaks
            // with scan_b in range, `ord` already holds the (≥ Equal) result for
            // the current B row, so the match test needs no second row_cmp.
            let mut ord = Ordering::Greater;
            while scan_b < b_end {
                ord = row_cmp(schema, &mb_b, scan_b, &mb_a, idx_a_row);
                if ord != Ordering::Less {
                    break;
                }
                scan_b += 1;
            }
            let matched = scan_b < b_end
                && ord == Ordering::Equal
                && cb.get_weight(scan_b) > 0;
            if !matched {
                if unmatched_start.is_none() {
                    unmatched_start = Some(idx_a_row);
                }
            } else if let Some(rs) = unmatched_start.take() {
                output.append_batch(ca, rs, idx_a_row);
            }
        }
        if let Some(rs) = unmatched_start.take() {
            output.append_batch(ca, rs, a_range.end);
        }
        m.seek(b_end);
    });

    output.sorted = true;
    output.consolidated = true;
    gnitz_debug!("op_anti_join_dd: a={} b={} out={}", n_a, n_b, output.count);
    output
}

// ---------------------------------------------------------------------------
// Inner join delta-delta
// ---------------------------------------------------------------------------

/// Delta-Delta inner join: ΔA ⋈ ΔB. Cartesian product per matching PK group.
/// Output schema: [left_PK, left_payload..., right_payload...].
/// Weight = w_a * w_b (wrapping). Zero-weight pairs are skipped.
pub fn op_join_delta_delta(
    batch_a: &Batch,
    batch_b: &Batch,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) -> Batch {
    let left_npc = left_schema.num_payload_cols();
    let right_npc = right_schema.num_payload_cols();
    let out_npc = left_npc + right_npc;

    let cs_a = Batch::consolidate_if_needed(batch_a, left_schema);
    let cs_b = Batch::consolidate_if_needed(batch_b, right_schema);
    let ca: &Batch = cs_a.as_deref().unwrap_or(batch_a);
    let cb: &Batch = cs_b.as_deref().unwrap_or(batch_b);
    let n_a = ca.count;
    let n_b = cb.count;

    if n_a == 0 || n_b == 0 {
        gnitz_debug!("op_join_dd: a={} b={} out=0", n_a, n_b);
        return Batch::empty(out_npc, left_schema.pk_stride());
    }

    let mb_a = ca.as_mem_batch();
    let mb_b = cb.as_mem_batch();
    let mut output = Batch::empty_joined(left_schema, right_schema);
    let mut bc = BatchCursor::new(cb);

    // `cogroup_intersection` over a `BatchCursor` on B emits at shared PKs only,
    // galloping past the gaps (the skewed tiny ⋈ huge case gets the same speedup
    // the delta-trace joins get). Inside each group, bracket B's equal-PK group
    // by index and emit the A-major × B-inner cartesian product (left payload
    // ascending then right payload ascending ⇒ output stays (PK, payload)-sorted).
    cogroup_intersection(ca, &mut bc, |_key, a_range, m| {
        let b_start = m.pos;
        let b_end = m.group_end();
        for i in a_range {
            let wa = ca.get_weight(i);
            if wa == 0 {
                continue;
            }
            for j in b_start..b_end {
                let wb = cb.get_weight(j);
                let w_out = wa.wrapping_mul(wb);
                if w_out != 0 {
                    write_join_row_from_batches(
                        &mut output,
                        &mb_a, i, &mb_b, j,
                        w_out,
                        left_schema, right_schema,
                    );
                }
            }
        }
        m.seek(b_end);
    });

    output.sorted = true;
    gnitz_debug!("op_join_dd: a={} b={} out={}", n_a, n_b, output.count);
    output
}

/// Write one composite join output row where both sides come from MemBatch.
#[allow(clippy::too_many_arguments)]
fn write_join_row_from_batches(
    output: &mut Batch,
    left_batch: &MemBatch,
    left_row: usize,
    right_batch: &MemBatch,
    right_row: usize,
    weight: i64,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
) {
    let left_null = left_batch.get_null_word(left_row);
    let right_null = right_batch.get_null_word(right_row);

    let left_npc = left_schema.num_payload_cols();
    let null_word = merge_null_words(left_null, right_null, left_npc);

    output.extend_pk_bytes(left_batch.get_pk_bytes(left_row));
    output.extend_weight(&weight.to_le_bytes());
    output.extend_null_bmp(&null_word.to_le_bytes());

    write_left_payload(output, left_batch, left_row, left_null, left_schema);

    // Right payload columns (from right MemBatch)
    for (rpi, _ci, col) in right_schema.payload_columns() {
        let cs = col.size() as usize;
        let is_null = (right_null >> rpi) & 1 != 0;
        let out_pi = left_npc + rpi;
        if is_null {
            output.fill_col_zero(out_pi, cs);
        } else if is_german_string(col.type_code) {
            write_string_from_batch(output, out_pi, right_batch, right_row, rpi);
        } else {
            output.extend_col(out_pi, right_batch.get_col_ptr(right_row, rpi, cs));
        }
    }

    output.count += 1;
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{SchemaColumn, SchemaDescriptor, type_code, SHORT_STRING_THRESHOLD};
    use crate::storage::{Batch, ConsolidatedBatch};
    use crate::test_support::{make_wide_batch, wide_pk_3xu64_schema};

    fn make_schema_u64_i64() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_schema_u64_string() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::STRING, 0),
            ],
            &[0],
        )
    }

    fn make_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, i64)],
    ) -> ConsolidatedBatch {
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
        ConsolidatedBatch::new_unchecked(b)
    }

    fn make_batch_str(
        schema: &SchemaDescriptor,
        rows: &[(u64, i64, &str)],
    ) -> ConsolidatedBatch {
        let n = rows.len();
        let mut b = Batch::with_schema(*schema, n.max(1));

        for &(pk, w, s) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());

            let bytes = s.as_bytes();
            let length = bytes.len() as u32;
            let mut gs = [0u8; 16];
            gs[0..4].copy_from_slice(&length.to_le_bytes());
            if bytes.len() <= SHORT_STRING_THRESHOLD {
                let copy_len = bytes.len().min(12);
                gs[4..4 + copy_len].copy_from_slice(&bytes[..copy_len]);
            } else {
                gs[4..8].copy_from_slice(&bytes[..4]);
                let offset = b.blob.len() as u64;
                gs[8..16].copy_from_slice(&offset.to_le_bytes());
                b.blob.extend_from_slice(bytes);
            }
            b.extend_col(0, &gs);
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    fn read_str_payload(batch: &Batch, col: usize, row: usize) -> String {
        let off = row * 16;
        let gs = &batch.col_data(col)[off..off + 16];
        let length = u32::from_le_bytes(gs[0..4].try_into().unwrap()) as usize;
        if length == 0 {
            return String::new();
        }
        if length <= SHORT_STRING_THRESHOLD {
            String::from_utf8_lossy(&gs[4..4 + length]).to_string()
        } else {
            let blob_offset = u64::from_le_bytes(gs[8..16].try_into().unwrap()) as usize;
            String::from_utf8_lossy(&batch.blob[blob_offset..blob_offset + length]).to_string()
        }
    }

    fn get_payload_i64(b: &Batch, row: usize) -> i64 {
        crate::util::read_i64_le(b.col_data(0), row * 8)
    }

    fn make_schema_compound() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        )
    }

    /// Pack a compound `(U64, U64)` PK into its 16-byte region: col0 then col1,
    /// each little-endian — the layout storage stores and orders by.
    fn compound_pk_bytes(c0: u64, c1: u64) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..8].copy_from_slice(&c0.to_le_bytes());
        b[8..16].copy_from_slice(&c1.to_le_bytes());
        b
    }

    fn make_compound_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, u64, i64, i64)],
    ) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(c0, c1, w, val) in rows {
            b.extend_pk_bytes(&compound_pk_bytes(c0, c1));
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    /// Build a band-join trace/delta over the `(U64 k, U64 range)` compound PK
    /// using OPK (big-endian) encoding, so memcmp on the PK region is numeric
    /// order — the contract the range probe relies on. Rows must be passed in
    /// ascending `(k, range)` order. (`make_compound_batch` writes native LE
    /// bytes, correct only for the equality DD joins that compare PKs for
    /// equality, not the range probe that compares them for order.)
    fn make_band_batch(
        schema: &SchemaDescriptor,
        rows: &[(u64, u64, i64, i64)],
    ) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(k, range, w, val) in rows {
            b.extend_pk_opk(schema, &[k as u128, range as u128]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    fn make_schema_signed() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_signed_batch(
        schema: &SchemaDescriptor,
        rows: &[(i64, i64, i64)],
    ) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            // Signed PK at rest is OPK (big-endian, sign-bit flipped) so the
            // join's compare_pk_bytes merge orders it correctly; the raw
            // `extend_pk` would store non-order-preserving native bytes.
            b.extend_pk_opk(schema, &[(pk as u64) as u128]);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    /// Decode a single signed-I64 PK at `row` from its OPK bytes to native.
    fn signed_pk_i64(b: &Batch, row: usize) -> i64 {
        let mut le = [0u8; 8];
        gnitz_wire::decode_pk_column(b.get_pk_bytes(row), type_code::I64, &mut le);
        i64::from_le_bytes(le)
    }

    // -----------------------------------------------------------------------
    // Anti-join DD tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dd_basic() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        // B has PK=2 but different payload (200 vs 20) → payload-aware: no match
        let b = make_batch(&schema, &[(2, 1, 200)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        // All 3 rows survive (different payloads)
        assert_eq!(out.count, 3);
        assert!(out.consolidated);
    }

    #[test]
    fn test_anti_join_dd_basic_matching_payload() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        // B has PK=2 with SAME payload (20) → exact match → excluded
        let b = make_batch(&schema, &[(2, 1, 20)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!((out.get_pk(1) as u64), 3);
    }

    #[test]
    fn test_anti_join_dd_empty_right() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let b = make_batch(&schema, &[]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!((out.get_pk(1) as u64), 2);
    }

    #[test]
    fn test_anti_join_dd_full_overlap() {
        let schema = make_schema_u64_i64();
        // Same PK AND same payload → full match → all excluded
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let b = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 0);
    }

    #[test]
    fn test_anti_join_dd_negative_weight_b() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        // PK=1: B has negative weight → should NOT suppress A rows
        // PK=2: B has matching payload (20) with positive weight → excluded
        let b = make_batch(&schema, &[(1, -1, 10), (2, 1, 20)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 1);
        assert_eq!((out.get_pk(0) as u64), 1);
    }

    /// Pin: the GENERIC `RowCmp` arm (`compare_rows`, German-string comparison)
    /// must drive `filter_join_dd_with_payload_inner`'s shared-PK (PK, payload)
    /// sub-merge. A `(U64 pk, STRING payload)` schema resolves to
    /// `PayloadCmpKind::Generic`, so `with_payload_cmp!` threads the
    /// string-aware comparator into the anti-join.
    ///
    /// A = [(1,1,"drop"), (1,1,"keep")] (payload-sorted: "drop" < "keep"),
    /// B = [(1,1,"drop")]. The anti-join (EXCEPT) excludes an A row only when B
    /// has a *matching (PK, payload)*: "drop" matches and is dropped; "keep" has
    /// no match and survives. The fixed-int comparator treats the German-string
    /// structs as raw 8-byte integers — it cannot tell "drop" from "keep"
    /// correctly, so it either suppresses the wrong row (dropping "keep") or
    /// fails to suppress "drop", changing the surviving string.
    #[test]
    fn test_anti_join_dd_generic_rowcmp_same_pk_string() {
        let schema = make_schema_u64_string();
        // Guard: the schema must select the GENERIC comparator, else this pin
        // would not exercise the string-aware arm.
        assert_eq!(
            schema.payload_cmp,
            crate::schema::PayloadCmpKind::Generic,
            "U64+STRING schema must use the GENERIC payload comparator",
        );

        // Rows supplied in (PK, payload) order ("drop" < "keep") so the
        // sorted+consolidated flags `make_batch_str` sets are truthful.
        let a = make_batch_str(&schema, &[(1, 1, "drop"), (1, 1, "keep")]);
        let b = make_batch_str(&schema, &[(1, 1, "drop")]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);

        assert_eq!(out.count, 1, "only the unmatched A row survives EXCEPT");
        assert_eq!(out.get_pk(0) as u64, 1);
        // The surviving row is "keep" — "drop" matched B and was excluded.
        assert_eq!(
            read_str_payload(&out, 0, 0), "keep",
            "generic string compare must keep \"keep\" and drop \"drop\"",
        );
    }

    // -----------------------------------------------------------------------
    // Semi-join DD tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_semi_join_dd_basic() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);
        let b = make_batch(&schema, &[(2, 1, 200), (3, 1, 300)]);
        let out = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 2);
        assert_eq!((out.get_pk(1) as u64), 3);
        assert!(out.consolidated);
    }

    #[test]
    fn test_anti_semi_complement_matching_payloads() {
        // When B has matching payloads, anti + semi = total
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40)]);
        let b = make_batch(&schema, &[(2, 1, 20), (4, 1, 40)]);
        let total = a.count;
        let anti = op_anti_join_delta_delta(&a, &b, &schema);
        let semi = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(anti.count + semi.count, total);
    }

    // -----------------------------------------------------------------------
    // Join DD tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_cartesian() {
        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();
        // PK=1: 2 rows in a, 3 rows in b → 6 output rows
        let a = make_batch(&left_schema, &[(1, 1, 10), (1, 2, 20)]);
        let b = make_batch(&right_schema, &[(1, 1, 100), (1, 3, 300), (1, -1, 500)]);
        let out = op_join_delta_delta(&a, &b, &left_schema, &right_schema);
        // 2 × 3 = 6, but check for zero-weight elimination:
        // (w=1*1=1), (w=1*3=3), (w=1*-1=-1), (w=2*1=2), (w=2*3=6), (w=2*-1=-2)
        // All non-zero → 6 rows
        assert_eq!(out.count, 6);
        assert!(out.sorted);

        // Check weights: row 0 = (a[0] × b[0]) = 1*1 = 1
        assert_eq!(out.get_weight(0), 1);
        // row 1 = (a[0] × b[1]) = 1*3 = 3
        assert_eq!(out.get_weight(1), 3);
    }

    #[test]
    fn test_join_dd_string_columns() {
        let left_schema = make_schema_u64_string();
        let right_schema = make_schema_u64_string();
        let a = make_batch_str(&left_schema, &[(1, 1, "hello")]);
        let b = make_batch_str(&right_schema, &[(1, 1, "world_longer_than_twelve")]);
        let out = op_join_delta_delta(&a, &b, &left_schema, &right_schema);
        assert_eq!(out.count, 1);
        // Left payload = col 0, right payload = col 1
        let left_str = read_str_payload(&out, 0, 0);
        let right_str = read_str_payload(&out, 1, 0);
        assert_eq!(left_str, "hello");
        assert_eq!(right_str, "world_longer_than_twelve");
    }

    #[test]
    fn test_join_dd_empty_inputs() {
        let ls = make_schema_u64_i64();
        let rs = make_schema_u64_i64();

        let out1 = op_join_delta_delta(&make_batch(&ls, &[]), &make_batch(&rs, &[(1, 1, 100)]), &ls, &rs);
        assert_eq!(out1.count, 0);

        let out2 = op_join_delta_delta(&make_batch(&rs, &[(1, 1, 100)]), &make_batch(&ls, &[]), &ls, &rs);
        assert_eq!(out2.count, 0);

        let out3 = op_join_delta_delta(&make_batch(&ls, &[]), &make_batch(&rs, &[]), &ls, &rs);
        assert_eq!(out3.count, 0);
    }

    #[test]
    fn test_join_dd_zero_weight_skip() {
        let ls = make_schema_u64_i64();
        let rs = make_schema_u64_i64();
        // a has weight=0 after consolidation wouldn't happen, but test the skip
        let a = make_batch(&ls, &[(1, 2, 10)]);
        // b has a pair that nets to zero weight after consolidation
        let mut b = Batch::with_schema(rs, 2);

        for &(pk, w, val) in &[(1u64, 1i64, 100i64), (1u64, -1i64, 100i64)] {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = false;

        let out = op_join_delta_delta(&a, &b, &ls, &rs);
        // b consolidates to empty (1 + -1 = 0 for same pk+payload) → no match
        assert_eq!(out.count, 0);
    }

    #[test]
    fn test_join_dd_wide_pk_works() {
        // Wide PK: 3× U64 = 24-byte stride. The byte-API port handles all widths
        // without panicking and produces correct Cartesian product output.
        let schema = wide_pk_3xu64_schema();
        let a = make_wide_batch(&schema, &[(1, 0, 0, 1, 7)]);
        // Self-join: (1,0,0) × (1,0,0) → one output row with weight 1*1=1.
        let out = op_join_delta_delta(&a, &a, &schema, &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_weight(0), 1);
    }

    #[test]
    fn test_write_join_row_compound_pk_bytes() {
        // Narrow compound-PK left: 2× U64, pk_stride = 16, no payload.
        let left_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
            ],
            &[0, 1],
        );
        let right_schema = make_schema_u64_i64();

        // Known 16-byte PK region: pk0 = 0x1122..., pk1 = 0xAABB...
        let pk0: u64 = 0x1122_3344_5566_7788;
        let pk1: u64 = 0xAABB_CCDD_EEFF_0011;
        let mut pk_bytes = [0u8; 16];
        pk_bytes[0..8].copy_from_slice(&pk0.to_le_bytes());
        pk_bytes[8..16].copy_from_slice(&pk1.to_le_bytes());

        let mut left = Batch::with_schema(left_schema, 1);
        left.extend_pk_bytes(&pk_bytes);
        left.extend_weight(&1i64.to_le_bytes());
        left.extend_null_bmp(&0u64.to_le_bytes());
        left.count += 1;

        let right = make_batch(&right_schema, &[(7, 1, 42)]);

        // Output schema: 2 PK cols + right payload (1 I64) = 3 cols.
        let out_schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0, 1],
        );
        let mut output = Batch::with_schema(out_schema, 1);

        write_join_row_from_batches(
            &mut output,
            &left.as_mem_batch(),
            0,
            &right.as_mem_batch(),
            0,
            1,
            &left_schema,
            &right_schema,
        );

        assert_eq!(output.count, 1);
        assert_eq!(output.get_pk_bytes(0), &pk_bytes);
    }

    // -----------------------------------------------------------------------
    // op_anti_join_delta_trace / op_semi_join_delta_trace
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_basic() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: pk=2 and pk=4 exist
        let trace = Rc::new(make_batch(&schema, &[(2, 1, 200), (4, 1, 400)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: pk=1,2,3
        let delta = make_batch(&schema, &[(1, 1, 10), (2, 1, 20), (3, 1, 30)]);

        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // pk=2 is in trace, so output should be pk=1 and pk=3
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!((out.get_pk(1) as u64), 3);
        assert!(out.consolidated);
    }

    #[test]
    fn test_semi_join_dt_basic() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: pk=2 and pk=3 exist with positive weight
        let trace = Rc::new(make_batch(&schema, &[(2, 1, 200), (3, 1, 300)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: pk=1,2,3,4
        let delta = make_batch(&schema, &[
            (1, 1, 10), (2, 1, 20), (3, 1, 30), (4, 1, 40),
        ]);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Only pk=2 and pk=3 have trace matches
        assert_eq!(out.count, 2);
        assert_eq!((out.get_pk(0) as u64), 2);
        assert_eq!((out.get_pk(1) as u64), 3);
    }

    /// Compound PK: the co-group compares trace PK vs delta PK by bytes. Raw
    /// u128 order is last-column-major, so a byte compare (not u128) is required
    /// or the scan mislocates and matches/anti-matches are wrong.
    #[test]
    fn test_semi_anti_join_dt_compound_pk() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_compound();
        // Trace (storage order): (1,5) and (2,3).
        let trace = Rc::new(make_compound_batch(&schema, &[(1, 5, 1, 100), (2, 3, 1, 200)]).into_inner());

        // Delta (storage order): (1,5), (2,3), (3,1).
        let delta = make_compound_batch(&schema, &[(1, 5, 1, 10), (2, 3, 1, 20), (3, 1, 1, 30)]);

        let mut ch = CursorHandle::from_owned(&[Rc::clone(&trace)], schema);
        let semi = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(semi.count, 2, "semi: (1,5) and (2,3) match the trace");
        assert_eq!(semi.get_pk_bytes(0), &compound_pk_bytes(1, 5));
        assert_eq!(semi.get_pk_bytes(1), &compound_pk_bytes(2, 3));

        let mut ch2 = CursorHandle::from_owned(&[trace], schema);
        let anti = op_anti_join_delta_trace(&delta, ch2.cursor_mut(), &schema);
        assert_eq!(anti.count, 1, "anti: only (3,1) is absent from the trace");
        assert_eq!(anti.get_pk_bytes(0), &compound_pk_bytes(3, 1));
    }

    /// Signed single-column PK: negatives sort first in storage but last in raw
    /// u128, so the advance loop must use `current_pk_cmp`.
    #[test]
    fn test_semi_anti_join_dt_signed_pk() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_signed();
        // Trace (signed order): -3, -1.
        let trace = Rc::new(make_signed_batch(&schema, &[(-3, 1, 30), (-1, 1, 10)]).into_inner());

        // Delta (signed order): -3, -1, 2.
        let delta = make_signed_batch(&schema, &[(-3, 1, 1), (-1, 1, 2), (2, 1, 3)]);

        let mut ch = CursorHandle::from_owned(&[Rc::clone(&trace)], schema);
        let semi = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(semi.count, 2, "semi: -3 and -1 match the trace");
        assert_eq!(signed_pk_i64(&semi, 0), -3);
        assert_eq!(signed_pk_i64(&semi, 1), -1);

        let mut ch2 = CursorHandle::from_owned(&[trace], schema);
        let anti = op_anti_join_delta_trace(&delta, ch2.cursor_mut(), &schema);
        assert_eq!(anti.count, 1, "anti: only 2 is absent from the trace");
        assert_eq!(signed_pk_i64(&anti, 0), 2);
    }

    #[test]
    fn test_semi_join_dt_nonconsolidated() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: pk=1 exists
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Non-consolidated delta: pk=1 appears twice (w=2 and w=3)
        let mut delta = Batch::with_schema(schema, 2);
        for (pk, w, val) in [(1u64, 2i64, 10i64), (1u64, 3i64, 10i64)] {
            delta.extend_pk(pk as u128);
            delta.extend_weight(&w.to_le_bytes());
            delta.extend_null_bmp(&0u64.to_le_bytes());
            delta.extend_col(0, &val.to_le_bytes());
            delta.count += 1;
        }
        delta.sorted = true;
        delta.consolidated = false;

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // After consolidation of delta: pk=1 w=5 → matches trace → 1 row
        assert_eq!(out.count, 1);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!(out.get_weight(0), 5);
    }

    // -----------------------------------------------------------------------
    // op_join_delta_trace_outer
    // -----------------------------------------------------------------------

    #[test]
    fn test_outer_join_null_fill() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();

        // Right trace: pk=1 val=100
        let trace = Rc::new(make_batch(&right_schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);

        // Delta (left): pk=1 val=10 (matches), pk=2 val=20 (no match → null fill)
        let delta = make_batch(&left_schema, &[(1, 1, 10), (2, 1, 20)]);

        let out = op_join_delta_trace_outer(&delta, ch.cursor_mut(), &left_schema, &right_schema);

        assert_eq!(out.count, 2);
        // pk=1: matched, left payload=10, right payload=100
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!(get_payload_i64(&out, 0), 10); // left val

        // pk=2: no match, left payload=20, right columns null-filled
        assert_eq!((out.get_pk(1) as u64), 2);
        assert_eq!(get_payload_i64(&out, 1), 20); // left val
        // Right column (payload col 1) should be null
        let null_word = u64::from_le_bytes(out.null_bmp_data()[8..16].try_into().unwrap());
        assert!(null_word & 2 != 0, "right payload column (bit 1) should be null for non-matching row");
    }

    fn make_schema_i32() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I32, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        )
    }

    fn make_i32_batch(schema: &SchemaDescriptor, rows: &[(i32, i64, i64)]) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, val) in rows {
            b.extend_pk((pk as u32) as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    /// Inner join delta×trace on a narrow (I32, 4-byte) signed PK. Every delta
    /// key has a trace match; all must be found, including the smallest key
    /// (regression guard for the byte-seek path at sub-8-byte stride).
    #[test]
    fn test_join_dt_i32_key_all_match() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let left_schema = make_schema_i32();
        let right_schema = make_schema_i32();
        // Trace (right): keys 1 and 2, both present.
        let trace = Rc::new(make_i32_batch(&right_schema, &[(1, 1, 100), (2, 1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);
        // Delta (left): keys 1 and 2.
        let delta = make_i32_batch(&left_schema, &[(1, 1, 10), (2, 1, 20)]);

        let out = op_join_delta_trace(&delta, ch.cursor_mut(), &left_schema, &right_schema);
        assert_eq!(out.count, 2, "both I32 keys must join (smallest key not dropped)");
        assert_eq!(out.get_pk(0) as i32, 1);
        assert_eq!(out.get_pk(1) as i32, 2);
    }

    #[test]
    fn test_join_dt_many_to_many_output_unsorted() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Many-to-many inner join: the unified co-group walks each trace group
        // once and products it against the whole delta group, emitting the
        // cartesian product in trace-major order — left payloads interleave out
        // of (PK, payload) order, so the output must be marked unsorted.
        let schema = make_schema_u64_i64();
        // Trace: PK=1 with two right payloads (100, 200).
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        // Delta: PK=1 with three left payloads.
        let delta = make_batch(&schema, &[(1, 1, 10), (1, 1, 20), (1, 1, 30)]).into_inner();

        let out = op_join_delta_trace(&delta, ch.cursor_mut(), &schema, &schema);
        assert_eq!(out.count, 6, "3 left × 2 right = 6 join outputs");

        // col 0 = left payload, col 1 = right payload (all PKs equal). The sorted
        // flag may be true ONLY if the rows are actually in (left, right) order.
        let pairs: Vec<(i64, i64)> = (0..out.count)
            .map(|r| (
                crate::util::read_i64_le(out.col_data(0), r * 8),
                crate::util::read_i64_le(out.col_data(1), r * 8),
            ))
            .collect();
        let actually_sorted = pairs.windows(2).all(|w| w[0] <= w[1]);
        assert!(
            !out.sorted || actually_sorted,
            "many-to-many join marked output sorted but payload order is {pairs:?}",
        );
    }

    // -----------------------------------------------------------------------
    // Range join DT
    // -----------------------------------------------------------------------

    /// Right-payload (trace I64 col) + weight of each emitted range-join row, in
    /// emission order. The trace payload identifies which trace rows matched.
    fn range_out_pairs(out: &Batch) -> Vec<(i64, i64)> {
        (0..out.count)
            .map(|r| (crate::util::read_i64_le(out.col_data(1), r * 8), out.get_weight(r)))
            .collect()
    }

    fn trace_cursor(batch: ConsolidatedBatch, schema: SchemaDescriptor) -> crate::storage::CursorHandle {
        use std::rc::Rc;
        crate::storage::CursorHandle::from_owned(&[Rc::new(batch.into_inner())], schema)
    }

    /// All four rels over an unsigned key, n_eq = 0. The boundary case x == y
    /// must match `Le`/`Ge` and not `Lt`/`Gt`. The trace payload tags the rows
    /// {y=10→110, y=20→120, y=30→130}; the delta probes x = 20.
    #[test]
    fn test_range_dt_four_rels_unsigned() {
        let schema = make_schema_u64_i64();
        let cases = [
            (RangeRel::Lt, vec![110]),       // y < 20
            (RangeRel::Le, vec![110, 120]),  // y <= 20
            (RangeRel::Gt, vec![130]),       // y > 20
            (RangeRel::Ge, vec![120, 130]),  // y >= 20
        ];
        for (rel, want) in cases {
            let mut ch = trace_cursor(
                make_batch(&schema, &[(10, 1, 110), (20, 1, 120), (30, 1, 130)]), schema);
            let delta = make_batch(&schema, &[(20, 1, 200)]);
            let out = op_join_delta_trace_range(&delta, ch.cursor_mut(), &schema, &schema, 0, rel);
            let got: Vec<i64> = range_out_pairs(&out).into_iter().map(|(p, _)| p).collect();
            assert_eq!(got, want, "rel {rel:?}");
            // The left PK (delta range key x) and left payload survive on every row.
            for r in 0..out.count {
                assert_eq!(out.get_pk(r) as u64, 20);
                assert_eq!(crate::util::read_i64_le(out.col_data(0), r * 8), 200);
            }
        }
    }

    /// Band join (n_eq = 1): the probe stays inside the delta row's equality
    /// group. A trace row in the NEXT eq group with an in-range slot must NOT
    /// match.
    #[test]
    fn test_range_dt_eq_prefix_stops_at_group_edge() {
        let schema = make_schema_compound(); // (U64 k, U64 range) PK, I64 payload
        // Trace sorted by (k, range): k=1 group then k=2 group. k=2,y=5 has an
        // in-range slot for x=15 but is in a different group → must not match.
        let trace = make_band_batch(&schema, &[
            (1, 10, 1, 110),
            (1, 20, 1, 120),
            (2, 5,  1, 205),
        ]);
        let mut ch = trace_cursor(trace, schema);
        let delta = make_band_batch(&schema, &[(1, 15, 1, 200)]);
        // rel Lt: {y < 15 within k=1} = {10}. k=2,y=5 excluded by the group edge.
        let out = op_join_delta_trace_range(&delta, ch.cursor_mut(), &schema, &schema, 1, RangeRel::Lt);
        let got: Vec<i64> = range_out_pairs(&out).into_iter().map(|(p, _)| p).collect();
        assert_eq!(got, vec![110]);
    }

    /// Band join, `Le` with a maximal range slot (`d = u64::MAX`): the end cut's
    /// carry ripples into the eq prefix (next group's first key). The whole k=1
    /// group matches and the k=2 group does not — no panic, no spill.
    #[test]
    fn test_range_dt_eq_prefix_ripple_le_max() {
        let schema = make_schema_compound();
        let trace = make_band_batch(&schema, &[
            (1, 0,        1, 100),
            (1, u64::MAX, 1, 199),
            (2, 0,        1, 200),
        ]);
        let mut ch = trace_cursor(trace, schema);
        let delta = make_band_batch(&schema, &[(1, u64::MAX, 1, 9)]);
        // y <= u64::MAX within k=1 → both k=1 rows; k=2 excluded.
        let out = op_join_delta_trace_range(&delta, ch.cursor_mut(), &schema, &schema, 1, RangeRel::Le);
        let mut got: Vec<i64> = range_out_pairs(&out).into_iter().map(|(p, _)| p).collect();
        got.sort_unstable();
        assert_eq!(got, vec![100, 199]);
    }

    /// `n_eq = 0`, `rel = Lt`, multiple delta rows: every row's start cut is the
    /// all-`0x00` lower bound, so each probe re-seeks the cursor *backward* from
    /// where the previous row's walk ended. All matches must still be found.
    #[test]
    fn test_range_dt_lt_backward_reseek() {
        let schema = make_schema_u64_i64();
        let mut ch = trace_cursor(
            make_batch(&schema, &[(5, 1, 105), (15, 1, 115), (25, 1, 125)]), schema);
        // Two delta rows; consolidation sorts them ascending (10, 30). Processing
        // 30 after 10 forces a backward seek to 0x00 for the second probe.
        let delta = make_batch(&schema, &[(30, 1, 300), (10, 1, 100)]);
        let out = op_join_delta_trace_range(&delta, ch.cursor_mut(), &schema, &schema, 0, RangeRel::Lt);
        // x=10 → {y<10}={105}; x=30 → {y<30}={105,115,125}. 4 rows total.
        let mut pairs: Vec<(u64, i64)> = (0..out.count)
            .map(|r| (out.get_pk(r) as u64, crate::util::read_i64_le(out.col_data(1), r * 8)))
            .collect();
        pairs.sort_unstable();
        assert_eq!(pairs, vec![(10, 105), (30, 105), (30, 115), (30, 125)]);
    }

    /// Signed range pair via cross-sign promotion (both sides reindex to I64).
    /// Negative trace keys must order below positives — guards an OPK sign-flip
    /// regression at the probe layer (raw bytes would put -100 above +50).
    #[test]
    fn test_range_dt_signed_promoted_key() {
        let schema = make_schema_signed(); // I64 PK, I64 payload
        let trace = make_signed_batch(&schema, &[(-100, 1, 1), (0, 1, 2), (50, 1, 3)]);
        let mut ch = trace_cursor(trace, schema);
        let delta = make_signed_batch(&schema, &[(0, 1, 9)]);
        // Gt: {y > 0} = {50→3}. Lt: {y < 0} = {-100→1}.
        let mut ch2 = trace_cursor(
            make_signed_batch(&schema, &[(-100, 1, 1), (0, 1, 2), (50, 1, 3)]), schema);
        let out_gt = op_join_delta_trace_range(&delta, ch.cursor_mut(), &schema, &schema, 0, RangeRel::Gt);
        let out_lt = op_join_delta_trace_range(&delta, ch2.cursor_mut(), &schema, &schema, 0, RangeRel::Lt);
        assert_eq!(range_out_pairs(&out_gt).into_iter().map(|(p, _)| p).collect::<Vec<_>>(), vec![3]);
        assert_eq!(range_out_pairs(&out_lt).into_iter().map(|(p, _)| p).collect::<Vec<_>>(), vec![1]);
    }

    /// Output weight = `w_delta × w_trace` (wrapping); a retraction delta negates,
    /// a doubled trace doubles, and a zero-weight trace row is skipped.
    #[test]
    fn test_range_dt_weights() {
        let schema = make_schema_u64_i64();
        // Trace: y=10 (w=2), y=20 (w=0, must be skipped), y=30 (w=1).
        let mut trace = Batch::with_schema(schema, 3);
        for &(pk, w, val) in &[(10u64, 2i64, 110i64), (20, 0, 120), (30, 1, 130)] {
            trace.extend_pk(pk as u128);
            trace.extend_weight(&w.to_le_bytes());
            trace.extend_null_bmp(&0u64.to_le_bytes());
            trace.extend_col(0, &val.to_le_bytes());
            trace.count += 1;
        }
        trace.sorted = true;
        let mut ch = trace_cursor(ConsolidatedBatch::new_unchecked(trace), schema);
        // Delta retraction: x=40, w=-1, so all matches negate.
        let delta = make_batch(&schema, &[(40, -1, 400)]);
        // rel Lt: {y < 40} = all three, but y=20's product is -1*0 = 0 (skipped).
        let out = op_join_delta_trace_range(&delta, ch.cursor_mut(), &schema, &schema, 0, RangeRel::Lt);
        let mut got = range_out_pairs(&out);
        got.sort_unstable();
        // y=10: -1*2 = -2; y=30: -1*1 = -1; y=20 skipped.
        assert_eq!(got, vec![(110, -2), (130, -1)]);
    }

    /// `Gt` of the maximal slot (n_eq = 0, x = u64::MAX) is provably empty — the
    /// cut helper returns `None` and the probe emits nothing without seeking.
    #[test]
    fn test_range_dt_gt_max_is_empty() {
        let schema = make_schema_u64_i64();
        let mut ch = trace_cursor(make_batch(&schema, &[(10, 1, 110)]), schema);
        let delta = make_batch(&schema, &[(u64::MAX, 1, 1)]);
        let out = op_join_delta_trace_range(&delta, ch.cursor_mut(), &schema, &schema, 0, RangeRel::Gt);
        assert_eq!(out.count, 0);
    }

    /// Empty delta → empty output, no panic.
    #[test]
    fn test_range_dt_empty_delta() {
        let schema = make_schema_u64_i64();
        let mut ch = trace_cursor(make_batch(&schema, &[(10, 1, 110)]), schema);
        let delta = make_batch(&schema, &[]);
        let out = op_join_delta_trace_range(&delta, ch.cursor_mut(), &schema, &schema, 0, RangeRel::Le);
        assert_eq!(out.count, 0);
    }

    // -----------------------------------------------------------------------
    // Range join DT: Strategy 2 (merge walk) vs Strategy 1 (per-row) oracle
    // -----------------------------------------------------------------------

    use crate::test_rng::Rng;

    /// Schema with `n_eq` U64 equality columns + 1 U64 range column (all PK) and
    /// a single trailing I64 payload — the canonical band-join reindex shape.
    fn make_range_schema(n_eq: usize) -> SchemaDescriptor {
        let mut cols: Vec<SchemaColumn> =
            (0..n_eq + 1).map(|_| SchemaColumn::new(type_code::U64, 0)).collect();
        cols.push(SchemaColumn::new(type_code::I64, 0)); // payload
        let pk: Vec<u32> = (0..n_eq as u32 + 1).collect();
        SchemaDescriptor::new(&cols, &pk)
    }

    /// Build a batch over `make_range_schema(n_eq)`. Each row is
    /// `(eq_cols, range, weight, payload)`; the `n_eq + 1` PK columns are
    /// OPK-encoded (big-endian U64) so memcmp equals numeric order. Rows must be
    /// pre-sorted ascending by `(eq.., range)` — the merge walk groups by a
    /// forward scan and the cursor expects sorted PK bytes.
    fn make_range_batch(
        schema: &SchemaDescriptor,
        rows: &[(Vec<u64>, u64, i64, i64)],
    ) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for (eq, range, w, val) in rows {
            let mut vals: Vec<u128> = eq.iter().map(|&x| x as u128).collect();
            vals.push(*range as u128);
            b.extend_pk_opk(schema, &vals);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            b.extend_col(0, &val.to_le_bytes());
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    /// Random small `(eq.., range, weight, payload)` rows, sorted by `(eq.., range)`.
    /// Weights span `{-2,-1,0,1,2}` so the trace carries tombstones (weight ≤ 0)
    /// and the delta carries retractions; the small key space forces dense eq
    /// groups, boundary equality (`d == s`), and non-matching groups.
    fn gen_range_rows(rng: &mut Rng, n_eq: usize, n_rows: usize, key_space: u64)
        -> Vec<(Vec<u64>, u64, i64, i64)> {
        const WEIGHTS: [i64; 5] = [-2, -1, 0, 1, 2];
        let mut rows: Vec<(Vec<u64>, u64, i64, i64)> = (0..n_rows).map(|_| {
            let eq: Vec<u64> = (0..n_eq).map(|_| rng.gen_range(key_space)).collect();
            let range = rng.gen_range(key_space);
            let w = WEIGHTS[rng.gen_range(5) as usize];
            let val = rng.gen_range(4) as i64; // few payloads → multiset duplicates
            (eq, range, w, val)
        }).collect();
        rows.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        rows
    }

    /// Output as a `(pk_bytes, left_payload, right_payload) → Σweight` multiset,
    /// dropping net-zero entries — the consolidated form the two strategies must
    /// agree on regardless of (delta-major vs trace-major) emission order.
    fn range_multiset(out: &Batch) -> std::collections::BTreeMap<(Vec<u8>, i64, i64), i64> {
        let mut m = std::collections::BTreeMap::new();
        for r in 0..out.count {
            let pk = out.get_pk_bytes(r).to_vec();
            let lp = crate::util::read_i64_le(out.col_data(0), r * 8);
            let rp = crate::util::read_i64_le(out.col_data(1), r * 8);
            *m.entry((pk, lp, rp)).or_insert(0i64) += out.get_weight(r);
        }
        m.retain(|_, w| *w != 0);
        m
    }

    /// Assert the trace-driven merge (Strategy 2) emits the same consolidated
    /// multiset as the per-row probe (Strategy 1, the shipped oracle). One trace
    /// batch drives both: per-row seeks absolutely, then a rewind feeds the merge
    /// from the head — exactly what the selector does. Returns the merged row
    /// count so callers can assert non-vacuous coverage.
    fn assert_merge_eq_per_row(
        schema: SchemaDescriptor,
        n_eq: usize,
        rel: RangeRel,
        delta: &Batch,
        trace: ConsolidatedBatch,
    ) -> usize {
        let mut ch = trace_cursor(trace, schema);
        let oracle = range_per_row_seek(delta, ch.cursor_mut(), &schema, &schema, n_eq, rel);
        ch.cursor_mut().rewind();
        let merged = range_merge_walk(delta, ch.cursor_mut(), &schema, &schema, n_eq, rel);
        assert!(!merged.sorted, "merge output must be marked unsorted");
        assert_eq!(
            range_multiset(&oracle), range_multiset(&merged),
            "merge vs per-row mismatch: n_eq={n_eq} rel={rel:?}",
        );
        merged.count
    }

    /// Differential: the merge walk matches the per-row oracle over random
    /// delta + trace for all four rels and `n_eq ∈ {0, 1, 2}`. The small key
    /// space exercises dense groups, boundary equality, tombstones (trace weight
    /// 0/≤0), retractions (delta weight < 0), multiset duplicates, empty trace,
    /// and non-matching groups in one sweep.
    #[test]
    fn test_range_dt_merge_vs_per_row_differential() {
        let rels = [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge];
        let mut rng = Rng::new(0xC0FF_EE12_3456_789A);
        let mut total = 0usize;
        for n_eq in 0..=2usize {
            let schema = make_range_schema(n_eq);
            for &rel in &rels {
                for _ in 0..40 {
                    let n_delta = 1 + rng.gen_range(7) as usize;
                    let n_trace = rng.gen_range(8) as usize;
                    let delta_rows = gen_range_rows(&mut rng, n_eq, n_delta, 4);
                    let trace_rows = gen_range_rows(&mut rng, n_eq, n_trace, 4);
                    let delta = make_range_batch(&schema, &delta_rows);
                    let trace = make_range_batch(&schema, &trace_rows);
                    total += assert_merge_eq_per_row(schema, n_eq, rel, &delta, trace);
                }
            }
        }
        assert!(total > 0, "differential never emitted a row — inputs too sparse");
    }

    /// `d == s` boundary: `Le`/`Ge` include the equal trace slot, `Lt`/`Gt`
    /// exclude it. Merge must split it exactly where the per-row cut does.
    #[test]
    fn test_range_dt_merge_boundary_equality() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(20, 1, 200)]);
        for rel in [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge] {
            let trace = make_batch(&schema, &[(10, 1, 110), (20, 1, 120), (30, 1, 130)]);
            assert_merge_eq_per_row(schema, 0, rel, &delta, trace);
        }
    }

    /// Maximal range slot, `n_eq = 0`: `Gt` of `u64::MAX` is provably empty in
    /// the per-row path (`range_cut_points → None`); the merge must agree (the
    /// maximal delta row is never `< s`, so the prefix excludes it). All rels.
    #[test]
    fn test_range_dt_merge_maximal_slot_n_eq0() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(u64::MAX, 1, 9)]);
        for rel in [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge] {
            let trace = make_batch(&schema, &[(0, 1, 100), (50, 1, 150), (u64::MAX, 1, 199)]);
            assert_merge_eq_per_row(schema, 0, rel, &delta, trace);
        }
    }

    /// Maximal slot in a non-maximal eq group (`n_eq = 1`): `Gt`/`Ge` of
    /// `u64::MAX` in group k=1 yield a zero-width `(next_E‖0, next_E‖0)` cut — no
    /// match, and crucially no spill into the k=2 group. Merge must agree.
    #[test]
    fn test_range_dt_merge_maximal_slot_n_eq1() {
        let schema = make_schema_compound(); // (U64 k, U64 range), I64 payload
        let delta = make_band_batch(&schema, &[(1, u64::MAX, 1, 9)]);
        for rel in [RangeRel::Gt, RangeRel::Ge, RangeRel::Lt, RangeRel::Le] {
            let trace = make_band_batch(&schema, &[(1, 0, 1, 100), (1, 50, 1, 150), (2, 0, 1, 200)]);
            assert_merge_eq_per_row(schema, 1, rel, &delta, trace);
        }
    }

    /// Mid-group tombstone: a trace row with weight ≤ 0 must still advance the
    /// delta pointer (it participates in the monotone walk) yet emit nothing
    /// (its product is net-zero / suppressed). Covers a zero and a negative
    /// trace weight against the per-row oracle.
    #[test]
    fn test_range_dt_merge_tombstone_advances_pointer() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(40, -1, 400)]); // retraction
        for rel in [RangeRel::Lt, RangeRel::Le] {
            // y=20 is a tombstone (w=0), y=25 carries a negative weight.
            let trace = make_batch(&schema, &[(10, 1, 110), (20, 0, 120), (25, -1, 125), (30, 1, 130)]);
            assert_merge_eq_per_row(schema, 0, rel, &delta, trace);
        }
    }

    /// High fan-out (`output ≫ r`), both pointer directions: every delta row
    /// matches most of its trace group. The suffix rel (`Lt`) over large `d` and
    /// the prefix rel (`Gt`) over small `d` each drive the monotone pointer the
    /// full width of the group; output must equal the oracle.
    #[test]
    fn test_range_dt_merge_high_fanout_both_directions() {
        let schema = make_schema_u64_i64();
        let trace_rows: Vec<(u64, i64, i64)> =
            (0..8u64).map(|y| (y, 1, 100 + y as i64)).collect();
        // suffix rel: large delta keys → each matches the whole low prefix.
        let delta_lt = make_batch(&schema, &[(5, 1, 1), (6, 1, 2), (7, 1, 3)]);
        let n1 = assert_merge_eq_per_row(schema, 0, RangeRel::Lt, &delta_lt,
            make_batch(&schema, &trace_rows));
        // prefix rel: small delta keys → each matches the whole high suffix.
        let delta_gt = make_batch(&schema, &[(0, 1, 1), (1, 1, 2), (2, 1, 3)]);
        let n2 = assert_merge_eq_per_row(schema, 0, RangeRel::Gt, &delta_gt,
            make_batch(&schema, &trace_rows));
        assert!(n1 > 8 && n2 > 8, "high fan-out should emit ≫ |trace| rows: {n1}, {n2}");
    }

    /// Multiset delta + multi-payload trace: duplicate `[eq‖d]` on both sides
    /// (distinct payloads). The merge must emit the full cross-product per trace
    /// row, weights multiplied — same as the per-row oracle.
    #[test]
    fn test_range_dt_merge_multiset_multipayload() {
        let schema = make_schema_u64_i64();
        // Trace PK=10 twice (distinct payloads); delta PK=15 twice.
        let trace = make_batch(&schema, &[(10, 1, 101), (10, 2, 102), (20, 1, 200)]);
        let delta = make_batch(&schema, &[(15, 1, 1), (15, 3, 2)]);
        // Lt: y < 15 → both PK=10 rows; each delta row matches both → 4 rows.
        assert_merge_eq_per_row(schema, 0, RangeRel::Lt, &delta, trace);
    }

    /// Seek-anchoring elides dead trace regions: the delta's covered span is a
    /// narrow slice of a large trace. `Gt`/`Ge` seek past the dead low head (to
    /// `succ(min_d)`); `Lt`/`Le` stop before the dead high tail (at `max_d`). Both
    /// must match the per-row oracle over the same large trace.
    #[test]
    fn test_range_dt_merge_seek_anchored_elides_dead_region() {
        let schema = make_schema_u64_i64();
        let trace_rows: Vec<(u64, i64, i64)> =
            (0..30u64).map(|y| (y, 1, 100 + y as i64)).collect();
        // Gt over a large min: covered = (25, 30); the 0..=25 head is seek-skipped.
        let delta_gt = make_batch(&schema, &[(25, 1, 1), (26, 1, 2)]);
        assert_merge_eq_per_row(schema, 0, RangeRel::Gt, &delta_gt,
            make_batch(&schema, &trace_rows));
        // Lt over a small max: covered = [0, 4); the 4..30 tail is early-stopped.
        let delta_lt = make_batch(&schema, &[(3, 1, 1), (4, 1, 2)]);
        assert_merge_eq_per_row(schema, 0, RangeRel::Lt, &delta_lt,
            make_batch(&schema, &trace_rows));
    }

    /// `n_eq = 0`, `Lt`/`Le`: one eq group spanning the whole trace, walked once.
    /// The degenerate single-group case the pure-range merge targets.
    #[test]
    fn test_range_dt_merge_n_eq0_single_walk() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(15, 1, 1), (25, 1, 2), (35, 1, 3)]);
        for rel in [RangeRel::Lt, RangeRel::Le] {
            let trace = make_batch(&schema, &[(10, 1, 110), (20, 1, 120), (30, 1, 130)]);
            assert_merge_eq_per_row(schema, 0, rel, &delta, trace);
        }
    }

    /// Empty trace with a non-empty delta: the merge (which the `n > trace_len`
    /// selector picks when `trace_len == 0`) walks zero trace rows and emits
    /// nothing — same as the per-row path landing every seek on an invalid cursor.
    #[test]
    fn test_range_dt_merge_empty_trace() {
        let schema = make_schema_u64_i64();
        let delta = make_batch(&schema, &[(10, 1, 1), (20, 1, 2)]);
        // Through the public selector: n=2 > trace_len=0 → merge path, empty out.
        let mut ch = trace_cursor(make_batch(&schema, &[]), schema);
        let out = op_join_delta_trace_range(&delta, ch.cursor_mut(), &schema, &schema, 0, RangeRel::Le);
        assert_eq!(out.count, 0);
    }

    /// Band join with non-matching groups (`n_eq = 1`): a trace eq group with no
    /// delta group (merge forward-skips it) and a delta eq group with no trace
    /// group (emits nothing). Only the shared group k=3 produces output.
    #[test]
    fn test_range_dt_merge_non_matching_groups() {
        let schema = make_schema_compound();
        // Trace groups k=1, k=3; delta groups k=2 (no trace), k=3 (matches).
        let delta = make_band_batch(&schema, &[(2, 7, 1, 2), (3, 7, 1, 3)]);
        for rel in [RangeRel::Lt, RangeRel::Le, RangeRel::Gt, RangeRel::Ge] {
            let trace = make_band_batch(&schema, &[(1, 5, 1, 15), (3, 5, 1, 35), (3, 9, 1, 39)]);
            assert_merge_eq_per_row(schema, 1, rel, &delta, trace);
        }
    }

    /// The public selector routes by size: an oversized delta (`n > trace_len`)
    /// takes the merge, a small delta the per-row path — both equal the per-row
    /// oracle. Pins the `n > trace_len` boundary end to end.
    #[test]
    fn test_range_dt_selector_routes_by_size() {
        let schema = make_schema_u64_i64();
        let trace_rows: Vec<(u64, i64, i64)> =
            (0..4u64).map(|y| (y * 10, 1, 100 + y as i64)).collect();
        // Oversized delta (8 > 4) → merge; small delta (2 ≤ 4) → per-row.
        for delta_rows in [
            vec![(5u64, 1i64, 0i64), (6, 1, 0), (7, 1, 0), (8, 1, 0),
                 (9, 1, 0), (11, 1, 0), (12, 1, 0), (13, 1, 0)],
            vec![(15, 1, 0), (25, 1, 0)],
        ] {
            let delta = make_batch(&schema, &delta_rows);
            // Oracle: force the per-row path directly.
            let mut ch_oracle = trace_cursor(make_batch(&schema, &trace_rows), schema);
            let oracle = range_per_row_seek(&delta, ch_oracle.cursor_mut(), &schema, &schema, 0, RangeRel::Lt);
            // Subject: the public selector.
            let mut ch = trace_cursor(make_batch(&schema, &trace_rows), schema);
            let out = op_join_delta_trace_range(&delta, ch.cursor_mut(), &schema, &schema, 0, RangeRel::Lt);
            assert_eq!(range_multiset(&oracle), range_multiset(&out));
        }
    }

    // -----------------------------------------------------------------------
    // Anti/Semi-join DT: cursor re-seek for same-PK rows
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_same_pk_different_payload() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: PK=1 exists with positive weight
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: two rows with PK=1 but different payloads (both should be matched)
        let delta = make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]);

        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Both rows have PK=1 which is in the trace → anti-join emits 0 rows
        assert_eq!(out.count, 0, "both same-PK rows should be matched by trace");
    }

    #[test]
    fn test_semi_join_dt_same_pk_different_payload() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: PK=1 exists with positive weight
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: two rows with PK=1 but different payloads (both should be matched)
        let delta = make_batch(&schema, &[(1, 1, 100), (1, 1, 200)]);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Both rows have PK=1 which is in the trace → semi-join emits 2 rows
        assert_eq!(out.count, 2, "both same-PK rows should be emitted by semi-join");
    }

    // -----------------------------------------------------------------------
    // Semi-join DT: large-delta regime weight inflation + u128::MAX sentinel
    // -----------------------------------------------------------------------

    #[test]
    fn test_semi_join_dt_large_delta_max_pk() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: PK=u64::MAX with positive weight — previously the sentinel
        // last_pk = u128::MAX would match this on first occurrence and skip it.
        let max_pk = u64::MAX;
        let trace = Rc::new(make_batch(&schema, &[(max_pk, 1, 0)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Build 25 delta rows: a large delta against a 1-row trace
        let mut rows: Vec<(u64, i64, i64)> = (1u64..=24).map(|i| (i, 1, i as i64)).collect();
        rows.push((max_pk, 1, 0));
        rows.sort_by_key(|r| r.0);
        let delta = make_batch(&schema, &rows);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Only PK=u64::MAX from delta matches — must emit exactly 1 row
        assert_eq!(out.count, 1, "PK=u64::MAX must match (large delta vs tiny trace)");
        assert_eq!((out.get_pk(0) as u64), max_pk);
    }

    #[test]
    fn test_semi_join_dt_no_weight_inflation() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;

        let schema = make_schema_u64_i64();

        // Trace: 3 entries for PK=1 with different payloads
        // This triggers the trace-has-many scenario
        let trace = Rc::new(make_batch(&schema, &[
            (1, 1, 100), (1, 1, 200), (1, 1, 300),
        ]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);

        // Delta: 25 rows — a large delta against a 3-row (same-PK) trace
        let mut rows: Vec<(u64, i64, i64)> = (1u64..=25).map(|pk| (pk, 1, pk as i64 * 10)).collect();
        rows.sort_by_key(|r| r.0);
        let delta = make_batch(&schema, &rows);

        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Only PK=1 from delta matches — should emit exactly 1 row, not 3
        assert_eq!(out.count, 1, "PK=1 should appear once, not inflated by trace duplicates");
        assert_eq!((out.get_pk(0) as u64), 1);
    }

    // -----------------------------------------------------------------------
    // Anti-join DD: payload-aware matching
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dd_same_pk_different_payload() {
        let schema = make_schema_u64_i64();
        // A: two rows with PK=1, different payloads
        let a = make_batch(&schema, &[(1, 1, 10), (1, 1, 20)]);
        // B: one row with PK=1, payload=20
        let b = make_batch(&schema, &[(1, 1, 20)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        // Only (PK=1, val=10) should survive — (PK=1, val=20) matches B exactly
        assert_eq!(out.count, 1);
        assert_eq!((out.get_pk(0) as u64), 1);
        assert_eq!(get_payload_i64(&out, 0), 10);
    }

    #[test]
    fn test_anti_join_dd_same_pk_same_payload() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10)]);
        let b = make_batch(&schema, &[(1, 1, 10)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        // Exact payload match → output should be empty
        assert_eq!(out.count, 0);
    }

    #[test]
    fn test_semi_join_dd_unchanged() {
        // Semi-join DD should still use PK-only semantics
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[(1, 1, 10), (1, 1, 20)]);
        let b = make_batch(&schema, &[(1, 1, 999)]); // different payload but same PK
        let out = op_semi_join_delta_delta(&a, &b, &schema);
        // PK-only: both A rows match B's PK → 2 rows
        assert_eq!(out.count, 2);
    }

    // -----------------------------------------------------------------------
    // BLOB payload copy in join writers (item 2)
    // -----------------------------------------------------------------------

    fn make_schema_u64_blob() -> SchemaDescriptor {
        SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::U64, 0),
                SchemaColumn::new(type_code::BLOB, 0),
            ],
            &[0],
        )
    }

    fn make_batch_blob(schema: &SchemaDescriptor, rows: &[(u64, i64, &[u8])]) -> ConsolidatedBatch {
        let mut b = Batch::with_schema(*schema, rows.len().max(1));
        for &(pk, w, bytes) in rows {
            b.extend_pk(pk as u128);
            b.extend_weight(&w.to_le_bytes());
            b.extend_null_bmp(&0u64.to_le_bytes());
            let length = bytes.len() as u32;
            let mut gs = [0u8; 16];
            gs[0..4].copy_from_slice(&length.to_le_bytes());
            if bytes.len() <= SHORT_STRING_THRESHOLD {
                gs[4..4 + bytes.len()].copy_from_slice(bytes);
            } else {
                gs[4..8].copy_from_slice(&bytes[..4]);
                let offset = b.blob.len() as u64;
                gs[8..16].copy_from_slice(&offset.to_le_bytes());
                b.blob.extend_from_slice(bytes);
            }
            b.extend_col(0, &gs);
            b.count += 1;
        }
        b.sorted = true;
        b.consolidated = true;
        ConsolidatedBatch::new_unchecked(b)
    }

    #[test]
    fn test_join_dd_blob_left_and_right_payload() {
        // BLOB payload columns on both sides of a DD join. write_left_payload
        // (left) and the right-side copy in write_join_row_from_batches must
        // relocate the blob, not copy the 16-byte struct verbatim.
        let left_schema = make_schema_u64_blob();
        let right_schema = make_schema_u64_blob();
        let left_blob: &[u8] = b"left-long-blob-value-beyond-twelve";
        let right_blob: &[u8] = b"right-long-blob-value-beyond-twelve";
        let a = make_batch_blob(&left_schema, &[(1, 1, left_blob)]);
        let b = make_batch_blob(&right_schema, &[(1, 1, right_blob)]);
        let out = op_join_delta_delta(&a, &b, &left_schema, &right_schema);
        assert_eq!(out.count, 1);
        assert!(!out.blob.is_empty(), "join output must carry blob buffer");
        // Left BLOB at out col 0, right BLOB at out col 1.
        let resolve = |col: usize| -> Vec<u8> {
            let gs = &out.col_data(col)[0..16];
            let len = u32::from_le_bytes(gs[0..4].try_into().unwrap()) as usize;
            let off = u64::from_le_bytes(gs[8..16].try_into().unwrap()) as usize;
            crate::schema::long_string_bytes(&out.blob, off, len).to_vec()
        };
        assert_eq!(resolve(0), left_blob, "left BLOB must resolve correctly");
        assert_eq!(resolve(1), right_blob, "right BLOB must resolve correctly");
    }

    // -----------------------------------------------------------------------
    // Stride consistency on early-exit empty join batches (item 3)
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_empty_stride_u64() {
        let ls = make_schema_u64_i64();
        let rs = make_schema_u64_i64();
        let out = op_join_delta_delta(&make_batch(&ls, &[]), &make_batch(&rs, &[(1, 1, 1)]), &ls, &rs);
        assert_eq!(out.count, 0);
        assert_eq!(out.pk_stride(), 8, "empty join output must use schema PK stride 8");
    }

    #[test]
    fn test_anti_join_dd_empty_stride_u64() {
        let schema = make_schema_u64_i64();
        let a = make_batch(&schema, &[]);
        let b = make_batch(&schema, &[(1, 1, 1)]);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 0);
        assert_eq!(out.pk_stride(), 8);
    }

    // -----------------------------------------------------------------------
    // Shift guards for 64-payload-column left schema (items 4b, 4c, 4d)
    // -----------------------------------------------------------------------

    fn make_wide_left_schema_64() -> SchemaDescriptor {
        let mut cols = vec![SchemaColumn::new(type_code::U64, 0)];
        for _ in 0..64 {
            cols.push(SchemaColumn::new(type_code::I64, 0));
        }
        SchemaDescriptor::new(&cols, &[0])
    }

    fn pk_only_schema() -> SchemaDescriptor {
        SchemaDescriptor::new(&[SchemaColumn::new(type_code::U64, 0)], &[0])
    }

    fn build_wide_left_row(left_schema: &SchemaDescriptor, pk: u64) -> Batch {
        let mut b = Batch::with_schema(*left_schema, 1);
        b.extend_pk(pk as u128);
        b.extend_weight(&1i64.to_le_bytes());
        b.extend_null_bmp(&0u64.to_le_bytes());
        for pi in 0..left_schema.num_payload_cols() {
            b.extend_col(pi, &0i64.to_le_bytes());
        }
        b.count += 1;
        b
    }

    #[test]
    fn test_write_join_row_from_batches_shift_guard_64() {
        // left_npc == 64, right_npc == 0: `right_null << 64` panics in debug.
        let left_schema = make_wide_left_schema_64();
        assert_eq!(left_schema.num_payload_cols(), 64);
        let right_schema = pk_only_schema();
        let left = build_wide_left_row(&left_schema, 1);
        let mut right = Batch::with_schema(right_schema, 1);
        right.extend_pk(1u128);
        right.extend_weight(&1i64.to_le_bytes());
        right.extend_null_bmp(&0u64.to_le_bytes());
        right.count += 1;

        let mut output = Batch::with_schema(left_schema, 1); // PK + 64 payload = 65 cols
        write_join_row_from_batches(
            &mut output, &left.as_mem_batch(), 0, &right.as_mem_batch(), 0,
            1, &left_schema, &right_schema,
        );
        assert_eq!(output.count, 1);
    }

    #[test]
    fn test_write_join_row_null_right_shift_guard_64() {
        let left_schema = make_wide_left_schema_64();
        let right_schema = pk_only_schema();
        let left = build_wide_left_row(&left_schema, 1);
        let mut output = Batch::with_schema(left_schema, 1);
        write_join_row_null_right(
            &mut output, &left.as_mem_batch(), 0, 1, &left_schema, &right_schema,
        );
        assert_eq!(output.count, 1);
    }

    #[test]
    fn test_write_join_row_shift_guard_64() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let left_schema = make_wide_left_schema_64();
        let right_schema = pk_only_schema();
        let left = build_wide_left_row(&left_schema, 1);

        let mut trace = Batch::with_schema(right_schema, 1);
        trace.extend_pk(1u128);
        trace.extend_weight(&1i64.to_le_bytes());
        trace.extend_null_bmp(&0u64.to_le_bytes());
        trace.count += 1;
        trace.sorted = true;
        trace.consolidated = true;
        let trace = Rc::new(trace);
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);
        let cursor = ch.cursor_mut();
        cursor.seek_bytes(&1u64.to_be_bytes());

        let mut output = Batch::with_schema(left_schema, 1);
        write_join_row(
            &mut output, &left.as_mem_batch(), 0, cursor, 1, &left_schema, &right_schema,
        );
        assert_eq!(output.count, 1);
    }

    // -----------------------------------------------------------------------
    // Anti-join DT uncompacted multi-entry trace (item 18)
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_uncompacted_tombstone_then_live() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let schema = make_schema_u64_i64();
        // Trace for PK=1 has two entries (sorted by payload): a tombstone
        // (payload 100, w=-1) then a live row (payload 200, w=+1). Net: PK=1
        // is present. Anti-join must NOT emit the delta row for PK=1.
        let trace = Rc::new(make_batch(&schema, &[(1, -1, 100), (1, 1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        let delta = make_batch(&schema, &[(1, 1, 5)]);
        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "PK=1 is in trace (net positive via second entry)");
    }

    #[test]
    fn test_semi_join_dt_uncompacted_tombstone_then_live() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let schema = make_schema_u64_i64();
        let trace = Rc::new(make_batch(&schema, &[(1, -1, 100), (1, 1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        let delta = make_batch(&schema, &[(1, 1, 5)]);
        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 1, "PK=1 has a positive-weight trace entry → emitted");
    }

    #[test]
    fn test_anti_join_dt_duplicate_pk_group() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let schema = make_schema_u64_i64();
        // Trace: PK=1 present (positive).
        let trace = Rc::new(make_batch(&schema, &[(1, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], schema);
        // Two consolidated delta rows with same PK=1, distinct payloads.
        let delta = make_batch(&schema, &[(1, 1, 10), (1, 1, 20)]);
        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 0, "both PK=1 rows excluded");
    }

    // -----------------------------------------------------------------------
    // Outer join uncompacted trace (item 25)
    // -----------------------------------------------------------------------

    #[test]
    fn test_outer_join_uncompacted_no_positive_entry() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();
        // Trace has a single tombstone entry for PK=1 (w=-1). No positive
        // entry → the key is NOT in Distinct(trace) → null-fill must be emitted.
        let trace = Rc::new(make_batch(&right_schema, &[(1, -1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);
        let delta = make_batch(&left_schema, &[(1, 1, 10)]);
        let out = op_join_delta_trace_outer(&delta, ch.cursor_mut(), &left_schema, &right_schema);
        // Expect: one inner row (w = 1 * -1 = -1) AND one null-extended row (w=+1).
        assert_eq!(out.count, 2, "inner row for the tombstone + null-fill (no positive match)");
        let mut weights: Vec<i64> = (0..out.count).map(|i| out.get_weight(i)).collect();
        weights.sort();
        assert_eq!(weights, vec![-1, 1]);
        // The null-filled row must have the right payload column null.
        let null_filled = (0..out.count).find(|&i| {
            let nw = u64::from_le_bytes(out.null_bmp_data()[i * 8..i * 8 + 8].try_into().unwrap());
            nw & 2 != 0
        });
        assert!(null_filled.is_some(), "a null-extended row must be present");
    }

    #[test]
    fn test_outer_join_uncompacted_positive_entry_no_null_fill() {
        use std::rc::Rc;
        use crate::storage::CursorHandle;
        let left_schema = make_schema_u64_i64();
        let right_schema = make_schema_u64_i64();
        // Trace for PK=1: a live entry (w=+1) and a tombstone (w=-1). The key
        // IS in Distinct(trace) → no null-fill.
        let trace = Rc::new(make_batch(&right_schema, &[(1, 1, 100), (1, -1, 200)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace], right_schema);
        let delta = make_batch(&left_schema, &[(1, 1, 10)]);
        let out = op_join_delta_trace_outer(&delta, ch.cursor_mut(), &left_schema, &right_schema);
        assert_eq!(out.count, 2, "two inner rows, no null-fill");
        for i in 0..out.count {
            let nw = u64::from_le_bytes(out.null_bmp_data()[i * 8..i * 8 + 8].try_into().unwrap());
            assert_eq!(nw & 2, 0, "no row should have a null right column");
        }
    }

    // -----------------------------------------------------------------------
    // Delta-delta join signed PK ordering (item 39)
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_signed_pk_match_found() {
        // left=[2], right sorted signed [-3, 2]. Shared key = 2. Raw-u128
        // comparison advances the left pointer past 2 (since 2 < (-3 as u128))
        // and misses the match. compare_pk_bytes dispatch must find it.
        let ls = make_schema_signed();
        let rs = make_schema_signed();
        let a = make_signed_batch(&ls, &[(2, 1, 10)]);
        let b = make_signed_batch(&rs, &[(-3, 1, 30), (2, 1, 20)]);
        let out = op_join_delta_delta(&a, &b, &ls, &rs);
        assert_eq!(out.count, 1, "key 2 is shared and must join");
        assert_eq!(signed_pk_i64(&out, 0), 2);
        assert_eq!(get_payload_i64(&out, 0), 10);
    }

    #[test]
    fn test_semi_anti_join_dd_signed_pk() {
        let schema = make_schema_signed();
        // a sorted signed: -3, 2. b sorted signed: -3.
        let a = make_signed_batch(&schema, &[(-3, 1, 1), (2, 1, 2)]);
        let b = make_signed_batch(&schema, &[(-3, 1, 1)]);
        let semi = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(semi.count, 1, "only -3 shares a PK");
        assert_eq!(signed_pk_i64(&semi, 0), -3);
        let anti = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(anti.count, 1, "2 is not in b");
        assert_eq!(signed_pk_i64(&anti, 0), 2);
    }

    #[test]
    fn test_join_dd_compound_pk_match_found() {
        // Compound (c0,c1). left=[(1,0)], right sorted [(0,1),(1,0)]. Shared
        // key (1,0). Raw-u128 packs c1 high, so (1,0)=1 < (0,1)=1<<64; the
        // walk would advance left past the match. Byte dispatch finds it.
        let schema = make_schema_compound();
        let a = make_compound_batch(&schema, &[(1, 0, 1, 10)]);
        let b = make_compound_batch(&schema, &[(0, 1, 1, 30), (1, 0, 1, 20)]);
        let out = op_join_delta_delta(&a, &b, &schema, &schema);
        assert_eq!(out.count, 1, "compound key (1,0) is shared");
        assert_eq!(out.get_pk_bytes(0), &compound_pk_bytes(1, 0));
    }

    // -----------------------------------------------------------------------
    // Wide-PK test helpers
    // -----------------------------------------------------------------------

    fn wide_pk_bytes(schema: &SchemaDescriptor, c0: u64, c1: u64, c2: u64) -> Vec<u8> {
        let mut tmp = Batch::with_schema(*schema, 1);
        tmp.extend_pk_opk(schema, &[c0 as u128, c1 as u128, c2 as u128]);
        tmp.get_pk_bytes(0).to_vec()
    }

    // -----------------------------------------------------------------------
    // Wide-PK inner-join multiset-delta tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dt_wide_pk_multiset_delta() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Two delta rows sharing the same wide PK but different payloads (multiset
        // delta). One trace row for that PK. Inner join must produce 2 output rows.
        // The co-group hands the whole same-PK delta group to the callback at
        // once, producted against the once-walked trace group (no re-seek).
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 0, 0, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let mut delta = Batch::with_schema(schema, 2);
        // Row 0: pk=(1,0,0) payload=10 w=+1
        delta.extend_pk_opk(&schema, &[1, 0, 0]);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &10i64.to_le_bytes());
        delta.count += 1;
        // Row 1: pk=(1,0,0) payload=20 w=+1 — same PK, different payload
        delta.extend_pk_opk(&schema, &[1, 0, 0]);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &20i64.to_le_bytes());
        delta.count += 1;
        delta.sorted = true;

        let out = op_join_delta_trace(&delta, ch.cursor_mut(), &schema, &schema);
        // 2 delta rows × 1 trace row = 2 output rows
        assert_eq!(out.count, 2, "multiset delta: expected 2 join outputs");
        assert_eq!(out.get_weight(0), 1);
        assert_eq!(out.get_weight(1), 1);
    }

    // -----------------------------------------------------------------------
    // Wide-PK inner-join prefix-collision test
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dt_wide_pk_prefix_collision() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Two wide-PK rows sharing the same first 16 OPK bytes (columns 0 and 1
        // identical) but differing in column 2. The co-group must treat them
        // as distinct keys, not conflate via u128 prefix.
        // Row A: pk=(1,1,2), Row B: pk=(1,1,9).
        // Trace: Row A (+1). Delta: Row A (+1) and Row B (+1).
        // Large delta (2 rows) vs 1-row trace.
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 2, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let delta_cb = make_wide_batch(&schema, &[
            (1, 1, 2, 1, 200), // matches trace pk
            (1, 1, 9, 1, 300), // different pk — must NOT match
        ]);
        let delta = delta_cb.into_inner();

        let out = op_join_delta_trace(&delta, ch.cursor_mut(), &schema, &schema);
        // Only Row A matches (delta row 0 × trace row 0). Row B has no trace entry.
        assert_eq!(out.count, 1, "prefix-collision: only matching PK should produce output");
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 2).as_slice());
        assert_eq!(out.get_weight(0), 1);
    }

    // -----------------------------------------------------------------------
    // Wide-PK outer join multiset delta
    // -----------------------------------------------------------------------

    #[test]
    fn test_outer_join_dt_wide_pk_multiset_delta() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Same multiset delta setup as the inner-join test, but for outer join.
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 0, 0, 1, 100)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let mut delta = Batch::with_schema(schema, 2);
        delta.extend_pk_opk(&schema, &[1, 0, 0]);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &10i64.to_le_bytes());
        delta.count += 1;
        delta.extend_pk_opk(&schema, &[1, 0, 0]);
        delta.extend_weight(&1i64.to_le_bytes());
        delta.extend_null_bmp(&0u64.to_le_bytes());
        delta.extend_col(0, &20i64.to_le_bytes());
        delta.count += 1;
        delta.sorted = true;

        let out = op_join_delta_trace_outer(&delta, ch.cursor_mut(), &schema, &schema);
        // Both delta rows match → 2 joined output rows, no null-fills.
        assert_eq!(out.count, 2, "outer multiset: expected 2 matched outputs");
        for i in 0..out.count {
            assert_eq!(out.get_weight(i), 1);
            // PK must be (1,0,0)
            assert_eq!(out.get_pk_bytes(i), wide_pk_bytes(&schema, 1, 0, 0).as_slice());
        }
    }

    // -----------------------------------------------------------------------
    // Wide-PK anti/semi-join delta-trace
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dt_wide_pk() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Delta: (1,1,2) and (1,1,9). Trace: (1,1,9) only.
        // Anti-join: emit delta rows whose PK is NOT in trace → only (1,1,2).
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 9, 1, 50)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let delta_cb = make_wide_batch(&schema, &[(1, 1, 2, 1, 10), (1, 1, 9, 1, 20)]);
        let delta = delta_cb.into_inner();
        let out = op_anti_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 2).as_slice());
    }

    #[test]
    fn test_semi_join_dt_wide_pk() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Delta: (1,1,2) and (1,1,9). Trace: (1,1,9) only.
        // Semi-join: emit delta rows whose PK IS in trace → only (1,1,9).
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 9, 1, 50)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        let delta_cb = make_wide_batch(&schema, &[(1, 1, 2, 1, 10), (1, 1, 9, 1, 20)]);
        let delta = delta_cb.into_inner();
        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 9).as_slice());
    }

    // -----------------------------------------------------------------------
    // Wide-PK semi-join (large delta) — prefix collision and negative weight
    // -----------------------------------------------------------------------

    #[test]
    fn test_semi_join_dt_wide_pk_prefix_collision() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Prefix-collision pair: same first 16 OPK bytes (c0=1,c1=1), differ
        // in c2. Large delta vs tiny trace exercises the galloping skip.
        // Trace has (1,1,0) only. Delta has (1,1,0) and (1,1,1<<56).
        // Only (1,1,0) should be semi-joined.
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(1, 1, 0, 1, 99)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        // Large c2 differs starting at byte 17 of the OPK key.
        let c2_large = 1u64 << 56;
        let delta_cb = make_wide_batch(&schema, &[
            (1, 1, 0,        1, 10),
            (1, 1, c2_large, 1, 20),
            (2, 0, 0,        1, 30), // extra row so the delta outnumbers the 1-row trace
        ]);
        let delta = delta_cb.into_inner();
        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Only (1,1,0) matches the trace.
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 1, 0).as_slice());
    }

    #[test]
    fn test_semi_join_dt_wide_pk_negative_weight_trace() {
        use crate::storage::CursorHandle;
        use std::rc::Rc;
        // Trace entry for a wide PK with weight <= 0 must be skipped (no emit).
        let schema = wide_pk_3xu64_schema();
        let trace_batch = Rc::new(make_wide_batch(&schema, &[(5, 5, 5, -1, 99)]).into_inner());
        let mut ch = CursorHandle::from_owned(&[trace_batch], schema);

        // Large delta vs a 1-row trace.
        let delta_cb = make_wide_batch(&schema, &[
            (5, 5, 5, 1, 10),
            (6, 6, 6, 1, 20),
        ]);
        let delta = delta_cb.into_inner();
        let out = op_semi_join_delta_trace(&delta, ch.cursor_mut(), &schema);
        // Negative-weight trace row must not produce any output.
        assert_eq!(out.count, 0, "negative-weight trace row must be skipped");
    }

    // -----------------------------------------------------------------------
    // Wide-PK delta-delta family
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_wide_pk_cartesian() {
        // Two wide-PK batches with two matching PKs.
        let schema = wide_pk_3xu64_schema();
        let a = make_wide_batch(&schema, &[
            (1, 0, 0, 1, 10),
            (2, 0, 0, 1, 20),
        ]);
        let b = make_wide_batch(&schema, &[
            (1, 0, 0, 1, 100),
            (2, 0, 0, 1, 200),
        ]);
        let out = op_join_delta_delta(&a, &b, &schema, &schema);
        // Both PKs match: (1,0,0)×(1,0,0) and (2,0,0)×(2,0,0) → 2 output rows.
        assert_eq!(out.count, 2);
        assert_eq!(out.get_weight(0), 1);
        assert_eq!(out.get_weight(1), 1);
    }

    #[test]
    fn test_semi_join_dd_wide_pk() {
        // Wide-PK semi-join: A has (1,0,0) and (3,0,0); B has (1,0,0) only.
        // Output: only (1,0,0) from A.
        let schema = wide_pk_3xu64_schema();
        let a = make_wide_batch(&schema, &[(1, 0, 0, 1, 10), (3, 0, 0, 1, 30)]);
        let b = make_wide_batch(&schema, &[(1, 0, 0, 1, 100)]);
        let out = op_semi_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 1, 0, 0).as_slice());
    }

    #[test]
    fn test_anti_join_dd_wide_pk() {
        // Wide-PK payload-aware anti-join (EXCEPT): A row excluded only when B has
        // an exact (PK, payload) match with positive weight.
        // A has (1,0,0,payload=10) and (3,0,0,payload=30); B has (1,0,0,payload=10).
        // Output: only (3,0,0) from A (the (1,0,0) row is excluded by exact match).
        let schema = wide_pk_3xu64_schema();
        let a = make_wide_batch(&schema, &[(1, 0, 0, 1, 10), (3, 0, 0, 1, 30)]);
        let b = make_wide_batch(&schema, &[(1, 0, 0, 1, 10)]);  // same payload as A's first row
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        assert_eq!(out.count, 1);
        assert_eq!(out.get_pk_bytes(0), wide_pk_bytes(&schema, 3, 0, 0).as_slice());
    }

    // -----------------------------------------------------------------------
    // Signed narrow PK ordering (I64 negative values)
    // -----------------------------------------------------------------------

    #[test]
    fn test_join_dd_signed_narrow_negative_pks() {
        // Single-column I64 PK: -3, -1, 2. Two batches. Assert (-1,-1) matches.
        let schema = SchemaDescriptor::new(
            &[
                SchemaColumn::new(type_code::I64, 0),
                SchemaColumn::new(type_code::I64, 0),
            ],
            &[0],
        );
        let left = make_signed_batch(&schema, &[(-3, 1, 30), (-1, 1, 10), (2, 1, 20)]);
        let right = make_signed_batch(&schema, &[(-1, 1, 100), (4, 1, 400)]);
        let out = op_join_delta_delta(&left, &right, &schema, &schema);
        // Only -1 matches on both sides.
        assert_eq!(out.count, 1);
        assert_eq!(signed_pk_i64(&out, 0), -1i64);
        assert_eq!(out.get_weight(0), 1);
    }

    // -----------------------------------------------------------------------
    // Anti-join DD monotonic scan_b correctness (item 30)
    // -----------------------------------------------------------------------

    #[test]
    fn test_anti_join_dd_large_same_pk_group() {
        // Single PK group with many payloads. A's payloads 0..50 even; B's
        // payloads 0..50 even with positive weight. Anti-join keeps A rows
        // whose payload is NOT in B. Build A = payloads {0,2,4,...,98},
        // B = payloads {0,4,8,...,96}. Survivors: payloads in A not in B.
        let schema = make_schema_u64_i64();
        let a_rows: Vec<(u64, i64, i64)> = (0..50).map(|k| (1u64, 1i64, (k * 2) as i64)).collect();
        let b_rows: Vec<(u64, i64, i64)> = (0..25).map(|k| (1u64, 1i64, (k * 4) as i64)).collect();
        let a = make_batch(&schema, &a_rows);
        let b = make_batch(&schema, &b_rows);
        let out = op_anti_join_delta_delta(&a, &b, &schema);
        // Reference: A payload p survives iff p not in B (multiples of 4).
        let expected: Vec<i64> = (0..50)
            .map(|k| (k * 2) as i64)
            .filter(|p| p % 4 != 0)
            .collect();
        assert_eq!(out.count, expected.len());
        for (i, &p) in expected.iter().enumerate() {
            assert_eq!(get_payload_i64(&out, i), p, "row {i}");
        }
    }
}
