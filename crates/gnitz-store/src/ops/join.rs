//! The delta-trace inner join, equi, non-equi and keyless.
//!
//! One opcode over a compiler-baked [`JoinProbe`]: `Equi` co-groups the delta
//! against the trace on equal key, `Range` walks an ordered half-open span per
//! delta equality group, `Cross` walks the whole trace once against the whole
//! delta. All three drive the same emission — the product of a contiguous
//! delta run with the cursor's current trace row — and all produce
//! `[left_PK, left_payload…, right_payload…]`.

use std::cmp::Ordering;

use crate::schema::key::{compare_pk_ordering, key_range_between_cuts, pk_bytes_eq, KeyCut, PkBuf};
use crate::schema::{DerivedSchema, SchemaDescriptor};
use crate::storage::{Batch, BlobCacheGuard, MemBatch, ReadCursor};

use super::cogroup::cogroup_intersection;

use gnitz_wire::{merge_null_words, RangeRel};

// ---------------------------------------------------------------------------
// The probe
// ---------------------------------------------------------------------------

/// How one join instruction probes its trace. Baked by the compiler, which holds
/// both sides' schemas and so resolves the probe once, off the epoch path.
#[derive(Clone, Copy)]
pub enum JoinProbe {
    /// Equal key: the trace group at each delta group's own PK.
    Equi,
    /// An ordered span within an equality group.
    Range(RangeProbe),
    /// Every trace row, for every delta row: the keyless join. Neither side's
    /// key takes part, so the probe has nothing to resolve.
    Cross,
}

/// A range relation as its two independent dimensions, plus the width they
/// operate at. [`RangeProbe::new`] is the only public constructor, so the pair
/// always spells a real relation over a key width the sweep can slice.
#[derive(Clone, Copy)]
pub struct RangeProbe {
    /// Width in OPK bytes of the equality-pinned leading slots; the rest of a PK
    /// region is the range slot.
    eq_size: usize,
    /// Matches lie above the delta row's slot (`Gt`/`Ge`), not below (`Lt`/`Le`).
    prefix: bool,
    /// A trace slot equal to the delta row's own matches (`Ge`/`Le`).
    nonstrict: bool,
}

impl RangeProbe {
    /// Resolve a wire range relation against the two input schemas, or name the
    /// precondition the circuit violated. `Result`, not a debug assert: a circuit
    /// is client-supplied catalog data, and the sweep's `pk[..eq_size]` /
    /// `pk[eq_size..]` slices have nothing else establishing them.
    pub fn new(
        delta_schema: &SchemaDescriptor,
        trace_schema: &SchemaDescriptor,
        n_eq: u8,
        rel: RangeRel,
    ) -> Result<RangeProbe, &'static str> {
        // The trace's reindexed key is `[eq slots…, range slot]`.
        if n_eq as usize + 1 != trace_schema.pk_indices().len() {
            return Err("range join: n_eq does not match trace key arity");
        }
        // Both sides reindex at the pair's common promoted type, so their PK
        // regions have one width — which the sweep slices both of at.
        if delta_schema.pk_stride() != trace_schema.pk_stride() {
            return Err(
                "range join: delta and trace PK strides differ (both sides must reindex at the pair's common type)",
            );
        }
        // Not implied by the arity check above: `leading_key_size` sums *schema*-
        // order columns, so a crafted circuit can spend the whole key on the eq
        // prefix while still naming `n_eq + 1` PK columns.
        let eq_size = trace_schema.leading_key_size(n_eq as usize);
        if eq_size >= trace_schema.pk_stride() {
            return Err("range join: eq prefix covers the whole key");
        }
        Ok(RangeProbe::of(eq_size, rel))
    }

    /// The relation's two dimensions at a validated `eq_size`. Private: only
    /// [`RangeProbe::new`] establishes that width against a real key region.
    fn of(eq_size: usize, rel: RangeRel) -> RangeProbe {
        let (prefix, nonstrict) = match rel {
            RangeRel::Gt => (true, false),
            RangeRel::Ge => (true, true),
            RangeRel::Lt => (false, false),
            RangeRel::Le => (false, true),
        };
        RangeProbe { eq_size, prefix, nonstrict }
    }

    /// The trace key range this probe covers for a delta row whose PK region is
    /// `pk` (`equality prefix ‖ range slot`). `None` when it is provably empty.
    fn cut_points(&self, pk: &[u8]) -> Option<(PkBuf, Option<PkBuf>)> {
        let group = &pk[..self.eq_size];
        let (start, end) = match (self.prefix, self.nonstrict) {
            // Gt: (slot, group end)
            (true, false) => (KeyCut::above(pk), KeyCut::above(group)),
            // Ge: [slot, group end)
            (true, true) => (KeyCut::min_of(pk), KeyCut::above(group)),
            // Lt: [group start, slot)
            (false, false) => (KeyCut::min_of(group), KeyCut::min_of(pk)),
            // Le: [group start, slot]
            (false, true) => (KeyCut::min_of(group), KeyCut::above(pk)),
        };
        key_range_between_cuts(start, end, pk.len())
    }

    /// The row of the delta group `[lo, hi)` whose slot the group's single cut is
    /// taken on. The union of the rows' spans opens at the group's smallest slot
    /// and closes at its largest, and only one of those two bounds is open.
    #[inline]
    fn cut_row(&self, lo: usize, hi: usize) -> usize {
        if self.prefix {
            lo
        } else {
            hi - 1
        }
    }

    /// The greatest ordering a delta slot may have against a trace slot while
    /// still falling before the split that slot induces in the group: `Gt` and
    /// `Le` put an equal slot after the split, `Ge` and `Lt` before it.
    #[inline]
    fn last_before_split(&self) -> Ordering {
        if self.prefix != self.nonstrict {
            Ordering::Less
        } else {
            Ordering::Equal
        }
    }
}

// ---------------------------------------------------------------------------
// The operator
// ---------------------------------------------------------------------------

/// Join delta rows against the trace. Output schema:
/// `[left_PK, left_payload..., right_payload...]`, built by
/// [`merge_schemas_for_join`] above and handed down as `out_schema`.
///
/// Emission is trace-major under every probe: each trace row is walked once and
/// producted against a contiguous, random-access delta run. So the output is
/// unfolded and not (PK, payload)-sorted — under `Cross` not even PK-sorted,
/// since each trace row re-emits the whole delta; it carries no layout claim,
/// and downstream re-sorts and consolidates.
pub fn op_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    left_schema: &SchemaDescriptor,
    right_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    probe: JoinProbe,
) -> Batch {
    let cs = Batch::consolidate_if_needed(delta, left_schema);
    let consolidated: &Batch = cs.as_ref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty_with_schema(out_schema);
    }
    let delta_mb = consolidated.as_mem_batch();
    // A keyless probe emits exactly `n × |trace|` rows, so the arena is sized once
    // rather than re-copied at every doubling. A keyed probe walks a bounded span
    // per delta row and has no such bound: `n` is its floor.
    let rows = match probe {
        JoinProbe::Cross => {
            cursor.rewind();
            n.saturating_mul(cursor.estimated_length())
        }
        _ => n,
    };
    let mut writer = JoinRowWriter::open(left_schema, right_schema, out_schema, rows);

    let mut emit = |rs: usize, re: usize, c: &ReadCursor| {
        let w_trace = c.current_weight;
        for i in rs..re {
            let w_out = delta_mb.get_weight(i).wrapping_mul(w_trace);
            if w_out != 0 {
                writer.write(&delta_mb, i, c, w_out);
            }
        }
    };

    match probe {
        JoinProbe::Equi => cogroup_intersection(consolidated, cursor, |key, r, m| {
            m.for_each_pk_group_row(key, |c| emit(r.start, r.end, c));
        }),
        JoinProbe::Range(probe) => range_merge_walk(&delta_mb, cursor, probe, emit),
        JoinProbe::Cross => {
            // Every probe positions its own cursor; this one has no key to seek
            // by, so it rewinds. Idempotent — the sizing pass above also rewinds,
            // and neither relies on the other having run.
            cursor.rewind();
            cursor.for_each_row_while(|_| true, |c| emit(0, n, c))
        }
    }

    writer.finish()
}

// ---------------------------------------------------------------------------
// The range walk
// ---------------------------------------------------------------------------

/// Trace-driven equality-group merge walk: sweep each delta equality group in
/// turn, each sweep saying where the next one starts.
fn range_merge_walk(
    delta: &MemBatch,
    cursor: &mut ReadCursor,
    probe: RangeProbe,
    mut emit: impl FnMut(usize, usize, &ReadCursor),
) {
    let mut lo = 0;
    while lo < delta.count {
        let hi = eq_group_end(delta, lo, probe.eq_size);
        // Group spans ascend across the walk, so once the trace runs out inside
        // one span no later group can match either.
        let Some(next) = sweep_eq_group(delta, cursor, probe, lo, hi, &mut emit) else {
            return;
        };
        lo = next;
    }
}

/// Sweep the delta equality group `[lo, hi)` against the trace: one cut spans the
/// whole group, so seek to its start and walk only the covered span with a
/// monotone delta pointer, handing `emit` the matching delta run per trace row.
/// The seek passes over untouched trace groups and the dead intra-group head.
///
/// Returns where the next delta group starts: `hi`, or further when the trace
/// itself proves the groups in between hold nothing. `None` when the trace is
/// exhausted.
fn sweep_eq_group(
    delta: &MemBatch,
    cursor: &mut ReadCursor,
    probe: RangeProbe,
    lo: usize,
    hi: usize,
    emit: &mut impl FnMut(usize, usize, &ReadCursor),
) -> Option<usize> {
    let Some((start, end)) = probe.cut_points(delta.get_pk_bytes(probe.cut_row(lo, hi))) else {
        // Provably empty, so the cursor is never positioned here — and so it says
        // nothing about what the trace holds after this group either.
        return Some(hi);
    };
    // Hoisted out of the sweep: the walk below reads these once per covered trace
    // row, where loading them back off `probe` costs more than the branch each
    // one decides — worth ~1% of the range kernel on `join_range_dt_bench`.
    let RangeProbe { eq_size, prefix, .. } = probe;
    let last_before_split = probe.last_before_split();

    // Galloping skip to the covered start: seeded at the live position it passes
    // over untouched trace groups and the dead head in one hop, and being
    // backward-capable it also repositions a cursor another op on the same trace
    // register left elsewhere.
    cursor.advance_to(start.pk_bytes());
    let end = end.as_ref().map(PkBuf::pk_bytes);
    let mut ptr = lo; // monotone across the whole sweep
    cursor.for_each_row_while(
        // Every key in [start, end) carries the group's equality prefix, so the
        // end bound alone delimits the group.
        |pk| end.is_none_or(|e| compare_pk_ordering(pk, e).is_lt()),
        |c| {
            let s = &c.current_pk_bytes()[eq_size..];
            // Matching rows form a prefix of the group under `Gt`/`Ge` and a
            // suffix under `Lt`/`Le`, so either way the pointer walks the rows
            // that come first.
            while ptr < hi && compare_pk_ordering(&delta.get_pk_bytes(ptr)[eq_size..], s) <= last_before_split {
                ptr += 1;
            }
            let (rs, re) = if prefix { (lo, ptr) } else { (ptr, hi) };
            emit(rs, re, c);
        },
    );
    // The cursor sits on the first trace row past the span, so its own equality
    // prefix is the next group the trace holds.
    cursor
        .valid
        .then(|| skip_to_group(delta, eq_size, hi, cursor.current_pk_bytes()))
}

/// First delta row past the equality group starting at `lo`.
fn eq_group_end(delta: &MemBatch, lo: usize, eq_size: usize) -> usize {
    let e = &delta.get_pk_bytes(lo)[..eq_size];
    let mut hi = lo + 1;
    while hi < delta.count && pk_bytes_eq(&delta.get_pk_bytes(hi)[..eq_size], e) {
        hi += 1;
    }
    hi
}

/// First delta row at index `from` or later whose equality prefix is not below
/// `group`'s. Every row passed has a strictly smaller prefix, so the landing row
/// heads its group.
fn skip_to_group(delta: &MemBatch, eq_size: usize, from: usize, group: &[u8]) -> usize {
    let group = &group[..eq_size];
    let mut hi = from;
    while hi < delta.count && compare_pk_ordering(&delta.get_pk_bytes(hi)[..eq_size], group).is_lt() {
        hi += 1;
    }
    hi
}

// ---------------------------------------------------------------------------
// The output schema
// ---------------------------------------------------------------------------

/// `left`'s PK, then both sides' payloads — the layout [`JoinRowWriter`] below
/// writes, homed beside it because that writer derives the same layout
/// independently, from the same `(left, right)` pair (`extend_pk_bytes(left)`,
/// then `append_payload_cols` at 0 and at `left_npc`). An outer join's null-fill
/// columns are appended by the compiler's own null-extend builder, not here.
pub fn merge_schemas_for_join(left: &SchemaDescriptor, right: &SchemaDescriptor) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    b.push_pk_of(left)?;
    for (_, c) in left.payload_columns().chain(right.payload_columns()) {
        b.push(*c)?;
    }
    Some(b.finish())
}

// ---------------------------------------------------------------------------
// The row writer
// ---------------------------------------------------------------------------

/// One join's output under construction: the batch, the blob dedup cache, and
/// the two input schemas that fix the row layout `[left_PK, left_payload...,
/// right_payload...]`. Opened from the same `(left, right)` pair
/// [`merge_schemas_for_join`] derives `out_schema` from, so the schema and the
/// rows written under it come from one input.
struct JoinRowWriter<'s> {
    output: Batch,
    /// One dedup cache for the whole join: a `D×T` key group re-appends each left
    /// payload `T` times and each right payload `D` times.
    cache: BlobCacheGuard,
    left_schema: &'s SchemaDescriptor,
    right_schema: &'s SchemaDescriptor,
    /// The left half's payload count: the right half's first payload slot, and
    /// the bit the two null words are joined at.
    left_npc: usize,
}

impl<'s> JoinRowWriter<'s> {
    /// `rows` seeds the output capacity: exact for a 1:1 key match, a floor
    /// otherwise.
    fn open(
        left_schema: &'s SchemaDescriptor,
        right_schema: &'s SchemaDescriptor,
        out_schema: &SchemaDescriptor,
        rows: usize,
    ) -> Self {
        JoinRowWriter {
            output: Batch::with_capacity(out_schema, rows),
            cache: BlobCacheGuard::acquire(out_schema, rows),
            left_schema,
            right_schema,
            left_npc: left_schema.num_payload_cols(),
        }
    }

    /// Write one output row at `weight`: the left half from `left[left_row]`, the
    /// right half from `right`'s current row — both through the shared,
    /// monomorphic `Batch::append_payload_cols` body (German-string blob
    /// relocation included).
    #[inline]
    fn write(&mut self, left: &MemBatch, left_row: usize, right: &ReadCursor, weight: i64) {
        let left_null = left.get_null_word(left_row);
        let right_null = right.current_null_word;
        let null_word = merge_null_words(left_null, right_null, self.left_npc);

        let output = &mut self.output;
        output.begin_row(left.get_pk_bytes(left_row), weight);

        output.append_payload_cols(0, self.left_schema, left, left_row, left_null, self.cache.get_mut());
        let (right_src, right_row) = right.current_row_source();
        output.append_payload_cols(
            self.left_npc,
            self.right_schema,
            right_src,
            right_row,
            right_null,
            self.cache.get_mut(),
        );

        output.commit_row(null_word);
    }

    fn finish(self) -> Batch {
        self.output
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/join.rs"]
mod tests;
