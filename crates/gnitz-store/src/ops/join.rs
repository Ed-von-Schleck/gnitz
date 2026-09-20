//! The delta-trace inner join, equi, non-equi and keyless.
//!
//! One opcode over a compiler-baked [`JoinProbe`]: `Equi` co-groups the delta
//! against the trace on equal key, `Range` walks an ordered half-open span per
//! delta equality group, `Cross` walks the whole trace once against the whole
//! delta. All three drive the same emission — the product of a contiguous
//! delta run with the cursor's current trace row — and all write
//! `[key, left payload…, right payload…]` over the SQL sides, keyed by the delta
//! PK, or by `[left PK…, right PK…]` under `Cross`.

use std::cmp::Ordering;
use std::ops::Range;

use crate::schema::key::{compare_pk_ordering, key_range_between_cuts, pk_bytes_eq, KeyCut, PkBuf};
use crate::schema::{DerivedSchema, OpBuildErr, SchemaDescriptor, MAX_PK_BYTES};
use crate::storage::{pk_group_end, Batch, BlobCacheGuard, MemBatch, ReadCursor};

use gnitz_expr::RowSource;
use gnitz_wire::{null_word_at, JoinKind, RangeRel};

// ---------------------------------------------------------------------------
// The plan
// ---------------------------------------------------------------------------

/// One join instruction's compiler-baked artifact: how it probes its trace, and
/// the schema it writes under.
pub struct JoinPlan {
    pub probe: JoinProbe,
    pub out_schema: SchemaDescriptor,
}

/// How one join instruction probes its trace, and which SQL side its delta port
/// carries. Baked by the compiler, which holds both sides' schemas and so
/// resolves the probe once, off the epoch path.
#[derive(Clone, Copy)]
pub struct JoinProbe {
    walk: Walk,
    /// The delta port is the join's **right** side, so the trace's half of the
    /// output comes first.
    delta_is_right: bool,
}

/// Which trace walk a probe drives.
#[derive(Clone, Copy)]
enum Walk {
    /// Equal key: the trace group at each delta group's own PK.
    Equi,
    /// An ordered span within an equality group.
    Range(RangeProbe),
    /// Every trace row, for every delta row: the keyless join. No key decides a
    /// match, so both key regions ride into the output as the pair key.
    Cross,
}

impl JoinPlan {
    /// Resolve a wire join node against its two input schemas, or name the
    /// precondition the circuit violated. A circuit is client-supplied catalog
    /// data, so these are refusals rather than debug asserts.
    pub fn from_wire(
        kind: JoinKind,
        delta_is_right: bool,
        delta: &SchemaDescriptor,
        trace: &SchemaDescriptor,
    ) -> Result<JoinPlan, OpBuildErr> {
        let walk = match kind {
            JoinKind::Equi => {
                same_pk_types(delta, trace)?;
                Walk::Equi
            }
            JoinKind::Range { n_eq, rel } => {
                same_pk_types(delta, trace)?;
                Walk::Range(RangeProbe::new(trace, n_eq, rel, delta_is_right)?)
            }
            JoinKind::Cross => Walk::Cross,
        };
        let (left, right) = match delta_is_right {
            true => (trace, delta),
            false => (delta, trace),
        };
        let out_schema = merge_schemas_for_join(kind, left, right)
            .ok_or_else(|| OpBuildErr::shape("join: merged schema exceeds MAX_COLUMNS"))?;
        Ok(JoinPlan {
            probe: JoinProbe { walk, delta_is_right },
            out_schema,
        })
    }
}

/// A keyed walk reads one side's PK region as the other's, so the two key
/// layouts must be identical down to the OPK encoding each column type implies.
fn same_pk_types(delta: &SchemaDescriptor, trace: &SchemaDescriptor) -> Result<(), OpBuildErr> {
    fn types(s: &SchemaDescriptor) -> impl Iterator<Item = u8> + '_ {
        s.pk_columns().map(|(_, c)| c.type_code)
    }
    if types(delta).eq(types(trace)) {
        return Ok(());
    }
    Err(OpBuildErr::shape(
        "join: delta and trace PK column types differ (both sides must reindex at the pair's common type)",
    ))
}

/// The key region, then both sides' payloads in SQL side order. A keyed join
/// inherits the shared key; `Cross` matches on neither, so it mints the pair.
fn merge_schemas_for_join(
    kind: JoinKind,
    left: &SchemaDescriptor,
    right: &SchemaDescriptor,
) -> Option<SchemaDescriptor> {
    let mut b = DerivedSchema::new();
    match kind {
        JoinKind::Cross => {
            b.push_pk_of(left)?;
            b.push_pk_of(right)?;
        }
        JoinKind::Equi | JoinKind::Range { .. } => b.push_pk_of(left)?,
    }
    for (_, c) in left.payload_columns().chain(right.payload_columns()) {
        b.push(*c)?;
    }
    Some(b.finish())
}

// ---------------------------------------------------------------------------
// The range probe
// ---------------------------------------------------------------------------

/// A range relation as the trace slot's relation to the delta slot, plus the
/// width the equality prefix operates at.
#[derive(Clone, Copy)]
struct RangeProbe {
    /// Width in OPK bytes of the equality-pinned leading slots; the rest of a PK
    /// region is the range slot.
    eq_size: usize,
    /// `{ trace_slot REL delta_slot }`.
    rel: RangeRel,
}

impl RangeProbe {
    /// Resolve a wire `left REL right` against the trace schema's key region.
    fn new(
        trace: &SchemaDescriptor,
        n_eq: u8,
        rel: RangeRel,
        delta_is_right: bool,
    ) -> Result<RangeProbe, OpBuildErr> {
        // The reindexed key is `[eq slots…, range slot]`.
        if n_eq as usize + 1 != trace.pk_indices().len() {
            return Err(OpBuildErr::shape("range join: n_eq does not match trace key arity"));
        }
        // In PK order, so the range slot always keeps a span of its own.
        let eq_size = trace.pk_columns().take(n_eq as usize).map(|(_, c)| c.size() as usize).sum();
        // `rel` relates left to right; the probe relates trace to delta.
        let rel = match delta_is_right {
            true => rel,
            false => rel.converse(),
        };
        Ok(RangeProbe { eq_size, rel })
    }

    /// Matches lie above the delta row's slot, not below it.
    #[inline]
    fn above(&self) -> bool {
        matches!(self.rel, RangeRel::Gt | RangeRel::Ge)
    }

    /// The cut falls below the delta row's own slot, so an equal trace slot lies
    /// on the far side of it.
    #[inline]
    fn cuts_below(&self) -> bool {
        matches!(self.rel, RangeRel::Ge | RangeRel::Lt)
    }

    /// The trace key range this probe covers for a delta row whose PK region is
    /// `pk` (`equality prefix ‖ range slot`). `None` when it is provably empty.
    fn cut_points(&self, pk: &[u8]) -> Option<(PkBuf, Option<PkBuf>)> {
        let group = &pk[..self.eq_size];
        let slot = match self.cuts_below() {
            true => KeyCut::min_of(pk),
            false => KeyCut::above(pk),
        };
        let (start, end) = match self.above() {
            true => (slot, KeyCut::above(group)),
            false => (KeyCut::min_of(group), slot),
        };
        key_range_between_cuts(start, end, pk.len())
    }

    /// The greatest ordering a delta slot may have against a trace slot while
    /// still falling before the split that slot induces in the group: `Gt` and
    /// `Le` put an equal slot after the split, `Ge` and `Lt` before it.
    #[inline]
    fn last_before_split(&self) -> Ordering {
        match self.cuts_below() {
            true => Ordering::Equal,
            false => Ordering::Less,
        }
    }
}

// ---------------------------------------------------------------------------
// The operator
// ---------------------------------------------------------------------------

/// Join delta rows against the trace, writing `[key, left payload…, right
/// payload…]` under the `out_schema` [`JoinPlan::from_wire`] derived above.
///
/// Emission is trace-major under every probe: each trace row is walked once and
/// producted against a contiguous, random-access delta run. So the output is
/// unfolded and not (PK, payload)-sorted — under `Cross` not even PK-sorted,
/// since each trace row re-emits the whole delta; it carries no layout claim,
/// and downstream re-sorts and consolidates.
pub fn op_join_delta_trace(
    delta: &Batch,
    cursor: &mut ReadCursor,
    delta_schema: &SchemaDescriptor,
    out_schema: &SchemaDescriptor,
    probe: JoinProbe,
) -> Batch {
    let cs = Batch::consolidate_if_needed(delta, delta_schema);
    let consolidated: &Batch = cs.as_ref().unwrap_or(delta);
    let n = consolidated.count;
    if n == 0 {
        return Batch::empty_with_schema(out_schema);
    }
    let delta_mb = consolidated.as_mem_batch();
    let is_cross = matches!(probe.walk, Walk::Cross);
    // The keyless probe reads no key, so it positions at row 0 instead of seeking.
    if is_cross {
        cursor.rewind();
    }
    // From there it emits at most `n × |trace|` rows, so the arena is sized once
    // rather than re-copied at every doubling; a keyed probe has no such bound.
    let rows = match is_cross {
        true => n.saturating_mul(cursor.estimated_length()),
        false => n,
    };

    // Both output regions run the left SQL side first, so one split places the
    // delta's and the trace's half of every row.
    let split = |d: usize, t: usize| -> (Range<usize>, Range<usize>) {
        match probe.delta_is_right {
            true => (t..t + d, 0..t),
            false => (0..d, d..d + t),
        }
    };
    let d_npc = delta_schema.num_payload_cols();
    let d_pk = delta_schema.pk_stride();
    let out_pk = out_schema.pk_stride();
    let (d_slots, t_slots) = split(d_npc, out_schema.num_payload_cols() - d_npc);
    let mut pair = is_cross.then(|| {
        let (delta, trace) = split(d_pk, out_pk - d_pk);
        PairKey {
            buf: [0u8; MAX_PK_BYTES],
            len: out_pk,
            delta,
            trace,
        }
    });

    let mut output = Batch::with_capacity(out_schema, rows);
    let mut cache = BlobCacheGuard::acquire(out_schema, rows);

    let mut emit = |rs: usize, re: usize, c: &ReadCursor| {
        let w_trace = c.current_weight;
        let (t_src, t_row) = c.current_row_source();
        let t_null = t_src.get_null_word(t_row);
        // Every row this call writes shares one trace row, so its null bits
        // rebase once rather than per delta row.
        let t_bits = null_word_at(t_null, t_slots.start);
        if let Some(k) = pair.as_mut() {
            k.buf[k.trace.clone()].copy_from_slice(c.current_pk_bytes());
        }
        for i in rs..re {
            let w_out = delta_mb.get_weight(i).wrapping_mul(w_trace);
            if w_out == 0 {
                continue;
            }
            let d_pk_bytes = delta_mb.get_pk_bytes(i);
            let pk = match pair.as_mut() {
                Some(k) => {
                    k.buf[k.delta.clone()].copy_from_slice(d_pk_bytes);
                    &k.buf[..k.len]
                }
                None => d_pk_bytes,
            };
            output.begin_row(pk, w_out);

            let d_null = delta_mb.get_null_word(i);
            output.append_payload_cols(d_slots.clone(), out_schema, &delta_mb, i, d_null, cache.get_mut());
            output.append_payload_cols(t_slots.clone(), out_schema, t_src, t_row, t_null, cache.get_mut());
            // Each half's null bits rebase onto the slot its columns landed at.
            output.commit_row(t_bits | null_word_at(d_null, d_slots.start));
        }
    };

    match probe.walk {
        Walk::Equi => equi_merge_walk(consolidated, cursor, emit),
        Walk::Range(range) => range_merge_walk(&delta_mb, cursor, range, emit),
        Walk::Cross => cursor.for_each_row_while(|_| true, |c| emit(0, n, c)),
    }

    output
}

/// `Cross`'s output key under construction: the pair `[left PK…, right PK…]`,
/// assembled per row because neither source key is the output's. A keyed probe
/// has none — its key is the delta's PK region verbatim.
struct PairKey {
    buf: [u8; MAX_PK_BYTES],
    len: usize,
    delta: Range<usize>,
    trace: Range<usize>,
}

// ---------------------------------------------------------------------------
// The equi walk
// ---------------------------------------------------------------------------

/// Equal-key merge walk. Both pointers galloping-skip to catch up, so the cost
/// is bounded by the smaller side's matches whichever side that is. It opens with
/// `advance_to(delta[0])`, which is backward-capable, so a trace cursor another
/// op left elsewhere in this epoch is repositioned rather than mis-walked.
fn equi_merge_walk(delta: &Batch, m: &mut ReadCursor, mut emit: impl FnMut(usize, usize, &ReadCursor)) {
    let n = delta.count;
    if n == 0 {
        return;
    }
    m.advance_to(delta.get_pk_bytes(0));
    let mut i = 0;
    while i < n && m.valid {
        let dk = delta.get_pk_bytes(i);
        match compare_pk_ordering(dk, m.current_pk_bytes()) {
            Ordering::Less => i = delta.advance_to(m.current_pk_bytes(), i), // skip delta
            // The comparison above IS the forward gallop's precondition.
            Ordering::Greater => m.advance_to_forward(dk), // skip trace
            Ordering::Equal => {
                let j = pk_group_end(delta, i); // delta group
                m.for_each_pk_group_row(dk, |c| emit(i, j, c));
                i = j;
            }
        }
    }
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
    // The group's spans share a closed bound, so one cut covers the group — taken
    // on the row carrying the open one.
    let cut_row = if probe.above() { lo } else { hi - 1 };
    let Some((start, end)) = probe.cut_points(delta.get_pk_bytes(cut_row)) else {
        // Provably empty, so the cursor is never positioned here — and so it says
        // nothing about what the trace holds after this group either.
        return Some(hi);
    };
    // Hoisted out of the sweep: the walk below reads these once per covered trace
    // row, where loading them back off `probe` costs more than the branch each one
    // decides.
    let eq_size = probe.eq_size;
    let above = probe.above();
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
            let (rs, re) = if above { (lo, ptr) } else { (ptr, hi) };
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[path = "tests/join.rs"]
mod tests;
