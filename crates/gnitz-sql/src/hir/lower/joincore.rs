//! The AST-free circuit primitives the join and EXISTS/IN lowering shells drive:
//! the equi and range/band prologues, the term pair each emits, the pure-range
//! threshold pipeline, the null-fill tails, and the synthetic-key column defs.
//!
//! The axis against [`super::prims`] is granularity: `prims` builds one program or
//! emits at most one node; every item here emits a multi-node DBSP construction.

use super::super::guards::converse_rel;
use super::super::{slot_of, EqPair, HirRange, JoinType};
use super::prims::{multi_null_filter_prog, null_gate, rekey_on_source_pk, self_derived_key};
use super::JoinSide;
use crate::error::GnitzSqlError;

use gnitz_core::{CircuitBuilder, ColumnDef, NodeId, RangeRel, ReindexRole, ReindexSlot, Schema, TypeCode};
use gnitz_wire::AggFunc as WireAggFunc;

/// A join's equality pairs resolved against both inputs: each side's key-column
/// positions in its own layout, and the per-pair promoted type codes.
struct EquiKeys {
    left: Vec<usize>,
    right: Vec<usize>,
    tcs: Vec<TypeCode>,
}

/// Resolve a join's equality pairs to [`EquiKeys`] — the shared key prologue of
/// every equi/range emit, join and EXISTS/IN alike.
fn resolve_eq_cols(eq: &[EqPair], left: &JoinSide, right: &JoinSide) -> Result<EquiKeys, GnitzSqlError> {
    Ok(EquiKeys {
        left: eq
            .iter()
            .map(|p| slot_of(&left.seg.frame.layout, p.left))
            .collect::<Result<_, _>>()?,
        right: eq
            .iter()
            .map(|p| slot_of(&right.seg.frame.layout, p.right))
            .collect::<Result<_, _>>()?,
        tcs: eq.iter().map(|p| p.tc).collect(),
    })
}

/// This side's reindex key: each slot's source column paired with the carried
/// target type from `carried_reindex_tc` — `T_i` only where this side's own
/// self-derived type disagrees with it, else `None`, so the engine stays the
/// sole producer of the derived slot type. Shared by the equi, range/band and
/// EXISTS/IN builders, which differ only in what `slot_tcs` spans.
fn side_reindex_key(cols: &[usize], coldefs: &[ColumnDef], slot_tcs: &[TypeCode]) -> Vec<ReindexSlot> {
    cols.iter()
        .zip(slot_tcs)
        .map(|(&c, &t)| (c as u32, coldefs[c].type_code.carried_reindex_tc(t)))
        .collect()
}

/// Normalize the two per-term join outputs onto the canonical `[A cols, B cols]`
/// payload layout and union them. Term AB is already canonical
/// (`[_join_pk × k, A, B]`); term BA is `[_join_pk × k, B, A]` and is reordered.
/// Shared verbatim by the equi (`k = eq slots`) and range (`k = n_eq + 1`)
/// builders, and by the band EXISTS/IN semi-join circuit.
fn normalize_to_ab(
    cb: &mut CircuitBuilder,
    join_ab: NodeId,
    join_ba: NodeId,
    k: usize,
    left_n: usize,
    right_n: usize,
) -> NodeId {
    // Term AB is already canonical behind its key region; term BA is swapped.
    let proj_ab: Vec<usize> = (k..k + left_n + right_n).collect();
    let proj_ba: Vec<usize> = ba_to_ab_cols(k, left_n, right_n).collect();
    let ab = cb.map(join_ab, &proj_ab);
    let ba = cb.map(join_ba, &proj_ba);
    cb.union(ab, ba)
}

/// Term BA's columns in canonical `[A, B]` order, behind a `k`-wide key region.
/// A join emits `[left_PK, left_payload…, right_payload…]`, so the BA term names
/// B as its left. A caller spends the list as a `map` projection or as a reindex
/// keep list; both index the same node.
pub(super) fn ba_to_ab_cols(k: usize, left_n: usize, right_n: usize) -> impl Iterator<Item = usize> {
    (k + right_n..k + right_n + left_n).chain(k..k + right_n)
}

/// One join side as a prologue gated and reindexed it.
pub(crate) struct EquiSide<'a> {
    side: &'a JoinSide,
    input: NodeId,
    cols: Vec<usize>,
    key: Vec<ReindexSlot>,
    key_nullable: bool,
    pub(crate) reindex: NodeId,
}

impl EquiSide<'_> {
    /// `P_all`: this side's *unfiltered* input re-keyed onto the join key. A NOT
    /// NULL key gated nothing, so there the emitted reindex already is it.
    fn all(&self, cb: &mut CircuitBuilder) -> NodeId {
        if self.key_nullable {
            cb.map_reindex(self.input, &self.key, &self.side.keep, ReindexRole::ScatterKey)
        } else {
            self.reindex
        }
    }
}

/// NULL-gate `input` over `cols` (SQL 3VL: a NULL key matches nothing) and
/// reindex it onto its key — the one side of either prologue.
fn gated_side<'a>(
    cb: &mut CircuitBuilder,
    side: &'a JoinSide,
    input: NodeId,
    cols: Vec<usize>,
    tcs: &[TypeCode],
) -> Result<EquiSide<'a>, GnitzSqlError> {
    let columns = &side.seg.frame.schema.columns;
    let key = side_reindex_key(&cols, columns, tcs);
    let (gated, key_nullable) = null_gate(cb, input, &cols, columns)?;
    let reindex = cb.map_reindex(gated, &key, &side.keep, ReindexRole::ScatterKey);
    Ok(EquiSide {
        side,
        input,
        cols,
        key,
        key_nullable,
        reindex,
    })
}

/// What [`equi_prologue`] hands back: the two sides as it built them, the term
/// pair over them, and the per-slot common types. The key arity, the output
/// `_join_pk` defs and each side's kept width are accessors here rather than
/// facts a caller restates.
pub(crate) struct EquiTerms<'a> {
    sides: [EquiSide<'a>; 2],
    tcs: Vec<TypeCode>,
    pub(crate) join_ab: NodeId,
    pub(crate) join_ba: NodeId,
}

impl<'a> EquiTerms<'a> {
    /// The join key arity — the width of the `_join_pk` region every term and
    /// null-fill branch leads with.
    pub(crate) fn k(&self) -> usize {
        self.tcs.len()
    }

    /// The inner output: both terms normalized onto `[_join_pk, kept-A, kept-B]`
    /// and unioned, each at the width its own keep list fixed.
    pub(crate) fn merged(&self, cb: &mut CircuitBuilder) -> NodeId {
        normalize_to_ab(
            cb,
            self.join_ab,
            self.join_ba,
            self.k(),
            self.kept_n(true),
            self.kept_n(false),
        )
    }

    /// The view's output PK column defs.
    pub(crate) fn out_pk_coldefs(&self) -> Vec<ColumnDef> {
        join_pk_coldefs(&self.tcs)
    }

    /// This side's kept-payload width in the merged `[_join_pk, kept-A, kept-B]`
    /// layout.
    pub(crate) fn kept_n(&self, is_left: bool) -> usize {
        self.side(is_left).side.n()
    }

    /// `P_all` — that side's unfiltered input re-keyed onto the join key.
    pub(crate) fn p_all(&self, cb: &mut CircuitBuilder, is_left: bool) -> NodeId {
        self.side(is_left).all(cb)
    }

    fn side(&self, is_left: bool) -> &EquiSide<'a> {
        &self.sides[usize::from(!is_left)]
    }
}

/// Gate and reindex both sides on the eq pairs, then emit the symmetric 2-term
/// join over them.
pub(crate) fn equi_prologue<'a>(
    cb: &mut CircuitBuilder,
    eq: &[EqPair],
    sides: &'a [JoinSide; 2],
    inputs: [NodeId; 2],
) -> Result<EquiTerms<'a>, GnitzSqlError> {
    let EquiKeys { left, right, tcs } = resolve_eq_cols(eq, &sides[0], &sides[1])?;
    let b = gated_side(cb, &sides[1], inputs[1], right, &tcs)?;
    let a = gated_side(cb, &sides[0], inputs[0], left, &tcs)?;
    let trace_a = cb.integrate_trace(a.reindex);
    let trace_b = cb.integrate_trace(b.reindex);
    let join_ab = cb.join_with_trace_node(a.reindex, trace_b); // ΔA ⋈ z^{-1}(I(B))
    let join_ba = cb.join_with_trace_node(b.reindex, trace_a); // ΔB ⋈ z^{-1}(I(A))
    Ok(EquiTerms { sides: [a, b], tcs, join_ab, join_ba })
}

/// The output PK columns a re-key onto source PKs mints: one per listed
/// `(schema, pk column)`, typed by the reindex's self-derive output type,
/// non-nullable, and hidden. The two producers below differ only in `name`.
fn rekey_pk_coldefs<'a>(
    cols: impl IntoIterator<Item = (&'a Schema, u32)>,
    name: impl Fn(usize, &ColumnDef) -> String,
) -> Vec<ColumnDef> {
    cols.into_iter()
        .enumerate()
        .map(|(slot, (schema, c))| {
            let src = &schema.columns[c as usize];
            ColumnDef::new(name(slot, src), src.type_code.reindex_output_type(), false).hidden()
        })
        .collect()
}

/// The leading output PK columns of a range/band or cross join: A's then B's
/// source PK, `_pair_pk_{slot}`-numbered across both sides. One home for both
/// range emitters.
pub(super) fn pair_pk_coldefs(left_schema: &Schema, right_schema: &Schema) -> Vec<ColumnDef> {
    rekey_pk_coldefs(
        left_schema
            .pk_cols
            .iter()
            .map(|&c| (left_schema, c))
            .chain(right_schema.pk_cols.iter().map(|&c| (right_schema, c))),
        |slot, _| format!("_pair_pk_{slot}"),
    )
}

/// The output PK columns of a range-correlated EXISTS/IN view: the outer source
/// PK under its own names (it also rides the payload verbatim).
pub(crate) fn src_pk_coldefs(schema: &Schema) -> Vec<ColumnDef> {
    rekey_pk_coldefs(schema.pk_cols.iter().map(|&c| (schema, c)), |_, src| src.name.clone())
}

/// The pair-PK slot list `[a.pk…, b.pk…]` over a layout carrying A's kept payload
/// from `a_base` and B's from `b_base`. Each side's PK arity comes from the side,
/// so only the two bases vary. One home, so the inner terms and the null-fill
/// branches key byte-identically and cancel.
pub(super) fn pair_pk_slots(sides: &[JoinSide; 2], a_base: usize, b_base: usize) -> Vec<usize> {
    (a_base..a_base + sides[0].pa())
        .chain(b_base..b_base + sides[1].pa())
        .collect()
}

/// The shared null-fill tail of a range/band outer join: null-extend `ν_P` with
/// the O-side NULL columns, re-key onto the pair-PK `[a.pk…, b.pk…]` (the other
/// side's PK rides in the NULL-O payload, packing to the synthetic 0), and
/// project to the FULL combined width `[pair-PK, A, B]`.
///
/// `preserved_is_left` selects the canonical (P=A) vs reordered (P=B) layout:
/// `null_extend` appends and `map_reindex` locks its payload to input order, so
/// only the final `cb.map` can place the NULL-O columns before P. All the geometry
/// derives from `sides`.
pub(super) fn emit_range_null_fill_tail(
    cb: &mut CircuitBuilder,
    sides: &[JoinSide; 2],
    nf_keyed: NodeId,
    preserved_is_left: bool,
) -> NodeId {
    let (left, right) = (&sides[0], &sides[1]);
    let pair_pk = left.pa() + right.pa();

    let (p, o) = if preserved_is_left {
        (left, right)
    } else {
        (right, left)
    };
    let (p_pk, p_n, o_n) = (p.pa(), p.n(), o.n());
    let nullfill = cb.null_extend(nf_keyed, &o.kept_type_codes()); // [P.pk × p_pk, P, NULL-O]

    // Pair-PK [a.pk…, b.pk…], both read out of the payload: the preserved side's
    // pk at the front of its own kept columns behind the key region, the other
    // side's at the front of the NULL-O payload (→ synthetic 0).
    let nf_pair_pk_cols = if preserved_is_left {
        pair_pk_slots(sides, p_pk, p_pk + p_n)
    } else {
        pair_pk_slots(sides, p_pk + p_n, p_pk)
    };

    // Keep the [P, NULL-O] payload and DROP the leading P.pk × p_pk key region —
    // its columns are also inside P, and nothing downstream reads the region.
    let keep: Vec<u32> = (p_pk as u32..(p_pk + p_n + o_n) as u32).collect();
    let nf_rekey = cb.map_reindex(
        nullfill,
        &self_derived_key(&nf_pair_pk_cols),
        &keep,
        ReindexRole::Auxiliary,
    );

    // nf_rekey output: [pair-PK, <[P, NULL-O] in input order>] — the payload sits
    // directly behind the pair-PK. P = A is already canonical; P = B carries
    // `[B, NULL-A]` and takes the same swap the BA join term does, so the branch
    // is full width `[pair-PK, A, B]` — the layout the post-union WHERE and the
    // final user projection both read.
    let nf_full_projection: Vec<usize> = if preserved_is_left {
        (pair_pk..pair_pk + p_n + o_n).collect()
    } else {
        ba_to_ab_cols(pair_pk, o_n, p_n).collect()
    };
    cb.map(nf_rekey, &nf_full_projection)
}

/// `(P_all, π_P(inner))`, the operands of a band `ν_P`, both keyed on P's source PK.
/// `payload_off` locates P inside the pre-rekey `[key region × k, A, B]` payload.
pub(crate) fn band_nu_operands(
    cb: &mut CircuitBuilder,
    merged: NodeId,
    k: usize,
    payload_off: usize,
    side: &JoinSide,
    raw: NodeId,
) -> (NodeId, NodeId) {
    let base = k + payload_off;
    let pk: Vec<usize> = (base..base + side.pa()).collect();
    let keep: Vec<u32> = (base as u32..(base + side.n()) as u32).collect();
    let pi = cb.map_reindex(merged, &self_derived_key(&pk), &keep, ReindexRole::Auxiliary);
    let all = rekey_on_source_pk(cb, raw, side, ReindexRole::Auxiliary);
    (all, pi)
}

/// What both range builders carry out of [`range_prologue`]: the two gated,
/// reindexed sides, the per-slot common types, and the canonical (left-to-right)
/// range relation — term BA's rel, term AB's being its converse.
pub(crate) struct RangePrologue<'a> {
    pub(crate) sides: [EquiSide<'a>; 2],
    all_tcs: Vec<TypeCode>,
    op: RangeRel,
}

impl RangePrologue<'_> {
    /// The key arity: the eq prefix plus the one range slot — the width of the
    /// key region every term leads with.
    pub(crate) fn k(&self) -> usize {
        self.all_tcs.len()
    }

    /// The eq-prefix width the range probe matches on before comparing.
    pub(crate) fn n_eq(&self) -> usize {
        self.k() - 1
    }

    /// The view's pre-rekey key-region column defs — one `_join_pk` slot per eq
    /// pair plus the range slot.
    pub(crate) fn out_pk_coldefs(&self) -> Vec<ColumnDef> {
        join_pk_coldefs(&self.all_tcs)
    }

    /// The two range terms and their normalization onto `[key region, A, B]`, each
    /// term's `(delta, trace, relation)` triple paired once. The traces are
    /// parameters because the pure-range shape integrates the worker-filtered slice
    /// rather than the reindex itself.
    pub(crate) fn range_merged(
        &self,
        cb: &mut CircuitBuilder,
        trace_a: NodeId,
        trace_b: NodeId,
        left_n: usize,
        right_n: usize,
    ) -> NodeId {
        let n_eq = self.n_eq() as u8;
        let (reindex_a, reindex_b) = (self.sides[0].reindex, self.sides[1].reindex);
        let join_ab = cb.join_with_trace_range_node(reindex_a, trace_b, n_eq, converse_rel(self.op));
        let join_ba = cb.join_with_trace_range_node(reindex_b, trace_a, n_eq, self.op);
        normalize_to_ab(cb, join_ab, join_ba, self.k(), left_n, right_n)
    }

    /// The pure-range matched set: the rows of A matching the one-row threshold
    /// `m = MAX/MIN(b.range)`, re-keyed onto `[a.pk…, A]`.
    pub(crate) fn pure_range_matched(&self, cb: &mut CircuitBuilder, int_a: NodeId, trace_a: NodeId) -> NodeId {
        let left = self.sides[0].side;
        let (k, n_eq, left_n) = (self.k(), self.n_eq() as u8, left.n());
        let want_max = matches!(self.op, RangeRel::Lt | RangeRel::Le);
        let agg_func = if want_max { WireAggFunc::Max } else { WireAggFunc::Min };

        // Decodes the OPK key into a native payload value, so MIN/MAX order values.
        let mbh = cb.map_hash_row(self.sides[1].reindex, &[(0, None)], 0);
        // Local over the broadcast B, so every worker holds the same extremum. No
        // ground row: a `m = NULL` seed would break `A − 0 = A` over an empty B.
        let red = cb.reduce_multi_local(mbh, &[], &[(agg_func, 1)], false); // [_group_pk:U128, m:Tc]
        let reindex_m = cb.map_reindex(red, &self_derived_key(&[1]), &[], ReindexRole::Auxiliary);
        let trace_m = cb.integrate_trace(reindex_m);

        let j_am = cb.join_with_trace_range_node(int_a, trace_m, n_eq, converse_rel(self.op));
        let j_ma = cb.join_with_trace_range_node(reindex_m, trace_a, n_eq, self.op);
        // Range-join output = [_join_pk x k, delta payload, trace payload]. `m` has no
        // payload, so A sits at `k..k + left_n` in both terms.
        let a_cols: Vec<usize> = (k..k + left_n).collect();
        let m_am = cb.map(j_am, &a_cols);
        let m_ma = cb.map(j_ma, &a_cols);
        let matched_raw = cb.union(m_am, m_ma); // [_join_pk(PK), A]
        rekey_pure_range_a(cb, matched_raw, left.pa(), left_n)
    }

    /// `A − matched`, plus the NULL-range-key rows no threshold can match; `raw` is
    /// A's input.
    pub(crate) fn pure_range_unmatched(
        &self,
        cb: &mut CircuitBuilder,
        matched: NodeId,
        int_a: NodeId,
        raw: NodeId,
    ) -> Result<NodeId, GnitzSqlError> {
        let left = self.sides[0].side;
        let a_pass = rekey_pure_range_a(cb, int_a, left.pa(), left.n());
        let neg = cb.negate(matched);
        let nf_match = cb.union(a_pass, neg);
        if self.sides[0].key_nullable {
            union_null_key_rows(cb, nf_match, raw, &self.sides[0].cols, left)
        } else {
            Ok(nf_match)
        }
    }
}

/// Gate and reindex both sides on `[eq slots…, range slot]`.
pub(crate) fn range_prologue<'a>(
    cb: &mut CircuitBuilder,
    sides: &'a [JoinSide; 2],
    inputs: [NodeId; 2],
    eq: &[EqPair],
    range: &HirRange,
) -> Result<RangePrologue<'a>, GnitzSqlError> {
    let (left, right) = (&sides[0], &sides[1]);
    let EquiKeys {
        left: left_cols,
        right: right_cols,
        tcs: eq_tcs,
    } = resolve_eq_cols(eq, left, right)?;
    let left_cols: Vec<usize> = left_cols
        .into_iter()
        .chain(std::iter::once(slot_of(&left.seg.frame.layout, range.left)?))
        .collect();
    let right_cols: Vec<usize> = right_cols
        .into_iter()
        .chain(std::iter::once(slot_of(&right.seg.frame.layout, range.right)?))
        .collect();
    let all_tcs: Vec<TypeCode> = eq_tcs.into_iter().chain(std::iter::once(range.tc)).collect();
    let a = gated_side(cb, left, inputs[0], left_cols, &all_tcs)?;
    let b = gated_side(cb, right, inputs[1], right_cols, &all_tcs)?;
    Ok(RangePrologue { sides: [a, b], all_tcs, op: range.op })
}

/// The pure-range `[Tc PK, A_kept]` -> `[a.pk…, A_kept]` re-key. One home, so the
/// matched set's `−a` and the passthrough's `+a` are byte-identical and cancel.
fn rekey_pure_range_a(cb: &mut CircuitBuilder, node: NodeId, pa: usize, pl: usize) -> NodeId {
    let keep: Vec<u32> = (1..1 + pl as u32).collect();
    cb.map_reindex(
        node,
        &self_derived_key(&(1..1 + pa).collect::<Vec<_>>()),
        &keep,
        ReindexRole::Auxiliary,
    )
}

/// The k synthetic `_join_pk` output PK columns of an equi join / equi EXISTS
/// view, hidden (a physical PK column, not a presentation column — `SELECT *`
/// omits it and it is not name-resolvable). Each slot carries its pair's common
/// type `T_i` — the single persisted stride both sides' reindex Maps and every
/// cross-process consumer re-derive. The first column keeps the name
/// `_join_pk` at k = 1 (preserving the shippable single-key catalog name);
/// composite keys use `_join_pk_{i}`.
pub(crate) fn join_pk_coldefs(target_tcs: &[TypeCode]) -> Vec<ColumnDef> {
    let k = target_tcs.len();
    target_tcs
        .iter()
        .enumerate()
        .map(|(i, &t)| {
            let name = if k == 1 {
                "_join_pk".to_string()
            } else {
                format!("_join_pk_{i}")
            };
            ColumnDef::new(name, t, false).hidden()
        })
        .collect()
}

/// Union the pure-range NULL-range-key rows into `nf_match` (`A − matched`):
/// NULL-key rows never reach the integrated trace (3VL) and never match the
/// threshold, so they get their own branch off the NULL-gate-unfiltered
/// `source`, re-keyed to the preserved side's source PK and routed ONCE by a
/// local `worker_filter` (no exchange) — broadcast would emit W× copies the
/// output shard would sum. (The compiler makes the filter a keep-all identity
/// for an all-replicated view, which runs correct-local over the full broadcast
/// on every worker.) `source` is the caller's semantic preserved input (the raw
/// input for the LEFT join; the locally pre-filtered outer for EXISTS).
fn union_null_key_rows(
    cb: &mut CircuitBuilder,
    nf_match: NodeId,
    source: NodeId,
    cols: &[usize],
    left: &JoinSide,
) -> Result<NodeId, GnitzSqlError> {
    let anull = cb.filter(
        source,
        multi_null_filter_prog(cols, &left.seg.frame.schema.columns, true)?,
    );
    let anull_keyed = rekey_on_source_pk(cb, anull, left, ReindexRole::Auxiliary);
    let anull_owned = cb.worker_filter(anull_keyed);
    Ok(cb.union(nf_match, anull_owned))
}

/// `inner ∪ ν_A ∪ ν_B` — the equi outer join's null-fill, unioned onto the inner
/// output for each preserved side; `inner_merged` unchanged for INNER.
/// Semi/Anti/Mark never reach here: `preserves` is `false` for all three, and
/// their own ν is composed in `exists.rs`.
///
/// Per preserved side P, `ν_P = positive_part(P_all − π_P(inner))`. `π_P(inner)`
/// re-keys the inner output back to P's identity carrying the matched
/// multiplicity `m = w_P·S`, and `P_all` re-keys the *unfiltered* P input with the
/// same reindex program, so a matched row's `+w_P` and `−m` cancel before the
/// clamp. Both operands sit on the `_join_pk` worker, so the difference is
/// partition-local. Subtracting the raw multiplicity keeps it weight-exact for a
/// bag-valued preserved side and absorbs the within-epoch and cross-epoch
/// transients.
///
/// `inner_merged` feeds both `π_P` and these unions, so it rides the
/// non-destructive second union operand throughout (`op_union` empties the first).
pub(crate) fn emit_equi_null_fill(
    cb: &mut CircuitBuilder,
    inner_merged: NodeId,
    kind: JoinType,
    terms: &EquiTerms<'_>,
) -> NodeId {
    if kind == JoinType::Inner {
        return inner_merged;
    }
    let k = terms.k();
    let (pl, pr) = (terms.kept_n(true), terms.kept_n(false));
    let mut merged = inner_merged;
    // Each preserved side P: `p0` locates its kept payload in the merged layout
    // (`k` for A, `k + pl` for B) and the other side supplies the NULL region.
    for (preserved_is_left, p0, p_n) in [(true, k, pl), (false, k + pl, pr)] {
        if !kind.preserves(preserved_is_left) {
            continue;
        }
        let p_all = terms.p_all(cb, preserved_is_left);
        let proj_p = cb.map(inner_merged, &(p0..p0 + p_n).collect::<Vec<_>>()); // π_P(inner) = [_join_pk, P]
        let nu_p = cb.positive_diff(p_all, proj_p); // max(0, P − π_P(inner))
                                                    // A side that keeps nothing contributes no NULL region at all.
        let o_tcs = terms.side(!preserved_is_left).side.kept_type_codes();
        let ext = if o_tcs.is_empty() {
            nu_p
        } else {
            cb.null_extend(nu_p, &o_tcs)
        };
        let branch = if preserved_is_left {
            ext // [_join_pk, A, NULL-B] — canonical
        } else {
            cb.map(ext, &ba_to_ab_cols(k, pl, pr).collect::<Vec<_>>())
        };
        merged = cb.union(branch, merged);
    }
    merged
}
