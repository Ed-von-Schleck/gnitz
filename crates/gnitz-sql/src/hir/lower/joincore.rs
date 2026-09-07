//! The AST-free circuit primitives the join and EXISTS/IN lowering shells drive:
//! the equi and range/band prologues, the term pair each emits, the pure-range
//! threshold pipeline, the null-fill tails, and the synthetic-key column defs.
//!
//! The axis against [`super::prims`] is granularity: `prims` builds one program or
//! emits at most one node; every item here emits a multi-node DBSP construction.

use super::super::guards::converse_rel;
use super::super::{slot_of, EqPair, HirRange, JoinType};
use super::prims::{multi_null_filter_prog, null_gate, rekey_aux_on_source_pk, self_derived_key};
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
            .map(|p| slot_of(&left.seg.layout, p.left))
            .collect::<Result<_, _>>()?,
        right: eq
            .iter()
            .map(|p| slot_of(&right.seg.layout, p.right))
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

/// One side of the symmetric equi-join term emission: its (unfiltered) input
/// node, its reindex key ([`side_reindex_key`]), the source column defs, and the
/// kept payload columns.
struct EquiSide<'a> {
    side: &'a JoinSide,
    input: NodeId,
    key: Vec<ReindexSlot>,
}

impl EquiSide<'_> {
    /// The key's source columns alone — what the NULL gate tests. They index the
    /// raw input, as the reindex key does.
    fn key_cols(&self) -> Vec<usize> {
        self.key.iter().map(|&(c, _)| c as usize).collect()
    }

    /// The type codes of this side's kept payload columns — what `null_extend`
    /// needs to synthesize the OTHER side's NULL region.
    fn kept_type_codes(&self) -> Vec<u8> {
        self.side.coldefs.iter().map(|c| c.type_code as u8).collect()
    }
}

/// One equi side as [`emit_equi_join_terms`] emitted it: the side, its NULL gate's
/// nullability fact, and the reindex that fact selects between. Bundled so `all`
/// cannot pair one side's gate with the other's reindex.
struct SideTerm<'a> {
    side: EquiSide<'a>,
    key_nullable: bool,
    reindex: NodeId,
}

impl SideTerm<'_> {
    /// `P_all`: this side's *unfiltered* input re-keyed onto the join key. A NOT
    /// NULL key gated nothing, so there the emitted reindex already is it.
    fn all(&self, cb: &mut CircuitBuilder) -> NodeId {
        if self.key_nullable {
            cb.map_reindex(
                self.side.input,
                &self.side.key,
                &self.side.side.keep,
                ReindexRole::ScatterKey,
            )
        } else {
            self.reindex
        }
    }
}

/// What [`equi_prologue`] hands back: the two sides as it built them, the term
/// pair over them, and the per-slot common types. The key arity, the output
/// `_join_pk` defs and each side's kept width are accessors here rather than
/// facts a caller restates.
pub(crate) struct EquiTerms<'a> {
    a: SideTerm<'a>,
    b: SideTerm<'a>,
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
        self.side(is_left).side.side.n()
    }

    /// `P_all` — that side's unfiltered input re-keyed onto the join key.
    pub(crate) fn p_all(&self, cb: &mut CircuitBuilder, is_left: bool) -> NodeId {
        self.side(is_left).all(cb)
    }

    /// `A_all` — the outer side's re-key, which the EXISTS/IN shells subtract
    /// `π_A(inner)` from.
    pub(crate) fn a_all(&self, cb: &mut CircuitBuilder) -> NodeId {
        self.p_all(cb, true)
    }

    fn side(&self, is_left: bool) -> &SideTerm<'a> {
        if is_left {
            &self.a
        } else {
            &self.b
        }
    }
}

/// The prologue every equi builder opens with, the twin of [`range_prologue`]:
/// resolve the eq pairs to each side's key slots and per-pair common type, then
/// emit the symmetric term pair over the NULL-gated, reindexed sides. `keep_a` /
/// `keep_b` are the reindex-payload keep lists, which prune the join traces.
pub(crate) fn equi_prologue<'a>(
    cb: &mut CircuitBuilder,
    eq: &[EqPair],
    a_in: EquiInput<'a>,
    b_in: EquiInput<'a>,
) -> Result<EquiTerms<'a>, GnitzSqlError> {
    let EquiKeys { left, right, tcs } = resolve_eq_cols(eq, a_in.side, b_in.side)?;
    let a = a_in.keyed(side_reindex_key(&left, &a_in.side.seg.schema.columns, &tcs));
    let b = b_in.keyed(side_reindex_key(&right, &b_in.side.seg.schema.columns, &tcs));
    emit_equi_join_terms(cb, a, b, tcs)
}

/// One equi side as its shell resolved it: the input relation, the (already
/// prefiltered) delta node, and the reindex-payload keep list that prunes its
/// trace.
#[derive(Clone, Copy)]
pub(crate) struct EquiInput<'a> {
    pub(crate) side: &'a JoinSide,
    pub(crate) node: NodeId,
}

impl<'a> EquiInput<'a> {
    fn keyed(self, key: Vec<ReindexSlot>) -> EquiSide<'a> {
        EquiSide { side: self.side, input: self.node, key }
    }
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
    let o_col_tcs: Vec<u8> = o.coldefs.iter().map(|c| c.type_code as u8).collect();
    let nullfill = cb.null_extend(nf_keyed, &o_col_tcs); // [P.pk × p_pk, P, NULL-O]

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

/// `π_P(inner)` for a band join: re-key the inner output onto the preserved side's
/// source PK and keep that side's payload, so the output is `[P.pk × p, P]`. Both
/// operands of `ν_P = positive_part(P_all − π_P(inner))` must be keyed
/// byte-identically or the clamp silently mis-weights, so every band ν builds its
/// π here.
///
/// `payload_off` is the side's offset inside the pre-rekey `[key region × k, A, B]`
/// payload; the key and the keep both derive from it.
pub(crate) fn band_pi_preserved(
    cb: &mut CircuitBuilder,
    merged: NodeId,
    k: usize,
    payload_off: usize,
    side: &JoinSide,
) -> NodeId {
    let base = k + payload_off;
    let pk: Vec<usize> = (base..base + side.pa()).collect();
    let keep: Vec<u32> = (base as u32..(base + side.n()) as u32).collect();
    cb.map_reindex(merged, &self_derived_key(&pk), &keep, ReindexRole::Auxiliary)
}

/// The pure-range unmatched set `A − matched`, union the NULL-range-key rows a
/// threshold comparison can never match. The pure-range LEFT JOIN null-fill and the
/// NOT EXISTS / mark-unmatched branch are the same node sequence, so they share it.
pub(crate) fn pure_range_unmatched(
    cb: &mut CircuitBuilder,
    matched: NodeId,
    a_pass: NodeId,
    key_nullable: bool,
    raw: NodeId,
    reindex_cols: &[usize],
    left: &JoinSide,
) -> Result<NodeId, GnitzSqlError> {
    let neg = cb.negate(matched);
    let nf_match = cb.union(a_pass, neg);
    if key_nullable {
        union_null_key_rows(cb, nf_match, raw, reindex_cols, left)
    } else {
        Ok(nf_match)
    }
}

/// Emit the symmetric 2-term equi join over two already-keyed sides —
/// B-first interleaved gates/reindexes, then both traces, then
/// `join_ab`/`join_ba` — the sequence shared by the join lowering and the equi
/// EXISTS/IN builder. A NULL equi-join key must match nothing
/// (SQL 3VL: NULL = anything, including NULL = NULL, is unknown); `map_reindex`
/// would promote a NULL integer key to synthetic PK 0 and a NULL string to the
/// empty-content hash 0, colliding with a real 0/"" key and with every other
/// NULL — so `null_gate` drops NULL-keyed rows from the match on both sides
/// (and leaves a NOT NULL side untouched, zero overhead).
fn emit_equi_join_terms<'a>(
    cb: &mut CircuitBuilder,
    a: EquiSide<'a>,
    b: EquiSide<'a>,
    tcs: Vec<TypeCode>,
) -> Result<EquiTerms<'a>, GnitzSqlError> {
    let (b_gated, key_nullable_b) = null_gate(cb, b.input, &b.key_cols(), &b.side.seg.schema.columns)?;
    let reindex_b = cb.map_reindex(b_gated, &b.key, &b.side.keep, ReindexRole::ScatterKey);
    let (a_gated, key_nullable_a) = null_gate(cb, a.input, &a.key_cols(), &a.side.seg.schema.columns)?;
    let reindex_a = cb.map_reindex(a_gated, &a.key, &a.side.keep, ReindexRole::ScatterKey);
    let trace_a = cb.integrate_trace(reindex_a);
    let trace_b = cb.integrate_trace(reindex_b);
    let join_ab = cb.join_with_trace_node(reindex_a, trace_b); // ΔA ⋈ z^{-1}(I(B))
    let join_ba = cb.join_with_trace_node(reindex_b, trace_a); // ΔB ⋈ z^{-1}(I(A))
    Ok(EquiTerms {
        a: SideTerm {
            side: a,
            key_nullable: key_nullable_a,
            reindex: reindex_a,
        },
        b: SideTerm {
            side: b,
            key_nullable: key_nullable_b,
            reindex: reindex_b,
        },
        tcs,
        join_ab,
        join_ba,
    })
}

/// What both range builders carry out of [`range_prologue`]: the two reindexed
/// sides, the LEFT key's nullability (the NULL-key branch's gate), the left key
/// slots (re-read by that branch), the per-slot common types, and the §3 term
/// relations (`rel_ab = converse(op)` for term AB, `rel_ba = op` for term BA).
pub(crate) struct RangePrologue {
    pub(crate) reindex_a: NodeId,
    pub(crate) reindex_b: NodeId,
    pub(crate) left_key_nullable: bool,
    pub(crate) left_reindex_cols: Vec<usize>,
    pub(crate) all_tcs: Vec<TypeCode>,
    pub(crate) rel_ab: RangeRel,
    pub(crate) rel_ba: RangeRel,
}

impl RangePrologue {
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
        let join_ab = cb.join_with_trace_range_node(self.reindex_a, trace_b, n_eq, self.rel_ab);
        let join_ba = cb.join_with_trace_range_node(self.reindex_b, trace_a, n_eq, self.rel_ba);
        normalize_to_ab(cb, join_ab, join_ba, self.k(), left_n, right_n)
    }
}

/// The prologue every range/band builder opens with: resolve the eq pairs and the
/// range conjunct to each side's key slots (`[eq slots…, range slot]`, the order
/// the whole range geometry is stated against), NULL-gate both sides over all of
/// them (SQL 3VL), and reindex each onto its key. Partition filters, traces and
/// join terms diverge per shape and stay with the callers.
pub(crate) fn range_prologue(
    cb: &mut CircuitBuilder,
    sides: &[JoinSide; 2],
    a_input: NodeId,
    b_input: NodeId,
    eq: &[EqPair],
    range: &HirRange,
) -> Result<RangePrologue, GnitzSqlError> {
    let (left, right) = (&sides[0], &sides[1]);
    let EquiKeys {
        left: left_cols,
        right: right_cols,
        tcs: eq_tcs,
    } = resolve_eq_cols(eq, left, right)?;
    let (left_coldefs, right_coldefs) = (&left.seg.schema.columns, &right.seg.schema.columns);

    let left_reindex_cols: Vec<usize> = left_cols
        .into_iter()
        .chain(std::iter::once(slot_of(&left.seg.layout, range.left)?))
        .collect();
    let right_reindex_cols: Vec<usize> = right_cols
        .into_iter()
        .chain(std::iter::once(slot_of(&right.seg.layout, range.right)?))
        .collect();
    let all_tcs: Vec<TypeCode> = eq_tcs.into_iter().chain(std::iter::once(range.tc)).collect();
    let left_key = side_reindex_key(&left_reindex_cols, left_coldefs, &all_tcs);
    let right_key = side_reindex_key(&right_reindex_cols, right_coldefs, &all_tcs);

    let (a_gated, left_key_nullable) = null_gate(cb, a_input, &left_reindex_cols, left_coldefs)?;
    let (b_gated, _) = null_gate(cb, b_input, &right_reindex_cols, right_coldefs)?;
    let reindex_a = cb.map_reindex(a_gated, &left_key, &left.keep, ReindexRole::ScatterKey);
    let reindex_b = cb.map_reindex(b_gated, &right_key, &right.keep, ReindexRole::ScatterKey);
    Ok(RangePrologue {
        reindex_a,
        reindex_b,
        left_key_nullable,
        left_reindex_cols,
        all_tcs,
        rel_ab: converse_rel(range.op),
        rel_ba: range.op,
    })
}

/// The inline pure-range (`n_eq == 0`) threshold pipeline: the one-row
/// `m = MAX/MIN(b.range_col)` reduce and its trace, the two matched terms against
/// it, and their `[a.pk…, A]` re-key. EXISTS is `matched`; NOT EXISTS and the
/// LEFT JOIN null-fill are `A − matched`.
///
/// `m` is computed locally on every worker over the broadcast `reindex_b`: a
/// global extremum over a fully-broadcast input is the same everywhere, so no
/// scatter/gather. `map_hash_row` moves the `Tc` PK into a payload column,
/// decoding OPK to native, so the reduce aggregates the value and not the OPK
/// bytes (which would invert signed MIN/MAX); non-float MIN/MAX carries its
/// source type, so `reindex_m` self-derives the OPK order back. `global_ground =
/// false`: a ground row would seed `(m=NULL)` and break `A − 0 = A` over an empty
/// other side.
///
/// The two terms mirror `join_ab`/`join_ba` exactly, with only the trace swapped.
/// Both carry `n_eq == 0`, as the shape's other two do, and must:
/// `circuit_join_relay` reads one arbitrary join node's kind and applies the
/// relay it derives to every source.
pub(crate) fn build_pure_range_threshold(
    cb: &mut CircuitBuilder,
    left: &JoinSide,
    range: &HirRange,
    reindex_b: NodeId,
    int_a: NodeId,
    trace_a: NodeId,
) -> NodeId {
    let left_n = left.n();
    let k = 1usize; // pure range: no eq prefix, one range slot
    let want_max = matches!(range.op, RangeRel::Lt | RangeRel::Le);
    let agg_func = if want_max { WireAggFunc::Max } else { WireAggFunc::Min };

    let mbh = cb.map_hash_row(reindex_b, &[(0, None)], 0);
    // Empty group set (one global threshold over the broadcast other side) -> the
    // synthetic `_group_pk` fold, like every other empty-group reduce.
    let red = cb.reduce_multi_local(mbh, &[], &[(agg_func, 1)], false); // [_group_pk:U128, m:Tc]
    let reindex_m = cb.map_reindex(red, &self_derived_key(&[1]), &[], ReindexRole::Auxiliary);
    let trace_m = cb.integrate_trace(reindex_m);

    let j_am = cb.join_with_trace_range_node(int_a, trace_m, 0, converse_rel(range.op));
    let j_ma = cb.join_with_trace_range_node(reindex_m, trace_a, 0, range.op);
    // Range-join output = [_join_pk x k, delta payload, trace payload]. `m` has no
    // payload, so A sits at `k..k + left_n` in both terms.
    let a_cols: Vec<usize> = (k..k + left_n).collect();
    let m_am = cb.map(j_am, &a_cols);
    let m_ma = cb.map(j_ma, &a_cols);
    let matched_raw = cb.union(m_am, m_ma); // [_join_pk(PK), A]
    rekey_pure_range_a(cb, matched_raw, left.pa(), left_n)
}

/// The pure-range `[Tc PK, A_kept]` -> `[a.pk…, A_kept]` re-key. One home, so the
/// matched set's `−a` and the passthrough's `+a` are byte-identical and cancel.
pub(crate) fn rekey_pure_range_a(cb: &mut CircuitBuilder, node: NodeId, pa: usize, pl: usize) -> NodeId {
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

/// Whether a column name is one this module mints for a synthetic join key.
/// Such a key identifies a matched *pair*, not a row, and is not unique — so a
/// relation keyed by one has no row identity. The predicate sits beside the two
/// producers ([`join_pk_coldefs`] and `pair_pk_coldefs`) so renaming either
/// cannot silently make a reader believe the key is a row key.
pub(crate) fn is_join_key_name(name: &str) -> bool {
    name.starts_with("_join_pk") || name.starts_with("_pair_pk")
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
    let schema = &left.seg.schema;
    let anull = cb.filter(source, multi_null_filter_prog(cols, &schema.columns, true)?);
    let anull_keyed = rekey_aux_on_source_pk(cb, anull, schema, &left.keep);
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
