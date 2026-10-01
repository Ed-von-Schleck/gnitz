use super::*;
use crate::query::compiler::fixtures::*;
use crate::test_support::{make_schema_u64_i64, pk_payload_schema, scan_keyed, self_typed_slots};
use gnitz_store::schema::{Placement, SchemaColumn};
use gnitz_store::storage::BatchBuilder;
use gnitz_wire::{Circuit, ComputeMap, JoinKind, KeyRange, NullKeys, PkColList, RangeRel, ReindexRole, TypeCode};

/// `derive`'s routing metadata, placed as a one-column-PK view.
fn derive(c: Circuit, registry: &RelationRegistry) -> Result<ViewMeta, String> {
    ViewMeta::derive(&loaded(c), registry, 1).map(|(meta, _)| meta)
}

/// A U64 PK and five I64 payload columns: every key a fixture names is payload.
fn wide_schema() -> SchemaDescriptor {
    let mut cols = vec![SchemaColumn::new(TypeCode::U64, false)];
    cols.extend((0..5).map(|_| SchemaColumn::new(TypeCode::I64, false)));
    SchemaDescriptor::new(&cols, &[0])
}

/// 3-column compound PK `(U32, U64, U64)` + one payload, distributed by its
/// leading PK column.
fn clustered_schema() -> SchemaDescriptor {
    pk_payload_schema(&[TypeCode::U32, TypeCode::U64, TypeCode::U64]).with_placement(Placement::Keyed { prefix_len: 1 })
}

fn range(n_eq: u8) -> JoinKind {
    JoinKind::Range { n_eq, rel: RangeRel::Lt }
}

/// `d ⋈ I(d)`: the smallest circuit in which `d`'s source reaches a join.
fn join_with_own_trace(c: &mut Circuit, d: NodeId, kind: JoinKind) -> NodeId {
    let t = c.integrate_trace(d);
    c.join(d, t, kind, false)
}

/// A reindex of `input` on `cols` of [`wide_schema`], stated as `source`'s route.
fn states_route(c: &mut Circuit, input: NodeId, source: u64, cols: &[u32]) -> NodeId {
    let key = self_typed_slots(&wide_schema(), cols);
    let role = ReindexRole::ScatterKey { source, source_key: key.clone() };
    c.map_reindex(input, &key, &[0], role, NullKeys::Keep)
}

/// A reindex on `key` that states no route.
fn aux_reindex(c: &mut Circuit, input: NodeId, key: &[gnitz_wire::ReindexSlot]) -> NodeId {
    c.map_reindex(input, key, &[0], ReindexRole::Auxiliary, NullKeys::Keep)
}

/// `relay` is keyed and splits rows by the join key over `cols` of `schema`: it
/// sends every row of a probe whose columns all differ where that key does.
fn assert_routes_by(relay: Option<&Relay>, schema: &SchemaDescriptor, cols: &[u32]) {
    let Some(Relay::Round(got) | Relay::Share(got)) = relay else {
        panic!("a keyed relay");
    };
    let mut bb = BatchBuilder::new(*schema);
    for i in 0..64u64 {
        bb.begin_row(i as u128, 1);
        for c in 0..schema.num_payload_cols() as u64 {
            bb.put_u64((i << 8 | c).wrapping_mul(0x9E37_79B9_7F4A_7C15));
        }
        bb.end_row();
    }
    let probe = bb.finish();
    let want = ScatterPlan::join(schema, &self_typed_slots(schema, cols)).unwrap();
    let (mut got_lists, mut want_lists) = (Vec::new(), Vec::new());
    assert_eq!(
        got.route(&probe, &mut got_lists, 4),
        want.route(&probe, &mut want_lists, 4)
    );
}

// ── The output exchange ─────────────────────────────────────────────────

/// The output shard moves nothing exactly where a walk back through row-local
/// nodes ends at a scan whose rows are already placed by the shard key. Exchanging
/// is correct too, so nothing above this layer can tell the skip fired.
#[test]
fn the_output_exchange_is_skipped_only_behind_a_row_local_walk_to_a_scan_placed_by_the_shard_key() {
    // PK last, so a shard column behind a map — an index into the map's output,
    // whose leading slots are the source PK — is not the source's column index.
    let i64 = SchemaColumn::new(TypeCode::I64, false);
    let u64 = SchemaColumn::new(TypeCode::U64, false);
    let pk_last = SchemaDescriptor::new(&[i64, i64, u64], &[2]);
    let clustered = clustered_schema();
    let compute = |c: &mut Circuit, n: NodeId| {
        c.map_expr(
            n,
            ComputeMap {
                program: dummy_expr_blob(),
                out_cols: vec![],
            },
        )
    };
    type Mid = fn(&mut Circuit, NodeId) -> NodeId;
    let mapped = SchemaDescriptor::new(&[u64, i64, i64], &[0]);
    let key_only = SchemaDescriptor::new(&[u64], &[0]);
    let rekeyed = SchemaDescriptor::new(&[i64, i64], &[0]);
    // (why, the source, the walk, the schema it hands the shard, shard columns, skipped)
    type Case = (
        &'static str,
        SchemaDescriptor,
        Mid,
        SchemaDescriptor,
        &'static [u32],
        bool,
    );
    let cases: [Case; 14] = [
        ("a bare scan", pk_last, |_, n| n, pk_last, &[2], true),
        ("a payload column", pk_last, |_, n| n, pk_last, &[0], false),
        ("an empty key", pk_last, |_, n| n, pk_last, &[], false),
        ("the CLUSTER BY prefix", clustered, |_, n| n, clustered, &[0], true),
        ("not the leading PK column", clustered, |_, n| n, clustered, &[1], false),
        (
            "a filter is transparent",
            pk_last,
            |c, n| c.filter(n, dummy_expr_blob()),
            pk_last,
            &[2],
            true,
        ),
        (
            "a copy list carries the PK region to its leading slot",
            pk_last,
            |c, n| c.map(n, &[0, 1]),
            mapped,
            &[0],
            true,
        ),
        (
            "a payload slot behind a map",
            pk_last,
            |c, n| c.map(n, &[0, 1]),
            mapped,
            &[1],
            false,
        ),
        (
            "an expression map carries the PK region",
            pk_last,
            compute,
            key_only,
            &[0],
            true,
        ),
        (
            "filters and maps interleave",
            pk_last,
            |c, n| {
                let f = c.filter(n, dummy_expr_blob());
                let m = c.map(f, &[0, 1]);
                c.filter(m, dummy_expr_blob())
            },
            mapped,
            &[0],
            true,
        ),
        (
            "a reindex re-keys the PK",
            pk_last,
            |c, n| aux_reindex(c, n, &[(0, TypeCode::I64)]),
            rekeyed,
            &[0],
            false,
        ),
        (
            "a WorkerFilter is not a Filter",
            pk_last,
            |c, n| c.worker_filter(n),
            pk_last,
            &[2],
            false,
        ),
        (
            "a fan-in draws from two sources",
            pk_last,
            |c, n| {
                let other = scan(c, 8);
                c.union(n, other)
            },
            pk_last,
            &[2],
            false,
        ),
        (
            "a filter over a fan-in",
            pk_last,
            |c, n| {
                let other = scan(c, 8);
                let u = c.union(n, other);
                c.filter(u, dummy_expr_blob())
            },
            pk_last,
            &[2],
            false,
        ),
    ];
    for (why, schema, mid, at_shard, cols, want) in cases {
        let mut c = Circuit::default();
        let source = scan(&mut c, 7);
        let tip = mid(&mut c, source);
        let shard = c.shard(tip, cols);
        let scatter = ScatterPlan::group(&at_shard, cols).expect(why);
        assert_eq!(
            skips_output_exchange(&loaded(c), shard, &scatter, &sources([(7, schema)])),
            want,
            "{why}"
        );
    }
}

// ── A join's sources ────────────────────────────────────────────────────

/// The two-term join of sources 7 and 9, each reindexed on `key_cols` and
/// optionally clamped to a set, behind an output shard:
///
///   ScanDelta(7) → Map(reindex) → [Distinct] ─┬→ IntegrateTrace ─╮
///   ScanDelta(9) → Map(reindex) → [Distinct] ─┴→ IntegrateTrace ─┴→ Join ×2 → Union
///                                                   → ExchangeShard([1]) → IntegrateSink
fn join_meta_in(kind: JoinKind, key_cols: &[u32], distinct: [bool; 2], ext: RelationRegistry) -> ViewMeta {
    let mut c = Circuit::default();
    let deltas = [(7, distinct[0]), (9, distinct[1])].map(|(source, set)| {
        let schema = ext.relation(source).expect("a registered source").schema();
        let keyed = scan_keyed(&mut c, source, &self_typed_slots(&schema, key_cols));
        match set {
            true => c.distinct(keyed),
            false => keyed,
        }
    });
    let traces = deltas.map(|d| c.integrate_trace(d));
    let joined = c.join_terms(deltas, traces, kind);
    let shard = c.shard(joined, &[1]);
    c.sink(shard);
    derive(c, &ext).expect("fixture routes")
}

/// A source's relay follows what its join needs co-located and what is already
/// in place: a keyed join's sources meet on the key's owner unless they sit there
/// or a replicated partner makes every match locally; a keyless join's matches
/// share no key, so a partitioned source is broadcast.
#[test]
fn a_joins_sources_relay_only_where_their_matches_are_not_already_local() {
    use Route::{Broadcast, Round, Stays};
    let base = make_schema_u64_i64();
    let replicated = base.with_placement(Placement::Replicated);
    // Compound PK (a, b) at columns 0, 1; column 2 is payload.
    let compound = pk_payload_schema(&[TypeCode::U64; 2]);
    let clustered = compound.with_placement(Placement::Keyed { prefix_len: 1 });
    let plain = [false; 2];
    /// `(why, join kind, key columns, set-clamped sides, schemas of 7 and 9, their routes)`.
    type Case = (
        &'static str,
        JoinKind,
        &'static [u32],
        [bool; 2],
        [SchemaDescriptor; 2],
        [Route; 2],
    );
    let cases: [Case; 14] = [
        (
            "the key is the PK sequence both sides are placed by",
            JoinKind::Equi,
            &[0, 1],
            plain,
            [compound; 2],
            [Stays, Stays],
        ),
        (
            "the permuted PK is not the distribution prefix",
            JoinKind::Equi,
            &[1, 0],
            plain,
            [compound; 2],
            [Round, Round],
        ),
        ("a payload key", JoinKind::Equi, &[1], plain, [base; 2], [Round, Round]),
        (
            "a replicated partner: the partitioned side joins its full local copy",
            JoinKind::Equi,
            &[1],
            plain,
            [replicated, base],
            [Stays, Stays],
        ),
        (
            "replicated ⋈ replicated",
            JoinKind::Equi,
            &[1],
            plain,
            [replicated; 2],
            [Stays, Stays],
        ),
        (
            "a partitioned side clamped to a set needs every row of a key on one worker",
            JoinKind::Equi,
            &[1],
            [false, true],
            [replicated, base],
            [Stays, Round],
        ),
        (
            "a replicated side clamps its whole copy on every worker",
            JoinKind::Equi,
            &[1],
            [true, false],
            [replicated, base],
            [Stays, Stays],
        ),
        (
            "a band join's equality prefix is the CLUSTER BY key",
            range(1),
            &[0, 1],
            plain,
            [clustered; 2],
            [Stays, Stays],
        ),
        (
            "the full-PK distribution is wider than a band join's equality prefix",
            range(1),
            &[0, 1],
            plain,
            [compound; 2],
            [Round, Round],
        ),
        (
            "a band join beside a replicated partner",
            range(1),
            &[0, 1],
            plain,
            [compound.with_placement(Placement::Replicated), compound],
            [Stays, Stays],
        ),
        (
            "an equi join on the key its sources are placed by",
            JoinKind::Equi,
            &[0],
            plain,
            [base; 2],
            [Stays, Stays],
        ),
        (
            "a pure-range join's matches spread over the whole other side",
            range(0),
            &[0],
            plain,
            [base; 2],
            [Broadcast, Broadcast],
        ),
        (
            "a cross join's too",
            JoinKind::Cross,
            &[0],
            plain,
            [base; 2],
            [Broadcast, Broadcast],
        ),
        (
            "a replicated source already holds what a broadcast would hand it",
            range(0),
            &[1],
            plain,
            [replicated, base],
            [Stays, Broadcast],
        ),
    ];
    for (why, kind, key_cols, distinct, [a, b], want) in cases {
        let meta = join_meta_in(kind, key_cols, distinct, sources([(7, a), (9, b)]));
        assert_eq!([7, 9].map(|tid| route(meta.source_route(tid))), want, "{why}");
    }
}

/// A band join (`n_eq >= 1`) scatters by the equality prefix, dropping the
/// trailing range slot: equal eq-values then co-partition both sides and the
/// range probe stays partition-local. An equi-join routes by the whole key.
#[test]
fn a_band_join_routes_by_the_equality_prefix_and_an_equi_join_by_the_whole_key() {
    let wide = || sources([(7, wide_schema()), (9, wide_schema())]);
    let band = join_meta_in(range(1), &[3, 4], [false; 2], wide());
    assert_routes_by(band.source_route(7), &wide_schema(), &[3]);
    let equi = join_meta_in(JoinKind::Equi, &[3, 4], [false; 2], wide());
    assert_routes_by(equi.source_route(7), &wide_schema(), &[3, 4]);
}

/// The replicated source 7 and the partitioned source 9 meet in an equi join, and
/// 7's reindexed delta also feeds a `Union` beside the join — the shape of an
/// outer or semi join's preserved side. Read there it counts once per worker, so
/// neither source may skip.
#[test]
fn a_replicated_delta_feeding_more_than_its_join_scatters_every_source() {
    let base = make_schema_u64_i64();
    let key = self_typed_slots(&base, &[1]);
    let mut c = Circuit::default();
    let (a, b) = (scan_keyed(&mut c, 7, &key), scan_keyed(&mut c, 9, &key));
    let tb = c.integrate_trace(b);
    let joined = c.join(a, tb, JoinKind::Equi, false);
    let both = c.union(a, joined);
    c.sink(both);
    let ext = sources([(7, base.with_placement(Placement::Replicated)), (9, base)]);
    let meta = derive(c, &ext).unwrap();
    assert_eq!(
        [7, 9].map(|tid| route(meta.source_route(tid))),
        [Route::Share, Route::Round]
    );
}

/// A source whose re-key only an owner filter reads routes by the whole key it
/// states, whatever relay the circuit's joins call for, and never takes the
/// replicated-partner skip: its filter drops every row the relay did not place.
///
///   ScanDelta(7) → Map(reindex key_cols) → WorkerFilter → IntegrateTrace ─┐
///   ScanDelta(9) → Map(reindex [1]) ────────────────────────────────────→ Join(kind)
#[test]
fn an_owner_trimmed_source_routes_by_the_key_it_states() {
    let base = make_schema_u64_i64();
    let replicated = base.with_placement(Placement::Replicated);
    let trimmed = |kind: JoinKind, key_cols: &[u32], [a, b]: [SchemaDescriptor; 2]| {
        let mut c = Circuit::default();
        let keyed = scan_keyed(&mut c, 7, &self_typed_slots(&a, key_cols));
        let owned = c.worker_filter(keyed);
        let trace = c.integrate_trace(owned);
        let delta = scan_keyed(&mut c, 9, &self_typed_slots(&b, &[1]));
        let joined = c.join(delta, trace, kind, false);
        c.sink(joined);
        derive(c, &sources([(7, a), (9, b)])).unwrap()
    };
    let routes = |meta: &ViewMeta| [7, 9].map(|tid| route(meta.source_route(tid)));

    let meta = trimmed(JoinKind::Equi, &[1], [base, replicated]);
    assert_eq!(routes(&meta), [Route::Round, Route::Stays]);
    assert_routes_by(meta.source_route(7), &base, &[1]);

    let meta = trimmed(range(0), &[0], [base, base]);
    assert_eq!(
        routes(&meta),
        [Route::Stays, Route::Broadcast],
        "already on its PK's owner"
    );

    let meta = trimmed(range(0), &[0], [replicated, base]);
    assert_eq!(
        routes(&meta),
        [Route::Share, Route::Broadcast],
        "a replicated copy is kept by key where it stands"
    );
    assert_routes_by(meta.source_route(7), &replicated, &[0]);
}

/// A source states one route however many reindexes name it, and none when it
/// feeds no join: source 20 reaches the sink through a `Union` beside the join's
/// output, so its rows are already where the view needs them.
#[test]
fn a_source_routes_by_the_one_key_it_states() {
    let mut c = Circuit::default();
    let source = scan(&mut c, 10);
    let delta = states_route(&mut c, source, 10, &[2]);
    let again = states_route(&mut c, source, 10, &[2]);
    let trace = c.integrate_trace(again);
    let joined = c.join(delta, trace, JoinKind::Equi, false);
    let other = scan(&mut c, 20);
    let both = c.union(joined, other);
    c.sink(both);
    let meta = derive(c, &sources([(10, wide_schema()), (20, wide_schema())])).unwrap();
    assert_routes_by(meta.source_route(10), &wide_schema(), &[2]);
    assert_eq!(route(meta.source_route(20)), Route::Stays);
}

/// A `Map` between the scan and the reindex moves the key, so the route is read
/// as the source's own columns: reading the node's own key would scatter source
/// 7 by column 2 of a relation whose join column is 5.
#[test]
fn the_route_is_the_stated_one_not_the_reindex_nodes_own_key() {
    let mut c = Circuit::default();
    let source = scan(&mut c, 7);
    let moved = c.map(source, &[5]);
    let role = ReindexRole::ScatterKey {
        source: 7,
        source_key: self_typed_slots(&wide_schema(), &[5]),
    };
    let delta = c.map_reindex(moved, &[(2, TypeCode::I64)], &[0], role, NullKeys::Keep);
    let joined = join_with_own_trace(&mut c, delta, JoinKind::Equi);
    c.sink(joined);
    let meta = derive(c, &sources([(7, wide_schema())])).unwrap();
    assert_routes_by(meta.source_route(7), &wide_schema(), &[5]);
}

/// A circuit is client-supplied, and workers route by its metadata mid-round,
/// where a refusal is fatal — so each route `derive` cannot stand behind is
/// refused there, on the master and on every worker alike.
#[test]
fn a_circuit_derive_cannot_route_is_rejected() {
    /// The band join of sources 7 and 9 on a three-slot key.
    fn band(c: &mut Circuit, n_eq: u8) {
        let key = self_typed_slots(&wide_schema(), &[1, 2, 3]);
        let (a, b) = (scan_keyed(c, 7, &key), scan_keyed(c, 9, &key));
        let tb = c.integrate_trace(b);
        let joined = c.join(a, tb, range(n_eq), false);
        c.sink(joined);
    }
    let cases: [(Build, &str); 8] = [
        // Integrated wherever it was produced, beside a trace scattered by the key.
        (
            |c: &mut Circuit| {
                let source = scan(c, 7);
                let delta = aux_reindex(c, source, &self_typed_slots(&wide_schema(), &[1]));
                let joined = join_with_own_trace(c, delta, JoinKind::Equi);
                c.sink(joined);
            },
            "source 7 feeds a join and states no scatter key",
        ),
        // Scattering by either key alone leaves the trace side keyed by the other.
        (
            |c: &mut Circuit| {
                let source = scan(c, 7);
                let (a, b) = (states_route(c, source, 7, &[1]), states_route(c, source, 7, &[2]));
                let tb = c.integrate_trace(b);
                let joined = c.join(a, tb, JoinKind::Equi, false);
                c.sink(joined);
            },
            "source 7 feeds several distinct scatter keys",
        ),
        // A column the relation has not got.
        (
            |c: &mut Circuit| {
                let delta = scan_keyed(c, 7, &[(9, TypeCode::I64)]);
                let joined = join_with_own_trace(c, delta, JoinKind::Equi);
                c.sink(joined);
            },
            "source 7 scatter key",
        ),
        // More equality slots than the reindex key holds, and fewer: the key is
        // `[eq…, range]`, and a route sliced out of anything else is not it.
        (
            |c: &mut Circuit| band(c, 5),
            "band join: n_eq does not match the source's reindex key arity",
        ),
        (
            |c: &mut Circuit| band(c, 1),
            "band join: n_eq does not match the source's reindex key arity",
        ),
        // A view's sources are routed by one relay.
        (
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 7), scan(c, 9));
                let (ta, tb) = (c.integrate_trace(a), c.integrate_trace(b));
                let ab = c.join(a, tb, JoinKind::Equi, false);
                let ba = c.join(b, ta, JoinKind::Cross, true);
                let both = c.union(ab, ba);
                c.sink(both);
            },
            "circuit joins need different relays",
        ),
        (
            |c: &mut Circuit| {
                let source = scan(c, 7);
                let delta = states_route(c, source, 9, &[1]);
                c.sink(delta);
            },
            "source 9 states a scatter key but is not scanned",
        ),
        (
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 7), scan(c, 8));
                let both = c.union(a, b);
                c.sink(both);
            },
            "relation 8 is not registered",
        ),
    ];
    for (build, guard) in cases {
        let mut c = Circuit::default();
        build(&mut c);
        let refused = rejection(derive(c, &sources([(7, wide_schema()), (9, wide_schema())])));
        assert!(refused.starts_with(guard), "{guard}: got {refused}");
    }
}

// ── Placement ───────────────────────────────────────────────────────────

/// A view's rows sit where its circuit leaves them: on every worker when every
/// source is replicated, on its own key's owner behind an exchange or a join, on
/// a lone source's owner while the walk back from the sink keeps that source's PK
/// region — and otherwise wherever they were produced.
#[test]
fn a_view_is_placed_by_where_its_circuit_leaves_its_rows() {
    let clustered = clustered_schema();
    let inherited = clustered.placement();
    let replicated = clustered.with_placement(Placement::Replicated);
    let cases: [(&str, Build, [SchemaDescriptor; 2], usize, Placement); 11] = [
        (
            "a bare scan re-emits its source's PK region",
            |c: &mut Circuit| {
                let a = scan(c, 7);
                c.sink(a);
            },
            [clustered; 2],
            3,
            inherited,
        ),
        (
            "at another PK arity the prefix names other columns",
            |c: &mut Circuit| {
                let a = scan(c, 7);
                c.sink(a);
            },
            [clustered; 2],
            2,
            Placement::Local,
        ),
        (
            "a filter and a projection keep the PK region",
            |c: &mut Circuit| {
                let a = scan(c, 7);
                let f = c.filter(a, dummy_expr_blob());
                let m = c.map(f, &[0]);
                c.sink(m);
            },
            [clustered; 2],
            3,
            inherited,
        ),
        (
            "a bare reindex exchanges nothing and still replaces the PK region",
            |c: &mut Circuit| {
                let a = scan(c, 7);
                let r = aux_reindex(c, a, &[(3, TypeCode::I64)]);
                c.sink(r);
            },
            [clustered; 2],
            3,
            Placement::Local,
        ),
        (
            "a WorkerFilter is not a Filter",
            |c: &mut Circuit| {
                let a = scan(c, 7);
                let w = c.worker_filter(a);
                c.sink(w);
            },
            [clustered; 2],
            3,
            Placement::Local,
        ),
        (
            "a shard moves the rows to the view's own key",
            |c: &mut Circuit| {
                let a = scan(c, 7);
                let s = c.shard(a, &[]);
                c.sink(s);
            },
            [clustered; 2],
            3,
            Placement::KEYED_DEFAULT,
        ),
        (
            "a join carries no shard and still re-keys",
            |c: &mut Circuit| {
                let d = scan_keyed(c, 7, &[(3, TypeCode::I64)]);
                let joined = join_with_own_trace(c, d, JoinKind::Equi);
                c.sink(joined);
            },
            [clustered; 2],
            3,
            Placement::KEYED_DEFAULT,
        ),
        (
            "two keyed sources",
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 7), scan(c, 9));
                let u = c.union(a, b);
                c.sink(u);
            },
            [clustered; 2],
            3,
            Placement::KEYED_DEFAULT,
        ),
        (
            "every source replicated: each worker computes the whole result",
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 7), scan(c, 9));
                let u = c.union(a, b);
                let s = c.shard(u, &[0]);
                c.sink(s);
            },
            [replicated; 2],
            3,
            Placement::Replicated,
        ),
        (
            "a replicated source beside a keyed one",
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 7), scan(c, 9));
                let u = c.union(a, b);
                c.sink(u);
            },
            [replicated, clustered],
            3,
            Placement::Local,
        ),
        (
            "a source whose rows no key places",
            |c: &mut Circuit| {
                let a = scan(c, 7);
                c.sink(a);
            },
            [clustered.with_placement(Placement::Local); 2],
            3,
            Placement::Local,
        ),
    ];
    for (why, build, [a, b], pk_arity, want) in cases {
        let mut c = Circuit::default();
        build(&mut c);
        let (_, placement) = ViewMeta::derive(&loaded(c), &sources([(7, a), (9, b)]), pk_arity).unwrap();
        assert_eq!(placement, want, "{why}");
    }
}

// ── Backfill bounds ─────────────────────────────────────────────────────

/// A backfill bound survives per source scanned once; a source scanned twice
/// shares one backfill cursor, so it keeps none even where one scan is bounded.
#[test]
fn a_source_scanned_once_keeps_its_backfill_bound() {
    let bound = |col: u32| ReadBound::Range(KeyRange::point(PkColList::from_slice(&[col]), &[], 0));
    let bounds_of = |scans: [(u64, ReadBound); 2]| {
        let mut c = Circuit::default();
        let [a, b] = scans.clone().map(|(source, bound)| c.input_delta(source, bound));
        let both = c.union(a, b);
        c.sink(both);
        let meta = derive(c, &sources([(10, wide_schema()), (11, wide_schema())])).unwrap();
        scans.map(|(source, _)| meta.source_bound(source))
    };
    assert_eq!(
        bounds_of([(10, bound(2)), (11, ReadBound::None)]),
        [bound(2), ReadBound::None],
        "each source scanned once keeps its own"
    );
    assert_eq!(
        bounds_of([(10, bound(2)), (10, ReadBound::None)]),
        [ReadBound::None, ReadBound::None],
        "a source scanned twice has none"
    );
}
