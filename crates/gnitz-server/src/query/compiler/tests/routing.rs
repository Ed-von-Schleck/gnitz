use super::*;
use crate::query::compiler::fixtures::*;
use crate::test_support::{
    make_schema_u64_i64, map_copy, mix, pk_payload_schema, scan_keyed, scan_routed, self_typed_slots, u64_pk_schema,
};
use gnitz_wire::{Circuit, JoinKind, NullKeys, RangeRel, ReindexRole, TypeCode};
use gnitz_zset::algebra::{Placement, Slot};
use gnitz_zset::repr::BatchBuilder;
use gnitz_zset::schema::SchemaColumn;

/// A registry for one worker of four: a view computed whole by one worker keeps
/// no route.
fn of_four<S: Into<Source>>(rows: impl IntoIterator<Item = (u64, S)>) -> RelationRegistry {
    sources_at(Slot::new(0, 4), rows)
}

/// A U64 PK and five I64 payload columns: every key a fixture names is payload.
fn wide_schema() -> SchemaDescriptor {
    u64_pk_schema(&[SchemaColumn::new(TypeCode::I64, false); 5])
}

/// A source's route without its key, `Stays` for none: the routing decision a
/// test compares.
#[derive(Debug, PartialEq)]
enum Route {
    Stays,
    Broadcast,
    Keyed,
}

fn route(plan: Option<&Rc<ScatterPlan>>) -> Route {
    match plan {
        None => Route::Stays,
        Some(plan) if plan.is_broadcast() => Route::Broadcast,
        Some(_) => Route::Keyed,
    }
}

/// The routes of sources 7 and 9.
fn routes(meta: &ViewMeta) -> [Route; 2] {
    [7, 9].map(|tid| route(meta.source_route(tid)))
}

/// A range join: a pure range over a one-slot key, a band over a wider one.
const RANGE: JoinKind = JoinKind::Range { rel: RangeRel::Lt };

/// `d ⋈ I(d)`: the smallest circuit in which `d`'s source reaches a join.
fn join_with_own_trace(c: &mut Circuit, d: NodeId, kind: JoinKind) -> NodeId {
    c.join([d, d], [d, d], kind)
}

/// A reindex of `input` on `cols` of [`wide_schema`], stated as the route of the
/// source `input` reads.
fn states_route(c: &mut Circuit, input: NodeId, cols: &[u32]) -> NodeId {
    let key = self_typed_slots(&wide_schema(), cols);
    let role = ReindexRole::ScatterKey { source_cols: cols.to_vec() };
    c.map_reindex(input, &key, &[0], role, NullKeys::Keep)
}

/// How many leading slots of a `key_len`-slot key a join of `kind` co-locates
/// its sides by: an equi join's whole key, a range join's equality prefix, none
/// of a cross join's.
fn routed(kind: JoinKind, key_len: usize) -> usize {
    match kind {
        JoinKind::Equi => key_len,
        JoinKind::Range { .. } => key_len - 1,
        JoinKind::Cross => 0,
    }
}

/// [`scan_routed`] by the slots a join of `kind` routes `key` by.
fn scan_joined(c: &mut Circuit, source: u64, key: &[gnitz_wire::ReindexSlot], kind: JoinKind) -> NodeId {
    scan_routed(c, source, key, routed(kind, key.len()))
}

/// A reindex on `key` that states no route.
fn aux_reindex(c: &mut Circuit, input: NodeId, key: &[gnitz_wire::ReindexSlot]) -> NodeId {
    c.map_reindex(input, key, &[0], ReindexRole::Auxiliary, NullKeys::Keep)
}

/// `relay` is keyed and splits rows by the join key over `cols` of `schema`: it
/// sends every row of a probe whose columns all differ where that key does.
fn assert_routes_by(relay: Option<&Rc<ScatterPlan>>, schema: &SchemaDescriptor, cols: &[u32]) {
    let got = relay.expect("a keyed relay");
    let mut bb = BatchBuilder::new(schema);
    for i in 0..64u64 {
        bb.begin_row(i as u128, 1);
        for c in 0..schema.num_payload_cols() as u64 {
            bb.put_u64(mix(i << 8 | c));
        }
        bb.end_row();
    }
    let probe = bb.finish();
    let want = ScatterPlan::join(schema, &self_typed_slots(schema, cols)).unwrap();
    let (mut got_lists, mut want_lists) = (Vec::new(), Vec::new());
    assert_eq!(
        got.route(&probe.as_mem_batch(), &mut got_lists, 4),
        want.route(&probe.as_mem_batch(), &mut want_lists, 4)
    );
}

// ── A join's sources ────────────────────────────────────────────────────

/// The two-term join of sources 7 and 9, each reindexed on `key_cols` and
/// optionally clamped to a set, behind an output shard.
fn join_meta_in(kind: JoinKind, key_cols: &[u32], distinct: [bool; 2], ext: RelationRegistry) -> ViewMeta {
    let mut c = Circuit::default();
    let deltas = [(7, distinct[0]), (9, distinct[1])].map(|(source, set)| {
        let schema = ext.relation(source).expect("a registered source").schema();
        let keyed = scan_joined(&mut c, source, &self_typed_slots(&schema, key_cols), kind);
        match set {
            true => c.distinct(keyed),
            false => keyed,
        }
    });
    let joined = c.join(deltas, deltas, kind);
    c.shard(joined);
    routing(&c, &ext).expect("fixture routes")
}

/// A source's relay follows what its join needs co-located and what is already
/// in place: a keyed join's sources meet on the key's owner unless they sit there
/// or a replicated partner makes every match locally; a keyless join's matches
/// share no key, so a partitioned source is broadcast.
#[test]
fn a_joins_sources_relay_only_where_their_matches_are_not_already_local() {
    use Route::{Broadcast, Keyed, Stays};
    let base = Source::from(make_schema_u64_i64());
    let replicated = base.placed(Placement::Replicated);
    // Compound PK (a, b) at columns 0, 1; column 2 is payload.
    let compound = Source::from(pk_payload_schema(&[TypeCode::U64; 2]));
    let clustered = compound.placed(Placement::keyed(&compound.schema, 1));
    let plain = [false; 2];
    /// `(why, join kind, key columns, set-clamped sides, sources 7 and 9, their routes)`.
    type Case = (
        &'static str,
        JoinKind,
        &'static [u32],
        [bool; 2],
        [Source; 2],
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
            [Keyed, Keyed],
        ),
        ("a payload key", JoinKind::Equi, &[1], plain, [base; 2], [Keyed, Keyed]),
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
            [Stays, Keyed],
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
            RANGE,
            &[0, 1],
            plain,
            [clustered; 2],
            [Stays, Stays],
        ),
        (
            "the full-PK distribution is wider than a band join's equality prefix",
            RANGE,
            &[0, 1],
            plain,
            [compound; 2],
            [Keyed, Keyed],
        ),
        (
            "a band join beside a replicated partner",
            RANGE,
            &[0, 1],
            plain,
            [compound.placed(Placement::Replicated), compound],
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
            RANGE,
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
            RANGE,
            &[1],
            plain,
            [replicated, base],
            [Stays, Broadcast],
        ),
    ];
    for (why, kind, key_cols, distinct, [a, b], want) in cases {
        let meta = join_meta_in(kind, key_cols, distinct, of_four([(7, a), (9, b)]));
        assert_eq!(routes(&meta), want, "{why}");
    }
}

/// A view one worker computes whole relays nothing. A key a relay would route
/// by is validated all the same: a restart may launch more workers.
#[test]
fn a_self_contained_view_keeps_no_route_and_still_validates_its_keys() {
    let joined = |key: &[gnitz_wire::ReindexSlot]| {
        let mut c = Circuit::default();
        let deltas = [7, 9].map(|source| scan_keyed(&mut c, source, key));
        c.join(deltas, deltas, JoinKind::Equi);
        c
    };
    let base = Source::from(make_schema_u64_i64());
    let replicated = base.placed(Placement::Replicated);
    let on_payload = self_typed_slots(&base.schema, &[1]);

    let solo = sources_at(Slot::SOLO, [(7, base), (9, base)]);
    for (why, registry) in [
        ("one worker", &solo),
        ("every source replicated", &of_four([(7, replicated), (9, replicated)])),
    ] {
        let meta = routing(&joined(&on_payload), registry).unwrap();
        assert!(meta.self_contained, "{why}");
        assert_eq!(routes(&meta), [Route::Stays, Route::Stays], "{why}");
    }
    let refused = rejection(routing(&joined(&[(9, TypeCode::I64)]), &solo));
    assert!(refused.starts_with("source 7 scatter key"), "{refused}");
}

/// A source is routed by the slots it states: a range join's sides state its
/// key's equality prefix, and an equi join's the whole key.
#[test]
fn a_source_routes_by_the_prefix_of_its_key_it_states() {
    for (kind, key, routed) in [
        (RANGE, &[3, 4][..], &[3][..]),
        (RANGE, &[2, 3, 4], &[2, 3]),
        (JoinKind::Equi, &[3, 4], &[3, 4]),
    ] {
        let meta = join_meta_in(kind, key, [false; 2], of_four([(7, wide_schema()), (9, wide_schema())]));
        assert_routes_by(meta.source_route(7), &wide_schema(), routed);
    }
}

/// A source whose re-key only an owner filter reads never takes the
/// replicated-partner skip: its filter drops every row the relay did not place.
#[test]
fn an_owner_trimmed_source_routes_by_the_key_it_states() {
    let base = Source::from(make_schema_u64_i64());
    let replicated = base.placed(Placement::Replicated);
    let trimmed = |kind: JoinKind, key_cols: &[u32], [a, b]: [Source; 2]| {
        let mut c = Circuit::default();
        let keyed = scan_keyed(&mut c, 7, &self_typed_slots(&a.schema, key_cols));
        let owned = c.worker_filter(keyed);
        let delta = scan_joined(&mut c, 9, &self_typed_slots(&b.schema, &[1]), kind);
        c.join([delta, owned], [delta, owned], kind);
        routing(&c, &of_four([(7, a), (9, b)])).unwrap()
    };

    let meta = trimmed(JoinKind::Equi, &[1], [base, replicated]);
    assert_eq!(routes(&meta), [Route::Keyed, Route::Stays]);
    assert_routes_by(meta.source_route(7), &base.schema, &[1]);

    let meta = trimmed(RANGE, &[0], [base, base]);
    assert_eq!(
        routes(&meta),
        [Route::Stays, Route::Broadcast],
        "already on its PK's owner"
    );

    let meta = trimmed(RANGE, &[0], [replicated, base]);
    assert_eq!(
        routes(&meta),
        [Route::Keyed, Route::Broadcast],
        "a replicated copy is kept by key where it stands"
    );
    assert_routes_by(meta.source_route(7), &replicated.schema, &[0]);
}

/// A source states one route however many reindexes name it, and none when it
/// feeds no join: source 20 reaches the output through a `Union` beside the join's
/// output, so its rows are already where the view needs them.
#[test]
fn a_source_routes_by_the_one_key_it_states() {
    let mut c = Circuit::default();
    let source = scan(&mut c, 10);
    let delta = states_route(&mut c, source, &[2]);
    let again = states_route(&mut c, source, &[2]);
    let joined = c.join([delta, again], [delta, again], JoinKind::Equi);
    let other = scan(&mut c, 20);
    c.union(joined, other);
    let meta = routing(&c, &of_four([(10, wide_schema()), (20, wide_schema())])).unwrap();
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
    let moved = map_copy(&mut c, source, &[(5, TypeCode::I64)]);
    let role = ReindexRole::ScatterKey { source_cols: vec![5] };
    let delta = c.map_reindex(moved, &[(2, TypeCode::I64)], &[0], role, NullKeys::Keep);
    join_with_own_trace(&mut c, delta, JoinKind::Equi);
    let meta = routing(&c, &of_four([(7, wide_schema())])).unwrap();
    assert_routes_by(meta.source_route(7), &wide_schema(), &[5]);
}

/// A circuit is client-supplied, and workers route by its metadata mid-round,
/// where a refusal is fatal — so each route the derivation cannot stand behind is
/// refused there, on the master and on every worker alike.
#[test]
fn a_circuit_routing_cannot_stand_behind_is_rejected() {
    let cases: [(Build, &str); 5] = [
        // Integrated wherever it was produced, beside a trace scattered by the key.
        (
            |c: &mut Circuit| {
                let source = scan(c, 7);
                let delta = aux_reindex(c, source, &self_typed_slots(&wide_schema(), &[1]));
                join_with_own_trace(c, delta, JoinKind::Equi);
            },
            "source 7 feeds a join and states no scatter key",
        ),
        // Scattering by either key alone leaves the trace side keyed by the other.
        (
            |c: &mut Circuit| {
                let source = scan(c, 7);
                let (a, b) = (states_route(c, source, &[1]), states_route(c, source, &[2]));
                c.join([a, b], [a, b], JoinKind::Equi);
            },
            "source 7 feeds several distinct scatter keys",
        ),
        // A column the relation has not got.
        (
            |c: &mut Circuit| {
                let delta = scan_keyed(c, 7, &[(9, TypeCode::I64)]);
                join_with_own_trace(c, delta, JoinKind::Equi);
            },
            "source 7 scatter key",
        ),
        (
            |c: &mut Circuit| {
                // A union's rows are no one source's.
                let (a, b) = (scan(c, 7), scan(c, 9));
                let both = c.union(a, b);
                states_route(c, both, &[1]);
            },
            "a scatter key over no scanned source",
        ),
        (
            |c: &mut Circuit| {
                let (a, b) = (scan(c, 7), scan(c, 8));
                c.union(a, b);
            },
            "relation 8 is not registered",
        ),
    ];
    for (build, guard) in cases {
        let mut c = Circuit::default();
        build(&mut c);
        let refused = rejection(routing(&c, &of_four([(7, wide_schema()), (9, wide_schema())])));
        assert!(refused.starts_with(guard), "{guard}: got {refused}");
    }
}

// ── Placement ───────────────────────────────────────────────────────────

/// A view's rows sit where its circuit leaves them: on every worker when every
/// source is replicated, on its own key's owner behind an exchange or a join, on
/// a source's owner while the walk back from the output keeps that source's PK
/// region and nothing relays its delta — and otherwise wherever they were produced.
#[test]
fn a_view_is_placed_by_where_its_circuit_leaves_its_rows() {
    use TypeCode::{I64, U32, U64};
    // A compound PK `(U32, U64, U64)` and an I64 payload, placed by its leading
    // PK column.
    let schema = pk_payload_schema(&[U32, U64, U64]);
    let clustered = Source::from(schema).placed(Placement::keyed(&schema, 1));
    let inherited = clustered.placement;
    let replicated = clustered.placed(Placement::Replicated);
    let cases: [(&str, Build, [Source; 2], Placement); 11] = [
        (
            "a bare scan re-emits its source's PK region",
            |c| _ = scan(c, 7),
            [clustered; 2],
            inherited,
        ),
        (
            "a filter and a projection keep the PK region",
            |c| {
                let a = scan(c, 7);
                let f = c.filter(a, predicate(3));
                map_copy(c, f, &[(3, I64)]);
            },
            [clustered; 2],
            inherited,
        ),
        (
            "a bare reindex exchanges nothing and still replaces the PK region",
            |c| {
                let a = scan(c, 7);
                aux_reindex(c, a, &[(3, I64)]);
            },
            [clustered; 2],
            Placement::Local,
        ),
        (
            "a WorkerFilter is not a Filter",
            |c| {
                let a = scan(c, 7);
                c.worker_filter(a);
            },
            [clustered; 2],
            Placement::Local,
        ),
        (
            "a shard moves the rows to the view's own key",
            |c| {
                let a = scan(c, 7);
                c.shard(a);
            },
            [clustered; 2],
            Placement::full_pk(&schema),
        ),
        (
            "a join carries no shard and still re-keys",
            |c| {
                let deltas = [7, 9].map(|s| scan_keyed(c, s, &[(3, I64)]));
                c.join(deltas, deltas, JoinKind::Equi);
            },
            [clustered; 2],
            Placement::Keyed { dist_stride: 8 },
        ),
        (
            "two keyed sources exchange nothing, so each row stays on its own source's owner",
            |c| {
                let (a, b) = (scan(c, 7), scan(c, 9));
                c.union(a, b);
            },
            [clustered; 2],
            Placement::Local,
        ),
        (
            "a source's rows walked back to its scan, while a join relays its delta",
            |c| {
                let a = scan(c, 7);
                let role = ReindexRole::ScatterKey { source_cols: vec![3] };
                let da = c.map_reindex(a, &[(3, I64)], &[0], role, NullKeys::Keep);
                let db = scan_keyed(c, 9, &[(3, I64)]);
                c.join([da, db], [da, db], JoinKind::Equi);
                c.filter(a, predicate(3));
            },
            [clustered; 2],
            Placement::Local,
        ),
        (
            "every source replicated: each worker computes the whole result",
            |c| {
                let (a, b) = (scan(c, 7), scan(c, 9));
                let u = c.union(a, b);
                c.shard(u);
            },
            [replicated; 2],
            Placement::Replicated,
        ),
        (
            "a replicated source beside a keyed one",
            |c| {
                let (a, b) = (scan(c, 7), scan(c, 9));
                c.union(a, b);
            },
            [replicated, clustered],
            Placement::Local,
        ),
        (
            "a source whose rows no key places",
            |c| _ = scan(c, 7),
            [clustered.placed(Placement::Local); 2],
            Placement::Local,
        ),
    ];
    for (why, build, [a, b], want) in cases {
        let mut c = Circuit::default();
        build(&mut c);
        let registry = of_four([(7, a), (9, b)]);
        // The view a compile admits: of the schema the circuit outputs.
        let view = *compile(&c, &registry, false).expect(why).vm.out_schema();
        let (_, placement) = ViewMeta::derive(&c, &registry, &view).unwrap();
        assert_eq!(placement, want, "{why}");
    }
}
