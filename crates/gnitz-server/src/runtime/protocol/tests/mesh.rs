use super::*;
use crate::test_support::{make_batch, make_schema_u64_i64};

const NW: usize = 3;

fn meshes() -> Vec<Mesh> {
    super::fixtures::meshes(NW)
}

/// `(pk, weight, payload)` of every row, in order.
fn rows(b: &Batch) -> Vec<(u128, i64, i64)> {
    (0..b.len())
        .map(|i| {
            let payload = i64::from_le_bytes(b.col_data(0)[i * 8..i * 8 + 8].try_into().unwrap());
            (b.get_pk(i), b.get_weight(i), payload)
        })
        .collect()
}

/// Worker `w`'s partition in round `round`: distinct per worker and per round, so
/// a slice read from the wrong outbox shows.
fn partition(w: usize, round: u64) -> Batch {
    let schema = make_schema_u64_i64();
    let rows: Vec<(u64, i64, i64)> = (0..8u64)
        .map(|i| {
            (
                round * 1000 + w as u64 * 100 + i,
                1 + i as i64 % 2,
                (round * 10 + w as u64) as i64,
            )
        })
        .collect();
    make_batch(&schema, &rows)
}

/// What receiver `r` must gather in round `round` under `spec`: its share of
/// every sender's partition summed, or with no spec all of it.
fn expected(spec: Option<ScatterSpec<'_>>, round: u64, r: usize) -> Vec<(u128, i64, i64)> {
    let schema = make_schema_u64_i64();
    let parts: Vec<Batch> = (0..NW).map(|w| partition(w, round)).collect();
    let summed = Batch::concat(&schema, parts.iter()).into_consolidated(&schema);
    match spec {
        None => rows(&summed),
        Some(spec) => {
            let mut pool = Vec::new();
            let routed = op_exchange_route(&summed, spec, &mut pool, NW).unwrap();
            rows(&summed.ascending_subset(&routed[r]))
        }
    }
}

/// Publish every worker's round-`round` partition in rank order: the round
/// completes on the last publish and not before.
fn publish_all(mesh: &mut [Mesh], round: u64, spec: Option<ScatterSpec<'_>>, drained: [bool; NW]) {
    for w in 0..NW {
        assert!(
            mesh.iter().all(|m| !m.complete()),
            "round {round}: incomplete before worker {w}"
        );
        mesh[w].publish(9, 4, drained[w], &partition(w, round), spec);
    }
    assert!(
        mesh.iter().all(Mesh::complete),
        "round {round}: complete on the last publish"
    );
}

/// Worker `r` gathers round `round`: what every sender routed to it, and the
/// AND of the drained bits.
fn gather_one(mesh: &mut Mesh, round: u64, spec: Option<ScatterSpec<'_>>, drained: [bool; NW]) {
    let r = mesh.rank;
    let (got, all_drained) = mesh.gather();
    assert_eq!(rows(&got), expected(spec, round, r), "round {round}: worker {r}");
    assert_eq!(got.layout(), Layout::Consolidated, "round {round}: worker {r}");
    assert_eq!(all_drained, drained.iter().all(|&d| d), "round {round}: worker {r}");
}

/// Four rounds, alternating a scattered and a broadcast one, each gathered
/// intact by every worker. Round 1 is published by two workers before the third
/// has gathered round 0.
#[test]
fn every_worker_gathers_each_round_from_every_peer() {
    let mut mesh = meshes();
    let scatter = Some(ScatterSpec::GroupKey(&[0]));
    let routes = [scatter, None, scatter, None];
    let drained = [[true, true, false], [true; NW], [false; NW], [true; NW]];
    let mut outboxes = [std::ptr::null_mut::<u8>(); 4];

    publish_all(&mut mesh, 0, routes[0], drained[0]);
    outboxes[0] = mesh[0].outbox(0);
    for w in 0..NW - 1 {
        gather_one(&mut mesh[w], 0, routes[0], drained[0]);
        mesh[w].publish(9, 4, drained[1][w], &partition(w, 1), routes[1]);
    }
    assert!(
        !mesh[0].complete(),
        "round 1 is incomplete until the last worker publishes it"
    );
    gather_one(&mut mesh[NW - 1], 0, routes[0], drained[0]);
    mesh[NW - 1].publish(9, 4, drained[1][NW - 1], &partition(NW - 1, 1), routes[1]);
    outboxes[1] = mesh[0].outbox(0);
    for m in mesh.iter_mut() {
        gather_one(m, 1, routes[1], drained[1]);
    }

    for round in 2..4u64 {
        let k = round as usize;
        publish_all(&mut mesh, round, routes[k], drained[k]);
        outboxes[k] = mesh[0].outbox(0);
        for m in mesh.iter_mut() {
            gather_one(m, round, routes[k], drained[k]);
        }
    }

    assert_ne!(outboxes[0], outboxes[1], "consecutive rounds use the two outboxes");
    assert_eq!(outboxes[0], outboxes[2], "round parity picks the outbox");
    assert_eq!(outboxes[1], outboxes[3], "round parity picks the outbox");
}

/// Publishing again before gathering would overwrite a round peers may still be
/// reading.
#[test]
#[should_panic(expected = "a round is already open")]
fn a_second_publish_before_the_gather_is_refused() {
    let mut mesh = meshes();
    let batch = partition(0, 0);
    mesh[0].publish(9, 4, false, &batch, None);
    mesh[0].publish(9, 4, false, &batch, None);
}

/// A gather before every worker published would read outboxes not yet written.
#[test]
#[should_panic(expected = "gathered before every worker published")]
fn a_gather_before_the_round_completes_is_refused() {
    let mut mesh = meshes();
    mesh[0].publish(9, 4, false, &partition(0, 0), None);
    mesh[0].gather();
}

/// Strings past the inline width live in a heap, which a scattered block cannot
/// frame in place: each receiver still gathers every sender's rows, strings
/// intact, scattered and broadcast alike.
#[test]
fn heap_strings_cross_the_mesh_intact() {
    use gnitz_store::schema::SchemaColumn;
    use gnitz_store::storage::BatchBuilder;
    use gnitz_wire::TypeCode;

    let schema = SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    );
    let text = |pk: u64| format!("a string long enough to leave the inline prefix, row {pk}");
    let partition = |w: u64| {
        let mut b = BatchBuilder::new(schema);
        for i in 0..6 {
            let pk = w * 100 + i;
            b.begin_row(pk as u128, 1);
            b.put_string(&text(pk));
            b.end_row();
        }
        b.finish()
    };
    let mut mesh = meshes();
    for (round, spec) in [Some(ScatterSpec::GroupKey(&[0])), None].into_iter().enumerate() {
        for (w, m) in mesh.iter_mut().enumerate() {
            m.publish(9, 4, false, &partition(w as u64), spec);
        }
        let mut seen = Vec::new();
        for m in mesh.iter_mut() {
            let (got, _) = m.gather();
            let mb = got.as_mem_batch();
            for i in 0..got.len() {
                let pk = got.get_pk(i) as u64;
                let bytes = gnitz_expr::payload_bytes(&mb, i, 0);
                assert_eq!(bytes, text(pk).as_bytes(), "round {round}: pk {pk}");
                seen.push(pk);
            }
        }
        seen.sort();
        let per_worker = if spec.is_some() { 1 } else { NW };
        let want: Vec<u64> = (0..NW as u64)
            .flat_map(|w| (0..6).map(move |i| w * 100 + i))
            .flat_map(|pk| std::iter::repeat_n(pk, per_worker))
            .collect();
        assert_eq!(seen, want, "round {round}");
    }
}
