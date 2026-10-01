use super::*;
use crate::test_support::{
    make_batch, make_batch_bytes_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64, make_string_batch,
    opk_pk, weighted_rows,
};
use gnitz_wire::wal::WAL_HEADER_SIZE;
use gnitz_zset::schema::Slot;

const NW: usize = 3;

/// One page of blocks past the head.
const SMALL_OUTBOX: usize = BLOCKS_AT + PAGE_BYTES;

fn meshes(nw: usize) -> Vec<Mesh> {
    super::fixtures::meshes(nw, SMALL_OUTBOX)
}

/// Routing by the leading PK column of `schema`.
fn by_pk(schema: &SchemaDescriptor) -> ScatterPlan {
    ScatterPlan::group(schema, &[0]).unwrap()
}

/// The receiver of 2 that [`by_pk`] routes a U64 `pk` to.
fn owner(pk: u64) -> usize {
    let schema = make_schema_u64_i64();
    schema.worker_for_pk(&opk_pk(&schema, &[pk as u128]), 2)
}

/// The first `n` pks from `from` up that [`owner`] routes to receiver `r`.
fn owned_by(r: usize, from: u64, n: usize) -> Vec<u64> {
    (from..).filter(|&pk| owner(pk) == r).take(n).collect()
}

/// Worker `w`'s partition in round `round`: distinct per worker and per round, so
/// a slice read from the wrong outbox shows.
fn partition(w: usize, round: u64) -> Batch {
    let rows: Vec<(u64, i64, i64)> = (0..8u64)
        .map(|i| {
            (
                round * 1000 + w as u64 * 100 + i,
                1 + i as i64 % 2,
                (round * 10 + w as u64) as i64,
            )
        })
        .collect();
    make_batch(&make_schema_u64_i64(), &rows)
}

/// What receiver `r` must gather from `parts` under `spec`: its share of every
/// sender's partition summed.
fn expected_of(parts: &[Batch], spec: &ScatterPlan, r: usize) -> Batch {
    let schema = *parts[0].schema();
    let summed = Batch::concat(&schema, parts.iter().map(Batch::as_mem_batch)).into_consolidated();
    spec.share(&summed, Slot::new(r as u32, parts.len() as u32))
}

/// Publish every worker's round-`round` partition in rank order: the round
/// completes on the last publish and not before.
fn publish_all(mesh: &mut [Mesh], round: u64, spec: &ScatterPlan, drained: [bool; NW]) {
    for w in 0..NW {
        assert!(
            mesh.iter().all(|m| !m.complete()),
            "round {round}: incomplete before worker {w}"
        );
        mesh[w].publish(9, drained[w], &partition(w, round), spec, true);
    }
    assert!(
        mesh.iter().all(Mesh::complete),
        "round {round}: complete on the last publish"
    );
}

/// Worker `r` gathers round `round`, a single part: what every sender routed to
/// it, and the AND of the drained bits.
fn gather_one(mesh: &mut Mesh, round: u64, spec: &ScatterPlan, drained: [bool; NW]) {
    let r = mesh.rank;
    let parts: Vec<Batch> = (0..NW).map(|w| partition(w, round)).collect();
    let (got, all_drained) = mesh.advance(None).expect("a round that fits one part");
    assert_eq!(
        weighted_rows(&got),
        weighted_rows(&expected_of(&parts, spec, r)),
        "round {round}: worker {r}"
    );
    assert_eq!(got.layout(), Layout::Consolidated, "round {round}: worker {r}");
    assert_eq!(all_drained, drained.iter().all(|&d| d), "round {round}: worker {r}");
}

/// Publish `parts[w]` on worker `w`, then [`finish_round`].
fn run_round(
    mesh: &mut [Mesh],
    parts: &[Batch],
    spec: &ScatterPlan,
    drained: &[bool],
    fold: bool,
) -> (Vec<(Batch, bool)>, usize) {
    for ((m, batch), &d) in mesh.iter_mut().zip(parts).zip(drained) {
        m.publish(9, d, batch, spec, fold);
    }
    finish_round(mesh, parts)
}

/// Advance every worker of a published round part by part until it closes:
/// each worker's rows and drained bit, and how many parts the round took.
fn finish_round(mesh: &mut [Mesh], parts: &[Batch]) -> (Vec<(Batch, bool)>, usize) {
    let mut count = 1;
    loop {
        assert!(mesh.iter().all(Mesh::complete), "part {count} completes");
        let out: Vec<Option<(Batch, bool)>> = mesh.iter_mut().zip(parts).map(|(m, b)| m.advance(Some(b))).collect();
        if out.iter().all(Option::is_some) {
            return (out.into_iter().map(Option::unwrap).collect(), count);
        }
        assert!(
            out.iter().all(Option::is_none),
            "part {count}: every worker agrees whether another part runs"
        );
        count += 1;
    }
}

/// A scattered round then a broadcast one, each gathered intact by every worker,
/// with round 1 published by two workers before the third gathered round 0.
#[test]
fn every_worker_gathers_each_round_from_every_peer() {
    let mut mesh = meshes(NW);
    let pk = by_pk(&make_schema_u64_i64());
    let routes = [&pk, &ScatterPlan::broadcast()];
    let drained = [[true, true, false], [true; NW]];

    publish_all(&mut mesh, 0, routes[0], drained[0]);
    for w in 0..NW - 1 {
        gather_one(&mut mesh[w], 0, routes[0], drained[0]);
        mesh[w].publish(9, drained[1][w], &partition(w, 1), routes[1], true);
    }
    assert!(
        !mesh[0].complete(),
        "round 1 is incomplete until the last worker publishes it"
    );
    gather_one(&mut mesh[NW - 1], 0, routes[0], drained[0]);
    mesh[NW - 1].publish(9, drained[1][NW - 1], &partition(NW - 1, 1), routes[1], true);
    for m in mesh.iter_mut() {
        gather_one(m, 1, routes[1], drained[1]);
    }
}

/// Publishing again before the round closes would overwrite a part peers may
/// still be reading.
#[test]
#[should_panic(expected = "a round is already open")]
fn a_second_publish_before_the_round_closes_is_refused() {
    let mut mesh = meshes(NW);
    let batch = partition(0, 0);
    mesh[0].publish(9, false, &batch, &ScatterPlan::broadcast(), true);
    mesh[0].publish(9, false, &batch, &ScatterPlan::broadcast(), true);
}

/// An advance before every worker published would read outboxes not yet written.
#[test]
#[should_panic(expected = "advanced before every worker published")]
fn an_advance_before_the_part_completes_is_refused() {
    let mut mesh = meshes(NW);
    mesh[0].publish(9, false, &partition(0, 0), &ScatterPlan::broadcast(), true);
    mesh[0].advance(None);
}

// ── Multi-part rounds ───────────────────────────────────────────────────────

/// `n` rows over `(U64 PK, I64)` from worker `w`, consolidated: every worker
/// sends the same `(pk, payload)` elements at its own weight, so the gather must
/// sum across senders.
fn wide_partition(w: usize, n: u64) -> Batch {
    let rows: Vec<(u64, i64, i64)> = (0..n).map(|i| (i, 1 + w as i64, (i % 7) as i64)).collect();
    make_batch(&make_schema_u64_i64(), &rows)
}

/// Partitions far larger than an outbox cross in several parts, and every
/// receiver gathers what one part would have carried.
#[test]
fn an_oversize_round_crosses_in_parts() {
    let mut mesh = meshes(NW);
    let pk = by_pk(&make_schema_u64_i64());
    for fold in [true, false] {
        for (spec, drained) in [(&pk, [true, false, true]), (&ScatterPlan::broadcast(), [true; NW])] {
            let parts: Vec<Batch> = (0..NW).map(|w| wide_partition(w, 1000)).collect();
            let (got, count) = run_round(&mut mesh, &parts, spec, &drained, fold);
            assert!(count > 2, "1000 rows per sender take several parts, not {count}");
            for (r, (got, all_drained)) in got.into_iter().enumerate() {
                let layout = if fold { Layout::Consolidated } else { Layout::Raw };
                assert_eq!(got.layout(), layout, "receiver {r}, fold {fold}");
                assert_eq!(all_drained, drained.iter().all(|&d| d), "receiver {r}");
                let want = weighted_rows(&expected_of(&parts, spec, r));
                assert_eq!(
                    weighted_rows(&got.into_consolidated()),
                    want,
                    "receiver {r}, fold {fold}"
                );
            }
        }
    }
}

/// A sender that runs out of rows early, or never had any, keeps arriving with
/// empty parts until the longest sender is done.
#[test]
fn a_sender_with_nothing_left_sends_empty_parts() {
    let mut mesh = meshes(NW);
    let schema = make_schema_u64_i64();
    let pk = by_pk(&schema);
    for spec in [&pk, &ScatterPlan::broadcast()] {
        let parts = vec![
            wide_partition(0, 1000),
            wide_partition(1, 40),
            Batch::empty_with_schema(&schema),
        ];
        let (got, count) = run_round(&mut mesh, &parts, spec, &[false, true, true], true);
        assert!(count > 2, "the long sender takes several parts, not {count}");
        for (r, (got, drained)) in got.into_iter().enumerate() {
            assert_eq!(
                weighted_rows(&got),
                weighted_rows(&expected_of(&parts, spec, r)),
                "receiver {r}"
            );
            assert_eq!(
                got.layout(),
                Layout::Consolidated,
                "receiver {r}: the empty raw sender wrote no block"
            );
            assert!(!drained, "receiver {r}: worker 0 was not drained");
        }
    }
}

/// A heap many times the space a part leaves crosses in parts, each block
/// carrying its own share of it.
#[test]
fn a_heap_heavy_round_is_sized_by_its_relocated_heap() {
    let mut mesh = meshes(2);
    let texts: Vec<String> = (0..60u64).map(|pk| format!("{pk:->200}")).collect();
    let rows_of = |w: i64| {
        (0..60u64)
            .map(|pk| (pk, 1 + w, texts[pk as usize].as_bytes()))
            .collect::<Vec<_>>()
    };
    let consolidated = make_string_batch(&rows_of(0));
    let raw = make_batch_bytes_raw(
        &make_schema_pk_u64_payload_string(),
        &rows_of(1).into_iter().rev().collect::<Vec<_>>(),
    );
    assert!(
        consolidated.blob().len() > SMALL_OUTBOX,
        "the heap alone overflows a part"
    );
    let pk = by_pk(&make_schema_pk_u64_payload_string());
    let parts = vec![consolidated, raw];
    for spec in [&pk, &ScatterPlan::broadcast()] {
        let (got, count) = run_round(&mut mesh, &parts, spec, &[true, true], true);
        assert!(
            count > 2,
            "12 KiB of strings per sender take several parts, not {count}"
        );
        for (r, (got, _)) in got.into_iter().enumerate() {
            assert_eq!(got.layout(), Layout::Raw, "receiver {r}: a raw sender's rows stay raw");
            let want = expected_of(&parts, spec, r);
            assert!(
                (0..want.len()).all(|i| want.get_weight(i) == 3),
                "receiver {r}: weights sum to 1 + 2"
            );
            assert_eq!(
                weighted_rows(&got.into_consolidated()),
                weighted_rows(&want),
                "receiver {r}"
            );
        }
    }
}

/// Worker 0's partition over the string schema: `pks`, each with `text(pk)`.
fn string_partition(mut pks: Vec<u64>, text: impl Fn(u64) -> String) -> Batch {
    pks.sort_unstable();
    let texts: Vec<String> = pks.iter().map(|&pk| text(pk)).collect();
    make_string_batch(
        &pks.iter()
            .zip(&texts)
            .map(|(&pk, t)| (pk, 1, t.as_bytes()))
            .collect::<Vec<_>>(),
    )
}

/// A receiver list that fills an outbox to its last byte ends the part cleanly:
/// the next list has no room for a row and moves whole to the next part.
#[test]
fn a_list_that_fills_the_outbox_exactly_defers_the_next() {
    let schema = make_schema_pk_u64_payload_string();
    let row_width = make_string_batch(&[(0, 1, b"")]).wire_byte_size_range(1) - WAL_HEADER_SIZE;
    let room = SMALL_OUTBOX - BLOCKS_AT - WAL_HEADER_SIZE;
    // Fixed-width rows, and one long string whose heap bytes take up the rest.
    let fill = room / row_width - 1;
    let slack = room - fill * row_width;
    assert!(
        slack > gnitz_wire::SHORT_STRING_THRESHOLD,
        "the slack spills to the heap"
    );
    let to_0 = owned_by(0, 0, fill);
    let anchor = to_0[0];
    let pks = to_0.into_iter().chain(owned_by(1, 0, 5)).collect();
    let full = string_partition(pks, |pk| if pk == anchor { "l".repeat(slack) } else { String::new() });

    let mut mesh = meshes(2);
    let pk = by_pk(&schema);
    let spec = &pk;
    let parts = vec![full, Batch::empty_with_schema(&schema)];
    for (m, batch) in mesh.iter_mut().zip(&parts) {
        m.publish(9, true, batch, spec, true);
    }
    // SAFETY: worker 0 wrote its head before its arrival.
    let head = unsafe { &*mesh[0].outbox(0).cast::<Head>() };
    assert_eq!(
        head.blocks[0].at + head.blocks[0].len,
        SMALL_OUTBOX as u64,
        "list 0 fills the outbox"
    );
    assert_eq!(head.blocks[1].len, 0, "list 1 waits for the next part");
    let (got, count) = finish_round(&mut mesh, &parts);
    assert_eq!(count, 2);
    for (r, (got, _)) in got.iter().enumerate() {
        assert_eq!(
            weighted_rows(got),
            weighted_rows(&expected_of(&parts, spec, r)),
            "receiver {r}"
        );
    }
}

/// A string row too large for the space list 0 left, but not for an empty
/// outbox, moves to the next part rather than ending the round.
#[test]
fn a_heap_row_that_fits_only_a_fresh_part_moves_to_it() {
    // Twenty short rows for receiver 0; one long row for receiver 1.
    let pks = owned_by(0, 0, 20).into_iter().chain(owned_by(1, 100_000, 1)).collect();
    let partition = string_partition(pks, |pk| {
        if pk >= 100_000 {
            "z".repeat(3000)
        } else {
            format!("{pk:->40}")
        }
    });
    let schema = make_schema_pk_u64_payload_string();
    let mut mesh = meshes(2);
    let pk = by_pk(&schema);
    let spec = &pk;
    let parts = vec![partition, Batch::empty_with_schema(&schema)];
    let (got, count) = run_round(&mut mesh, &parts, spec, &[true, true], true);
    assert_eq!(count, 2, "the long row takes the second part");
    for (r, (got, _)) in got.iter().enumerate() {
        assert_eq!(
            weighted_rows(got),
            weighted_rows(&expected_of(&parts, spec, r)),
            "receiver {r}"
        );
    }
}
