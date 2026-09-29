use super::*;
use crate::test_support::{make_batch, make_schema_u64_i64};
use gnitz_store::schema::SchemaColumn;
use gnitz_store::storage::BatchBuilder;
use gnitz_wire::wal::WAL_HEADER_SIZE;
use gnitz_wire::TypeCode;

const NW: usize = 3;

const SMALL_OUTBOX: usize = 2 * PAGE_BYTES;

fn meshes() -> Vec<Mesh> {
    super::fixtures::meshes(NW, OUTBOX_BYTES)
}

/// Routing by the leading PK column of `schema`.
fn by_pk(schema: &SchemaDescriptor) -> ScatterPlan {
    ScatterPlan::group(schema, &[0]).unwrap()
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

/// What receiver `r` of `nw` must gather from `parts` under `spec`: its share of
/// every sender's partition summed, or with no spec all of it.
fn expected_of(parts: &[Batch], spec: Option<&ScatterPlan>, r: usize) -> Batch {
    let schema = *parts[0].schema();
    let summed = Batch::concat(&schema, parts.iter().map(Batch::as_mem_batch)).into_consolidated();
    match spec {
        None => summed,
        Some(spec) => {
            let mut pool = Vec::new();
            let routed = op_exchange_route(&summed, spec, &mut pool, parts.len());
            summed.ascending_subset(&routed[r])
        }
    }
}

fn expected(spec: Option<&ScatterPlan>, round: u64, r: usize) -> Vec<(u128, i64, i64)> {
    let parts: Vec<Batch> = (0..NW).map(|w| partition(w, round)).collect();
    rows(&expected_of(&parts, spec, r))
}

/// Publish every worker's round-`round` partition in rank order: the round
/// completes on the last publish and not before.
fn publish_all(mesh: &mut [Mesh], round: u64, spec: Option<&ScatterPlan>, drained: [bool; NW]) {
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
fn gather_one(mesh: &mut Mesh, round: u64, spec: Option<&ScatterPlan>, drained: [bool; NW]) {
    let r = mesh.rank;
    let (got, all_drained) = mesh.advance(None).expect("a round that fits one part");
    assert_eq!(rows(&got), expected(spec, round, r), "round {round}: worker {r}");
    assert_eq!(got.layout(), Layout::Consolidated, "round {round}: worker {r}");
    assert_eq!(all_drained, drained.iter().all(|&d| d), "round {round}: worker {r}");
}

/// Publish `parts[w]` on worker `w`, then [`finish_round`].
fn run_round(
    mesh: &mut [Mesh],
    parts: &[Batch],
    spec: Option<&ScatterPlan>,
    drained: &[bool],
) -> (Vec<(Batch, bool)>, usize) {
    for ((m, batch), &d) in mesh.iter_mut().zip(parts).zip(drained) {
        m.publish(9, d, batch, spec, true);
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

/// Four rounds, alternating a scattered and a broadcast one, each gathered
/// intact by every worker. Round 1 is published by two workers before the third
/// has gathered round 0.
#[test]
fn every_worker_gathers_each_round_from_every_peer() {
    let mut mesh = meshes();
    let pk = by_pk(&make_schema_u64_i64());
    let scatter = Some(&pk);
    let routes = [scatter, None, scatter, None];
    let drained = [[true, true, false], [true; NW], [false; NW], [true; NW]];

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

    for round in 2..4u64 {
        let k = round as usize;
        publish_all(&mut mesh, round, routes[k], drained[k]);
        for m in mesh.iter_mut() {
            gather_one(m, round, routes[k], drained[k]);
        }
    }
}

/// A round landing in a register that does not fold is concatenated, not
/// merged: the receiver gets the same Z-set, left raw.
#[test]
fn a_round_into_a_non_folding_register_concatenates() {
    let mut mesh = meshes();
    let pk = by_pk(&make_schema_u64_i64());
    for (w, m) in mesh.iter_mut().enumerate() {
        m.publish(9, false, &partition(w, 0), Some(&pk), false);
    }
    for m in mesh.iter_mut() {
        let r = m.rank;
        let (got, _) = m.advance(None).expect("a round that fits one part");
        assert_eq!(got.layout(), Layout::Raw, "worker {r}");
        assert_eq!(rows(&got.into_consolidated()), expected(Some(&pk), 0, r), "worker {r}");
    }
}

/// Publishing again before the round closes would overwrite a part peers may
/// still be reading.
#[test]
#[should_panic(expected = "a round is already open")]
fn a_second_publish_before_the_round_closes_is_refused() {
    let mut mesh = meshes();
    let batch = partition(0, 0);
    mesh[0].publish(9, false, &batch, None, true);
    mesh[0].publish(9, false, &batch, None, true);
}

/// An advance before every worker published would read outboxes not yet written.
#[test]
#[should_panic(expected = "advanced before every worker published")]
fn an_advance_before_the_part_completes_is_refused() {
    let mut mesh = meshes();
    mesh[0].publish(9, false, &partition(0, 0), None, true);
    mesh[0].advance(None);
}

/// A `(U64 PK, STRING)` schema.
fn string_schema() -> SchemaDescriptor {
    SchemaDescriptor::new(
        &[
            SchemaColumn::new(TypeCode::U64, false),
            SchemaColumn::new(TypeCode::String, false),
        ],
        &[0],
    )
}

/// `(pk, weight, text)` rows over [`string_schema`], in the order given.
fn string_batch(rows: &[(u64, i64, String)]) -> Batch {
    let mut b = BatchBuilder::new(string_schema());
    for (pk, w, text) in rows {
        b.begin_row(*pk as u128, *w);
        b.put_string(text);
        b.end_row();
    }
    b.finish()
}

/// `(pk, weight, text)` of every row, in order.
fn string_rows(b: &Batch) -> Vec<(u128, i64, Vec<u8>)> {
    let mb = b.as_mem_batch();
    (0..b.len())
        .map(|i| {
            (
                b.get_pk(i),
                b.get_weight(i),
                gnitz_expr::payload_bytes(&mb, i, 0).to_vec(),
            )
        })
        .collect()
}

/// Strings past the inline width live in a heap, which each scattered block
/// carries its own share of: each receiver still gathers every sender's rows,
/// strings intact, scattered and broadcast alike.
#[test]
fn heap_strings_cross_the_mesh_intact() {
    let text = |pk: u64| format!("a string long enough to leave the inline prefix, row {pk}");
    let partition = |w: u64| string_batch(&(0..6).map(|i| (w * 100 + i, 1, text(w * 100 + i))).collect::<Vec<_>>());
    let mut mesh = meshes();
    let pk = by_pk(&string_schema());
    for (round, spec) in [Some(&pk), None].into_iter().enumerate() {
        for (w, m) in mesh.iter_mut().enumerate() {
            m.publish(9, false, &partition(w as u64), spec, true);
        }
        let mut seen = Vec::new();
        for m in mesh.iter_mut() {
            let (got, _) = m.advance(None).expect("one part");
            for (pk, _, bytes) in string_rows(&got) {
                assert_eq!(bytes, text(pk as u64).as_bytes(), "round {round}: pk {pk}");
                seen.push(pk as u64);
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

// ── Multi-part rounds ───────────────────────────────────────────────────────

/// `n` rows over `(U64 PK, I64)` from worker `w`, consolidated: every worker
/// sends the same `(pk, payload)` elements at its own weight, so the gather must
/// sum across senders.
fn wide_partition(w: usize, n: u64) -> Batch {
    let rows: Vec<(u64, i64, i64)> = (0..n).map(|i| (i, 1 + w as i64, (i % 7) as i64)).collect();
    make_batch(&make_schema_u64_i64(), &rows)
}

/// Partitions far larger than an outbox cross in several parts, scattered and
/// broadcast, and every receiver gathers exactly what one part would have
/// carried: its share, summed across senders, consolidated, drained ANDed.
#[test]
fn an_oversize_round_crosses_in_parts() {
    let mut mesh = super::fixtures::meshes(NW, SMALL_OUTBOX);
    let pk = by_pk(&make_schema_u64_i64());
    for (spec, drained) in [(Some(&pk), [true, false, true]), (None, [true; NW])] {
        let parts: Vec<Batch> = (0..NW).map(|w| wide_partition(w, 1000)).collect();
        let want: Vec<_> = (0..NW).map(|r| rows(&expected_of(&parts, spec, r))).collect();
        let (got, count) = run_round(&mut mesh, &parts, spec, &drained);
        assert!(count > 2, "1000 rows per sender take several parts, not {count}");
        for (r, (got, all_drained)) in got.iter().enumerate() {
            assert_eq!(rows(got), want[r], "receiver {r}");
            assert_eq!(got.layout(), Layout::Consolidated, "receiver {r}");
            assert_eq!(*all_drained, drained.iter().all(|&d| d), "receiver {r}");
        }
    }
}

/// A sender that runs out of rows early, or never had any, keeps arriving with
/// empty parts until the longest sender is done; an empty partition writes no
/// block, so no receiver's gather sees an empty slice.
#[test]
fn a_sender_with_nothing_left_sends_empty_parts() {
    let mut mesh = super::fixtures::meshes(NW, SMALL_OUTBOX);
    let schema = make_schema_u64_i64();
    let pk = by_pk(&schema);
    for spec in [Some(&pk), None] {
        let parts = vec![
            wide_partition(0, 1000),
            wide_partition(1, 40),
            Batch::empty_with_schema(&schema),
        ];
        let want: Vec<_> = (0..NW).map(|r| rows(&expected_of(&parts, spec, r))).collect();
        for (m, (batch, drained)) in mesh.iter_mut().zip(parts.iter().zip([false, true, true])) {
            m.publish(9, drained, batch, spec, true);
        }
        // SAFETY: worker 2 wrote its head before its arrival, and rewrites it
        // only after this part is advanced.
        let head = unsafe { &*mesh[2].outbox(2).cast::<Head>() };
        assert!(
            head.blocks.iter().all(|s| s.len == 0),
            "an empty partition names no block"
        );
        assert_eq!(head.more, 0);
        let (got, count) = finish_round(&mut mesh, &parts);
        assert!(count > 2, "the long sender takes several parts, not {count}");
        for (r, (got, drained)) in got.into_iter().enumerate() {
            assert_eq!(rows(&got), want[r], "receiver {r}");
            assert!(!drained, "receiver {r}: worker 0 was not drained");
        }
    }
}

/// Long distinct strings whose heap is many times the space a part leaves, from
/// a consolidated and a raw sender: each block is sized by its relocated heap,
/// the round needs several parts, and the result is raw with every weight exact.
#[test]
fn a_heap_heavy_round_is_sized_by_its_relocated_heap() {
    let mut mesh = super::fixtures::meshes(2, SMALL_OUTBOX);
    let text = |pk: u64| format!("{pk:->200}");
    let rows_of = |w: i64| (0..60u64).map(|pk| (pk, 1 + w, text(pk))).collect::<Vec<_>>();
    let mut consolidated = string_batch(&rows_of(0));
    consolidated.certify_layout(Layout::Consolidated);
    let raw = string_batch(&rows_of(1).into_iter().rev().collect::<Vec<_>>());
    assert!(
        consolidated.blob().len() > SMALL_OUTBOX,
        "the heap alone overflows a part"
    );
    let schema = string_schema();
    let pk = by_pk(&schema);
    let spec = Some(&pk);
    let parts = vec![consolidated, raw];
    let want: Vec<_> = (0..2).map(|r| string_rows(&expected_of(&parts, spec, r))).collect();
    let (got, count) = run_round(&mut mesh, &parts, spec, &[true, true]);
    assert!(
        count > 2,
        "12 KiB of strings per sender take several parts, not {count}"
    );
    for (r, (got, _)) in got.into_iter().enumerate() {
        assert_eq!(got.layout(), Layout::Raw, "receiver {r}: a raw sender's rows stay raw");
        let got = got.into_consolidated();
        assert_eq!(string_rows(&got), want[r], "receiver {r}");
        assert!(
            want[r].iter().all(|&(_, w, _)| w == 3),
            "receiver {r}: weights sum to 1 + 2"
        );
    }
}

/// Worker 0's rows under `spec` among 2 workers: `per[r]` rows to receiver `r`,
/// picked off an ascending pk walk.
fn routed_partition(per: [usize; 2], make: impl Fn(&[u64]) -> Batch) -> Batch {
    let candidates: Vec<u64> = (0..4096).collect();
    let probe = make(&candidates);
    let mut lists = Vec::new();
    let routed = op_exchange_route(&probe, &by_pk(probe.schema()), &mut lists, 2);
    let mut pks: Vec<u64> = (0..2)
        .flat_map(|r| routed[r][..per[r]].iter().map(|&i| probe.get_pk(i as usize) as u64))
        .collect();
    pks.sort_unstable();
    make(&pks)
}

/// A receiver list that fills an outbox to its last byte ends the part cleanly:
/// the next list has no room for a row and moves whole to the next part.
#[test]
fn a_list_that_fills_the_outbox_exactly_defers_the_next() {
    let schema = string_schema();
    let row_width = string_batch(&[(0, 1, String::new())]).wire_byte_size_range(1) - WAL_HEADER_SIZE;
    let room = SMALL_OUTBOX - BLOCKS_AT - WAL_HEADER_SIZE;
    // Fixed-width rows, and one long string whose heap bytes take up the rest.
    let fill = room / row_width - 1;
    let slack = room - fill * row_width;
    assert!(
        slack > gnitz_wire::SHORT_STRING_THRESHOLD,
        "the slack spills to the heap"
    );
    // The one long string sits on the smallest key worker 0 owns.
    let anchor = {
        let probe = string_batch(&(0..4096).map(|pk| (pk, 1, String::new())).collect::<Vec<_>>());
        let mut lists = Vec::new();
        probe.get_pk(op_exchange_route(&probe, &by_pk(&schema), &mut lists, 2)[0][0] as usize) as u64
    };
    let make = |pks: &[u64]| {
        let text = |pk: u64| if pk == anchor { "l".repeat(slack) } else { String::new() };
        let mut b = string_batch(&pks.iter().map(|&pk| (pk, 1, text(pk))).collect::<Vec<_>>());
        b.certify_layout(Layout::Consolidated);
        b
    };
    let full = routed_partition([fill, 5], make);
    let mut mesh = super::fixtures::meshes(2, SMALL_OUTBOX);
    let pk = by_pk(&schema);
    let spec = Some(&pk);
    let parts = vec![full, Batch::empty_with_schema(&schema)];
    let want: Vec<_> = (0..2).map(|r| expected_of(&parts, spec, r).len()).collect();
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
    assert_eq!(head.more, 1);
    assert!(mesh.iter_mut().zip(&parts).all(|(m, b)| m.advance(Some(b)).is_none()));
    let got: Vec<_> = mesh
        .iter_mut()
        .zip(&parts)
        .map(|(m, b)| m.advance(Some(b)).expect("the second part closes").0)
        .collect();
    assert_eq!(got.iter().map(Batch::len).collect::<Vec<_>>(), want);
}

/// A string row too large for the space list 0 left, but not for an empty
/// outbox, moves to the next part rather than ending the round.
#[test]
fn a_heap_row_that_fits_only_a_fresh_part_moves_to_it() {
    let text = |pk: u64| match pk {
        // Only the last row's string is long.
        _ if pk >= 100_000 => "z".repeat(3000),
        _ => format!("{pk:->40}"),
    };
    let make = |pks: &[u64]| {
        let mut b = string_batch(&pks.iter().map(|&pk| (pk, 1, text(pk))).collect::<Vec<_>>());
        b.certify_layout(Layout::Consolidated);
        b
    };
    // Twenty short rows for receiver 0; one long row for receiver 1.
    let short = routed_partition([20, 0], make);
    let long = (100_000..).find(|&pk| {
        let probe = make(&[pk]);
        let mut lists = Vec::new();
        op_exchange_route(&probe, &by_pk(probe.schema()), &mut lists, 2)[1].len() == 1
    });
    let pks: Vec<u64> = (0..short.len()).map(|i| short.get_pk(i) as u64).chain(long).collect();
    let partition = make(&pks);
    let mut mesh = super::fixtures::meshes(2, SMALL_OUTBOX);
    let pk = by_pk(&string_schema());
    let spec = Some(&pk);
    let parts = vec![partition, Batch::empty_with_schema(&string_schema())];
    let want: Vec<_> = (0..2).map(|r| string_rows(&expected_of(&parts, spec, r))).collect();
    let (got, count) = run_round(&mut mesh, &parts, spec, &[true, true]);
    assert_eq!(count, 2, "the long row takes the second part");
    for (r, (got, _)) in got.iter().enumerate() {
        assert_eq!(string_rows(got), want[r], "receiver {r}");
    }
}
