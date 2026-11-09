use super::*;
use crate::query::Lane;
use crate::test_support::{
    make_batch, make_batch_bytes_raw, make_schema_pk_u64_payload_string, make_schema_u64_i64, make_string_batch,
    opk_pk, weighted_rows,
};
use gnitz_wire::wal::WAL_HEADER_SIZE;
use gnitz_zset::algebra::{Placement, Slot};

impl Mesh {
    /// Open a round of one lane, of view `view`.
    pub(crate) fn open_one<'b>(
        &mut self,
        view: u64,
        batch: Cow<'b, Batch>,
        plan: &'b ScatterPlan,
        fold: bool,
        drained: bool,
    ) -> Round<'b> {
        self.open(std::iter::once(Lane { view, batch, plan, fold }), drained)
    }

    /// [`Self::step`] of a round of one lane: its rows, and the round's verdict.
    pub(crate) fn step_one(&mut self, round: &mut Round<'_>) -> Option<(Batch, bool)> {
        let mut rows = Vec::with_capacity(1);
        let drained = self.step(round, &mut rows)?;
        assert_eq!(rows.len(), 1, "a round of one lane");
        Some((rows.pop().expect("one lane"), drained))
    }
}

const NW: usize = 3;

/// One page of blocks past the head.
const SMALL_OUTBOX: usize = BLOCKS_AT + PAGE_BYTES;

/// `nw` workers' views of one mesh of `outbox_bytes` outboxes, over parks of
/// their own.
pub(super) fn meshes(nw: usize, outbox_bytes: usize) -> Vec<Mesh> {
    create(outbox_bytes, WorkerParks::create(nw).expect("map the parks")).expect("map the mesh")
}

/// Routing by the PK of `schema`.
pub(super) fn by_pk(schema: &SchemaDescriptor) -> ScatterPlan {
    ScatterPlan::native(Placement::full_pk(schema))
}

/// The first `n` U64 pks from `from` up that [`by_pk`] routes to receiver `r` of 2.
fn owned_by(r: usize, from: u64, n: usize) -> Vec<u64> {
    let schema = make_schema_u64_i64();
    let placement = Placement::full_pk(&schema);
    let owner = |pk: u64| {
        placement
            .owner(&opk_pk(&schema, &[pk as u128]), 2)
            .expect("a keyed placement owns every key")
    };
    (from..).filter(|&pk| owner(pk) == r).take(n).collect()
}

/// Worker `w`'s `n` rows of round `round`, consolidated: every worker sends the
/// same elements at weight `1 + w`, so a gather must sum across senders, and a
/// block read from the wrong worker or round shows.
pub(super) fn partition(w: usize, round: u64, n: u64) -> Batch {
    let rows: Vec<(u64, i64, i64)> = (0..n).map(|i| (round * n + i, 1 + w as i64, (i % 7) as i64)).collect();
    make_batch(&make_schema_u64_i64(), &rows)
}

/// Receiver `r`'s close of a round of `parts` under `spec`: its share of every
/// sender's partition summed, merged iff every sender of rows to it published
/// consolidated under `fold`, and the AND of the `drained` claims.
fn assert_gathered(
    r: usize,
    (got, all_drained): (Batch, bool),
    parts: &[Batch],
    spec: &ScatterPlan,
    drained: &[bool],
    fold: bool,
) {
    let slot = Slot::new(r as u32, parts.len() as u32);
    let merged = parts
        .iter()
        .all(|p| spec.share(p, slot).is_empty() || fold && p.is_consolidated());
    let summed = Batch::concat(parts[0].schema(), parts.iter().map(Batch::as_mem_batch)).into_consolidated();
    // A batch of at most one row is consolidated whatever its senders were.
    let consolidated = merged || got.len() <= 1;
    assert_eq!(got.is_consolidated(), consolidated, "receiver {r}, fold {fold}");
    assert_eq!(all_drained, drained.iter().all(|&d| d), "receiver {r}");
    assert_eq!(
        weighted_rows(&got.into_consolidated()),
        weighted_rows(&spec.share(&summed, slot)),
        "receiver {r}, fold {fold}"
    );
}

/// Worker `r` gathers round `round` of [`partition`]s of 8 rows, a single part.
fn gather_one(mesh: &mut Mesh, open: &mut Round<'_>, round: u64, spec: &ScatterPlan, drained: [bool; NW]) {
    let parts: Vec<Batch> = (0..NW).map(|w| partition(w, round, 8)).collect();
    let got = mesh.step_one(open).expect("a round that fits one part");
    assert_gathered(mesh.rank, got, &parts, spec, &drained, true);
}

/// Open a round of `parts[w]` on worker `w`, then [`finish_round`].
fn run_round(mesh: &mut [Mesh], parts: &[Batch], spec: &ScatterPlan, drained: &[bool], fold: bool) -> usize {
    let rounds = mesh
        .iter_mut()
        .zip(parts)
        .zip(drained)
        .map(|((m, batch), &d)| m.open_one(9, Cow::Borrowed(batch), spec, fold, d))
        .collect();
    finish_round(mesh, rounds, parts, spec, drained, fold)
}

/// Step every worker of an open round of `parts` part by part until it closes,
/// checking what each gathered; how many parts the round took.
fn finish_round(
    mesh: &mut [Mesh],
    mut rounds: Vec<Round<'_>>,
    parts: &[Batch],
    spec: &ScatterPlan,
    drained: &[bool],
    fold: bool,
) -> usize {
    let mut count = 1;
    loop {
        assert!(mesh.iter().all(Mesh::complete), "part {count} completes");
        let out: Vec<Option<(Batch, bool)>> = mesh.iter_mut().zip(&mut rounds).map(|(m, r)| m.step_one(r)).collect();
        if out.iter().all(Option::is_some) {
            for (r, got) in out.into_iter().flatten().enumerate() {
                assert_gathered(r, got, parts, spec, drained, fold);
            }
            return count;
        }
        assert!(
            out.iter().all(Option::is_none),
            "part {count}: every worker agrees whether another part runs"
        );
        count += 1;
    }
}

/// A scattered round then a broadcast one, each gathered intact by every worker,
/// with round 1 opened by two workers before the third gathered round 0. A part
/// completes on its last open and not before, and a step of an incomplete one
/// waits.
#[test]
fn every_worker_gathers_each_round_from_every_peer() {
    let mut mesh = meshes(NW, SMALL_OUTBOX);
    let pk = by_pk(&make_schema_u64_i64());
    let routes = [&pk, &ScatterPlan::broadcast()];
    let drained = [[true, true, false], [true; NW]];
    let open = |m: &mut Mesh, w: usize, round: usize| {
        m.open_one(
            9,
            Cow::Owned(partition(w, round as u64, 8)),
            routes[round],
            true,
            drained[round][w],
        )
    };

    let mut rounds: Vec<Round> = (0..NW)
        .map(|w| {
            assert!(mesh.iter().all(|m| !m.complete()), "incomplete before worker {w}");
            open(&mut mesh[w], w, 0)
        })
        .collect();
    assert!(mesh.iter().all(Mesh::complete), "complete on the last open");
    for w in 0..NW - 1 {
        gather_one(&mut mesh[w], &mut rounds[w], 0, routes[0], drained[0]);
        rounds[w] = open(&mut mesh[w], w, 1);
    }
    assert!(
        mesh[0].step_one(&mut rounds[0]).is_none(),
        "round 1 is incomplete until the last worker opens it"
    );
    let last = NW - 1;
    gather_one(&mut mesh[last], &mut rounds[last], 0, routes[0], drained[0]);
    rounds[last] = open(&mut mesh[last], last, 1);
    for (m, open) in mesh.iter_mut().zip(&mut rounds) {
        gather_one(m, open, 1, routes[1], drained[1]);
    }
}

/// Opening again before the round closes would overwrite a part peers may
/// still be reading.
#[test]
#[should_panic(expected = "a round is already open")]
fn a_second_open_before_the_round_closes_is_refused() {
    let mut mesh = meshes(NW, SMALL_OUTBOX);
    let batch = partition(0, 0, 8);
    let plan = ScatterPlan::broadcast();
    let _first = mesh[0].open_one(9, Cow::Borrowed(&batch), &plan, true, true);
    mesh[0].open_one(9, Cow::Borrowed(&batch), &plan, true, true);
}

// ── Multi-part rounds ───────────────────────────────────────────────────────

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

/// A round too large for one part crosses in several, and every receiver
/// gathers what one part would have carried, scattered or broadcast, folded or
/// not: partitions far larger than an outbox; a sender that runs out early or
/// never had rows, and keeps arriving with empty parts; a heap many times a
/// part's room, each block carrying its own share of it, beside a raw sender
/// whose rows stay raw; and a heap row too large for the room an earlier list
/// left but not for a fresh part, which moves to the next part rather than
/// ending the round.
#[test]
fn a_round_too_large_for_one_part_crosses_in_several() {
    let fixed = make_schema_u64_i64();
    let strings = make_schema_pk_u64_payload_string();
    let texts: Vec<String> = (0..60u64).map(|pk| format!("{pk:->200}")).collect();
    let heap_rows = |w: i64| {
        (0..60u64)
            .map(|pk| (pk, 1 + w, texts[pk as usize].as_bytes()))
            .collect::<Vec<_>>()
    };
    let heap = make_string_batch(&heap_rows(0));
    assert!(heap.blob().len() > SMALL_OUTBOX, "the heap alone overflows a part");
    let raw_heap = make_batch_bytes_raw(&strings, &heap_rows(1).into_iter().rev().collect::<Vec<_>>());
    // Twenty short rows for receiver 0; one long row for receiver 1.
    let pks = owned_by(0, 0, 20).into_iter().chain(owned_by(1, 100_000, 1)).collect();
    let one_long = string_partition(pks, |pk| match pk >= 100_000 {
        true => "z".repeat(3000),
        false => format!("{pk:->40}"),
    });

    let cases: [(&str, Vec<Batch>, &[bool], usize); 4] = [
        (
            "oversize",
            (0..NW).map(|w| partition(w, 0, 1000)).collect(),
            &[true, false, true],
            3,
        ),
        (
            "a sender runs out",
            vec![
                partition(0, 0, 1000),
                partition(1, 0, 40),
                Batch::empty_with_schema(&fixed),
            ],
            &[false, true, true],
            3,
        ),
        ("heap-heavy", vec![heap, raw_heap], &[true, true], 3),
        (
            "a heap row fits only a fresh part",
            vec![one_long, Batch::empty_with_schema(&strings)],
            &[true, true],
            2,
        ),
    ];
    for (case, parts, drained, min_parts) in &cases {
        let mut mesh = meshes(parts.len(), SMALL_OUTBOX);
        let pk = by_pk(parts[0].schema());
        for spec in [&pk, &ScatterPlan::broadcast()] {
            for fold in [true, false] {
                let count = run_round(&mut mesh, parts, spec, drained, fold);
                assert!(count >= *min_parts, "{case}: {count} parts");
            }
        }
    }
}

/// A receiver list that fills an outbox to its last byte ends the part cleanly:
/// the next list has no room for a row and moves whole to the next part.
#[test]
fn a_list_that_fills_the_outbox_exactly_defers_the_next() {
    let schema = make_schema_pk_u64_payload_string();
    let row_width = schema.row_width();
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

    let mut mesh = meshes(2, SMALL_OUTBOX);
    let spec = by_pk(&schema);
    let parts = vec![full, Batch::empty_with_schema(&schema)];
    let rounds: Vec<Round> = mesh
        .iter_mut()
        .zip(&parts)
        .map(|(m, batch)| m.open_one(9, Cow::Borrowed(batch), &spec, true, true))
        .collect();
    // SAFETY: worker 0 wrote its lane's spans before its arrival.
    let spans = unsafe { &*mesh[0].lane_at(0, 0).1.cast::<[Span; 2]>() };
    assert_eq!(
        spans[0].at + spans[0].len,
        SMALL_OUTBOX as u32,
        "list 0 fills the outbox"
    );
    assert_eq!(spans[1].len, 0, "list 1 waits for the next part");
    assert_eq!(finish_round(&mut mesh, rounds, &parts, &spec, &[true, true], true), 2);
}

/// Several views' lanes share one round: each lane is gathered as a round of
/// its own would be — an empty lane, a broadcast one, and one that overflows
/// into later parts while the lanes before it are already sent.
#[test]
fn a_round_of_several_lanes_gathers_each_as_its_own() {
    let mut mesh = meshes(NW, SMALL_OUTBOX);
    let schema = make_schema_u64_i64();
    let pk = by_pk(&schema);
    let broadcast = ScatterPlan::broadcast();
    let specs = [&pk, &pk, &broadcast, &pk];
    let lanes: Vec<Vec<Batch>> = vec![
        (0..NW).map(|w| partition(w, 0, 8)).collect(),
        (0..NW).map(|_| Batch::empty_with_schema(&schema)).collect(),
        (0..NW).map(|w| partition(w, 1, 5)).collect(),
        (0..NW).map(|w| partition(w, 2, 1000)).collect(),
    ];
    assert!(mesh[0].max_lanes() >= lanes.len());
    let mut rounds: Vec<Round> = mesh
        .iter_mut()
        .enumerate()
        .map(|(w, m)| {
            let lanes: Vec<Lane> = (lanes.iter().zip(specs).enumerate())
                .map(|(i, (parts, plan))| Lane {
                    view: 7 + i as u64,
                    batch: Cow::Borrowed(&parts[w]),
                    plan,
                    fold: true,
                })
                .collect();
            m.open(lanes.into_iter(), true)
        })
        .collect();
    let mut count = 1;
    let gathered = loop {
        let out: Vec<_> = (mesh.iter_mut().zip(&mut rounds))
            .map(|(m, r)| {
                let mut rows = Vec::new();
                m.step(r, &mut rows).map(|drained| (rows, drained))
            })
            .collect();
        if out.iter().all(Option::is_some) {
            break out;
        }
        assert!(
            out.iter().all(Option::is_none),
            "every worker agrees whether another part runs"
        );
        count += 1;
    };
    assert!(count >= 3, "the last lane crosses several parts: {count}");
    for (r, (rows, drained)) in gathered.into_iter().flatten().enumerate() {
        assert_eq!(rows.len(), lanes.len());
        for ((rows, parts), spec) in rows.into_iter().zip(&lanes).zip(specs) {
            assert_gathered(r, (rows, drained), parts, spec, &[true; NW], true);
        }
    }
}

/// A trim frees the pages past the part, in a mapping every worker shares.
#[test]
fn trim_returns_the_pages_past_a_part() {
    let mut m = meshes(1, 2 * RESIDENT_SLACK_BYTES).pop().unwrap();
    let resident = BLOCKS_AT + RESIDENT_SLACK_BYTES + 2 * PAGE_BYTES;
    unsafe { m.outbox(0).add(resident - 1).write(7) };
    m.resident[0] = resident;
    m.trim(BLOCKS_AT);
    assert_eq!(unsafe { m.outbox(0).add(resident - 1).read() }, 0);
    assert_eq!(m.resident[0], BLOCKS_AT);
}
