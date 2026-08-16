//! Server bootstrap: single entry point for server startup.
//!
//! `server_main()` opens the catalog, allocates shared IPC resources, forks workers,
//! runs SAL recovery, and enters the executor event loop.

use std::collections::{HashMap, HashSet};

use crate::catalog::{CatalogEngine, FIRST_USER_TABLE_ID};
use crate::foundation::fault::Seam;
use crate::foundation::posix_io;
use crate::runtime::executor::{ServerExecutor, TlsListener};
use crate::runtime::master::MasterDispatcher;
use crate::runtime::sal::{
    sal_mmap_size, sal_tail_slot_count, EpochGate, SalReader, SalStep, SalWriter, FLAG_DDL_SYNC, FLAG_PUSH,
    FLAG_TXN_COMMIT, FLAG_ZONE_START,
};
use crate::runtime::w2m::{W2mReceiver, W2mWriter};
use crate::runtime::w2m_ring::{self, W2M_REGION_SIZE};
use crate::runtime::wire as ipc;
use crate::runtime::worker::{buffer_pending_delta, WorkerProcess};
use crate::storage::{partition_range, Batch};

/// Boot-progress line on stdout (the server log). Used instead of the log
/// macros where the raw, untagged line is the documented boot output (e.g. the
/// "GnitzDB ready" marker tests wait for).
fn boot_log(msg: &str) {
    let _ = posix_io::write_all_fd(1, msg.as_bytes());
}

// ---------------------------------------------------------------------------
// SAL recovery: the zone LSN is the atomic unit.
// ---------------------------------------------------------------------------
//
// Two passes over the same walk. Pass 1 (`collect_committed_lsns`) decides which
// zone LSNs are committed; pass 2 (`recover_sal`) applies their groups. A whole
// CREATE is one zone — its N families ride one `FLAG_DDL_TXN` bundle under a
// single zone LSN — so its groups apply iff that one sentinel is durable: all of
// COL_TAB and TABLE_TAB, or none.
//
// The same walker handles both system-table replay (master pre-fork) and
// user-table replay (per-worker post-fork): callers vary only the family_lsns
// scope and the apply closure.

/// One step of the recovery walk: a group that was published at an offset, or an
/// offset whose bytes were published but do not verify.
enum WalkStep {
    Group(crate::runtime::sal::SalMessage<'static>),
    Corrupt(u64),
}

/// Walk the SAL from offset 0 against the walk epoch `E`, resyncing past damage
/// to the next candidate valid at `E` so that one bad offset does not discard the
/// committed groups behind it. A zero prefix or a header at another epoch ends
/// the walk — the ring's leftovers begin there.
///
/// Both passes walk identically, so they cannot disagree about where the log
/// ends. Successive resyncs examine disjoint increasing ranges, so a walk sweeps
/// the mapping at most once in total.
fn sal_walk(sal_reader: &SalReader, epoch: u32) -> impl Iterator<Item = WalkStep> + '_ {
    let mut offset: u64 = 0;
    std::iter::from_fn(move || match sal_reader.read_at(offset, EpochGate::Walk(epoch)) {
        SalStep::Group(msg, next) => {
            offset = next;
            Some(WalkStep::Group(msg))
        }
        SalStep::Corrupt => {
            let corrupt_at = offset;
            offset = sal_reader
                .valid_headers_from(offset + 8)
                .find(|&(_, _, e)| e == epoch)
                .map_or(sal_reader.mmap_size(), |(base, _, _)| base);
            Some(WalkStep::Corrupt(corrupt_at))
        }
        SalStep::Absent | SalStep::OtherEpoch => None,
    })
}

/// The groups of one zone, as pass 1 accumulates them.
#[derive(Default)]
struct Zone {
    lsn: u64,
    /// The `FLAG_ZONE_START` group's offset — the start of the zone's byte span.
    start: u64,
    /// The first corrupt offset inside the span, if any.
    damage: Option<u64>,
    /// `(offset, target_id, slot count)` per non-sentinel group.
    groups: Vec<(u64, u32, u32)>,
}

/// Pass 1: the LSNs whose zone is both closed and intact.
///
/// A zone's byte span runs from its `FLAG_ZONE_START` group to its
/// `FLAG_TXN_COMMIT` sentinel, and nothing else lives in it — a zone is written
/// with no suspension point inside it, so no other writer can place a group
/// there. Damage costs that zone a group, and the verdict follows from where it
/// sits:
///
/// * inside any zone but the last — `Err`, naming the offset. A later committed
///   zone is durable behind it, so this is a hole in the log rather than a torn
///   tail, and the boot must not proceed past it.
/// * inside the **last** zone — demote it. A sentinel on disk is not an ACK, so
///   failing the boot there would brick it on a transaction nobody was promised.
/// * outside every zone span — an `lsn = 0` command group, or a zone that never
///   closed. Nothing was promised; the walk resyncs and the boot proceeds.
///
/// Two more shapes are fatal for the same reason: a sentinel that arrives with no
/// zone open lost its zone's *first* group, and a zone with a group after it but
/// no sentinel lost its *sentinel* (an unclosed zone can only be the last thing
/// in the log).
///
/// The last surviving zone's blocks are then decode-validated across every slot
/// its groups declare, not only this reader's, so worker 3 cannot demote a zone
/// worker 0 applies. Only the families this walk covers are validated — no zone
/// mixes system and user families, so the master would otherwise decode a whole
/// user push to reach a verdict it never acts on.
fn collect_committed_lsns(
    sal_reader: &SalReader,
    epoch: u32,
    family_lsns: &HashMap<i64, u64>,
) -> Result<HashSet<u64>, String> {
    let mut committed: HashSet<u64> = HashSet::new();
    let mut open: Option<Zone> = None;
    // A damaged zone that did close. Fatal once a later sentinel proves a
    // committed zone is durable behind it; discarded if it was the log's last.
    let mut torn: Option<String> = None;
    let mut last_closed: Option<Zone> = None;

    for step in sal_walk(sal_reader, epoch) {
        let msg = match step {
            WalkStep::Corrupt(off) => {
                if let Some(z) = open.as_mut() {
                    z.damage.get_or_insert(off);
                }
                continue;
            }
            WalkStep::Group(msg) => msg,
        };
        let mine = open.as_ref().is_some_and(|z| z.lsn == msg.lsn);
        if msg.flags & FLAG_TXN_COMMIT != 0 {
            if let Some(e) = torn.take() {
                return Err(e);
            }
            // A sentinel with no zone open lost the zone's first group; one that
            // closes a *different* zone lost that zone's sentinel as well.
            let Some(zone) = open.take().filter(|_| mine) else {
                return Err(format!(
                    "SAL replay: committed zone lsn={} lost a group before its commit sentinel at \
                     offset={}; its FLAG_ZONE_START group is not in the log",
                    msg.lsn, msg.base
                ));
            };
            match zone.damage {
                None => {
                    committed.insert(zone.lsn);
                }
                Some(at) => {
                    torn = Some(format!(
                        "SAL replay: committed zone lsn={} lost a group at offset={at} (zone ends \
                         at offset={}); a later committed zone is durable behind it, so this is a \
                         hole in the log rather than a torn tail",
                        zone.lsn, msg.base
                    ))
                }
            }
            last_closed = Some(zone);
            continue;
        }
        // A group arriving while another zone is open means that zone did close
        // and its sentinel was destroyed.
        if !mine {
            if let Some(z) = open.take() {
                return Err(format!(
                    "SAL replay: committed zone lsn={} at offset={} lost its commit sentinel (a \
                     group at offset={} follows it, so the zone did close)",
                    z.lsn, z.start, msg.base
                ));
            }
        }
        if msg.flags & FLAG_ZONE_START != 0 {
            open = Some(Zone {
                lsn: msg.lsn,
                start: msg.base,
                ..Zone::default()
            });
        }
        // Only groups behind an open zone start are the zone's. A non-zero `lsn`
        // alone does not make one: the ephemeral flush round carries the
        // checkpoint generation there and belongs to no zone.
        if let Some(z) = open.as_mut() {
            z.groups.push((msg.base, msg.target_id, msg.slots));
        }
    }

    // The last surviving zone's blocks, across every slot its groups declare.
    if let Some(zone) = last_closed {
        if committed.contains(&zone.lsn) && !zone_blocks_decode(sal_reader, &zone, family_lsns) {
            committed.remove(&zone.lsn);
        }
    }
    Ok(committed)
}

/// Whether every block of `zone` that this walk would apply decodes.
///
/// "Decodes" is not "carries rows": a push whose rows all land on one worker
/// leaves the other slots a control block and a schema block with no data block,
/// which reaches the replay closures as `Ok` with no batch and is correctly a
/// no-op.
fn zone_blocks_decode(sal_reader: &SalReader, zone: &Zone, family_lsns: &HashMap<i64, u64>) -> bool {
    let lsn = zone.lsn;
    for &(base, target_id, slots) in &zone.groups {
        if !family_lsns.contains_key(&(target_id as i64)) {
            continue;
        }
        for w in 0..slots {
            if let Some(bytes) = sal_reader.slot_at(base, w) {
                if let Err(e) = ipc::decode_wire(bytes) {
                    gnitz_warn!(
                        "SAL replay: last committed zone lsn={lsn} is torn (offset={base} slot={w} \
                         target={target_id}: {e}); skipping it whole"
                    );
                    return false;
                }
            }
        }
    }
    true
}

/// Pass 2: walk the SAL applying every committed group whose LSN is
/// in `family_lsns` and exceeds the recorded flushed LSN. The closure
/// receives the group's raw wire bytes and may filter by flag (e.g. master
/// applies only FLAG_DDL_SYNC, worker only FLAG_PUSH). It decodes what it
/// keeps: `ipc::decode_wire` copies the whole batch out of the SAL, so decoding
/// ahead of the flag test would pay for every group the caller drops.
fn recover_sal<F>(
    sal_reader: &SalReader,
    epoch: u32,
    catalog: &mut CatalogEngine,
    committed: &HashSet<u64>,
    family_lsns: &HashMap<i64, u64>,
    mut apply: F,
) -> Result<u32, String>
where
    F: FnMut(&mut CatalogEngine, &crate::runtime::sal::SalMessage, &[u8]) -> Result<bool, String>,
{
    let mut applied: u32 = 0;
    for step in sal_walk(sal_reader, epoch) {
        // Pass 1 already reached its verdict on every corrupt offset: it either
        // failed the boot or established that nothing committed lies there.
        let WalkStep::Group(msg) = step else { continue };
        if !committed.contains(&msg.lsn) {
            continue;
        }

        let tid = msg.target_id as i64;
        let flushed = match family_lsns.get(&tid) {
            Some(&lsn) => lsn,
            None => continue,
        };
        if msg.lsn <= flushed {
            continue;
        }

        let data = match msg.wire_data {
            Some(d) => d,
            None => continue,
        };
        if apply(catalog, &msg, data)? {
            applied += 1;
        }
    }
    Ok(applied)
}

/// Master pre-fork system-table replay. Builds the system-table family
/// map from the flushed LSNs, then walks the SAL via
/// `recover_sal`. The closure ingests every committed FLAG_DDL_SYNC
/// batch addressed to a system table — orphan COL_TAB rows from a
/// crashed DDL are skipped because their zone never closed.
///
/// Returns the walk's epoch, which is the floor the next writer epoch and the
/// workers' initial `expected_epoch` are taken from.
fn recover_system_tables_from_sal(sal_ptr: *const u8, catalog: &mut CatalogEngine) -> Result<u32, String> {
    let sal_reader = SalReader::for_walk(sal_ptr, 0, sal_mmap_size());
    let all_lsns = catalog.collect_all_flushed_lsns();
    let family_lsns: HashMap<i64, u64> = all_lsns
        .into_iter()
        .filter(|&(tid, _)| tid > 0 && tid < FIRST_USER_TABLE_ID)
        .collect();

    // Derived once, before either pass: on a boot whose offset-0 header is
    // damaged this costs a full-ring sweep, and both passes must anchor on the
    // same answer anyway.
    let epoch = sal_reader.walk_epoch();
    let committed = collect_committed_lsns(&sal_reader, epoch, &family_lsns)?;
    let replayed = recover_sal(
        &sal_reader,
        epoch,
        catalog,
        &committed,
        &family_lsns,
        |cat, msg, data| {
            if msg.flags & FLAG_DDL_SYNC == 0 {
                return Ok(false);
            }
            // Pass 1 already demoted the last zone if it was torn, so anything that
            // fails here has a durable committed zone behind it — a hole, not a crash
            // artifact. Dropping it silently would lose an ACKed, fdatasync'd DDL.
            let decoded = ipc::decode_wire(data).map_err(|e| {
                format!(
                    "SAL replay: corrupt block at offset={} lsn={} target={}: {e}",
                    msg.base, msg.lsn, msg.target_id
                )
            })?;
            let batch = match decoded.data_batch {
                Some(b) if b.count > 0 => b,
                _ => return Ok(false),
            };
            // §3.2: route through `ddl_sync` (→ `apply_local`), NOT `ingest_to_family`
            // (→ `submit` → `precheck_family`). These rows are master-validated by
            // definition, and the Column precheck arm would false-reject a replayed
            // DROP TABLE cascade's independent COL `-1` groups (unpaired on a
            // still-registered owner) — the swallowed `.is_ok()` would then silently
            // orphan the column band. `apply_local` still fires hooks, so a DROP NOT
            // NULL pair in the un-checkpointed SAL tail actually swaps the base
            // FixedIntNonnull → Generic here, before the post-ALTER NULL pushes
            // replay. A replay error aborts boot, before the SAL sentinel is reset: a
            // silently-swallowed swap failure would leave the base under the stale
            // comparator → NULL-vs-0 corruption. Idempotent on a re-run (cascade
            // `count>0` guard + net-dead register gate make a re-scan of an
            // already-retracted band a no-op, not an Err), so failing here never aborts
            // a legitimate boot.
            cat.ddl_sync(msg.target_id as i64, batch).map(|()| true).map_err(|e| {
                format!(
                    "SAL system-table recovery apply failed (table_id={}, lsn={}): {e}",
                    msg.target_id, msg.lsn
                )
            })
        },
    )?;

    if replayed > 0 {
        boot_log(&format!("SAL system table recovery: replayed {replayed} entries\n"));
    }
    Ok(epoch)
}

/// `GNITZ_INJECT_RECOVERY_PANIC=<stage>`: panic when recovery reaches the named
/// stage, so the crash-window E2E tests can cut boot at a precise point.
static RECOVERY_PANIC: Seam = Seam::new("GNITZ_INJECT_RECOVERY_PANIC");

/// `GNITZ_INJECT_BOOT_FLUSH_ERROR` / `GNITZ_INJECT_SYS_FLUSH_ERROR`: fail the
/// base-table or system-table boot flush, whose swallowed failure would destroy
/// the replayed DDL's only durable copy at the SAL reset.
static BOOT_FLUSH_ERROR: Seam = Seam::new("GNITZ_INJECT_BOOT_FLUSH_ERROR");
static SYS_FLUSH_ERROR: Seam = Seam::new("GNITZ_INJECT_SYS_FLUSH_ERROR");

fn inject_recovery_panic(stage: &str) {
    if RECOVERY_PANIC.at(stage) {
        panic!("injected recovery panic at {stage}");
    }
}

/// Base tables feeding ≥1 view, sorted for a reproducible drive order. The ONE
/// definition of the recovery sweep set: each worker buffers exactly these
/// tables' effective deltas during SAL replay, and the master's post-reset tick
/// sweep drains exactly these — same function, so the swept set equals the
/// buffered set by construction (no leak, no gap).
fn swept_base_tables(catalog: &mut CatalogEngine) -> Vec<i64> {
    let view_ids = catalog.dag.view_ids();
    catalog.dag.base_tables_reachable_from(view_ids)
}

/// Per-worker post-fork user-table replay for `rank` of `num_workers`. The apply
/// closure decodes each FLAG_PUSH group's batch and applies it through the
/// PK-enforcement path (`ingest_returning_effective`, the exact call
/// `handle_push` makes) so retractions cancel correctly, and — for every base
/// table feeding ≥1
/// view — buffers the returned effective delta into the returned map. That map
/// seeds the worker's `pending_deltas`; the master's post-reset recovery tick
/// sweep drains it into the views. Viewless bases ingest-and-discard (nothing to
/// drive), so their tail never leaks into the sweep.
///
/// The tail is written pre-sliced, one slot per worker of the boot that wrote it.
/// At the launched count this rank's own slot already holds exactly its rows; at
/// any other count it holds neither all of them nor only them, so every written
/// slot is walked and each partitioned group re-cut for the launched topology.
fn recover_from_sal(
    sal_ptr: *const u8,
    rank: u32,
    num_workers: u32,
    walk_epoch: u32,
    catalog: &mut CatalogEngine,
) -> Result<HashMap<i64, Batch>, String> {
    let all_lsns = catalog.collect_all_flushed_lsns();
    let family_lsns: HashMap<i64, u64> = all_lsns
        .into_iter()
        .filter(|&(tid, _)| tid >= FIRST_USER_TABLE_ID)
        .collect();

    let buffered_bases: HashSet<i64> = swept_base_tables(catalog).into_iter().collect();

    // `unwrap_or` covers the two cases the probe answers `None` for, and neither
    // can lose rows: a zero prefix at offset 0 (fresh or reset ring — the walk
    // ends at its first step), and a corrupt first header (pass 1 has already
    // either failed the boot or established that nothing committed lies behind
    // it). Either way it picks a re-slice mode for a tail with nothing to replay.
    let written = unsafe { sal_tail_slot_count(sal_ptr, sal_mmap_size() as u64) }.unwrap_or(num_workers);
    let reslice = written != num_workers;
    let slots = if reslice { 0..written } else { rank..rank + 1 };

    // The walk runs at `walk_epoch`, which the master derived once pre-fork.
    let readers: Vec<SalReader> = slots
        .map(|s| SalReader::for_walk(sal_ptr, s, sal_mmap_size()))
        .collect();
    // Group headers are shared by every slot and pass 1 validates a zone across
    // all of them, so its verdict is the same whichever reader is handed to it.
    let committed = collect_committed_lsns(&readers[0], walk_epoch, &family_lsns)?;

    let mut pending: HashMap<i64, Batch> = HashMap::new();
    let mut replayed: u32 = 0;
    for (idx, sal_reader) in readers.iter().enumerate() {
        replayed += recover_sal(
            sal_reader,
            walk_epoch,
            catalog,
            &committed,
            &family_lsns,
            |cat, msg, data| {
                if msg.flags & FLAG_PUSH == 0 {
                    return Ok(false);
                }
                let tid = msg.target_id as i64;
                // The catalog's schema, not the wire's: only the catalog stamps the
                // `replicated` bit the branch below reads. `SchemaDescriptor` is `Copy`,
                // so this holds no borrow on `cat` across the `&mut cat` ingest.
                let schema = cat
                    .get_schema_desc(tid)
                    .ok_or_else(|| format!("SAL replay: no schema for table_id={tid} (lsn={})", msg.lsn))?;
                // Broadcast, not sliced: every slot holds the whole copy, so a second
                // reader would re-ingest the same rows and add their weights again. Never
                // re-sliced either — this worker needs the full copy, not a share of it.
                let replicated = schema.placement().is_replicated();
                if replicated && idx > 0 {
                    return Ok(false);
                }
                // Pass 1 demoted the last zone if it was torn, so a failure here has a
                // durable committed zone behind it — a hole, not a crash artifact.
                // Dropping it silently would lose an ACKed, fdatasync'd transaction.
                let decoded = ipc::decode_wire(data).map_err(|e| {
                    format!(
                        "SAL replay: corrupt block at offset={} lsn={} target={}: {e}",
                        msg.base, msg.lsn, msg.target_id
                    )
                })?;
                let mut batch = match decoded.data_batch {
                    Some(b) if b.count > 0 => b,
                    _ => return Ok(false),
                };
                // The one place an old-width batch enters the engine. A pre-ALTER
                // `FLAG_PUSH` frame decodes against its own embedded schema
                // block, but the catalog is already at its final width here (the
                // master applied every catalog SAL entry pre-fork, and workers
                // replay pushes only). Widen ahead of the reslice, which rebuilds
                // through `Batch::from_indexed_rows(&mb, …, &schema)` and would
                // read `schema`'s payload columns past the narrower source's
                // regions; the non-reslice arm's `enforce_unique_pk` accumulator
                // is already shaped by the table schema. One site covers both.
                let in_schema = decoded
                    .schema
                    .as_ref()
                    .ok_or_else(|| format!("SAL replay: push frame carries no schema (lsn={})", msg.lsn))?;
                if in_schema.num_payload_cols() < schema.num_payload_cols() {
                    batch = batch.widened_with_null_tail(in_schema, &schema);
                }
                let owned = if reslice && !replicated {
                    // Re-cut with the write path's own router, so what survives is exactly
                    // what the master would have written to this rank's slot: same
                    // distribution-prefix hash, same partition→worker map.
                    let mb = batch.as_mem_batch();
                    crate::runtime::master::scatter::with_worker_indices(&batch, &schema, num_workers as usize, |wi| {
                        Batch::from_indexed_rows(&mb, &wi[rank as usize], &[], &schema)
                    })
                } else {
                    batch
                };
                if owned.count == 0 {
                    return Ok(false);
                }
                // The error rides the startup ACK: the master fails boot BEFORE zeroing
                // the SAL sentinel, so the replayed data's only durable copy survives. A
                // swallowed error here would zero the sentinel and orphan the un-applied
                // committed data.
                let effective = cat.ingest_returning_effective(tid, owned).map_err(|e| {
                    format!(
                        "SAL replay apply failed (table_id={}, lsn={}): {e}",
                        msg.target_id, msg.lsn
                    )
                })?;
                // Buffer the effective delta for the sweep; viewless bases discard it
                // (nothing to drive).
                if buffered_bases.contains(&tid) {
                    buffer_pending_delta(&mut pending, tid, effective);
                }
                Ok(true)
            },
        )?;
    }

    if replayed > 0 {
        boot_log(&format!("SAL recovery: replayed {replayed} blocks\n"));
    }
    Ok(pending)
}

/// Worker-boot catalog recovery. The order below is required:
///
/// 1. Rebuild every secondary index slice-local, replacing the fork-inherited
///    full parent-dir copy, BEFORE SAL replay — replay projects the committed
///    unflushed tail into each index exactly once (`ingest_store_and_indices`),
///    so a rebuild afterwards would double-count every replayed row.
/// 2. Replay unflushed push data from the SAL.
/// 3. Flush the replayed rows to shards before accepting requests: reset_sal()
///    resets the write cursor to 0, so a second crash before a checkpoint would
///    overwrite SAL entries and make replayed data unreachable (the SAL walk
///    stops at the first partially-overwritten group).
///
/// The Err rides the startup ACK (see worker.run): a failed boot must abort
/// before the master zeroes the SAL sentinel, or the replayed rows' only
/// durable copy is destroyed.
fn worker_boot_recovery(
    catalog: &mut CatalogEngine,
    sal_ptr: *const u8,
    rank: u32,
    num_workers: u32,
    walk_epoch: u32,
) -> Result<HashMap<i64, Batch>, String> {
    catalog
        .backfill_all_indexes()
        .map_err(|e| format!("boot index backfill failed: {e}"))?;
    let pending_deltas = recover_from_sal(sal_ptr, rank, num_workers, walk_epoch, catalog)?;
    // Keep the boot flush: the non-windowed recovery resets the SAL before the
    // master-driven tick sweep, so the replayed base rows must be shard-durable
    // first — else the reset would drop acknowledged tail data.
    for tid in catalog.iter_user_table_ids() {
        catalog
            .flush_family(tid)
            .map_err(|e| format!("boot flush of table {tid} failed: {e}"))?;
    }
    if BOOT_FLUSH_ERROR.armed() {
        return Err("injected boot flush fault".to_string());
    }
    inject_recovery_panic("bootflush");
    Ok(pending_deltas)
}

// ---------------------------------------------------------------------------
// Recovery tick sweep + invalid-view rebuild
// ---------------------------------------------------------------------------

/// Apply the un-checkpointed SAL tail — buffered as `pending_deltas` during
/// replay — to every view via one master-driven blocking tick per reachable base,
/// on the freshly-reset SAL. Each tick drains that source's buffered delta through
/// `handle_tick` → `evaluate_dag`; empty-buffer sources still tick so exchange
/// views stay in lockstep. The sweep drives ALL views: resumed (valid) views are
/// extended state-exactly (single-source-per-epoch reduces the whole-tail drive to
/// a live batched push); invalid views are harmlessly polluted and reset+rebuilt in
/// step-4. The transitive verdict guarantees no valid view reads an invalid one.
fn recovery_tick_sweep(catalog: &mut CatalogEngine, dispatcher: &MasterDispatcher) -> Result<(), String> {
    for src in swept_base_tables(catalog) {
        dispatcher.drain_tick_blocking(src)?;
    }
    Ok(())
}

/// Step-4: rebuild only the views the boot verdict rejected, through the same
/// dependency-ordered driver a live CREATE VIEW uses. The worker resets a view's
/// output partitions + operator scratch on the FIRST backfill command it receives
/// (gated on its COW-inherited `invalid_views` set), then fills. The driver is
/// view-scoped, so a resumed sibling's loaded shards are never re-derived and
/// double-counted.
fn rebuild_invalid_views(catalog: &mut CatalogEngine, dispatcher: &MasterDispatcher) -> Result<(), String> {
    let invalid: Vec<i64> = catalog.invalid_views.iter().copied().collect();
    // Resume-vs-rebuild marker (asserted by the "no backfill on clean restart"
    // E2E): 0 ⇒ every view resumed from its checkpoint.
    boot_log(&format!("recovery: rebuilding {} invalid view(s)\n", invalid.len()));
    dispatcher.backfill_views_in_dep_order(&invalid)
}

// ---------------------------------------------------------------------------
// Server main entry point
// ---------------------------------------------------------------------------

/// TLS listener request from the CLI: the address to bind, optional operator
/// cert/key PEM paths (a self-signed dev cert is minted when absent), the
/// optional client-auth CA (enables required mTLS), the
/// `--allow-unauthenticated` escape hatch, and the global connection cap.
pub struct TlsCli {
    pub listen: std::net::SocketAddr,
    pub cert_key: Option<(String, String)>,
    pub client_ca: Option<String>,
    pub allow_unauthenticated: bool,
    pub max_conns: u32,
}

/// Single entry point for the entire server bootstrap: opens the catalog,
/// allocates the shared IPC regions, forks the workers, runs recovery, and
/// enters the executor event loop.
///
/// Returns 0 on clean exit, non-zero on error.
pub fn server_main(
    data_dir: &str,
    socket_path: &str,
    num_workers: u32,
    log_level: u32,
    tls_cli: Option<TlsCli>,
) -> i32 {
    match run_server(data_dir, socket_path, num_workers, log_level, tls_cli) {
        Ok(rc) => rc,
        Err(e) => {
            gnitz_error!("{e}");
            1
        }
    }
}

/// Every shared region and descriptor the master and its forked workers use to
/// talk to each other.
///
/// Nothing here is reclaimed on error: the only response to a failed boot is to
/// exit the process, which returns all of it to the kernel.
struct SharedIpc {
    sal_fd: i32,
    sal_ptr: *mut u8,
    w2m_ptrs: Vec<*mut u8>,
    w2m_fds: Vec<i32>,
    m2w_efds: Vec<i32>,
}

/// Open and map the SAL, one W2M ring per worker, and the M2W eventfds.
fn acquire_shared_ipc(data_dir: &str, nw: usize) -> Result<SharedIpc, String> {
    // A fresh SAL file reads all-zero through O_CREAT + fallocate, which is
    // exactly the empty-SAL state recovery expects.
    let sal_fd = {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o644)
            .open(format!("{data_dir}/wal.sal"))
            .map(std::os::fd::IntoRawFd::into_raw_fd)
            .map_err(|e| format!("failed to open SAL file: {e}"))?
    };
    posix_io::try_set_nocow(sal_fd);
    // `Reserved`: the SAL is a real file, and reserving its blocks now is what
    // keeps a later write from failing for want of disk space.
    let sal_ptr = posix_io::map_shared_sized(sal_fd, sal_mmap_size(), posix_io::Backing::Reserved)
        .map_err(|e| format!("failed to map SAL ({} bytes): {e}", sal_mmap_size()))?;

    let mut w2m_ptrs: Vec<*mut u8> = Vec::with_capacity(nw);
    let mut w2m_fds: Vec<i32> = Vec::with_capacity(nw);
    for w in 0..nw {
        let wfd = posix_io::memfd_create(format!("w2m_{w}").as_bytes());
        if wfd < 0 {
            return Err(format!("memfd_create for W{w} failed"));
        }
        // `Sized`, not `Reserved`: a memfd's pages are RAM charged on first
        // touch, so reserving would commit the whole region per worker.
        let wptr = posix_io::map_shared_sized(wfd, W2M_REGION_SIZE, posix_io::Backing::Sized)
            .map_err(|e| format!("failed to map W2M region for W{w}: {e}"))?;
        // Hint THP backing for the W2M region (memfd/shmem backing).
        // Requires: echo advise > /sys/kernel/mm/transparent_hugepage/shmem_enabled
        // If shmem_enabled remains "never", this call is silently inert — no harm.
        posix_io::madvise_hugepage(wptr, W2M_REGION_SIZE);
        // Initialize the SPSC ring header (cursors at HEADER_SIZE,
        // capacity = full region).
        unsafe {
            w2m_ring::init_region(wptr, W2M_REGION_SIZE as u64);
        }
        w2m_ptrs.push(wptr);
        w2m_fds.push(wfd);
    }

    // M2W eventfds (master→worker signaling; W2M wakes via futex).
    let mut m2w_efds: Vec<i32> = Vec::with_capacity(nw);
    for w in 0..nw {
        let efd = posix_io::eventfd_create();
        if efd < 0 {
            return Err(format!("eventfd_create for W{w} failed"));
        }
        m2w_efds.push(efd);
    }

    Ok(SharedIpc {
        sal_fd,
        sal_ptr,
        w2m_ptrs,
        w2m_fds,
        m2w_efds,
    })
}

/// The forked child's whole life: latch its rank, redirect its logs to
/// `worker_N.log`, trim to its partition range, recover, and run the worker
/// loop. Never returns — the worker exits via `libc::_exit`.
#[allow(clippy::too_many_arguments)]
fn run_worker_child(
    w: usize,
    num_workers: u32,
    log_level: u32,
    data_dir: &str,
    master_pid: i32,
    catalog_ptr: *mut CatalogEngine,
    ipc: &SharedIpc,
    boot_epoch: u32,
) -> ! {
    // Die immediately if the master exits for any reason.  The getppid() check
    // in sal_reader.wait() is a belt-and-suspenders fallback; this closes the
    // ~30s polling gap.
    unsafe { libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL, 0, 0, 0) };
    // Re-check: parent may have died in the fork→prctl window.
    if unsafe { libc::getppid() } != master_pid {
        unsafe { libc::_exit(0) };
    }

    // Latch this worker's rank/count (and its Worker role) before ANY catalog
    // work below (trim, rehome, index rebuild, SAL replay, view backfill): every
    // plan compiled during boot must see this process's real (rank,
    // num_workers) and its index tables must home into the per-rank subdir.
    // Single owner of the rank — no longer set in WorkerProcess::new.
    crate::foundation::worker_ctx::set_worker_rank(w as u32, num_workers);

    // Redirect stdout/stderr to worker log file
    {
        use std::os::unix::fs::OpenOptionsExt;
        if let Ok(f) = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o644)
            .open(format!("{data_dir}/worker_{w}.log"))
        {
            let log_fd = std::os::fd::IntoRawFd::into_raw_fd(f);
            unsafe {
                libc::dup2(log_fd, 1);
                libc::dup2(log_fd, 2);
                libc::close(log_fd);
            }
        }
    }

    // Re-tag logging as this worker before any boot work, so every line the
    // recovery below emits carries `W{w}` rather than the inherited master tag.
    crate::foundation::log::init(log_level, format!("W{w}").as_bytes());

    // Close M2W eventfds of OTHER workers (W2M uses futex, no fd).
    for (j, &efd) in ipc.m2w_efds.iter().enumerate() {
        if j != w {
            unsafe {
                libc::close(efd);
            }
        }
    }

    let catalog = unsafe { &mut *catalog_ptr };

    // Set active partition range
    let (part_start, part_end) = partition_range(w as u32, num_workers);
    catalog.set_active_partitions(part_start, part_end);
    catalog.trim_worker_partitions(part_start, part_end);

    // `boot_epoch` is what the master's `boot_reset` will set, so the live drain
    // accepts exactly the groups written after the reset.
    let sal_reader = SalReader::new(
        ipc.sal_ptr as *const u8,
        w as u32,
        sal_mmap_size(),
        ipc.m2w_efds[w],
        boot_epoch,
    );
    let w2m_writer = W2mWriter::new(ipc.w2m_ptrs[w], W2M_REGION_SIZE as u64);

    // Re-home inherited unhashed (replicated / replicated-derived) stores
    // from the pre-fork master's `rep_0` to THIS worker's own `rep_{w}` dir before
    // any flush — all workers share the data directory, so a fixed `rep_0` would
    // collide. The inherited store is empty; the pre-fork `reconcile_child_dirs`
    // already put this rank's checkpointed shards under `rep_{w}` (loaded on open)
    // and FLAG_PUSH replay adds the SAL tail.
    //
    // Then recover: rebuild indexes, replay the SAL tail (buffering effective base
    // deltas), boot-flush the replayed rows durable. The buffered deltas seed
    // `pending_deltas`; the master's post-reset tick sweep drives them into the
    // views. All view derivation moved to the master's sweep + step-4 rebuild — no
    // child-side view backfill.
    //
    // Either failure rides the startup ACK, which fails boot before the master
    // zeroes the SAL sentinel.
    let (pending_deltas, boot_err): (HashMap<i64, Batch>, Option<String>) = match catalog
        .rehome_unhashed_stores()
        .map_err(|e| format!("W{w} rehome unhashed stores failed: {e}"))
        .and_then(|()| worker_boot_recovery(catalog, ipc.sal_ptr as *const u8, w as u32, num_workers, boot_epoch - 1))
    {
        Ok(pd) => (pd, None),
        Err(e) => {
            // stderr is redirected to worker_N.log above.
            gnitz_error!("{e}");
            (HashMap::new(), Some(e))
        }
    };

    catalog.invalidate_all_plans();

    boot_log(&format!(
        "Worker {} (pid {}) partitions [{}, {})\n",
        w,
        unsafe { libc::getpid() },
        part_start,
        part_end,
    ));

    let mut worker = WorkerProcess::new(master_pid, catalog_ptr, sal_reader, w2m_writer, pending_deltas);
    let rc = worker.run(boot_err);

    unsafe {
        libc::_exit(rc);
    }
}

fn run_server(
    data_dir: &str,
    socket_path: &str,
    num_workers: u32,
    log_level: u32,
    tls_cli: Option<TlsCli>,
) -> Result<i32, String> {
    // Latch the Master role before any catalog work: the pre-fork replay hooks
    // in CatalogEngine::open must see Master so they skip the index backfill
    // their forked children rebuild slice-local.
    crate::foundation::worker_ctx::set_master_role();

    // Raise fd limit (partition directories + shard files)
    posix_io::raise_fd_limit(65536);

    gnitz_info!("Opening database at {}", data_dir);

    let catalog = CatalogEngine::open(data_dir).map_err(|e| format!("failed to open catalog: {e}"))?;
    let catalog_ptr = Box::into_raw(Box::new(catalog));

    let nw = num_workers as usize;
    boot_log(&format!("Starting {num_workers} workers\n"));
    boot_log(&format!(
        "Worker logs: {}/worker_N.log (N=0..{})\n",
        data_dir,
        num_workers - 1
    ));

    let ipc = acquire_shared_ipc(data_dir, nw)?;

    // --- System table SAL recovery (before forking workers) ---
    //
    // The recovered walk epoch is the epoch floor for this boot: the master's
    // `boot_reset` and every worker's initial `expected_epoch` start one above it,
    // so a previous boot's leftover in the ring always carries a strictly lower
    // epoch than anything this boot writes.
    let boot_epoch;
    {
        let catalog = unsafe { &mut *catalog_ptr };
        boot_epoch = recover_system_tables_from_sal(ipc.sal_ptr as *const u8, catalog)? + 1;
        // Abort before forking workers and long before the SAL reset: the
        // replayed DDL lives only in master memory until this flush makes it
        // durable, so a swallowed failure followed by the SAL reset destroys
        // its only durable copy (and gc_orphan_directories would later delete
        // the now-catalog-less entities' flushed shards).
        catalog.flush_all_system_tables()?;
        if SYS_FLUSH_ERROR.armed() {
            return Err("injected system table flush fault".to_string());
        }

        // Reclaim table/view/index directories whose DROP committed but whose
        // deferred deletion was lost to a crash before the next checkpoint.
        // Runs only now that both shard replay and SAL replay have populated
        // dag.tables, so a SAL-committed-but-unflushed CREATE is not mistaken
        // for an orphan.
        catalog.gc_orphan_directories();

        // Bring the relation child directories into this boot's worker count and
        // seed any replicated base table copy it needs. Must run after SAL replay
        // (so `dag.tables` is complete and dropped subtrees are already gone) and
        // before the fork, since each worker's `rehome_unhashed_stores`
        // opens `rep_{rank}` unconditionally.
        catalog
            .reconcile_child_dirs(num_workers)
            .map_err(|e| format!("child-dir reconciliation failed: {e}"))?;
    }

    // --- Boot invalid-view verdict + recovery-start generation bump ---
    //
    // Both pre-fork, while the master's active range is still full: the verdict's
    // manifest peeks see every partition, and the durable generation advance is
    // COW-inherited by every worker (and is durable long before the parent resets
    // the SAL after worker readiness).
    {
        let catalog = unsafe { &mut *catalog_ptr };
        // Per-view resume-vs-rebuild verdict against the checkpointed manifests
        // (generation + topology + transitive source validity). Reads
        // `worker_ctx::committed_generation()` (the recovered G), so it runs BEFORE
        // the recovery-start bump advances the durable generation.
        let invalid = catalog.compute_invalid_views(num_workers);
        catalog.invalid_views = invalid;

        // Durably advance the checkpoint generation G → G+1 without publishing to
        // worker_ctx, closing the reset→boot_checkpoint crash window.
        catalog
            .recovery_start_generation_bump()
            .map_err(|e| format!("recovery-start generation bump failed: {e}"))?;
        inject_recovery_panic("genbump");

        // Name the replay path the workers will take, so the boot record shows
        // whether the tail was re-sliced. Pre-fork, so this still reaches the
        // master's stdout (the children redirect fd 1 to their own logs), and read
        // from the same place the workers read it.
        if let Some(written) = unsafe { sal_tail_slot_count(ipc.sal_ptr as *const u8, sal_mmap_size() as u64) } {
            if written != num_workers {
                boot_log(&format!(
                    "SAL tail written by {written} workers, launching {num_workers}\n"
                ));
            }
        }
    }

    // Log fd assignments
    boot_log(&format!("SAL fd={}\n", ipc.sal_fd));
    for w in 0..nw {
        boot_log(&format!(
            "W{} m2w_efd={} w2m_fd={}\n",
            w, ipc.m2w_efds[w], ipc.w2m_fds[w]
        ));
    }

    let master_pid = unsafe { libc::getpid() };

    // --- Fork workers ---
    let mut worker_pids: Vec<i32> = vec![0; nw];
    for (w, slot) in worker_pids.iter_mut().enumerate() {
        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err("fork failed".to_string());
        }
        if pid == 0 {
            run_worker_child(
                w,
                num_workers,
                log_level,
                data_dir,
                master_pid,
                catalog_ptr,
                &ipc,
                boot_epoch,
            );
        }
        *slot = pid;
    }

    // --- Parent process ---
    let catalog = unsafe { &mut *catalog_ptr };
    catalog.close_user_table_partitions();
    catalog.set_active_partitions(0, 0);

    let sal_writer = SalWriter::new(ipc.sal_ptr, ipc.sal_fd, sal_mmap_size() as u64, ipc.m2w_efds.clone());
    let w2m_receiver = std::rc::Rc::new(W2mReceiver::new(ipc.w2m_ptrs.clone()));

    let dispatcher = MasterDispatcher::new(nw, worker_pids.clone(), catalog_ptr, sal_writer, w2m_receiver);
    let dispatcher_rc = std::rc::Rc::new(dispatcher);

    // Wait for all workers to complete recovery and signal readiness
    let dispatcher = &*dispatcher_rc;
    dispatcher
        .collect_acks()
        .map_err(|e| format!("Error collecting worker acks: {e}"))?;

    // Reset SAL for fresh use (all workers have recovered)
    dispatcher.reset_sal(boot_epoch);

    inject_recovery_panic("reset");

    // Recovery tick sweep: apply the un-checkpointed SAL tail (buffered as
    // pending_deltas during replay) to every view via one master-driven tick per
    // reachable base, on the freshly-reset SAL. Resumed views are extended;
    // invalid views are polluted (reset+rebuilt next).
    recovery_tick_sweep(catalog, dispatcher).map_err(|e| format!("recovery tick sweep failed: {e}"))?;

    inject_recovery_panic("sweep");

    // Step-4: reset (on the workers) and rebuild only the invalid views.
    rebuild_invalid_views(catalog, dispatcher).map_err(|e| format!("invalid-view rebuild failed: {e}"))?;

    inject_recovery_panic("backfill");

    // Boot-end checkpoint: record the launched topology, bump the generation
    // (G+1 → G+2), and durably checkpoint the resumed + rebuilt view state (base +
    // ephemeral rounds) before the socket opens, so a clean restart resumes from
    // it. No drain — recovery already drained everything and no pushes are admitted
    // yet.
    dispatcher
        .boot_checkpoint(num_workers)
        .map_err(|e| format!("boot checkpoint failed: {e}"))?;

    // Create server socket and run executor
    gnitz_info!("Listening on {}", socket_path);
    let server_fd = posix_io::server_create(socket_path)
        .map(std::os::fd::IntoRawFd::into_raw_fd)
        .map_err(|e| format!("failed to create server socket: {e}"))?;

    // Optional TLS listener — after the worker fork (same position as
    // `server_create`), so no fd inheritance. TLS was explicitly requested
    // via --tls-listen, so any setup failure aborts boot loudly (silently
    // continuing AF_UNIX-only would be surprising).
    let tls_init = match tls_cli {
        Some(cli) => Some(setup_tls_listener(data_dir, &cli)?),
        None => None,
    };
    boot_log("GnitzDB ready\n");

    Ok(ServerExecutor::run(
        catalog_ptr,
        std::rc::Rc::clone(&dispatcher_rc),
        server_fd,
        tls_init,
    ))
}

/// Build the rustls server config (minting + persisting the public dev cert
/// when no PEM pair is given), bind the TCP listener, and publish the bound
/// address to `<data_dir>/tls_endpoint` (atomically: tmp + rename, so
/// existence implies complete content).
///
/// **Bind refusal:** a non-loopback bind with neither `--tls-client-ca` nor
/// `--allow-unauthenticated` aborts boot — an unauthenticated public bind is
/// impossible by accident. `--allow-unauthenticated` is the single, loud
/// escape hatch; the CA enables required mTLS.
fn setup_tls_listener(data_dir: &str, cli: &TlsCli) -> Result<TlsListener, String> {
    // `is_loopback()` is the conservative test: `0.0.0.0`/`::` (bind-all),
    // IPv4-mapped `::ffff:127.0.0.1`, and any specific LAN/link-local IP are
    // all non-loopback → refused unless a CA or the escape hatch is present.
    if !cli.listen.ip().is_loopback() && cli.client_ca.is_none() && !cli.allow_unauthenticated {
        return Err(format!(
            "refusing to bind a non-loopback TLS listener {} without client authentication; \
             pass --tls-client-ca=PEM or --allow-unauthenticated",
            cli.listen,
        ));
    }

    let cert_key = cli.cert_key.as_ref().map(|(c, k)| (c.as_str(), k.as_str()));
    let (config, dev_pem) = crate::runtime::tls::config::server_crypto(cert_key, cli.client_ca.as_deref())?;
    if let Some(pem) = dev_pem {
        let path = format!("{data_dir}/tls_dev_cert.pem");
        std::fs::write(&path, pem).map_err(|e| format!("failed to write {path}: {e}"))?;
        gnitz_info!(
            "TLS: minted a self-signed dev certificate (identity is ephemeral, regenerated every boot); \
             public PEM at {path}"
        );
    }
    let listen_fd =
        posix_io::tcp_bind(&cli.listen).map_err(|e| format!("failed to bind TLS listener {}: {e}", cli.listen))?;
    let bound = posix_io::tcp_local_addr(listen_fd).unwrap_or(cli.listen);
    let endpoint_path = format!("{data_dir}/tls_endpoint");
    let tmp_path = format!("{endpoint_path}.tmp");
    std::fs::write(&tmp_path, format!("{bound}\n")).map_err(|e| format!("failed to write {tmp_path}: {e}"))?;
    std::fs::rename(&tmp_path, &endpoint_path).map_err(|e| format!("failed to publish {endpoint_path}: {e}"))?;
    gnitz_info!("Listening on tls://{}", bound);
    // A deliberately-unauthenticated non-loopback bind (escape hatch, no CA)
    // stays loud. With a CA the listener is authenticated — no warning.
    if !bound.ip().is_loopback() && cli.client_ca.is_none() {
        gnitz_warn!(
            "TLS listener bound to NON-LOOPBACK address {} with --allow-unauthenticated and NO client \
             authentication: anyone who can reach this port gets full DDL/DML/scan access. Prefer \
             --tls-client-ca=PEM (required mTLS). Note: even a loopback bind trusts every local UID.",
            bound,
        );
    }
    Ok(TlsListener {
        fd: listen_fd,
        cfg: config,
        max_conns: cli.max_conns,
    })
}

// ---------------------------------------------------------------------------
// Walk, resync, epoch and zone-span rules over hand-built logs: the group-header
// shapes are what recovery classifies, and the payloads are opaque (no family
// map), so the zone rules are isolated from block decoding. The end-to-end crash
// path is covered in `crates/gnitz-py/tests/test_crash_recovery.py`.
// ---------------------------------------------------------------------------

/// The committed set of a quiescent log, sorted — the shape the tests assert on.
#[cfg(test)]
fn committed_lsns(reader: &SalReader, families: &HashMap<i64, u64>) -> Result<Vec<u64>, String> {
    collect_committed_lsns(reader, reader.walk_epoch(), families).map(|set| {
        let mut v: Vec<u64> = set.into_iter().collect();
        v.sort_unstable();
        v
    })
}

#[cfg(test)]
mod walk_tests {
    use super::*;
    use crate::runtime::sal::{group_header_size, sal_write_group, SalReader, SalWriter, FLAG_TICK};
    use crate::test_support::{sweep_bit_flips, SharedRegion};

    const SIZE: usize = 1 << 20;

    /// A hand-built SAL: groups appended at a cursor the embedded `SalWriter`
    /// tracks, so the commit sentinels come from the production writer.
    struct Log {
        ptr: *mut u8,
        writer: SalWriter,
        epoch: u32,
    }

    impl Log {
        fn new(region: &SharedRegion, epoch: u32) -> Log {
            let ptr = region.ptr();
            let writer = SalWriter::new(ptr, -1, SIZE as u64, vec![-1]);
            writer.reset(0, epoch);
            Log { ptr, writer, epoch }
        }

        /// One ordinary group with a 64-byte slot per worker. Returns its base.
        fn group(&self, target: u32, lsn: u64, flags: u32) -> u64 {
            let base = self.writer.cursor();
            let payload = [0u8; 64];
            let next =
                unsafe { sal_write_group(self.ptr, base, target, lsn, flags, self.epoch, SIZE as u64, &[&payload]) }
                    .expect("group fits");
            self.writer.reset(next, self.epoch);
            base
        }

        /// A closed zone: one group per entry of `targets`, the first carrying
        /// `FLAG_ZONE_START`, then the commit sentinel. Returns every base, the
        /// sentinel's last.
        fn zone(&self, lsn: u64, targets: &[u32]) -> Vec<u64> {
            let mut bases: Vec<u64> = targets
                .iter()
                .enumerate()
                .map(|(i, &t)| self.group(t, lsn, FLAG_DDL_SYNC | if i == 0 { FLAG_ZONE_START } else { 0 }))
                .collect();
            bases.push(self.writer.cursor());
            self.writer.write_commit_sentinel(lsn).expect("sentinel fits");
            bases
        }

        /// The ephemeral command group the committer fires between zones: `lsn = 0`,
        /// in no zone's span.
        fn command(&self) -> u64 {
            self.group(9, 0, FLAG_TICK)
        }

        fn reader(&self) -> SalReader {
            SalReader::for_walk(self.ptr as *const u8, 0, SIZE)
        }

        /// Flip one bit of the header at `base`.
        fn damage_header(&self, base: u64) {
            unsafe { *self.ptr.add(base as usize + 8) ^= 1 };
        }

        /// The state unordered mmap writeback leaves when a group's prefix and
        /// header fall on different pages: the prefix persists, the header does not.
        fn zero_header(&self, base: u64, slots: usize) {
            unsafe { std::ptr::write_bytes(self.ptr.add(base as usize + 8), 0, group_header_size(slots)) };
        }

        /// The publication prefix word at `base`, as a mutable byte slice. The
        /// mapping is shared, so a reader built from the same region sees the edit.
        fn prefix_bytes(&mut self, base: u64) -> &mut [u8] {
            unsafe { std::slice::from_raw_parts_mut(self.ptr.add(base as usize), 8) }
        }
    }

    /// The `(lsn, target_id)` pairs a walk reads, and the offsets it reports
    /// corrupt.
    fn walk(reader: &SalReader) -> (Vec<(u64, u32)>, Vec<u64>) {
        let mut groups = Vec::new();
        let mut corrupt = Vec::new();
        for step in sal_walk(reader, reader.walk_epoch()) {
            match step {
                WalkStep::Group(m) => groups.push((m.lsn, m.target_id)),
                WalkStep::Corrupt(off) => corrupt.push(off),
            }
        }
        (groups, corrupt)
    }

    fn committed(reader: &SalReader) -> Result<Vec<u64>, String> {
        super::committed_lsns(reader, &HashMap::new())
    }

    // -----------------------------------------------------------------------
    // The publication prefix
    // -----------------------------------------------------------------------

    /// No bit of the prefix word changes a walk: the stride comes from the
    /// digested directory and the generation from the header's own epoch.
    #[test]
    fn the_whole_prefix_word_is_neutralised() {
        let region = SharedRegion::new(SIZE);
        let mut log = Log::new(&region, 3);
        log.group(11, 101, FLAG_DDL_SYNC);
        let middle = log.group(22, 102, FLAG_DDL_SYNC);
        log.group(33, 103, FLAG_DDL_SYNC);

        let reader = log.reader();
        let clean = walk(&reader);
        assert_eq!(clean.0, vec![(101, 11), (102, 22), (103, 33)]);
        assert!(clean.1.is_empty());

        sweep_bit_flips(log.prefix_bytes(middle), 0..8, |byte, bit, _| {
            assert_eq!(walk(&reader), clean, "prefix byte {byte} bit {bit} changed the walk");
        });
    }

    /// Presence is the whole prefix word, not its `payload_size` half. A commit
    /// sentinel's payload is one set bit, so a low-half-only test would let a
    /// single flip stop the walk before that transaction's sentinel.
    #[test]
    fn a_sentinels_prefix_is_not_single_bit_zeroable() {
        let region = SharedRegion::new(SIZE);
        let mut log = Log::new(&region, 1);
        let bases = log.zone(7, &[11]);
        let sentinel = *bases.last().unwrap();
        log.group(33, 0, FLAG_TICK);
        // A 4-slot empty group, whose payload is 64 — also one bit.
        let empty4 = log.writer.cursor();
        unsafe {
            sal_write_group(log.ptr, empty4, 44, 0, FLAG_TICK, 1, SIZE as u64, &[&[], &[], &[], &[]])
                .expect("group fits")
        };

        let reader = log.reader();
        let clean = walk(&reader);
        assert_eq!(clean.0.len(), 4, "zone group, sentinel, tick, 4-slot empty group");
        assert_eq!(committed(&reader).unwrap(), vec![7]);

        for &base in &[sentinel, empty4] {
            sweep_bit_flips(log.prefix_bytes(base), 0..8, |byte, bit, _| {
                assert_eq!(walk(&reader), clean, "prefix {base}+{byte} bit {bit} changed the walk");
                assert_eq!(
                    committed(&reader).unwrap(),
                    vec![7],
                    "prefix {base}+{byte} bit {bit} lost the zone"
                );
            });
        }
    }

    // -----------------------------------------------------------------------
    // The walk epoch
    // -----------------------------------------------------------------------

    /// A previous epoch's leftover past the new frontier is absent, not damage —
    /// the ring wrap is an ordinary end-of-log.
    #[test]
    fn a_previous_epochs_leftover_ends_the_walk() {
        let region = SharedRegion::new(SIZE);
        {
            let old = Log::new(&region, 1);
            old.group(11, 1, FLAG_DDL_SYNC);
            old.group(22, 2, FLAG_DDL_SYNC);
            old.group(33, 3, FLAG_DDL_SYNC);
        }
        // A shorter epoch-2 log over the same bytes: one group, so epoch 1's
        // second and third groups survive past the new frontier.
        let new = Log::new(&region, 2);
        new.group(44, 9, FLAG_DDL_SYNC);

        let reader = new.reader();
        assert_eq!(reader.walk_epoch(), 2, "offset 0's header anchors the walk");
        let (groups, corrupt) = walk(&reader);
        assert_eq!(groups, vec![(9, 44)], "the walk ends at the epoch-1 leftover");
        assert!(corrupt.is_empty(), "a leftover is absent, not corrupt");
    }

    /// The walk epoch comes from group 0's digested header, not its prefix copy,
    /// so no single flip there can make every later comparison fail.
    #[test]
    fn group_zeros_prefix_epoch_is_not_a_single_point_of_failure() {
        let region = SharedRegion::new(SIZE);
        let mut log = Log::new(&region, 5);
        log.group(11, 101, FLAG_DDL_SYNC);
        log.group(22, 102, FLAG_DDL_SYNC);
        let reader = log.reader();
        let clean = walk(&reader);
        assert_eq!(clean.0.len(), 2);

        // Bytes 4..8 of the prefix word are its epoch copy.
        sweep_bit_flips(log.prefix_bytes(0), 4..8, |byte, bit, _| {
            assert_eq!(
                reader.walk_epoch(),
                5,
                "epoch byte {byte} bit {bit} moved the walk epoch"
            );
            assert_eq!(walk(&reader), clean, "epoch byte {byte} bit {bit} changed the walk");
        });
    }

    /// With offset 0's header damaged, the anchor is the *maximum* epoch in the
    /// ring — not the first valid header found, which under a page revert can be
    /// an older leftover at a low offset.
    #[test]
    fn the_walk_epoch_survives_a_damaged_offset_zero() {
        let region = SharedRegion::new(SIZE);
        // A previous epoch's leftover further in, left behind by a shorter later
        // pass, plus the live epoch-4 log over the front.
        {
            let old = Log::new(&region, 2);
            for i in 0..8 {
                old.group(90 + i, 1, FLAG_DDL_SYNC);
            }
        }
        let log = Log::new(&region, 4);
        log.group(11, 101, FLAG_DDL_SYNC);
        log.group(22, 102, FLAG_DDL_SYNC);
        log.group(33, 103, FLAG_DDL_SYNC);
        log.damage_header(0);

        let reader = log.reader();
        assert_eq!(
            reader.walk_epoch(),
            4,
            "the maximum epoch in the ring, not a leftover's"
        );
        let (groups, corrupt) = walk(&reader);
        assert_eq!(corrupt, vec![0], "offset 0 is the damage");
        assert_eq!(
            groups,
            vec![(102, 22), (103, 33)],
            "the resync must recover every committed group behind the damage"
        );
    }

    /// Only a damaged offset 0 selects the ring-wide sweep. A fresh ring and a
    /// reset ring must both answer from offset 0 alone — otherwise every boot of
    /// every fresh database pays a full-mapping hash sweep.
    #[test]
    fn only_a_damaged_offset_zero_takes_a_sweep() {
        // A fresh all-zero mapping: no header, zero prefix, epoch floor 0.
        let region = SharedRegion::new(SIZE);
        assert_eq!(Log::new(&region, 0).reader().walk_epoch(), 0);

        // A reset ring: `boot_reset`/`checkpoint_reset` zero offset 0's prefix but
        // leave its header, so the probe answers and the floor carries forward.
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 7);
        log.group(11, 1, FLAG_DDL_SYNC);
        log.writer.boot_reset(8);
        assert_eq!(
            log.reader().walk_epoch(),
            7,
            "a reset ring's floor comes from the surviving header, not the zeroed prefix"
        );
        assert_eq!(log.writer.epoch(), 8, "the next writer epoch is one above the floor");

        // A damaged offset 0 with a non-zero prefix is the one shape that sweeps.
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 6);
        log.group(11, 1, FLAG_DDL_SYNC);
        log.group(22, 2, FLAG_DDL_SYNC);
        log.damage_header(0);
        assert_eq!(log.reader().walk_epoch(), 6);
    }

    // -----------------------------------------------------------------------
    // The zone-span rule
    // -----------------------------------------------------------------------

    /// (a) Damage inside a middle committed zone is a hole: a later committed
    /// zone is durable behind it, so the boot must fail naming the offset.
    #[test]
    fn damage_in_a_middle_zone_fails_the_boot() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11, 12]);
        let second = log.zone(2, &[21, 22]);
        log.zone(3, &[31, 32]);
        log.damage_header(second[1]);

        let reader = log.reader();
        let err = committed(&reader).expect_err("a hole must fail the boot");
        assert!(
            err.contains(&format!("offset={}", second[1])),
            "the error names the offset: {err}"
        );
        assert!(err.contains("lsn=2"), "and the zone: {err}");
    }

    /// (b) Damage inside the last zone demotes it; every earlier zone still
    /// applies.
    #[test]
    fn damage_in_the_last_zone_demotes_it() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11, 12]);
        let last = log.zone(2, &[21, 22]);
        log.damage_header(last[1]);

        assert_eq!(committed(&log.reader()).unwrap(), vec![1]);
    }

    /// (c) Damage in an `lsn = 0` command group between two zones costs nothing,
    /// and the committer fires one of these ticks after every push.
    #[test]
    fn damage_in_a_command_group_between_zones_costs_nothing() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11]);
        let tick = log.command();
        log.zone(2, &[21]);
        log.zone(3, &[31]);
        log.damage_header(tick);

        assert_eq!(
            committed(&log.reader()).unwrap(),
            vec![1, 2, 3],
            "every committed zone must survive rot in a tick group"
        );
    }

    /// (d) Damage in an unclosed tail zone costs nothing — the zone was never
    /// promised to anyone.
    #[test]
    fn damage_in_an_unclosed_tail_zone_costs_nothing() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11]);
        log.group(21, 2, FLAG_DDL_SYNC | FLAG_ZONE_START);
        let torn = log.group(22, 2, FLAG_DDL_SYNC);
        log.damage_header(torn);

        assert_eq!(committed(&log.reader()).unwrap(), vec![1]);
    }

    /// (e) A sentinel arriving with no zone open lost that zone's first group;
    /// applying its siblings would half-apply an atomic unit.
    #[test]
    fn a_zone_that_lost_its_first_group_fails_the_boot() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11]);
        let second = log.zone(2, &[21, 22]);
        log.zone(3, &[31]);
        log.damage_header(second[0]);

        let err = committed(&log.reader()).expect_err("a lost head group must fail the boot");
        assert!(err.contains("lsn=2"), "{err}");
    }

    /// A zone with a readable start, no sentinel, and a group after it did close:
    /// its sentinel was destroyed, not omitted.
    #[test]
    fn a_lost_sentinel_is_not_a_zone_that_never_closed() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        let first = log.zone(1, &[11]);
        log.zone(2, &[21]);
        // Destroy the first zone's sentinel.
        log.damage_header(*first.last().unwrap());

        let err = committed(&log.reader()).expect_err("a destroyed sentinel must fail the boot");
        assert!(err.contains("lost its commit sentinel"), "{err}");
    }

    // -----------------------------------------------------------------------
    // The resync scan
    // -----------------------------------------------------------------------

    /// The resync scan skips what the walk stops on: zero words, and an intact
    /// previous-epoch header below the frontier (the page-revert shape). Both must
    /// still reach the zone's sentinel and report the hole.
    #[test]
    fn the_resync_scan_sweeps_past_zero_runs_and_leftovers() {
        for leftover in [false, true] {
            let region = SharedRegion::new(SIZE);
            if leftover {
                // A previous epoch's group parked where the live log will straddle
                // it, so the scan meets an intact epoch-1 header below the frontier.
                let old = Log::new(&region, 1);
                for _ in 0..40 {
                    old.group(90, 1, FLAG_DDL_SYNC);
                }
            }
            let log = Log::new(&region, 4);
            log.zone(1, &[11]);
            let second_start = log.group(21, 2, FLAG_DDL_SYNC | FLAG_ZONE_START);
            log.group(22, 2, FLAG_DDL_SYNC);
            log.writer.write_commit_sentinel(2).expect("sentinel fits");
            log.zone(3, &[31]);
            // Destroy the zone's first group's header entirely, leaving its prefix:
            // the walk must resync across whatever follows.
            log.zero_header(second_start, 1);

            let err = committed(&log.reader()).expect_err("the hole must be reported (leftover={leftover})");
            assert!(err.contains("lsn=2"), "leftover={leftover}: {err}");
        }
    }

    /// A group's prefix and header can fall on different pages, and unordered
    /// writeback can persist the prefix and lose the header. Without the resync
    /// scan that would be terminal and throw the committed prefix away.
    #[test]
    fn a_torn_header_page_does_not_cost_the_committed_prefix() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11]);
        log.zone(2, &[21]);
        // An unclosed tail zone whose head group's header was lost.
        let torn = log.group(31, 3, FLAG_DDL_SYNC | FLAG_ZONE_START);
        log.zero_header(torn, 1);

        assert_eq!(
            committed(&log.reader()).unwrap(),
            vec![1, 2],
            "every committed zone before the torn page must survive"
        );
    }

    /// Both passes run the same walk, so a zone pass 1 commits is a zone pass 2
    /// reaches. A pass 2 that stopped at the damage instead would drop zone 2's
    /// groups on the floor while reporting it committed — an ACKed transaction
    /// lost in silence.
    #[test]
    fn both_passes_see_the_same_groups_past_damage() {
        let region = SharedRegion::new(SIZE);
        let log = Log::new(&region, 1);
        log.zone(1, &[11]);
        let tick = log.command();
        log.zone(2, &[21]);
        log.damage_header(tick);

        let reader = log.reader();
        let (groups, corrupt) = walk(&reader);
        assert_eq!(corrupt, vec![tick]);
        assert_eq!(
            groups,
            vec![(1, 11), (1, 0), (2, 21), (2, 0)],
            "zone 1 + its sentinel, then zone 2 + its sentinel; the tick is the damage"
        );
        assert_eq!(committed(&reader).unwrap(), vec![1, 2]);
    }
}

// ---------------------------------------------------------------------------
// The zone predicate over real wire slots: pass 1 decode-validates the last
// committed zone across every slot its groups declare, and "validate" means the
// block decodes, not that it carries rows.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod zone_block_tests {
    use super::*;
    use crate::runtime::master::scatter::with_commit_indices;
    use crate::runtime::sal::{sal_write_group, SalReader, SalWriter, FLAG_TICK};
    use crate::runtime::wire::build_schema_wire_block;
    use crate::storage::compute_wire_props;
    use crate::test_support::{make_batch, make_schema_u64_i64, SharedRegion};

    const SIZE: usize = 1 << 20;
    const NW: usize = 4;
    const TID: u32 = 16;

    /// A push zone over `NW` workers: one `scatter_wire_group` per entry of
    /// `targets` (the first opening the zone), then the commit sentinel. Rows are
    /// PK-partitioned, so most slots are `ctrl + schema` with no data block —
    /// exactly the shape a partitioned push leaves. Returns each group's base.
    fn push_zone(writer: &SalWriter, lsn: u64, targets: &[u32]) -> Vec<u64> {
        let schema = make_schema_u64_i64();
        let batch = make_batch(&schema, &[(1, 1, 10), (2, 1, 20)]);
        let block = build_schema_wire_block(&schema, TID);
        let props = compute_wire_props(&schema);
        let req_ids: Vec<u64> = (0..NW as u64).collect();
        let mut bases = Vec::new();
        for (i, &t) in targets.iter().enumerate() {
            bases.push(writer.cursor());
            let flags = FLAG_PUSH | if i == 0 { FLAG_ZONE_START } else { 0 };
            with_commit_indices(&batch, &schema, NW, |wi| {
                writer
                    .scatter_wire_group(
                        &batch,
                        wi,
                        &schema,
                        t,
                        lsn,
                        flags,
                        0,
                        0,
                        &req_ids,
                        Some(block.as_slice()),
                        Some(props),
                    )
                    .expect("group fits")
            });
        }
        writer.write_commit_sentinel(lsn).expect("sentinel fits");
        bases
    }

    fn families(targets: &[u32]) -> HashMap<i64, u64> {
        targets.iter().map(|&t| (t as i64, 0u64)).collect()
    }

    fn reader_at(region: &SharedRegion, w: u32) -> SalReader {
        SalReader::for_walk(region.ptr() as *const u8, w, SIZE)
    }

    /// Corrupt one byte of slot `w` of the group at `base`, past the control
    /// block's own header so the block checksum is what catches it.
    fn damage_slot(region: &SharedRegion, base: u64, w: u32) {
        let slot = reader_at(region, w).slot_at(base, w).expect("slot carries bytes");
        let off = (slot.as_ptr() as usize) - (region.ptr() as usize);
        unsafe { *region.ptr().add(off + gnitz_wire::WAL_HEADER_SIZE) ^= 0xFF };
    }

    /// The highest slot of the group at `base` that carries rows.
    fn a_slot_with_rows(region: &SharedRegion, base: u64) -> u32 {
        (0..NW as u32)
            .rev()
            .find(|&w| reader_at(region, w).slot_at(base, w).is_some())
            .expect("some slot carries rows")
    }

    /// A push whose rows do not reach every worker leaves the remaining slots a
    /// control block and a schema block with no data block. Pass 1 must read that
    /// as "decodes", not as "carries rows" — otherwise every partitioned push
    /// demotes its own zone.
    #[test]
    fn a_row_less_slot_is_not_a_damaged_one() {
        let region = SharedRegion::new(SIZE);
        let writer = SalWriter::new(region.ptr(), -1, SIZE as u64, vec![-1; NW]);
        writer.reset(0, 1);
        let bases = push_zone(&writer, 5, &[TID]);

        // The fixture must actually leave a row-less slot, or it proves nothing.
        let row_less = (0..NW as u32)
            .filter(|&w| {
                let slot = reader_at(&region, w)
                    .slot_at(bases[0], w)
                    .expect("every slot is written");
                ipc::decode_wire(slot).expect("slot decodes").data_batch.is_none()
            })
            .count();
        assert!(row_less > 0, "the fixture must leave at least one slot row-less");

        assert_eq!(
            committed_lsns(&reader_at(&region, 0), &families(&[TID])).unwrap(),
            vec![5]
        );
    }

    /// The demotion verdict is global: rot in worker 3's slot must demote the
    /// zone on every walk, worker 0's included. A per-slot verdict would apply the
    /// zone on three workers and skip it on one.
    #[test]
    fn the_demotion_is_global_across_slots() {
        for damage_offset_zero in [false, true] {
            let region = SharedRegion::new(SIZE);
            let writer = SalWriter::new(region.ptr(), -1, SIZE as u64, vec![-1; NW]);
            writer.reset(0, 1);
            // A leading command group, so offset 0 is not the zone's own head and
            // can be damaged independently: with it gone there is no tail-wide slot
            // count to read, and only each group's own count is available.
            let next = unsafe {
                sal_write_group(region.ptr(), 0, 9, 0, FLAG_TICK, 1, SIZE as u64, &[&[0u8; 8]]).expect("fits")
            };
            writer.reset(next, 1);
            let bases = push_zone(&writer, 5, &[TID]);

            // Rot the highest slot carrying rows, so the reader below (worker 0)
            // is not the damaged one.
            let victim = a_slot_with_rows(&region, bases[0]);
            damage_slot(&region, bases[0], victim);
            if damage_offset_zero {
                unsafe { *region.ptr().add(8) ^= 1 };
            }

            for w in 0..NW as u32 {
                assert!(
                    committed_lsns(&reader_at(&region, w), &families(&[TID]))
                        .unwrap()
                        .is_empty(),
                    "worker {w} must demote the zone rot lives in slot {victim} of \
                     (offset0_damaged={damage_offset_zero})"
                );
            }
        }
    }

    /// A torn last zone is skipped whole, not partially: pass 1 drops the LSN, so
    /// pass 2 never applies one family's group while skipping its sibling's.
    #[test]
    fn a_torn_last_zone_is_skipped_whole() {
        let region = SharedRegion::new(SIZE);
        let writer = SalWriter::new(region.ptr(), -1, SIZE as u64, vec![-1; NW]);
        writer.reset(0, 1);
        push_zone(&writer, 4, &[TID]);
        let last = push_zone(&writer, 5, &[TID, TID + 1]);
        // Rot the FIRST family of the last zone.
        damage_slot(&region, last[0], a_slot_with_rows(&region, last[0]));

        assert_eq!(
            committed_lsns(&reader_at(&region, 0), &families(&[TID, TID + 1])).unwrap(),
            vec![4],
            "the torn zone must be dropped whole, and the durable one before it kept"
        );
    }
}
