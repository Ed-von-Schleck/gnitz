//! Boot-time relayout of a base relation's children onto the launched worker
//! count.
//!
//! A child `w{k}of{n}` holds the rows worker `k` of `n` owns, so a restart at a
//! different count moves data: the launched-count set is written beside a
//! complete source set, which is removed only once the target is durable.

use std::collections::BTreeSet;
use std::fs;

use super::dirs::{cluster_children, subdir_names};
use super::{ChildAddr, ChildKind, StoreConfig};
use crate::storage::{RecoverySource, Table, flush_barrier, link_stores, retire_stores};
use gnitz_foundation::gnitz_info;
use gnitz_trace::runs::{StorageError, from_runs};
use gnitz_zset::algebra::{Placement, ScatterPlan, Slot};
use gnitz_zset::repr::Batch;
use gnitz_zset::schema::SchemaDescriptor;

/// The worker count of the complete child set to relay onto `launched`; `None`
/// when nothing moves. A set missing a rank's manifest never finished a
/// checkpoint, so the SAL tail still holds its rows.
fn relay_source(rel_dir: &str, launched: u32) -> Result<Option<u32>, String> {
    let mut foreign = BTreeSet::new();
    for name in subdir_names(rel_dir)? {
        match ChildAddr::parse(&name) {
            Some(ChildAddr { kind: ChildKind::Rows, slot }) if slot.of != launched => {
                foreign.insert(slot.of);
            }
            Some(_) => {}
            None => {
                return Err(format!(
                    "{rel_dir}/{name} is in no child-directory grammar this build knows; \
                     it may hold rows written by an incompatible layout. Refusing to boot — \
                     delete the relation directory deliberately if the data is expendable."
                ));
            }
        }
    }
    let complete = |of: u32| -> Result<bool, String> {
        for m in cluster_children(ChildKind::Rows, of).map(|c| c.manifest(rel_dir)) {
            if !fs::exists(&m).map_err(|e| format!("repartition: {m}: {e}"))? {
                return Ok(false);
            }
        }
        Ok(true)
    };
    if foreign.is_empty() || complete(launched)? {
        return Ok(None);
    }
    // Any two complete sets hold the same rows: `reconcile_child_dirs` fails the
    // boot rather than leave a retired one behind.
    for of in foreign {
        if complete(of)? {
            return Ok(Some(of));
        }
    }
    Ok(None)
}

/// Bring `rel_dir`'s children onto `launched` workers, with stores tuned by
/// `config`.
pub(super) fn repartition_relation(
    rel_dir: &str,
    schema: &SchemaDescriptor,
    placement: Placement,
    launched: u32,
    config: &StoreConfig,
) -> Result<(), String> {
    let Some(source) = relay_source(rel_dir, launched)? else {
        return Ok(());
    };
    gnitz_info!("repartition: {} from {} to {} worker(s)", rel_dir, source, launched);
    let relay = || -> Result<(), StorageError> {
        // A crashed attempt's partial target would double every row it holds.
        remove_set(rel_dir, launched)?;
        if placement.is_replicated() {
            // Every child is a copy, so each target hard-links the replica
            // owner's published files, which no publish rewrites in place.
            let owner = ChildAddr {
                kind: ChildKind::Rows,
                slot: Slot::new(Placement::REPLICA_OWNER, source),
            }
            .dir(rel_dir);
            let pairs: Vec<(String, String)> = cluster_children(ChildKind::Rows, launched)
                .map(|target| (owner.clone(), target.dir(rel_dir)))
                .collect();
            link_stores(&pairs)?;
        } else {
            rewrite_targets(rel_dir, schema, placement, source, launched, config)?;
        }
        // Each target's first publish made its entry in `rel_dir` durable.
        remove_set(rel_dir, source)
    };
    relay().map_err(|e| format!("repartition {rel_dir} to {launched} worker(s): {e}"))
}

fn remove_set(rel_dir: &str, of: u32) -> Result<(), StorageError> {
    let set: Vec<String> = cluster_children(ChildKind::Rows, of).map(|c| c.dir(rel_dir)).collect();
    retire_stores(&set)
}

/// Key-routed relations: scatter the merged source set to its owners, cutting
/// each target into terminal runs of one RAM tier.
fn rewrite_targets(
    rel_dir: &str,
    schema: &SchemaDescriptor,
    placement: Placement,
    source: u32,
    launched: u32,
    config: &StoreConfig,
) -> Result<(), StorageError> {
    // The relay writes its copy's heaps plain: a compressed relation's store
    // codes them as its compactions rewrite them.
    let open = |of: u32| {
        cluster_children(ChildKind::Rows, of)
            .map(|c| {
                Table::new(
                    &c.dir(rel_dir),
                    *schema,
                    RecoverySource::SalReplay,
                    config.ram_tier_bytes,
                    None,
                    false,
                )
            })
            .collect::<Result<Vec<Table>, _>>()
    };
    let sources = open(source)?;
    for t in &sources {
        t.verify_shards()?;
    }
    let mut cursor = from_runs(sources.iter().flat_map(|t| t.runs()), *schema);
    let mut targets = open(launched)?;
    let mut buffers: Vec<Batch> = targets.iter().map(|_| Batch::empty_with_schema(schema)).collect();
    let plan = ScatterPlan::native(placement);
    let mut rows: Vec<Vec<u32>> = Vec::new();
    while let Some(chunk) = cursor.drain_chunk(config.scan_chunk_rows) {
        let slots = plan.route(&chunk.as_mem_batch(), &mut rows, launched as usize);
        for ((target, buffer), idx) in targets.iter_mut().zip(&mut buffers).zip(slots.iter()) {
            if idx.is_empty() {
                continue;
            }
            // Ascending subsets of one consolidated cursor, in cursor order.
            buffer.append_above(chunk.ascending_subset(idx));
            if buffer.total_bytes() >= config.ram_tier_bytes {
                target.append_terminal_run(&buffer.take())?;
            }
        }
    }
    for (target, buffer) in targets.iter_mut().zip(&mut buffers) {
        if !buffer.is_empty() {
            target.append_terminal_run(&buffer.take())?;
        }
    }
    flush_barrier(targets.iter_mut(), 0)
}

#[cfg(test)]
#[path = "tests/repartition.rs"]
mod tests;
