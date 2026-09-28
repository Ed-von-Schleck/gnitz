//! Boot-time relayout of a base relation's children onto the launched worker
//! count.
//!
//! A child's *contents* depend on the worker count — `w{k}of{n}` holds exactly
//! the rows `worker_for_pk(pk, n) == k` — so a restart at a different count has
//! to move data: the launched-count set is written beside a complete source set,
//! which is removed only once the target is durable, so a crash leaves the
//! source intact. Runs on the master pre-fork, before the relation's store opens.

use std::collections::BTreeSet;
use std::fs;

use gnitz_foundation::posix_io::fsync_dir;

use super::dirs::{cluster_children, subdir_names};
use super::{ChildAddr, ChildKind};
use crate::schema::{SchemaDescriptor, Slot};
use crate::storage::{
    flush_barrier, from_runs, link_store, retire_store, route_rows_by_pk, Batch, Layout, RecoverySource, StorageError,
    StoreBudgets, Table,
};

/// The worker count of the complete child set to relay onto `launched`; `None`
/// when nothing moves. A set missing a rank's manifest never finished a
/// checkpoint, so the SAL tail still holds its rows.
fn relay_source(rel_dir: &str, launched: u32) -> Result<Option<u32>, String> {
    let mut foreign = BTreeSet::new();
    let names = subdir_names(rel_dir).map_err(|e| format!("repartition: list {rel_dir}: {e}"))?;
    for name in names {
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
                ))
            }
        }
    }
    let complete = |of: u32| -> Result<bool, String> {
        for m in cluster_children(of).map(|c| c.manifest(rel_dir)) {
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

/// Bring `rel_dir`'s children onto `launched` workers. `ram_tier_bytes` and
/// `chunk_rows` are the registry's own store tuning.
pub(super) fn repartition_relation(
    rel_dir: &str,
    schema: &SchemaDescriptor,
    launched: u32,
    ram_tier_bytes: usize,
    chunk_rows: usize,
) -> Result<(), String> {
    let Some(source) = relay_source(rel_dir, launched)? else {
        return Ok(());
    };
    gnitz_info!("repartition: {} from {} to {} worker(s)", rel_dir, source, launched);
    relay(rel_dir, schema, source, launched, ram_tier_bytes, chunk_rows)
        .map_err(|e| format!("repartition {rel_dir} to {launched} worker(s): {e}"))
}

fn relay(
    rel_dir: &str,
    schema: &SchemaDescriptor,
    source: u32,
    launched: u32,
    ram_tier_bytes: usize,
    chunk_rows: usize,
) -> Result<(), StorageError> {
    // A crashed attempt's partial target would double every row it holds.
    remove_set(rel_dir, launched)?;
    if schema.placement().is_replicated() {
        link_targets(rel_dir, source, launched)?;
    } else {
        rewrite_targets(rel_dir, schema, source, launched, ram_tier_bytes, chunk_rows)?;
    }
    fsync_dir(rel_dir)?;
    remove_set(rel_dir, source)
}

fn remove_set(rel_dir: &str, of: u32) -> Result<(), StorageError> {
    cluster_children(of).try_for_each(|c| retire_store(&c.dir(rel_dir)))
}

/// Replicated relations: every child is a copy, so each target hard-links rank
/// 0's published files, which no publish rewrites in place.
fn link_targets(rel_dir: &str, source: u32, launched: u32) -> Result<(), StorageError> {
    let source_dir = ChildAddr {
        kind: ChildKind::Rows,
        slot: Slot::new(0, source),
    }
    .dir(rel_dir);
    cluster_children(launched).try_for_each(|target| link_store(&source_dir, &target.dir(rel_dir)))
}

/// Key-routed relations: scatter the merged source set to its owners, cutting
/// each target into terminal runs of one RAM tier.
fn rewrite_targets(
    rel_dir: &str,
    schema: &SchemaDescriptor,
    source: u32,
    launched: u32,
    ram_tier_bytes: usize,
    chunk_rows: usize,
) -> Result<(), StorageError> {
    let budgets = StoreBudgets::new(ram_tier_bytes);
    let open = |of: u32| {
        cluster_children(of)
            .map(|c| Table::new(&c.dir(rel_dir), *schema, RecoverySource::SalReplay, budgets))
            .collect::<Result<Vec<Table>, _>>()
    };
    let sources = open(source)?;
    for t in &sources {
        t.verify_shards()?;
    }
    let mut cursor = from_runs(sources.iter().flat_map(Table::runs), *schema, 0);
    let mut targets = open(launched)?;
    let mut buffers: Vec<Batch> = targets.iter().map(|_| Batch::empty_with_schema(schema)).collect();
    let mut rows: Vec<Vec<u32>> = Vec::new();
    while let Some(chunk) = cursor.drain_chunk(chunk_rows) {
        let slots = route_rows_by_pk(&chunk.as_mem_batch(), schema, &mut rows, launched as usize);
        for ((target, buffer), idx) in targets.iter_mut().zip(&mut buffers).zip(slots.iter()) {
            if idx.is_empty() {
                continue;
            }
            let slice = chunk.ascending_subset(idx);
            buffer.append_batch(&slice, 0, slice.count);
            if buffer.total_bytes() >= ram_tier_bytes {
                write_run(target, buffer)?;
            }
        }
    }
    for (target, buffer) in targets.iter_mut().zip(&mut buffers) {
        if buffer.count > 0 {
            write_run(target, buffer)?;
        }
    }
    flush_barrier(targets.iter_mut(), 0)
}

/// `run` is ascending subsets of one consolidated cursor, appended in cursor
/// order, so it is consolidated itself.
fn write_run(target: &mut Table, run: &mut Batch) -> Result<(), StorageError> {
    run.certify_layout(Layout::Consolidated);
    target.append_terminal_run(run)?;
    run.clear();
    Ok(())
}

#[cfg(test)]
#[path = "tests/repartition.rs"]
mod tests;
