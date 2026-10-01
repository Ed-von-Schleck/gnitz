//! What a data directory holds on disk, read from its files alone: no relation
//! is opened and no schema is needed, so it reads a directory a server is
//! running on as well as one at rest.

use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;

use super::dirs::{parse_relation_dir_name, relations_dir, subdir_names, ChildAddr, ChildKind};
use crate::storage::shard_files;
use gnitz_zset::repr::ShardDirectory;

/// One store of one relation, summed over its slots, at one LSM level; `None`
/// is the shards no manifest names — written and not yet published, or
/// superseded and not yet unlinked.
type StoreLevel = (u64, String, Option<u64>);

/// One region role under one encoding: `(role, encoding name)`.
type RegionKey = (String, &'static str);

#[derive(Default)]
struct ShardTotals {
    files: u64,
    /// The files among them that hold skeleton rows.
    skeletons: u64,
    rows: u64,
    bytes: u64,
    regions: BTreeMap<RegionKey, u64>,
}

#[derive(Default)]
struct FileTotals {
    files: u64,
    bytes: u64,
    allocated: u64,
}

impl FileTotals {
    fn add(&mut self, meta: &fs::Metadata) {
        self.files += 1;
        self.bytes += meta.len();
        self.allocated += meta.blocks() * 512;
    }
}

/// A data directory's bytes by what holds them. `Display` is the report.
#[derive(Default)]
pub struct DiskUsage {
    shards: BTreeMap<StoreLevel, ShardTotals>,
    /// Bytes the filesystem allocated to the shards counted above.
    shard_allocated: u64,
    /// Shard files that are further names of one already counted.
    linked: FileTotals,
    /// Files named as shards whose descriptive prefix does not read as one.
    unreadable: FileTotals,
    /// Stores holding a shard another store holds byte for byte, with the
    /// files and bytes past one copy.
    identical: BTreeMap<Vec<(u64, String)>, FileTotals>,
    /// Every other file, by name.
    other: BTreeMap<String, FileTotals>,
}

impl DiskUsage {
    /// Shard bytes, each file counted once however many names it has.
    pub fn shard_bytes(&self) -> u64 {
        self.shards.values().map(|t| t.bytes).sum()
    }
}

/// Walk `base_dir`. A file that vanishes mid-walk is skipped: a running server
/// unlinks shards as it compacts.
pub fn disk_usage(base_dir: &str) -> Result<DiskUsage, String> {
    let mut usage = DiskUsage::default();
    let mut inodes = HashSet::new();
    // (length, body digest, rows) -> the stores holding such a shard.
    let mut bodies: BTreeMap<(u64, u64, usize), Vec<(u64, String)>> = BTreeMap::new();
    let root = relations_dir(base_dir);
    let listed = |dir: &str| subdir_names(dir).map_err(|e| format!("list '{dir}': {e}"));
    let mut shards = Vec::new();
    for rel in listed(&root)? {
        let Some(id) = parse_relation_dir_name(&rel) else {
            continue;
        };
        let rel_dir = format!("{root}/{rel}");
        // A system family's one store is its relation directory itself.
        let mut stores = vec![(rel_dir.clone(), ChildKind::Rows.label())];
        for child in listed(&rel_dir)? {
            if let Some(addr) = ChildAddr::parse(&child) {
                stores.push((format!("{rel_dir}/{child}"), addr.kind.label()));
            }
        }
        for (dir, label) in stores {
            let Ok(files) = shard_files(&dir) else { continue };
            shards.extend(
                files
                    .into_iter()
                    .map(|(path, level)| (level.is_none(), path, level, id, label.clone())),
            );
        }
    }
    // A file with several names is counted under one a manifest publishes.
    shards.sort();
    for (_, path, level, id, label) in shards {
        let Ok(meta) = fs::metadata(&path) else { continue };
        if !inodes.insert((meta.dev(), meta.ino())) {
            usage.linked.add(&meta);
            continue;
        }
        let Ok(shard) = ShardDirectory::read(&path) else {
            usage.unreadable.add(&meta);
            continue;
        };
        usage.shard_allocated += meta.blocks() * 512;
        bodies
            .entry((meta.len(), shard.body_checksum, shard.rows))
            .or_default()
            .push((id, label.clone()));
        let totals = usage.shards.entry((id, label, level)).or_default();
        totals.files += 1;
        totals.skeletons += u64::from(shard.skeleton);
        totals.rows += shard.rows as u64;
        totals.bytes += meta.len();
        for (role, encoding, size) in shard.regions {
            *totals.regions.entry((role, encoding)).or_default() += size as u64;
        }
    }
    for ((len, _, _), mut holders) in bodies {
        let copies = holders.len() as u64;
        holders.sort();
        holders.dedup();
        if copies > 1 {
            let t = usage.identical.entry(holders).or_default();
            t.files += copies - 1;
            t.bytes += len * (copies - 1);
        }
    }
    other_files(Path::new(base_dir), &inodes, &mut usage.other).map_err(|e| format!("walk '{base_dir}': {e}"))?;
    Ok(usage)
}

/// Every file under `dir` that is none of `shards`, the shard files by inode.
fn other_files(
    dir: &Path,
    shards: &HashSet<(u64, u64)>,
    out: &mut BTreeMap<String, FileTotals>,
) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            other_files(&entry.path(), shards, out)?;
        } else if !shards.contains(&(meta.dev(), meta.ino())) {
            let name = entry.file_name().to_string_lossy().into_owned();
            out.entry(name).or_default().add(&meta);
        }
    }
    Ok(())
}

impl fmt::Display for DiskUsage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{:>8} {:<24} {:>5} {:>6} {:>9} {:>11} {:>13} {:>7}  regions, share of the store's bytes",
            "relation", "store", "level", "files", "skeleton", "rows", "bytes", "B/row"
        )?;
        for ((id, store, level), t) in &self.shards {
            let level = level.map_or("-".to_string(), |l| format!("L{l}"));
            let mut regions: Vec<(&RegionKey, &u64)> = t.regions.iter().collect();
            regions.sort_by_key(|&(_, &bytes)| std::cmp::Reverse(bytes));
            let shares: Vec<String> = regions
                .iter()
                .map(|((role, encoding), &bytes)| (role, encoding, bytes * 100 / t.bytes.max(1)))
                .filter(|&(_, _, pct)| pct > 0)
                .map(|(role, encoding, pct)| format!("{role} {encoding} {pct}%"))
                .collect();
            writeln!(
                f,
                "{id:>8} {store:<24} {level:>5} {:>6} {:>9} {:>11} {:>13} {:>7.1}  {}",
                t.files,
                t.skeletons,
                t.rows,
                t.bytes,
                t.bytes as f64 / t.rows.max(1) as f64,
                shares.join(", ")
            )?;
        }
        let files: u64 = self.shards.values().map(|t| t.files).sum();
        writeln!(
            f,
            "shards: {files} files, {} bytes, {} allocated",
            self.shard_bytes(),
            self.shard_allocated
        )?;
        if self.linked.files > 0 {
            writeln!(
                f,
                "hard links: {} further names of shards counted above, {} bytes they do not occupy",
                self.linked.files, self.linked.bytes
            )?;
        }
        if self.unreadable.files > 0 {
            writeln!(
                f,
                "unreadable: {} files named as shards, {} bytes",
                self.unreadable.files, self.unreadable.bytes
            )?;
        }
        for (holders, t) in &self.identical {
            let holders: Vec<String> = holders.iter().map(|(id, store)| format!("{id} {store}")).collect();
            writeln!(
                f,
                "identical shards: {} files, {} bytes past one copy, held by {}",
                t.files,
                t.bytes,
                holders.join(" = ")
            )?;
        }
        for (name, t) in &self.other {
            writeln!(
                f,
                "other: {name}: {} files, {} bytes, {} allocated",
                t.files, t.bytes, t.allocated
            )?;
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "tests/disk_usage.rs"]
mod tests;
