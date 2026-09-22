//! CPU affinity: every worker on a physical core of its own, the master on
//! everything left over. `GNITZ_CPU_AFFINITY=0` turns it off.
//!
//! The placement sees only `sched_getaffinity`, so two servers on one host pin
//! onto each other unless each runs in its own cgroup cpuset.

use std::collections::BTreeMap;

const CPU_ROOT: &str = "/sys/devices/system/cpu";
const NODE_ROOT: &str = "/sys/devices/system/node";

/// One physical core's allowed logical CPUs, ascending.
type Core = Vec<u32>;

/// Every process is pinned or none is: a roaming master would land on pinned
/// workers that cannot move away.
pub struct Pinning {
    master: Vec<u32>,
    /// Indexed by worker rank.
    workers: Vec<Core>,
}

impl Pinning {
    /// Confine the calling process to worker `w`'s core.
    pub fn enter_worker(&self, w: usize) -> Result<(), String> {
        pin_self(&format!("W{w}"), &self.workers[w])
    }

    /// Confine the calling process to the CPUs no worker owns.
    pub fn enter_master(&self) -> Result<(), String> {
        pin_self("master", &self.master)
    }
}

/// Plan the placement, log it, and pin the calling (master) process to its
/// share. `Ok(None)`: nothing is pinned.
pub fn pin_master(workers: usize) -> Result<Option<Pinning>, String> {
    if !gnitz_foundation::env::env_flag("GNITZ_CPU_AFFINITY", true) {
        gnitz_note!("affinity: not applied (disabled by GNITZ_CPU_AFFINITY)");
        return Ok(None);
    }
    let order = read_order(&allowed_cpus());
    let usable = order.len();
    let Some(p) = assign(order, workers) else {
        gnitz_note!("affinity: not applied ({workers} workers, {usable} usable cores)");
        return Ok(None);
    };
    for (w, core) in p.workers.iter().enumerate() {
        gnitz_note!("affinity: W{w} {}", fmt_cpu_list(core));
    }
    gnitz_note!("affinity: master {}", fmt_cpu_list(&p.master));
    p.enter_master()?;
    Ok(Some(p))
}

/// Parse a sysfs CPU list — `"0-3,8,12-15"` — into ascending CPU ids; a
/// malformed one yields none rather than a wrong set.
fn parse_cpu_list(s: &str) -> Vec<u32> {
    let range = |part: &str| -> Option<std::ops::RangeInclusive<u32>> {
        let (a, b) = part.split_once('-').unwrap_or((part, part));
        let (a, b) = (a.trim().parse::<u32>().ok()?, b.trim().parse::<u32>().ok()?);
        (a <= b).then_some(a..=b)
    };
    let fields: Option<Vec<_>> = s.trim().split(',').filter(|p| !p.is_empty()).map(range).collect();
    let mut out: Vec<u32> = fields.unwrap_or_default().into_iter().flatten().collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// An unreadable file reads as an empty list.
fn read_cpu_list(path: &str) -> Vec<u32> {
    std::fs::read_to_string(path)
        .map(|s| parse_cpu_list(&s))
        .unwrap_or_default()
}

/// `handout_order` over this machine's sysfs.
fn read_order(allowed: &[u32]) -> Vec<Core> {
    let node_cpus: Vec<Vec<u32>> = read_cpu_list(&format!("{NODE_ROOT}/online"))
        .iter()
        .map(|n| read_cpu_list(&format!("{NODE_ROOT}/node{n}/cpulist")))
        .collect();
    handout_order(allowed, &node_cpus, |c| {
        read_cpu_list(&format!("{CPU_ROOT}/cpu{c}/topology/thread_siblings_list"))
    })
}

/// The cores of `allowed` in the order workers take them: one per NUMA node
/// per round, spreading the workers over the nodes.
fn handout_order(allowed: &[u32], node_cpus: &[Vec<u32>], siblings: impl Fn(u32) -> Vec<u32>) -> Vec<Core> {
    let unnamed = node_cpus.len();
    let node_of = |c: u32| {
        node_cpus
            .iter()
            .position(|n| n.binary_search(&c).is_ok())
            .unwrap_or(unnamed)
    };
    let mut nodes: BTreeMap<usize, BTreeMap<u32, Core>> = BTreeMap::new();
    for &c in allowed {
        let first_sibling = siblings(c).first().copied().unwrap_or(c);
        nodes
            .entry(node_of(c))
            .or_default()
            .entry(first_sibling)
            .or_default()
            .push(c);
    }
    let mut nodes: Vec<_> = nodes.into_values().map(BTreeMap::into_values).collect();
    let mut order = Vec::new();
    loop {
        let before = order.len();
        order.extend(nodes.iter_mut().filter_map(Iterator::next));
        if order.len() == before {
            return order;
        }
    }
}

/// One core per worker and every remaining CPU to the master, or `None` if the
/// master would be left without a core. A worker gets a whole core because SMT
/// siblings share L1 and L2.
fn assign(order: Vec<Core>, workers: usize) -> Option<Pinning> {
    if workers >= order.len() {
        return None;
    }
    let mut order = order.into_iter();
    let cores: Vec<Core> = order.by_ref().take(workers).collect();
    let mut master: Vec<u32> = order.flatten().collect();
    master.sort_unstable();
    Some(Pinning { master, workers: cores })
}

/// The CPUs this process may run on: its affinity mask, which the kernel
/// already narrows to the cgroup cpuset and to active CPUs.
fn allowed_cpus() -> Vec<u32> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    if unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) } != 0 {
        return Vec::new();
    }
    (0..libc::CPU_SETSIZE as usize)
        .filter(|&c| unsafe { libc::CPU_ISSET(c, &set) })
        .map(|c| c as u32)
        .collect()
}

fn pin_self(who: &str, cpus: &[u32]) -> Result<(), String> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    for &c in cpus {
        unsafe { libc::CPU_SET(c as usize, &mut set) };
    }
    match unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) } {
        0 => Ok(()),
        _ => Err(format!(
            "affinity: pinning {who} to CPUs {} refused: {}",
            fmt_cpu_list(cpus),
            std::io::Error::last_os_error()
        )),
    }
}

/// `[0, 1, 2, 3, 8]` → `"0-3,8"`, the sysfs spelling `parse_cpu_list` reads.
fn fmt_cpu_list(cpus: &[u32]) -> String {
    use std::fmt::Write;
    let mut out = String::new();
    for run in cpus.chunk_by(|a, b| a + 1 == *b) {
        let sep = if out.is_empty() { "" } else { "," };
        let (lo, hi) = (run[0], run[run.len() - 1]);
        let _ = if lo == hi {
            write!(out, "{sep}{lo}")
        } else {
            write!(out, "{sep}{lo}-{hi}")
        };
    }
    out
}

#[cfg(test)]
#[path = "tests/affinity.rs"]
mod tests;
