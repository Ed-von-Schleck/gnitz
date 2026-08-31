//! CPU affinity: give every worker a physical core of its own and the master
//! everything left over.
//!
//! The placement is computed once by the master before it opens the catalog and
//! applied by each process to itself, so the sysfs walk happens once and a
//! forked child does one syscall and no I/O. `GNITZ_CPU_AFFINITY=0` turns it
//! off.
//!
//! The master pins itself before any io_uring ring exists, because a ring's
//! io-wq pool takes the mask its creating thread held and never revisits it —
//! a later `sched_setaffinity` on the reactor thread does not reach the
//! `iou-wrk` threads that every pre-ACK `fdatasync` is punted to. Each worker
//! therefore inherits the master's mask across the fork and narrows to its own
//! core; one that cannot widens back to every CPU the server may use.
//!
//! **The placement assumes this server owns its CPU set.** It is derived from
//! `sched_getaffinity` with no knowledge of other tenants, so two servers on
//! one host pin onto each other. Confining each to its own cgroup cpuset is
//! what makes that safe — `sched_getaffinity` then reports the cpuset, and the
//! placement covers only those CPUs.
//!
//! Placement is all-or-nothing: pinning the workers while the master roams is
//! worse than pinning nothing, because the master lands on top of a pinned
//! worker that can no longer move away. So a worker that cannot be seated on a
//! whole physical core means nothing is pinned at all, and the master is always
//! pinned off the workers' cores.

const CPU_ROOT: &str = "/sys/devices/system/cpu";
const NODE_ROOT: &str = "/sys/devices/system/node";

/// One physical core's allowed logical CPUs, ascending.
type Core = Vec<u32>;

/// Which CPUs each process is confined to. A struct rather than one `Vec` with
/// an index-0-is-the-master convention, so neither call site carries an offset.
/// The masks stay private: `assign` is the only thing that chooses one, and
/// pinning goes through the methods below, so no caller can pin a process to a
/// mask the placement did not hand it.
#[derive(Debug)]
pub struct Placement {
    master: Vec<u32>,
    /// One mask per worker, indexed by rank.
    workers: Vec<Vec<u32>>,
}

impl Placement {
    /// The boot record, one CPU list per process:
    /// `W0 [0, 1] W1 [2, 3] master [4, 5, 6, 7]`.
    pub fn describe(&self) -> String {
        let mut s = String::new();
        for (w, cpus) in self.workers.iter().enumerate() {
            s.push_str(&format!("W{w} {cpus:?} "));
        }
        s.push_str(&format!("master {:?}", self.master));
        s
    }

    /// Confine the master to the CPUs no worker owns.
    pub fn pin_master(&self) {
        if !pin_self(&self.master) {
            gnitz_warn!(
                "master failed to pin to CPUs {:?}; workers stay pinned beneath it",
                self.master
            );
        }
    }

    /// Confine worker `w` to its core. If the kernel refuses, widen to every
    /// CPU the server may use: the child inherited the pinned master's mask
    /// across the fork, so leaving it there would park a worker on the master's
    /// CPUs, and `sched_setaffinity` is bounded by the cgroup cpuset rather than
    /// by the mask a process inherited. The warning stays because a partial pin
    /// is the arrangement this module refuses.
    pub fn pin_worker(&self, w: usize) {
        if pin_self(&self.workers[w]) {
            return;
        }
        let all: Vec<u32> = self
            .master
            .iter()
            .chain(self.workers.iter().flatten())
            .copied()
            .collect();
        pin_self(&all);
        gnitz_warn!(
            "failed to pin to CPUs {:?}; running unpinned among pinned siblings",
            self.workers[w]
        );
    }
}

/// Parse a sysfs CPU list — `"0-3,8,12-15"` — into ascending CPU ids. The node
/// id list, the node cpulists and the sibling lists all use this format. An
/// unparseable field yields no ids rather than a wrong set.
fn parse_cpu_list(s: &str) -> Vec<u32> {
    // A singleton is a degenerate range, so one rule covers both spellings.
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

fn read_cpu_list(path: &str) -> Vec<u32> {
    std::fs::read_to_string(path)
        .map(|s| parse_cpu_list(&s))
        .unwrap_or_default()
}

/// The machine's NUMA nodes in id order, each holding its cores in ascending
/// order. Only CPUs in `allowed` appear.
///
/// Every sysfs read degrades rather than fails: no node list means one
/// pseudo-node holding every core, and an unreadable sibling list makes that
/// CPU its own core. Both leave a usable placement on a kernel or container
/// that hides `/sys`. A core no online node names is dropped here, so its CPUs
/// fall to the master's complement in `assign`.
fn cores_by_node(allowed: &[u32]) -> Vec<Vec<Core>> {
    let cores = group_cores(allowed);
    let node_ids = read_cpu_list(&format!("{NODE_ROOT}/online"));
    if node_ids.is_empty() {
        return vec![cores];
    }
    // SMT siblings never straddle a node, so a core belongs to whichever node
    // names its first CPU — bucketing, not a second grouping pass.
    node_ids
        .iter()
        .map(|n| {
            let cpus = read_cpu_list(&format!("{NODE_ROOT}/node{n}/cpulist"));
            cores
                .iter()
                .filter(|c| cpus.binary_search(&c[0]).is_ok())
                .cloned()
                .collect()
        })
        .filter(|cores: &Vec<Core>| !cores.is_empty())
        .collect()
}

/// Group `cpus` into physical cores by SMT sibling set. Cores come out ordered
/// by their lowest CPU id, siblings ascending within a core. `cpus` must be
/// ascending.
fn group_cores(cpus: &[u32]) -> Vec<Core> {
    let mut seen: Vec<u32> = Vec::new();
    let mut cores: Vec<Core> = Vec::new();
    for &c in cpus {
        if seen.contains(&c) {
            continue;
        }
        let sibs = read_cpu_list(&format!("{CPU_ROOT}/cpu{c}/topology/thread_siblings_list"));
        let mut core: Core = sibs.iter().copied().filter(|s| cpus.binary_search(s).is_ok()).collect();
        if core.is_empty() {
            core = vec![c];
        }
        core.sort_unstable();
        seen.extend(core.iter().copied());
        cores.push(core);
    }
    cores.sort_by_key(|c| c[0]);
    cores
}

/// Seat each worker on a whole physical core and give the master every CPU no
/// worker owns, or refuse when the workers would leave the master no core.
/// `Err` carries the usable core count, which is what the boot record has to
/// state to explain the refusal.
///
/// A worker gets a whole core or nothing: SMT siblings share L1d and L2, so
/// once two workers share a core the cache retention this exists to buy is
/// already gone and all that is left is taking away the scheduler's freedom to
/// fill an idle CPU. There is deliberately no fallback tier of single logical
/// CPUs.
///
/// The master takes the complement rather than a core, because it is a reactor
/// thread plus an io_uring pool whose `iou-wrk` threads absorb every pre-ACK
/// `fdatasync`: it needs room to burst, and it needs that burst off the
/// workers' cores.
///
/// Cores are handed out round-robin across NUMA nodes, so a worker's own store
/// and trace memory — the large, randomly-accessed footprint, allocated by the
/// worker itself and therefore first-touched node-local — is spread across
/// nodes instead of filling one until it is exhausted and then landing remote.
/// The SAL is left where the master's first touch puts it: every worker reads
/// it, so some worker is remote from it under any placement, and a sequential
/// streaming read is what an interconnect handles best.
///
/// `allowed` is passed in rather than derived from `nodes` because a CPU that
/// is allowed but named by no online node belongs to the master's complement
/// even though it is in no core.
fn assign(nodes: &[Vec<Core>], allowed: &[u32], workers: usize) -> Result<Placement, usize> {
    let depth = nodes.iter().map(|cores| cores.len()).max().unwrap_or(0);
    let mut order: Vec<&Core> = Vec::new();
    for i in 0..depth {
        for node in nodes {
            if let Some(core) = node.get(i) {
                order.push(core);
            }
        }
    }
    if workers >= order.len() {
        return Err(order.len());
    }
    let worker_cpus: Vec<Vec<u32>> = order.iter().take(workers).map(|c| (*c).clone()).collect();
    let taken: Vec<u32> = worker_cpus.iter().flatten().copied().collect();
    let master: Vec<u32> = allowed.iter().copied().filter(|c| !taken.contains(c)).collect();
    Ok(Placement {
        master,
        workers: worker_cpus,
    })
}

/// Plan the whole server's placement — one sysfs walk, one decision — or say
/// why there is none, in the words the boot record prints.
pub fn plan(workers: usize) -> Result<Placement, String> {
    if !gnitz_store::foundation::env::env_flag("GNITZ_CPU_AFFINITY", true) {
        return Err("GNITZ_CPU_AFFINITY=0".to_string());
    }
    let allowed = allowed_cpus();
    assign(&cores_by_node(&allowed), &allowed, workers)
        .map_err(|cores| format!("{workers} workers, {cores} usable cores"))
}

/// The CPUs this process may run on: the cgroup cpuset, narrowed by any
/// affinity an operator already imposed, and intersected with the online set so
/// a hot-unplugged CPU is never handed out as a slot. Never `nproc` — reading
/// the mask is what makes a container and a `taskset` wrapper both work by
/// construction. Read before anything is pinned, so it is the machine's answer
/// and not a mask this server set itself.
fn allowed_cpus() -> Vec<u32> {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
    if rc != 0 {
        return Vec::new();
    }
    let online = read_cpu_list(&format!("{CPU_ROOT}/online"));
    (0..libc::CPU_SETSIZE as usize)
        .filter(|&c| unsafe { libc::CPU_ISSET(c, &set) })
        .map(|c| c as u32)
        .filter(|c| online.is_empty() || online.binary_search(c).is_ok())
        .collect()
}

/// Confine the calling thread — and, at boot, therefore the whole process — to
/// `cpus`, reporting whether the kernel accepted it.
fn pin_self(cpus: &[u32]) -> bool {
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe { libc::CPU_ZERO(&mut set) };
    for &c in cpus {
        if (c as usize) < libc::CPU_SETSIZE as usize {
            unsafe { libc::CPU_SET(c as usize, &mut set) };
        }
    }
    unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) == 0 }
}

#[cfg(test)]
#[path = "tests/affinity.rs"]
mod tests;
