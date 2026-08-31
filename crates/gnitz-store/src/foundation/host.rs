//! Host sensing: what the machine — or the container — will actually give us.
//!
//! A policy input, not a syscall wrapper: a budget expressed as a fraction of
//! this scales with the deployment where a flat constant cannot.

use std::sync::OnceLock;

/// Best-effort memory budget for the process, in bytes: the cgroup v2 limit when
/// there is one, else total physical RAM. `0` if every source fails — callers
/// clamp, since a budget of zero is not a usable answer. Cached: it cannot
/// change for the life of the process.
pub fn available_memory_bytes() -> usize {
    static CACHED: OnceLock<usize> = OnceLock::new();
    *CACHED.get_or_init(|| {
        if let Some(v) = cgroup_v2_memory_max() {
            return v;
        }
        let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) };
        if pages > 0 && page_size > 0 {
            (pages as usize).saturating_mul(page_size as usize)
        } else {
            0
        }
    })
}

/// The tightest finite cgroup v2 `memory.max` from this process's own cgroup up
/// to the root; `None` when nothing on the path sets one — which is also what a
/// v1/hybrid host yields, since it writes one line per controller rather than
/// the unified `0::<path>` matched below.
fn cgroup_v2_memory_max() -> Option<usize> {
    // The path must come from here: `/sys/fs/cgroup` alone names the *root*
    // cgroup, which sets no `memory.max` on a systemd host, so a `MemoryMax=`d
    // unit would read as host RAM.
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    // The v2 path is absolute; `join` would discard the mount point.
    let rel = cgroup.lines().find_map(|l| l.strip_prefix("0::"))?.trim();
    std::path::Path::new("/sys/fs/cgroup")
        .join(rel.trim_start_matches('/'))
        .ancestors()
        .take_while(|d| d.starts_with("/sys/fs/cgroup"))
        // The literal `"max"` (unlimited at this level) fails the parse, as
        // does an absent file; both are skipped.
        .filter_map(|d| std::fs::read_to_string(d.join("memory.max")).ok())
        .filter_map(|s| s.trim().parse::<usize>().ok())
        .filter(|&v| v > 0)
        .min()
}
