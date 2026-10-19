//! Host sensing: what the machine — or the container — will actually give us.
//!
//! A policy input, not a syscall wrapper: a budget expressed as a fraction of
//! this scales with the deployment where a flat constant cannot.

use std::path::Path;

/// Best-effort memory budget for the process, in bytes: the cgroup v2 limit when
/// there is one, else total physical RAM. `0` if every source fails — callers
/// clamp, since a budget of zero is not a usable answer.
pub fn available_memory_bytes() -> usize {
    if let Some(v) = own_cgroup_tightest("memory.max", memory_max) {
        return v;
    }
    let pages = unsafe { libc::sysconf(libc::_SC_PHYS_PAGES) };
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) };
    if pages > 0 && page_size > 0 {
        (pages as usize).saturating_mul(page_size as usize)
    } else {
        0
    }
}

/// The whole CPUs this process's cgroup v2 CPU quota grants — `--cpus=1.5`
/// reads as `1` — or `None` when no cgroup on its path sets one.
pub fn cpu_quota() -> Option<usize> {
    own_cgroup_tightest("cpu.max", cpu_max)
}

/// A `memory.max` in bytes; the literal `"max"` (unlimited at this level) fails
/// the parse.
fn memory_max(s: &str) -> Option<usize> {
    s.parse().ok().filter(|&v| v > 0)
}

/// A `cpu.max` (`"<quota> <period>"`) in whole CPUs; a quota of `"max"` fails
/// the parse.
fn cpu_max(s: &str) -> Option<usize> {
    let (quota, period) = s.split_once(' ')?;
    let cpus = quota.parse::<u64>().ok()?.checked_div(period.parse().ok()?)?;
    usize::try_from(cpus).ok()
}

/// [`tightest_on_path`] over this process's own cgroup.
fn own_cgroup_tightest<T: Ord>(file: &str, parse: fn(&str) -> Option<T>) -> Option<T> {
    // The path must come from here: `/sys/fs/cgroup` alone names the *root*
    // cgroup, which sets no limit on a systemd host, so a `MemoryMax=`d unit
    // would read as unlimited.
    let cgroup = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    tightest_on_path(&cgroup, Path::new("/sys/fs/cgroup"), file, parse)
}

/// The smallest value `parse` reads from `file` in any cgroup on the path
/// `proc_cgroup` (the contents of `/proc/<pid>/cgroup`) names, from that cgroup
/// up to the cgroup v2 mount `root`; a file absent or unparsed at a level is
/// skipped. A v1/hybrid host yields `None`, since it writes one line per
/// controller rather than the unified `0::<path>` matched below.
fn tightest_on_path<T: Ord>(proc_cgroup: &str, root: &Path, file: &str, parse: fn(&str) -> Option<T>) -> Option<T> {
    // The v2 path is absolute; `join` would discard the mount point.
    let rel = proc_cgroup.lines().find_map(|l| l.strip_prefix("0::"))?.trim();
    root.join(rel.trim_start_matches('/'))
        .ancestors()
        .take_while(|d| d.starts_with(root))
        .filter_map(|d| std::fs::read_to_string(d.join(file)).ok())
        .filter_map(|s| parse(s.trim()))
        .min()
}

/// Why this host refused an io_uring syscall with `e`, and the remedy; `None`
/// for an error that is no refusal, which explains itself.
pub fn io_uring_refusal(e: &std::io::Error) -> Option<String> {
    let disabled = std::fs::read_to_string("/proc/sys/kernel/io_uring_disabled").ok();
    refusal(e.raw_os_error(), disabled.as_deref().map(str::trim))
}

const SECCOMP_REMEDY: &str = "in a container, run it under a seccomp profile that also allows io_uring_setup, \
                              io_uring_enter and io_uring_register";

/// [`io_uring_refusal`] for `errno`, given the `kernel.io_uring_disabled`
/// sysctl's value (`None` where the kernel has no such sysctl).
fn refusal(errno: Option<i32>, disabled: Option<&str>) -> Option<String> {
    let why = match (errno?, disabled) {
        (libc::EPERM | libc::EACCES, Some("2")) => {
            "kernel.io_uring_disabled=2 disables io_uring for every process; set it to 0".to_string()
        }
        (libc::EPERM | libc::EACCES, Some("1")) => "kernel.io_uring_disabled=1 allows io_uring only to \
             CAP_SYS_ADMIN and the group kernel.io_uring_group names; set it to 0, or run the server in that group"
            .to_string(),
        (libc::ENOSYS, _) => format!(
            "a seccomp filter denies it (podman's default profile does), the kernel has no io_uring, or the \
             binary runs under an emulator that has none (qemu-user does not: run the build for this \
             machine's architecture); {SECCOMP_REMEDY}"
        ),
        (libc::EPERM | libc::EACCES, _) => format!(
            "a seccomp filter denies it (Docker's default profile does), or a security module does; {SECCOMP_REMEDY}"
        ),
        _ => return None,
    };
    Some(why)
}

#[cfg(test)]
#[path = "tests/host.rs"]
mod tests;
