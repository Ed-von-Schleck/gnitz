//! Cost probes for benchmarks and cost-claim tests, immune to the frequency and
//! scheduling noise in wall clock: instructions retired, core cycles and
//! voluntary context switches on the calling thread, and this process's
//! resident set.

use std::fs::File;
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd};

/// `perf_event_attr`'s version-0 layout; the kernel reads `size` bytes of it.
#[repr(C)]
#[derive(Default)]
struct PerfEventAttr {
    type_: u32,
    size: u32,
    config: u64,
    sample_period: u64,
    sample_type: u64,
    read_format: u64,
    /// Bit 0 `disabled`, bit 5 `exclude_kernel`, bit 6 `exclude_hv`.
    flags: u64,
    wakeup_events: u32,
    bp_type: u32,
    config1: u64,
}

const _: () = assert!(std::mem::size_of::<PerfEventAttr>() == 64);

const PERF_TYPE_HARDWARE: u32 = 0;
const PERF_COUNT_HW_CPU_CYCLES: u64 = 0;
const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;
const PERF_COUNT_HW_BRANCH_MISSES: u64 = 5;
const FLAG_DISABLED: u64 = 1 << 0;
const FLAG_EXCLUDE_KERNEL: u64 = 1 << 5;
const FLAG_EXCLUDE_HV: u64 = 1 << 6;
const IOC_ENABLE: libc::c_ulong = 0x2400;
const IOC_DISABLE: libc::c_ulong = 0x2401;
const IOC_RESET: libc::c_ulong = 0x2403;

/// An open hardware counter over the calling thread's user-space execution.
///
/// Kernel mode is excluded on every machine, so a figure does not depend on
/// `perf_event_paranoid`. What a call costs in the kernel is counted as requests
/// and as [`voluntary_ctx_switches`].
pub struct Counter(File);

impl Counter {
    /// Instructions retired. Panics where the kernel refuses the counter.
    pub fn instructions() -> Self {
        Self::open(PERF_COUNT_HW_INSTRUCTIONS)
    }

    /// Core cycles. Panics where the kernel refuses the counter.
    pub fn cycles() -> Self {
        Self::open(PERF_COUNT_HW_CPU_CYCLES)
    }

    /// Mispredicted branches. Panics where the kernel refuses the counter.
    pub fn branch_misses() -> Self {
        Self::open(PERF_COUNT_HW_BRANCH_MISSES)
    }

    /// Instructions retired, user and kernel mode both.
    pub fn instructions_all() -> Self {
        Self::open_flags(PERF_COUNT_HW_INSTRUCTIONS, FLAG_DISABLED | FLAG_EXCLUDE_HV)
    }

    /// Core cycles, user and kernel mode both.
    pub fn cycles_all() -> Self {
        Self::open_flags(PERF_COUNT_HW_CPU_CYCLES, FLAG_DISABLED | FLAG_EXCLUDE_HV)
    }

    fn open(config: u64) -> Self {
        Self::open_flags(config, FLAG_DISABLED | FLAG_EXCLUDE_KERNEL | FLAG_EXCLUDE_HV)
    }

    fn open_flags(config: u64, flags: u64) -> Self {
        let attr = PerfEventAttr {
            type_: PERF_TYPE_HARDWARE,
            size: std::mem::size_of::<PerfEventAttr>() as u32,
            config,
            flags,
            ..Default::default()
        };
        // pid 0 = this thread, cpu -1 = any, no group, no flags.
        let fd = unsafe { libc::syscall(libc::SYS_perf_event_open, &attr as *const _, 0, -1, -1, 0) };
        assert!(fd >= 0, "perf_event_open: {}", std::io::Error::last_os_error());
        // SAFETY: a descriptor the syscall just returned, owned by nothing else.
        Counter(unsafe { File::from_raw_fd(fd as libc::c_int) })
    }

    /// The events `f` counted on this thread.
    pub fn measure<T>(&self, f: impl FnOnce() -> T) -> (T, u64) {
        let fd = self.0.as_raw_fd();
        unsafe {
            libc::ioctl(fd, IOC_RESET, 0);
            libc::ioctl(fd, IOC_ENABLE, 0);
        }
        let out = f();
        unsafe { libc::ioctl(fd, IOC_DISABLE, 0) };
        let mut buf = [0u8; 8];
        (&self.0).read_exact(&mut buf).expect("read the perf counter");
        (out, u64::from_le_bytes(buf))
    }
}

/// This thread's voluntary context switches — one per park. Same scope as
/// [`Counter`].
pub fn voluntary_ctx_switches() -> i64 {
    // SAFETY: `getrusage` writes a plain POD struct this call owns.
    unsafe {
        let mut ru: libc::rusage = std::mem::zeroed();
        libc::getrusage(libc::RUSAGE_THREAD, &mut ru);
        ru.ru_nvcsw
    }
}

/// Bytes of `field` (`VmRSS`, `VmHWM`, …) in `/proc/self/status`.
fn status_bytes(field: &str) -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("read /proc/self/status");
    let kb = status
        .lines()
        .find_map(|l| l.strip_prefix(field)?.strip_prefix(':'))
        .and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok());
    kb.unwrap_or_else(|| panic!("no {field} in /proc/self/status")) * 1024
}

/// What to run a benchmark under for [`Resident`] to answer.
pub const PIN_MMAP_THRESHOLD: &str = "GLIBC_TUNABLES=glibc.malloc.mmap_threshold=131072";

/// What, written to `/proc/self/clear_refs`, resets `VmHWM` to the resident set.
const RESET_PEAK_RSS: &str = "5";

/// This process's resident memory against a baseline, in bytes.
pub struct Resident {
    base: u64,
}

impl Resident {
    /// The baseline, taken now. `None` unless [`PIN_MMAP_THRESHOLD`] is set:
    /// glibc otherwise keeps freed memory resident and hands it out again, so
    /// a reading would count what earlier work left behind.
    pub fn baseline() -> Option<Self> {
        let pinned = std::env::var("GLIBC_TUNABLES").is_ok_and(|t| t.contains("glibc.malloc.mmap_threshold="));
        pinned.then(|| {
            let base = Self::now();
            std::fs::write("/proc/self/clear_refs", RESET_PEAK_RSS).expect("reset the peak resident set");
            Resident { base }
        })
    }

    /// What is resident beyond the baseline, the allocator's free memory handed back.
    pub fn added(&self) -> u64 {
        Self::now().saturating_sub(self.base)
    }

    /// The most that was resident beyond the baseline since it was taken.
    pub fn peak_added(&self) -> u64 {
        status_bytes("VmHWM").saturating_sub(self.base)
    }

    fn now() -> u64 {
        // SAFETY: `malloc_trim` only releases memory the allocator holds free.
        unsafe { libc::malloc_trim(0) };
        status_bytes("VmRSS")
    }
}

/// The pass count a benchmark loops over, from `GNITZ_BENCH_PASSES`; `1` when
/// unset. Two runs at different counts, differenced, cancel everything that
/// happens once per process.
pub fn bench_passes() -> usize {
    crate::env::env_num("GNITZ_BENCH_PASSES", 1)
}
