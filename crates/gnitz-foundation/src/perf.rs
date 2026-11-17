//! Cost probes for benchmarks, immune to the frequency and scheduling noise in
//! wall clock: hardware events on the calling thread, its voluntary context
//! switches, and this process's resident set.

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
const FORMAT_TOTAL_TIME_ENABLED: u64 = 1 << 0;
const FORMAT_TOTAL_TIME_RUNNING: u64 = 1 << 1;
const FORMAT_GROUP: u64 = 1 << 3;
const IOC_ENABLE: libc::c_ulong = 0x2400;
const IOC_DISABLE: libc::c_ulong = 0x2401;
const IOC_RESET: libc::c_ulong = 0x2403;
const IOC_FLAG_GROUP: libc::c_ulong = 1;

/// A hardware event, counted in user mode unless [`Event::with_kernel`] says
/// otherwise. A user-mode count does not depend on `perf_event_paranoid`; one
/// that takes in the kernel needs it at 1 or below.
#[derive(Clone, Copy)]
pub struct Event {
    config: u64,
    kernel: bool,
}

impl Event {
    pub const INSTRUCTIONS: Event = Event {
        config: PERF_COUNT_HW_INSTRUCTIONS,
        kernel: false,
    };
    pub const CYCLES: Event = Event {
        config: PERF_COUNT_HW_CPU_CYCLES,
        kernel: false,
    };
    pub const BRANCH_MISSES: Event = Event {
        config: PERF_COUNT_HW_BRANCH_MISSES,
        kernel: false,
    };

    /// The same event, kernel mode counted too: what a call costs in the
    /// kernel on this thread's behalf.
    pub const fn with_kernel(self) -> Event {
        Event { kernel: true, ..self }
    }
}

/// `N` hardware counters over the calling thread, opened as one group: enabled,
/// disabled and read together, so every one counts the same window and none
/// counts another's ioctls.
pub struct Counters<const N: usize> {
    /// The group's leader first.
    fds: [File; N],
}

impl<const N: usize> Counters<N> {
    /// Panics where the kernel refuses an event.
    pub fn open(events: [Event; N]) -> Self {
        const { assert!(N > 0, "a group has a leader") };
        let mut leader = -1;
        let fds = events.map(|e| {
            let attr = PerfEventAttr {
                type_: PERF_TYPE_HARDWARE,
                size: std::mem::size_of::<PerfEventAttr>() as u32,
                config: e.config,
                read_format: FORMAT_GROUP | FORMAT_TOTAL_TIME_ENABLED | FORMAT_TOTAL_TIME_RUNNING,
                // Only the leader starts disabled: a member follows its leader.
                flags: (if leader < 0 { FLAG_DISABLED } else { 0 })
                    | (if e.kernel { 0 } else { FLAG_EXCLUDE_KERNEL })
                    | FLAG_EXCLUDE_HV,
                ..Default::default()
            };
            // pid 0 = this thread, cpu -1 = any, the leader's group, no flags.
            let fd = unsafe { libc::syscall(libc::SYS_perf_event_open, &attr as *const _, 0, -1, leader, 0) };
            assert!(fd >= 0, "perf_event_open: {}", std::io::Error::last_os_error());
            if leader < 0 {
                leader = fd as libc::c_int;
            }
            // SAFETY: a descriptor the syscall just returned, owned by nothing else.
            unsafe { File::from_raw_fd(fd as libc::c_int) }
        });
        Counters { fds }
    }

    /// The events `f` counted on this thread, in the order they were opened.
    ///
    /// # Panics
    ///
    /// Where the group was not counting for the whole of every window it was
    /// enabled for — the kernel multiplexed it with other events, or the
    /// thread ran on a core the events do not cover — since its counts would
    /// then be partial.
    pub fn measure<T>(&self, f: impl FnOnce() -> T) -> (T, [u64; N]) {
        let fd = self.fds[0].as_raw_fd();
        unsafe {
            libc::ioctl(fd, IOC_RESET, IOC_FLAG_GROUP);
            libc::ioctl(fd, IOC_ENABLE, IOC_FLAG_GROUP);
        }
        let out = f();
        unsafe { libc::ioctl(fd, IOC_DISABLE, IOC_FLAG_GROUP) };
        // `{ nr, time_enabled, time_running, value[nr] }`. The times add up
        // over every window and only advance in one, so they stay equal while
        // each window counted throughout.
        let mut buf = vec![0u8; 8 * (3 + N)];
        (&self.fds[0]).read_exact(&mut buf).expect("read the perf counters");
        let word = |i: usize| u64::from_ne_bytes(buf[8 * i..8 * i + 8].try_into().unwrap());
        assert_eq!(word(1), word(2), "the perf counters ran for part of their window");
        (out, std::array::from_fn(|i| word(3 + i)))
    }
}

/// User-mode instructions retired on the calling thread: the one event most
/// benches read alone.
pub struct Counter(Counters<1>);

impl Counter {
    /// Panics where the kernel refuses the counter.
    pub fn instructions() -> Self {
        Counter(Counters::open([Event::INSTRUCTIONS]))
    }

    /// The instructions `f` retired on this thread.
    pub fn measure<T>(&self, f: impl FnOnce() -> T) -> (T, u64) {
        let (out, [n]) = self.0.measure(f);
        (out, n)
    }
}

/// This thread's voluntary context switches — one per park. Same scope as
/// [`Counters`].
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
