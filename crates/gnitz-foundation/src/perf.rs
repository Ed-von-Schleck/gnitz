//! Cost probes for benchmarks and cost-claim tests, immune to the frequency and
//! scheduling noise in wall clock: instructions retired, core cycles and
//! voluntary context switches on the calling thread, and this process's
//! resident set.

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
const FLAG_DISABLED: u64 = 1 << 0;
const FLAG_EXCLUDE_KERNEL: u64 = 1 << 5;
const FLAG_EXCLUDE_HV: u64 = 1 << 6;
const IOC_ENABLE: libc::c_ulong = 0x2400;
const IOC_DISABLE: libc::c_ulong = 0x2401;
const IOC_RESET: libc::c_ulong = 0x2403;

/// An open hardware counter for the calling thread.
pub struct Counter {
    fd: libc::c_int,
    /// Whether kernel-mode instructions are counted; `perf_event_paranoid` can
    /// refuse them.
    pub counts_kernel: bool,
}

impl Counter {
    /// Instructions retired; `None` where the kernel refuses the counter outright.
    pub fn instructions() -> Option<Self> {
        Self::open(PERF_COUNT_HW_INSTRUCTIONS)
    }

    /// Core cycles; `None` where the kernel refuses the counter outright.
    pub fn cycles() -> Option<Self> {
        Self::open(PERF_COUNT_HW_CPU_CYCLES)
    }

    fn open(config: u64) -> Option<Self> {
        Self::open_with(config, false)
            .map(|fd| Counter { fd, counts_kernel: true })
            .or_else(|| Self::open_with(config, true).map(|fd| Counter { fd, counts_kernel: false }))
    }

    fn open_with(config: u64, exclude_kernel: bool) -> Option<libc::c_int> {
        let attr = PerfEventAttr {
            type_: PERF_TYPE_HARDWARE,
            size: std::mem::size_of::<PerfEventAttr>() as u32,
            config,
            flags: FLAG_DISABLED | FLAG_EXCLUDE_HV | if exclude_kernel { FLAG_EXCLUDE_KERNEL } else { 0 },
            ..Default::default()
        };
        // pid 0 = this thread, cpu -1 = any, no group, no flags.
        let fd = unsafe { libc::syscall(libc::SYS_perf_event_open, &attr as *const _, 0, -1, -1, 0) };
        (fd >= 0).then_some(fd as libc::c_int)
    }

    /// The events `f` counted on this thread.
    pub fn measure<T>(&self, f: impl FnOnce() -> T) -> (T, u64) {
        unsafe {
            libc::ioctl(self.fd, IOC_RESET, 0);
            libc::ioctl(self.fd, IOC_ENABLE, 0);
        }
        let out = f();
        let mut buf = [0u8; 8];
        unsafe {
            libc::ioctl(self.fd, IOC_DISABLE, 0);
            libc::read(self.fd, buf.as_mut_ptr() as *mut libc::c_void, 8);
        }
        (out, u64::from_le_bytes(buf))
    }
}

impl Drop for Counter {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
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

/// Bytes of `field` (`VmRSS`, `VmHWM`, …) in `/proc/self/status`. `0` where
/// `/proc` does not answer.
fn status_bytes(field: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix(field)?.strip_prefix(':'))
                .and_then(|v| v.split_whitespace().next()?.parse::<u64>().ok())
        })
        .map_or(0, |kb| kb * 1024)
}

/// This process's resident set, in bytes. `0` where `/proc` does not answer.
pub fn rss_bytes() -> u64 {
    status_bytes("VmRSS")
}

/// This process's peak resident set since it started or since the last
/// [`reset_peak_rss`], in bytes. `0` where `/proc` does not answer.
pub fn peak_rss_bytes() -> u64 {
    status_bytes("VmHWM")
}

/// Reset [`peak_rss_bytes`] to the current resident set, so a peak measures one
/// region. A no-op where `/proc/self/clear_refs` is not writable.
pub fn reset_peak_rss() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}
