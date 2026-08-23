//! A thread-local instructions-retired counter, for the one claim in this suite
//! that is about cost rather than about answers.
//!
//! Wall clock is not evidence here: the number that would falsify "the round
//! trip is gone" is retired instructions, which is immune to the frequency and
//! scheduling noise a wall-clock comparison folds in.
//!
//! It counts **this thread**, kernel time included where the kernel allows it —
//! which is the right scope for the claim, because what a mirror removes from a
//! caller is the caller's own syscall, socket and wakeup work. The server
//! processes' work is outside it by construction and is not the caller's cost.
//!
//! `perf_event_attr`'s first 64 bytes are its version-0 layout, and the kernel
//! reads `min(size, its own sizeof)` — so a struct of exactly those fields, with
//! `size` set to match, is the whole ABI this needs.
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

const PERF_TYPE_HARDWARE: u32 = 0;
const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;
const FLAG_DISABLED: u64 = 1 << 0;
const FLAG_EXCLUDE_KERNEL: u64 = 1 << 5;
const FLAG_EXCLUDE_HV: u64 = 1 << 6;
const IOC_ENABLE: libc::c_ulong = 0x2400;
const IOC_DISABLE: libc::c_ulong = 0x2401;
const IOC_RESET: libc::c_ulong = 0x2403;

/// An open counter for the calling thread.
pub struct Instructions {
    fd: libc::c_int,
    /// Whether kernel-mode instructions are counted. They are what a served
    /// read's syscall, socket and wakeup path spend on the caller's behalf — the
    /// cost a local read removes — so the counter asks for them and only falls
    /// back to user-only where `perf_event_paranoid` refuses.
    pub counts_kernel: bool,
}

impl Instructions {
    /// `None` where the kernel refuses the counter outright —
    /// `perf_event_paranoid` at its strictest, a container without the
    /// capability — so a caller skips rather than fails.
    pub fn open() -> Option<Self> {
        Self::open_with(false)
            .map(|fd| Instructions {
                fd,
                counts_kernel: true,
            })
            .or_else(|| {
                Self::open_with(true).map(|fd| Instructions {
                    fd,
                    counts_kernel: false,
                })
            })
    }

    fn open_with(exclude_kernel: bool) -> Option<libc::c_int> {
        let attr = PerfEventAttr {
            type_: PERF_TYPE_HARDWARE,
            size: std::mem::size_of::<PerfEventAttr>() as u32,
            config: PERF_COUNT_HW_INSTRUCTIONS,
            flags: FLAG_DISABLED | FLAG_EXCLUDE_HV | if exclude_kernel { FLAG_EXCLUDE_KERNEL } else { 0 },
            ..Default::default()
        };
        // pid 0 = this thread, cpu -1 = any, no group, no flags.
        let fd = unsafe { libc::syscall(libc::SYS_perf_event_open, &attr as *const _, 0, -1, -1, 0) };
        (fd >= 0).then_some(fd as libc::c_int)
    }

    /// Instructions retired by `f` on this thread.
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

impl Drop for Instructions {
    fn drop(&mut self) {
        unsafe { libc::close(self.fd) };
    }
}
