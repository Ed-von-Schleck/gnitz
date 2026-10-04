//! The futex park every transport sleeps through: [`Park`], one word a side
//! sleeps on and its peer wakes, and [`WorkerParks`], the mapping where each
//! worker sleeps for a SAL group or an exchange part.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use io_uring::types::FutexWaitV;

// ---------------------------------------------------------------------------
// Futex wait/wake
// ---------------------------------------------------------------------------

/// The 32-bit futex word aliasing a park word's low half. A ring cursor advances
/// by at most its ring's capacity and a wake sequence by one, so under 2^32 an
/// advance always moves the half a futex compares.
#[inline]
fn futex_word(word: &AtomicU64) -> *const AtomicU32 {
    word as *const AtomicU64 as *const AtomicU32
}

/// `futex2(2)` flags byte for a 32-bit atomic. Matches the kernel constant
/// `FUTEX2_SIZE_U32` (=2). No `FUTEX2_PRIVATE` bit — a park word is
/// `MAP_SHARED` across `fork()`.
const FUTEX2_SIZE_U32: u32 = 2;

/// One `futex_waitv` entry: sleep on `word` while it still reads `expected`.
#[inline]
pub(super) fn futex_waitv_entry(word: *const AtomicU32, expected: u32) -> FutexWaitV {
    FutexWaitV::new()
        .val(expected as u64)
        .uaddr(word as u64)
        .flags(FUTEX2_SIZE_U32)
}

/// The errno of the most recent failed syscall. The futex wrappers go through
/// `libc::syscall`, which returns `-1` rather than `-errno`.
#[inline]
fn errno() -> i32 {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}

/// Wake the waiter parked on `ptr` (v1 `FUTEX_WAKE`, no `FUTEX_PRIVATE_FLAG` — a
/// park word is shared). Aborts rather than reporting: a lost wake leaves the
/// peer blocked on a queue that has work in it, so there is no caller-side
/// answer to distinguish. `site` names the caller in the abort.
#[inline]
fn futex_wake_u32(ptr: *const AtomicU32, site: &str) {
    let rc = unsafe {
        libc::syscall(
            libc::SYS_futex,
            ptr as *const libc::c_void,
            libc::FUTEX_WAKE,
            1 as libc::c_int,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0u32,
        ) as i32
    };
    if rc < 0 {
        futex_failed(site, errno());
    }
}

/// Sleep on `word` while it reads `expected` (v1 `FUTEX_WAIT`, no
/// `FUTEX_PRIVATE_FLAG` — a park word is shared). A wake, a moved value and a
/// signal all return; anything else aborts.
pub(super) fn futex_wait_u32(word: *const AtomicU32, expected: u32, site: &str) {
    let rc = unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const libc::c_void,
            libc::FUTEX_WAIT,
            expected as libc::c_int,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0u32,
        ) as i32
    };
    if rc < 0 {
        let e = errno();
        if e != libc::EAGAIN && e != libc::EINTR {
            futex_failed(site, e);
        }
    }
}

/// Outlined so `site` is not spilled onto the stack on the paths that never
/// abort.
#[cold]
#[inline(never)]
fn futex_failed(site: &str, errno: i32) -> ! {
    gnitz_fatal_abort!("{}: futex failed: errno={}", site, errno);
}

/// An anonymous read-write mapping shared with every `fork()`ed child, zeroed.
/// `MAP_NORESERVE`: its pages are charged as they are touched, so under
/// heuristic overcommit a mapping sized far above its occupancy is not refused
/// for its size.
pub(super) fn map_anon_shared(size: usize) -> std::io::Result<*mut u8> {
    // SAFETY: a fresh mapping at an address the kernel picks.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            size,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_SHARED | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        return Err(std::io::Error::last_os_error());
    }
    Ok(ptr.cast())
}

// ---------------------------------------------------------------------------
// Park
// ---------------------------------------------------------------------------

/// Bit 63 of a park word: the parker is armed. Outside the low half a futex compares.
const ARMED: u64 = 1 << 63;

/// One side's sleep, in one word: a value in bits 0..63 that only grows, and the
/// parker's arm in bit 63.
///
/// The arm and every advance are RMWs on the one atomic, so they are totally
/// ordered: an advance behind the arm sees it and wakes, and an arm behind the
/// advance returns the advanced value for the parker's re-test.
#[repr(transparent)]
pub(super) struct Park(AtomicU64);

impl Park {
    #[inline]
    pub(super) fn value(&self) -> u64 {
        self.0.load(Ordering::Acquire) & !ARMED
    }

    /// Arm, returning the value the arm was taken at.
    #[inline]
    pub(super) fn arm(&self) -> u64 {
        self.0.fetch_or(ARMED, Ordering::AcqRel) & !ARMED
    }

    /// Drop the arm. The result is deliberately unused: binding it would force a
    /// `lock cmpxchg` retry loop where a discarded one lowers to a `lock and`.
    #[inline]
    pub(super) fn disarm(&self) {
        self.0.fetch_and(!ARMED, Ordering::AcqRel);
    }

    /// The value's one writer stores `v`, waking the parker iff it armed first.
    /// The swap takes the arm, so later publishes spend no syscall until a re-arm.
    #[inline]
    pub(super) fn publish(&self, v: u64, site: &str) {
        debug_assert_eq!(v & ARMED, 0, "a value reached the armed bit");
        if self.0.swap(v, Ordering::AcqRel) & ARMED != 0 {
            futex_wake_u32(self.futex_word(), site);
        }
    }

    /// One of any number of writers advances the value by one, waking an armed
    /// parker. The arm stays the parker's to clear: cleared here in a second RMW,
    /// it could erase an arm taken after the wake.
    #[inline]
    pub(super) fn bump(&self, site: &str) {
        if self.0.fetch_add(1, Ordering::AcqRel) & ARMED != 0 {
            futex_wake_u32(self.futex_word(), site);
        }
    }

    /// Arm, then sleep until a wake unless `still_waiting`, tested behind the arm,
    /// says otherwise. Disarms either way.
    pub(super) fn park(&self, site: &str, still_waiting: impl FnOnce() -> bool) {
        let at = self.arm();
        if still_waiting() {
            futex_wait_u32(self.futex_word(), at as u32, site);
        }
        self.disarm();
    }

    #[inline]
    pub(super) fn futex_word(&self) -> *const AtomicU32 {
        futex_word(&self.0)
    }

    #[cfg(test)]
    pub(super) fn armed(&self) -> bool {
        self.0.load(Ordering::Acquire) & ARMED != 0
    }
}

// ---------------------------------------------------------------------------
// WorkerParks
// ---------------------------------------------------------------------------

/// One worker's park word, alone on its cache line.
#[repr(C, align(64))]
struct Line(Park);

/// Every worker's park: where a worker sleeps for a SAL group or an exchange part,
/// and what the master and its peers wake. Mapped once before the fork, never unmapped.
#[derive(Clone, Copy)]
pub(crate) struct WorkerParks(&'static [Line]);

impl WorkerParks {
    pub(crate) fn create(nw: usize) -> std::io::Result<Self> {
        let base = map_anon_shared((nw * size_of::<Line>()).max(1))?;
        // SAFETY: a fresh page-aligned shared mapping of at least `nw` lines, never
        // unmapped; zeroed is a `Park` nobody sleeps on.
        Ok(WorkerParks(unsafe { std::slice::from_raw_parts(base.cast(), nw) }))
    }

    pub(crate) fn len(self) -> usize {
        self.0.len()
    }

    /// Wake worker `w` if it sleeps.
    pub(crate) fn wake(self, w: usize) {
        self.0[w].0.bump("WorkerParks::wake");
    }

    /// Worker `w`'s park; one per worker process.
    pub(crate) fn park(self, w: usize) -> WorkerPark {
        WorkerPark(&self.0[w].0)
    }

    /// How many wakes worker `w` was sent.
    #[cfg(test)]
    pub(crate) fn wake_seq(self, w: usize) -> u64 {
        self.0[w].0.value()
    }
}

/// A worker's park on the SAL and its exchange rounds, ended by a
/// [`WorkerParks::wake`].
pub(crate) struct WorkerPark(&'static Park);

impl WorkerPark {
    /// Sleep until the next wake, unless `still_empty` says otherwise.
    /// No timeout: `PR_SET_PDEATHSIG` kills the worker when the master dies.
    pub(crate) fn park(&self, still_empty: impl FnOnce() -> bool) {
        self.0.park("WorkerPark::park", still_empty);
    }
}

#[cfg(test)]
#[path = "tests/park.rs"]
mod tests;

#[cfg(test)]
#[path = "benches/park.rs"]
mod bench;
