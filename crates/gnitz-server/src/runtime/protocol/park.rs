//! The futex park every transport sleeps through: [`Park`], one word a side
//! sleeps on and its peer wakes, and [`WorkerParks`], the mapping where each
//! worker sleeps for a SAL group or an exchange part.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use gnitz_foundation::posix_io;
use gnitz_wire::MAX_WORKERS;

// ---------------------------------------------------------------------------
// Futex wait/wake
// ---------------------------------------------------------------------------

/// One v1 futex call on `word`, with no `FUTEX_PRIVATE_FLAG` — a park word is
/// shared: `Err` holds the errno.
#[inline]
fn futex(word: *const AtomicU32, op: libc::c_int, val: u32) -> Result<(), i32> {
    let rc = unsafe {
        libc::syscall(
            libc::SYS_futex,
            word as *const libc::c_void,
            op,
            val as libc::c_int,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0u32,
        )
    };
    match rc < 0 {
        // `libc::syscall` returns -1, not -errno.
        true => Err(std::io::Error::last_os_error().raw_os_error().unwrap_or(0)),
        false => Ok(()),
    }
}

/// Wake the waiter parked on `word`. Aborts rather than reporting: a lost wake
/// leaves the peer blocked on a queue that has work in it, so there is no
/// caller-side answer to distinguish. `site` names the caller in the abort.
#[inline]
fn futex_wake(word: *const AtomicU32, site: &str) {
    if let Err(e) = futex(word, libc::FUTEX_WAKE, 1) {
        futex_failed(site, e);
    }
}

/// Outlined so `site` is not spilled onto the stack on the paths that never
/// abort.
#[cold]
#[inline(never)]
fn futex_failed(site: &str, errno: i32) -> ! {
    gnitz_fatal_abort!("{}: futex failed: errno={}", site, errno);
}

// ---------------------------------------------------------------------------
// Park
// ---------------------------------------------------------------------------

/// Bit 63 of a park word: the parker is armed. Outside the low half a futex compares.
const ARMED: u64 = 1 << 63;

/// One side's sleep, in one word: a value in bits 0..63 that only grows, and the
/// parker's arm in bit 63. The parker arms, re-tests what it waits for, and
/// sleeps on the value the arm returned; a peer wakes it by moving that value.
/// The two kinds of wake order against the arm differently.
///
/// - **An RMW wake** ([`Self::publish`], [`Self::bump`]): the arm and every
///   advance are RMWs on the one atomic, so they are totally ordered: an advance
///   behind the arm sees it and wakes, and an arm behind the advance reads
///   everything the advance released, so the parker's re-test sees it.
/// - **A load wake** ([`Self::take_arm`]): the peer stores the word the parker
///   waits on and then loads the park; the parker arms and then loads that word.
///   All four are SeqCst, so in each pair at least one side sees the other: the
///   peer sees the arm and takes it, or the parker's re-test sees the stored word
///   and does not sleep. A wake that finds no arm costs no RMW.
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
        self.0.fetch_or(ARMED, Ordering::SeqCst) & !ARMED
    }

    /// Drop the arm. The result is deliberately unused: binding it would force a
    /// `lock cmpxchg` retry loop where a discarded one lowers to a `lock and`.
    #[inline]
    pub(super) fn disarm(&self) {
        self.0.fetch_and(!ARMED, Ordering::SeqCst);
    }

    /// Take the arm, if set, and wake the parker: the load wake, called after the
    /// caller's SeqCst store of the word the parker re-tests.
    ///
    /// One CAS clears the arm and advances the value by one, so an arm has one
    /// taker, and a take ahead of the parker's futex wait still moves the value
    /// it compares. A failed CAS read a value later than the load's: another
    /// take, which woke the parker, or a disarm or a newer arm, which is ordered
    /// after the load and so followed by a re-test that sees the caller's word.
    /// So a failed CAS neither retries nor wakes.
    #[inline]
    pub(super) fn take_arm(&self, site: &str) {
        let cur = self.0.load(Ordering::SeqCst);
        if cur & ARMED != 0
            && self
                .0
                .compare_exchange(cur, (cur + 1) & !ARMED, Ordering::SeqCst, Ordering::Relaxed)
                .is_ok()
        {
            futex_wake(self.futex_word(), site);
        }
    }

    /// The value's one writer stores `v`, waking the parker iff it armed first.
    /// The swap takes the arm, so later publishes spend no syscall until a re-arm.
    #[inline]
    pub(super) fn publish(&self, v: u64, site: &str) {
        debug_assert_eq!(v & ARMED, 0, "a value reached the armed bit");
        if self.0.swap(v, Ordering::AcqRel) & ARMED != 0 {
            futex_wake(self.futex_word(), site);
        }
    }

    /// One of any number of writers advances the value by one, waking an armed
    /// parker. The arm stays the parker's to clear: cleared here in a second RMW,
    /// it could erase an arm taken after the wake.
    #[inline]
    pub(super) fn bump(&self, site: &str) {
        if self.0.fetch_add(1, Ordering::AcqRel) & ARMED != 0 {
            futex_wake(self.futex_word(), site);
        }
    }

    /// Arm, then sleep until a wake unless `still_waiting`, tested behind the arm,
    /// says otherwise. Disarms either way.
    pub(super) fn park(&self, site: &str, still_waiting: impl FnOnce() -> bool) {
        let at = self.arm();
        // A wake, a moved value and a signal all return; anything else aborts.
        if still_waiting() {
            if let Err(e) = futex(self.futex_word(), libc::FUTEX_WAIT, at as u32) {
                if e != libc::EAGAIN && e != libc::EINTR {
                    futex_failed(site, e);
                }
            }
        }
        self.disarm();
    }

    /// The 32-bit futex word aliasing the low half. A ring cursor advances by at
    /// most its ring's capacity, and a wake sequence or a take by one, so under
    /// 2^32 an advance always moves the half a futex compares.
    #[inline]
    pub(super) fn futex_word(&self) -> *const AtomicU32 {
        &self.0 as *const AtomicU64 as *const AtomicU32
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
        assert!(nw <= MAX_WORKERS, "{nw} workers: a WorkerSet holds at most MAX_WORKERS");
        let base = posix_io::map_shared(None, (nw * size_of::<Line>()).max(1))?;
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

    /// Worker `w`'s park word: its value is how many wakes `w` was sent.
    #[cfg(test)]
    pub(super) fn word(self, w: usize) -> &'static Park {
        &self.0[w].0
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
