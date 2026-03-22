# gnitz/server/uring_ffi.py
#
# Raw io_uring FFI for RPython.  No liburing dependency — uses syscalls
# 425/426/427 directly.  All ring internals (memory barriers, SQE layout)
# are handled in C; RPython sees an opaque ring handle.
#
# Target: Linux x86_64, kernel >= 6.16 (IORING_SETUP_NO_SQARRAY).
# Falls back gracefully (StorageError) on older kernels or restricted
# environments (containers with io_uring disabled).

from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.translator.tool.cbuild import ExternalCompilationInfo
from rpython.rlib import jit
from rpython.rlib.rarithmetic import intmask, r_uint64
from rpython.rlib.objectmodel import newlist_hint

from gnitz.core.errors import StorageError

# ---------------------------------------------------------------------------
# CQE flag constants (exported for Step 5)
# ---------------------------------------------------------------------------

CQE_F_MORE = 1 << 1  # Multishot: parent SQE will generate more CQEs

# ---------------------------------------------------------------------------
# Inline C implementation
# ---------------------------------------------------------------------------

URING_C_CODE = """
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <sys/syscall.h>
#include <sys/mman.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <stdlib.h>

/* ---- io_uring constants (stable kernel UAPI, no header needed) ---- */

#define GNITZ_NR_IO_URING_SETUP    425
#define GNITZ_NR_IO_URING_ENTER    426
#define GNITZ_NR_IO_URING_REGISTER 427

/* Opcodes */
#define GNITZ_IORING_OP_NOP     0
#define GNITZ_IORING_OP_ACCEPT 13
#define GNITZ_IORING_OP_READ   22
#define GNITZ_IORING_OP_WRITE  23
#define GNITZ_IORING_OP_SEND   26
#define GNITZ_IORING_OP_RECV   27

/* Setup flags */
#define GNITZ_IORING_SETUP_NO_SQARRAY  (1U << 16)

/* Enter flags */
#define GNITZ_IORING_ENTER_GETEVENTS   (1U << 0)

/* Feature flags (returned by kernel in params.features) */
#define GNITZ_IORING_FEAT_SINGLE_MMAP  (1U << 0)

/* Accept flags (stored in sqe->ioprio) */
#define GNITZ_IORING_ACCEPT_MULTISHOT  (1U << 0)

/* Mmap offsets */
#define GNITZ_IORING_OFF_SQ_RING  0x0ULL
#define GNITZ_IORING_OFF_SQES     0x10000000ULL

/* SQE size */
#define GNITZ_SQE_SIZE 64

/* CQE size */
#define GNITZ_CQE_SIZE 16

/* ---- Inline struct definitions (match kernel UAPI layout) ---- */

struct gnitz_io_sqring_offsets {
    uint32_t head;
    uint32_t tail;
    uint32_t ring_mask;
    uint32_t ring_entries;
    uint32_t flags;
    uint32_t dropped;
    uint32_t array;
    uint32_t resv1;
    uint64_t user_addr;
};

struct gnitz_io_cqring_offsets {
    uint32_t head;
    uint32_t tail;
    uint32_t ring_mask;
    uint32_t ring_entries;
    uint32_t overflow;
    uint32_t cqes;
    uint32_t flags;
    uint32_t resv1;
    uint64_t user_addr;
};

struct gnitz_io_uring_params {
    uint32_t sq_entries;
    uint32_t cq_entries;
    uint32_t flags;
    uint32_t sq_thread_cpu;
    uint32_t sq_thread_idle;
    uint32_t features;
    uint32_t wq_fd;
    uint32_t resv[3];
    struct gnitz_io_sqring_offsets sq_off;
    struct gnitz_io_cqring_offsets cq_off;
};

/* ---- Ring state (opaque to RPython) ---- */

struct gnitz_uring {
    int ring_fd;
    /* SQ state */
    uint32_t *sq_head;       /* kernel-updated */
    uint32_t *sq_tail;       /* userspace-updated */
    uint32_t sq_mask;
    uint32_t sq_entries;
    char     *sqes;          /* SQE array base */
    /* CQ state */
    uint32_t *cq_head;       /* userspace-updated */
    uint32_t *cq_tail;       /* kernel-updated */
    uint32_t cq_mask;
    char     *cqes;          /* CQE array base */
    /* Bookkeeping for munmap */
    char   *sq_ring_ptr;
    size_t  sq_ring_size;
    char   *sqes_ptr;
    size_t  sqes_size;
};

/* ---- Memory barriers ---- */

static inline void store_release_u32(uint32_t *p, uint32_t v) {
    __atomic_store_n(p, v, __ATOMIC_RELEASE);
}

static inline uint32_t load_acquire_u32(const uint32_t *p) {
    return __atomic_load_n(p, __ATOMIC_ACQUIRE);
}

/* ---- Internal SQE helpers ---- */

static char *_get_sqe(struct gnitz_uring *r) {
    uint32_t head = load_acquire_u32(r->sq_head);
    uint32_t tail = *r->sq_tail;
    if (tail - head >= r->sq_entries) return NULL;
    char *sqe = r->sqes + (uint64_t)(tail & r->sq_mask) * GNITZ_SQE_SIZE;
    memset(sqe, 0, GNITZ_SQE_SIZE);
    return sqe;
}

static void _advance_sq_tail(struct gnitz_uring *r) {
    store_release_u32(r->sq_tail, *r->sq_tail + 1);
}

/* ---- Exported functions ---- */

void *gnitz_uring_create(int entries) {
    struct gnitz_io_uring_params params;
    memset(&params, 0, sizeof(params));
    params.flags = GNITZ_IORING_SETUP_NO_SQARRAY;

    int fd = (int)syscall(GNITZ_NR_IO_URING_SETUP,
                          (unsigned)entries, &params);
    if (fd < 0) return NULL;

    /* Require SINGLE_MMAP — SQ + CQ share one mmap region */
    if (!(params.features & GNITZ_IORING_FEAT_SINGLE_MMAP)) {
        close(fd);
        return NULL;
    }

    /* Compute mmap size: must cover both SQ and CQ regions.
       With SINGLE_MMAP, the CQ ring is embedded in the SQ ring mmap.
       Size = max(end of SQ array region, end of CQ entries region). */
    size_t sq_end = params.sq_off.array
                    + params.sq_entries * sizeof(uint32_t);
    size_t cq_end = params.cq_off.cqes
                    + params.cq_entries * GNITZ_CQE_SIZE;
    size_t ring_size = sq_end > cq_end ? sq_end : cq_end;

    char *ring_ptr = (char *)mmap(NULL, ring_size,
                                  PROT_READ | PROT_WRITE,
                                  MAP_SHARED | MAP_POPULATE,
                                  fd, GNITZ_IORING_OFF_SQ_RING);
    if (ring_ptr == MAP_FAILED) {
        close(fd);
        return NULL;
    }

    size_t sqes_size = params.sq_entries * GNITZ_SQE_SIZE;
    char *sqes_ptr = (char *)mmap(NULL, sqes_size,
                                  PROT_READ | PROT_WRITE,
                                  MAP_SHARED | MAP_POPULATE,
                                  fd, GNITZ_IORING_OFF_SQES);
    if (sqes_ptr == MAP_FAILED) {
        munmap(ring_ptr, ring_size);
        close(fd);
        return NULL;
    }

    struct gnitz_uring *r = (struct gnitz_uring *)malloc(sizeof(*r));
    if (!r) {
        munmap(sqes_ptr, sqes_size);
        munmap(ring_ptr, ring_size);
        close(fd);
        return NULL;
    }

    r->ring_fd     = fd;
    r->sq_head     = (uint32_t *)(ring_ptr + params.sq_off.head);
    r->sq_tail     = (uint32_t *)(ring_ptr + params.sq_off.tail);
    r->sq_mask     = *(uint32_t *)(ring_ptr + params.sq_off.ring_mask);
    r->sq_entries  = *(uint32_t *)(ring_ptr + params.sq_off.ring_entries);
    r->sqes        = sqes_ptr;

    r->cq_head     = (uint32_t *)(ring_ptr + params.cq_off.head);
    r->cq_tail     = (uint32_t *)(ring_ptr + params.cq_off.tail);
    r->cq_mask     = *(uint32_t *)(ring_ptr + params.cq_off.ring_mask);
    r->cqes        = ring_ptr + params.cq_off.cqes;

    r->sq_ring_ptr  = ring_ptr;
    r->sq_ring_size = ring_size;
    r->sqes_ptr     = sqes_ptr;
    r->sqes_size    = sqes_size;

    return (void *)r;
}

void gnitz_uring_destroy(void *ring) {
    if (!ring) return;
    struct gnitz_uring *r = (struct gnitz_uring *)ring;
    munmap(r->sqes_ptr, r->sqes_size);
    munmap(r->sq_ring_ptr, r->sq_ring_size);
    close(r->ring_fd);
    free(r);
}

int gnitz_uring_prep_nop(void *ring, unsigned long long user_data) {
    struct gnitz_uring *r = (struct gnitz_uring *)ring;
    char *sqe = _get_sqe(r);
    if (!sqe) return -1;
    sqe[0] = GNITZ_IORING_OP_NOP;                   /* +0: opcode */
    *(uint64_t *)(sqe + 32) = user_data;             /* +32: user_data */
    _advance_sq_tail(r);
    return 0;
}

int gnitz_uring_prep_accept(void *ring, int fd,
                            unsigned long long user_data) {
    struct gnitz_uring *r = (struct gnitz_uring *)ring;
    char *sqe = _get_sqe(r);
    if (!sqe) return -1;
    sqe[0] = GNITZ_IORING_OP_ACCEPT;                /* +0: opcode */
    *(uint16_t *)(sqe + 2)  = GNITZ_IORING_ACCEPT_MULTISHOT; /* +2: ioprio */
    *(int32_t  *)(sqe + 4)  = fd;                    /* +4: fd */
    *(uint64_t *)(sqe + 32) = user_data;             /* +32: user_data */
    _advance_sq_tail(r);
    return 0;
}

int gnitz_uring_prep_recv(void *ring, int fd, char *buf,
                          unsigned int len,
                          unsigned long long user_data) {
    struct gnitz_uring *r = (struct gnitz_uring *)ring;
    char *sqe = _get_sqe(r);
    if (!sqe) return -1;
    sqe[0] = GNITZ_IORING_OP_RECV;                  /* +0: opcode */
    *(int32_t  *)(sqe + 4)  = fd;                    /* +4: fd */
    *(uint64_t *)(sqe + 16) = (uint64_t)(uintptr_t)buf; /* +16: addr */
    *(uint32_t *)(sqe + 24) = len;                   /* +24: len */
    *(uint64_t *)(sqe + 32) = user_data;             /* +32: user_data */
    _advance_sq_tail(r);
    return 0;
}

int gnitz_uring_prep_send(void *ring, int fd, const char *buf,
                          unsigned int len,
                          unsigned long long user_data) {
    struct gnitz_uring *r = (struct gnitz_uring *)ring;
    char *sqe = _get_sqe(r);
    if (!sqe) return -1;
    sqe[0] = GNITZ_IORING_OP_SEND;                  /* +0: opcode */
    *(int32_t  *)(sqe + 4)  = fd;                    /* +4: fd */
    *(uint64_t *)(sqe + 16) = (uint64_t)(uintptr_t)buf; /* +16: addr */
    *(uint32_t *)(sqe + 24) = len;                   /* +24: len */
    *(uint64_t *)(sqe + 32) = user_data;             /* +32: user_data */
    _advance_sq_tail(r);
    return 0;
}

int gnitz_uring_prep_read(void *ring, int fd, char *buf,
                          unsigned int len, unsigned long long offset,
                          unsigned long long user_data) {
    struct gnitz_uring *r = (struct gnitz_uring *)ring;
    char *sqe = _get_sqe(r);
    if (!sqe) return -1;
    sqe[0] = GNITZ_IORING_OP_READ;                  /* +0: opcode */
    *(int32_t  *)(sqe + 4)  = fd;                    /* +4: fd */
    *(uint64_t *)(sqe + 8)  = offset;                /* +8: off */
    *(uint64_t *)(sqe + 16) = (uint64_t)(uintptr_t)buf; /* +16: addr */
    *(uint32_t *)(sqe + 24) = len;                   /* +24: len */
    *(uint64_t *)(sqe + 32) = user_data;             /* +32: user_data */
    _advance_sq_tail(r);
    return 0;
}

int gnitz_uring_prep_write(void *ring, int fd, const char *buf,
                           unsigned int len, unsigned long long offset,
                           unsigned long long user_data) {
    struct gnitz_uring *r = (struct gnitz_uring *)ring;
    char *sqe = _get_sqe(r);
    if (!sqe) return -1;
    sqe[0] = GNITZ_IORING_OP_WRITE;                 /* +0: opcode */
    *(int32_t  *)(sqe + 4)  = fd;                    /* +4: fd */
    *(uint64_t *)(sqe + 8)  = offset;                /* +8: off */
    *(uint64_t *)(sqe + 16) = (uint64_t)(uintptr_t)buf; /* +16: addr */
    *(uint32_t *)(sqe + 24) = len;                   /* +24: len */
    *(uint64_t *)(sqe + 32) = user_data;             /* +32: user_data */
    _advance_sq_tail(r);
    return 0;
}

int gnitz_uring_submit_and_wait(void *ring, int min_complete) {
    struct gnitz_uring *r = (struct gnitz_uring *)ring;
    uint32_t to_submit = *r->sq_tail - load_acquire_u32(r->sq_head);
    unsigned flags = 0;
    if (min_complete > 0) flags |= GNITZ_IORING_ENTER_GETEVENTS;

    int ret;
    do {
        ret = (int)syscall(GNITZ_NR_IO_URING_ENTER,
                           r->ring_fd, to_submit, (unsigned)min_complete,
                           flags, (void *)0, (size_t)0);
    } while (ret < 0 && errno == EINTR);

    return ret < 0 ? -errno : ret;
}

int gnitz_uring_drain(void *ring,
                      void *raw_udata,
                      int *out_res,
                      unsigned int *out_flags,
                      int max) {
    struct gnitz_uring *r = (struct gnitz_uring *)ring;
    uint64_t *out_udata = (uint64_t *)raw_udata;
    uint32_t head = *r->cq_head;
    uint32_t tail = load_acquire_u32(r->cq_tail);
    int count = 0;

    while (head != tail && count < max) {
        char *cqe = r->cqes + (uint64_t)(head & r->cq_mask) * GNITZ_CQE_SIZE;
        out_udata[count] = *(uint64_t *)(cqe + 0);   /* +0: user_data */
        out_res[count]   = *(int32_t  *)(cqe + 8);   /* +8: res */
        out_flags[count] = *(uint32_t *)(cqe + 12);  /* +12: flags */
        head++;
        count++;
    }

    store_release_u32(r->cq_head, head);
    return count;
}
"""

# ---------------------------------------------------------------------------
# ExternalCompilationInfo
# ---------------------------------------------------------------------------

eci = ExternalCompilationInfo(
    pre_include_bits=[
        "void *gnitz_uring_create(int entries);",
        "void gnitz_uring_destroy(void *ring);",
        "int gnitz_uring_prep_nop(void *ring, unsigned long long user_data);",
        "int gnitz_uring_prep_accept(void *ring, int fd, unsigned long long user_data);",
        "int gnitz_uring_prep_recv(void *ring, int fd, char *buf, unsigned int len, unsigned long long user_data);",
        "int gnitz_uring_prep_send(void *ring, int fd, const char *buf, unsigned int len, unsigned long long user_data);",
        "int gnitz_uring_prep_read(void *ring, int fd, char *buf, unsigned int len, unsigned long long offset, unsigned long long user_data);",
        "int gnitz_uring_prep_write(void *ring, int fd, const char *buf, unsigned int len, unsigned long long offset, unsigned long long user_data);",
        "int gnitz_uring_submit_and_wait(void *ring, int min_complete);",
        "int gnitz_uring_drain(void *ring, void *raw_udata, int *out_res, unsigned int *out_flags, int max);",
    ],
    separate_module_sources=[URING_C_CODE],
    includes=[
        "sys/syscall.h", "sys/mman.h", "unistd.h", "string.h",
        "stdint.h", "errno.h", "stdlib.h",
    ],
)

# ---------------------------------------------------------------------------
# Raw FFI bindings (private)
# ---------------------------------------------------------------------------

_gnitz_uring_create = rffi.llexternal(
    "gnitz_uring_create", [rffi.INT], rffi.VOIDP,
    compilation_info=eci,
)

_gnitz_uring_destroy = rffi.llexternal(
    "gnitz_uring_destroy", [rffi.VOIDP], lltype.Void,
    compilation_info=eci,
)

_gnitz_uring_prep_nop = rffi.llexternal(
    "gnitz_uring_prep_nop", [rffi.VOIDP, rffi.ULONGLONG], rffi.INT,
    compilation_info=eci,
)

_gnitz_uring_prep_accept = rffi.llexternal(
    "gnitz_uring_prep_accept", [rffi.VOIDP, rffi.INT, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_gnitz_uring_prep_recv = rffi.llexternal(
    "gnitz_uring_prep_recv",
    [rffi.VOIDP, rffi.INT, rffi.CCHARP, rffi.UINT, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_gnitz_uring_prep_send = rffi.llexternal(
    "gnitz_uring_prep_send",
    [rffi.VOIDP, rffi.INT, rffi.CCHARP, rffi.UINT, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_gnitz_uring_prep_read = rffi.llexternal(
    "gnitz_uring_prep_read",
    [rffi.VOIDP, rffi.INT, rffi.CCHARP, rffi.UINT,
     rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_gnitz_uring_prep_write = rffi.llexternal(
    "gnitz_uring_prep_write",
    [rffi.VOIDP, rffi.INT, rffi.CCHARP, rffi.UINT,
     rffi.ULONGLONG, rffi.ULONGLONG],
    rffi.INT,
    compilation_info=eci,
)

_gnitz_uring_submit_and_wait = rffi.llexternal(
    "gnitz_uring_submit_and_wait", [rffi.VOIDP, rffi.INT], rffi.INT,
    compilation_info=eci,
)

_gnitz_uring_drain = rffi.llexternal(
    "gnitz_uring_drain",
    [rffi.VOIDP, rffi.VOIDP, rffi.INTP, rffi.UINTP, rffi.INT],
    rffi.INT,
    compilation_info=eci,
)

# ---------------------------------------------------------------------------
# Public RPython wrappers
# ---------------------------------------------------------------------------


@jit.dont_look_inside
def uring_create(entries):
    """Create an io_uring instance with the given number of SQ entries.

    Returns an opaque ring handle (rffi.VOIDP).  Raises StorageError if
    io_uring is unavailable (old kernel, container restrictions, etc.).
    """
    ring = _gnitz_uring_create(rffi.cast(rffi.INT, entries))
    if not ring:
        raise StorageError("io_uring_setup failed")
    return ring


@jit.dont_look_inside
def uring_destroy(ring):
    """Destroy an io_uring instance: munmap rings, close fd, free state."""
    _gnitz_uring_destroy(ring)


@jit.dont_look_inside
def uring_prep_nop(ring, user_data):
    """Prepare a NOP submission.  Raises StorageError if the SQ is full."""
    ret = _gnitz_uring_prep_nop(
        ring, rffi.cast(rffi.ULONGLONG, user_data))
    if rffi.cast(lltype.Signed, ret) < 0:
        raise StorageError("io_uring SQ full (prep_nop)")


@jit.dont_look_inside
def uring_prep_accept(ring, fd, user_data):
    """Prepare a multishot ACCEPT.  Raises StorageError if the SQ is full."""
    ret = _gnitz_uring_prep_accept(
        ring, rffi.cast(rffi.INT, fd),
        rffi.cast(rffi.ULONGLONG, user_data))
    if rffi.cast(lltype.Signed, ret) < 0:
        raise StorageError("io_uring SQ full (prep_accept)")


@jit.dont_look_inside
def uring_prep_recv(ring, fd, buf, length, user_data):
    """Prepare a RECV into buf.  Raises StorageError if the SQ is full."""
    ret = _gnitz_uring_prep_recv(
        ring, rffi.cast(rffi.INT, fd),
        buf, rffi.cast(rffi.UINT, length),
        rffi.cast(rffi.ULONGLONG, user_data))
    if rffi.cast(lltype.Signed, ret) < 0:
        raise StorageError("io_uring SQ full (prep_recv)")


@jit.dont_look_inside
def uring_prep_send(ring, fd, buf, length, user_data):
    """Prepare a SEND from buf.  Raises StorageError if the SQ is full."""
    ret = _gnitz_uring_prep_send(
        ring, rffi.cast(rffi.INT, fd),
        buf, rffi.cast(rffi.UINT, length),
        rffi.cast(rffi.ULONGLONG, user_data))
    if rffi.cast(lltype.Signed, ret) < 0:
        raise StorageError("io_uring SQ full (prep_send)")


@jit.dont_look_inside
def uring_prep_read(ring, fd, buf, length, offset, user_data):
    """Prepare a READ from a file at the given offset.  Raises on SQ full."""
    ret = _gnitz_uring_prep_read(
        ring, rffi.cast(rffi.INT, fd),
        buf, rffi.cast(rffi.UINT, length),
        rffi.cast(rffi.ULONGLONG, offset),
        rffi.cast(rffi.ULONGLONG, user_data))
    if rffi.cast(lltype.Signed, ret) < 0:
        raise StorageError("io_uring SQ full (prep_read)")


@jit.dont_look_inside
def uring_prep_write(ring, fd, buf, length, offset, user_data):
    """Prepare a WRITE to a file at the given offset.  Raises on SQ full."""
    ret = _gnitz_uring_prep_write(
        ring, rffi.cast(rffi.INT, fd),
        buf, rffi.cast(rffi.UINT, length),
        rffi.cast(rffi.ULONGLONG, offset),
        rffi.cast(rffi.ULONGLONG, user_data))
    if rffi.cast(lltype.Signed, ret) < 0:
        raise StorageError("io_uring SQ full (prep_write)")


@jit.dont_look_inside
def uring_submit_and_wait(ring, min_complete):
    """Submit pending SQEs and optionally wait for completions.

    Returns the number of SQEs submitted.  Raises StorageError on failure.
    """
    ret = _gnitz_uring_submit_and_wait(
        ring, rffi.cast(rffi.INT, min_complete))
    r = rffi.cast(lltype.Signed, ret)
    if r < 0:
        raise StorageError("io_uring_enter failed")
    return intmask(r)


@jit.dont_look_inside
def uring_drain(ring, max_cqes):
    """Drain up to max_cqes completions from the CQ ring.

    Returns (count, udata_list, res_list, flags_list) where:
      udata_list: list[r_uint64] — user_data from each CQE
      res_list:   list[int]      — result code (fd for accept, bytes for I/O)
      flags_list: list[int]      — CQE flags (check CQE_F_MORE for multishot)
    """
    c_udata = lltype.malloc(rffi.ULONGLONGP.TO, max_cqes, flavor='raw')
    c_res = lltype.malloc(rffi.INTP.TO, max_cqes, flavor='raw')
    c_flags = lltype.malloc(rffi.UINTP.TO, max_cqes, flavor='raw')

    try:
        n = intmask(_gnitz_uring_drain(
            ring, rffi.cast(rffi.VOIDP, c_udata), c_res, c_flags,
            rffi.cast(rffi.INT, max_cqes)))

        udata_list = newlist_hint(n)
        res_list = newlist_hint(n)
        flags_list = newlist_hint(n)
        for i in range(n):
            udata_list.append(r_uint64(c_udata[i]))
            res_list.append(intmask(c_res[i]))
            flags_list.append(intmask(c_flags[i]))
    finally:
        lltype.free(c_udata, flavor='raw')
        lltype.free(c_res, flavor='raw')
        lltype.free(c_flags, flavor='raw')

    return n, udata_list, res_list, flags_list
