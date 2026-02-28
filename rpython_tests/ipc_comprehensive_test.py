# ipc_comprehensive_test.py

import os
from rpython.rlib import rposix, rsocket, jit
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_int64, r_uint64, r_ulonglonglong as r_uint128, intmask

from gnitz.core import types, values, batch, errors
from gnitz.storage import buffer, mmap_posix
from gnitz.server import ipc, ipc_ffi, executor

# ------------------------------------------------------------------------------
# Assertions & Helpers
# ------------------------------------------------------------------------------

def assert_true(condition, msg):
    if not condition:
        os.write(2, "FAIL: " + msg + "\n")
        raise Exception("Assertion Failed")

def assert_equal_i(expected, actual, msg):
    if expected != actual:
        os.write(2, "FAIL: " + msg + "\n")
        raise Exception("Value Mismatch")

def make_test_schema():
    cols = [
        types.ColumnDefinition(types.TYPE_U64, name="id"),
        types.ColumnDefinition(types.TYPE_STRING, name="data")
    ]
    return types.TableSchema(cols, 0)

# ------------------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------------------

def test_unowned_buffer_lifecycle():
    """Verifies that unowned buffers inhibit growth and respect physical limits."""
    os.write(1, "[IPC] Testing Unowned Buffer Lifecycle...\n")
    
    # 1. Manual allocation to simulate external memory
    size = 128
    raw_mem = lltype.malloc(rffi.CCHARP.TO, size, flavor='raw')
    
    try:
        # Wrap the memory in a non-owning buffer
        buf = buffer.Buffer.from_external_ptr(raw_mem, size)
        assert_true(not buf.is_owned, "Buffer should be unowned")
        assert_true(not buf.growable, "Unowned buffer should not be growable")
        
        # Success allocation
        ptr = buf.alloc(64, alignment=1)
        assert_equal_i(rffi.cast(lltype.Signed, raw_mem), rffi.cast(lltype.Signed, ptr), "Pointer mismatch")
        
        # Fail allocation (Overflow)
        raised = False
        try:
            buf.alloc(128)
        except errors.MemTableFullError:
            raised = True
        assert_true(raised, "Unowned buffer failed to block overflow")
        
        # Free should neutralize but not release raw_mem
        buf.free()
        assert_true(buf.base_ptr == lltype.nullptr(rffi.CCHARP.TO), "Base ptr not neutralized")
        
        # Verify raw_mem is still accessible (if we were owned, this would be a UAF/Segfault)
        raw_mem[0] = 'A'
        assert_true(raw_mem[0] == 'A', "Underlying memory corrupted after buffer free")
        
    finally:
        lltype.free(raw_mem, flavor='raw')

def test_ipc_fd_hardening():
    """Verifies that the C-FFI layer handles FD passing and prevents leaks."""
    os.write(1, "[IPC] Testing Hardened FD Passing (SCM_RIGHTS)...\n")
    
    # Create a SEQPACKET socket pair
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
    
    try:
        # 1. Create a dummy file descriptor to send
        fd_to_send = mmap_posix.memfd_create_c("test_fd")
        mmap_posix.ftruncate_c(fd_to_send, 1024)
        
        # Send via s1
        res = ipc_ffi.send_fd(s1.fd, fd_to_send)
        assert_equal_i(0, res, "Failed to send FD")
        
        # Receive via s2
        received_fd = ipc_ffi.recv_fd(s2.fd)
        assert_true(received_fd >= 0, "Failed to receive FD")
        
        # Verify it is the same logical file (check size)
        assert_equal_i(1024, mmap_posix.fget_size(received_fd), "FD content mismatch")
        
        os.close(fd_to_send)
        os.close(received_fd)
    finally:
        s1.close()
        s2.close()

def test_ipc_bounds_and_forgery():
    """Verifies that the receiver detects malicious/overflowing headers."""
    os.write(1, "[IPC] Testing Header Validation & Overflow Protection...\n")
    
    schema = make_test_schema()
    fd = mmap_posix.memfd_create_c("malicious_header")
    
    try:
        # Total size is 1KB
        total_sz = 1024
        mmap_posix.ftruncate_c(fd, total_sz)
        ptr = mmap_posix.mmap_file(fd, total_sz, prot=mmap_posix.PROT_READ | mmap_posix.PROT_WRITE)
        
        # 1. Write valid magic but malicious primary_sz (overflow attempt)
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, ipc.OFF_MAGIC))[0] = ipc.MAGIC_IPC
        
        # primary_sz = large value that would cause offset + size > total_size
        rffi.cast(rffi.ULONGLONGP, rffi.ptradd(ptr, ipc.OFF_PRIMARY_SZ))[0] = r_uint64(2000)
        
        # We need a socket to use receive_payload. socketpair is easiest.
        s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
        try:
            ipc_ffi.send_fd(s1.fd, fd)
            
            raised = False
            try:
                ipc.receive_payload(s2.fd, schema)
            except errors.StorageError:
                # Expected: primary_sz > (total_size - primary_off)
                raised = True
            assert_true(raised, "IPC layer failed to detect primary arena overflow")
        finally:
            s1.close()
            s2.close()
            mmap_posix.munmap_file(ptr, total_sz)
    finally:
        os.close(fd)

def test_zero_copy_roundtrip():
    """Verifies full serialization, transport, and reconstruction."""
    os.write(1, "[IPC] Testing Zero-Copy Roundtrip...\n")
    
    schema = make_test_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
    
    try:
        # 1. Create a batch with variable length data
        zbatch = batch.ArenaZSetBatch(schema, initial_capacity=10)
        row = values.make_payload_row(schema)
        row.append_string("Hello Zero-Copy World")
        zbatch.append(r_uint128(42), r_int64(1), row)
        
        # 2. Send through IPC
        ipc.send_batch(s1.fd, zbatch, status=0, error_msg="Success")
        
        # 3. Receive and Reconstruct
        payload = ipc.receive_payload(s2.fd, schema)
        
        # Verify Metadata
        assert_equal_i(0, payload.status, "Status mismatch")
        assert_true(payload.error_msg == "Success", "Error msg mismatch")
        
        # Verify Batch Contents
        rec_batch = payload.batch
        assert_equal_i(1, rec_batch.length(), "Batch count mismatch")
        assert_true(rec_batch.primary_arena.is_owned == False, "Received batch should use unowned views")
        
        acc = rec_batch.get_accessor(0)
        assert_true(acc.get_str_struct(1)[4] is None, "Accessor should point to raw memory, not Python str")
        
        # Verify string content
        row_recovered = rec_batch.get_row(0)
        assert_true(row_recovered.get_str(0) == "Hello Zero-Copy World", "String corruption in transport")
        
        payload.close()
        zbatch.free()
    finally:
        s1.close()
        s2.close()

def test_executor_error_path():
    """Verifies that VM errors are sent without dummy batch allocations."""
    os.write(1, "[IPC] Testing Executor Error Propagation...\n")
    
    schema = make_test_schema()
    s1, s2 = rsocket.socketpair(rsocket.AF_UNIX, rsocket.SOCK_SEQPACKET)
    
    try:
        # Mocking an error send directly
        ipc.send_error(s1.fd, "Fatal VM Crash")
        
        payload = ipc.receive_payload(s2.fd, schema)
        assert_equal_i(1, payload.status, "Error status mismatch")
        assert_true(payload.error_msg == "Fatal VM Crash", "Error string mismatch")
        assert_equal_i(0, payload.batch.length(), "Error payload should have 0 records")
        
        payload.close()
    finally:
        s1.close()
        s2.close()

# ------------------------------------------------------------------------------
# Entry Point
# ------------------------------------------------------------------------------

def entry_point(argv):
    os.write(1, "--- GnitzDB IPC Transport Test ---\n")
    try:
        test_unowned_buffer_lifecycle()
        test_ipc_fd_hardening()
        test_ipc_bounds_and_forgery()
        test_zero_copy_roundtrip()
        test_executor_error_path()
        os.write(1, "\nALL IPC TRANSPORT TESTS PASSED\n")
    except Exception as e:
        # Basic RPython error logging
        os.write(2, "TESTING FAILED\n")
        return 1
    return 0

def target(driver, args):
    return entry_point, None

if __name__ == "__main__":
    import sys
    entry_point(sys.argv)
