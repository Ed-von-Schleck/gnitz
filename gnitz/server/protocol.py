# gnitz/server/protocol.py

from rpython.rlib.rarithmetic import r_uint32, r_uint64, intmask
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rstring import StringBuilder

STATUS_OK = 0
STATUS_ERROR = 1
STATUS_YIELD = 2

class ProtocolError(Exception):
    def __init__(self, msg):
        self.msg = msg

class ProtocolEnvelope(object):
    """
    Zero-copy protocol envelope for local IPC.
    The payload is maintained as a raw C pointer to avoid allocating
    massive strings in the RPython GC.
    """
    _immutable_fields_ =['status', 'error_msg', 'payload_size', 'payload_ptr']

    def __init__(self, status, error_msg, payload_size, payload_ptr):
        self.status = status
        self.error_msg = error_msg
        self.payload_size = payload_size
        self.payload_ptr = payload_ptr

    def has_payload(self):
        return self.payload_size > 0 and self.payload_ptr != lltype.nullptr(rffi.CCHARP.TO)


def _read_u32_le(ptr, offset):
    b0 = r_uint32(ord(ptr[offset]))
    b1 = r_uint32(ord(ptr[offset + 1]))
    b2 = r_uint32(ord(ptr[offset + 2]))
    b3 = r_uint32(ord(ptr[offset + 3]))
    return b0 | (b1 << 8) | (b2 << 16) | (b3 << 24)


def _read_u64_le(ptr, offset):
    val = r_uint64(0)
    for i in range(8):
        b = r_uint64(ord(ptr[offset + i]))
        val |= (b << (i * 8))
    return val


def _write_u32_le(ptr, offset, val):
    val_u32 = r_uint32(val)
    ptr[offset]     = chr(intmask(val_u32 & 0xFF))
    ptr[offset + 1] = chr(intmask((val_u32 >> 8) & 0xFF))
    ptr[offset + 2] = chr(intmask((val_u32 >> 16) & 0xFF))
    ptr[offset + 3] = chr(intmask((val_u32 >> 24) & 0xFF))


def _write_u64_le(ptr, offset, val):
    val_u64 = r_uint64(val)
    for i in range(8):
        ptr[offset + i] = chr(intmask((val_u64 >> (i * 8)) & 0xFF))


def deserialize_envelope(ptr, total_length):
    """
    Parses a protocol envelope from a raw C pointer.
    Validates bounds strictly to prevent buffer overflows.
    """
    if ptr == lltype.nullptr(rffi.CCHARP.TO):
        raise ProtocolError("Cannot deserialize from null pointer")

    if total_length < 13: # 1 (status) + 4 (err_len) + 8 (payload_size)
        raise ProtocolError("Buffer too small for protocol header")

    status = ord(ptr[0])
    err_len = intmask(_read_u32_le(ptr, 1))

    if err_len < 0:
        raise ProtocolError("Corrupt error message length")

    if total_length < 13 + err_len:
        raise ProtocolError("Buffer too small for error message and payload size")

    error_msg = ""
    if err_len > 0:
        error_msg = rffi.charpsize2str(rffi.ptradd(ptr, 5), err_len)

    payload_size_offset = 5 + err_len
    payload_size = _read_u64_le(ptr, payload_size_offset)

    expected_total = r_uint64(payload_size_offset) + 8 + payload_size
    if r_uint64(total_length) < expected_total:
        raise ProtocolError("Buffer smaller than advertised payload size")

    payload_offset = payload_size_offset + 8
    
    if payload_size > 0:
        payload_ptr = rffi.ptradd(ptr, payload_offset)
    else:
        payload_ptr = lltype.nullptr(rffi.CCHARP.TO)

    return ProtocolEnvelope(status, error_msg, payload_size, payload_ptr)


def serialize_header(status, error_msg, payload_size):
    """
    Serializes the header of the protocol envelope into an RPython string.
    This string can be sent over a socket prior to the raw payload,
    achieving a zero-copy send for the payload itself.
    """
    err_len = len(error_msg)
    header_size = 1 + 4 + err_len + 8
    
    builder = StringBuilder(header_size)
    builder.append(chr(status & 0xFF))
    
    val_u32 = r_uint32(err_len)
    builder.append(chr(intmask(val_u32 & 0xFF)))
    builder.append(chr(intmask((val_u32 >> 8) & 0xFF)))
    builder.append(chr(intmask((val_u32 >> 16) & 0xFF)))
    builder.append(chr(intmask((val_u32 >> 24) & 0xFF)))
    
    builder.append(error_msg)
    
    val_u64 = r_uint64(payload_size)
    for i in range(8):
        builder.append(chr(intmask((val_u64 >> (i * 8)) & 0xFF)))
        
    return builder.build()


def serialize_header_into(ptr, status, error_msg, payload_size):
    """
    Serializes the header directly into a raw C pointer.
    Returns the number of bytes written (the header size).
    Assumes the caller has ensured sufficient capacity at `ptr`.
    """
    ptr[0] = chr(status & 0xFF)
    err_len = len(error_msg)
    _write_u32_le(ptr, 1, r_uint32(err_len))
    
    for i in range(err_len):
        ptr[5 + i] = error_msg[i]
        
    _write_u64_le(ptr, 5 + err_len, r_uint64(payload_size))
    return 1 + 4 + err_len + 8
