from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib import rposix
from gnitz.storage import errors
import os

# Manifest File Format Constants
MAGIC_NUMBER = 0x4D414E49464E5447  # "MANIFNGT"
VERSION = 1
HEADER_SIZE = 64
ENTRY_SIZE = 168

# Header offsets
OFF_MAGIC = 0
OFF_VERSION = 8
OFF_ENTRY_COUNT = 16
OFF_RESERVED = 24

# Entry offsets (relative to entry start)
OFF_COMPONENT_ID = 0
OFF_MIN_ENTITY_ID = 8
OFF_MAX_ENTITY_ID = 16
OFF_MIN_LSN = 24
OFF_MAX_LSN = 32
OFF_FILENAME = 40
FILENAME_MAX_LEN = 128

class ManifestEntry(object):
    """
    Represents a single shard reference in the manifest.
    """
    def __init__(self, component_id, shard_filename, min_eid, max_eid, min_lsn, max_lsn):
        self.component_id = component_id
        self.shard_filename = shard_filename
        self.min_entity_id = min_eid
        self.max_entity_id = max_eid
        self.min_lsn = min_lsn
        self.max_lsn = max_lsn

def _write_manifest_header(fd, entry_count):
    """
    Writes the manifest header to a file descriptor.
    
    Args:
        fd: File descriptor (opened for writing)
        entry_count: Number of entries that will be in the manifest
    """
    header = lltype.malloc(rffi.CCHARP.TO, HEADER_SIZE, flavor='raw')
    try:
        # Zero out the header
        for i in range(HEADER_SIZE):
            header[i] = '\x00'
        
        # Write magic number
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, OFF_MAGIC))[0] = rffi.cast(rffi.LONGLONG, MAGIC_NUMBER)
        
        # Write version
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, OFF_VERSION))[0] = rffi.cast(rffi.LONGLONG, VERSION)
        
        # Write entry count
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, OFF_ENTRY_COUNT))[0] = rffi.cast(rffi.LONGLONG, entry_count)
        
        # Write to file
        header_str = rffi.charpsize2str(header, HEADER_SIZE)
        written = os.write(fd, header_str)
        if written != HEADER_SIZE:
            raise errors.StorageError()
    finally:
        lltype.free(header, flavor='raw')

def _read_manifest_header(fd):
    """
    Reads and validates the manifest header from a file descriptor.
    
    Args:
        fd: File descriptor (opened for reading)
    
    Returns:
        Tuple of (version, entry_count)
    
    Raises:
        CorruptShardError: If magic number is invalid
    """
    header = lltype.malloc(rffi.CCHARP.TO, HEADER_SIZE, flavor='raw')
    try:
        # Read header
        read_bytes = os.read(fd, HEADER_SIZE)
        if len(read_bytes) != HEADER_SIZE:
            raise errors.CorruptShardError("Manifest header too short")
        
        # Copy into buffer
        for i in range(HEADER_SIZE):
            header[i] = read_bytes[i]
        
        # Validate magic number
        magic = rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, OFF_MAGIC))[0]
        if magic != MAGIC_NUMBER:
            raise errors.CorruptShardError("Invalid manifest magic number")
        
        # Read version
        version = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, OFF_VERSION))[0])
        
        # Read entry count
        entry_count = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, OFF_ENTRY_COUNT))[0])
        
        return version, entry_count
    finally:
        lltype.free(header, flavor='raw')

def _write_manifest_entry(fd, entry):
    """
    Writes a single manifest entry to a file descriptor.
    
    Args:
        fd: File descriptor (opened for writing)
        entry: ManifestEntry object
    """
    entry_buf = lltype.malloc(rffi.CCHARP.TO, ENTRY_SIZE, flavor='raw')
    try:
        # Zero out the entry buffer
        for i in range(ENTRY_SIZE):
            entry_buf[i] = '\x00'
        
        # Write component ID
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(entry_buf, OFF_COMPONENT_ID))[0] = rffi.cast(rffi.LONGLONG, entry.component_id)
        
        # Write min entity ID
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(entry_buf, OFF_MIN_ENTITY_ID))[0] = rffi.cast(rffi.LONGLONG, entry.min_entity_id)
        
        # Write max entity ID
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(entry_buf, OFF_MAX_ENTITY_ID))[0] = rffi.cast(rffi.LONGLONG, entry.max_entity_id)
        
        # Write min LSN
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(entry_buf, OFF_MIN_LSN))[0] = rffi.cast(rffi.LONGLONG, entry.min_lsn)
        
        # Write max LSN
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(entry_buf, OFF_MAX_LSN))[0] = rffi.cast(rffi.LONGLONG, entry.max_lsn)
        
        # Write filename (null-terminated, truncate if too long)
        filename_ptr = rffi.ptradd(entry_buf, OFF_FILENAME)
        filename_len = len(entry.shard_filename)
        if filename_len > FILENAME_MAX_LEN - 1:
            filename_len = FILENAME_MAX_LEN - 1
        
        for i in range(filename_len):
            filename_ptr[i] = entry.shard_filename[i]
        # Null terminator is already there from zero initialization
        
        # Write to file
        entry_str = rffi.charpsize2str(entry_buf, ENTRY_SIZE)
        written = os.write(fd, entry_str)
        if written != ENTRY_SIZE:
            raise errors.StorageError()
    finally:
        lltype.free(entry_buf, flavor='raw')

def _read_manifest_entry(fd):
    """
    Reads a single manifest entry from a file descriptor.
    
    Args:
        fd: File descriptor (opened for reading)
    
    Returns:
        ManifestEntry object or None if EOF
    """
    entry_buf = lltype.malloc(rffi.CCHARP.TO, ENTRY_SIZE, flavor='raw')
    try:
        # Read entry
        read_bytes = os.read(fd, ENTRY_SIZE)
        if len(read_bytes) == 0:
            return None  # EOF
        if len(read_bytes) != ENTRY_SIZE:
            raise errors.CorruptShardError("Incomplete manifest entry")
        
        # Copy into buffer
        for i in range(ENTRY_SIZE):
            entry_buf[i] = read_bytes[i]
        
        # Read component ID
        component_id = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, rffi.ptradd(entry_buf, OFF_COMPONENT_ID))[0])
        
        # Read min entity ID
        min_eid = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, rffi.ptradd(entry_buf, OFF_MIN_ENTITY_ID))[0])
        
        # Read max entity ID
        max_eid = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, rffi.ptradd(entry_buf, OFF_MAX_ENTITY_ID))[0])
        
        # Read min LSN
        min_lsn = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, rffi.ptradd(entry_buf, OFF_MIN_LSN))[0])
        
        # Read max LSN
        max_lsn = rffi.cast(lltype.Signed, rffi.cast(rffi.LONGLONGP, rffi.ptradd(entry_buf, OFF_MAX_LSN))[0])
        
        # Read filename (null-terminated string)
        filename_ptr = rffi.ptradd(entry_buf, OFF_FILENAME)
        filename_chars = []
        for i in range(FILENAME_MAX_LEN):
            c = filename_ptr[i]
            if c == '\x00':
                break
            filename_chars.append(c)
        filename = ''.join(filename_chars)
        
        return ManifestEntry(component_id, filename, min_eid, max_eid, min_lsn, max_lsn)
    finally:
        lltype.free(entry_buf, flavor='raw')


class ManifestWriter(object):
    """
    High-level interface for creating manifest files.
    Accumulates entries and writes them atomically on finalize.
    """
    def __init__(self, filename):
        self.filename = filename
        self.entries = []
        self.finalized = False
    
    def add_entry(self, component_id, shard_filename, min_eid, max_eid, min_lsn, max_lsn):
        """
        Adds a shard entry to the manifest.
        
        Args:
            component_id: Component type ID
            shard_filename: Path to the shard file
            min_eid: Minimum entity ID in shard
            max_eid: Maximum entity ID in shard
            min_lsn: Minimum LSN in shard
            max_lsn: Maximum LSN in shard
        """
        if self.finalized:
            raise errors.StorageError()  # Cannot add after finalize
        
        entry = ManifestEntry(component_id, shard_filename, min_eid, max_eid, min_lsn, max_lsn)
        self.entries.append(entry)
    
    def finalize(self):
        """
        Writes all entries to disk and closes the manifest file.
        After this, no more entries can be added.
        """
        if self.finalized:
            return  # Already finalized
        
        fd = rposix.open(self.filename, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            # Write header with entry count
            _write_manifest_header(fd, len(self.entries))
            
            # Write all entries
            for entry in self.entries:
                _write_manifest_entry(fd, entry)
        finally:
            rposix.close(fd)
        
        self.finalized = True


class ManifestReader(object):
    """
    High-level interface for reading manifest files.
    Provides indexed access and iteration over entries.
    """
    def __init__(self, filename):
        self.filename = filename
        self.fd = -1
        self.entry_count = 0
        self.version = 0
        
        # Open and read header
        self.fd = rposix.open(filename, os.O_RDONLY, 0)
        self.version, self.entry_count = _read_manifest_header(self.fd)
    
    def get_entry_count(self):
        """Returns the total number of entries in the manifest."""
        return self.entry_count
    
    def read_entry(self, index):
        """
        Reads the entry at the given index.
        
        Args:
            index: Zero-based entry index
        
        Returns:
            ManifestEntry object
        
        Raises:
            StorageError: If index is out of bounds
        """
        if index < 0 or index >= self.entry_count:
            raise errors.StorageError()  # Index out of bounds
        
        # Seek to the entry position
        # Position = HEADER_SIZE + (index * ENTRY_SIZE)
        offset = HEADER_SIZE + (index * ENTRY_SIZE)
        rposix.lseek(self.fd, offset, 0)  # SEEK_SET = 0
        
        # Read the entry
        entry = _read_manifest_entry(self.fd)
        if entry is None:
            raise errors.CorruptShardError("Unexpected EOF in manifest")
        
        return entry
    
    def iterate_entries(self):
        """
        Iterator that yields all entries in order.
        
        Yields:
            ManifestEntry objects
        """
        # Seek to start of entries
        rposix.lseek(self.fd, HEADER_SIZE, 0)  # SEEK_SET = 0
        
        for i in range(self.entry_count):
            entry = _read_manifest_entry(self.fd)
            if entry is None:
                raise errors.CorruptShardError("Unexpected EOF in manifest")
            yield entry
    
    def close(self):
        """Closes the manifest file."""
        if self.fd != -1:
            rposix.close(self.fd)
            self.fd = -1


class ManifestManager(object):
    """
    Manages manifest versioning and atomic updates.
    Handles the canonical manifest file and provides atomic swap operations.
    """
    def __init__(self, manifest_path):
        """
        Args:
            manifest_path: Path to the canonical manifest file (e.g., "manifest.db")
        """
        self.manifest_path = manifest_path
        self.temp_path = manifest_path + ".tmp"
    
    def exists(self):
        """Returns True if a manifest file exists."""
        return os.path.exists(self.manifest_path)
    
    def load_current(self):
        """
        Loads the current active manifest.
        
        Returns:
            ManifestReader for the current manifest
        
        Raises:
            OSError: If manifest doesn't exist
        """
        return ManifestReader(self.manifest_path)
    
    def publish_new_version(self, entries):
        """
        Atomically publishes a new version of the manifest.
        
        This creates a new manifest with the given entries and atomically
        replaces the current manifest. The operation is crash-safe.
        
        Args:
            entries: List of ManifestEntry objects
        """
        # Write to temporary file
        writer = ManifestWriter(self.temp_path)
        for entry in entries:
            writer.add_entry(
                entry.component_id,
                entry.shard_filename,
                entry.min_entity_id,
                entry.max_entity_id,
                entry.min_lsn,
                entry.max_lsn
            )
        writer.finalize()
        
        # Atomic rename
        os.rename(self.temp_path, self.manifest_path)
