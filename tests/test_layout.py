from gnitz.storage import layout

def test_magic_number():
    # Ensure magic number is an integer (needed for RPython)
    assert isinstance(layout.MAGIC_NUMBER, int) or isinstance(layout.MAGIC_NUMBER, long)

def test_header_structure():
    # Ensure offsets don't overlap (basic sanity check)
    offsets = [
        layout.OFF_MAGIC,
        layout.OFF_COUNT,
        layout.OFF_REG_W,
        layout.OFF_REG_O,
        layout.OFF_REG_K,
        layout.OFF_REG_V
    ]
    # Check that they are sorted and spaced by at least 8 bytes
    for i in range(len(offsets) - 1):
        assert offsets[i+1] >= offsets[i] + 8

def test_alignment():
    # Header size must be aligned to the cache line
    assert layout.HEADER_SIZE % layout.ALIGNMENT == 0
