"""
tests/test_shard_ecs.py
"""
import unittest
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from rpython.rlib.rarithmetic import r_uint64
from gnitz.core import types, strings, checksum
from gnitz.storage import layout, shard_ecs

def create_test_shard(filename, component_layout, entities, components_data, weights=None):
    count = len(entities)
    stride = component_layout.stride
    
    if weights is None:
        weights = [1] * count
    
    # Calculate offsets (packed layout for testing)
    off_e = layout.HEADER_SIZE
    size_e = count * 8
    
    off_w = off_e + size_e
    size_w = count * 8
    
    off_c = off_w + size_w
    size_c = count * stride
    
    off_b = off_c + size_c
    
    # Allocate buffers for regions (we'll compute checksums from these)
    e_buf = lltype.malloc(rffi.CCHARP.TO, size_e, flavor='raw')
    w_buf = lltype.malloc(rffi.CCHARP.TO, size_w, flavor='raw')
    c_buf = lltype.malloc(rffi.CCHARP.TO, size_c, flavor='raw')
    
    try:
        # 1. Build Region E (Entities)
        e_ptr = rffi.cast(rffi.LONGLONGP, e_buf)
        for i, eid in enumerate(entities):
            e_ptr[i] = rffi.cast(rffi.LONGLONG, eid)
        
        # 2. Build Region W (Weights)
        w_ptr = rffi.cast(rffi.LONGLONGP, w_buf)
        for i, w in enumerate(weights):
            w_ptr[i] = rffi.cast(rffi.LONGLONG, w)
        
        # 3. Build Region C (Components)
        for i in range(size_c): c_buf[i] = '\x00'
        for i, data in enumerate(components_data):
            dest_ptr = rffi.ptradd(c_buf, i * stride)
            for j in range(len(data)):
                dest_ptr[j] = data[j]
        
        # 4. Compute checksums
        checksum_e = checksum.compute_checksum(e_buf, size_e)
        checksum_w = checksum.compute_checksum(w_buf, size_w)
        
        # 5. Write to file
        with open(filename, "wb") as f:
            # Write Header with checksums
            header = lltype.malloc(rffi.CCHARP.TO, layout.HEADER_SIZE, flavor='raw')
            try:
                for i in range(layout.HEADER_SIZE): header[i] = '\x00'
                
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_MAGIC))[0] = rffi.cast(rffi.LONGLONG, layout.MAGIC_NUMBER)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_COUNT))[0] = rffi.cast(rffi.LONGLONG, count)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_E_ECS))[0] = rffi.cast(rffi.LONGLONG, off_e)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_W_ECS))[0] = rffi.cast(rffi.LONGLONG, off_w)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_C_ECS))[0] = rffi.cast(rffi.LONGLONG, off_c)
                rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_B_ECS))[0] = rffi.cast(rffi.LONGLONG, off_b)
                
                # Write checksums as ULONGLONG (unsigned)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_CHECKSUM_E))[0] = rffi.cast(rffi.ULONGLONG, checksum_e)
                rffi.cast(rffi.ULONGLONGP, rffi.ptradd(header, layout.OFF_CHECKSUM_W))[0] = rffi.cast(rffi.ULONGLONG, checksum_w)
                
                f.write(rffi.charpsize2str(header, layout.HEADER_SIZE))
            finally:
                lltype.free(header, flavor='raw')
            
            # Write Region E
            f.write(rffi.charpsize2str(e_buf, size_e))
            
            # Write Region W
            f.write(rffi.charpsize2str(w_buf, size_w))
            
            # Write Region C
            f.write(rffi.charpsize2str(c_buf, size_c))
            
    finally:
        lltype.free(e_buf, flavor='raw')
        lltype.free(w_buf, flavor='raw')
        lltype.free(c_buf, flavor='raw')

class TestECSShardView(unittest.TestCase):
    def setUp(self):
        self.fn = "test_ecs_shard.db"
        self.layout = types.ComponentLayout([types.TYPE_U64, types.TYPE_STRING])
        self.entities = [10, 20, 30]
        components = []
        
        # Component 1 (Entity 10)
        c1_buf = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')
        for i in range(self.layout.stride): c1_buf[i] = '\x00'
        rffi.cast(rffi.LONGLONGP, c1_buf)[0] = rffi.cast(rffi.LONGLONG, 100)
        strings.pack_string(rffi.ptradd(c1_buf, 8), "alpha", 0)
        components.append(rffi.charpsize2str(c1_buf, self.layout.stride))
        lltype.free(c1_buf, flavor='raw')

        # Component 2 (Entity 20)
        c2_buf = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')
        for i in range(self.layout.stride): c2_buf[i] = '\x00'
        rffi.cast(rffi.LONGLONGP, c2_buf)[0] = rffi.cast(rffi.LONGLONG, 200)
        strings.pack_string(rffi.ptradd(c2_buf, 8), "beta", 0)
        components.append(rffi.charpsize2str(c2_buf, self.layout.stride))
        lltype.free(c2_buf, flavor='raw')

        # Component 3 (Entity 30)
        c3_buf = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')
        for i in range(self.layout.stride): c3_buf[i] = '\x00'
        rffi.cast(rffi.LONGLONGP, c3_buf)[0] = rffi.cast(rffi.LONGLONG, 300)
        strings.pack_string(rffi.ptradd(c3_buf, 8), "gamma", 0)
        components.append(rffi.charpsize2str(c3_buf, self.layout.stride))
        lltype.free(c3_buf, flavor='raw')

        # Create shard with explicit weights and checksums
        weights = [1, -1, 1]
        create_test_shard(self.fn, self.layout, self.entities, components, weights)
        
        # Now validation will work!
        self.view = shard_ecs.ECSShardView(self.fn, self.layout)

    def tearDown(self):
        self.view.close()
        if os.path.exists(self.fn):
            os.unlink(self.fn)

    def test_read_header(self):
        self.assertEqual(self.view.count, 3)

    def test_find_entity(self):
        self.assertEqual(self.view.find_entity_index(10), 0)
        self.assertEqual(self.view.find_entity_index(20), 1)
        self.assertEqual(self.view.find_entity_index(30), 2)
        self.assertEqual(self.view.find_entity_index(99), -1)

    def test_read_primitive_field(self):
        # Entity 20 is at index 1
        val = self.view.read_field_i64(1, 0)
        self.assertEqual(val, 200)

    def test_read_string_field(self):
        self.assertTrue(self.view.string_field_equals(0, 1, "alpha"))
        self.assertFalse(self.view.string_field_equals(0, 1, "beta"))
        self.assertTrue(self.view.string_field_equals(1, 1, "beta"))
        self.assertTrue(self.view.string_field_equals(2, 1, "gamma"))

    def test_read_weight(self):
        # We wrote weights [1, -1, 1]
        self.assertEqual(self.view.get_weight(0), 1)
        self.assertEqual(self.view.get_weight(1), -1)
        self.assertEqual(self.view.get_weight(2), 1)

if __name__ == '__main__':
    unittest.main()
