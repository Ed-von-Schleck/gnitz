"""
tests/test_shard_ecs.py
"""
import unittest
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, strings
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
    
    with open(filename, "wb") as f:
        # 1. Write Header
        header = lltype.malloc(rffi.CCHARP.TO, layout.HEADER_SIZE, flavor='raw')
        try:
            for i in range(layout.HEADER_SIZE): header[i] = '\x00'
            
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_MAGIC))[0] = rffi.cast(rffi.LONGLONG, layout.MAGIC_NUMBER)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_COUNT))[0] = rffi.cast(rffi.LONGLONG, count)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_E_ECS))[0] = rffi.cast(rffi.LONGLONG, off_e)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_W_ECS))[0] = rffi.cast(rffi.LONGLONG, off_w)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_C_ECS))[0] = rffi.cast(rffi.LONGLONG, off_c)
            rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_B_ECS))[0] = rffi.cast(rffi.LONGLONG, off_b)
            
            f.write(rffi.charpsize2str(header, layout.HEADER_SIZE))
        finally:
            lltype.free(header, flavor='raw')
        
        # 2. Write Region E (Entities)
        e_buf = lltype.malloc(rffi.CCHARP.TO, size_e, flavor='raw')
        try:
            e_ptr = rffi.cast(rffi.LONGLONGP, e_buf)
            for i, eid in enumerate(entities):
                e_ptr[i] = rffi.cast(rffi.LONGLONG, eid)
            f.write(rffi.charpsize2str(e_buf, size_e))
        finally:
            lltype.free(e_buf, flavor='raw')
        
        # 3. Write Region W (Weights)
        w_buf = lltype.malloc(rffi.CCHARP.TO, size_w, flavor='raw')
        try:
            w_ptr = rffi.cast(rffi.LONGLONGP, w_buf)
            for i, w in enumerate(weights):
                w_ptr[i] = rffi.cast(rffi.LONGLONG, w)
            f.write(rffi.charpsize2str(w_buf, size_w))
        finally:
            lltype.free(w_buf, flavor='raw')
        
        # 4. Write Region C (Components)
        c_buf = lltype.malloc(rffi.CCHARP.TO, size_c, flavor='raw')
        try:
            # Zero initialize to prevent garbage
            for i in range(size_c): c_buf[i] = '\x00'
            
            for i, data in enumerate(components_data):
                dest_ptr = rffi.ptradd(c_buf, i * stride)
                for j in range(len(data)):
                    dest_ptr[j] = data[j]
            f.write(rffi.charpsize2str(c_buf, size_c))
        finally:
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

        # Component 3 (Entity 30) - WAS MISSING in original test
        c3_buf = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')
        for i in range(self.layout.stride): c3_buf[i] = '\x00'
        rffi.cast(rffi.LONGLONGP, c3_buf)[0] = rffi.cast(rffi.LONGLONG, 300)
        strings.pack_string(rffi.ptradd(c3_buf, 8), "gamma", 0)
        components.append(rffi.charpsize2str(c3_buf, self.layout.stride))
        lltype.free(c3_buf, flavor='raw')

        # Create shard with explicit weights
        weights = [1, -1, 1]
        create_test_shard(self.fn, self.layout, self.entities, components, weights)
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
