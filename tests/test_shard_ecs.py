import unittest
import os
from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.core import types, strings
from gnitz.storage import layout, shard_ecs

def create_test_shard(filename, component_layout, entities, components_data):
    count = len(entities)
    stride = component_layout.stride
    
    off_e = layout.HEADER_SIZE
    size_e = count * 8
    off_c = off_e + size_e
    size_c = count * stride
    off_b = off_c + size_c
    
    with open(filename, "wb") as f:
        header = lltype.malloc(rffi.CCHARP.TO, layout.HEADER_SIZE, flavor='raw')
        for i in range(layout.HEADER_SIZE): header[i] = '\x00'
        
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_MAGIC))[0] = rffi.cast(rffi.LONGLONG, layout.MAGIC_NUMBER)
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_COUNT))[0] = rffi.cast(rffi.LONGLONG, count)
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_E_ECS))[0] = rffi.cast(rffi.LONGLONG, off_e)
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_C_ECS))[0] = rffi.cast(rffi.LONGLONG, off_c)
        rffi.cast(rffi.LONGLONGP, rffi.ptradd(header, layout.OFF_REG_B_ECS))[0] = rffi.cast(rffi.LONGLONG, off_b)
        
        f.write(rffi.charpsize2str(header, layout.HEADER_SIZE))
        lltype.free(header, flavor='raw')
        
        e_buf = lltype.malloc(rffi.CCHARP.TO, size_e, flavor='raw')
        e_ptr = rffi.cast(rffi.LONGLONGP, e_buf)
        for i, eid in enumerate(entities):
            e_ptr[i] = rffi.cast(rffi.LONGLONG, eid)
        f.write(rffi.charpsize2str(e_buf, size_e))
        lltype.free(e_buf, flavor='raw')
        
        c_buf = lltype.malloc(rffi.CCHARP.TO, size_c, flavor='raw')
        for i, data in enumerate(components_data):
            dest_ptr = rffi.ptradd(c_buf, i * stride)
            for j in range(len(data)):
                dest_ptr[j] = data[j]
        f.write(rffi.charpsize2str(c_buf, size_c))
        lltype.free(c_buf, flavor='raw')

class TestECSShardView(unittest.TestCase):
    def setUp(self):
        self.fn = "test_ecs_shard.db"
        self.layout = types.ComponentLayout([types.TYPE_U64, types.TYPE_STRING])
        self.entities = [10, 20, 30]
        components = []
        
        c1_buf = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')
        rffi.cast(rffi.LONGLONGP, c1_buf)[0] = rffi.cast(rffi.LONGLONG, 100)
        strings.pack_string(rffi.ptradd(c1_buf, 8), "alpha", 0)
        components.append(rffi.charpsize2str(c1_buf, self.layout.stride))
        lltype.free(c1_buf, flavor='raw')

        c2_buf = lltype.malloc(rffi.CCHARP.TO, self.layout.stride, flavor='raw')
        rffi.cast(rffi.LONGLONGP, c2_buf)[0] = rffi.cast(rffi.LONGLONG, 200)
        strings.pack_string(rffi.ptradd(c2_buf, 8), "beta", 0)
        components.append(rffi.charpsize2str(c2_buf, self.layout.stride))
        lltype.free(c2_buf, flavor='raw')

        create_test_shard(self.fn, self.layout, self.entities, components)
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
        val = self.view.read_field_i64(1, 0)
        self.assertEqual(val, 200)

    def test_read_string_field(self):
        self.assertTrue(self.view.string_field_equals(0, 1, "alpha"))
        self.assertFalse(self.view.string_field_equals(0, 1, "beta"))
        self.assertTrue(self.view.string_field_equals(1, 1, "beta"))

if __name__ == '__main__':
    unittest.main()
