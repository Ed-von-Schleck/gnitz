from rpython.rtyper.lltypesystem import rffi, lltype

class FieldType(object):
    """
    Represents a primitive data type, its size, and alignment requirements.
    This is the building block for user-defined components.
    """
    _immutable_fields_ = ['code', 'size', 'alignment']
    def __init__(self, code, size, alignment):
        self.code = code
        self.size = size
        self.alignment = alignment

# ============================================================================
# Primitive Type Definitions
# ============================================================================

TYPE_U8     = FieldType(1, 1, 1)
TYPE_I8     = FieldType(2, 1, 1)
TYPE_U16    = FieldType(3, 2, 2)
TYPE_I16    = FieldType(4, 2, 2)
TYPE_U32    = FieldType(5, 4, 4)
TYPE_I32    = FieldType(6, 4, 4)
TYPE_F32    = FieldType(7, 4, 4)
TYPE_U64    = FieldType(8, 8, 8)
TYPE_I64    = FieldType(9, 8, 8)
TYPE_F64    = FieldType(10, 8, 8)

# The "German String" type.
TYPE_STRING = FieldType(11, 16, 8)

# ============================================================================
# Component Layout Calculator
# ============================================================================

def _align(offset, alignment):
    """Helper to align an offset to the next alignment boundary."""
    return (offset + alignment - 1) & ~(alignment - 1)

class ComponentLayout(object):
    """
    Calculates and stores the physical memory layout for a user-defined
    component struct.
    """
    _immutable_fields_ = ['stride', 'field_offsets[*]', 'field_types[*]']
    
    def __init__(self, field_types):
        self.field_types = field_types
        self.field_offsets = [0] * len(field_types)
        
        current_offset = 0
        max_alignment = 1
        
        for i in range(len(field_types)):
            field_type = field_types[i]
            
            # Add padding to meet the alignment requirement of the current field
            current_offset = _align(current_offset, field_type.alignment)
            self.field_offsets[i] = current_offset
            current_offset += field_type.size
            
            if field_type.alignment > max_alignment:
                max_alignment = field_type.alignment
            
        self.stride = _align(current_offset, max_alignment)

    def get_field_offset(self, field_idx):
        return self.field_offsets[field_idx]
