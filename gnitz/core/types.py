from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import errors

class FieldType(object):
    _immutable_fields_ = ['code', 'size', 'alignment']
    def __init__(self, code, size, alignment):
        self.code = code
        self.size = size
        self.alignment = alignment

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
TYPE_STRING = FieldType(11, 16, 8)
TYPE_U128   = FieldType(12, 16, 16)

class ColumnDefinition(object):
    _immutable_fields_ = ['field_type', 'is_nullable']
    def __init__(self, field_type, is_nullable=False):
        self.field_type = field_type
        self.is_nullable = is_nullable

def _align(offset, alignment):
    return (offset + alignment - 1) & ~(alignment - 1)

def _to_col_def(c):
    if isinstance(c, ColumnDefinition):
        return c
    if isinstance(c, FieldType):
        return ColumnDefinition(c)
    raise errors.LayoutError("Invalid column definition")

class TableSchema(object):
    _immutable_fields_ = ['columns[*]', 'pk_index', 'column_offsets[*]', 'memtable_stride']
    
    def __init__(self, columns, pk_index=0):
        if len(columns) > 64:
            raise errors.LayoutError("Maximum 64 columns supported")
            
        # Use list comprehension to avoid resize_allowed=False issues
        self.columns = [_to_col_def(c) for c in columns]
        self.pk_index = pk_index
        
        num_cols = len(self.columns)
        temp_offsets = [0] * num_cols
        
        current_offset = 0
        max_alignment = 1
        
        for i in range(num_cols):
            if i == pk_index:
                temp_offsets[i] = -1
                continue

            field_type = self.columns[i].field_type
            current_offset = _align(current_offset, field_type.alignment)
            temp_offsets[i] = current_offset
            current_offset += field_type.size
            
            if field_type.alignment > max_alignment:
                max_alignment = field_type.alignment
            
        self.column_offsets = temp_offsets
        self.memtable_stride = _align(current_offset, max_alignment)

    @property
    def stride(self):
        return self.memtable_stride

    def get_column_offset(self, col_idx):
        return self.column_offsets[col_idx]

    def get_pk_column(self):
        return self.columns[self.pk_index]
