# gnitz/core/types.py

from rpython.rtyper.lltypesystem import rffi, lltype
from gnitz.storage import errors


class FieldType(object):
    _immutable_fields_ = ["code", "size", "alignment"]

    def __init__(self, code, size, alignment):
        self.code = code
        self.size = size
        self.alignment = alignment


# Primitive Type Definitions
TYPE_U8 = FieldType(1, 1, 1)
TYPE_I8 = FieldType(2, 1, 1)
TYPE_U16 = FieldType(3, 2, 2)
TYPE_I16 = FieldType(4, 2, 2)
TYPE_U32 = FieldType(5, 4, 4)
TYPE_I32 = FieldType(6, 4, 4)
TYPE_F32 = FieldType(7, 4, 4)
TYPE_U64 = FieldType(8, 8, 8)
TYPE_I64 = FieldType(9, 8, 8)
TYPE_F64 = FieldType(10, 8, 8)
TYPE_STRING = FieldType(11, 16, 8)
TYPE_U128 = FieldType(12, 16, 16)


class ColumnDefinition(object):
    _immutable_fields_ = ["field_type", "is_nullable", "name"]

    def __init__(self, field_type, is_nullable=False, name=""):
        self.field_type = field_type
        self.is_nullable = is_nullable
        self.name = name


def _align(offset, alignment):
    return (offset + alignment - 1) & ~(alignment - 1)


def _to_col_def(c):
    if isinstance(c, ColumnDefinition):
        return c
    if isinstance(c, FieldType):
        return ColumnDefinition(c)
    raise errors.LayoutError("Invalid column definition")


class TableSchema(object):
    """
    Defines the physical and logical layout of a Z-Set.
    
    Attributes:
        columns: List of ColumnDefinition.
        pk_index: The index of the column that serves as the Primary Key.
                  GnitzDB requires PKs to be TYPE_U64 or TYPE_U128.
        column_offsets: Physical byte offsets for columns in a packed row (AoS).
                        The PK column always has an offset of -1 as it is 
                        stored separately from the payload.
    """
    _immutable_fields_ = [
        "columns[*]",
        "pk_index",
        "column_offsets[*]",
        "memtable_stride",
    ]

    def __init__(self, columns, pk_index=0):
        if len(columns) > 64:
            raise errors.LayoutError("Maximum 64 columns supported")

        # Use list comprehension to satisfy RPython list constraints
        self.columns = [_to_col_def(c) for c in columns]
        self.pk_index = pk_index

        # Validate PK type
        pk_type = self.columns[pk_index].field_type
        if pk_type.code != TYPE_U64.code and pk_type.code != TYPE_U128.code:
            raise errors.LayoutError("Primary Key must be TYPE_U64 or TYPE_U128")

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


# -----------------------------------------------------------------------------
# Schema Algebra (Used by QueryBuilder and VM)
# -----------------------------------------------------------------------------

def merge_schemas_for_join(schema_left, schema_right):
    """
    Constructs the resulting schema for a Join operation.
    Output Schema = [Left_PK] + [Left_Payloads] + [Right_Payloads].
    Note: The right-side PK is dropped as it matches the left-side PK.
    """
    pk_left = schema_left.get_pk_column()
    pk_right = schema_right.get_pk_column()

    if pk_left.field_type.code != pk_right.field_type.code:
        raise errors.LayoutError(
            "Join PK Type Mismatch: Left=%d Right=%d"
            % (pk_left.field_type.code, pk_right.field_type.code)
        )

    # Use list comprehension to build the column list
    new_cols = []
    # 1. Add PK (index 0)
    new_cols.append(pk_left)

    # 2. Add Left Payloads
    for i in range(len(schema_left.columns)):
        if i != schema_left.pk_index:
            new_cols.append(schema_left.columns[i])

    # 3. Add Right Payloads (skipping right PK)
    for i in range(len(schema_right.columns)):
        if i != schema_right.pk_index:
            new_cols.append(schema_right.columns[i])

    return TableSchema(new_cols, pk_index=0)


def _build_reduce_output_schema(input_schema, group_by_cols, agg_func):
    """
    Constructs the output schema for a REDUCE operator.
    
    Rules:
    - If grouping by a single U64 or U128 column, that column is the PK.
    - Otherwise, a synthetic U128 PK is created (to hold a hash of group columns).
    - All group columns and the aggregate result are stored in the payload.
    """
    new_cols = []
    
    # Check if we can use a "Natural" PK (single int col)
    use_natural_pk = False
    if len(group_by_cols) == 1:
        col_idx = group_by_cols[0]
        t = input_schema.columns[col_idx].field_type
        if t.code == TYPE_U64.code or t.code == TYPE_U128.code:
            use_natural_pk = True

    if use_natural_pk:
        # Col 0: The Group Column (PK)
        # Col 1: The Aggregate result
        new_cols.append(input_schema.columns[group_by_cols[0]])
        new_cols.append(ColumnDefinition(agg_func.output_column_type(), name="agg"))
        return TableSchema(new_cols, pk_index=0)
    else:
        # Synthetic PK required (e.g. GROUP BY string_col or multiple cols)
        # Col 0: Synthetic U128 Hash (PK)
        # Col 1..N: Group Columns (Payload)
        # Col N+1: Aggregate Result (Payload)
        new_cols.append(ColumnDefinition(TYPE_U128, name="_group_hash"))
        for idx in group_by_cols:
            new_cols.append(input_schema.columns[idx])
        new_cols.append(ColumnDefinition(agg_func.output_column_type(), name="agg"))
        return TableSchema(new_cols, pk_index=0)
