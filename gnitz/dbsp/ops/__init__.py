# gnitz/dbsp/ops/__init__.py

from gnitz.dbsp.ops.linear import (
    op_filter, 
    op_map, 
    op_negate, 
    op_union, 
    op_delay, 
    op_integrate
)
from gnitz.dbsp.ops.join import op_join_delta_trace, op_join_delta_delta
from gnitz.dbsp.ops.reduce import op_reduce
from gnitz.dbsp.ops.distinct import op_distinct
from gnitz.dbsp.ops.source import op_scan_trace, op_seek_trace, op_clear_deltas # <--- New
