# gnitz/vm/ops/__init__.py

from gnitz.vm.ops.linear import op_filter, op_map, op_negate, op_union, op_delay, op_integrate
from gnitz.vm.ops.join import op_join_delta_trace, op_join_delta_delta
from gnitz.vm.ops.reduce import op_reduce
from gnitz.vm.ops.distinct import op_distinct
