PYPY2     := pypy2
PYPY_PATH ?= ../../pypy
ENV       := PYTHONPATH=".:..:$(PYPY_PATH)"

# target-name  →  checks/script.py
PROOFS := \
	mix64:mix64 \
	xor8:xor8_bounds \
	bloom:bloom_safety \
	string-prefix:string_prefix \
	align-up:align_up \
	u128:u128_roundtrip \
	promote-key:promote_key \
	bounds:bounds_check \
	serialize-align:serialize_align \
	promote-group:promote_group \
	null-word:null_word \
	load-const:load_const \
	exchange-reduce:exchange_reduce \
	shard-bounds:shard_bounds \
	distinct-bounds:distinct_bounds \
	ipc-wire:ipc_wire \
	catalog-keys:catalog_keys \
	pending-key:pending_key \
	partition-assign:partition_assign \
	weight-arith:weight_arith \
	xor8-capacity:xor8_capacity \
	manifest-layout:manifest_layout \
	heap-index:heap_index \
	xor8-fingerprint:xor8_fingerprint \
	float-const:float_const \
	wal-header:wal_header \
	bloom-sizing:bloom_sizing \
	byte-encoding:byte_encoding \
	bsearch-midpoint:bsearch_midpoint \
	range-key:range_key \
	wal-directory:wal_directory \
	shard-directory:shard_directory \
	ipc-string-offsets:ipc_string_offsets \
	reduce-sentinel:reduce_sentinel \
	serialize-offsets:serialize_offsets \
	ipc-column-chain:ipc_column_chain \
	agg-spec-pack:agg_spec_pack \
	ipc-flags:ipc_flags \
	opcode-slots:opcode_slots \
	type-constants:type_constants \
	join-mapping:join_mapping \
	group-index-key:group_index_key \
	schema-stride:schema_stride \
	distinct-sign:distinct_sign \
	ipc-header-layout:ipc_header_layout \
	german-string-layout:german_string_layout \
	mergesort-partition:mergesort_partition \
	wal-region-count:wal_region_count \
	expr-opcodes:expr_opcodes \
	mix64-consistency:mix64_consistency \
	short-string-unpack:short_string_unpack \
	reduce-mapping-count:reduce_mapping_count \
	expr-instr-width:expr_instr_width \
	catalog-node-edge-dep-pk:catalog_node_edge_dep_pk \
	tournament-tree-key-split:tournament_tree_key_split \
	promote-col-to-u128:promote_col_to_u128 \
	promote-group-narrow:promote_group_narrow \
	partition-key-routing:partition_key_routing \
	name-tid-hash:name_tid_hash \
	manifest-key-split:manifest_key_split \
	negate-involution:negate_involution \
	union-merge-sorted:union_merge_sorted \
	anti-semi-complement:anti_semi_complement \
	dbsp-join-correctness:dbsp_join_correctness

TARGETS := $(foreach p,$(PROOFS),prove-$(firstword $(subst :, ,$(p))))

.PHONY: prove $(TARGETS)

prove: $(TARGETS)

# Pattern: prove-<name> runs checks/<script>.py
$(foreach p,$(PROOFS),$(eval prove-$(firstword $(subst :, ,$(p))): ; $(ENV) $(PYPY2) checks/$(lastword $(subst :, ,$(p))).py))
