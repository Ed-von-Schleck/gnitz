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
	agg-spec-pack:agg_spec_pack

TARGETS := $(foreach p,$(PROOFS),prove-$(firstword $(subst :, ,$(p))))

.PHONY: prove $(TARGETS)

prove: $(TARGETS)

# Pattern: prove-<name> runs checks/<script>.py
$(foreach p,$(PROOFS),$(eval prove-$(firstword $(subst :, ,$(p))): ; $(ENV) $(PYPY2) checks/$(lastword $(subst :, ,$(p))).py))
