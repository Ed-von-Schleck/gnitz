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
	weight-arith:weight_arith

TARGETS := $(foreach p,$(PROOFS),prove-$(firstword $(subst :, ,$(p))))

.PHONY: prove $(TARGETS)

prove: $(TARGETS)

# Pattern: prove-<name> runs checks/<script>.py
$(foreach p,$(PROOFS),$(eval prove-$(firstword $(subst :, ,$(p))): ; $(ENV) $(PYPY2) checks/$(lastword $(subst :, ,$(p))).py))
