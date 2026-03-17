PYPY2     := pypy2
PYPY_PATH ?= ../../pypy
ENV       := PYTHONPATH=".:..:$(PYPY_PATH)"

.PHONY: prove prove-mix64 prove-xor8 prove-bloom prove-string-prefix prove-align-up prove-u128 prove-promote-key prove-bounds

prove: prove-mix64 prove-xor8 prove-bloom prove-string-prefix prove-align-up prove-u128 prove-promote-key prove-bounds

prove-mix64:
	$(ENV) $(PYPY2) checks/mix64.py

prove-xor8:
	$(ENV) $(PYPY2) checks/xor8_bounds.py

prove-bloom:
	$(ENV) $(PYPY2) checks/bloom_safety.py

prove-string-prefix:
	$(ENV) $(PYPY2) checks/string_prefix.py

prove-align-up:
	$(ENV) $(PYPY2) checks/align_up.py

prove-u128:
	$(ENV) $(PYPY2) checks/u128_roundtrip.py

prove-promote-key:
	$(ENV) $(PYPY2) checks/promote_key.py

prove-bounds:
	$(ENV) $(PYPY2) checks/bounds_check.py
