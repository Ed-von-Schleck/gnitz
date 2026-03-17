PYPY2     := pypy2
PYPY_PATH ?= ../../pypy
ENV       := PYTHONPATH=".:..:$(PYPY_PATH)"

.PHONY: prove prove-mix64 prove-xor8 prove-bloom

prove: prove-mix64 prove-xor8 prove-bloom

prove-mix64:
	$(ENV) $(PYPY2) checks/mix64.py

prove-xor8:
	$(ENV) $(PYPY2) checks/xor8_bounds.py

prove-bloom:
	$(ENV) $(PYPY2) checks/bloom_safety.py
