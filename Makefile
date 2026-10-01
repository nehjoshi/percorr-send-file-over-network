PYTHON ?= python3
VENV := .venv
PY := $(VENV)/bin/python

HOST ?= 127.0.0.1
PORT ?= 7070
DATA ?= data
SOURCE ?= $(DATA)/source.bin
DESTINATION ?= $(DATA)/destination.bin
SIZE_MB ?= 16
RESTARTED_TIMEOUT ?= 5
BLOB_TIMEOUT ?= 30
VERBOSE ?=

COMMON_FLAGS := --host $(HOST) --port $(PORT) $(if $(VERBOSE),--verbose)
LEADER := $(PY) -m src leader --file $(SOURCE) $(COMMON_FLAGS) \
	--restarted-timeout $(RESTARTED_TIMEOUT) --blob-timeout $(BLOB_TIMEOUT)
FOLLOWER := $(PY) -m src follower --file $(DESTINATION) $(COMMON_FLAGS)

.DEFAULT_GOAL := help
.PHONY: help venv sample follower leader demo verify check clean

help: ## Show this help
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*## "} {printf "  %-10s %s\n", $$1, $$2}'
	@echo
	@echo "Variables: HOST PORT SOURCE DESTINATION SIZE_MB RESTARTED_TIMEOUT BLOB_TIMEOUT VERBOSE=1"

$(PY):
	$(PYTHON) -m venv $(VENV)

venv: $(PY) ## Create the virtual environment

$(SOURCE):
	mkdir -p $(dir $@)
	head -c $$(( $(SIZE_MB) * 1024 * 1024 )) /dev/urandom > $@

sample: ## Regenerate SOURCE with SIZE_MB of random bytes
	rm -f $(SOURCE)
	$(MAKE) --no-print-directory $(SOURCE)

follower: $(PY) ## Run the Follower in the foreground, receiving into DESTINATION
	mkdir -p $(dir $(DESTINATION))
	$(FOLLOWER)

leader: $(PY) $(SOURCE) ## Run the Leader in the foreground, sending SOURCE
	$(LEADER)

demo: $(PY) $(SOURCE) ## Run both on this machine, then verify the copy
	rm -f $(DESTINATION)
	mkdir -p $(dir $(DESTINATION))
	$(FOLLOWER) & follower=$$!; \
	trap 'kill $$follower 2>/dev/null; wait $$follower 2>/dev/null' EXIT; \
	$(LEADER)
	$(MAKE) --no-print-directory verify

verify: ## Check that DESTINATION is byte-identical to SOURCE
	cmp $(SOURCE) $(DESTINATION) && echo "OK: $(DESTINATION) matches $(SOURCE)"

check: $(PY) ## Byte-compile the sources
	$(PY) -m compileall -q src

clean: ## Remove generated data and caches
	rm -rf $(DATA)
	find src -name __pycache__ -type d -prune -exec rm -rf {} +
