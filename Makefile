SHELL := /bin/bash

# Detect Python executable (prefer `python` from PATH, fallback to `python3`)
PYTHON := $(shell command -v python || command -v python3)

# Virtual environment paths
VENV := venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff
REQ := requirements.txt

.PHONY: venv install test run clean lint format compose-up compose-down compose-logs compose-build ruff pytest

# Create a local virtual environment
venv: $(PY)

$(PY):
	$(PYTHON) -m venv $(VENV)

# Install project dependencies into the venv (only when requirements change)
install: $(VENV)/.installed

$(VENV)/.installed: $(REQ) | venv
	PIP_DISABLE_PIP_VERSION_CHECK=1 $(PIP) install -r $(REQ)
	touch $@

# Ensure pytest is available in the venv
pytest: venv
	$(PIP) install -q pytest pytest-asyncio

# Run the test suite inside the venv
test: install pytest
	$(PYTEST) -q

# Run the app inside the venv
run: install
	$(PY) src/meshgram.py

# Clean local artifacts
clean:
	rm -rf $(VENV) __pycache__ .pytest_cache src/__pycache__ tests/__pycache__

# Lint the codebase using ruff
lint: install ruff
	$(RUFF) check .

# Format the codebase using ruff formatter
format: install ruff
	$(RUFF) format

# Ensure ruff is available in the venv
ruff: venv
	@if [ ! -x "$(RUFF)" ]; then \
		$(PIP) install -q ruff; \
	fi

# Docker Compose helpers
compose-up:
	docker compose up -d

compose-down:
	docker compose down

compose-logs:
	docker compose logs -f

compose-build:
	docker compose build
