SHELL := /bin/bash

# Virtual environment paths
VENV := venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
PYTEST := $(VENV)/bin/pytest
RUFF := $(VENV)/bin/ruff

.PHONY: venv install test run clean lint format compose-up compose-down compose-logs compose-build ruff

# Create a local virtual environment
venv: $(PY)

$(PY):
	python3 -m venv $(VENV)

# Install project dependencies into the venv
install: venv
	$(PIP) install -r requirements.txt

# Run the test suite inside the venv
test: install
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
