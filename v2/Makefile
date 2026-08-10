PYTHON ?= python3
VENV := .venv
VENV_PYTHON := $(VENV)/bin/python

.PHONY: bootstrap test check ingest-sample ingest-schedule-sample ingest-changes-sample help

help:
	@echo "bootstrap      create the local Python environment"
	@echo "test           run the unit test suite"
	@echo "check          run all local quality checks"
	@echo "ingest-sample  land the checked-in MLB game fixture locally"
	@echo "ingest-schedule-sample  land the checked-in schedule fixture"
	@echo "ingest-changes-sample  land the checked-in correction-feed fixture"

bootstrap:
	@$(PYTHON) -c 'import sys; assert sys.version_info >= (3, 9), "Python 3.9+ is required"'
	@test -x $(VENV_PYTHON) || $(PYTHON) -m venv $(VENV)
	@$(VENV_PYTHON) -c 'import sys; print(f"ready: {sys.executable} ({sys.version.split()[0]})")'

test:
	@PYTHONPATH=src $(VENV_PYTHON) -m unittest discover -s tests -v

check:
	@PYTHONPATH=src $(VENV_PYTHON) -m compileall -q src tests
	@$(MAKE) test

ingest-sample:
	@PYTHONPATH=src ZAVANT_DATA_DIR=$${ZAVANT_DATA_DIR:-.local/lake} $(VENV_PYTHON) -m zavant land-game-file tests/fixtures/example-game-raw.json --trigger initial

ingest-schedule-sample:
	@PYTHONPATH=src ZAVANT_DATA_DIR=$${ZAVANT_DATA_DIR:-.local/lake} $(VENV_PYTHON) -m zavant land-schedule-file tests/fixtures/example-schedule.json --start-date 2026-08-08 --end-date 2026-08-08 --requested-at 2026-08-09T00:00:00Z --run-id 00000000-0000-0000-0000-000000000002

ingest-changes-sample:
	@PYTHONPATH=src ZAVANT_DATA_DIR=$${ZAVANT_DATA_DIR:-.local/lake} $(VENV_PYTHON) -m zavant land-changes-file tests/fixtures/example-game-changes.json --updated-since 2026-08-08T00:00:00Z --window-end 2026-08-09T00:00:00Z --run-id 00000000-0000-0000-0000-000000000001
