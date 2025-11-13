ENV = local
CONFIG_PATH = "config/dev.yaml"

.PHONY: migrate-hash
migrate-hash:
	atlas migrate hash

.PHONY: migrate
migrate:
	atlas migrate apply --env ${ENV}

.PHONY: lint
lint:
	poetry run mypy .
	poetry run flake8 .

.PHONY: fmt
fmt:
	poetry run black .

.PHONY: test
test:
	poetry run pytest -m "not integration"

.PHONY: integration-test
integration-test:
	poetry run pytest -m "integration"

.PHONY: ingestion
ingestion:
	poetry run python -m gic.ingestion.job -c ${CONFIG_PATH}

.PHONY: report
report:
	poetry run python -m gic.report.job -c ${CONFIG_PATH}

.PHONY: package
package:
	poetry build