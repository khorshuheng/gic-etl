ENV = local
EXTERNAL_FUND_SRC_DIR = "data/external-funds"
LOCAL_DB_URL = "jdbc:sqlite:database/local.db"
PRICING_RECONCILIATION_REPORT_PATH = "reports/pricing_reconciliation_report"

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
	poetry run python -m gic.ingestion.job -s ${EXTERNAL_FUND_SRC_DIR} -d ${LOCAL_DB_URL}

.PHONY: report
report:
	poetry run python -m gic.report.job -s ${LOCAL_DB_URL} -d ${PRICING_RECONCILIATION_REPORT_PATH}

.PHONY: package
package:
	poetry build