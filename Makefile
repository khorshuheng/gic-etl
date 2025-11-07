ENV = local

migrate-hash:
	atlas migrate hash

migrate:
	atlas migrate apply --env ${ENV}

lint:
	poetry run mypy .

fmt:
	poetry run