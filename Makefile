dependencies:
	pip install -r requirements.txt

dev-dependencies:
	pip install -r requirements-dev.txt

test:
	pytest -v ./tests/

coverage:
	coverage run -m pytest -v

seed-dev-env:
	docker compose up -d postgres
	docker compose exec postgres bash -c 'psql -h 127.0.0.1 -U pguser < /seed.sql'
