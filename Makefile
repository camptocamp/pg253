dependencies:
	pip install -r requirements.txt

dev-dependencies:
	pip install -r requirements-dev.txt

test:
	pytest -v ./tests/unit

integration:
	docker compose up -d garage
	docker compose up -d garage-init
	$(MAKE) seed-dev-env
	pytest -v ./tests/integration

coverage:
	coverage run -m pytest -v

seed-dev-env:
	docker compose up -d postgres
	until docker compose exec postgres pg_isready; do sleep 1; done
	if ! docker compose exec postgres bash -c 'psql -lqt | cut -d \| -f 1 | grep -qw big'; then \
	docker compose exec postgres bash -c 'psql -h 127.0.0.1 -U pguser < /seed.sql'; \
	fi
