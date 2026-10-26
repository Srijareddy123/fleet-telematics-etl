.PHONY: demo up down test lint eval bench-load logs db-shell clean

PYTHON ?= python3

# Start the stack, run all three pipelines on the committed sample data, print results.
demo:
	@test -f .env || cp .env.example .env
	docker compose up --build -d
	$(PYTHON) scripts/demo.py

up:
	@test -f .env || cp .env.example .env
	docker compose up --build -d

down:
	docker compose down

test:
	$(PYTHON) -m pytest

lint:
	$(PYTHON) -m ruff check .

# Throughput on 100000 generated telemetry rows. No Docker needed.
eval:
	$(PYTHON) scripts/benchmark.py --rows 100000

# Same, plus a Postgres upsert. Needs the stack from `make up`.
bench-load:
	POSTGRES_HOST=localhost POSTGRES_PORT=$${POSTGRES_HOST_PORT:-5432} \
		$(PYTHON) scripts/benchmark.py --rows 100000 --load

logs:
	docker compose logs -f

db-shell:
	docker compose exec postgres psql -U etl_user -d fleet_dw

# Removes containers and volumes (all warehouse and MinIO data).
clean:
	docker compose down -v
