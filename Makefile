.PHONY: up down init-db test dashboard help

help:
	@echo "Available targets:"
	@echo "  up         Start the Docker stack (Kafka, TimescaleDB, Kafka UI)"
	@echo "  down       Stop the Docker stack (data preserved)"
	@echo "  down-v     Stop the Docker stack and wipe data volumes"
	@echo "  init-db    Initialise the database schema (run once after 'make up')"
	@echo "  test       Run the pytest test suite"
	@echo "  dashboard  Launch the Streamlit dashboard"

up:
	docker-compose up -d

down:
	docker-compose down

down-v:
	docker-compose down -v

init-db:
	python -c "from storage.connection import init_db; init_db()"

test:
	pytest tests/ -v

dashboard:
	streamlit run dashboard/app.py
