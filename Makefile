PROJECT_NAME=crypto-data-platform

.PHONY: help init-folders up down restart logs ps airflow-init shell-web shell-scheduler test smoke init-db

help:
	@echo "Perintah tersedia:"
	@echo "  make init-folders    -> buat folder dasar project"
	@echo "  make airflow-init    -> init database Airflow + buat user admin"
	@echo "  make up              -> jalankan Airflow stack"
	@echo "  make down            -> matikan Airflow stack"
	@echo "  make restart         -> restart Airflow stack"
	@echo "  make logs            -> lihat logs semua service"
	@echo "  make ps              -> lihat status container"
	@echo "  make shell-web       -> masuk ke container airflow-webserver"
	@echo "  make shell-scheduler -> masuk ke container airflow-scheduler"
	@echo "  make init-db         -> buat database dan tabel MySQL project"
	@echo "  make smoke           -> jalankan smoke test sederhana"
	@echo "  make test            -> jalankan pytest"

init-folders:
	mkdir -p dags src scripts sql/ddl sql/dml sql/quality config docs tests data/raw data/processed data/sample logs

airflow-init:
	docker compose up airflow-postgres -d
	docker compose run --rm airflow-init

up:
	docker compose up -d airflow-postgres airflow-webserver airflow-scheduler

down:
	docker compose down

restart:
	docker compose down
	docker compose up -d airflow-postgres airflow-webserver airflow-scheduler

logs:
	docker compose logs -f

ps:
	docker compose ps

shell-web:
	docker exec -it crypto_airflow_webserver bash

shell-scheduler:
	docker exec -it crypto_airflow_scheduler bash

init-db:
	python3 scripts/init_db.py

smoke:
	python3 scripts/smoke_test.py

test:
	pytest -q