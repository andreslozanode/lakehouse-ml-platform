.PHONY: help install lint fmt typecheck test build up down ingest bronze silver gold pipeline train-classical train-bert train-tf serve

PY ?= python
ENV ?= dev

help:            ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*##' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*##"}{printf "  %-18s %s\n", $$1, $$2}'

install:         ## Install package + dev/spark/ml extras
	$(PY) -m pip install -e ".[dev,spark,ml,serving,delivery,cdc]"
	pre-commit install

lint:            ## Ruff + Black (check only)
	ruff check src tests
	black --check src tests

fmt:             ## Auto-format
	ruff check --fix src tests
	black src tests

typecheck:       ## mypy static analysis
	mypy src

test:            ## Unit tests with coverage
	pytest

build:           ## Build wheel
	$(PY) -m pip wheel . -w dist --no-deps

up:              ## Local CDC stack (Postgres + Kafka + Debezium)
	docker compose up -d
	./cdc/debezium/register.sh

down:            ## Tear down local stack
	docker compose down -v

ingest:          ## Kaggle + Reddit -> landing zone
	LAKEHOUSE_ENV=$(ENV) $(PY) -m lakehouse.cli ingest

bronze:          ## Landing -> Bronze Delta
	LAKEHOUSE_ENV=$(ENV) $(PY) -m lakehouse.cli bronze

silver:          ## Bronze -> Silver (clean + conform + DQ)
	LAKEHOUSE_ENV=$(ENV) $(PY) -m lakehouse.cli silver

gold:            ## Silver -> Gold (aggregates + feature table)
	LAKEHOUSE_ENV=$(ENV) $(PY) -m lakehouse.cli gold

pipeline: ingest bronze silver gold ## Full medallion run

train-classical: ## KNN + RandomForest with MLflow
	LAKEHOUSE_ENV=$(ENV) $(PY) -m lakehouse.ml.train_classical

train-bert:      ## DistilBERT fine-tuning (CUDA if available)
	LAKEHOUSE_ENV=$(ENV) $(PY) -m lakehouse.ml.train_bert_torch

train-tf:        ## TensorFlow/Keras baseline
	LAKEHOUSE_ENV=$(ENV) $(PY) -m lakehouse.ml.train_tensorflow

mlflow-ui:       ## Open local MLflow Tracking Server (docker compose service)
	@echo "MLflow UI -> http://localhost:5000 (start with 'make up')"

serve:           ## FastAPI model serving (local)
	uvicorn lakehouse.ml.serving.app:app --host 0.0.0.0 --port 8000
