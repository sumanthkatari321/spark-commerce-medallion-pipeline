# Spark Medallion Data Pipeline

A production-minded batch data pipeline built with PySpark. It ingests e-commerce orders from CSV, validates and standardizes them through bronze and silver layers, and publishes daily sales KPIs in a gold layer.

## Architecture

```text
CSV source -> Bronze (raw parquet + audit) -> Silver (cleaned orders) -> Gold (daily sales metrics)
```

| Layer | Purpose | Output |
| --- | --- | --- |
| Bronze | Preserve the source data and ingestion metadata | `data/lake/bronze/orders/` |
| Silver | Enforce types, remove duplicates, and quarantine bad records | `data/lake/silver/orders/` |
| Gold | Build analytics-ready daily revenue KPIs | `data/lake/gold/daily_sales/` |

## Highlights

- Explicit schema and data-quality rules for every run
- Idempotent overwrite writes for predictable reruns
- Quarantine table for invalid business records
- Run-level audit metrics saved beside the lake
- Unit tests for key transformations
- Docker and GitHub Actions CI included

## Quick start

Prerequisites: Python 3.10+ and Java 17 (required by Spark).

```bash
python -m venv .venv
.venv\\Scripts\\activate  # Windows PowerShell: .\\.venv\\Scripts\\Activate.ps1
pip install -r requirements.txt
python -m spark_pipeline.run_pipeline --config config/pipeline.yaml
```

The command writes parquet datasets under `data/lake/` and prints a summary of rows processed. To execute just one stage, append `--stage bronze`, `--stage silver`, or `--stage gold`.

## Test and lint

```bash
pytest -q
ruff check .
```

## Docker

```bash
docker compose up --build
```

## Project layout

```text
src/spark_pipeline/   Pipeline code and transformations
config/               Run configuration
data/raw/              Example source data
tests/                 Transformation tests
```

For production, replace the local filesystem paths in `config/pipeline.yaml` with object storage locations such as S3, ADLS, or GCS and schedule the module with Airflow, Databricks Workflows, or another orchestrator.
